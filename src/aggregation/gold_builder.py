"""
src/aggregation/gold_builder.py
Gold layer: builds the FULL star schema for BI consumption (Power BI,
etc.) - not just fact_sales. Reads Silver, applies point-in-time joins
where SCD2 history matters, writes every table as a managed Unity
Catalog table (or a local path in dev) under the "gold" schema/folder.

Build order matters: dimensions first (dim_customer, dim_product,
dim_store, dim_supplier, dim_date), then fact_sales (needs all four
business dims + dim_date), then fact_returns (needs fact_sales, for
customer_sk/product_sk inheritance), then fact_online_orders, then
fact_shipping (needs fact_online_orders), then fact_inventory_snapshot.
build_all_gold_tables() runs them in this order.
"""
from __future__ import annotations

import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config_loader import resolve_table_ref
from src.common.delta_utils import _read_delta, _write_delta, _is_catalog_ref, _table_exists
from src.common.logger import get_logger, log_pipeline_event
from src.common.spark_session import get_spark_session

logger = get_logger(__name__)


def _optimize(spark, ref: str, zorder_cols: str) -> None:
    target = ref if _is_catalog_ref(ref) else f"delta.`{ref}`"
    spark.sql(f"OPTIMIZE {target} ZORDER BY ({zorder_cols})")


def _point_in_time_join(
    fact_df: DataFrame, dim_df: DataFrame,
    fact_key_col: str, dim_key_col: str, fact_date_col: str, dim_surrogate_key_col: str,
) -> DataFrame:
    """
    Joins a fact to an SCD2 dimension using the version effective ON THE
    FACT'S EVENT DATE - not the dimension's current version. See
    docs/architecture.md for why this matters (prevents ML data leakage,
    ensures historically accurate reporting).
    """
    dim_with_sentinel = dim_df.withColumn(
        "_effective_end_date_resolved",
        F.coalesce(F.col("effective_end_date"), F.lit("9999-12-31").cast("date")),
    )
    join_condition = (
        (fact_df[fact_key_col] == dim_with_sentinel[dim_key_col])
        & (fact_df[fact_date_col] >= dim_with_sentinel["effective_start_date"])
        & (fact_df[fact_date_col] <= dim_with_sentinel["_effective_end_date_resolved"])
    )
    joined = fact_df.join(
        dim_with_sentinel.select(dim_key_col, dim_surrogate_key_col, "effective_start_date", "_effective_end_date_resolved"),
        on=join_condition, how="left",
    ).drop("_effective_end_date_resolved", "effective_start_date", dim_key_col)
    return joined


# ---------------------------------------------------------------------
# Dimensions - Gold versions are Silver pass-throughs. Silver already
# holds the conformed, deduplicated, (for SCD2 tables) historized data;
# Gold's job here is just to expose them under business-facing names in
# the gold schema for BI tools to consume directly, without also
# granting BI tools read access to the Silver schema.
# ---------------------------------------------------------------------

def build_dim_customer_gold() -> dict[str, int]:
    spark = get_spark_session()
    df = _read_delta(spark, resolve_table_ref("silver", "customer"))
    ref = resolve_table_ref("gold", "dim_customer")
    _write_delta(df, ref, mode="overwrite")
    row_count = df.count()
    log_pipeline_event(logger, "gold_dim_customer_build_complete", row_count=row_count)
    return {"row_count": row_count}


def build_dim_product_gold() -> dict[str, int]:
    spark = get_spark_session()
    df = _read_delta(spark, resolve_table_ref("silver", "product"))
    ref = resolve_table_ref("gold", "dim_product")
    _write_delta(df, ref, mode="overwrite")
    row_count = df.count()
    log_pipeline_event(logger, "gold_dim_product_build_complete", row_count=row_count)
    return {"row_count": row_count}


def build_dim_store_gold() -> dict[str, int]:
    spark = get_spark_session()
    df = _read_delta(spark, resolve_table_ref("silver", "store"))
    ref = resolve_table_ref("gold", "dim_store")
    _write_delta(df, ref, mode="overwrite")
    row_count = df.count()
    log_pipeline_event(logger, "gold_dim_store_build_complete", row_count=row_count)
    return {"row_count": row_count}


def build_dim_supplier_gold() -> dict[str, int]:
    spark = get_spark_session()
    df = _read_delta(spark, resolve_table_ref("silver", "supplier"))
    ref = resolve_table_ref("gold", "dim_supplier")
    _write_delta(df, ref, mode="overwrite")
    row_count = df.count()
    log_pipeline_event(logger, "gold_dim_supplier_build_complete", row_count=row_count)
    return {"row_count": row_count}


def build_dim_date_gold(start: str = "2026-01-01", end: str = "2026-12-31") -> dict[str, int]:
    """
    Generates a standard calendar date dimension. Not sourced from any
    Silver table - date dimensions are conventionally generated once,
    not extracted from an operational system.
    """
    spark = get_spark_session()
    start_date = datetime.date.fromisoformat(start)
    end_date = datetime.date.fromisoformat(end)

    rows = []
    current = start_date
    while current <= end_date:
        rows.append((
            int(current.strftime("%Y%m%d")),
            current,
            current.year,
            current.month,
            current.day,
            (current.month - 1) // 3 + 1,
            current.strftime("%A"),
            current.strftime("%B"),
            current.weekday() >= 5,
        ))
        current += datetime.timedelta(days=1)

    df = spark.createDataFrame(
        rows,
        ["date_sk", "full_date", "year", "month", "day", "quarter", "day_name", "month_name", "is_weekend"],
    )
    ref = resolve_table_ref("gold", "dim_date")
    _write_delta(df, ref, mode="overwrite")
    row_count = df.count()
    log_pipeline_event(logger, "gold_dim_date_build_complete", row_count=row_count)
    return {"row_count": row_count}


# ---------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------

def build_fact_sales_gold() -> dict[str, int]:
    spark = get_spark_session()
    log_pipeline_event(logger, "gold_fact_sales_build_started")

    fact_sales = _read_delta(spark, resolve_table_ref("silver", "sales"))
    dim_customer = _read_delta(spark, resolve_table_ref("silver", "customer"))
    dim_product = _read_delta(spark, resolve_table_ref("silver", "product"))
    dim_store = _read_delta(spark, resolve_table_ref("silver", "store"))

    with_customer_sk = _point_in_time_join(
        fact_sales, dim_customer,
        fact_key_col="customer_id", dim_key_col="customer_id",
        fact_date_col="sale_date", dim_surrogate_key_col="customer_sk",
    )
    with_product_sk = _point_in_time_join(
        with_customer_sk, dim_product,
        fact_key_col="stock_code", dim_key_col="stock_code",
        fact_date_col="sale_date", dim_surrogate_key_col="product_sk",
    )

    store_cols = ["store_id"] + (["store_sk"] if "store_sk" in dim_store.columns else [])
    gold_fact_sales = with_product_sk.join(dim_store.select(*store_cols), on="store_id", how="left")
    gold_fact_sales = gold_fact_sales.withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    gold_fact_sales = gold_fact_sales.withColumn("date_sk", F.date_format("sale_date", "yyyyMMdd").cast("int"))

    select_cols = ["invoice_id", "customer_sk", "product_sk", "date_sk", "sale_date", "quantity", "unit_price", "line_total"]
    if "store_sk" in gold_fact_sales.columns:
        select_cols.insert(3, "store_sk")
    elif "store_id" in gold_fact_sales.columns:
        select_cols.insert(3, "store_id")
    gold_fact_sales = gold_fact_sales.select(*select_cols)

    row_count = gold_fact_sales.count()
    ref = resolve_table_ref("gold", "fact_sales")
    _write_delta(gold_fact_sales, ref, mode="overwrite", partition_by=["sale_date"])
    _optimize(spark, ref, "customer_sk, product_sk")

    log_pipeline_event(logger, "gold_fact_sales_build_complete", row_count=row_count)
    return {"row_count": row_count}


def build_fact_returns_gold() -> dict[str, int]:
    """
    Joins to Gold fact_sales (not Silver dims directly) via
    original_invoice_id - inherits customer_sk/product_sk/sale_date
    that were ALREADY correctly point-in-time resolved for the original
    sale, rather than re-deriving them (a return's "as of" date should
    logically be the original sale's date, not the return's own date).
    """
    spark = get_spark_session()
    fact_returns = _read_delta(spark, resolve_table_ref("silver", "returns"))
    gold_fact_sales_ref = resolve_table_ref("gold", "fact_sales")

    if not _table_exists(spark, gold_fact_sales_ref):
        log_pipeline_event(logger, "gold_fact_returns_skipped", level="WARNING", reason="fact_sales not yet built")
        return {"row_count": 0}

    gold_fact_sales = _read_delta(spark, gold_fact_sales_ref)

    joined = fact_returns.join(
        gold_fact_sales.select(
            F.col("invoice_id").alias("original_invoice_id"),
            "customer_sk", "product_sk",
        ),
        on="original_invoice_id", how="left",
    )
    result = joined.select("return_invoice_id", "original_invoice_id", "customer_sk", "product_sk", "quantity")

    row_count = result.count()
    ref = resolve_table_ref("gold", "fact_returns")
    _write_delta(result, ref, mode="overwrite")
    log_pipeline_event(logger, "gold_fact_returns_build_complete", row_count=row_count)
    return {"row_count": row_count}


def build_fact_online_orders_gold() -> dict[str, int]:
    spark = get_spark_session()
    fact_orders = _read_delta(spark, resolve_table_ref("silver", "online_orders"))
    dim_customer = _read_delta(spark, resolve_table_ref("silver", "customer"))
    dim_product = _read_delta(spark, resolve_table_ref("silver", "product"))

    with_customer_sk = _point_in_time_join(
        fact_orders, dim_customer,
        fact_key_col="customer_id", dim_key_col="customer_id",
        fact_date_col="order_date", dim_surrogate_key_col="customer_sk",
    )
    with_product_sk = _point_in_time_join(
        with_customer_sk, dim_product,
        fact_key_col="stock_code", dim_key_col="stock_code",
        fact_date_col="order_date", dim_surrogate_key_col="product_sk",
    )
    result = with_product_sk.select("order_id", "customer_sk", "product_sk", "order_status", "order_date")

    row_count = result.count()
    ref = resolve_table_ref("gold", "fact_online_orders")
    _write_delta(result, ref, mode="overwrite", partition_by=["order_date"])
    log_pipeline_event(logger, "gold_fact_online_orders_build_complete", row_count=row_count)
    return {"row_count": row_count}


def build_fact_shipping_gold() -> dict[str, int]:
    """Inherits customer_sk from Gold fact_online_orders via order_id."""
    spark = get_spark_session()
    fact_shipping = _read_delta(spark, resolve_table_ref("silver", "shipping"))
    gold_orders_ref = resolve_table_ref("gold", "fact_online_orders")

    if not _table_exists(spark, gold_orders_ref):
        log_pipeline_event(logger, "gold_fact_shipping_skipped", level="WARNING", reason="fact_online_orders not yet built")
        return {"row_count": 0}

    gold_orders = _read_delta(spark, gold_orders_ref)
    joined = fact_shipping.join(gold_orders.select("order_id", "customer_sk"), on="order_id", how="left")
    result = joined.select("shipment_id", "order_id", "customer_sk", "carrier", "ship_date")

    row_count = result.count()
    ref = resolve_table_ref("gold", "fact_shipping")
    _write_delta(result, ref, mode="overwrite")
    log_pipeline_event(logger, "gold_fact_shipping_build_complete", row_count=row_count)
    return {"row_count": row_count}


def build_fact_inventory_snapshot_gold() -> dict[str, int]:
    spark = get_spark_session()
    fact_inv = _read_delta(spark, resolve_table_ref("silver", "inventory"))
    dim_product = _read_delta(spark, resolve_table_ref("silver", "product"))
    dim_store = _read_delta(spark, resolve_table_ref("silver", "store"))

    with_product_sk = _point_in_time_join(
        fact_inv, dim_product,
        fact_key_col="stock_code", dim_key_col="stock_code",
        fact_date_col="snapshot_date", dim_surrogate_key_col="product_sk",
    )
    store_cols = ["store_id"] + (["store_sk"] if "store_sk" in dim_store.columns else [])
    joined = with_product_sk.join(dim_store.select(*store_cols), on="store_id", how="left")

    select_cols = ["product_sk", "snapshot_date", "stock_on_hand"]
    if "store_sk" in joined.columns:
        select_cols.insert(0, "store_sk")
    elif "store_id" in joined.columns:
        select_cols.insert(0, "store_id")
    result = joined.select(*select_cols)

    row_count = result.count()
    ref = resolve_table_ref("gold", "fact_inventory_snapshot")
    _write_delta(result, ref, mode="overwrite", partition_by=["snapshot_date"])
    log_pipeline_event(logger, "gold_fact_inventory_snapshot_build_complete", row_count=row_count)
    return {"row_count": row_count}


# ---------------------------------------------------------------------
# Orchestrator - build order matters (dims before facts; fact_sales
# before fact_returns; fact_online_orders before fact_shipping)
# ---------------------------------------------------------------------

def build_all_gold_tables() -> dict[str, dict[str, int]]:
    results = {}
    results["dim_customer"] = build_dim_customer_gold()
    results["dim_product"] = build_dim_product_gold()
    results["dim_store"] = build_dim_store_gold()
    results["dim_supplier"] = build_dim_supplier_gold()
    results["dim_date"] = build_dim_date_gold()

    results["fact_sales"] = build_fact_sales_gold()
    results["fact_returns"] = build_fact_returns_gold()
    results["fact_online_orders"] = build_fact_online_orders_gold()
    results["fact_shipping"] = build_fact_shipping_gold()
    results["fact_inventory_snapshot"] = build_fact_inventory_snapshot_gold()

    log_pipeline_event(logger, "gold_build_all_complete", tables_built=list(results.keys()))
    return results
