"""
src/aggregation/gold_builder.py
Gold layer: point-in-time join of Silver facts to SCD2 dimensions,
producing the star-schema fact_sales mart.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config_loader import resolve_layer_path
from src.common.logger import get_logger, log_pipeline_event
from src.common.spark_session import get_spark_session

logger = get_logger(__name__)


def _point_in_time_join(
    fact_df: DataFrame, dim_df: DataFrame,
    fact_key_col: str, dim_key_col: str, fact_date_col: str, dim_surrogate_key_col: str,
) -> DataFrame:
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


def build_fact_sales_gold() -> dict[str, int]:
    spark = get_spark_session()
    log_pipeline_event(logger, "gold_fact_sales_build_started")

    fact_sales = spark.read.format("delta").load(resolve_layer_path("silver", "sales"))
    dim_customer = spark.read.format("delta").load(resolve_layer_path("silver", "customer"))
    dim_product = spark.read.format("delta").load(resolve_layer_path("silver", "product"))
    dim_store = spark.read.format("delta").load(resolve_layer_path("silver", "store"))

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

    # dim_store: SCD Type 1 - plain equi-join (built via merge_upsert, so
    # its surrogate key column, if present, is just "store_sk" per config
    # convention; if store.csv doesn't have one yet, this join simply
    # won't add store_sk - see note in table_config.yaml about adding a
    # surrogate key generation step for SCD1 tables if you need store_sk
    # downstream).
    store_cols = ["store_id"] + (["store_sk"] if "store_sk" in dim_store.columns else [])
    gold_fact_sales = with_product_sk.join(dim_store.select(*store_cols), on="store_id", how="left")

    gold_fact_sales = (
        gold_fact_sales
        .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    )

    select_cols = ["invoice_id", "customer_sk", "product_sk", "sale_date", "quantity", "unit_price", "line_total"]
    if "store_sk" in gold_fact_sales.columns:
        select_cols.insert(3, "store_sk")
    elif "store_id" in gold_fact_sales.columns:
        select_cols.insert(3, "store_id")

    gold_fact_sales = gold_fact_sales.select(*select_cols)

    row_count = gold_fact_sales.count()
    gold_path = resolve_layer_path("gold", "fact_sales")
    gold_fact_sales.write.format("delta").mode("overwrite").partitionBy("sale_date").save(gold_path)

    zorder_cols = "customer_sk, product_sk"
    spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY ({zorder_cols})")

    log_pipeline_event(logger, "gold_fact_sales_build_complete", row_count=row_count)
    return {"row_count": row_count}
