"""
src/common/delta_utils.py
Generic, config-driven Delta merge utilities: merge_upsert (facts / SCD1)
and scd2_merge (SCD Type 2 dimensions).

Both work against a "ref" that is EITHER a filesystem path (local dev,
or a Volume for landing files) OR a managed Unity Catalog table name
("catalog.schema.table"). Callers get this ref from
config_loader.resolve_table_ref() and never need to know which kind
it is - _is_catalog_ref() detects it here, once, and every function
branches accordingly.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.logger import get_logger, log_pipeline_event

logger = get_logger(__name__)


def _is_catalog_ref(ref: str) -> bool:
    """
    A catalog table name looks like 'catalog.schema.table' - no
    slashes. A filesystem path always contains at least one '/'
    (absolute local path, /Volumes/..., or relative 'data/...').
    """
    return "/" not in ref


def _table_exists(spark: SparkSession, ref: str) -> bool:
    if _is_catalog_ref(ref):
        return spark.catalog.tableExists(ref)
    return DeltaTable.isDeltaTable(spark, ref)


def _read_delta(spark: SparkSession, ref: str) -> DataFrame:
    if _is_catalog_ref(ref):
        return spark.table(ref)
    return spark.read.format("delta").load(ref)


def _get_delta_table(spark: SparkSession, ref: str) -> DeltaTable:
    if _is_catalog_ref(ref):
        return DeltaTable.forName(spark, ref)
    return DeltaTable.forPath(spark, ref)


def _write_delta(df: DataFrame, ref: str, mode: str, partition_by: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if _is_catalog_ref(ref):
        writer.saveAsTable(ref)
    else:
        writer.save(ref)


def merge_upsert(spark: SparkSession, source_df: DataFrame, target_ref: str, business_key: list[str]) -> None:
    if not _table_exists(spark, target_ref):
        _write_delta(source_df, target_ref, mode="overwrite")
        log_pipeline_event(logger, "merge_upsert_initial_write", target=target_ref, row_count=source_df.count())
        return

    target_table = _get_delta_table(spark, target_ref)
    join_condition = " AND ".join(f"target.{k} = source.{k}" for k in business_key)

    (
        target_table.alias("target")
        .merge(source_df.alias("source"), join_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log_pipeline_event(logger, "merge_upsert_complete", target=target_ref, business_key=business_key)


def scd2_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_ref: str,
    business_key: list[str],
    tracked_columns: list[str],
    surrogate_key_col: str,
) -> None:
    source_hashed = source_df.withColumn(
        "_row_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_columns]), 256),
    )

    if not _table_exists(spark, target_ref):
        initial_df = (
            source_hashed
            .withColumn(surrogate_key_col, F.monotonically_increasing_id())
            .withColumn("effective_start_date", F.current_date())
            .withColumn("effective_end_date", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
        )
        _write_delta(initial_df, target_ref, mode="overwrite")
        log_pipeline_event(logger, "scd2_initial_load", target=target_ref, row_count=initial_df.count())
        return

    target_table = _get_delta_table(spark, target_ref)
    target_current_df = target_table.toDF().filter(F.col("is_current") == True)  # noqa: E712
    join_condition = " AND ".join(f"t.{k} = s.{k}" for k in business_key)

    changed_or_new = (
        source_hashed.alias("s")
        .join(target_current_df.alias("t"), on=business_key, how="left")
        .where(F.col("t." + surrogate_key_col).isNull() | (F.col("t._row_hash") != F.col("s._row_hash")))
        .select("s.*")
    )

    if changed_or_new.take(1):
        (
            target_table.alias("t")
            .merge(changed_or_new.alias("s"), f"{join_condition} AND t.is_current = true")
            .whenMatchedUpdate(set={"is_current": F.lit(False), "effective_end_date": F.current_date()})
            .execute()
        )

        max_existing_sk = target_table.toDF().agg(F.max(surrogate_key_col)).collect()[0][0] or 0
        new_rows = (
            changed_or_new
            .withColumn(surrogate_key_col, F.lit(max_existing_sk) + F.monotonically_increasing_id() + F.lit(1))
            .withColumn("effective_start_date", F.current_date())
            .withColumn("effective_end_date", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
        )
        _write_delta(new_rows, target_ref, mode="append")
        log_pipeline_event(logger, "scd2_merge_complete", target=target_ref, new_or_changed_count=new_rows.count())
    else:
        log_pipeline_event(logger, "scd2_merge_noop", target=target_ref, reason="no_new_or_changed_rows")
