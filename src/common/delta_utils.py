"""
src/common/delta_utils.py
Generic, config-driven Delta merge utilities: merge_upsert (facts / SCD1)
and scd2_merge (SCD Type 2 dimensions).
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.logger import get_logger, log_pipeline_event

logger = get_logger(__name__)


def _table_exists(spark: SparkSession, table_path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, table_path)


def merge_upsert(spark: SparkSession, source_df: DataFrame, target_path: str, business_key: list[str]) -> None:
    if not _table_exists(spark, target_path):
        source_df.write.format("delta").mode("overwrite").save(target_path)
        log_pipeline_event(logger, "merge_upsert_initial_write", target=target_path, row_count=source_df.count())
        return

    target_table = DeltaTable.forPath(spark, target_path)
    join_condition = " AND ".join(f"target.{k} = source.{k}" for k in business_key)

    (
        target_table.alias("target")
        .merge(source_df.alias("source"), join_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log_pipeline_event(logger, "merge_upsert_complete", target=target_path, business_key=business_key)


def scd2_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    business_key: list[str],
    tracked_columns: list[str],
    surrogate_key_col: str,
) -> None:
    source_hashed = source_df.withColumn(
        "_row_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_columns]), 256),
    )

    if not _table_exists(spark, target_path):
        initial_df = (
            source_hashed
            .withColumn(surrogate_key_col, F.monotonically_increasing_id())
            .withColumn("effective_start_date", F.current_date())
            .withColumn("effective_end_date", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
        )
        initial_df.write.format("delta").mode("overwrite").save(target_path)
        log_pipeline_event(logger, "scd2_initial_load", target=target_path, row_count=initial_df.count())
        return

    target_table = DeltaTable.forPath(spark, target_path)
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
        new_rows.write.format("delta").mode("append").save(target_path)
        log_pipeline_event(logger, "scd2_merge_complete", target=target_path, new_or_changed_count=new_rows.count())
    else:
        log_pipeline_event(logger, "scd2_merge_noop", target=target_path, reason="no_new_or_changed_rows")
