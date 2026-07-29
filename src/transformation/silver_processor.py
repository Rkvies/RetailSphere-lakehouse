"""
src/transformation/silver_processor.py
Silver layer - FACT TABLE (and SCD1) path: incremental read from
Bronze, deduplicate, re-validate, merge_upsert into Silver.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config_loader import load_table_config, resolve_layer_path, resolve_catalog_table_name
from src.common.data_quality import validate
from src.common.delta_utils import merge_upsert, register_as_table, _table_exists
from src.common.logger import get_logger, log_pipeline_event
from src.common.spark_session import get_spark_session

logger = get_logger(__name__)


def _read_incremental_bronze(spark: SparkSession, bronze_path: str, silver_path: str) -> DataFrame:
    bronze_df = spark.read.format("delta").load(bronze_path)

    if not _table_exists(spark, silver_path):
        return bronze_df

    silver_df = spark.read.format("delta").load(silver_path)
    max_processed_date = silver_df.agg(F.max("_ingest_date")).collect()[0][0]

    if max_processed_date is None:
        return bronze_df

    return bronze_df.filter(F.col("_ingest_date") > F.lit(max_processed_date))


def _deduplicate_on_business_key(df: DataFrame, business_key: list[str]) -> DataFrame:
    window = Window.partitionBy(*business_key).orderBy(F.col("_ingest_ts").desc())
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def process_fact_table(table_name: str) -> dict[str, int]:
    table_conf = load_table_config(table_name)
    if table_conf.get("scd_type") == 2:
        raise ValueError(
            f"'{table_name}' is configured as scd_type=2 - use scd2_handler.process_dimension_table() instead."
        )

    spark = get_spark_session()
    bronze_path = resolve_layer_path("bronze", table_name)
    silver_path = resolve_layer_path("silver", table_name)
    business_key = table_conf["business_key"]

    log_pipeline_event(logger, "silver_fact_processing_started", table=table_name)

    incremental_df = _read_incremental_bronze(spark, bronze_path, silver_path)
    incremental_count = incremental_df.count()

    if incremental_count == 0:
        log_pipeline_event(logger, "silver_fact_processing_skipped", table=table_name, reason="no_new_bronze_data")
        return {"incremental_count": 0, "deduplicated_count": 0, "valid_count": 0, "invalid_count": 0}

    deduplicated_df = _deduplicate_on_business_key(incremental_df, business_key)
    deduplicated_count = deduplicated_df.count()

    dq_rules = table_conf.get("dq_rules", [])
    result = validate(deduplicated_df, dq_rules=dq_rules, table_name=f"{table_name}_silver")

    if result.invalid_count > 0:
        quarantine_path = resolve_layer_path("quarantine", f"{table_name}_silver")
        result.invalid_df.write.format("delta").mode("append").save(quarantine_path)

    merge_upsert(spark, result.valid_df, silver_path, business_key=business_key)
    register_as_table(spark, silver_path, resolve_catalog_table_name("silver", table_name))

    log_pipeline_event(
        logger, "silver_fact_processing_complete", table=table_name,
        incremental_count=incremental_count, deduplicated_count=deduplicated_count,
        valid_count=result.valid_count, invalid_count=result.invalid_count,
    )
    return {
        "incremental_count": incremental_count, "deduplicated_count": deduplicated_count,
        "valid_count": result.valid_count, "invalid_count": result.invalid_count,
    }
