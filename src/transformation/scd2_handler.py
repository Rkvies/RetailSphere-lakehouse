"""
src/transformation/scd2_handler.py
Silver layer - SCD Type 2 dimension path.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config_loader import load_table_config, resolve_layer_path
from src.common.data_quality import validate
from src.common.delta_utils import scd2_merge
from src.common.logger import get_logger, log_pipeline_event
from src.common.spark_session import get_spark_session
from src.transformation.silver_processor import _read_incremental_bronze

logger = get_logger(__name__)


def _deduplicate_within_batch(df: DataFrame, business_key: list[str]) -> DataFrame:
    window = Window.partitionBy(*business_key).orderBy(F.col("_ingest_ts").desc())
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def process_dimension_table(table_name: str) -> dict[str, int]:
    table_conf = load_table_config(table_name)
    if table_conf.get("scd_type") != 2:
        raise ValueError(f"'{table_name}' is not configured as scd_type=2.")

    spark = get_spark_session()
    bronze_path = resolve_layer_path("bronze", table_name)
    silver_path = resolve_layer_path("silver", table_name)
    business_key = table_conf["business_key"]
    tracked_columns = table_conf["tracked_columns"]
    surrogate_key_col = table_conf["surrogate_key_col"]

    log_pipeline_event(logger, "silver_dimension_processing_started", table=table_name)

    incremental_df = _read_incremental_bronze(spark, bronze_path, silver_path)
    incremental_count = incremental_df.count()

    if incremental_count == 0:
        log_pipeline_event(logger, "silver_dimension_processing_skipped", table=table_name, reason="no_new_bronze_data")
        return {"incremental_count": 0, "deduplicated_count": 0, "valid_count": 0, "invalid_count": 0}

    deduplicated_df = _deduplicate_within_batch(incremental_df, business_key)
    deduplicated_count = deduplicated_df.count()

    dq_rules = table_conf.get("dq_rules", [])
    result = validate(deduplicated_df, dq_rules=dq_rules, table_name=f"{table_name}_silver")

    if result.invalid_count > 0:
        quarantine_path = resolve_layer_path("quarantine", f"{table_name}_silver")
        result.invalid_df.write.format("delta").mode("append").save(quarantine_path)

    scd2_merge(
        spark, result.valid_df, silver_path,
        business_key=business_key, tracked_columns=tracked_columns,
        surrogate_key_col=surrogate_key_col,
    )

    log_pipeline_event(
        logger, "silver_dimension_processing_complete", table=table_name,
        incremental_count=incremental_count, deduplicated_count=deduplicated_count,
        valid_count=result.valid_count, invalid_count=result.invalid_count,
    )
    return {
        "incremental_count": incremental_count, "deduplicated_count": deduplicated_count,
        "valid_count": result.valid_count, "invalid_count": result.invalid_count,
    }
