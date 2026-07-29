"""
src/ingestion/bronze_loader.py
Generic, config-driven Bronze ingestion. Paths are resolved via
config_loader.resolve_layer_path() - NOT hardcoded - so this works
unchanged across local, Databricks/DBFS, or Unity Catalog Volumes;
only config/<env>_config.yaml's paths.* values differ per environment.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.common.config_loader import load_table_config, resolve_layer_path, resolve_table_ref
from src.common.data_quality import validate
from src.common.delta_utils import _write_delta
from src.common.exception_handler import SchemaValidationError, SourceFileNotFoundError
from src.common.logger import get_logger, log_pipeline_event
from src.common.spark_session import get_spark_session

logger = get_logger(__name__)


def _build_schema(schema_config: list[dict[str, str]]) -> StructType:
    from pyspark.sql.types import (
        StringType, IntegerType, DoubleType, DateType, TimestampType, StructField,
    )
    type_map = {
        "string": StringType(), "integer": IntegerType(), "double": DoubleType(),
        "date": DateType(), "timestamp": TimestampType(),
    }
    fields = []
    for col_def in schema_config:
        spark_type = type_map.get(col_def["type"])
        if spark_type is None:
            raise SchemaValidationError(f"Unsupported schema type '{col_def['type']}' for column '{col_def['name']}'")
        fields.append(StructField(col_def["name"], spark_type, nullable=True))
    return StructType(fields)


def _tag_metadata(df: DataFrame, source_path: str, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.lit(source_path))
        .withColumn("_ingest_ts", F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingest_date", F.current_date())
    )


def run_bronze_ingestion(table_name: str) -> dict[str, int]:
    batch_id = str(uuid.uuid4())
    table_conf = load_table_config(table_name)
    spark = get_spark_session()

    log_pipeline_event(logger, "bronze_ingestion_started", table=table_name, batch_id=batch_id)

    source_path = resolve_layer_path("landing", table_name)
    if not os.path.exists(source_path):
        raise SourceFileNotFoundError(f"Source path does not exist: {source_path}")

    schema = _build_schema(table_conf["schema"])

    try:
        raw_df = spark.read.csv(source_path, header=True, schema=schema, mode="FAILFAST")
    except Exception as e:
        raise SchemaValidationError(
            f"Failed to read '{table_name}' from {source_path} against configured schema: {e}"
        ) from e

    tagged_df = _tag_metadata(raw_df, source_path, batch_id)

    dq_rules = table_conf.get("dq_rules", [])
    result = validate(tagged_df, dq_rules=dq_rules, table_name=table_name)

    bronze_ref = resolve_table_ref("bronze", table_name)
    quarantine_ref = resolve_table_ref("quarantine", table_name)

    _write_delta(result.valid_df, bronze_ref, mode="append", partition_by=["_ingest_date"])

    if result.invalid_count > 0:
        _write_delta(result.invalid_df, quarantine_ref, mode="append")

    log_pipeline_event(
        logger, "bronze_ingestion_complete", table=table_name, batch_id=batch_id,
        valid_count=result.valid_count, invalid_count=result.invalid_count,
        pass_rate=round(result.pass_rate, 2),
    )
    return {"valid_count": result.valid_count, "invalid_count": result.invalid_count}
