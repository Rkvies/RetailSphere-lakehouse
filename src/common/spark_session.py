"""
src/common/spark_session.py

Centralized SparkSession builder with Delta Lake extensions configured
consistently across every pipeline module.

Databricks-aware: on Databricks, a SparkSession already exists (injected
as the global `spark` variable in every notebook/job) with Delta already
configured at the cluster level. We detect that environment and reuse
the active session rather than building a second, conflicting one.
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession

from src.common.logger import get_logger, log_pipeline_event

logger = get_logger(__name__)

_spark_session: Optional[SparkSession] = None


def get_spark_session(app_name: str = "retail_lakehouse", shuffle_partitions: int = 8) -> SparkSession:
    """
    Returns a singleton SparkSession, correctly configured for whichever
    environment this code is running in (Databricks vs. local).
    """
    global _spark_session

    if _spark_session is not None:
        return _spark_session

    is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if is_databricks:
        active = SparkSession.getActiveSession()
        if active is not None:
            _spark_session = active
            log_pipeline_event(logger, "spark_session_reused_databricks", app_name=app_name)
            return _spark_session
        _spark_session = SparkSession.builder.appName(app_name).getOrCreate()
        log_pipeline_event(logger, "spark_session_created_databricks_fallback", app_name=app_name)
        return _spark_session

    # Local (non-Databricks) path - build a Delta-configured session.
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    )
    _spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    log_pipeline_event(logger, "spark_session_created", app_name=app_name, shuffle_partitions=shuffle_partitions)
    return _spark_session


def stop_spark_session() -> None:
    """Stops and clears the cached session reference.

    On Databricks, do NOT actually call spark.stop() - that kills the
    notebook's shared session for everyone. This only clears our own
    module-level cache so get_spark_session() re-resolves next call.
    """
    global _spark_session
    is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
    if _spark_session is not None and not is_databricks:
        _spark_session.stop()
    _spark_session = None
