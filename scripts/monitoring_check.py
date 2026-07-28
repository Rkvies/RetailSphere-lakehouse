"""
scripts/monitoring_check.py

A lightweight, run-on-a-schedule health check script - simulates what
a production monitoring job (e.g. a scheduled Lambda, or an Airflow
"monitoring" DAG) would do: read recent pipeline logs/metrics and flag
conditions that would trigger a real alert in production.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common.logger import get_logger, log_pipeline_event
from src.common.spark_session import get_spark_session

logger = get_logger(__name__)

PASS_RATE_WARNING_THRESHOLD = 95.0
PASS_RATE_CRITICAL_THRESHOLD = 90.0
NULL_FK_WARNING_THRESHOLD_PCT = 1.0


def check_referential_integrity(gold_table_path: str, fk_column: str) -> dict:
    """
    Simulates the referential-integrity alert condition from the
    monitoring plan: counts NULL surrogate keys in a Gold table as a
    proxy for facts that failed to resolve to a dimension row - the
    known gap documented in testing_strategy.md, now given an actual
    (if manual-trigger) monitoring check rather than staying purely
    theoretical.
    """
    spark = get_spark_session()
    df = spark.read.format("delta").load(gold_table_path)
    total = df.count()
    null_count = df.filter(F.col(fk_column).isNull()).count()
    null_pct = (null_count / total * 100.0) if total > 0 else 0.0

    result = {"table": gold_table_path, "fk_column": fk_column, "null_pct": round(null_pct, 3)}

    if null_pct > NULL_FK_WARNING_THRESHOLD_PCT:
        log_pipeline_event(
            logger, "referential_integrity_alert", level="WARNING",
            **result, threshold=NULL_FK_WARNING_THRESHOLD_PCT,
        )
    else:
        log_pipeline_event(logger, "referential_integrity_check_passed", **result)

    return result


def main():
    check_referential_integrity("data/gold/fact_sales", "customer_sk")
    check_referential_integrity("data/gold/fact_sales", "product_sk")


if __name__ == "__main__":
    main()