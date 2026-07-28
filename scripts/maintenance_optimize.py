"""
scripts/maintenance_optimize.py

Periodic Delta table maintenance - separate from the daily pipeline
run, since OPTIMIZE/VACUUM are maintenance operations, not part of the
core ETL contract, and running them too frequently wastes compute for
no benefit (small daily deltas don't create enough new small files to
justify a daily OPTIMIZE on every table).
"""
from src.common.spark_session import get_spark_session
from src.common.logger import get_logger, log_pipeline_event

logger = get_logger(__name__)

# Weekly cadence, not daily - OPTIMIZE has a real compute/write cost;
# running it too often wastes cluster time compacting files that
# haven't yet accumulated enough fragmentation to matter.
TABLES_TO_OPTIMIZE = {
    "data/gold/fact_sales": "customer_sk, product_sk",
    "data/silver/customer": "customer_id",
    "data/silver/product": "stock_code",
}


def run_optimize_and_vacuum():
    spark = get_spark_session()
    for table_path, zorder_cols in TABLES_TO_OPTIMIZE.items():
        spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_cols})")
        # VACUUM removes files no longer referenced by the Delta log,
        # older than the retention threshold - default 7 days, NEVER
        # lowered below Delta's safety minimum without disabling the
        # safety check explicitly, since doing so risks corrupting
        # concurrent readers/time-travel queries mid-flight.
        spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS")
        log_pipeline_event(logger, "table_maintenance_complete", table=table_path)


if __name__ == "__main__":
    run_optimize_and_vacuum()