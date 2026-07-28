"""Entry point invoked by Airflow/Databricks Job (or manually) to run
Bronze ingestion across every configured table."""

from src.common.config_loader import list_configured_tables
from src.common.logger import get_logger, log_pipeline_event
from src.ingestion.bronze_loader import run_bronze_ingestion

logger = get_logger(__name__)


def main():
    results = {}
    for table_name in list_configured_tables():
        try:
            results[table_name] = run_bronze_ingestion(table_name)
        except Exception as e:
            log_pipeline_event(
                logger, "bronze_table_failed", level="ERROR",
                table=table_name, error=str(e),
            )
            raise
    return results


if __name__ == "__main__":
    main()
