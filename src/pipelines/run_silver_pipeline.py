"""Entry point for Silver processing - routes each configured table to
the fact or SCD2 dimension processor based on its scd_type."""

from src.common.config_loader import list_configured_tables, load_table_config
from src.common.logger import get_logger, log_pipeline_event
from src.transformation.silver_processor import process_fact_table
from src.transformation.scd2_handler import process_dimension_table

logger = get_logger(__name__)


def main():
    results = {}
    for table_name in list_configured_tables():
        table_conf = load_table_config(table_name)
        try:
            if table_conf.get("scd_type") == 2:
                results[table_name] = process_dimension_table(table_name)
            else:
                results[table_name] = process_fact_table(table_name)
        except Exception as e:
            log_pipeline_event(
                logger, "silver_table_failed", level="ERROR",
                table=table_name, error=str(e),
            )
            raise
    return results


if __name__ == "__main__":
    main()
