"""Entry point for Gold layer build - constructs the full star schema
(all dimension and fact tables), not just fact_sales."""

from src.common.logger import get_logger, log_pipeline_event
from src.aggregation.gold_builder import build_all_gold_tables

logger = get_logger(__name__)


def main():
    try:
        return build_all_gold_tables()
    except Exception as e:
        log_pipeline_event(logger, "gold_build_failed", level="ERROR", error=str(e))
        raise


if __name__ == "__main__":
    main()
