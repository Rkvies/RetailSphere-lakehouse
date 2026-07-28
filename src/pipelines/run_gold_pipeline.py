"""Entry point for Gold aggregation."""

from src.common.logger import get_logger, log_pipeline_event
from src.aggregation.gold_builder import build_fact_sales_gold

logger = get_logger(__name__)


def main():
    try:
        return build_fact_sales_gold()
    except Exception as e:
        log_pipeline_event(logger, "gold_build_failed", level="ERROR", error=str(e))
        raise


if __name__ == "__main__":
    main()
