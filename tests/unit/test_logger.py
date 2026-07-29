import json
import logging

from src.common.logger import get_logger, log_pipeline_event, JsonFormatter


def test_logger_returns_configured_logger():
    logger = get_logger("test_module_logger", level="DEBUG")
    assert logger.level == logging.DEBUG
    assert logger.name == "test_module_logger"


def test_logger_does_not_duplicate_handlers_on_repeated_calls():
    logger1 = get_logger("test_dup_module")
    logger2 = get_logger("test_dup_module")
    assert logger1 is logger2
    assert len(logger1.handlers) == 1


def test_json_formatter_produces_valid_json():
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="test_event", args=(), exc_info=None,
    )
    parsed = json.loads(formatter.format(record))
    assert parsed["message"] == "test_event"
    assert parsed["level"] == "INFO"
    assert "timestamp" in parsed


def test_json_formatter_includes_extra_context_fields():
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="bronze_load_complete", args=(), exc_info=None,
    )
    record.table = "sales"
    record.row_count = 15000
    parsed = json.loads(formatter.format(record))
    assert parsed["table"] == "sales"
    assert parsed["row_count"] == 15000


def test_log_pipeline_event_respects_level(caplog):
    logger = get_logger("test_level_module", level="DEBUG")
    with caplog.at_level(logging.WARNING, logger="test_level_module"):
        log_pipeline_event(logger, "quarantine_triggered", level="WARNING", rejected_count=5)
    assert "quarantine_triggered" in caplog.text
