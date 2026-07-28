"""
src/common/logger.py
Centralized structured (JSON) logging, imported by every other module.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_object: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage(),
        }
        reserved_keys = logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()
        extra_fields = {
            key: value for key, value in record.__dict__.items()
            if key not in reserved_keys and key != "message"
        }
        log_object.update(extra_fields)
        if record.exc_info:
            log_object["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_object, default=str)


def get_logger(name: str, level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        formatter = JsonFormatter()
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        logger.propagate = False

    return logger


def log_pipeline_event(logger: logging.Logger, event: str, level: str = "INFO", **context: Any) -> None:
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(event, extra=context)
