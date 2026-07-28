"""
src/common/exception_handler.py
Custom exception hierarchy (transient vs fatal) + retry decorator.
"""
from __future__ import annotations

import functools
import time
from typing import Callable, Optional, ParamSpec, Type, TypeVar

from src.common.logger import get_logger, log_pipeline_event

logger = get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


class PipelineError(Exception):
    """Base class for all custom exceptions in this platform."""


class TransientPipelineError(PipelineError):
    """Retrying has a reasonable chance of succeeding."""


class FatalPipelineError(PipelineError):
    """Retrying will not help - needs a code/config/data fix."""


class SourceFileTemporarilyUnavailableError(TransientPipelineError):
    pass


class DeltaConcurrencyError(TransientPipelineError):
    """Wraps Delta's ConcurrentAppendException - worth retrying."""


class SchemaValidationError(FatalPipelineError):
    pass


class SourceFileNotFoundError(FatalPipelineError):
    pass


class ConfigurationError(FatalPipelineError):
    pass


class BusinessKeyViolationError(FatalPipelineError):
    pass


def retry(
    max_attempts: int = 3,
    initial_delay_seconds: float = 2.0,
    backoff_multiplier: float = 2.0,
    retryable_exceptions: tuple[Type[Exception], ...] = (TransientPipelineError,),
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            delay = initial_delay_seconds
            last_exception: Optional[Exception] = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt == max_attempts:
                        log_pipeline_event(
                            logger, "retry_exhausted", level="ERROR",
                            function=func.__name__, attempts=attempt, error=str(e),
                        )
                        raise
                    log_pipeline_event(
                        logger, "retry_attempt", level="WARNING",
                        function=func.__name__, attempt=attempt,
                        max_attempts=max_attempts, delay_seconds=delay, error=str(e),
                    )
                    time.sleep(delay)
                    delay *= backoff_multiplier

            if last_exception:
                raise last_exception
            raise RuntimeError("retry() exited loop unexpectedly")

        return wrapper
    return decorator
