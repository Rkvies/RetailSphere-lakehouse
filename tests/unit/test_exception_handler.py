"""
tests/unit/test_exception_handler.py
"""
import pytest

from src.common.exception_handler import (
    retry,
    TransientPipelineError,
    FatalPipelineError,
    SchemaValidationError,
)


def test_retry_succeeds_after_transient_failures():
    call_count = {"n": 0}

    @retry(max_attempts=3, initial_delay_seconds=0.01)
    def flaky_operation():
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise TransientPipelineError("temporary glitch")
        return "success"

    result = flaky_operation()
    assert result == "success"
    assert call_count["n"] == 3


def test_retry_exhausts_and_raises_after_max_attempts():
    call_count = {"n": 0}

    @retry(max_attempts=2, initial_delay_seconds=0.01)
    def always_fails():
        call_count["n"] += 1
        raise TransientPipelineError("still broken")

    with pytest.raises(TransientPipelineError, match="still broken"):
        always_fails()
    assert call_count["n"] == 2  # exactly max_attempts, no more


def test_retry_does_not_catch_fatal_errors():
    call_count = {"n": 0}

    @retry(max_attempts=3, initial_delay_seconds=0.01)
    def fatal_operation():
        call_count["n"] += 1
        raise SchemaValidationError("missing columns")

    with pytest.raises(SchemaValidationError):
        fatal_operation()
    # Fatal error propagates immediately - only ONE call, no retries wasted
    assert call_count["n"] == 1


def test_retry_uses_exponential_backoff(monkeypatch):
    sleep_calls = []
    monkeypatch.setattr(
        "src.common.exception_handler.time.sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    @retry(max_attempts=4, initial_delay_seconds=1.0, backoff_multiplier=2.0)
    def always_fails():
        raise TransientPipelineError("nope")

    with pytest.raises(TransientPipelineError):
        always_fails()

    assert sleep_calls == [1.0, 2.0, 4.0]  # 3 retries before final failure on attempt 4


def test_exception_hierarchy_is_correctly_structured():
    assert issubclass(SchemaValidationError, FatalPipelineError)
    assert not issubclass(SchemaValidationError, TransientPipelineError)