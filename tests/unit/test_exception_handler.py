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

    assert flaky_operation() == "success"
    assert call_count["n"] == 3


def test_retry_exhausts_and_raises_after_max_attempts():
    call_count = {"n": 0}

    @retry(max_attempts=2, initial_delay_seconds=0.01)
    def always_fails():
        call_count["n"] += 1
        raise TransientPipelineError("still broken")

    with pytest.raises(TransientPipelineError, match="still broken"):
        always_fails()
    assert call_count["n"] == 2


def test_retry_does_not_catch_fatal_errors():
    call_count = {"n": 0}

    @retry(max_attempts=3, initial_delay_seconds=0.01)
    def fatal_operation():
        call_count["n"] += 1
        raise SchemaValidationError("missing columns")

    with pytest.raises(SchemaValidationError):
        fatal_operation()
    assert call_count["n"] == 1


def test_retry_uses_exponential_backoff(monkeypatch):
    sleep_calls = []
    monkeypatch.setattr("src.common.exception_handler.time.sleep", lambda s: sleep_calls.append(s))

    @retry(max_attempts=4, initial_delay_seconds=1.0, backoff_multiplier=2.0)
    def always_fails():
        raise TransientPipelineError("nope")

    with pytest.raises(TransientPipelineError):
        always_fails()

    assert sleep_calls == [1.0, 2.0, 4.0]


def test_exception_hierarchy_is_correctly_structured():
    assert issubclass(SchemaValidationError, FatalPipelineError)
    assert not issubclass(SchemaValidationError, TransientPipelineError)
