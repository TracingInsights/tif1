"""Tests for retry module."""

import pytest

from tif1.retry import (
    CircuitBreaker,
    get_circuit_breaker,
    reset_circuit_breaker,
    retry_with_backoff,
)


class TestCircuitBreaker:
    def test_open_state_blocks_when_timeout_not_elapsed(self):
        breaker = CircuitBreaker(threshold=1, timeout=60)
        breaker.record_failure()
        assert breaker.state == "open"

        should_proceed, state = breaker.check_and_update_state()
        assert should_proceed is False
        assert state == "open"

    def test_open_state_transitions_to_half_open_after_timeout(self):
        breaker = CircuitBreaker(threshold=1, timeout=1)
        breaker.record_failure()
        assert breaker.state == "open"

        # Simulate elapsed timeout deterministically.
        assert breaker._last_failure_monotonic is not None
        breaker._last_failure_monotonic -= 5

        should_proceed, state = breaker.check_and_update_state()
        assert should_proceed is True
        assert state == "half-open"

    def test_success_in_half_open_closes_circuit(self):
        breaker = CircuitBreaker(threshold=1, timeout=1)
        breaker.record_failure()
        assert breaker.state == "open"

        assert breaker._last_failure_monotonic is not None
        breaker._last_failure_monotonic -= 5

        result = breaker.call(lambda: "ok")
        assert result == "ok"
        assert breaker.state == "closed"
        assert breaker.failures == 0


class TestRetryDecorator:
    def test_retry_raises_last_exception_after_exhaustion(self):
        reset_circuit_breaker()  # Ensure clean state

        @retry_with_backoff(max_retries=2, backoff_factor=0, jitter=False, exceptions=(ValueError,))
        def always_fail():
            raise ValueError("fail")

        with pytest.raises(ValueError, match="fail"):
            always_fail()


class TestCircuitBreakerCall:
    """Test CircuitBreaker.call edge cases."""

    def test_call_open_timeout_not_elapsed_raises(self):
        """call() raises when circuit is open and timeout has not elapsed."""
        reset_circuit_breaker()  # Ensure clean state
        breaker = CircuitBreaker(threshold=1, timeout=9999)
        breaker.record_failure()
        assert breaker.state == "open"

        with pytest.raises(Exception, match="Circuit breaker is open"):
            breaker.call(lambda: "should not run")

    def test_call_failure_increments_and_opens(self):
        """Failures through call() increment counter and eventually open circuit."""
        reset_circuit_breaker()  # Ensure clean state
        breaker = CircuitBreaker(threshold=3, timeout=60)
        assert breaker.state == "closed"

        for _i in range(3):
            with pytest.raises(ValueError, match="boom"):
                breaker.call(lambda: (_ for _ in ()).throw(ValueError("boom")))

        assert breaker.state == "open"
        assert breaker.failures == 3


class TestRecordSuccess:
    """Test record_success in closed state."""

    def test_resets_failures_in_closed_state(self):
        """record_success in closed state resets failure counter."""
        reset_circuit_breaker()  # Ensure clean state
        breaker = CircuitBreaker(threshold=5, timeout=60)
        breaker.failures = 3
        breaker.record_success()
        assert breaker.failures == 0
        assert breaker.state == "closed"

    def test_does_not_reset_failures_in_open_state(self):
        """record_success in open state does not reset failure counter."""
        reset_circuit_breaker()  # Ensure clean state
        breaker = CircuitBreaker(threshold=5, timeout=60)
        # Open the circuit
        for _ in range(5):
            breaker.record_failure()
        assert breaker.state == "open"
        assert breaker.failures == 5

        # Success should not reset failures when circuit is open
        breaker.record_success()
        assert breaker.failures == 5
        assert breaker.state == "open"


class TestIsTimeoutElapsed:
    """Test _is_timeout_elapsed edge case."""

    def test_none_last_failure_returns_false(self):
        """Returns False when _last_failure_monotonic is None."""
        breaker = CircuitBreaker()
        assert breaker._last_failure_monotonic is None
        assert breaker._is_timeout_elapsed(9999.0) is False


class TestGlobalCircuitBreaker:
    """Test get_circuit_breaker and reset_circuit_breaker."""

    def test_get_circuit_breaker_returns_instance(self):
        """get_circuit_breaker returns the global instance."""
        cb = get_circuit_breaker()
        assert isinstance(cb, CircuitBreaker)

    def test_reset_circuit_breaker_creates_new(self):
        """reset_circuit_breaker replaces the global instance."""
        cb_before = get_circuit_breaker()
        reset_circuit_breaker()
        cb_after = get_circuit_breaker()
        assert cb_before is not cb_after


class TestRetryWithBackoff:
    """Test retry_with_backoff decorator."""

    def test_successful_call_returns_result(self):
        """Decorated function returns result on success."""
        reset_circuit_breaker()

        @retry_with_backoff(max_retries=3, backoff_factor=0, jitter=False)
        def succeed():
            return "ok"

        assert succeed() == "ok"

    def test_retries_on_matching_exception_then_succeeds(self):
        """Decorated function retries on matching exception and succeeds on later attempt."""
        reset_circuit_breaker()
        call_count = 0

        @retry_with_backoff(max_retries=3, backoff_factor=0, jitter=False, exceptions=(ValueError,))
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient")
            return "recovered"

        result = flaky()
        assert result == "recovered"
        assert call_count == 3
