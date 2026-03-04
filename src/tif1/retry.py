"""Retry strategies with circuit breaker pattern."""

import logging
import random
import threading
import time
from collections.abc import Callable
from datetime import datetime
from functools import wraps
from typing import TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitBreaker:
    """Circuit breaker to prevent cascading failures (thread-safe with atomic operations)."""

    def __init__(self, threshold: int = 5, timeout: int = 60):
        self.threshold = threshold
        self.timeout = timeout
        self._failures = 0
        self.last_failure_time: datetime | None = None
        self._last_failure_monotonic: float | None = None
        self._state = "closed"  # closed, open, half-open
        self._lock = threading.RLock()  # Reentrant lock for nested calls

    def _is_timeout_elapsed(self, now_monotonic: float | None = None) -> bool:
        """Check whether open timeout has elapsed using monotonic time.

        Must be called with lock held.

        Args:
            now_monotonic: Optional monotonic time value. If None, uses current time.
        """
        if self._last_failure_monotonic is None:
            return False
        if now_monotonic is None:
            now_monotonic = time.monotonic()
        return (now_monotonic - self._last_failure_monotonic) > self.timeout

    @property
    def failures(self) -> int:
        """Get current failure count (thread-safe)."""
        with self._lock:
            return self._failures

    @failures.setter
    def failures(self, value: int) -> None:
        """Set failure count (for testing purposes)."""
        with self._lock:
            self._failures = value

    @property
    def state(self) -> str:
        """Get current state (thread-safe)."""
        with self._lock:
            return self._state

    def check_and_update_state(self) -> tuple[bool, str]:
        """Check circuit breaker state and update if needed (thread-safe).

        Returns:
            Tuple of (should_proceed, state)
        """
        with self._lock:
            if self._state == "open":
                if self._is_timeout_elapsed():
                    self._state = "half-open"
                    logger.info("Circuit breaker entering half-open state")
                    return True, "half-open"
                return False, "open"
            return True, self._state

    def record_success(self) -> None:
        """Record successful request with atomic state transition (thread-safe)."""
        with self._lock:
            # Only reset failures if circuit is not open
            # Open circuit can only transition via timeout -> half-open -> success -> closed
            if self._state != "open":
                self._failures = 0
            if self._state == "half-open":
                self._state = "closed"
                logger.info("Circuit breaker closed")

    def record_failure(self) -> None:
        """Record failed request with atomic counter increment (thread-safe).

        Atomically increments failure counter and transitions to open state
        if threshold is reached. All operations protected by lock.
        """
        now_dt = datetime.now()
        now_mono = time.monotonic()
        with self._lock:
            # Atomic increment - all reads and writes protected by same lock
            self._failures += 1
            self.last_failure_time = now_dt
            self._last_failure_monotonic = now_mono

            # Atomic transition: * -> open (if threshold reached)
            if self._failures >= self.threshold:
                self._state = "open"
                logger.warning(f"Circuit breaker opened after {self._failures} failures")

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute function with circuit breaker protection using atomic state transitions.

        Uses compare-and-swap pattern for state changes and monotonic time for timeouts.
        Executes function outside lock to prevent deadlocks during I/O operations.

        Args:
            func: Function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Result from func

        Raises:
            Exception: If circuit breaker is open or func raises
        """
        # Atomic state check and update
        with self._lock:
            if self._state == "open":
                if self._is_timeout_elapsed():
                    # Atomic transition: open -> half-open
                    self._state = "half-open"
                    logger.info("Circuit breaker entering half-open state")
                else:
                    raise Exception("Circuit breaker is open")

            # Capture pre-call state for correct success handling
            pre_call_state = self._state

        # Execute function outside lock to avoid holding during I/O
        try:
            result = func(*args, **kwargs)
            self._record_success(pre_call_state)
            return result
        except Exception:
            self._record_failure()
            raise

    def _record_success(self, pre_call_state: str) -> None:
        """Record success with atomic state transition.

        Args:
            pre_call_state: State captured before function execution
        """
        with self._lock:
            self._failures = 0
            # Atomic transition: half-open -> closed (only if still half-open)
            if pre_call_state == "half-open":
                self._state = "closed"
                logger.info("Circuit breaker closed")

    def _record_failure(self) -> None:
        """Record failure with atomic counter increment and state transition."""
        now_dt = datetime.now()
        now_mono = time.monotonic()
        with self._lock:
            # Atomic increment
            self._failures += 1
            self.last_failure_time = now_dt
            self._last_failure_monotonic = now_mono

            # Atomic transition: * -> open (if threshold reached)
            if self._failures >= self.threshold:
                self._state = "open"
                logger.warning(f"Circuit breaker opened after {self._failures} failures")


def _create_circuit_breaker() -> CircuitBreaker:
    """Create circuit breaker from configuration."""
    try:
        from .config import get_config

        config = get_config()
        threshold = int(config.get("circuit_breaker_threshold", 5))
        timeout = int(config.get("circuit_breaker_timeout", 60))
        return CircuitBreaker(threshold=max(1, threshold), timeout=max(1, timeout))
    except Exception as e:
        logger.warning(f"Falling back to default circuit breaker config: {e}")
        return CircuitBreaker()


_circuit_breaker = _create_circuit_breaker()


def retry_with_backoff(
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    exceptions: tuple = (Exception,),
):
    """Decorator for retry with exponential backoff and jitter."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception: BaseException | None = None

            for attempt in range(max_retries):
                try:
                    return _circuit_breaker.call(func, *args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_retries - 1:
                        break

                    # Calculate backoff with jitter
                    backoff = backoff_factor**attempt
                    if jitter:
                        backoff *= 0.5 + random.random()

                    logger.warning(f"Retry {attempt + 1}/{max_retries} after {backoff:.2f}s: {e}")
                    time.sleep(backoff)

            if last_exception is not None:
                raise last_exception
            raise RuntimeError("Retry failed with no exception captured")

        return wrapper

    return decorator


def get_circuit_breaker() -> CircuitBreaker:
    """Get global circuit breaker instance."""
    return _circuit_breaker


def reset_circuit_breaker():
    """Reset circuit breaker state."""
    global _circuit_breaker
    _circuit_breaker = _create_circuit_breaker()
