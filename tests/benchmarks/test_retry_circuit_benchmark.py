"""Benchmarks for CircuitBreaker state-check performance."""

import threading
import time
from datetime import datetime, timedelta

import pytest


class _LegacyCircuitBreaker:
    def __init__(self, threshold: int = 5, timeout: int = 60):
        self.threshold = threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure_time: datetime | None = None
        self.state = "closed"
        self._lock = threading.Lock()

    def check_and_update_state(self) -> tuple[bool, str]:
        with self._lock:
            if self.state == "open":
                if self.last_failure_time and datetime.now() - self.last_failure_time > timedelta(
                    seconds=self.timeout
                ):
                    self.state = "half-open"
                    return True, "half-open"
                return False, "open"
            return True, self.state

    def record_failure(self) -> None:
        with self._lock:
            self.failures += 1
            self.last_failure_time = datetime.now()
            if self.failures >= self.threshold:
                self.state = "open"


class _CandidateCircuitBreaker:
    def __init__(self, threshold: int = 5, timeout: int = 60):
        self.threshold = threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure_time: datetime | None = None
        self._last_failure_monotonic: float | None = None
        self.state = "closed"
        self._lock = threading.Lock()

    def _is_timeout_elapsed(self, now_monotonic: float) -> bool:
        if self._last_failure_monotonic is None:
            return False
        return (now_monotonic - self._last_failure_monotonic) > self.timeout

    def check_and_update_state(self) -> tuple[bool, str]:
        with self._lock:
            if self.state == "open":
                if self._is_timeout_elapsed(time.monotonic()):
                    self.state = "half-open"
                    return True, "half-open"
                return False, "open"
            return True, self.state

    def record_failure(self) -> None:
        now_dt = datetime.now()
        now_mono = time.monotonic()
        with self._lock:
            self.failures += 1
            self.last_failure_time = now_dt
            self._last_failure_monotonic = now_mono
            if self.failures >= self.threshold:
                self.state = "open"


def _run_check_loop(breaker, iterations: int = 50_000) -> int:
    proceed_count = 0
    for _ in range(iterations):
        should_proceed, _ = breaker.check_and_update_state()
        if should_proceed:
            proceed_count += 1
    return proceed_count


def _run_record_failure_loop(breaker, iterations: int = 30_000) -> tuple[int, str]:
    for _ in range(iterations):
        breaker.record_failure()
    return breaker.failures, breaker.state


def test_candidate_parity_open_not_elapsed():
    legacy = _LegacyCircuitBreaker(timeout=60)
    candidate = _CandidateCircuitBreaker(timeout=60)

    legacy.state = "open"
    legacy.last_failure_time = datetime.now()
    candidate.state = "open"
    candidate.last_failure_time = legacy.last_failure_time
    candidate._last_failure_monotonic = time.monotonic()

    assert legacy.check_and_update_state() == candidate.check_and_update_state()


def test_candidate_parity_open_elapsed():
    legacy = _LegacyCircuitBreaker(timeout=1)
    candidate = _CandidateCircuitBreaker(timeout=1)

    legacy.state = "open"
    legacy.last_failure_time = datetime.now() - timedelta(seconds=5)
    candidate.state = "open"
    candidate.last_failure_time = legacy.last_failure_time
    candidate._last_failure_monotonic = time.monotonic() - 5

    assert legacy.check_and_update_state() == candidate.check_and_update_state()


def test_candidate_parity_threshold_open():
    legacy = _LegacyCircuitBreaker(threshold=3)
    candidate = _CandidateCircuitBreaker(threshold=3)

    for _ in range(3):
        legacy.record_failure()
        candidate.record_failure()

    assert legacy.state == candidate.state == "open"
    assert legacy.failures == candidate.failures == 3


@pytest.mark.benchmark(group="retry_circuit")
class TestRetryCircuitBenchmark:
    def test_legacy_check_open(self, benchmark):
        breaker = _LegacyCircuitBreaker(timeout=60)
        breaker.state = "open"
        breaker.last_failure_time = datetime.now()

        proceed_count = benchmark(_run_check_loop, breaker)
        assert proceed_count == 0

    def test_candidate_check_open(self, benchmark):
        breaker = _CandidateCircuitBreaker(timeout=60)
        breaker.state = "open"
        breaker.last_failure_time = datetime.now()
        breaker._last_failure_monotonic = time.monotonic()

        proceed_count = benchmark(_run_check_loop, breaker)
        assert proceed_count == 0

    def test_legacy_record_failure(self, benchmark):
        breaker = _LegacyCircuitBreaker(threshold=100_000_000)
        failures, state = benchmark(_run_record_failure_loop, breaker)
        assert failures >= 30_000
        assert failures % 30_000 == 0
        assert state == "closed"

    def test_candidate_record_failure(self, benchmark):
        breaker = _CandidateCircuitBreaker(threshold=100_000_000)
        failures, state = benchmark(_run_record_failure_loop, breaker)
        assert failures >= 30_000
        assert failures % 30_000 == 0
        assert state == "closed"
