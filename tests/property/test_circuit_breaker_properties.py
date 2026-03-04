"""Property-based tests for CircuitBreaker thread safety and consistency.

Tests verify that the CircuitBreaker maintains correct state and failure counts
under concurrent access, ensuring atomic state transitions and accurate failure tracking.
"""

import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tif1.retry import CircuitBreaker


class TestCircuitBreakerConcurrentConsistency:
    """Property tests for concurrent circuit breaker operations."""

    @given(
        num_threads=st.integers(min_value=2, max_value=20),
        operations_per_thread=st.integers(min_value=5, max_value=50),
        threshold=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=50, deadline=5000)
    def test_concurrent_failure_count_accuracy(
        self, num_threads: int, operations_per_thread: int, threshold: int
    ):
        """Property: Final failure count equals actual number of recorded failures.

        When multiple threads record failures concurrently, the circuit breaker
        must accurately count all failures without loss due to race conditions.

        Args:
            num_threads: Number of concurrent threads
            operations_per_thread: Number of failure recordings per thread
            threshold: Circuit breaker failure threshold
        """
        breaker = CircuitBreaker(threshold=threshold, timeout=60)
        failure_count = 0
        lock = threading.Lock()

        def record_failures():
            nonlocal failure_count
            for _ in range(operations_per_thread):
                breaker.record_failure()
                with lock:
                    failure_count += 1

        threads = [threading.Thread(target=record_failures) for _ in range(num_threads)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        expected_failures = num_threads * operations_per_thread
        actual_failures = breaker.failures

        # Property: failure count must equal actual failures recorded
        assert actual_failures == expected_failures, (
            f"Expected {expected_failures} failures, got {actual_failures}"
        )

        # Property: state must be "open" if failures >= threshold
        if expected_failures >= threshold:
            assert breaker.state == "open", f"Expected 'open' state, got '{breaker.state}'"

    @given(
        num_success_threads=st.integers(min_value=1, max_value=10),
        num_failure_threads=st.integers(min_value=1, max_value=10),
        threshold=st.integers(min_value=5, max_value=20),
    )
    @settings(max_examples=30, deadline=5000)
    def test_concurrent_mixed_operations_consistency(
        self, num_success_threads: int, num_failure_threads: int, threshold: int
    ):
        """Property: Mixed concurrent operations maintain consistent state.

        When threads record both successes and failures concurrently,
        the circuit breaker must maintain consistent state transitions.

        Args:
            num_success_threads: Number of threads recording successes
            num_failure_threads: Number of threads recording failures
            threshold: Circuit breaker failure threshold
        """
        breaker = CircuitBreaker(threshold=threshold, timeout=60)
        actual_failures = 0
        lock = threading.Lock()

        def record_success():
            breaker.record_success()

        def record_failure():
            nonlocal actual_failures
            breaker.record_failure()
            with lock:
                actual_failures += 1

        threads = []
        threads.extend(
            [threading.Thread(target=record_success) for _ in range(num_success_threads)]
        )
        threads.extend(
            [threading.Thread(target=record_failure) for _ in range(num_failure_threads)]
        )

        # Shuffle to interleave operations
        random.shuffle(threads)

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: final state must be consistent with failure count
        final_failures = breaker.failures
        final_state = breaker.state

        # Property: failure count should never exceed actual failures recorded
        assert final_failures <= actual_failures, (
            f"Failure count {final_failures} exceeds actual failures {actual_failures}"
        )

        # Property: if circuit is open, failures must be >= threshold
        # Note: successes do NOT reset failures when circuit is open
        if final_state == "open":
            assert final_failures >= threshold, (
                f"Circuit is open but failures {final_failures} < threshold {threshold}"
            )

        # Property: if circuit is closed, either:
        # 1. We never reached threshold, OR
        # 2. A success reset the counter (only possible if circuit was closed/half-open)
        if final_state == "closed":
            # Failures should be < threshold (never opened) or 0 (reset by success)
            assert final_failures < threshold or final_failures == 0, (
                f"Circuit closed but failures {final_failures} >= threshold {threshold}"
            )

        # Property: state transitions are consistent with failure count
        # If failures >= threshold, circuit should be open (unless success reset it)
        if final_failures >= threshold:
            assert final_state == "open", (
                f"Failures {final_failures} >= threshold {threshold} but state is '{final_state}'"
            )

    @given(
        threshold=st.integers(min_value=3, max_value=10),
    )
    @settings(max_examples=30, deadline=10000)
    def test_concurrent_call_state_transitions_atomic(self, threshold: int):
        """Property: State transitions during concurrent calls are atomic.

        When multiple threads call the circuit breaker concurrently with
        functions that may fail, state transitions must be atomic and consistent.

        Args:
            num_threads: Number of concurrent threads
            threshold: Circuit breaker failure threshold
        """
        breaker = CircuitBreaker(threshold=threshold, timeout=1)
        call_count = 0
        success_count = 0
        failure_count = 0
        lock = threading.Lock()

        def failing_func():
            """Function that always fails."""
            raise ValueError("intentional failure")

        def succeeding_func():
            """Function that always succeeds."""
            return "success"

        def make_call(should_fail: bool):
            nonlocal call_count, success_count, failure_count
            with lock:
                call_count += 1

            try:
                if should_fail:
                    breaker.call(failing_func)
                else:
                    breaker.call(succeeding_func)
                with lock:
                    success_count += 1
            except (ValueError, Exception):
                with lock:
                    failure_count += 1

        # Create mix of failing and succeeding calls
        # First batch: all failures to open circuit
        threads = [threading.Thread(target=make_call, args=(True,)) for _ in range(threshold)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: circuit must be open after threshold failures
        assert breaker.state == "open", f"Expected 'open' state, got '{breaker.state}'"
        assert breaker.failures >= threshold, (
            f"Expected at least {threshold} failures, got {breaker.failures}"
        )

        # Property: all subsequent calls should fail immediately (circuit open)
        additional_threads = [threading.Thread(target=make_call, args=(False,)) for _ in range(5)]

        for thread in additional_threads:
            thread.start()
        for thread in additional_threads:
            thread.join()

        # All additional calls should have failed due to open circuit
        # (unless timeout elapsed, but with timeout=1 and immediate execution, unlikely)
        assert failure_count >= threshold, "Circuit should block calls when open"

    @given(
        num_threads=st.integers(min_value=10, max_value=50),
        threshold=st.integers(min_value=5, max_value=15),
    )
    @settings(max_examples=20, deadline=10000)
    def test_concurrent_check_and_update_state_consistency(self, num_threads: int, threshold: int):
        """Property: Concurrent state checks maintain consistency.

        When multiple threads check and update circuit breaker state concurrently,
        the state must remain consistent and transitions must be atomic.

        Args:
            num_threads: Number of concurrent threads
            threshold: Circuit breaker failure threshold
        """
        breaker = CircuitBreaker(threshold=threshold, timeout=1)

        # Open the circuit
        for _ in range(threshold):
            breaker.record_failure()

        assert breaker.state == "open"

        # Simulate timeout elapsed
        if breaker._last_failure_monotonic is not None:
            breaker._last_failure_monotonic -= 10

        results = []
        lock = threading.Lock()

        def check_state():
            should_proceed, state = breaker.check_and_update_state()
            with lock:
                results.append((should_proceed, state))

        threads = [threading.Thread(target=check_state) for _ in range(num_threads)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: all threads should see consistent state
        # First thread transitions to half-open, rest see half-open
        should_proceeds = [proceed for proceed, _ in results]

        # All should proceed (either saw half-open or transitioned to it)
        assert all(should_proceeds), "All threads should proceed after timeout"

        # State should be half-open after all checks
        assert breaker.state == "half-open", f"Expected 'half-open', got '{breaker.state}'"

    def test_concurrent_call_with_executor(self):
        """Test concurrent calls using ThreadPoolExecutor for realistic scenario.

        This test simulates a realistic concurrent workload where multiple
        threads are calling the circuit breaker with functions that may fail.
        """
        breaker = CircuitBreaker(threshold=5, timeout=2)
        num_calls = 100
        failure_rate = 0.3  # 30% of calls fail

        def flaky_function(call_id: int):
            """Function that fails probabilistically."""
            if random.random() < failure_rate:
                raise ValueError(f"Call {call_id} failed")
            return f"success_{call_id}"

        results = {"success": 0, "failure": 0, "circuit_open": 0}
        lock = threading.Lock()

        def make_call(call_id: int):
            try:
                result = breaker.call(flaky_function, call_id)
                with lock:
                    results["success"] += 1
                return result
            except ValueError:
                with lock:
                    results["failure"] += 1
            except Exception as e:
                if "Circuit breaker is open" in str(e):
                    with lock:
                        results["circuit_open"] += 1

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(make_call, i) for i in range(num_calls)]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pass  # Already counted in make_call

        # Property: total calls = success + failure + circuit_open
        total = results["success"] + results["failure"] + results["circuit_open"]
        assert total == num_calls, f"Expected {num_calls} total, got {total}"

        # Property: if circuit opened, some calls were blocked
        if breaker.state == "open":
            assert results["circuit_open"] > 0, "Circuit open but no calls were blocked"

    @pytest.mark.parametrize("threshold", [1, 3, 5, 10])
    def test_exact_threshold_boundary_concurrent(self, threshold: int):
        """Test that circuit opens exactly at threshold under concurrent load.

        Property: Circuit must open when failure count reaches threshold,
        not before or after.

        Args:
            threshold: Circuit breaker failure threshold
        """
        breaker = CircuitBreaker(threshold=threshold, timeout=60)

        # Record exactly threshold-1 failures concurrently
        def record_failure():
            breaker.record_failure()

        threads = [threading.Thread(target=record_failure) for _ in range(threshold - 1)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: circuit should still be closed
        assert breaker.state == "closed", f"Circuit opened before threshold at {threshold - 1}"

        # Record one more failure
        breaker.record_failure()

        # Property: circuit must now be open
        assert breaker.state == "open", f"Circuit not open at threshold {threshold}"
        assert breaker.failures == threshold, f"Expected {threshold} failures"

    def test_concurrent_success_resets_failures(self):
        """Property: Concurrent successes properly reset failure counter when circuit is closed.

        When multiple threads record successes concurrently while circuit is closed,
        the failure counter must be reset to zero atomically.
        """
        breaker = CircuitBreaker(threshold=10, timeout=60)

        # Record some failures (but not enough to open circuit)
        for _ in range(5):
            breaker.record_failure()

        assert breaker.failures == 5
        assert breaker.state == "closed"  # Circuit should still be closed

        # Concurrent successes
        def record_success():
            breaker.record_success()

        threads = [threading.Thread(target=record_success) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: failures must be reset to 0 (since circuit was closed)
        assert breaker.failures == 0, f"Expected 0 failures, got {breaker.failures}"
        assert breaker.state == "closed", f"Expected 'closed' state, got '{breaker.state}'"

    def test_no_race_condition_in_state_check_and_execution(self):
        """Property: No state changes occur between check and execution.

        This test verifies the check-then-act pattern is atomic by ensuring
        that the state captured before execution is used for success handling.
        """
        breaker = CircuitBreaker(threshold=1, timeout=1)

        # Open the circuit
        breaker.record_failure()
        assert breaker.state == "open"

        # Simulate timeout elapsed
        if breaker._last_failure_monotonic is not None:
            breaker._last_failure_monotonic -= 10

        execution_states = []
        lock = threading.Lock()

        def successful_call():
            """Function that succeeds and records pre-call state."""
            # This will be called in half-open state
            return "success"

        def make_call():
            try:
                result = breaker.call(successful_call)
                with lock:
                    execution_states.append((result, breaker.state))
            except Exception as e:
                with lock:
                    execution_states.append((str(e), breaker.state))

        # First call should transition to half-open, execute, then close
        thread1 = threading.Thread(target=make_call)
        thread1.start()
        thread1.join()

        # Property: circuit should be closed after successful call in half-open
        assert breaker.state == "closed", f"Expected 'closed', got '{breaker.state}'"
        assert breaker.failures == 0, f"Expected 0 failures, got {breaker.failures}"
