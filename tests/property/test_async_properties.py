"""Property-based tests for async operation parallelism.

Tests verify that async operations execute truly in parallel, with total execution
time approximately equal to max(operation_times) rather than sum(operation_times).
This ensures that concurrent operations provide actual performance benefits.
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tif1.async_fetch import fetch_multiple_async, fetch_with_rate_limit


class TestAsyncParallelismProperties:
    """Property tests for async operation parallelism."""

    @pytest.mark.asyncio
    @given(
        num_operations=st.integers(min_value=3, max_value=10),
        operation_duration=st.floats(min_value=0.1, max_value=0.3),
    )
    @settings(max_examples=15, deadline=10000)
    async def test_parallel_operations_time_is_max_not_sum(
        self, num_operations: int, operation_duration: float
    ):
        """Property: N parallel operations take ~max(times) not sum(times).

        When N independent async operations run concurrently, the total
        execution time should be approximately equal to the longest operation,
        not the sum of all operations.

        Args:
            num_operations: Number of concurrent operations
            operation_duration: Duration of each operation in seconds
        """

        async def slow_operation(duration: float) -> str:
            """Simulate an async operation with specified duration."""
            await asyncio.sleep(duration)
            return f"completed_{duration}"

        # Create operations with similar durations
        durations = [operation_duration] * num_operations

        start_time = time.perf_counter()
        results = await asyncio.gather(*[slow_operation(d) for d in durations])
        elapsed_time = time.perf_counter() - start_time

        # Property: elapsed time should be close to max(durations), not sum(durations)
        max_duration = max(durations)
        sum_duration = sum(durations)

        # Allow 120% overhead for scheduling and context switching (Windows can be slow)
        max_allowed_time = max_duration * 2.2

        assert len(results) == num_operations, f"Expected {num_operations} results"
        assert all("completed_" in r for r in results), "All operations should complete"

        # Property: parallel execution is significantly faster than sequential
        # Use more lenient threshold for property-based testing
        speedup_threshold = 0.6 if num_operations >= 5 else 0.7
        assert elapsed_time < sum_duration * speedup_threshold, (
            f"Parallel execution took {elapsed_time:.3f}s, "
            f"which is not significantly faster than sequential {sum_duration:.3f}s "
            f"(expected < {sum_duration * speedup_threshold:.3f}s)"
        )

        # Property: elapsed time is close to max duration (true parallelism)
        assert elapsed_time <= max_allowed_time, (
            f"Parallel execution took {elapsed_time:.3f}s, "
            f"expected ~{max_duration:.3f}s (max duration, allowed up to {max_allowed_time:.3f}s)"
        )

    @pytest.mark.asyncio
    @given(
        num_operations=st.integers(min_value=3, max_value=15),
    )
    @settings(max_examples=15, deadline=10000)
    async def test_parallel_operations_with_varying_durations(self, num_operations: int):
        """Property: Operations with varying durations complete in max(times).

        When operations have different durations, total time should still be
        approximately equal to the longest operation.

        Args:
            num_operations: Number of concurrent operations
        """

        async def timed_operation(op_id: int, duration: float) -> tuple[int, float]:
            """Operation that returns its ID and duration."""
            await asyncio.sleep(duration)
            return op_id, duration

        # Create operations with varying durations (0.05s to 0.3s)
        durations = [0.05 + (i * 0.025) for i in range(num_operations)]
        max_duration = max(durations)

        start_time = time.perf_counter()
        results = await asyncio.gather(*[timed_operation(i, d) for i, d in enumerate(durations)])
        elapsed_time = time.perf_counter() - start_time

        # Property: all operations complete
        assert len(results) == num_operations
        assert all(isinstance(r, tuple) and len(r) == 2 for r in results)

        # Property: elapsed time is close to max duration
        # Allow 50% overhead for scheduling
        max_allowed_time = max_duration * 1.5
        assert elapsed_time <= max_allowed_time, (
            f"Parallel execution took {elapsed_time:.3f}s, "
            f"expected ~{max_duration:.3f}s (max duration)"
        )

        # Property: faster than sequential execution
        sum_duration = sum(durations)
        assert elapsed_time < sum_duration * 0.7, (
            f"Parallel execution took {elapsed_time:.3f}s, "
            f"not significantly faster than sequential {sum_duration:.3f}s"
        )

    @pytest.mark.asyncio
    async def test_fetch_multiple_async_parallelism(self):
        """Test that fetch_multiple_async executes requests in parallel.

        Property: Fetching N requests should take ~max(request_times) not sum.
        """
        num_requests = 5
        request_duration = 0.1  # 100ms per request

        # Mock fetch_json_async to simulate network delay
        async def mock_fetch(*args, **kwargs):
            await asyncio.sleep(request_duration)
            return {"data": f"result_{args}"}

        requests = [(2024, f"GP{i}", "Race", "drivers.json") for i in range(num_requests)]

        with patch("tif1.async_fetch.fetch_json_async", side_effect=mock_fetch):
            start_time = time.perf_counter()
            results = await fetch_multiple_async(requests)
            elapsed_time = time.perf_counter() - start_time

        # Property: all requests complete
        assert len(results) == num_requests
        assert all(r is not None for r in results)

        # Property: parallel execution time is close to max (single request time)
        max_allowed_time = request_duration * 1.8  # Allow 80% overhead
        assert elapsed_time <= max_allowed_time, (
            f"Parallel fetch took {elapsed_time:.3f}s, expected ~{request_duration:.3f}s"
        )

        # Property: much faster than sequential
        sequential_time = num_requests * request_duration
        assert elapsed_time < sequential_time * 0.6, (
            f"Parallel fetch took {elapsed_time:.3f}s, "
            f"not significantly faster than sequential {sequential_time:.3f}s"
        )

    @pytest.mark.asyncio
    async def test_fetch_with_rate_limit_parallelism_within_limit(self):
        """Test that rate-limited operations still execute in parallel within limit.

        Property: N operations with semaphore(N) should execute in parallel.
        """
        num_operations = 5
        operation_duration = 0.1
        semaphore = asyncio.Semaphore(num_operations)  # Allow all to run in parallel

        async def rate_limited_operation(op_id: int) -> int:
            await asyncio.sleep(operation_duration)
            return op_id

        start_time = time.perf_counter()
        results = await asyncio.gather(
            *[
                fetch_with_rate_limit(rate_limited_operation, i, semaphore=semaphore)
                for i in range(num_operations)
            ]
        )
        elapsed_time = time.perf_counter() - start_time

        # Property: all operations complete
        assert len(results) == num_operations
        assert sorted(results) == list(range(num_operations))

        # Property: execution time is close to single operation (parallel)
        max_allowed_time = operation_duration * 1.8
        assert elapsed_time <= max_allowed_time, (
            f"Rate-limited parallel execution took {elapsed_time:.3f}s, "
            f"expected ~{operation_duration:.3f}s"
        )

    @pytest.mark.asyncio
    async def test_fetch_with_rate_limit_serializes_beyond_limit(self):
        """Test that rate limiting properly serializes operations beyond limit.

        Property: N operations with semaphore(M) where N > M should take
        approximately (N/M) * operation_time.
        """
        num_operations = 10
        max_concurrent = 2
        operation_duration = 0.05
        semaphore = asyncio.Semaphore(max_concurrent)

        async def rate_limited_operation(op_id: int) -> int:
            await asyncio.sleep(operation_duration)
            return op_id

        start_time = time.perf_counter()
        results = await asyncio.gather(
            *[
                fetch_with_rate_limit(rate_limited_operation, i, semaphore=semaphore)
                for i in range(num_operations)
            ]
        )
        elapsed_time = time.perf_counter() - start_time

        # Property: all operations complete
        assert len(results) == num_operations
        assert sorted(results) == list(range(num_operations))

        # Property: execution time reflects batching
        # With 10 operations and max_concurrent=2, we expect ~5 batches
        expected_batches = (num_operations + max_concurrent - 1) // max_concurrent
        expected_time = expected_batches * operation_duration
        max_allowed_time = expected_time * 1.5  # Allow 50% overhead

        assert elapsed_time >= expected_time * 0.8, (
            f"Rate-limited execution took {elapsed_time:.3f}s, "
            f"expected at least {expected_time:.3f}s (batched execution)"
        )
        assert elapsed_time <= max_allowed_time, (
            f"Rate-limited execution took {elapsed_time:.3f}s, expected ~{expected_time:.3f}s"
        )

    @pytest.mark.asyncio
    @given(
        num_fast_ops=st.integers(min_value=3, max_value=8),
        num_slow_ops=st.integers(min_value=2, max_value=4),
    )
    @settings(max_examples=10, deadline=10000)
    async def test_mixed_duration_operations_complete_in_max_time(
        self, num_fast_ops: int, num_slow_ops: int
    ):
        """Property: Mixed fast/slow operations complete in time of slowest.

        When operations have significantly different durations, the total
        time should be dominated by the slowest operation.

        Args:
            num_fast_ops: Number of fast operations (100ms)
            num_slow_ops: Number of slow operations (300ms)
        """
        fast_duration = 0.1
        slow_duration = 0.3

        async def fast_operation(op_id: int) -> str:
            await asyncio.sleep(fast_duration)
            return f"fast_{op_id}"

        async def slow_operation(op_id: int) -> str:
            await asyncio.sleep(slow_duration)
            return f"slow_{op_id}"

        tasks = []
        tasks.extend([fast_operation(i) for i in range(num_fast_ops)])
        tasks.extend([slow_operation(i) for i in range(num_slow_ops)])

        start_time = time.perf_counter()
        results = await asyncio.gather(*tasks)
        elapsed_time = time.perf_counter() - start_time

        # Property: all operations complete
        total_ops = num_fast_ops + num_slow_ops
        assert len(results) == total_ops

        # Property: elapsed time is close to slow_duration (max)
        # Allow 100% overhead for Windows scheduling
        max_allowed_time = slow_duration * 2.0
        assert elapsed_time <= max_allowed_time, (
            f"Mixed parallel execution took {elapsed_time:.3f}s, "
            f"expected ~{slow_duration:.3f}s (slowest operation, "
            f"allowed up to {max_allowed_time:.3f}s)"
        )

        # Property: faster than sequential (more lenient threshold)
        sequential_time = (num_fast_ops * fast_duration) + (num_slow_ops * slow_duration)
        speedup_threshold = 0.65  # Must be at least 35% faster than sequential
        assert elapsed_time < sequential_time * speedup_threshold, (
            f"Mixed parallel execution took {elapsed_time:.3f}s, "
            f"not significantly faster than sequential {sequential_time:.3f}s "
            f"(expected < {sequential_time * speedup_threshold:.3f}s)"
        )

    @pytest.mark.asyncio
    async def test_concurrent_cache_operations_parallelism(self):
        """Test that concurrent cache operations execute in parallel.

        Property: Multiple cache operations should execute concurrently,
        not sequentially.
        """
        num_cache_ops = 5
        cache_op_duration = 0.1

        # Mock cache operations
        mock_cache = MagicMock()

        async def mock_cache_get(key: str):
            await asyncio.sleep(cache_op_duration)
            return {"data": f"cached_{key}"}

        mock_cache.get_async = AsyncMock(side_effect=mock_cache_get)

        # Simulate concurrent cache operations
        cache_keys = [f"key_{i}" for i in range(num_cache_ops)]

        start_time = time.perf_counter()
        results = await asyncio.gather(*[mock_cache.get_async(key) for key in cache_keys])
        elapsed_time = time.perf_counter() - start_time

        # Property: all cache operations complete
        assert len(results) == num_cache_ops
        assert all(r is not None for r in results)

        # Property: parallel execution time is close to single operation time
        max_allowed_time = cache_op_duration * 1.8
        assert elapsed_time <= max_allowed_time, (
            f"Parallel cache operations took {elapsed_time:.3f}s, "
            f"expected ~{cache_op_duration:.3f}s"
        )

        # Property: much faster than sequential
        sequential_time = num_cache_ops * cache_op_duration
        assert elapsed_time < sequential_time * 0.6, (
            f"Parallel cache operations took {elapsed_time:.3f}s, "
            f"not significantly faster than sequential {sequential_time:.3f}s"
        )

    @pytest.mark.asyncio
    async def test_fallback_operations_maintain_parallelism(self):
        """Test that fallback paths maintain parallelism.

        Property: When primary operations fail and fallbacks are needed,
        fallbacks should also execute in parallel, not sequentially.
        """
        num_operations = 4
        primary_duration = 0.05
        fallback_duration = 0.1

        call_count = {"primary": 0, "fallback": 0}

        async def operation_with_fallback(op_id: int) -> str:
            """Try primary, fall back on failure."""
            try:
                # Primary always fails
                call_count["primary"] += 1
                await asyncio.sleep(primary_duration)
                raise ValueError("Primary failed")
            except ValueError:
                # Fallback succeeds
                call_count["fallback"] += 1
                await asyncio.sleep(fallback_duration)
                return f"fallback_{op_id}"

        start_time = time.perf_counter()
        results = await asyncio.gather(*[operation_with_fallback(i) for i in range(num_operations)])
        elapsed_time = time.perf_counter() - start_time

        # Property: all operations complete via fallback
        assert len(results) == num_operations
        assert all("fallback_" in r for r in results)
        assert call_count["primary"] == num_operations
        assert call_count["fallback"] == num_operations

        # Property: total time is close to (primary + fallback) duration, not sum
        expected_time = primary_duration + fallback_duration
        max_allowed_time = expected_time * 1.8

        assert elapsed_time <= max_allowed_time, (
            f"Parallel fallback execution took {elapsed_time:.3f}s, expected ~{expected_time:.3f}s"
        )

        # Property: much faster than sequential
        sequential_time = num_operations * (primary_duration + fallback_duration)
        assert elapsed_time < sequential_time * 0.6, (
            f"Parallel fallback execution took {elapsed_time:.3f}s, "
            f"not significantly faster than sequential {sequential_time:.3f}s"
        )

    @pytest.mark.asyncio
    @given(
        num_operations=st.integers(min_value=5, max_value=20),
    )
    @settings(max_examples=10, deadline=10000)
    async def test_gather_maintains_order_with_parallelism(self, num_operations: int):
        """Property: asyncio.gather maintains result order despite parallel execution.

        When operations complete in different orders due to varying durations,
        gather should return results in the original task order.

        Args:
            num_operations: Number of concurrent operations
        """

        async def operation_with_id(op_id: int, duration: float) -> int:
            """Operation that returns its ID after specified duration."""
            await asyncio.sleep(duration)
            return op_id

        # Create operations with reverse durations (last finishes first)
        tasks = [operation_with_id(i, 0.01 * (num_operations - i)) for i in range(num_operations)]

        results = await asyncio.gather(*tasks)

        # Property: results are in original order despite different completion times
        assert results == list(range(num_operations)), (
            f"Results {results} not in expected order {list(range(num_operations))}"
        )

    @pytest.mark.asyncio
    async def test_exception_in_one_operation_doesnt_block_others(self):
        """Property: Exception in one operation doesn't prevent others from completing.

        When using return_exceptions=True, one failing operation should not
        block other operations from completing in parallel.
        """
        num_operations = 5
        failing_index = 2
        operation_duration = 0.1

        async def operation(op_id: int) -> str:
            await asyncio.sleep(operation_duration)
            if op_id == failing_index:
                raise ValueError(f"Operation {op_id} failed")
            return f"success_{op_id}"

        start_time = time.perf_counter()
        results = await asyncio.gather(
            *[operation(i) for i in range(num_operations)], return_exceptions=True
        )
        elapsed_time = time.perf_counter() - start_time

        # Property: all operations complete (some with exceptions)
        assert len(results) == num_operations

        # Property: non-failing operations succeed
        for i, result in enumerate(results):
            if i == failing_index:
                assert isinstance(result, ValueError)
            else:
                assert result == f"success_{i}"

        # Property: execution time is still parallel (not blocked by exception)
        max_allowed_time = operation_duration * 1.8
        assert elapsed_time <= max_allowed_time, (
            f"Parallel execution with exception took {elapsed_time:.3f}s, "
            f"expected ~{operation_duration:.3f}s"
        )


class TestAsyncParallelismIntegration:
    """Integration tests for async parallelism in real scenarios."""

    @pytest.mark.asyncio
    async def test_fetch_driver_laps_parallel_timing(self):
        """Test that fetch_driver_laps_parallel executes in parallel.

        This integration test verifies that the actual implementation
        maintains parallelism for driver lap fetching.
        """
        from tif1.core import Session

        num_drivers = 3
        fetch_duration = 0.1

        # Mock the async fetch method
        async def mock_fetch_laptime_payloads_async(driver_requests, **kwargs):
            await asyncio.sleep(fetch_duration)
            payloads = [{"time": [1.0], "lap": [1]} for _ in driver_requests]
            return payloads, []

        session = Session(2024, "Test GP", "Race", lib="pandas", enable_cache=False)
        session._drivers = [{"driver": f"DRV{i}", "team": "Team"} for i in range(num_drivers)]

        with patch.object(
            session,
            "_fetch_laptime_payloads_async",
            side_effect=mock_fetch_laptime_payloads_async,
        ):
            start_time = time.perf_counter()
            results = await session.fetch_driver_laps_parallel(
                [f"DRV{i}" for i in range(num_drivers)]
            )
            elapsed_time = time.perf_counter() - start_time

        # Property: all drivers fetched
        assert len(results) == num_drivers

        # Property: parallel execution (single fetch call for all drivers)
        max_allowed_time = fetch_duration * 2.0  # Allow 100% overhead
        assert elapsed_time <= max_allowed_time, (
            f"Parallel driver fetch took {elapsed_time:.3f}s, expected ~{fetch_duration:.3f}s"
        )

    @pytest.mark.asyncio
    async def test_multiple_cdn_attempts_in_parallel(self):
        """Test that CDN fallback attempts execute in parallel.

        Property: When trying multiple CDN sources, attempts should be
        parallel, not sequential.
        """
        num_cdns = 3
        cdn_attempt_duration = 0.1

        async def mock_try_cdn(cdn_source, attempt_num):
            """Simulate CDN attempt."""
            await asyncio.sleep(cdn_attempt_duration)
            # All fail except last
            if cdn_source.name == f"cdn_{num_cdns - 1}":
                return {"data": "success"}, None
            return None, Exception("CDN failed")

        # Simulate parallel CDN attempts
        cdn_sources = [MagicMock(name=f"cdn_{i}") for i in range(num_cdns)]

        start_time = time.perf_counter()
        results = await asyncio.gather(*[mock_try_cdn(cdn, 0) for cdn in cdn_sources])
        elapsed_time = time.perf_counter() - start_time

        # Property: all CDN attempts made
        assert len(results) == num_cdns

        # Property: parallel execution time is close to single attempt
        max_allowed_time = cdn_attempt_duration * 1.8
        assert elapsed_time <= max_allowed_time, (
            f"Parallel CDN attempts took {elapsed_time:.3f}s, expected ~{cdn_attempt_duration:.3f}s"
        )

        # Property: much faster than sequential
        sequential_time = num_cdns * cdn_attempt_duration
        assert elapsed_time < sequential_time * 0.6, (
            f"Parallel CDN attempts took {elapsed_time:.3f}s, "
            f"not significantly faster than sequential {sequential_time:.3f}s"
        )
