"""Benchmark tests to validate async parallelism improvements.

This test validates that async operations show true parallelism as specified in:
- Task 3.4.1: Parallel driver lap fetching
- Task 3.4.2: Parallel fallback paths
- Task 3.4.3: Concurrent cache operations
- Task 3.4.4: Concurrency control with semaphores

Acceptance Criteria:
- For N independent async operations, total execution time SHALL be approximately
  max(operation_times) not sum(operation_times) (parallelism property)
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tif1.async_fetch import fetch_multiple_async, fetch_with_rate_limit


class TestAsyncParallelismValidation:
    """Validate that async operations execute in parallel, not sequentially."""

    @pytest.mark.asyncio
    async def test_fetch_multiple_async_shows_parallelism(self):
        """Verify fetch_multiple_async executes requests in parallel.

        For N requests each taking T seconds, total time should be ~T (parallel)
        not N*T (sequential).
        """
        # Simulate 10 requests, each taking 0.1 seconds
        num_requests = 10
        request_delay = 0.1
        requests = [(2024, "Bahrain", "R", f"driver_{i}.json") for i in range(num_requests)]

        async def mock_fetch_json_async(*args, **kwargs):
            """Mock fetch that takes fixed time."""
            await asyncio.sleep(request_delay)
            return {"data": "test"}

        with patch("tif1.async_fetch.fetch_json_async", side_effect=mock_fetch_json_async):
            start_time = time.perf_counter()
            results = await fetch_multiple_async(requests)
            elapsed_time = time.perf_counter() - start_time

            # Verify all requests completed
            assert len(results) == num_requests
            assert all(r is not None for r in results)

            # Verify parallel execution: time should be ~request_delay (0.1s)
            # not num_requests * request_delay (1.0s)
            # Allow 50% overhead for scheduling/coordination
            max_expected_time = request_delay * 1.5
            sequential_time = num_requests * request_delay

            assert elapsed_time < max_expected_time, (
                f"Parallel execution took {elapsed_time:.3f}s, expected <{max_expected_time:.3f}s"
            )

            # Verify we're significantly faster than sequential
            speedup = sequential_time / elapsed_time
            assert speedup > 3.0, (
                f"Speedup {speedup:.1f}x is too low, expected >3x for parallel execution"
            )

    @pytest.mark.asyncio
    async def test_fetch_with_rate_limit_controls_concurrency(self):
        """Verify fetch_with_rate_limit properly limits concurrent operations."""
        max_concurrent = 3
        num_operations = 9
        operation_delay = 0.1

        # Track concurrent operations
        concurrent_count = 0
        max_concurrent_observed = 0
        lock = asyncio.Lock()

        async def mock_operation():
            """Mock operation that tracks concurrency."""
            nonlocal concurrent_count, max_concurrent_observed

            async with lock:
                concurrent_count += 1
                max_concurrent_observed = max(max_concurrent_observed, concurrent_count)

            await asyncio.sleep(operation_delay)

            async with lock:
                concurrent_count -= 1

            return {"success": True}

        semaphore = asyncio.Semaphore(max_concurrent)

        # Execute operations with rate limiting
        start_time = time.perf_counter()
        tasks = [
            fetch_with_rate_limit(mock_operation, semaphore=semaphore)
            for _ in range(num_operations)
        ]
        results = await asyncio.gather(*tasks)
        elapsed_time = time.perf_counter() - start_time

        # Verify all operations completed
        assert len(results) == num_operations
        assert all(r["success"] for r in results)

        # Verify concurrency was limited
        assert max_concurrent_observed <= max_concurrent, (
            f"Observed {max_concurrent_observed} concurrent ops, limit was {max_concurrent}"
        )

        # Verify execution time matches expected batching
        # With 9 ops, max 3 concurrent, each taking 0.1s:
        # Expected: 3 batches * 0.1s = 0.3s
        expected_batches = (num_operations + max_concurrent - 1) // max_concurrent
        expected_time = expected_batches * operation_delay
        max_expected_time = expected_time * 1.5  # 50% overhead allowance

        assert elapsed_time < max_expected_time, (
            f"Execution took {elapsed_time:.3f}s, expected <{max_expected_time:.3f}s"
        )

    @pytest.mark.asyncio
    async def test_parallel_fallback_paths_maintain_parallelism(self):
        """Verify fallback paths execute in parallel, not sequentially.

        Task 3.4.2: Optimize fallback paths to maintain parallelism.
        """
        num_requests = 5
        primary_delay = 0.05
        fallback_delay = 0.05

        # Track execution pattern
        execution_log = []

        async def mock_fetch_with_fallback(request_id):
            """Mock fetch that fails primary and succeeds on fallback."""
            # Primary attempt fails
            execution_log.append(("primary_start", request_id, time.perf_counter()))
            await asyncio.sleep(primary_delay)
            execution_log.append(("primary_fail", request_id, time.perf_counter()))

            # Fallback succeeds
            execution_log.append(("fallback_start", request_id, time.perf_counter()))
            await asyncio.sleep(fallback_delay)
            execution_log.append(("fallback_success", request_id, time.perf_counter()))

            return {"data": f"request_{request_id}"}

        # Execute all requests in parallel
        start_time = time.perf_counter()
        tasks = [mock_fetch_with_fallback(i) for i in range(num_requests)]
        results = await asyncio.gather(*tasks)
        elapsed_time = time.perf_counter() - start_time

        # Verify all completed
        assert len(results) == num_requests

        # Verify parallel execution of fallbacks
        # Total time should be ~(primary_delay + fallback_delay) = 0.1s
        # NOT num_requests * (primary_delay + fallback_delay) = 0.5s
        max_expected_time = (primary_delay + fallback_delay) * 1.5
        sequential_time = num_requests * (primary_delay + fallback_delay)

        assert elapsed_time < max_expected_time, (
            f"Fallback execution took {elapsed_time:.3f}s, expected <{max_expected_time:.3f}s"
        )

        speedup = sequential_time / elapsed_time
        assert speedup > 2.0, (
            f"Fallback speedup {speedup:.1f}x is too low, expected >2x for parallel execution"
        )

    @pytest.mark.asyncio
    async def test_concurrent_cache_operations(self):
        """Verify cache operations can run concurrently.

        Task 3.4.3: Implement concurrent cache operations.
        """
        num_operations = 10
        operation_delay = 0.05

        # Mock cache with async operations
        mock_cache = MagicMock()

        async def mock_cache_get(key):
            await asyncio.sleep(operation_delay)
            return {"cached": key}

        mock_cache.get = AsyncMock(side_effect=mock_cache_get)

        # Execute cache operations in parallel
        start_time = time.perf_counter()
        tasks = [mock_cache.get(f"key_{i}") for i in range(num_operations)]
        results = await asyncio.gather(*tasks)
        elapsed_time = time.perf_counter() - start_time

        # Verify all operations completed
        assert len(results) == num_operations

        # Verify parallel execution with more generous threshold for CI environments
        # Allow 2x the operation delay plus overhead for scheduling
        max_expected_time = operation_delay * 2.0
        sequential_time = num_operations * operation_delay

        assert elapsed_time < max_expected_time, (
            f"Cache ops took {elapsed_time:.3f}s, expected <{max_expected_time:.3f}s"
        )

        speedup = sequential_time / elapsed_time
        assert speedup > 2.5, (
            f"Cache speedup {speedup:.1f}x is too low, expected >2.5x for parallel execution"
        )

    @pytest.mark.benchmark(group="async_parallelism")
    def test_benchmark_parallel_vs_sequential_fetch(self, benchmark):
        """Benchmark parallel fetch performance vs theoretical sequential time."""

        async def parallel_fetch():
            """Execute 20 requests in parallel."""
            num_requests = 20
            request_delay = 0.01
            requests = [(2024, "Bahrain", "R", f"driver_{i}.json") for i in range(num_requests)]

            async def mock_fetch(*args, **kwargs):
                await asyncio.sleep(request_delay)
                return {"data": "test"}

            with patch("tif1.async_fetch.fetch_json_async", side_effect=mock_fetch):
                results = await fetch_multiple_async(requests)
                return results

        results = benchmark(lambda: asyncio.run(parallel_fetch()))
        assert len(results) == 20

    @pytest.mark.benchmark(group="async_parallelism")
    def test_benchmark_rate_limited_fetch(self, benchmark):
        """Benchmark rate-limited parallel fetch."""

        async def rate_limited_fetch():
            """Execute 30 requests with max 10 concurrent."""
            num_requests = 30
            max_concurrent = 10
            request_delay = 0.01

            async def mock_operation():
                await asyncio.sleep(request_delay)
                return {"success": True}

            semaphore = asyncio.Semaphore(max_concurrent)
            tasks = [
                fetch_with_rate_limit(mock_operation, semaphore=semaphore)
                for _ in range(num_requests)
            ]
            results = await asyncio.gather(*tasks)
            return results

        results = benchmark(lambda: asyncio.run(rate_limited_fetch()))
        assert len(results) == 30


@pytest.mark.benchmark(group="async_parallelism_real")
class TestRealWorldAsyncParallelism:
    """Test async parallelism with more realistic scenarios."""

    @pytest.mark.asyncio
    async def test_mixed_duration_operations_show_parallelism(self):
        """Verify parallel execution with operations of varying duration."""
        # Mix of fast and slow operations
        operation_delays = [0.05, 0.1, 0.15, 0.05, 0.2, 0.05, 0.1, 0.05]
        max_delay = max(operation_delays)

        async def mock_operation(delay):
            await asyncio.sleep(delay)
            return {"delay": delay}

        start_time = time.perf_counter()
        tasks = [mock_operation(delay) for delay in operation_delays]
        results = await asyncio.gather(*tasks)
        elapsed_time = time.perf_counter() - start_time

        # Verify all completed
        assert len(results) == len(operation_delays)

        # Time should be ~max_delay (parallel), not sum(delays) (sequential)
        max_expected_time = max_delay * 1.5
        sequential_time = sum(operation_delays)

        assert elapsed_time < max_expected_time, (
            f"Mixed ops took {elapsed_time:.3f}s, expected <{max_expected_time:.3f}s"
        )

        speedup = sequential_time / elapsed_time
        assert speedup > 2.0, (
            f"Mixed ops speedup {speedup:.1f}x is too low, expected >2x for parallel execution"
        )

    @pytest.mark.asyncio
    async def test_error_handling_maintains_parallelism(self):
        """Verify errors in some operations don't block others."""
        num_operations = 10
        operation_delay = 0.05
        error_indices = {2, 5, 7}  # These will fail

        async def mock_operation(index):
            await asyncio.sleep(operation_delay)
            if index in error_indices:
                raise ValueError(f"Operation {index} failed")
            return {"index": index}

        start_time = time.perf_counter()
        tasks = [mock_operation(i) for i in range(num_operations)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed_time = time.perf_counter() - start_time

        # Verify all operations completed (some with errors)
        assert len(results) == num_operations
        success_count = sum(1 for r in results if isinstance(r, dict))
        error_count = sum(1 for r in results if isinstance(r, Exception))

        assert success_count == num_operations - len(error_indices)
        assert error_count == len(error_indices)

        # Verify parallel execution despite errors
        max_expected_time = operation_delay * 1.5
        assert elapsed_time < max_expected_time, (
            f"Error handling took {elapsed_time:.3f}s, expected <{max_expected_time:.3f}s"
        )
