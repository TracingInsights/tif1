"""Unit tests for fetch_with_rate_limit concurrency control."""

import asyncio
import time

import pytest

from tif1.async_fetch import fetch_with_rate_limit


class TestFetchWithRateLimit:
    """Test suite for fetch_with_rate_limit function."""

    @pytest.mark.asyncio
    async def test_rate_limit_with_explicit_semaphore(self):
        """Test that explicit semaphore limits concurrency."""
        semaphore = asyncio.Semaphore(2)
        concurrent_count = 0
        max_concurrent = 0

        async def mock_operation(delay: float):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(delay)
            concurrent_count -= 1
            return "success"

        # Launch 5 operations with max concurrency of 2
        tasks = [fetch_with_rate_limit(mock_operation, 0.1, semaphore=semaphore) for _ in range(5)]
        results = await asyncio.gather(*tasks)

        assert all(r == "success" for r in results)
        assert max_concurrent == 2, f"Expected max 2 concurrent, got {max_concurrent}"

    @pytest.mark.asyncio
    async def test_rate_limit_with_default_semaphore(self, monkeypatch):
        """Test that default semaphore is created from config."""

        # Mock config to return max_concurrent_requests=3
        class MockConfig:
            def get(self, key, default=None):
                if key == "max_concurrent_requests":
                    return 3
                return default

        def mock_get_config():
            return MockConfig()

        monkeypatch.setattr("tif1.config.get_config", mock_get_config)

        concurrent_count = 0
        max_concurrent = 0

        async def mock_operation(delay: float):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(delay)
            concurrent_count -= 1
            return "success"

        # Create a shared semaphore using the config
        from tif1.config import get_config

        config = get_config()
        max_concurrent_requests = max(1, config.get("max_concurrent_requests", 20))
        shared_semaphore = asyncio.Semaphore(max_concurrent_requests)

        # Launch 6 operations with shared semaphore
        tasks = [
            fetch_with_rate_limit(mock_operation, 0.1, semaphore=shared_semaphore) for _ in range(6)
        ]
        results = await asyncio.gather(*tasks)

        assert all(r == "success" for r in results)
        # The mock config returns 3, so max concurrent should be 3
        assert max_concurrent == 3, f"Expected max 3 concurrent, got {max_concurrent}"

    @pytest.mark.asyncio
    async def test_rate_limit_propagates_exceptions(self):
        """Test that exceptions from wrapped function are propagated."""
        semaphore = asyncio.Semaphore(5)

        async def failing_operation():
            raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            await fetch_with_rate_limit(failing_operation, semaphore=semaphore)

    @pytest.mark.asyncio
    async def test_rate_limit_with_args_and_kwargs(self):
        """Test that args and kwargs are passed correctly."""
        semaphore = asyncio.Semaphore(5)

        async def operation_with_params(a, b, c=None):
            return f"{a}-{b}-{c}"

        result = await fetch_with_rate_limit(
            operation_with_params, "arg1", "arg2", c="kwarg1", semaphore=semaphore
        )

        assert result == "arg1-arg2-kwarg1"

    @pytest.mark.asyncio
    async def test_rate_limit_enforces_sequential_execution_with_semaphore_1(self):
        """Test that semaphore(1) enforces sequential execution."""
        semaphore = asyncio.Semaphore(1)
        execution_order = []

        async def tracked_operation(task_id: int):
            execution_order.append(f"start-{task_id}")
            await asyncio.sleep(0.05)
            execution_order.append(f"end-{task_id}")
            return task_id

        tasks = [fetch_with_rate_limit(tracked_operation, i, semaphore=semaphore) for i in range(3)]
        results = await asyncio.gather(*tasks)

        assert results == [0, 1, 2]
        # With semaphore(1), each task should complete before next starts
        assert execution_order[0].startswith("start")
        assert execution_order[1].startswith("end")

    @pytest.mark.asyncio
    async def test_rate_limit_performance_improvement(self):
        """Test that rate limiting allows parallel execution within limit."""
        semaphore = asyncio.Semaphore(5)

        async def slow_operation():
            await asyncio.sleep(0.1)
            return "done"

        # Without rate limiting, 10 operations would take ~1s sequentially
        # With semaphore(5), should take ~0.2s (2 batches of 5)
        start = time.perf_counter()
        tasks = [fetch_with_rate_limit(slow_operation, semaphore=semaphore) for _ in range(10)]
        results = await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

        assert all(r == "done" for r in results)
        # Should complete in roughly 0.2s (allow some overhead)
        assert elapsed < 0.4, f"Expected ~0.2s, took {elapsed:.2f}s"
        # But should not be instant (would indicate no actual delay)
        assert elapsed > 0.15, f"Too fast: {elapsed:.2f}s"
