"""Property-based tests for resource cleanup idempotence.

Tests verify that resource cleanup operations are idempotent - calling cleanup
multiple times from various initial states always results in a clean state without
errors or resource leaks.
"""

import tempfile
import threading
from pathlib import Path
from unittest.mock import MagicMock

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tif1 import async_fetch
from tif1.cache import Cache, _cleanup_cache
from tif1.core_utils.resource_manager import ResourceManager
from tif1.http_session import close_session


class MockResource:
    """Mock resource for testing cleanup behavior."""

    def __init__(self, name: str, fail_on_close: bool = False, close_count: int = 0):
        self.name = name
        self.closed = False
        self.fail_on_close = fail_on_close
        self.close_count = close_count

    def close(self) -> None:
        """Close the resource, tracking call count."""
        self.close_count += 1
        if self.fail_on_close:
            raise RuntimeError(f"Failed to close {self.name}")
        self.closed = True


class MockExecutor:
    """Mock executor for testing shutdown behavior."""

    def __init__(self, name: str, fail_on_shutdown: bool = False, shutdown_count: int = 0):
        self.name = name
        self.shutdown_called = False
        self.fail_on_shutdown = fail_on_shutdown
        self.shutdown_count = shutdown_count

    def shutdown(self, *, wait: bool = True) -> None:  # noqa: ARG002
        """Shutdown the executor, tracking call count."""
        self.shutdown_count += 1
        if self.fail_on_shutdown:
            raise RuntimeError(f"Failed to shutdown {self.name}")
        self.shutdown_called = True


class TestResourceManagerIdempotence:
    """Property tests for ResourceManager cleanup idempotence."""

    @given(
        num_resources=st.integers(min_value=0, max_value=20),
        num_cleanup_calls=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=50, deadline=2000)
    def test_cleanup_idempotence_multiple_calls(self, num_resources: int, num_cleanup_calls: int):
        """Property: Calling cleanup multiple times is safe and idempotent.

        Regardless of how many times cleanup is called, the system should
        reach a clean state with all resources released and no errors raised.

        Args:
            num_resources: Number of resources to register
            num_cleanup_calls: Number of times to call cleanup
        """
        manager = ResourceManager()
        resources = [MockResource(f"resource_{i}") for i in range(num_resources)]

        # Register all resources
        for i, resource in enumerate(resources):
            manager._register_resource(f"r{i}", resource)

        # Call cleanup multiple times
        for _ in range(num_cleanup_calls):
            manager._cleanup_resources()

        # Property: All resources should be closed
        for resource in resources:
            assert resource.closed, f"Resource {resource.name} not closed"

        # Property: Resource list should be empty
        assert len(manager._resources) == 0, "Resource list not empty after cleanup"

        # Property: Additional cleanup calls should be safe
        manager._cleanup_resources()
        assert len(manager._resources) == 0

    @given(
        num_resources=st.integers(min_value=1, max_value=15),
        failure_indices=st.lists(st.integers(min_value=0, max_value=14), max_size=5),
    )
    @settings(max_examples=40, deadline=2000)
    def test_cleanup_idempotence_with_failures(
        self, num_resources: int, failure_indices: list[int]
    ):
        """Property: Cleanup is idempotent even when some resources fail to close.

        When some resources fail during cleanup, calling cleanup again should
        still result in a clean state for resources that can be cleaned.

        Args:
            num_resources: Number of resources to register
            failure_indices: Indices of resources that should fail on close
        """
        manager = ResourceManager()
        resources = []

        for i in range(num_resources):
            fail = i in failure_indices
            resource = MockResource(f"resource_{i}", fail_on_close=fail)
            resources.append(resource)
            manager._register_resource(f"r{i}", resource)

        # First cleanup
        manager._cleanup_resources()

        # Property: Resources that can close should be closed
        for i, resource in enumerate(resources):
            if i not in failure_indices:
                assert resource.closed, f"Resource {resource.name} should be closed"

        # Property: Resource list should be empty even with failures
        assert len(manager._resources) == 0, "Resource list not empty after cleanup"

        # Property: Second cleanup should be safe
        manager._cleanup_resources()
        assert len(manager._resources) == 0

    @given(
        initial_state=st.sampled_from(["empty", "partial", "full"]),
        num_cleanup_calls=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=30, deadline=2000)
    def test_cleanup_from_various_initial_states(self, initial_state: str, num_cleanup_calls: int):
        """Property: Cleanup reaches clean state from any initial state.

        Whether starting from empty, partially initialized, or fully initialized
        state, cleanup should always result in a clean state.

        Args:
            initial_state: Starting state (empty, partial, full)
            num_cleanup_calls: Number of cleanup calls to make
        """
        manager = ResourceManager()

        if initial_state == "empty":
            # No resources registered
            pass
        elif initial_state == "partial":
            # Some resources registered
            for i in range(3):
                manager._register_resource(f"r{i}", MockResource(f"resource_{i}"))
        elif initial_state == "full":
            # Many resources registered
            for i in range(10):
                manager._register_resource(f"r{i}", MockResource(f"resource_{i}"))

        # Call cleanup multiple times
        for _ in range(num_cleanup_calls):
            manager._cleanup_resources()

        # Property: System reaches clean state
        assert len(manager._resources) == 0, "System not in clean state"

        # Property: Additional cleanup is safe
        manager._cleanup_resources()
        assert len(manager._resources) == 0

    def test_cleanup_idempotence_with_mixed_resource_types(self):
        """Property: Cleanup is idempotent with mixed resource types.

        Resources with close(), shutdown(), or no cleanup method should all
        be handled correctly across multiple cleanup calls.
        """
        manager = ResourceManager()

        closeable = MockResource("closeable")
        executor = MockExecutor("executor")
        no_cleanup = object()

        manager._register_resource("closeable", closeable)
        manager._register_resource("executor", executor)
        manager._register_resource("no_cleanup", no_cleanup)

        # Multiple cleanup calls
        for _ in range(3):
            manager._cleanup_resources()

        # Property: Resources with cleanup methods are cleaned
        assert closeable.closed
        assert executor.shutdown_called

        # Property: Resource list is empty
        assert len(manager._resources) == 0

    @given(num_threads=st.integers(min_value=2, max_value=10))
    @settings(max_examples=20, deadline=5000)
    def test_concurrent_cleanup_idempotence(self, num_threads: int):
        """Property: Concurrent cleanup calls are safe and idempotent.

        Multiple threads calling cleanup concurrently should result in
        a clean state without race conditions or errors.

        Args:
            num_threads: Number of concurrent threads calling cleanup
        """
        manager = ResourceManager()
        resources = [MockResource(f"resource_{i}") for i in range(10)]

        for i, resource in enumerate(resources):
            manager._register_resource(f"r{i}", resource)

        # Concurrent cleanup calls
        def cleanup():
            manager._cleanup_resources()

        threads = [threading.Thread(target=cleanup) for _ in range(num_threads)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: All resources should be closed
        for resource in resources:
            assert resource.closed, f"Resource {resource.name} not closed"

        # Property: Resource list should be empty
        assert len(manager._resources) == 0


class TestAsyncFetchCleanupIdempotence:
    """Property tests for async_fetch cleanup_resources idempotence."""

    def setup_method(self):
        """Reset global state before each test."""
        async_fetch._async_session = None
        async_fetch._executor = None

    def teardown_method(self):
        """Clean up after each test."""
        async_fetch._async_session = None
        async_fetch._executor = None

    @given(num_cleanup_calls=st.integers(min_value=1, max_value=10))
    @settings(max_examples=30, deadline=2000)
    def test_cleanup_resources_idempotence(self, num_cleanup_calls: int):
        """Property: cleanup_resources can be called multiple times safely.

        Calling cleanup_resources multiple times should not raise errors
        and should leave the system in a clean state.

        Args:
            num_cleanup_calls: Number of times to call cleanup_resources
        """
        # Set up mock resources
        mock_session = MagicMock()
        mock_session.close = MagicMock()
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        # Call cleanup multiple times
        for _ in range(num_cleanup_calls):
            async_fetch.cleanup_resources()

        # Property: Globals should be None
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

        # Property: Resources should only be closed once
        assert mock_session.close.call_count == 1
        assert mock_executor.shutdown.call_count == 1

    @given(
        initial_state=st.sampled_from(["none", "session_only", "executor_only", "both"]),
        num_cleanup_calls=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=40, deadline=2000)
    def test_cleanup_from_various_states(self, initial_state: str, num_cleanup_calls: int):
        """Property: Cleanup reaches clean state from any initial resource state.

        Whether starting with no resources, only session, only executor, or both,
        cleanup should always result in a clean state.

        Args:
            initial_state: Initial resource allocation state
            num_cleanup_calls: Number of cleanup calls to make
        """
        mock_session = MagicMock()
        mock_session.close = MagicMock()
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        # Set up initial state
        if initial_state == "none":
            async_fetch._async_session = None
            async_fetch._executor = None
        elif initial_state == "session_only":
            async_fetch._async_session = mock_session
            async_fetch._executor = None
        elif initial_state == "executor_only":
            async_fetch._async_session = None
            async_fetch._executor = mock_executor
        elif initial_state == "both":
            async_fetch._async_session = mock_session
            async_fetch._executor = mock_executor

        # Call cleanup multiple times
        for _ in range(num_cleanup_calls):
            async_fetch.cleanup_resources()

        # Property: System reaches clean state
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_cleanup_idempotence_with_failures(self):
        """Property: Cleanup is idempotent even when resources fail to close.

        When resources fail during cleanup, subsequent cleanup calls should
        still be safe and leave the system in a clean state.
        """
        # Set up resources that fail on cleanup
        mock_session = MagicMock()
        mock_session.close = MagicMock(side_effect=RuntimeError("Session close failed"))
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock(side_effect=RuntimeError("Executor shutdown failed"))

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        # First cleanup (with failures)
        async_fetch.cleanup_resources()

        # Property: Globals should be None despite failures
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

        # Property: Second cleanup should be safe
        async_fetch.cleanup_resources()
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    @given(num_threads=st.integers(min_value=2, max_value=10))
    @settings(max_examples=20, deadline=5000)
    def test_concurrent_cleanup_idempotence(self, num_threads: int):
        """Property: Concurrent cleanup calls are safe.

        Multiple threads calling cleanup_resources concurrently should
        result in a clean state without race conditions.

        Args:
            num_threads: Number of concurrent threads
        """
        mock_session = MagicMock()
        mock_session.close = MagicMock()
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        # Concurrent cleanup calls
        def cleanup():
            async_fetch.cleanup_resources()

        threads = [threading.Thread(target=cleanup) for _ in range(num_threads)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: System reaches clean state
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

        # Property: Resources closed exactly once
        assert mock_session.close.call_count == 1
        assert mock_executor.shutdown.call_count == 1


class TestCacheCleanupIdempotence:
    """Property tests for Cache cleanup idempotence."""

    @given(num_close_calls=st.integers(min_value=1, max_value=10))
    @settings(
        max_examples=30,
        deadline=2000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_cache_close_idempotence(self, num_close_calls: int, tmp_path):
        """Property: Cache.close() can be called multiple times safely.

        Calling close() multiple times should not raise errors and should
        leave the cache in a clean state.

        Args:
            num_close_calls: Number of times to call close()
            tmp_path: Temporary directory for cache
        """
        # Create unique cache directory for each hypothesis example
        cache_dir = Path(tempfile.mkdtemp(dir=tmp_path))
        cache = Cache(cache_dir=cache_dir)

        # Verify cache is initialized
        assert cache.conn is not None

        # Call close multiple times
        for _ in range(num_close_calls):
            cache.close()

        # Property: Connection should be None
        assert cache.conn is None

        # Property: Memory caches should be empty
        assert len(cache._memory_cache) == 0
        assert len(cache._memory_telemetry_cache) == 0

    @given(
        has_data=st.booleans(),
        num_close_calls=st.integers(min_value=1, max_value=5),
    )
    @settings(
        max_examples=40,
        deadline=3000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_cache_close_from_various_states(self, has_data: bool, num_close_calls: int, tmp_path):
        """Property: Cache.close() reaches clean state from any initial state.

        Whether the cache has data or is empty, close() should always
        result in a clean state.

        Args:
            has_data: Whether to populate cache with data before closing
            num_close_calls: Number of close calls to make
            tmp_path: Temporary directory for cache
        """
        # Create unique cache directory for each hypothesis example
        cache_dir = Path(tempfile.mkdtemp(dir=tmp_path))
        cache = Cache(cache_dir=cache_dir)

        if has_data:
            # Populate cache with some data
            cache.set("key1", {"data": "value1"})
            cache.set("key2", {"data": "value2"})
            cache.set_telemetry(2024, "monaco", "race", "VER", 1, {"speed": [300, 310]})

        # Call close multiple times
        for _ in range(num_close_calls):
            cache.close()

        # Property: System reaches clean state
        assert cache.conn is None
        assert len(cache._memory_cache) == 0
        assert len(cache._memory_telemetry_cache) == 0

    def test_cache_close_with_pending_writes(self, tmp_path):
        """Property: Cache.close() commits pending writes before closing.

        When there are pending writes, close() should commit them and
        subsequent close() calls should be safe.
        """
        cache = Cache(cache_dir=tmp_path)

        # Add data without forcing commit
        cache.set("key1", {"data": "value1"})

        # First close (should commit pending writes)
        cache.close()

        # Property: Connection should be None
        assert cache.conn is None

        # Property: Second close should be safe
        cache.close()
        assert cache.conn is None

    @given(num_threads=st.integers(min_value=2, max_value=10))
    @settings(
        max_examples=20,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_concurrent_cache_close_idempotence(self, num_threads: int, tmp_path):
        """Property: Concurrent Cache.close() calls are safe.

        Multiple threads calling close() concurrently should result in
        a clean state without race conditions.

        Args:
            num_threads: Number of concurrent threads
            tmp_path: Temporary directory for cache
        """
        # Create unique cache directory for each hypothesis example
        cache_dir = Path(tempfile.mkdtemp(dir=tmp_path))
        cache = Cache(cache_dir=cache_dir)

        # Add some data
        cache.set("key1", {"data": "value1"})

        # Concurrent close calls
        def close():
            cache.close()

        threads = [threading.Thread(target=close) for _ in range(num_threads)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: System reaches clean state
        assert cache.conn is None
        assert len(cache._memory_cache) == 0
        assert len(cache._memory_telemetry_cache) == 0


class TestIntegratedResourceCleanupIdempotence:
    """Property tests for integrated resource cleanup across modules."""

    def setup_method(self):
        """Reset global state before each test."""
        async_fetch._async_session = None
        async_fetch._executor = None

    def teardown_method(self):
        """Clean up after each test."""
        async_fetch.cleanup_resources()
        _cleanup_cache()

    @given(num_cleanup_cycles=st.integers(min_value=1, max_value=5))
    @settings(
        max_examples=20,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_full_system_cleanup_idempotence(self, num_cleanup_cycles: int, tmp_path):
        """Property: Full system cleanup is idempotent across all modules.

        Cleaning up all resources (async_fetch, cache, http_session) multiple
        times should be safe and result in a clean state.

        Args:
            num_cleanup_cycles: Number of full cleanup cycles
            tmp_path: Temporary directory for cache
        """
        # Create unique cache directory for each hypothesis example
        cache_dir = Path(tempfile.mkdtemp(dir=tmp_path))

        # Initialize resources
        mock_session = MagicMock()
        mock_session.close = MagicMock()
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        cache = Cache(cache_dir=cache_dir)
        cache.set("key1", {"data": "value1"})

        # Perform multiple cleanup cycles
        for _ in range(num_cleanup_cycles):
            async_fetch.cleanup_resources()
            cache.close()
            close_session()

        # Property: All resources should be in clean state
        assert async_fetch._async_session is None
        assert async_fetch._executor is None
        assert cache.conn is None
        assert len(cache._memory_cache) == 0

    def test_cleanup_order_independence(self, tmp_path):
        """Property: Cleanup order doesn't affect final clean state.

        Cleaning up resources in different orders should all result in
        the same clean state.
        """
        # Test cleanup order 1: async_fetch -> cache -> http_session
        mock_session1 = MagicMock()
        mock_session1.close = MagicMock()
        mock_executor1 = MagicMock()
        mock_executor1.shutdown = MagicMock()

        async_fetch._async_session = mock_session1
        async_fetch._executor = mock_executor1
        cache1 = Cache(cache_dir=tmp_path / "cache1")

        async_fetch.cleanup_resources()
        cache1.close()
        close_session()

        assert async_fetch._async_session is None
        assert async_fetch._executor is None
        assert cache1.conn is None

        # Test cleanup order 2: cache -> http_session -> async_fetch
        mock_session2 = MagicMock()
        mock_session2.close = MagicMock()
        mock_executor2 = MagicMock()
        mock_executor2.shutdown = MagicMock()

        async_fetch._async_session = mock_session2
        async_fetch._executor = mock_executor2
        cache2 = Cache(cache_dir=tmp_path / "cache2")

        cache2.close()
        close_session()
        async_fetch.cleanup_resources()

        assert async_fetch._async_session is None
        assert async_fetch._executor is None
        assert cache2.conn is None

        # Property: Both cleanup orders result in same clean state
        # (verified by assertions above)
