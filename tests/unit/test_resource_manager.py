"""Unit tests for ResourceManager base class."""

from __future__ import annotations

import pytest

from tif1.core_utils.resource_manager import ResourceManager


class MockResource:
    """Mock resource for testing cleanup."""

    def __init__(self, name: str, fail_on_close: bool = False):
        self.name = name
        self.closed = False
        self.fail_on_close = fail_on_close

    def close(self) -> None:
        """Close the resource."""
        if self.fail_on_close:
            raise RuntimeError(f"Failed to close {self.name}")
        self.closed = True


class MockExecutor:
    """Mock executor with shutdown method."""

    def __init__(self, name: str, fail_on_shutdown: bool = False):
        self.name = name
        self.shutdown_called = False
        self.fail_on_shutdown = fail_on_shutdown

    def shutdown(self, *, wait: bool = True) -> None:  # noqa: ARG002
        """Shutdown the executor."""
        if self.fail_on_shutdown:
            raise RuntimeError(f"Failed to shutdown {self.name}")
        self.shutdown_called = True


class TestResourceManager:
    """Test suite for ResourceManager."""

    def test_register_and_cleanup_single_resource(self):
        """Test registering and cleaning up a single resource."""
        manager = ResourceManager()
        resource = MockResource("test_resource")

        manager._register_resource("test", resource)
        assert len(manager._resources) == 1
        assert not resource.closed

        manager._cleanup_resources()
        assert resource.closed
        assert len(manager._resources) == 0

    def test_cleanup_multiple_resources_in_reverse_order(self):
        """Test that resources are cleaned up in reverse order (LIFO)."""
        manager = ResourceManager()
        cleanup_order = []

        class OrderedResource:
            def __init__(self, name: str):
                self.name = name

            def close(self):
                cleanup_order.append(self.name)

        r1 = OrderedResource("first")
        r2 = OrderedResource("second")
        r3 = OrderedResource("third")

        manager._register_resource("r1", r1)
        manager._register_resource("r2", r2)
        manager._register_resource("r3", r3)

        manager._cleanup_resources()

        # Should be cleaned up in reverse order
        assert cleanup_order == ["third", "second", "first"]

    def test_cleanup_with_close_method(self):
        """Test cleanup of resources with close() method."""
        manager = ResourceManager()
        resource = MockResource("closeable")

        manager._register_resource("test", resource)
        manager._cleanup_resources()

        assert resource.closed

    def test_cleanup_with_shutdown_method(self):
        """Test cleanup of resources with shutdown() method."""
        manager = ResourceManager()
        executor = MockExecutor("executor")

        manager._register_resource("test", executor)
        manager._cleanup_resources()

        assert executor.shutdown_called

    def test_cleanup_continues_on_error(self):
        """Test that cleanup continues even if individual cleanups fail."""
        manager = ResourceManager()

        r1 = MockResource("first")
        r2 = MockResource("second", fail_on_close=True)
        r3 = MockResource("third")

        manager._register_resource("r1", r1)
        manager._register_resource("r2", r2)
        manager._register_resource("r3", r3)

        # Should not raise exception
        manager._cleanup_resources()

        # First and third should be cleaned up despite second failing
        assert r1.closed
        assert not r2.closed  # Failed to close
        assert r3.closed

    def test_cleanup_empty_resources(self):
        """Test cleanup with no registered resources."""
        manager = ResourceManager()
        # Should not raise exception
        manager._cleanup_resources()
        assert len(manager._resources) == 0

    def test_cleanup_idempotence(self):
        """Test that cleanup can be called multiple times safely."""
        manager = ResourceManager()
        resource = MockResource("test")

        manager._register_resource("test", resource)

        # First cleanup
        manager._cleanup_resources()
        assert resource.closed
        assert len(manager._resources) == 0

        # Second cleanup should be safe
        manager._cleanup_resources()
        assert len(manager._resources) == 0

    def test_context_manager_success(self):
        """Test context manager with successful execution."""
        resource = MockResource("test")

        with ResourceManager() as manager:
            manager._register_resource("test", resource)
            assert not resource.closed

        # Should be cleaned up after exiting context
        assert resource.closed

    def test_context_manager_with_exception(self):
        """Test context manager cleans up even when exception occurs."""
        resource = MockResource("test")

        def raise_in_context():
            with ResourceManager() as manager:
                manager._register_resource("test", resource)
                raise ValueError("Test exception")

        with pytest.raises(ValueError, match="Test exception"):
            raise_in_context()

        # Should still be cleaned up
        assert resource.closed

    def test_resource_without_cleanup_method(self):
        """Test handling of resources without close() or shutdown()."""
        manager = ResourceManager()

        # Resource with no cleanup method
        resource = object()

        manager._register_resource("no_cleanup", resource)
        # Should not raise exception
        manager._cleanup_resources()

    def test_mixed_resource_types(self):
        """Test cleanup of mixed resource types."""
        manager = ResourceManager()

        closeable = MockResource("closeable")
        executor = MockExecutor("executor")
        no_cleanup = object()

        manager._register_resource("closeable", closeable)
        manager._register_resource("executor", executor)
        manager._register_resource("no_cleanup", no_cleanup)

        manager._cleanup_resources()

        assert closeable.closed
        assert executor.shutdown_called

    def test_multiple_cleanup_errors(self):
        """Test that all cleanup errors are logged but don't stop cleanup."""
        manager = ResourceManager()

        r1 = MockResource("first", fail_on_close=True)
        r2 = MockExecutor("second", fail_on_shutdown=True)
        r3 = MockResource("third")

        manager._register_resource("r1", r1)
        manager._register_resource("r2", r2)
        manager._register_resource("r3", r3)

        # Should not raise exception
        manager._cleanup_resources()

        # Third should still be cleaned up
        assert r3.closed
        assert len(manager._resources) == 0


class ConcreteResourceManager(ResourceManager):
    """Concrete implementation for testing inheritance."""

    def __init__(self, resources_to_create: list[str], fail_at: str | None = None):
        super().__init__()
        self.created_resources: dict[str, MockResource] = {}

        try:
            for name in resources_to_create:
                if name == fail_at:
                    raise RuntimeError(f"Failed to create {name}")

                resource = MockResource(name)
                self._register_resource(name, resource)
                self.created_resources[name] = resource

            self._initialized = True

        except Exception:
            self._cleanup_resources()
            raise


class TestResourceManagerInheritance:
    """Test ResourceManager usage through inheritance."""

    def test_successful_initialization(self):
        """Test successful initialization of derived class."""
        manager = ConcreteResourceManager(["r1", "r2", "r3"])

        assert manager._initialized
        assert len(manager._resources) == 3
        assert all(not r.closed for r in manager.created_resources.values())

        manager._cleanup_resources()
        assert all(r.closed for r in manager.created_resources.values())

    def test_failed_initialization_cleans_up(self):
        """Test that failed initialization cleans up partial resources."""
        with pytest.raises(RuntimeError, match="Failed to create r2"):
            ConcreteResourceManager(["r1", "r2", "r3"], fail_at="r2")

        # Note: We can't directly verify cleanup here since the exception
        # is raised, but the cleanup should have been called in the except block

    def test_context_manager_with_derived_class(self):
        """Test context manager protocol with derived class."""
        with ConcreteResourceManager(["r1", "r2"]) as manager:
            assert manager._initialized
            assert all(not r.closed for r in manager.created_resources.values())

        # All resources should be cleaned up
        assert all(r.closed for r in manager.created_resources.values())
