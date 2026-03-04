"""Unit tests for error-safe ThreadPoolExecutor initialization."""

import threading
from unittest.mock import MagicMock, patch

import pytest

from tif1.exceptions import NetworkError


class TestExecutorInitialization:
    """Test error-safe executor initialization."""

    def test_executor_init_success(self):
        """Test successful executor initialization."""
        # Import here to avoid side effects
        from tif1 import async_fetch

        # Reset global state
        async_fetch._executor = None

        try:
            executor = async_fetch._get_executor()
            assert executor is not None
            assert async_fetch._executor is executor
        finally:
            # Cleanup
            if async_fetch._executor:
                async_fetch._executor.shutdown(wait=False)
                async_fetch._executor = None

    def test_executor_init_failure_no_partial_state(self):
        """Test that failed executor init leaves no partial state."""
        from tif1 import async_fetch

        # Reset global state
        async_fetch._executor = None

        # Mock ThreadPoolExecutor to raise during initialization
        with patch("tif1.async_fetch._import_executor") as mock_import:
            mock_executor_class = MagicMock()
            mock_executor_class.side_effect = RuntimeError("Initialization failed")
            mock_import.return_value = mock_executor_class

            # Should raise NetworkError
            with pytest.raises(NetworkError) as exc_info:
                async_fetch._get_executor()

            # Verify the original exception is preserved
            assert exc_info.value.__cause__.__class__.__name__ == "RuntimeError"
            assert "Initialization failed" in str(exc_info.value.__cause__)

            # Verify no partial state remains
            assert async_fetch._executor is None

    def test_executor_init_failure_cleanup_on_partial_creation(self):
        """Test cleanup when executor is partially created."""
        from tif1 import async_fetch

        # Reset global state
        async_fetch._executor = None

        # Mock ThreadPoolExecutor to succeed but then fail during max() call
        with patch("tif1.async_fetch._import_executor") as mock_import:
            # Make the executor constructor raise an exception
            def raise_on_call(*args, **kwargs):
                raise RuntimeError("Executor creation failed")

            mock_executor_class = MagicMock(side_effect=raise_on_call)
            mock_import.return_value = mock_executor_class

            # Should raise NetworkError
            with pytest.raises(NetworkError) as exc_info:
                async_fetch._get_executor()

            # Verify the original exception is preserved
            assert "Executor creation failed" in str(exc_info.value.__cause__)

            # Verify no partial state remains
            assert async_fetch._executor is None

    def test_executor_singleton_behavior(self):
        """Test that executor is created only once."""
        from tif1 import async_fetch

        # Reset global state
        async_fetch._executor = None

        try:
            executor1 = async_fetch._get_executor()
            executor2 = async_fetch._get_executor()

            # Should return same instance
            assert executor1 is executor2
            assert async_fetch._executor is executor1
        finally:
            # Cleanup
            if async_fetch._executor:
                async_fetch._executor.shutdown(wait=False)
                async_fetch._executor = None

    def test_executor_thread_safety(self):
        """Test that concurrent initialization is thread-safe."""
        from tif1 import async_fetch

        # Reset global state
        async_fetch._executor = None

        executors = []
        errors = []

        def create_executor():
            try:
                executor = async_fetch._get_executor()
                executors.append(executor)
            except Exception as e:
                errors.append(e)

        try:
            # Create multiple threads trying to initialize
            threads = [threading.Thread(target=create_executor) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            # Should have no errors
            assert len(errors) == 0

            # All threads should get the same executor instance
            assert len({id(e) for e in executors}) == 1
            assert all(e is async_fetch._executor for e in executors)
        finally:
            # Cleanup
            if async_fetch._executor:
                async_fetch._executor.shutdown(wait=False)
                async_fetch._executor = None
