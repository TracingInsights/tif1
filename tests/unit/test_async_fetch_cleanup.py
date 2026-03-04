"""Unit tests for async_fetch cleanup_resources function."""

import logging
from unittest.mock import MagicMock

from tif1 import async_fetch


class TestCleanupResources:
    """Test cleanup_resources function meets all acceptance criteria."""

    def test_cleanup_closes_all_resources_regardless_of_state(self):
        """Test that cleanup_resources closes all resources regardless of their state."""
        # Create mock resources
        mock_session = MagicMock()
        mock_session.close = MagicMock()
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        # Set up global state
        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        # Call cleanup
        async_fetch.cleanup_resources()

        # Verify both resources were closed
        mock_session.close.assert_called_once()
        mock_executor.shutdown.assert_called_once_with(wait=True)

        # Verify globals are None
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_cleanup_handles_session_close_error_gracefully(self, caplog):
        """Test that cleanup handles session close errors without raising."""
        # Create mock session that raises on close
        mock_session = MagicMock()
        mock_session.close = MagicMock(side_effect=RuntimeError("Session close failed"))
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        # Cleanup should not raise
        with caplog.at_level(logging.WARNING):
            async_fetch.cleanup_resources()

        # Verify error was logged
        assert any("Error closing HTTP session" in record.message for record in caplog.records)

        # Verify executor was still cleaned up despite session error
        mock_executor.shutdown.assert_called_once_with(wait=True)

        # Verify globals are None
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_cleanup_handles_executor_shutdown_error_gracefully(self, caplog):
        """Test that cleanup handles executor shutdown errors without raising."""
        # Create mock executor that raises on shutdown
        mock_session = MagicMock()
        mock_session.close = MagicMock()
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock(side_effect=RuntimeError("Executor shutdown failed"))

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        # Cleanup should not raise
        with caplog.at_level(logging.WARNING):
            async_fetch.cleanup_resources()

        # Verify error was logged
        assert any("Error shutting down executor" in record.message for record in caplog.records)

        # Verify session was still cleaned up despite executor error
        mock_session.close.assert_called_once()

        # Verify globals are None
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_cleanup_handles_both_resources_failing(self, caplog):
        """Test that cleanup handles both resources failing without raising."""
        # Create mock resources that both raise on cleanup
        mock_session = MagicMock()
        mock_session.close = MagicMock(side_effect=RuntimeError("Session close failed"))
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock(side_effect=RuntimeError("Executor shutdown failed"))

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        # Cleanup should not raise
        with caplog.at_level(logging.WARNING):
            async_fetch.cleanup_resources()

        # Verify both errors were logged
        assert any("Error closing HTTP session" in record.message for record in caplog.records)
        assert any("Error shutting down executor" in record.message for record in caplog.records)

        # Verify globals are None
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_cleanup_with_no_resources(self):
        """Test that cleanup works when no resources are allocated."""
        # Ensure no resources are set
        async_fetch._async_session = None
        async_fetch._executor = None

        # Should not raise
        async_fetch.cleanup_resources()

        # Verify still None
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_cleanup_with_only_session(self):
        """Test that cleanup works with only session allocated."""
        mock_session = MagicMock()
        mock_session.close = MagicMock()

        async_fetch._async_session = mock_session
        async_fetch._executor = None

        async_fetch.cleanup_resources()

        mock_session.close.assert_called_once()
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_cleanup_with_only_executor(self):
        """Test that cleanup works with only executor allocated."""
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        async_fetch._async_session = None
        async_fetch._executor = mock_executor

        async_fetch.cleanup_resources()

        mock_executor.shutdown.assert_called_once_with(wait=True)
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_cleanup_is_idempotent(self):
        """Test that cleanup can be called multiple times safely."""
        mock_session = MagicMock()
        mock_session.close = MagicMock()
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        # First cleanup
        async_fetch.cleanup_resources()

        # Second cleanup should not raise
        async_fetch.cleanup_resources()

        # Verify resources were only closed once
        mock_session.close.assert_called_once()
        mock_executor.shutdown.assert_called_once_with(wait=True)

    def test_cleanup_handles_session_without_close_method(self):
        """Test that cleanup handles session objects without close method."""
        # Create mock session without close method
        mock_session = MagicMock(spec=[])  # No methods
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        # Should not raise
        async_fetch.cleanup_resources()

        # Executor should still be cleaned up
        mock_executor.shutdown.assert_called_once_with(wait=True)

        # Verify globals are None
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_cleanup_logs_success_messages(self, caplog):
        """Test that cleanup logs success messages for each resource."""
        mock_session = MagicMock()
        mock_session.close = MagicMock()
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        with caplog.at_level(logging.DEBUG, logger="tif1.async_fetch"):
            async_fetch.cleanup_resources()

        # Verify success messages were logged
        assert any("HTTP session closed" in record.message for record in caplog.records)
        assert any("Thread pool executor shutdown" in record.message for record in caplog.records)
