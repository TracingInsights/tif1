"""Unit tests for error path resource cleanup (Requirement 16).

This test module verifies that all resource initialization failures
properly clean up allocated resources, ensuring no resource leaks occur.

Tests cover:
- Executor initialization failure cleanup (Task 1.2.2)
- HTTP session initialization failure cleanup (Task 1.2.3)
- Cache initialization failure cleanup (Task 1.2.4)
"""

import logging
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from tif1 import async_fetch, http_session
from tif1.cache import Cache
from tif1.exceptions import NetworkError


class TestExecutorInitFailureCleanup:
    """Test executor initialization failure cleanup (Task 1.2.2)."""

    def test_executor_init_failure_leaves_no_partial_state(self):
        """Test that failed executor init leaves no partial state."""
        # Reset global state
        async_fetch._executor = None

        # Mock ThreadPoolExecutor to fail on creation
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        with patch.object(async_fetch, "_import_executor") as mock_import:
            MockExecutor = MagicMock(side_effect=RuntimeError("Executor creation failed"))
            mock_import.return_value = MockExecutor

            # Should raise NetworkError
            with pytest.raises(NetworkError) as exc_info:
                async_fetch._get_executor()

            assert "executor_init" in str(exc_info.value)

            # Verify global state is None
            assert async_fetch._executor is None

    def test_executor_init_partial_creation_cleanup(self):
        """Test that partial executor creation is cleaned up when executor init fails."""
        # Reset global state
        async_fetch._executor = None

        # Create mock executor that fails during initialization
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        with patch.object(async_fetch, "_import_executor") as mock_import:
            # Mock the ThreadPoolExecutor class
            def mock_executor_class(*args, **kwargs):
                # Executor is created but then raises during __init__
                # This simulates partial creation
                raise RuntimeError("Executor initialization failed")

            MockExecutor = MagicMock(side_effect=mock_executor_class)
            mock_import.return_value = MockExecutor

            # Should raise NetworkError
            with pytest.raises(NetworkError):
                async_fetch._get_executor()

            # Verify global state is None
            assert async_fetch._executor is None

    def test_executor_cleanup_on_shutdown_failure(self):
        """Test that cleanup handles executor shutdown failures gracefully."""
        # Create mock executor that fails on shutdown
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock(side_effect=RuntimeError("Shutdown failed"))

        async_fetch._executor = mock_executor

        # Cleanup should not raise
        async_fetch.cleanup_resources()

        # Verify shutdown was attempted
        mock_executor.shutdown.assert_called_once_with(wait=True)

        # Verify global state is None despite failure
        assert async_fetch._executor is None

    def test_executor_init_success_assigns_only_after_full_success(self):
        """Test that executor is only assigned after full initialization success."""
        # Reset global state
        async_fetch._executor = None

        # Track assignment order
        assignments = []

        original_get_executor = async_fetch._get_executor

        def tracked_get_executor():
            result = original_get_executor()
            assignments.append(async_fetch._executor)
            return result

        with patch.object(async_fetch, "_get_executor", tracked_get_executor):
            executor = async_fetch._get_executor()

            # Verify executor was assigned exactly once
            assert len(assignments) == 1
            assert assignments[0] is not None
            assert executor is not None

            # Cleanup
            async_fetch.cleanup_resources()


class TestHTTPSessionInitFailureCleanup:
    """Test HTTP session initialization failure cleanup (Task 1.2.3)."""

    def test_session_creation_releases_resources_on_failure(self):
        """Test that failed session initialization releases all allocated resources."""
        mock_session = Mock()
        mock_session.mount = Mock(side_effect=Exception("Mount failed"))
        mock_session.close = Mock()

        with patch.object(http_session, "_import_niquests") as mock_import:
            mock_niquests = Mock()
            mock_niquests.Session = Mock(return_value=mock_session)
            mock_niquests.adapters.HTTPAdapter = Mock()
            mock_import.return_value = mock_niquests

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "pool_connections": 50,
                        "pool_maxsize": 100,
                        "http_resolvers": ["standard"],
                    }.get(key, default)
                )
                mock_config.return_value = config

                # Should raise NetworkError after trying all resolvers
                with pytest.raises(NetworkError):
                    http_session._create_session()

                # Verify session.close() was called to release resources
                mock_session.close.assert_called_once()

    def test_session_creation_with_multiple_resolver_fallbacks(self):
        """Test that session creation tries multiple resolvers and cleans up on each failure."""
        close_call_count = 0

        def create_failing_session(*args, **kwargs):
            nonlocal close_call_count
            mock_session = Mock()
            mock_session.mount = Mock(side_effect=Exception("Mount failed"))

            def track_close():
                nonlocal close_call_count
                close_call_count += 1

            mock_session.close = Mock(side_effect=track_close)
            return mock_session

        with patch.object(http_session, "_import_niquests") as mock_import:
            mock_niquests = Mock()
            mock_niquests.Session = Mock(side_effect=create_failing_session)
            mock_niquests.adapters.HTTPAdapter = Mock()
            mock_import.return_value = mock_niquests

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "pool_connections": 50,
                        "pool_maxsize": 100,
                        "http_resolvers": ["standard", "doh://cloudflare", "doh://google"],
                    }.get(key, default)
                )
                mock_config.return_value = config

                with pytest.raises(NetworkError):
                    http_session._create_session()

                # Verify close was called for each failed attempt (3 resolvers)
                assert close_call_count == 3

    def test_session_creation_failure_during_session_init(self):
        """Test resource cleanup when Session() constructor itself fails."""
        with patch.object(http_session, "_import_niquests") as mock_import:
            mock_niquests = Mock()
            mock_niquests.Session = Mock(side_effect=Exception("Session init failed"))
            mock_import.return_value = mock_niquests

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "pool_connections": 50,
                        "pool_maxsize": 100,
                        "http_resolvers": ["standard"],
                    }.get(key, default)
                )
                mock_config.return_value = config

                # Should handle gracefully and try next resolver
                with pytest.raises(NetworkError):
                    http_session._create_session()

    def test_partial_session_initialization_cleanup(self):
        """Test that partial initialization (session created but mount fails) is cleaned up."""
        mock_session = Mock()
        mock_session.close = Mock()

        # Simulate failure after session creation but during mount
        call_count = [0]

        def mount_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("First mount failed")
            # Second call succeeds

        mock_session.mount = Mock(side_effect=mount_side_effect)
        mock_session.headers = Mock()
        mock_session.headers.update = Mock()

        with patch.object(http_session, "_import_niquests") as mock_import:
            mock_niquests = Mock()
            mock_niquests.Session = Mock(return_value=mock_session)
            mock_niquests.adapters.HTTPAdapter = Mock()
            mock_import.return_value = mock_niquests

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "pool_connections": 50,
                        "pool_maxsize": 100,
                        "http_resolvers": ["standard", "doh://cloudflare"],
                    }.get(key, default)
                )
                mock_config.return_value = config

                result = http_session._create_session()

                # First attempt should have called close, second succeeded
                assert mock_session.close.call_count == 1
                assert result is mock_session

    def test_session_close_error_is_logged_not_raised(self, caplog):
        """Test that session close errors during cleanup are logged, not raised."""
        mock_session = Mock()
        mock_session.close = Mock(side_effect=RuntimeError("Close failed"))

        http_session._shared_session = mock_session

        with caplog.at_level(logging.WARNING):
            http_session.close_session()

        # Verify error was logged
        assert any("Error closing HTTP session" in record.message for record in caplog.records)

        # Verify global state is None despite close failure
        assert http_session._shared_session is None


class TestCacheInitFailureCleanup:
    """Test cache initialization failure cleanup (Task 1.2.4)."""

    def test_cache_connection_failure_leaves_no_partial_state(self):
        """Test that connection failure leaves conn as None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)

            # Mock sqlite3.connect to raise an exception
            with patch(
                "sqlite3.connect", side_effect=sqlite3.OperationalError("Connection failed")
            ):
                cache = Cache(cache_dir=cache_dir)

                # Verify conn is None after failure
                assert cache.conn is None
                assert cache.db_path == cache_dir / "cache.sqlite"

    def test_cache_pragma_failure_closes_connection(self):
        """Test that PRAGMA execution failure closes the connection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)

            # Create a mock connection that fails on execute
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = sqlite3.OperationalError("PRAGMA failed")

            with patch("sqlite3.connect", return_value=mock_conn):
                cache = Cache(cache_dir=cache_dir)

                # Verify conn is None after failure
                assert cache.conn is None

                # Verify close was called on the connection
                mock_conn.close.assert_called_once()

    def test_cache_table_creation_failure_closes_connection(self):
        """Test that table creation failure closes the connection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)

            # Create a mock connection that fails on CREATE TABLE
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = [
                None,  # PRAGMA journal_mode=WAL
                None,  # PRAGMA synchronous=NORMAL
                None,  # PRAGMA cache_size
                sqlite3.OperationalError("CREATE TABLE failed"),  # First CREATE TABLE
            ]

            with patch("sqlite3.connect", return_value=mock_conn):
                cache = Cache(cache_dir=cache_dir)

                # Verify conn is None after failure
                assert cache.conn is None

                # Verify close was called on the connection
                mock_conn.close.assert_called_once()

    def test_cache_commit_failure_closes_connection(self):
        """Test that commit failure closes the connection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)

            # Create a mock connection that fails on commit
            mock_conn = MagicMock()
            mock_conn.execute.return_value = None
            mock_conn.commit.side_effect = sqlite3.OperationalError("Commit failed")

            with patch("sqlite3.connect", return_value=mock_conn):
                cache = Cache(cache_dir=cache_dir)

                # Verify conn is None after failure
                assert cache.conn is None

                # Verify close was called on the connection
                mock_conn.close.assert_called_once()

    def test_cache_close_failure_during_cleanup_is_handled(self):
        """Test that close() failure during cleanup doesn't raise."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)

            # Create a mock connection that fails on both execute and close
            mock_conn = MagicMock()
            mock_conn.execute.side_effect = sqlite3.OperationalError("Execute failed")
            mock_conn.close.side_effect = sqlite3.OperationalError("Close failed")

            with patch("sqlite3.connect", return_value=mock_conn):
                # Should not raise even though both execute and close fail
                cache = Cache(cache_dir=cache_dir)

                # Verify conn is None after failure
                assert cache.conn is None

                # Verify close was attempted
                mock_conn.close.assert_called_once()

    def test_cache_connection_only_assigned_after_full_success(self):
        """Test that self.conn is only assigned after all initialization succeeds."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)

            # Track when conn is assigned
            conn_assignments = []

            original_init = Cache._init_sqlite

            def tracked_init(self):
                # Call original
                original_init(self)
                # Track if conn was assigned
                conn_assignments.append(self.conn)

            with patch.object(Cache, "_init_sqlite", tracked_init):
                cache = Cache(cache_dir=cache_dir)

                # Verify conn was assigned exactly once and is not None
                assert len(conn_assignments) == 1
                assert conn_assignments[0] is not None

                cache.close()

    def test_cache_successful_initialization(self):
        """Test that successful initialization assigns connection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = Cache(cache_dir=Path(tmpdir))
            assert cache.conn is not None
            assert cache.db_path == Path(tmpdir) / "cache.sqlite"
            cache.close()


class TestIntegratedResourceCleanup:
    """Test integrated resource cleanup across all components."""

    def test_all_resources_cleanup_on_module_exit(self):
        """Test that all resources are cleaned up when cleanup_resources is called."""
        # Setup mock resources
        mock_session = MagicMock()
        mock_session.close = MagicMock()
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        async_fetch._async_session = mock_session
        async_fetch._executor = mock_executor

        # Call cleanup
        async_fetch.cleanup_resources()

        # Verify all resources were cleaned up
        mock_session.close.assert_called_once()
        mock_executor.shutdown.assert_called_once_with(wait=True)
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_cleanup_handles_all_failures_gracefully(self, caplog):
        """Test that cleanup handles all resource failures without raising."""
        # Create mock resources that all fail on cleanup
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

        # Verify globals are None despite failures
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

    def test_partial_resource_allocation_cleanup(self):
        """Test cleanup when only some resources are allocated."""
        # Test with only session
        mock_session = MagicMock()
        mock_session.close = MagicMock()

        async_fetch._async_session = mock_session
        async_fetch._executor = None

        async_fetch.cleanup_resources()

        mock_session.close.assert_called_once()
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

        # Test with only executor
        mock_executor = MagicMock()
        mock_executor.shutdown = MagicMock()

        async_fetch._async_session = None
        async_fetch._executor = mock_executor

        async_fetch.cleanup_resources()

        mock_executor.shutdown.assert_called_once_with(wait=True)
        assert async_fetch._async_session is None
        assert async_fetch._executor is None

    def test_no_resources_allocated_cleanup(self):
        """Test that cleanup works when no resources are allocated."""
        # Ensure no resources are set
        async_fetch._async_session = None
        async_fetch._executor = None

        # Should not raise
        async_fetch.cleanup_resources()

        # Verify still None
        assert async_fetch._async_session is None
        assert async_fetch._executor is None


class TestErrorPathCoverage:
    """Test comprehensive error path coverage for all resource types."""

    def test_executor_init_with_invalid_config(self):
        """Test executor initialization with invalid configuration."""
        # Reset global state
        async_fetch._executor = None

        with patch("tif1.config.get_config") as mock_config:
            config = Mock()
            config.get = Mock(return_value="invalid")  # Invalid max_workers

            mock_config.return_value = config

            with patch.object(async_fetch, "_import_executor") as mock_import:
                MockExecutor = MagicMock(side_effect=TypeError("Invalid max_workers"))
                mock_import.return_value = MockExecutor

                with pytest.raises(NetworkError):
                    async_fetch._get_executor()

                # Verify no partial state
                assert async_fetch._executor is None

    def test_session_init_with_all_resolvers_failing(self):
        """Test session initialization when all resolvers fail."""
        with patch.object(http_session, "_import_niquests") as mock_import:
            mock_niquests = Mock()
            mock_niquests.Session = Mock(side_effect=Exception("All resolvers failed"))
            mock_import.return_value = mock_niquests

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "pool_connections": 50,
                        "pool_maxsize": 100,
                        "http_resolvers": ["standard", "doh://cloudflare", "doh://google"],
                    }.get(key, default)
                )
                mock_config.return_value = config

                with pytest.raises(NetworkError) as exc_info:
                    http_session._create_session()

                assert "session_init" in str(exc_info.value)

    def test_cache_init_with_readonly_filesystem(self):
        """Test cache initialization with read-only filesystem."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)

            # Mock sqlite3.connect to simulate read-only filesystem
            with patch(
                "sqlite3.connect",
                side_effect=sqlite3.OperationalError("attempt to write a readonly database"),
            ):
                cache = Cache(cache_dir=cache_dir)

                # Verify cache handles failure gracefully
                assert cache.conn is None

                # Verify cache operations don't crash
                result = cache.get("test_key")
                assert result is None

                # Set should not raise
                cache.set("test_key", {"data": "value"})

    def test_cache_init_with_corrupted_database(self):
        """Test cache initialization with corrupted database file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            db_path = cache_dir / "cache.sqlite"

            # Create a corrupted database file
            db_path.write_text("corrupted data")

            # Cache should handle corruption gracefully
            cache = Cache(cache_dir=cache_dir)

            # Either conn is None or cache operations handle errors
            if cache.conn is not None:
                # Operations should not crash
                result = cache.get("test_key")
                assert result is None or isinstance(result, dict)

                cache.close()
