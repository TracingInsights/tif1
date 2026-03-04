"""Unit tests for error-safe Cache initialization (Requirement 2)."""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from tif1.cache import Cache


class TestCacheInitialization:
    """Test error-safe Cache initialization."""

    def test_successful_initialization(self):
        """Test that successful initialization assigns connection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = Cache(cache_dir=Path(tmpdir))
            assert cache.conn is not None
            assert cache.db_path == Path(tmpdir) / "cache.sqlite"
            cache.close()

    def test_connection_failure_leaves_no_partial_state(self):
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

    def test_pragma_failure_closes_connection(self):
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

    def test_table_creation_failure_closes_connection(self):
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

    def test_commit_failure_closes_connection(self):
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

    def test_close_failure_during_cleanup_is_handled(self):
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

    def test_connection_only_assigned_after_full_success(self):
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
