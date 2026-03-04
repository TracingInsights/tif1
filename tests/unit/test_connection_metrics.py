"""Unit tests for connection reuse metrics and logging."""

from unittest.mock import patch


def test_track_connection_created():
    """Test that connection pool creation is tracked."""
    from tif1.http_session import (
        _track_connection_created,
        get_connection_stats,
        reset_connection_stats,
    )

    reset_connection_stats()
    initial_stats = get_connection_stats()
    assert initial_stats["connections_created"] == 0

    _track_connection_created()
    stats = get_connection_stats()
    assert stats["connections_created"] == 1


def test_track_request_reused():
    """Test that reused connections are tracked correctly."""
    from tif1.http_session import _track_request, get_connection_stats, reset_connection_stats

    reset_connection_stats()

    # Track 10 requests, all reused
    for _ in range(10):
        _track_request(reused=True)

    stats = get_connection_stats()
    assert stats["total_requests"] == 10
    assert stats["connections_reused"] == 10
    assert stats["reuse_rate"] == 100.0


def test_track_request_mixed():
    """Test tracking mix of reused and new connections."""
    from tif1.http_session import _track_request, get_connection_stats, reset_connection_stats

    reset_connection_stats()

    # Track 8 reused, 2 new
    for _ in range(8):
        _track_request(reused=True)
    for _ in range(2):
        _track_request(reused=False)

    stats = get_connection_stats()
    assert stats["total_requests"] == 10
    assert stats["connections_reused"] == 8
    assert stats["reuse_rate"] == 80.0


def test_connection_stats_zero_requests():
    """Test stats when no requests have been made."""
    from tif1.http_session import get_connection_stats, reset_connection_stats

    reset_connection_stats()
    stats = get_connection_stats()

    assert stats["total_requests"] == 0
    assert stats["connections_reused"] == 0
    assert stats["reuse_rate"] == 0.0


def test_reset_connection_stats():
    """Test that reset clears all metrics."""
    from tif1.http_session import (
        _track_connection_created,
        _track_request,
        get_connection_stats,
        reset_connection_stats,
    )

    reset_connection_stats()

    # Add some data
    _track_connection_created()
    _track_request(reused=True)
    _track_request(reused=True)

    stats = get_connection_stats()
    assert stats["total_requests"] == 2
    assert stats["connections_created"] == 1

    # Reset and verify
    reset_connection_stats()
    stats = get_connection_stats()
    assert stats["total_requests"] == 0
    assert stats["connections_reused"] == 0
    assert stats["connections_created"] == 0
    assert stats["reuse_rate"] == 0.0


@patch("tif1.http_session.logger")
def test_periodic_logging(mock_logger):
    """Test that stats are logged periodically."""
    from tif1.http_session import _track_request, reset_connection_stats

    reset_connection_stats()

    # Mock time to trigger logging
    with patch("tif1.http_session.time.monotonic") as mock_time:
        # First request at t=0
        mock_time.return_value = 0.0
        _track_request(reused=True)

        # Should not log yet
        mock_logger.info.assert_not_called()

        # Second request at t=61 (past log interval)
        mock_time.return_value = 61.0
        _track_request(reused=True)

        # Should log now
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        assert "Connection pool stats" in call_args
        assert "2 requests" in call_args


def test_connection_reuse_rate_calculation():
    """Test reuse rate calculation with various scenarios."""
    from tif1.http_session import _track_request, get_connection_stats, reset_connection_stats

    # Test 100% reuse
    reset_connection_stats()
    for _ in range(100):
        _track_request(reused=True)
    stats = get_connection_stats()
    assert stats["reuse_rate"] == 100.0

    # Test 0% reuse
    reset_connection_stats()
    for _ in range(100):
        _track_request(reused=False)
    stats = get_connection_stats()
    assert stats["reuse_rate"] == 0.0

    # Test 90% reuse (acceptance criteria)
    reset_connection_stats()
    for _ in range(90):
        _track_request(reused=True)
    for _ in range(10):
        _track_request(reused=False)
    stats = get_connection_stats()
    assert stats["reuse_rate"] == 90.0


def test_get_session_tracks_creation():
    """Test that get_session tracks connection pool creation."""
    from tif1.http_session import close_session, get_connection_stats, reset_connection_stats

    reset_connection_stats()

    # Close any existing session
    close_session()

    initial_stats = get_connection_stats()
    initial_created = initial_stats["connections_created"]

    # Get session should create and track
    from tif1.http_session import get_session

    session = get_session()
    assert session is not None

    stats = get_connection_stats()
    assert stats["connections_created"] == initial_created + 1

    # Getting again should not increment
    session2 = get_session()
    assert session2 is session

    stats = get_connection_stats()
    assert stats["connections_created"] == initial_created + 1


@patch("tif1.http_session.logger")
def test_log_connection_stats_format(mock_logger):
    """Test the format of logged connection statistics."""
    from tif1.http_session import _track_request, reset_connection_stats

    reset_connection_stats()

    with patch("tif1.http_session.time.monotonic") as mock_time:
        mock_time.return_value = 0.0
        for _ in range(50):
            _track_request(reused=True)

        mock_time.return_value = 61.0
        _track_request(reused=True)

        # Check log message format
        mock_logger.info.assert_called_once()
        log_msg = mock_logger.info.call_args[0][0]

        assert "51 requests" in log_msg
        assert "51 reused" in log_msg
        assert "100.0%" in log_msg
        assert "pools created" in log_msg
