"""Tests for fetch_driver_laps_parallel method."""

from unittest.mock import AsyncMock, patch

import pytest

from tif1.core import Session


@pytest.fixture
def mock_session():
    """Create a mock session for testing."""
    session = Session(2025, "Monaco Grand Prix", "Race", lib="pandas")
    return session


@pytest.fixture
def mock_drivers():
    """Mock driver list."""
    return [
        {"driver": "VER", "team": "Red Bull Racing"},
        {"driver": "HAM", "team": "Mercedes"},
        {"driver": "LEC", "team": "Ferrari"},
    ]


@pytest.fixture
def mock_lap_data():
    """Mock lap data for a driver."""
    return {
        "time": [78.123, 77.456, 76.789],
        "lap": [1, 2, 3],
        "compound": ["SOFT", "SOFT", "SOFT"],
        "stint": [1, 1, 1],
    }


@pytest.mark.asyncio
async def test_fetch_driver_laps_parallel_basic(mock_session, mock_drivers, mock_lap_data):
    """Test basic parallel fetching of driver laps."""
    # Set _drivers directly to avoid CDN fetch
    mock_session._drivers = mock_drivers

    with patch.object(
        mock_session,
        "_fetch_laptime_payloads_async",
        new_callable=AsyncMock,
    ) as mock_fetch:
        # Mock the fetch to return lap data for all drivers
        mock_fetch.return_value = (
            [mock_lap_data, mock_lap_data, mock_lap_data],
            [],
        )

        result = await mock_session.fetch_driver_laps_parallel(["VER", "HAM", "LEC"])

        # Verify all drivers returned
        assert "VER" in result
        assert "HAM" in result
        assert "LEC" in result

        # Verify fetch was called once with all drivers
        assert mock_fetch.call_count == 1


@pytest.mark.asyncio
async def test_fetch_driver_laps_parallel_empty_list(mock_session):
    """Test with empty driver list."""
    result = await mock_session.fetch_driver_laps_parallel([])
    assert result == {}


@pytest.mark.asyncio
async def test_fetch_driver_laps_parallel_no_drivers_in_session(mock_session):
    """Test when session has no drivers."""
    # Set _drivers to empty list to simulate no drivers in session
    mock_session._drivers = []
    result = await mock_session.fetch_driver_laps_parallel(["VER"])

    assert "VER" in result
    # Should return empty DataFrame
    assert len(result["VER"]) == 0


@pytest.mark.asyncio
async def test_fetch_driver_laps_parallel_invalid_driver(mock_session, mock_drivers):
    """Test with driver not in session."""
    mock_session._drivers = mock_drivers

    with patch.object(
        mock_session,
        "_fetch_laptime_payloads_async",
        new_callable=AsyncMock,
    ) as mock_fetch:
        mock_fetch.return_value = ([], [])

        result = await mock_session.fetch_driver_laps_parallel(["INVALID"])

        assert "INVALID" in result
        # Should return empty DataFrame for invalid driver
        assert len(result["INVALID"]) == 0


@pytest.mark.asyncio
async def test_fetch_driver_laps_parallel_mixed_valid_invalid(
    mock_session, mock_drivers, mock_lap_data
):
    """Test with mix of valid and invalid drivers."""
    mock_session._drivers = mock_drivers

    with patch.object(
        mock_session,
        "_fetch_laptime_payloads_async",
        new_callable=AsyncMock,
    ) as mock_fetch:
        # Only return data for valid driver
        mock_fetch.return_value = ([mock_lap_data], [])

        result = await mock_session.fetch_driver_laps_parallel(["VER", "INVALID"])

        assert "VER" in result
        assert "INVALID" in result
        # VER should have data, INVALID should be empty
        assert len(result["VER"]) > 0
        assert len(result["INVALID"]) == 0


@pytest.mark.asyncio
async def test_fetch_driver_laps_parallel_fetch_failure(mock_session, mock_drivers):
    """Test handling of fetch failures."""
    mock_session._drivers = mock_drivers

    with patch.object(
        mock_session,
        "_fetch_laptime_payloads_async",
        new_callable=AsyncMock,
    ) as mock_fetch:
        # Return None for failed fetch
        mock_fetch.return_value = ([None, None], [])

        result = await mock_session.fetch_driver_laps_parallel(["VER", "HAM"])

        assert "VER" in result
        assert "HAM" in result
        # Both should return empty DataFrames
        assert len(result["VER"]) == 0
        assert len(result["HAM"]) == 0


@pytest.mark.asyncio
async def test_fetch_driver_laps_parallel_ultra_cold_mode(
    mock_session, mock_drivers, mock_lap_data
):
    """Test ultra cold mode cache backfill."""
    # Set _drivers to avoid fetching from network
    mock_session._drivers = mock_drivers

    with (
        patch.object(mock_session, "_resolve_ultra_cold_mode", return_value=True),
        patch.object(mock_session, "_should_backfill_ultra_cold_cache", return_value=True),
        patch.object(
            mock_session,
            "_schedule_background_cache_fill",
        ) as mock_backfill,
        patch.object(
            mock_session,
            "_fetch_laptime_payloads_async",
            new_callable=AsyncMock,
        ) as mock_fetch,
    ):
        # Return data with ultra cold payloads
        mock_fetch.return_value = (
            [mock_lap_data],
            [("VER/laptimes.json", mock_lap_data)],
        )

        await mock_session.fetch_driver_laps_parallel(["VER"])

        # Verify backfill was scheduled (only for lap data, not drivers since we set _drivers directly)
        assert mock_backfill.call_count == 1


@pytest.mark.asyncio
async def test_fetch_driver_laps_parallel_uses_asyncio_gather_internally(
    mock_session, mock_drivers, mock_lap_data
):
    """Test that the method uses parallel fetching internally."""
    mock_session._drivers = mock_drivers

    with patch.object(
        mock_session,
        "_fetch_laptime_payloads_async",
        new_callable=AsyncMock,
    ) as mock_fetch:
        # Mock returns data for all drivers
        mock_fetch.return_value = (
            [mock_lap_data, mock_lap_data, mock_lap_data],
            [],
        )

        # Fetch multiple drivers
        await mock_session.fetch_driver_laps_parallel(["VER", "HAM", "LEC"])

        # Verify single call was made (parallel fetch)
        assert mock_fetch.call_count == 1

        # Verify all drivers were requested in single call
        call_args = mock_fetch.call_args
        driver_requests = call_args[0][0]
        assert len(driver_requests) == 3
