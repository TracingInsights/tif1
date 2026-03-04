"""Integration tests for tif1 with real data."""

import importlib.util

import pytest

import tif1
from tif1.events import EventSchedule


@pytest.mark.integration
class TestIntegration:
    """Integration tests with real data from TracingInsights."""

    def test_get_events(self):
        """Test getting events for a year."""
        events = tif1.get_events(2025)
        assert isinstance(events, EventSchedule)
        assert len(events) > 0
        assert "Abu Dhabi Grand Prix" in events["EventName"].tolist()

    def test_get_sessions(self):
        """Test getting sessions for an event."""
        sessions = tif1.get_sessions(2025, "Abu Dhabi Grand Prix")
        assert isinstance(sessions, list)
        assert "Practice 1" in sessions
        assert "Race" in sessions

    def test_session_drivers(self):
        """Test loading drivers from a session."""
        session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        drivers = session.drivers
        assert isinstance(drivers, list)
        assert len(drivers) > 0
        assert all(isinstance(d, str) and d.isdigit() for d in drivers)

    def test_session_laps_pandas(self):
        """Test loading laps with pandas lib."""
        session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", lib="pandas")
        laps = session.laps
        assert len(laps) > 0
        assert "Driver" in laps.columns
        assert "Team" in laps.columns
        assert laps["Driver"].dtype.name == "category"

    def test_session_laps_polars(self):
        """Test loading laps with polars lib."""
        if importlib.util.find_spec("polars") is None:
            pytest.skip("Polars not available")

        session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", lib="polars")
        laps = session.laps
        assert len(laps) > 0
        assert "Driver" in laps.columns
        assert "Team" in laps.columns

    def test_driver_laps(self):
        """Test loading driver-specific laps."""
        session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        ver = session.get_driver("VER")
        laps = ver.laps
        assert len(laps) > 0
        assert "LapNumber" in laps.columns

    def test_lap_telemetry(self):
        """Test loading lap telemetry."""
        session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        ver = session.get_driver("VER")
        lap = ver.get_lap(19)
        telemetry = lap.telemetry
        assert len(telemetry) > 0
        assert "Speed" in telemetry.columns
        assert "Throttle" in telemetry.columns

    def test_fastest_laps_by_driver(self):
        """Test getting fastest laps per driver."""
        session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        fastest = session.get_fastest_laps(by_driver=True)
        assert len(fastest) > 0

    def test_fastest_lap_overall(self):
        """Test getting overall fastest lap."""
        session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        fastest = session.get_fastest_laps(by_driver=False)
        assert len(fastest) == 1

    def test_driver_fastest_lap(self):
        """Test getting driver's fastest lap."""
        session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        ver = session.get_driver("VER")
        fastest = ver.get_fastest_lap()
        assert len(fastest) > 0

    @pytest.mark.asyncio
    async def test_async_laps(self):
        """Test async lap loading."""
        session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        laps = await session.laps_async()
        assert len(laps) > 0

    def test_cache_functionality(self):
        """Test caching works."""
        # First call - fetches from network
        session1 = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        laps1 = session1.laps

        # Second call - should use cache
        session2 = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        laps2 = session2.laps

        assert len(laps1) == len(laps2)

    def test_invalid_event_name_is_fuzzy_corrected(self):
        """Test fuzzy correction for invalid event names."""
        session = tif1.get_session(2025, "Abu Dabi Grand Prix", "Practice 1")
        drivers = session.drivers
        assert isinstance(drivers, list)
        assert len(drivers) > 0
