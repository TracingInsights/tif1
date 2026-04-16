"""Unit tests for tif1 core functionality."""

import asyncio
from typing import ClassVar
from unittest.mock import patch

import pandas as pd
import pytest

from tif1.core import (
    Driver,
    Lap,
    Session,
    clear_lap_cache,
    get_session,
)
from tif1.exceptions import (
    DataNotFoundError,
    DriverNotFoundError,
    InvalidDataError,
    LapNotFoundError,
    NetworkError,
)


class TestSession:
    """Test Session class."""

    def test_session_init(self):
        """Test session initialization."""
        session = Session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        assert session.year == 2025
        assert "Abu%20Dhabi" in session.gp
        assert "Practice%201" in session.session

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_fetch_json_success(self, mock_fetch):
        """Test successful JSON fetch."""
        mock_fetch.return_value = {"drivers": []}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        data = session._fetch_json("drivers.json")
        assert isinstance(data, dict)

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_fetch_json_404(self, mock_fetch):
        """Test 404 error handling."""
        mock_fetch.side_effect = DataNotFoundError(year=2025, event="Test GP", session="Race")

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        with pytest.raises(DataNotFoundError):
            session._fetch_json("missing.json")

    def test_backend_selection(self):
        """Test lib selection."""
        session_pandas = Session(2025, "Test GP", "Race", lib="pandas")
        assert session_pandas.lib == "pandas"

        session_polars = Session(2025, "Test GP", "Race", lib="polars")
        assert session_polars.lib in ["polars", "pandas"]  # Falls back if not available


class TestDriver:
    """Test Driver class."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_driver_init(self, mock_fetch):
        """Test driver initialization."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}
        session = Session(2025, "Test GP", "Race")
        driver = Driver(session, "VER")
        assert driver.driver == "VER"
        assert driver.session == session


class TestLap:
    """Test Lap class."""

    def test_lap_init(self):
        """Test lap initialization."""
        session = Session(2025, "Test GP", "Race")
        lap = Lap({"Driver": "VER", "LapNumber": 19}, session=session)
        assert lap.driver == "VER"
        assert lap.lap_number == 19


class TestGetSession:
    """Test get_session function."""

    @patch("tif1.events.get_sessions")
    def test_get_session(self, mock_get_sessions):
        """Test get_session returns Session object."""
        mock_get_sessions.return_value = [
            "Practice 1",
            "Practice 2",
            "Practice 3",
            "Qualifying",
            "Race",
        ]
        session = get_session(2025, "Test GP", "Race")
        assert isinstance(session, Session)
        assert session.year == 2025

    @patch("tif1.events.get_sessions")
    def test_get_session_with_backend(self, mock_get_sessions):
        """Test get_session with lib parameter."""
        mock_get_sessions.return_value = [
            "Practice 1",
            "Practice 2",
            "Practice 3",
            "Qualifying",
            "Race",
        ]
        session = get_session(2025, "Test GP", "Race", lib="pandas")
        assert session.lib == "pandas"

    @patch("tif1.events.get_sessions")
    def test_get_session_with_cache_disabled(self, mock_get_sessions):
        """Test get_session with cache disabled."""
        mock_get_sessions.return_value = [
            "Practice 1",
            "Practice 2",
            "Practice 3",
            "Qualifying",
            "Race",
        ]
        session = get_session(2025, "Test GP", "Race", enable_cache=False)
        assert session.enable_cache is False

    def test_get_session_invalid_session_raises_error(self):
        """Test get_session raises ValueError for invalid session with helpful message."""
        with pytest.raises(ValueError, match=r"Session 'Sprint' does not exist") as exc_info:
            get_session(2024, "Bahrain", "S")

        error_message = str(exc_info.value)
        assert "2024 Bahrain Grand Prix" in error_message
        assert "Available sessions:" in error_message
        assert "Practice 1" in error_message
        assert "Practice 1" in error_message

    def test_get_session_invalid_session_shows_available_sessions(self):
        """Test get_session error message includes all available sessions."""
        with pytest.raises(ValueError, match=r"Session .* does not exist") as exc_info:
            get_session(2025, "Bahrain", "Sprint")

        error_message = str(exc_info.value)
        # Should show all standard sessions for Bahrain (no Sprint)
        assert "Practice 1" in error_message
        assert "Practice 2" in error_message
        assert "Practice 3" in error_message
        assert "Qualifying" in error_message
        assert "Race" in error_message


class TestSessionAdvanced:
    """Advanced Session tests."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_fetch_json_network_error(self, mock_fetch):
        """Test network error handling with retries."""
        mock_fetch.side_effect = NetworkError(reason="Network error")

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        with pytest.raises(NetworkError):
            session._fetch_json("test.json")

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_fetch_json_invalid_data(self, mock_fetch):
        """Test invalid data error."""
        mock_fetch.side_effect = InvalidDataError(reason="Expected dict, got list")

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        with pytest.raises(InvalidDataError):
            session._fetch_json("test.json")

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_property(self, mock_fetch):
        """Test drivers property."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        drivers = session.drivers
        assert len(drivers) == 1
        assert drivers[0] == "1"  # drivers property returns list of driver numbers
        # Test caching
        drivers2 = session.drivers
        assert drivers == drivers2

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_property_empty(self, mock_fetch):
        """Test drivers property with empty data."""
        mock_fetch.return_value = {"drivers": []}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        drivers = session.drivers
        assert len(drivers) == 0

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_laps_property_pandas(self, mock_fetch):
        """Test laps property with pandas lib."""

        def side_effect(path):
            if "drivers.json" in path:
                return {"drivers": [{"driver": "VER", "team": "Red Bull"}]}
            return {"time": [90.5], "lap": [1], "compound": ["SOFT"], "status": ["Valid"]}

        mock_fetch.side_effect = side_effect

        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        laps = session.laps
        assert isinstance(laps, pd.DataFrame)
        assert "Driver" in laps.columns
        assert "LapTime" in laps.columns
        assert "LapNumber" in laps.columns

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_laps_property_error_handling(self, mock_fetch):
        """Test laps property with error handling."""
        call_count = [0]

        def side_effect(path):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"drivers": [{"driver": "VER", "team": "Red Bull"}]}
            raise InvalidDataError(reason="Failed to load laps")

        mock_fetch.side_effect = side_effect

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        laps = session.laps
        assert isinstance(laps, pd.DataFrame)

    @patch("tif1.core.fetch_multiple_async")
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_laps_async(self, mock_fetch, mock_fetch_async):
        """Test async laps loading."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "team": "Red Bull"}]}
        mock_fetch_async.return_value = [
            {"time": [90.5], "lap": [1], "compound": ["SOFT"], "status": ["Valid"]}
        ]

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        laps = asyncio.run(session.laps_async())
        assert isinstance(laps, pd.DataFrame)

    @patch("tif1.core.fetch_multiple_async")
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_laps_async_polars_relaxed_concat(self, mock_fetch, mock_fetch_async):
        """Polars laps async should tolerate mixed source dtypes across drivers."""
        pl = pytest.importorskip("polars")

        mock_fetch.return_value = {
            "drivers": [
                {"driver": "VER", "team": "Red Bull"},
                {"driver": "HAM", "team": "Mercedes"},
            ]
        }
        mock_fetch_async.return_value = [
            {"time": [90.5], "lap": [1], "compound": ["SOFT"], "status": ["Valid"]},
            {"time": ["None"], "lap": [1], "compound": ["MEDIUM"], "status": ["Valid"]},
        ]

        clear_lap_cache()
        try:
            session = Session(2025, "Test GP", "Race", enable_cache=False, lib="polars")
            laps = asyncio.run(session.laps_async())
            assert isinstance(laps, pl.DataFrame)
            assert len(laps) == 2
            assert "LapTime" in laps.columns
        finally:
            clear_lap_cache()

    @patch("tif1.core.fetch_multiple_async")
    def test_laps_async_reuses_local_payloads_before_fetch(self, mock_fetch_multiple):
        """laps_async should skip async requests for already memoized laptime payloads."""

        async def fake_fetch_multiple_async(requests, **_kwargs):
            assert len(requests) == 1
            assert requests[0][3] == "session_laptimes.json"
            return [
                {
                    "drv": ["HAM"],
                    "team": ["Mercedes"],
                    "time": [91.0],
                    "lap": [1],
                    "compound": ["MEDIUM"],
                    "status": ["Valid"],
                }
            ]

        mock_fetch_multiple.side_effect = fake_fetch_multiple_async
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [
            {"driver": "VER", "team": "Red Bull"},
            {"driver": "HAM", "team": "Mercedes"},
        ]
        session._remember_local_payload(
            "VER/laptimes.json",
            {"time": [90.5], "lap": [1], "compound": ["SOFT"], "status": ["Valid"]},
        )

        laps = asyncio.run(session.laps_async())

        assert isinstance(laps, pd.DataFrame)
        assert set(laps["Driver"]) == {"VER", "HAM"}
        assert mock_fetch_multiple.call_count == 1

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_get_fastest_laps_pandas(self, mock_fetch):
        """Test get_fastest_laps with pandas."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "team": "Red Bull"}]}

        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame(
            {"Driver": ["VER", "VER"], "LapTime": [90.5, 91.0], "LapNumber": [1, 2]}
        )

        fastest = session.get_fastest_laps(by_driver=True)
        assert len(fastest) == 1

        overall = session.get_fastest_laps(by_driver=False)
        assert len(overall) == 1

    def test_get_fastest_laps_empty(self):
        """Test get_fastest_laps with no valid laps."""
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame({"Driver": [], "LapTime": []})

        fastest = session.get_fastest_laps(by_driver=True)
        assert len(fastest) == 0

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_get_driver(self, mock_fetch):
        """Test get_driver method."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "team": "Red Bull", "number": 1}]}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver = session.get_driver("VER")
        assert isinstance(driver, Driver)
        assert driver.driver == "VER"

    @patch("tif1.core.fetch_multiple_async")
    def test_get_fastest_laps_tels_hydrates_fastest_lap_cache(self, mock_fetch_multiple):
        """Bulk fastest telemetry should hydrate the single fastest-lap telemetry cache."""

        async def fake_fetch_multiple_async(requests, **_kwargs):
            payloads = []
            for _year, _gp, _session, path in requests:
                _driver, lap_file = path.split("/", maxsplit=1)
                lap_num = int(lap_file.split("_", maxsplit=1)[0])
                payloads.append(
                    {"tel": {"time": [0.0, 1.0], "speed": [100, 110], "lap": [lap_num, lap_num]}}
                )
            return payloads

        mock_fetch_multiple.side_effect = fake_fetch_multiple_async
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER", "HAM"],
                "LapTime": [89.4, 90.1],
                "LapNumber": [2, 1],
                "Team": ["Red Bull", "Mercedes"],
            }
        )

        bulk_tels = session.get_fastest_laps_tels(by_driver=True)

        assert isinstance(bulk_tels, pd.DataFrame)
        assert not bulk_tels.empty
        assert session._fastest_lap_tel_ref == ("VER", 2)

        with patch.object(
            session, "_get_telemetry_df_for_ref", side_effect=AssertionError("unexpected fetch")
        ):
            fastest_tel = session.get_fastest_lap_tel()

        assert isinstance(fastest_tel, pd.DataFrame)
        assert not fastest_tel.empty
        assert (fastest_tel["Driver"] == "VER").all()
        assert (fastest_tel["LapNumber"] == 2).all()

    @patch("tif1.core.fetch_multiple_async")
    def test_get_driver_prefetches_laps_for_cold_cached_session(self, mock_fetch_multiple):
        """Cold cached lookup should prefetch laps and reuse payload in Driver.laps."""

        async def fake_fetch_multiple_async(requests, **_kwargs):
            assert requests[0][3] == "drivers.json"
            assert requests[1][3] == "session_laptimes.json"
            return [
                {"drivers": [{"driver": "VER", "team": "Red Bull", "number": 1}]},
                {
                    "drv": ["VER"],
                    "team": ["Red Bull"],
                    "time": [90.5],
                    "lap": [1],
                    "compound": ["SOFT"],
                    "status": ["Valid"],
                },
            ]

        mock_fetch_multiple.side_effect = fake_fetch_multiple_async
        session = Session(2025, "Test GP", "Race", enable_cache=True)

        driver = session.get_driver("VER")
        with patch.object(session, "_fetch_json", side_effect=AssertionError("unexpected fetch")):
            laps = driver.laps

        assert isinstance(driver, Driver)
        assert isinstance(laps, pd.DataFrame)
        assert not laps.empty
        assert mock_fetch_multiple.call_count == 1

    @patch("tif1.core.fetch_multiple_async")
    def test_get_driver_prefetches_laps_with_cache_disabled(self, mock_fetch_multiple):
        """Driver prefetch should run even when persistent cache is disabled."""

        async def fake_fetch_multiple_async(requests, **kwargs):
            assert requests[0][3] == "drivers.json"
            assert requests[1][3] == "session_laptimes.json"
            assert kwargs.get("use_cache") is False
            assert kwargs.get("write_cache") is False
            return [
                {"drivers": [{"driver": "VER", "team": "Red Bull", "number": 1}]},
                {
                    "drv": ["VER"],
                    "team": ["Red Bull"],
                    "time": [90.5],
                    "lap": [1],
                    "compound": ["SOFT"],
                    "status": ["Valid"],
                },
            ]

        mock_fetch_multiple.side_effect = fake_fetch_multiple_async
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        driver = session.get_driver("VER")
        with patch.object(session, "_fetch_json", side_effect=AssertionError("unexpected fetch")):
            laps = driver.laps

        assert isinstance(driver, Driver)
        assert isinstance(laps, pd.DataFrame)
        assert not laps.empty
        assert mock_fetch_multiple.call_count == 1

    def test_get_fastest_laps_async(self):
        """Async fastest-laps API should work from preloaded laps."""
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER", "HAM", "VER"],
                "LapTime": [90.5, 91.1, 89.9],
                "LapNumber": [1, 1, 2],
                "Team": ["Red Bull", "Mercedes", "Red Bull"],
            }
        )

        fastest = asyncio.run(session.get_fastest_laps_async(by_driver=False))

        assert isinstance(fastest, pd.DataFrame)
        assert len(fastest) == 1
        assert fastest.iloc[0]["Driver"] == "VER"

    @patch("tif1.core.fetch_multiple_async")
    def test_get_fastest_lap_tel_async(self, mock_fetch_multiple):
        """Async fastest-lap telemetry should fetch and memoize single-lap telemetry."""

        async def fake_fetch_multiple_async(requests, **_kwargs):
            assert len(requests) == 1
            assert requests[0][3] == "VER/7_tel.json"
            return [{"tel": {"time": [0.0, 1.0], "speed": [100, 120]}}]

        mock_fetch_multiple.side_effect = fake_fetch_multiple_async
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"driver": "VER", "team": "Red Bull"}]
        session._fastest_lap_ref = ("VER", 7)
        session._fastest_lap_ref_driver_source_id = id(session._drivers)

        tel = asyncio.run(session.get_fastest_lap_tel_async())

        assert isinstance(tel, pd.DataFrame)
        assert not tel.empty
        assert tel.iloc[0]["Driver"] == "VER"
        assert int(tel.iloc[0]["LapNumber"]) == 7
        assert mock_fetch_multiple.call_count == 1

    @patch("tif1.core.fetch_multiple_async")
    def test_get_fastest_laps_tels_async(self, mock_fetch_multiple):
        """Async bulk fastest telemetry API should fetch telemetry without sync event loops."""

        async def fake_fetch_multiple_async(requests, **_kwargs):
            assert len(requests) == 1
            assert requests[0][3] == "VER/2_tel.json"
            return [{"tel": {"time": [0.0, 1.0], "speed": [101, 123]}}]

        mock_fetch_multiple.side_effect = fake_fetch_multiple_async
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER"],
                "LapTime": [89.4],
                "LapNumber": [2],
                "Team": ["Red Bull"],
            }
        )

        tels = asyncio.run(session.get_fastest_laps_tels_async(by_driver=True))

        assert isinstance(tels, pd.DataFrame)
        assert not tels.empty
        assert tels.iloc[0]["Driver"] == "VER"
        assert int(tels.iloc[0]["LapNumber"]) == 2
        assert mock_fetch_multiple.call_count == 1

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_get_driver_not_found_when_drivers_not_preloaded(self, mock_fetch):
        """Test get_driver raises after lazy-loading drivers when code is missing."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "team": "Red Bull", "number": 1}]}

        session = Session(2025, "Test GP", "Race", enable_cache=False)

        with pytest.raises(DriverNotFoundError):
            session.get_driver("HAM")

    def test_get_driver_not_found_after_drivers_loaded(self):
        """Test get_driver raises when requested driver is not in loaded session drivers."""
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"driver": "VER", "team": "Red Bull"}]

        with pytest.raises(DriverNotFoundError):
            session.get_driver("HAM")

    def test_get_driver_found_after_drivers_loaded(self):
        """Test get_driver works when drivers are preloaded on the session."""
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"driver": "VER", "team": "Red Bull"}]

        driver = session.get_driver("VER")
        assert isinstance(driver, Driver)
        assert driver.driver == "VER"


class TestDriverAdvanced:
    """Advanced Driver tests."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_laps_property(self, mock_fetch):
        """Test driver laps property."""
        mock_fetch.return_value = {
            "time": [90.5],
            "lap": [1],
            "compound": ["SOFT"],
            "status": ["Valid"],
        }

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver = Driver(session, "VER")
        laps = driver.laps
        assert isinstance(laps, pd.DataFrame)

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_laps_property_reuses_loaded_session_laps(self, mock_fetch):
        """Driver.laps should reuse in-memory session laps when already available."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}
        session = Session(2025, "Test GP", "Race", enable_cache=True)
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER", "HAM", "VER"],
                "LapNumber": [1, 1, 2],
                "LapTime": [90.5, 91.1, 90.7],
                "Team": ["Red Bull", "Mercedes", "Red Bull"],
            }
        )

        driver = Driver(session, "VER")
        with patch.object(driver, "_load_laps", side_effect=AssertionError("unexpected fetch")):
            laps = driver.laps

        assert isinstance(laps, pd.DataFrame)
        assert not laps.empty
        assert set(laps["Driver"]) == {"VER"}

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_laps_property_error(self, mock_fetch):
        """Test driver laps property with error."""

        def side_effect(path):
            if path == "drivers.json":
                return {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}
            raise InvalidDataError(reason="Failed")

        mock_fetch.side_effect = side_effect

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver = Driver(session, "VER")
        laps = driver.laps
        assert isinstance(laps, pd.DataFrame)
        assert len(laps) == 0

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_get_fastest_lap(self, mock_fetch):
        """Test driver get_fastest_lap."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver = Driver(session, "VER")
        driver._laps = pd.DataFrame({"LapTime": [90.5, 91.0], "LapNumber": [1, 2]})

        fastest = driver.get_fastest_lap()
        assert len(fastest) == 1

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_get_lap(self, mock_fetch):
        """Test driver get_lap method."""
        mock_fetch.return_value = {
            "time": [90.5],
            "lap": [19],
            "compound": ["SOFT"],
            "status": ["Valid"],
        }

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver = Driver(session, "VER")
        lap = driver.get_lap(19)
        assert isinstance(lap, Lap)
        assert lap.lap_number == 19

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_get_lap_missing_raises_when_laps_loaded(self, mock_fetch):
        """Test get_lap raises when lap is not present in preloaded laps."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver = Driver(session, "VER")
        driver._laps = pd.DataFrame({"LapNumber": [1, 2, 3], "LapTime": [90.5, 91.0, 92.0]})

        with pytest.raises(LapNotFoundError):
            driver.get_lap(4)

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_get_lap_rebuilds_lookup_when_laps_replaced(self, mock_fetch):
        """Test get_lap lookup is rebuilt when _laps DataFrame is replaced."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver = Driver(session, "VER")
        driver._laps = pd.DataFrame({"LapNumber": [1]})
        assert driver.get_lap(1).lap_number == 1

        driver._laps = pd.DataFrame({"LapNumber": [2]})
        with pytest.raises(LapNotFoundError):
            driver.get_lap(1)
        assert driver.get_lap(2).lap_number == 2

    @patch("tif1.core.Driver._load_laps")
    def test_driver_laps_rebuilds_driver_info_index_when_drivers_change(self, mock_load_laps):
        """Test team lookup is refreshed when session driver list is replaced."""
        mock_load_laps.return_value = {"time": [90.5], "lap": [1]}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"driver": "VER", "team": "Alpha"}]

        first_driver = Driver(session, "VER")
        first_laps = first_driver.laps
        assert first_laps["Team"].iloc[0] == "Alpha"

        session._drivers = [{"driver": "VER", "team": "Beta"}]
        second_driver = Driver(session, "VER")
        second_laps = second_driver.laps
        assert second_laps["Team"].iloc[0] == "Beta"


class TestLapAdvanced:
    """Advanced Lap tests."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_telemetry_property(self, mock_fetch):
        """Test lap telemetry property."""
        mock_fetch.return_value = {"tel": {"time": [0, 1], "speed": [100, 200]}}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        lap = Lap({"Driver": "VER", "LapNumber": 19}, session=session)
        telemetry = lap.telemetry
        assert isinstance(telemetry, pd.DataFrame)
        assert "Speed" in telemetry.columns
        assert "Time" in telemetry.columns

    def test_telemetry_property_auto_ultra_cold_skips_cache_lookup(self, monkeypatch):
        """Cold telemetry path should skip cache lookup and use unvalidated fetch."""
        session = Session(2025, "Test GP", "Race", enable_cache=True)
        lap = Lap({"Driver": "VER", "LapNumber": 19}, session=session)

        class _CacheProbe:
            def __init__(self):
                self.calls = 0

            def get_telemetry(self, *_args, **_kwargs):
                self.calls += 1

        cache_probe = _CacheProbe()
        monkeypatch.setattr("tif1.core.get_cache", lambda: cache_probe)
        monkeypatch.setattr(session, "_resolve_ultra_cold_mode", lambda _value: True)
        monkeypatch.setattr(
            session,
            "_fetch_json_unvalidated",
            lambda _path: {"tel": {"time": [0, 1], "speed": [100, 200]}},
        )

        telemetry = lap.telemetry

        assert isinstance(telemetry, pd.DataFrame)
        assert not telemetry.empty
        assert cache_probe.calls == 0

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_telemetry_property_empty(self, mock_fetch):
        """Test lap telemetry property with empty data."""
        mock_fetch.return_value = {"tel": {}}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        lap = Lap({"Driver": "VER", "LapNumber": 19}, session=session)
        telemetry = lap.telemetry
        assert isinstance(telemetry, pd.DataFrame)

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_telemetry_property_error(self, mock_fetch):
        """Test lap telemetry property with error."""
        mock_fetch.side_effect = InvalidDataError(reason="Failed")

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        lap = Lap({"Driver": "VER", "LapNumber": 19}, session=session)
        telemetry = lap.telemetry
        assert isinstance(telemetry, pd.DataFrame)
        assert len(telemetry) == 0


class TestCircuitInfo:
    """Tests for Session.get_circuit_info() and the CircuitInfo dataclass."""

    _SAMPLE_PAYLOAD: ClassVar[dict] = {
        "CornerNumber": [1, 2, 3],
        "X": [-3063.956, -6314.695, -8209.921],
        "Y": [-2208.506, 3218.050, 5460.177],
        "Angle": [-115.780, 32.816, 147.539],
        "Distance": [422.995, 1068.073, 1361.295],
        "Rotation": 1.0,
    }

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_circuit_info_basic(self, mock_fetch):
        """Corners DataFrame has correct columns, dtypes and rotation."""
        from tif1.core import CircuitInfo

        mock_fetch.return_value = self._SAMPLE_PAYLOAD

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        ci = session.get_circuit_info()

        assert isinstance(ci, CircuitInfo)
        assert isinstance(ci.corners, pd.DataFrame)
        assert list(ci.corners.columns) == ["X", "Y", "Number", "Letter", "Angle", "Distance"]
        assert len(ci.corners) == 3
        # Dtypes
        assert ci.corners["X"].dtype == "float64"
        assert ci.corners["Y"].dtype == "float64"
        assert ci.corners["Number"].dtype == "int64"
        assert ci.corners["Angle"].dtype == "float64"
        assert ci.corners["Distance"].dtype == "float64"
        # Values
        assert ci.corners["Number"].tolist() == [1, 2, 3]
        assert ci.rotation == 1.0

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_circuit_info_none_values(self, mock_fetch):
        """'None' string sentinels are coerced to NaN for floats."""
        import math

        mock_fetch.return_value = {
            "CornerNumber": [1, 2],
            "X": [-3063.9, "None"],
            "Y": ["None", 3218.1],
            "Angle": [None, 32.8],
            "Distance": ["None", "None"],
            "Rotation": "None",
        }

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        ci = session.get_circuit_info()

        assert math.isnan(ci.corners["X"].iloc[1])
        assert math.isnan(ci.corners["Y"].iloc[0])
        assert math.isnan(ci.corners["Angle"].iloc[0])
        assert math.isnan(ci.corners["Distance"].iloc[0])
        assert math.isnan(ci.corners["Distance"].iloc[1])
        assert ci.rotation == 0.0  # "None" → 0.0

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_circuit_info_not_found(self, mock_fetch):
        """DataNotFoundError yields empty DataFrames and rotation=0.0."""
        mock_fetch.side_effect = DataNotFoundError(year=2025, event="Test GP", session="Race")

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        ci = session.get_circuit_info()

        assert isinstance(ci.corners, pd.DataFrame)
        assert len(ci.corners) == 0
        assert list(ci.corners.columns) == ["X", "Y", "Number", "Letter", "Angle", "Distance"]
        assert ci.rotation == 0.0

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_circuit_info_caching(self, mock_fetch):
        """get_circuit_info() returns cached result on second call."""
        mock_fetch.return_value = self._SAMPLE_PAYLOAD

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        ci1 = session.get_circuit_info()
        ci2 = session.get_circuit_info()

        assert ci1 is ci2  # same object
        assert mock_fetch.call_count == 1  # fetched only once

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_circuit_info_marshal_dfs_empty(self, mock_fetch):
        """marshal_lights and marshal_sectors are always empty with correct columns."""
        mock_fetch.return_value = self._SAMPLE_PAYLOAD

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        ci = session.get_circuit_info()

        _expected_cols = ["X", "Y", "Number", "Letter", "Angle", "Distance"]
        assert list(ci.marshal_lights.columns) == _expected_cols
        assert len(ci.marshal_lights) == 0
        assert list(ci.marshal_sectors.columns) == _expected_cols
        assert len(ci.marshal_sectors) == 0

    def test_circuit_info_exported(self):
        """CircuitInfo is importable from the top-level tif1 package."""
        import tif1

        assert hasattr(tif1, "CircuitInfo")
        from tif1 import CircuitInfo  # noqa: F401

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_circuit_info_empty_payload(self, mock_fetch):
        """An empty dict payload returns empty corners with rotation=0.0."""
        mock_fetch.return_value = {}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        ci = session.get_circuit_info()

        assert len(ci.corners) == 0
        assert ci.rotation == 0.0

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_circuit_info_letter_column_empty_string(self, mock_fetch):
        """Letter column is always an empty string (not present in our data source)."""
        mock_fetch.return_value = self._SAMPLE_PAYLOAD

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        ci = session.get_circuit_info()

        assert (ci.corners["Letter"] == "").all()
