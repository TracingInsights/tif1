"""Additional core.py coverage tests - Session methods and Driver/Lap paths."""

from unittest.mock import patch

import pandas as pd
import pytest

from tif1.core import (
    Driver,
    Lap,
    Session,
    _ensure_nested_loop_support,
    _validate_json_payload,
)
from tif1.exceptions import DataNotFoundError, InvalidDataError


class TestEnsureNestedLoopSupport:
    """Test _ensure_nested_loop_support."""

    def test_no_running_loop(self):
        _ensure_nested_loop_support("test")  # Should not raise


class TestValidateJsonPayloadDrivers:
    """Test _validate_json_payload with drivers.json."""

    @patch("tif1.core.config")
    def test_drivers_json_with_validation_enabled(self, mock_config):
        mock_config.get.return_value = True
        data = {
            "drivers": [
                {
                    "driver": "VER",
                    "team": "Red Bull",
                    "dn": "1",
                    "fn": "Max",
                    "ln": "Verstappen",
                    "tc": "#3671C6",
                    "url": "https://example.com/ver.png",
                }
            ]
        }
        result = _validate_json_payload("drivers.json", data)
        assert isinstance(result, dict)
        assert result["drivers"][0]["dn"] == "1"
        assert result["drivers"][0]["fn"] == "Max"
        assert result["drivers"][0]["ln"] == "Verstappen"
        assert result["drivers"][0]["tc"] == "#3671C6"
        assert result["drivers"][0]["url"] == "https://example.com/ver.png"

    @patch("tif1.core.config")
    def test_laptimes_json_with_validation_enabled(self, mock_config):
        mock_config.get.return_value = True
        data = {
            "time": [90.5],
            "lap": [1],
            "compound": ["SOFT"],
            "stint": [1],
            "s1": [30.0],
            "s2": [30.0],
            "s3": [30.5],
            "life": [1],
            "pos": [1],
            "status": ["Valid"],
            "pb": [True],
            "sesT": [100.0],
            "drv": ["VER"],
            "dNum": ["1"],
            "vi1": [280.0],
            "vi2": [290.0],
            "vfl": [300.0],
            "vst": [305.0],
            "fresh": [True],
            "team": ["Red Bull"],
            "lST": [0.0],
            "lSD": ["2025-03-01T10:00:00"],
            "del": [False],
            "delR": [None],
            "ff1G": [False],
            "iacc": [True],
            "wT": [100.0],
            "wAT": [25.1],
            "wH": [40.0],
            "wP": [1012.0],
            "wR": [False],
            "wTT": [32.5],
            "wWD": [180.0],
            "wWS": [2.2],
        }
        result = _validate_json_payload("VER/laptimes.json", data)
        assert isinstance(result, dict)
        assert result["session_time"] == [100.0]
        assert result["source_driver"] == ["VER"]
        assert result["driver_number"] == ["1"]
        assert result["speed_i1"] == [280.0]
        assert result["source_team"] == ["Red Bull"]
        assert result["weather_time"] == [100.0]
        assert result["air_temp"] == [25.1]
        assert result["track_temp"] == [32.5]
        assert result["wind_direction"] == [180.0]
        assert result["wind_speed"] == [2.2]
        assert "sesT" not in result
        assert "drv" not in result
        assert "team" not in result
        assert "wAT" not in result

    @patch("tif1.core.config")
    def test_telemetry_json_with_tel_key(self, mock_config):
        mock_config.get.return_value = True
        data = {
            "tel": {
                "speed": [100, 200],
                "time": [0, 1],
                "DriverAhead": ["VER", "VER"],
                "DistanceToDriverAhead": [12.0, 11.6],
                "dataKey": ["k1", "k2"],
            }
        }
        result = _validate_json_payload("VER/1_tel.json", data)
        assert isinstance(result, dict)
        assert isinstance(result.get("tel"), dict)
        tel = result["tel"]
        assert tel["driver_ahead"] == ["VER", "VER"]
        assert tel["distance_to_driver_ahead"] == [12.0, 11.6]
        assert tel["data_key"] == ["k1", "k2"]
        assert "DriverAhead" not in tel
        assert "DistanceToDriverAhead" not in tel
        assert "dataKey" not in tel

    @patch("tif1.core.config")
    def test_telemetry_json_without_tel_key(self, mock_config):
        mock_config.get.return_value = True
        data = {
            "speed": [100, 200],
            "time": [0, 1],
            "DriverAhead": ["VER", "VER"],
            "DistanceToDriverAhead": [12.0, 11.6],
            "dataKey": ["k1", "k2"],
        }
        result = _validate_json_payload("VER/1_tel.json", data)
        assert isinstance(result, dict)
        assert result["driver_ahead"] == ["VER", "VER"]
        assert result["distance_to_driver_ahead"] == [12.0, 11.6]
        assert result["data_key"] == ["k1", "k2"]
        assert "DriverAhead" not in result
        assert "DistanceToDriverAhead" not in result
        assert "dataKey" not in result

    @patch("tif1.core.config")
    def test_race_control_json_with_validation_enabled(self, mock_config):
        mock_config.get.return_value = True
        data = {
            "time": [10.0, 20.0],
            "cat": ["Other", "Track"],
            "msg": ["Incident", "Green flag"],
            "status": [None, "Clear"],
            "flag": [None, "GREEN"],
            "scope": [None, "Sector"],
            "sector": [None, 2],
            "dNum": [None, "1"],
            "lap": [5, 6],
        }
        result = _validate_json_payload("rcm.json", data)
        assert isinstance(result, dict)
        assert result["category"] == ["Other", "Track"]
        assert result["message"] == ["Incident", "Green flag"]
        assert result["racing_number"] == [None, "1"]
        assert "cat" not in result
        assert "msg" not in result
        assert "dNum" not in result

    @patch("tif1.core.config")
    def test_weather_json_with_validation_enabled(self, mock_config):
        mock_config.get.return_value = True
        data = {
            "wT": [10.0, 20.0],
            "wAT": [25.0, 25.2],
            "wH": [40.0, 41.0],
            "wP": [1012.0, 1011.8],
            "wR": [False, False],
            "wTT": [30.0, 30.2],
            "wWD": [180.0, 181.0],
            "wWS": [2.2, 2.1],
        }
        result = _validate_json_payload("weather.json", data)
        assert isinstance(result, dict)
        assert result["time"] == [10.0, 20.0]
        assert result["air_temp"] == [25.0, 25.2]
        assert result["wind_direction"] == [180, 181]
        assert result["rainfall"] == [False, False]
        assert "wT" not in result
        assert "wAT" not in result

    @patch("tif1.core.config")
    def test_weather_json_pascalcase_keys(self, mock_config):
        """Real CDN weather.json uses PascalCase keys — these must be accepted."""
        mock_config.get.return_value = True
        data = {
            "Time": [53.837, 113.843],
            "AirTemp": [27.1, 27.1],
            "Humidity": [48.0, 48.0],
            "Pressure": [1017.1, 1017.1],
            "Rainfall": [False, False],
            "TrackTemp": [34.4, 34.4],
            "WindDirection": [122, 261],
            "WindSpeed": [1.0, 0.7],
        }
        result = _validate_json_payload("weather.json", data)
        assert isinstance(result, dict)
        assert result["time"] == [53.837, 113.843]
        assert result["air_temp"] == [27.1, 27.1]
        assert result["wind_direction"] == [122, 261]
        assert result["rainfall"] == [False, False]
        assert "Time" not in result
        assert "AirTemp" not in result

    @patch("tif1.core.config")
    def test_weather_json_none_strings_coerced(self, mock_config):
        """Missing values encoded as the string 'None' are converted to Python None."""
        mock_config.get.return_value = True
        data = {
            "Time": [53.837, 113.843],
            "AirTemp": [27.1, "None"],
            "Humidity": [48.0, "None"],
            "Pressure": [1017.1, "None"],
            "Rainfall": [False, "None"],
            "TrackTemp": [34.4, "None"],
            "WindDirection": [122, "None"],
            "WindSpeed": [1.0, "None"],
        }
        result = _validate_json_payload("weather.json", data)
        assert result["air_temp"] == [27.1, None]
        assert result["humidity"] == [48.0, None]
        assert result["rainfall"] == [False, None]
        assert result["wind_direction"] == [122, None]


class TestSessionFetchJson:
    """Test Session._fetch_json with various scenarios."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_fetch_json_caches_result_in_local_payloads(self, mock_fetch):
        mock_fetch.return_value = {"drivers": [{"driver": "VER"}]}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._fetch_json("drivers.json")
        assert session._get_local_payload("drivers.json") is not None

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_fetch_json_uses_local_payload(self, mock_fetch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._remember_local_payload("drivers.json", {"drivers": []})
        result = session._fetch_json("drivers.json")
        assert result == {"drivers": []}
        mock_fetch.assert_not_called()


class TestSessionDriversProperty:
    """Test Session.drivers property edge cases."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_empty_list(self, mock_fetch):
        mock_fetch.return_value = {}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        drivers = session.drivers
        assert drivers == []

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_cached_on_second_access(self, mock_fetch):
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        d1 = session.drivers
        d2 = session.drivers
        # Drivers property returns a new list each time, but with same content
        assert d1 == d2
        # Verify fetch was only called once (data is cached internally)
        assert mock_fetch.call_count == 1


class TestSessionRaceControlAndWeather:
    """Test session-level race control and weather data accessors."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_race_control_messages_property(self, mock_fetch):
        mock_fetch.side_effect = lambda path: (
            {
                "time": [
                    "2025-06-29T12:20:01.000000000",
                    "2025-06-29T12:20:26.000000000",
                    "2025-06-29T12:20:27.000000000",
                ],
                "cat": ["Flag", "Flag", "Other"],
                "msg": [
                    "GREEN LIGHT - PIT EXIT OPEN",
                    "YELLOW IN TRACK SECTOR 3",
                    "DRS DISABLED IN ZONE 1",
                ],
                "status": ["None", "DISABLED", "DEPLOYED"],
                "flag": ["GREEN", "YELLOW", "CLEAR"],
                "scope": ["Track", "Sector", "Sector"],
                "sector": ["None", 3.0, 3.0],
                "dNum": ["None", "43", "1"],
                "lap": [1, 1, 1],
            }
            if path == "rcm.json"
            else {}
        )

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        result = session.race_control_messages

        assert isinstance(result, pd.DataFrame)
        assert "Time" in result.columns
        assert "Category" in result.columns
        assert "Message" in result.columns
        assert "RacingNumber" in result.columns

        # Verify final dtypes
        assert result["Time"].dtype == "datetime64[ns]", (
            f"Expected datetime64[ns], got {result['Time'].dtype}"
        )
        assert result["Category"].dtype == object
        assert result["Message"].dtype == object
        assert result["Status"].dtype == object
        assert result["Flag"].dtype == object
        assert result["Scope"].dtype == object
        assert result["Sector"].dtype == "float64", (
            f"Expected float64, got {result['Sector'].dtype}"
        )
        assert result["Lap"].dtype == "int64", f"Expected int64, got {result['Lap'].dtype}"

        # Verify "None" sentinel was properly converted
        assert result["Status"].iloc[0] is None
        assert result["RacingNumber"].iloc[0] is None
        import math

        assert math.isnan(result["Sector"].iloc[0])

        # Verify non-None values are preserved
        assert result["Category"].iloc[0] == "Flag"
        assert result["RacingNumber"].iloc[1] == "43"

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_weather_property(self, mock_fetch):
        mock_fetch.side_effect = lambda path: (
            {
                "wT": [10.0],
                "wAT": [25.0],
                "wH": [40.0],
                "wP": [1012.0],
                "wR": [False],
                "wTT": [30.0],
                "wWD": [180.0],
                "wWS": [2.2],
            }
            if path == "weather.json"
            else {}
        )

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        result = session.weather

        assert isinstance(result, pd.DataFrame)
        assert "Time" in result.columns
        assert "AirTemp" in result.columns
        assert "Rainfall" in result.columns
        assert "TrackTemp" in result.columns
        assert "WindDirection" in result.columns
        assert result["AirTemp"].iloc[0] == 25.0


class TestSessionLapsProperty:
    """Test Session.laps property."""

    @patch("tif1.core.fetch_multiple_async")
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_laps_property_caches_globally(self, mock_fetch, mock_async):
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "team": "Red Bull"}]}
        mock_async.return_value = [
            {"time": [90.5], "lap": [1], "compound": ["SOFT"], "status": ["Valid"]}
        ]
        session = Session(2025, "TestCache GP", "Race", enable_cache=False)
        laps1 = session.laps
        laps2 = session.laps
        assert laps1 is laps2


class TestSessionGetFastestLapFromRaw:
    """Test _get_fastest_laps_from_raw."""

    @patch("tif1.core.fetch_multiple_async")
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_fastest_laps_from_raw_by_driver(self, mock_fetch, mock_async):
        mock_fetch.return_value = {
            "drivers": [
                {"driver": "VER", "team": "Red Bull"},
                {"driver": "HAM", "team": "Mercedes"},
            ]
        }
        mock_async.return_value = [
            {"time": [90.5, 89.0], "lap": [1, 2]},
            {"time": [91.0, 90.0], "lap": [1, 2]},
        ]
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        result = session._get_fastest_laps_from_raw(by_driver=True)
        assert isinstance(result, pd.DataFrame)

    @patch("tif1.core.fetch_multiple_async")
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_fastest_laps_from_raw_overall(self, mock_fetch, mock_async):
        mock_fetch.return_value = {
            "drivers": [
                {"driver": "VER", "team": "Red Bull"},
                {"driver": "HAM", "team": "Mercedes"},
            ]
        }
        mock_async.return_value = [
            {"time": [90.5, 89.0], "lap": [1, 2]},
            {"time": [91.0, 90.0], "lap": [1, 2]},
        ]
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        result = session._get_fastest_laps_from_raw(by_driver=False)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1


class TestSessionFetchTelemetryBatch:
    """Test _fetch_telemetry_batch."""

    def test_with_memoized_telemetry(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._remember_telemetry_payload("VER", 1, {"speed": [100, 200], "time": [0, 1]})
        fastest_laps = pd.DataFrame({"Driver": ["VER"], "LapNumber": [1]})
        requests, _lap_info, tels = session._fetch_telemetry_batch(fastest_laps)
        assert len(tels) == 1
        assert len(requests) == 0

    def test_with_no_memoized_telemetry(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        fastest_laps = pd.DataFrame({"Driver": ["VER"], "LapNumber": [1]})
        requests, lap_info, tels = session._fetch_telemetry_batch(fastest_laps)
        assert len(requests) == 1
        assert len(tels) == 0
        assert lap_info[0] == ("VER", 1)


class TestSessionProcessTelemetryResults:
    """Test _process_telemetry_results."""

    def test_processes_valid_results(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        results = [{"tel": {"speed": [100, 200], "time": [0, 1]}}]
        lap_info = [("VER", 1)]
        tels = session._process_telemetry_results(results, lap_info, [])
        assert len(tels) == 1

    def test_skips_non_dict_results(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        results = [None, "invalid"]
        lap_info = [("VER", 1), ("HAM", 1)]
        tels = session._process_telemetry_results(results, lap_info, [])
        assert len(tels) == 0

    def test_skips_empty_tel_payload(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        results = [{"tel": {}}, {"tel": None}]
        lap_info = [("VER", 1), ("HAM", 1)]
        tels = session._process_telemetry_results(results, lap_info, [])
        assert len(tels) == 0


class TestSessionGetFastestLapTel:
    """Test get_fastest_lap_tel."""

    @patch("tif1.core.fetch_multiple_async")
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_returns_empty_when_no_drivers(self, mock_fetch, mock_async):
        mock_fetch.return_value = {"drivers": []}
        mock_async.return_value = []
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        result = session.get_fastest_lap_tel()
        assert isinstance(result, pd.DataFrame)

    def test_returns_cached_result(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        cached_df = pd.DataFrame({"Speed": [100, 200]})
        session._fastest_lap_tel_ref = ("VER", 1)
        session._fastest_lap_tel_df = cached_df
        session._fastest_lap_ref = ("VER", 1)
        session._fastest_lap_ref_laps_source_id = None
        session._fastest_lap_ref_driver_source_id = None

        with patch.object(session, "_get_fastest_lap_reference", return_value=("VER", 1)):
            result = session.get_fastest_lap_tel()
        assert result is cached_df


class TestSessionExtractFastestLapFromLoadedLaps:
    """Test _extract_fastest_lap_from_loaded_laps."""

    def test_with_valid_laps(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER", "HAM"],
                "LapTime": [89.5, 91.0],
                "LapNumber": [1, 1],
            }
        )
        result = session._extract_fastest_lap_from_loaded_laps()
        assert result is not None
        assert result == ("VER", 1)

    def test_with_empty_laps(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame({"Driver": [], "LapTime": []})
        result = session._extract_fastest_lap_from_loaded_laps()
        assert result is None


class TestSessionFetchJsonUnvalidated:
    """Test _fetch_json_unvalidated."""

    def test_returns_local_payload_if_available(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._remember_local_payload("drivers.json", {"drivers": []})
        result = session._fetch_json_unvalidated("drivers.json")
        assert result == {"drivers": []}

    @patch("tif1.core.Session._fetch_from_cdn_fast")
    def test_fetches_from_cdn_fast(self, mock_fetch):
        mock_fetch.return_value = {"drivers": [{"driver": "VER"}]}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        result = session._fetch_json_unvalidated("drivers.json")
        assert result == {"drivers": [{"driver": "VER"}]}

    @patch("tif1.core.Session._fetch_from_cdn_fast")
    def test_raises_on_non_dict_result(self, mock_fetch):
        mock_fetch.return_value = "not a dict"
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        with pytest.raises(InvalidDataError):
            session._fetch_json_unvalidated("drivers.json")


class TestSessionScheduleBackgroundCacheFill:
    """Test _schedule_background_cache_fill."""

    def test_skips_when_cache_disabled(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._schedule_background_cache_fill(json_payloads=[("drivers.json", {"drivers": []})])

    def test_skips_when_empty_payloads(self):
        session = Session(2025, "Test GP", "Race", enable_cache=True)
        session._schedule_background_cache_fill()


class TestDriverLapsProperty:
    """Test Driver.laps property paths."""

    def test_uses_prefetched_lap_data(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"driver": "VER", "team": "Red Bull"}]
        prefetched = {"time": [90.5], "lap": [1], "compound": ["SOFT"]}
        driver = Driver(session, "VER", prefetched_lap_data=prefetched)
        laps = driver.laps
        assert isinstance(laps, pd.DataFrame)
        assert not laps.empty
        assert laps["Driver"].iloc[0] == "VER"

    def test_data_not_found_returns_empty(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"driver": "VER", "team": "Red Bull"}]
        driver = Driver(session, "VER")
        with patch.object(session, "_fetch_from_cdn", side_effect=DataNotFoundError(year=2025)):
            laps = driver.laps
        assert isinstance(laps, pd.DataFrame)
        assert laps.empty


class TestDriverLoadLaps:
    """Test Driver._load_laps."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_load_laps_standard(self, mock_fetch):
        mock_fetch.return_value = {"time": [90.5], "lap": [1]}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver = Driver(session, "VER")
        result = driver._load_laps()
        assert isinstance(result, dict)


class TestLapFetchTelemetry:
    """Test Lap._fetch_telemetry."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_fetch_telemetry_standard(self, mock_fetch):
        mock_fetch.return_value = {"tel": {"speed": [100], "time": [0]}}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        lap = Lap({"Driver": "VER", "LapNumber": 1}, session=session)
        result = lap._fetch_telemetry()
        assert isinstance(result, dict)
        assert "speed" in result

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_fetch_telemetry_non_dict_tel(self, mock_fetch):
        mock_fetch.return_value = {"tel": "invalid"}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        lap = Lap({"Driver": "VER", "LapNumber": 1}, session=session)
        result = lap._fetch_telemetry()
        assert result == {}


class TestLapTelemetryProperty:
    """Test Lap.telemetry property edge cases."""

    def test_uses_memoized_telemetry(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._remember_telemetry_payload("VER", 1, {"speed": [100], "time": [0]})
        lap = Lap({"Driver": "VER", "LapNumber": 1}, session=session)
        telemetry = lap.telemetry
        assert isinstance(telemetry, pd.DataFrame)
        assert not telemetry.empty


class TestSessionGetFastestLapsWithLoadedLaps:
    """Test get_fastest_laps when _laps is already loaded."""

    def test_overall_fastest_with_loaded_laps(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER", "HAM", "VER"],
                "LapTime": [90.5, 91.0, 89.0],
                "LapNumber": [1, 1, 2],
                "Team": ["Red Bull", "Mercedes", "Red Bull"],
            }
        )
        fastest = session.get_fastest_laps(by_driver=False)
        assert len(fastest) == 1
        assert fastest.iloc[0]["LapTime"] == pd.to_timedelta(89.0, unit="s")
        assert fastest.iloc[0]["LapTimeSeconds"] == 89.0

    def test_all_invalid_laptimes_returns_empty(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER"],
                "LapTime": ["None"],
                "LapNumber": [1],
            }
        )
        fastest = session.get_fastest_laps(by_driver=True)
        assert len(fastest) == 0


class TestGetFastestLapReference:
    """Test _get_fastest_lap_reference."""

    def test_with_loaded_laps(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER", "HAM"],
                "LapTime": [89.0, 91.0],
                "LapNumber": [1, 1],
            }
        )
        ref = session._get_fastest_lap_reference()
        assert ref == ("VER", 1)

    def test_with_loaded_laps_memoized(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        laps = pd.DataFrame(
            {
                "Driver": ["VER"],
                "LapTime": [89.0],
                "LapNumber": [1],
            }
        )
        session._laps = laps
        ref1 = session._get_fastest_lap_reference()
        ref2 = session._get_fastest_lap_reference()
        assert ref1 == ref2


class TestSessionDriversDf:
    """Test Session.drivers_df property with all columns."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_df_all_columns_present(self, mock_fetch):
        """Test that drivers_df returns all expected columns."""
        mock_fetch.return_value = {
            "drivers": [
                {
                    "driver": "VER",
                    "team": "Red Bull Racing",
                    "driver_number": "1",
                    "first_name": "Max",
                    "last_name": "Verstappen",
                    "team_color": "#3671C6",
                    "headshot_url": "https://example.com/ver.png",
                }
            ]
        }
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        df = session.drivers_df

        # Check all expected columns are present
        expected_columns = [
            "Driver",
            "Team",
            "DriverNumber",
            "FirstName",
            "LastName",
            "TeamColor",
            "HeadshotUrl",
        ]
        assert list(df.columns) == expected_columns

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_df_column_values(self, mock_fetch):
        """Test that drivers_df correctly maps all column values."""
        mock_fetch.return_value = {
            "drivers": [
                {
                    "driver": "VER",
                    "team": "Red Bull Racing",
                    "dn": "1",
                    "fn": "Max",
                    "ln": "Verstappen",
                    "tc": "#3671C6",
                    "url": "https://example.com/ver.png",
                },
                {
                    "driver": "HAM",
                    "team": "Mercedes",
                    "dn": "44",
                    "fn": "Lewis",
                    "ln": "Hamilton",
                    "tc": "#6CD3BF",
                    "url": "https://example.com/ham.png",
                },
            ]
        }
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        df = session.drivers_df

        # Check first driver
        assert df.iloc[0]["Driver"] == "VER"
        assert df.iloc[0]["Team"] == "Red Bull Racing"
        assert df.iloc[0]["DriverNumber"] == "1"
        assert df.iloc[0]["FirstName"] == "Max"
        assert df.iloc[0]["LastName"] == "Verstappen"
        assert df.iloc[0]["TeamColor"] == "#3671C6"
        assert df.iloc[0]["HeadshotUrl"] == "https://example.com/ver.png"

        # Check second driver
        assert df.iloc[1]["Driver"] == "HAM"
        assert df.iloc[1]["Team"] == "Mercedes"
        assert df.iloc[1]["DriverNumber"] == "44"
        assert df.iloc[1]["FirstName"] == "Lewis"
        assert df.iloc[1]["LastName"] == "Hamilton"
        assert df.iloc[1]["TeamColor"] == "#6CD3BF"
        assert df.iloc[1]["HeadshotUrl"] == "https://example.com/ham.png"

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_df_all_fields(self, mock_fetch):
        """Test that drivers_df maps all fields from new schema."""
        mock_fetch.return_value = {
            "drivers": [
                {
                    "driver": "LEC",
                    "team": "Ferrari",
                    "dn": "16",
                    "fn": "Charles",
                    "ln": "Leclerc",
                    "tc": "#F91536",
                    "url": "https://example.com/lec.png",
                }
            ]
        }
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        df = session.drivers_df

        assert df.iloc[0]["Driver"] == "LEC"
        assert df.iloc[0]["Team"] == "Ferrari"
        assert df.iloc[0]["DriverNumber"] == "16"
        assert df.iloc[0]["FirstName"] == "Charles"
        assert df.iloc[0]["LastName"] == "Leclerc"
        assert df.iloc[0]["TeamColor"] == "#F91536"
        assert df.iloc[0]["HeadshotUrl"] == "https://example.com/lec.png"

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_df_missing_optional_fields(self, mock_fetch):
        """Test that drivers_df handles missing optional fields gracefully."""
        mock_fetch.return_value = {
            "drivers": [
                {
                    "driver": "NOR",
                    "team": "McLaren",
                }
            ]
        }
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        df = session.drivers_df

        assert df.iloc[0]["Driver"] == "NOR"
        assert df.iloc[0]["Team"] == "McLaren"
        assert df.iloc[0]["DriverNumber"] == ""
        assert df.iloc[0]["FirstName"] == ""
        assert df.iloc[0]["LastName"] == ""
        assert df.iloc[0]["TeamColor"] == ""
        assert df.iloc[0]["HeadshotUrl"] == ""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_df_empty_drivers_list(self, mock_fetch):
        """Test that drivers_df returns empty DataFrame with correct columns."""
        mock_fetch.return_value = {"drivers": []}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        df = session.drivers_df

        assert len(df) == 0
        expected_columns = [
            "Driver",
            "Team",
            "DriverNumber",
            "FirstName",
            "LastName",
            "TeamColor",
            "HeadshotUrl",
        ]
        assert list(df.columns) == expected_columns

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_df_no_drivers_key(self, mock_fetch):
        """Test that drivers_df handles missing drivers key."""
        mock_fetch.return_value = {}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        df = session.drivers_df

        assert len(df) == 0
        expected_columns = [
            "Driver",
            "Team",
            "DriverNumber",
            "FirstName",
            "LastName",
            "TeamColor",
            "HeadshotUrl",
        ]
        assert list(df.columns) == expected_columns

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_df_skips_invalid_entries(self, mock_fetch):
        """Test that drivers_df skips non-dict entries."""
        mock_fetch.return_value = {
            "drivers": [
                {"driver": "VER", "team": "Red Bull Racing", "dn": "1"},
                "invalid_string",
                None,
                {"driver": "HAM", "team": "Mercedes", "dn": "44"},
            ]
        }
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        df = session.drivers_df

        # Should only have 2 valid drivers
        assert len(df) == 2
        assert df.iloc[0]["Driver"] == "VER"
        assert df.iloc[1]["Driver"] == "HAM"

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_drivers_df_multiple_drivers_complete(self, mock_fetch):
        """Test drivers_df with multiple drivers and all fields."""
        mock_fetch.return_value = {
            "drivers": [
                {
                    "driver": "VER",
                    "team": "Red Bull Racing",
                    "dn": "1",
                    "fn": "Max",
                    "ln": "Verstappen",
                    "tc": "#3671C6",
                    "url": "https://example.com/ver.png",
                },
                {
                    "driver": "GAS",
                    "team": "Alpine",
                    "dn": "10",
                    "fn": "Pierre",
                    "ln": "Gasly",
                    "tc": "#2293D1",
                    "url": "https://example.com/gas.png",
                },
                {
                    "driver": "PER",
                    "team": "Red Bull Racing",
                    "dn": "11",
                    "fn": "Sergio",
                    "ln": "Perez",
                    "tc": "#3671C6",
                    "url": "https://example.com/per.png",
                },
            ]
        }
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        df = session.drivers_df

        assert len(df) == 3
        assert df["Driver"].tolist() == ["VER", "GAS", "PER"]
        assert df["Team"].tolist() == ["Red Bull Racing", "Alpine", "Red Bull Racing"]
        assert df["DriverNumber"].tolist() == ["1", "10", "11"]
        assert df["FirstName"].tolist() == ["Max", "Pierre", "Sergio"]
        assert df["LastName"].tolist() == ["Verstappen", "Gasly", "Perez"]
        assert df["TeamColor"].tolist() == ["#3671C6", "#2293D1", "#3671C6"]
        assert all(url.startswith("https://example.com/") for url in df["HeadshotUrl"])
