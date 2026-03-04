"""Additional coverage tests for core module."""

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import tif1.core as core_module
from tif1.core import (
    Driver,
    LazyTelemetryDict,
    LRUCache,
    Session,
    _coerce_lap_number,
    _coerce_lap_time,
    _create_lap_df,
    _extract_driver_codes,
    _extract_driver_info_map,
    _extract_lap_numbers,
    _get_lap_column,
    _process_lap_df,
    _resolve_session_options,
    _validate_json_payload,
    clear_lap_cache,
    config,
)
from tif1.exceptions import DataNotFoundError, InvalidDataError, NetworkError


class TestLRUCache:
    """Test LRUCache class."""

    def test_init(self):
        cache = LRUCache(maxsize=5)
        assert cache.maxsize == 5
        assert len(cache.cache) == 0

    def test_get_miss(self):
        cache = LRUCache()
        assert cache.get("missing") is None

    def test_set_and_get(self):
        cache = LRUCache()
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_eviction(self):
        cache = LRUCache(maxsize=2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        assert cache.get("a") is None
        assert cache.get("b") == 2
        assert cache.get("c") == 3

    def test_move_to_end_on_get(self):
        cache = LRUCache(maxsize=2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.get("a")  # Move "a" to end
        cache.set("c", 3)  # Should evict "b"
        assert cache.get("a") == 1
        assert cache.get("b") is None
        assert cache.get("c") == 3

    def test_update_existing_key(self):
        cache = LRUCache(maxsize=2)
        cache.set("a", 1)
        cache.set("a", 2)
        assert cache.get("a") == 2
        assert len(cache.cache) == 1

    def test_clear(self):
        cache = LRUCache()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.clear()
        assert cache.get("a") is None
        assert cache.get("b") is None


class TestValidateJsonPayload:
    """Test _validate_json_payload."""

    def test_non_dict_data_passes_through(self):
        result = _validate_json_payload("some.json", [1, 2, 3])
        assert result == [1, 2, 3]

    def test_unmatched_path_passes_through(self):
        data = {"key": "value"}
        result = _validate_json_payload("unknown.json", data)
        assert result == data

    @patch("tif1.core.config")
    def test_drivers_json_validation_disabled(self, mock_config):
        mock_config.get.return_value = False
        data = {"drivers": []}
        result = _validate_json_payload("drivers.json", data)
        assert result == data

    @patch("tif1.core.config")
    def test_laptimes_json_validation_disabled(self, mock_config):
        mock_config.get.return_value = False
        data = {"time": [90.5], "lap": [1]}
        result = _validate_json_payload("VER/laptimes.json", data)
        assert result == data


class TestGetLapColumn:
    """Test _get_lap_column."""

    def test_returns_lapnumber_when_present(self):
        lap_dataframe = pd.DataFrame({"LapNumber": [1, 2], "LapTime": [90.5, 91.0]})
        assert _get_lap_column(lap_dataframe, "pandas") == "LapNumber"

    def test_returns_lap_when_lapnumber_missing(self):
        lap_dataframe = pd.DataFrame({"lap": [1, 2], "LapTime": [90.5, 91.0]})
        assert _get_lap_column(lap_dataframe, "pandas") == "lap"


class TestExtractDriverCodes:
    """Test _extract_driver_codes."""

    def test_empty_list(self):
        assert _extract_driver_codes([]) == set()

    def test_none(self):
        assert _extract_driver_codes(None) == set()

    def test_valid(self):
        drivers = [{"driver": "VER"}, {"driver": "HAM"}]
        assert _extract_driver_codes(drivers) == {"VER", "HAM"}

    def test_invalid_entries(self):
        drivers = [{"driver": "VER"}, "invalid", {"driver": 123}, {"team": "Red Bull"}]
        assert _extract_driver_codes(drivers) == {"VER"}


class TestExtractDriverInfoMap:
    """Test _extract_driver_info_map."""

    def test_empty(self):
        assert _extract_driver_info_map([]) == {}

    def test_none(self):
        assert _extract_driver_info_map(None) == {}

    def test_valid(self):
        drivers = [{"driver": "VER", "team": "Red Bull"}]
        result = _extract_driver_info_map(drivers)
        assert "VER" in result
        assert result["VER"]["team"] == "Red Bull"

    def test_skips_non_dict(self):
        drivers = [{"driver": "VER"}, "invalid"]
        result = _extract_driver_info_map(drivers)
        assert len(result) == 1


class TestCoerceLapNumber:
    """Test _coerce_lap_number."""

    def test_valid_int(self):
        assert _coerce_lap_number(5) == 5

    def test_valid_str(self):
        assert _coerce_lap_number("3") == 3

    def test_none_raises(self):
        with pytest.raises(ValueError, match="No lap number"):
            _coerce_lap_number(None)

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid lap number"):
            _coerce_lap_number("abc")


class TestCoerceLapTime:
    """Test _coerce_lap_time."""

    def test_valid_float(self):
        assert _coerce_lap_time(90.5) == 90.5

    def test_valid_str(self):
        assert _coerce_lap_time("90.5") == 90.5

    def test_none_raises(self):
        with pytest.raises(ValueError, match="No lap time"):
            _coerce_lap_time(None)

    def test_nan_raises(self):
        with pytest.raises(ValueError, match="Invalid lap time"):
            _coerce_lap_time(float("nan"))

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid lap time"):
            _coerce_lap_time("abc")


class TestExtractLapNumbers:
    """Test _extract_lap_numbers."""

    def test_empty_df(self):
        lap_dataframe = pd.DataFrame()
        assert _extract_lap_numbers(lap_dataframe, "pandas") == set()

    def test_valid(self):
        lap_dataframe = pd.DataFrame({"LapNumber": [1, 2, 3]})
        assert _extract_lap_numbers(lap_dataframe, "pandas") == {1, 2, 3}

    def test_missing_column(self):
        lap_dataframe = pd.DataFrame({"other": [1, 2]})
        assert _extract_lap_numbers(lap_dataframe, "pandas") == set()

    def test_with_invalid_values(self):
        lap_dataframe = pd.DataFrame({"LapNumber": [1, "abc", 3, None]})
        result = _extract_lap_numbers(lap_dataframe, "pandas")
        assert 1 in result
        assert 3 in result


class TestCreateLapDf:
    """Test _create_lap_df."""

    def test_pandas(self):
        lap_data = {"time": [90.5, 91.0], "lap": [1, 2]}
        result = _create_lap_df(lap_data, "VER", "Red Bull", "pandas")
        assert isinstance(result, pd.DataFrame)
        assert "Driver" in result.columns
        assert "Team" in result.columns
        assert result["Driver"].iloc[0] == "VER"
        assert result["Team"].iloc[0] == "Red Bull"


class TestProcessLapDf:
    """Test _process_lap_df."""

    def test_renames_and_categorizes(self):
        lap_dataframe = pd.DataFrame(
            {
                "time": [90.5],
                "lap": [1],
                "compound": ["SOFT"],
                "Driver": ["VER"],
                "Team": ["Red Bull"],
            }
        )
        result = _process_lap_df(lap_dataframe, "pandas")
        assert "LapTime" in result.columns
        assert "LapNumber" in result.columns
        assert "Compound" in result.columns
        assert result["Driver"].dtype.name == "category"

    def test_polars_skips_categorical_when_disabled(self, monkeypatch):
        pl = pytest.importorskip("polars")

        lap_dataframe = pl.DataFrame(
            {
                "time": [90.5],
                "lap": [1],
                "compound": ["SOFT"],
                "Driver": ["VER"],
                "Team": ["Red Bull"],
            }
        )

        monkeypatch.setitem(config._config, "polars_lap_categorical", False)
        result = _process_lap_df(lap_dataframe, "polars")

        assert "LapTime" in result.columns
        assert result.schema["Driver"] != pl.Categorical

    def test_polars_applies_categorical_when_enabled(self, monkeypatch):
        pl = pytest.importorskip("polars")

        lap_dataframe = pl.DataFrame(
            {
                "time": [90.5],
                "lap": [1],
                "compound": ["SOFT"],
                "Driver": ["VER"],
                "Team": ["Red Bull"],
            }
        )

        monkeypatch.setitem(config._config, "polars_lap_categorical", True)
        result = _process_lap_df(lap_dataframe, "polars")

        assert "LapTime" in result.columns
        assert result.schema["Driver"] == pl.Categorical

    def test_renames_extended_lap_columns(self):
        lap_dataframe = pd.DataFrame(
            {
                "time": [90.5],
                "lap": [1],
                "compound": ["SOFT"],
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
                "Driver": ["VER"],
                "Team": ["Red Bull"],
            }
        )
        result = _process_lap_df(lap_dataframe, "pandas")
        assert "Time" in result.columns
        assert "DriverNumber" in result.columns
        assert "SpeedI1" in result.columns
        assert "SpeedI2" in result.columns
        assert "SpeedFL" in result.columns
        assert "SpeedST" in result.columns
        assert "FreshTyre" in result.columns
        assert "LapStartTime" in result.columns
        assert "LapStartDate" in result.columns
        assert "Deleted" in result.columns
        assert "DeletedReason" in result.columns
        assert "FastF1Generated" in result.columns
        assert "IsAccurate" in result.columns
        assert "WeatherTime" in result.columns
        assert "AirTemp" in result.columns
        assert "Humidity" in result.columns
        assert "Pressure" in result.columns
        assert "Rainfall" in result.columns
        assert "TrackTemp" in result.columns
        assert "WindDirection" in result.columns
        assert "WindSpeed" in result.columns


class TestResolveSessionOptions:
    """Test _resolve_session_options."""

    def test_defaults(self):
        cache, lib = _resolve_session_options(None, None, log_warnings=False)
        assert isinstance(cache, bool)
        assert lib in ("pandas", "polars")

    def test_explicit_values(self):
        cache, lib = _resolve_session_options(False, "pandas")
        assert cache is False
        assert lib == "pandas"

    def test_invalid_backend_falls_back(self):
        _, lib = _resolve_session_options(None, "invalid", log_warnings=False)
        assert lib == "pandas"

    def test_invalid_enable_cache_falls_back(self):
        cache, _ = _resolve_session_options("not_bool", None, log_warnings=False)
        assert cache is True

    def test_polars_recheck_after_prior_failure(self, monkeypatch):
        fake_polars = SimpleNamespace(DataFrame=object)
        monkeypatch.setattr(core_module, "POLARS_AVAILABLE", False)
        monkeypatch.setattr(core_module, "pl", None)
        monkeypatch.setitem(sys.modules, "polars", fake_polars)

        assert core_module._ensure_polars_available() is True
        assert core_module.POLARS_AVAILABLE is True
        assert core_module.pl is fake_polars


class TestTimedeltaCoercion:
    """Test warning-safe timedelta coercion paths."""

    def test_process_lap_df_never_passes_nan_to_seconds_timedelta(self, monkeypatch):
        original_to_timedelta = core_module.pd.to_timedelta

        def guarded_to_timedelta(values, *args, **kwargs):
            if kwargs.get("unit") == "s":
                as_series = pd.Series(values)
                if bool(as_series.isna().any()):
                    raise AssertionError(
                        "NaN values must not be passed to pd.to_timedelta(unit='s')"
                    )
            return original_to_timedelta(values, *args, **kwargs)

        monkeypatch.setattr(core_module.pd, "to_timedelta", guarded_to_timedelta)

        lap_df = pd.DataFrame(
            {
                "Driver": ["VER", "HAM", "LEC"],
                "Team": ["RBR", "MER", "FER"],
                "LapNumber": [1, 2, 3],
                "LapTime": [90.125, None, "0:01:31.500"],
                "Time": [1.5, None, 2.75],
                "WeatherTime": [None, 4.0, 5.0],
                "Sector1Time": [30.0, None, 31.2],
            }
        )

        result = _process_lap_df(lap_df, "pandas")
        assert pd.api.types.is_timedelta64_ns_dtype(result["LapTime"])
        assert pd.api.types.is_timedelta64_ns_dtype(result["Time"])
        assert pd.api.types.is_timedelta64_ns_dtype(result["WeatherTime"])
        assert pd.api.types.is_timedelta64_ns_dtype(result["Sector1Time"])
        assert result["LapTime"].isna().sum() == 1


class TestClearLapCache:
    """Test clear_lap_cache."""

    def test_clears(self):
        from tif1.core import _global_lap_cache, _global_lap_cache_polars

        _global_lap_cache.set("test", "value")
        _global_lap_cache_polars.set("test", "value")
        assert _global_lap_cache.get("test") is not None
        assert _global_lap_cache_polars.get("test") is not None
        clear_lap_cache()
        assert _global_lap_cache.get("test") is None
        assert _global_lap_cache_polars.get("test") is None


class TestBackendLapCacheIsolation:
    """Test lib-specific global lap cache isolation."""

    def test_laps_property_uses_backend_specific_cache_when_enabled(self):
        pl = pytest.importorskip("polars")

        from tif1.core import _global_lap_cache, _global_lap_cache_polars

        clear_lap_cache()
        try:
            pandas_session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
            polars_session = Session(2025, "Test GP", "Race", enable_cache=True, lib="polars")

            cache_key = f"{pandas_session.year}_{pandas_session.gp}_{pandas_session.session}_laps"
            pandas_laps = pd.DataFrame({"Driver": ["VER"], "LapNumber": [1], "LapTime": [90.1]})
            polars_laps = pl.DataFrame({"Driver": ["VER"], "LapNumber": [1], "LapTime": [90.1]})

            _global_lap_cache.set(cache_key, pandas_laps)
            _global_lap_cache_polars.set(cache_key, polars_laps)

            assert pandas_session.laps is pandas_laps
            assert polars_session.laps is polars_laps
        finally:
            clear_lap_cache()

    def test_laps_property_skips_global_cache_when_disabled(self, monkeypatch):
        from tif1.core import _global_lap_cache

        clear_lap_cache()
        try:
            session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
            cache_key = f"{session.year}_{session.gp}_{session.session}_laps"
            cached_laps = pd.DataFrame({"Driver": ["CACHED"], "LapNumber": [1], "LapTime": [90.1]})
            fetched_laps = pd.DataFrame({"Driver": ["LIVE"], "LapNumber": [2], "LapTime": [91.2]})
            _global_lap_cache.set(cache_key, cached_laps)

            async def fake_laps_async(self):
                return fetched_laps

            monkeypatch.setattr(Session, "laps_async", fake_laps_async)
            result = session.laps

            assert result.shape == fetched_laps.shape
            assert (result["LapNumber"].values == fetched_laps["LapNumber"].values).all()
            assert result is not cached_laps
        finally:
            clear_lap_cache()


class TestSessionInternalMethods:
    """Test Session internal methods."""

    def test_remember_and_get_local_payload(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._remember_local_payload("drivers.json", {"drivers": []})
        assert session._get_local_payload("drivers.json") == {"drivers": []}

    def test_remember_and_get_local_payload_race_control(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._remember_local_payload("rcm.json", {"time": [10.0]})
        assert session._get_local_payload("rcm.json") == {"time": [10.0]}

    def test_remember_and_get_local_payload_weather(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._remember_local_payload("weather.json", {"wT": [10.0]})
        assert session._get_local_payload("weather.json") == {"wT": [10.0]}

    def test_remember_local_payload_ignores_non_dict(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._remember_local_payload("drivers.json", [1, 2, 3])
        assert session._get_local_payload("drivers.json") is None

    def test_remember_local_payload_ignores_unknown_path(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._remember_local_payload("unknown.json", {"data": 1})
        assert session._get_local_payload("unknown.json") is None

    def test_remember_and_get_telemetry_payload(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        tel = {"speed": [100, 200]}
        session._remember_telemetry_payload("VER", 1, tel)
        assert session._get_telemetry_payload("VER", 1) == tel

    def test_get_telemetry_payload_miss(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        assert session._get_telemetry_payload("VER", 1) is None

    def test_remember_telemetry_ignores_empty(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._remember_telemetry_payload("VER", 1, {})
        assert session._get_telemetry_payload("VER", 1) is None

    def test_resolve_ultra_cold_mode_explicit_true(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        assert session._resolve_ultra_cold_mode(True) is True

    def test_resolve_ultra_cold_mode_explicit_false(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        assert session._resolve_ultra_cold_mode(False) is False

    def test_is_fastest_lap_tel_cold_start(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        assert session._is_fastest_lap_tel_cold_start() is True

    def test_is_fastest_lap_tel_cold_start_not_cold(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._laps = pd.DataFrame({"LapNumber": [1]})
        assert session._is_fastest_lap_tel_cold_start() is False

    def test_should_backfill_ultra_cold_cache_disabled(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        assert session._should_backfill_ultra_cold_cache(True) is False

    def test_should_backfill_not_ultra_cold(self):
        session = Session(2025, "Test GP", "Race", enable_cache=True)
        assert session._should_backfill_ultra_cold_cache(False) is False

    def test_mark_session_cache_populated(self):
        session = Session(2025, "Test GP", "Race", enable_cache=True)
        session._mark_session_cache_populated()
        assert session._cache_has_session_data is True

    def test_mark_session_cache_not_populated_when_disabled(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._mark_session_cache_populated()
        assert session._cache_has_session_data is None

    def test_build_driver_laptime_requests_empty_drivers(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = []
        result = session._build_driver_laptime_requests()
        assert result == []

    def test_build_driver_laptime_requests_with_filter(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [
            {"driver": "VER", "team": "Red Bull"},
            {"driver": "HAM", "team": "Mercedes"},
        ]
        result = session._build_driver_laptime_requests(drivers_filter=["VER"])
        assert len(result) == 1
        assert result[0][0]["driver"] == "VER"

    def test_build_driver_laptime_requests_skips_invalid(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [
            {"driver": "VER", "team": "Red Bull"},
            "invalid",
            {"team": "only team"},
            {"driver": "", "team": "empty"},
        ]
        result = session._build_driver_laptime_requests()
        assert len(result) == 1

    def test_extract_fastest_lap_candidate_valid(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        lap_data = {"lap": [1, 2, 3], "time": [91.0, 89.5, 90.0]}
        result = session._extract_fastest_lap_candidate("VER", lap_data)
        assert result == ("VER", 2, 89.5)

    def test_extract_fastest_lap_candidate_none_data(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        assert session._extract_fastest_lap_candidate("VER", None) is None

    def test_extract_fastest_lap_candidate_no_lap_key(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        assert session._extract_fastest_lap_candidate("VER", {"data": "other"}) is None

    def test_extract_fastest_lap_candidate_all_invalid(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        lap_data = {"lap": ["a", "b"], "time": ["x", "y"]}
        assert session._extract_fastest_lap_candidate("VER", lap_data) is None

    def test_extract_fastest_lap_row_valid(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver_info = {"driver": "VER", "team": "Red Bull"}
        lap_data = {
            "lap": [1, 2],
            "time": [91.0, 89.5],
            "compound": ["SOFT", "MEDIUM"],
        }
        result = session._extract_fastest_lap_row(driver_info, lap_data)
        assert result is not None
        assert result["Driver"] == "VER"
        assert result["Team"] == "Red Bull"
        assert result["lap"] == 2
        assert result["time"] == 89.5

    def test_extract_fastest_lap_row_no_driver_code(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver_info = {"team": "Red Bull"}
        lap_data = {"lap": [1], "time": [90.0]}
        assert session._extract_fastest_lap_row(driver_info, lap_data) is None

    def test_extract_fastest_lap_row_non_dict(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        assert session._extract_fastest_lap_row({"driver": "VER"}, "not a dict") is None

    def test_session_cache_available_disabled(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        assert session._session_cache_available() is False

    def test_refresh_driver_indices_none_drivers(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = None
        session._refresh_driver_indices()
        assert session._driver_codes == set()

    def test_has_driver_code_with_loaded_drivers(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"driver": "VER", "team": "Red Bull"}]
        assert session._has_driver_code("VER") is True
        assert session._has_driver_code("HAM") is False

    def test_get_driver_info_with_loaded_drivers(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"driver": "VER", "team": "Red Bull"}]
        info = session._get_driver_info("VER")
        assert info["team"] == "Red Bull"

    def test_get_driver_info_missing(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"driver": "VER", "team": "Red Bull"}]
        info = session._get_driver_info("HAM")
        assert info["driver"] == "HAM"
        assert info["team"] == ""

    def test_get_from_cache_disabled(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        assert session._get_from_cache("key") is None


class TestSessionGetFastestLapsWithDrivers:
    """Test get_fastest_laps with driver filter on preloaded laps."""

    def test_filters_by_driver(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER", "HAM", "VER", "HAM"],
                "LapTime": [90.5, 91.0, 89.0, 92.0],
                "LapNumber": [1, 1, 2, 2],
                "Team": ["Red Bull", "Mercedes", "Red Bull", "Mercedes"],
            }
        )
        fastest = session.get_fastest_laps(by_driver=True, drivers=["VER"])
        assert len(fastest) == 1
        assert fastest.iloc[0]["Driver"] == "VER"

    def test_empty_after_driver_filter(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER"],
                "LapTime": [90.5],
                "LapNumber": [1],
            }
        )
        fastest = session.get_fastest_laps(by_driver=True, drivers=["HAM"])
        assert len(fastest) == 0

    def test_invalid_drivers_param_raises(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        with pytest.raises(TypeError):
            session.get_fastest_laps(drivers="VER")

    def test_empty_drivers_list_raises(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        with pytest.raises(ValueError, match="drivers"):
            session.get_fastest_laps(drivers=[])


class TestDriverGetFastestLapTel:
    """Test Driver.get_fastest_lap_tel."""

    def test_uses_raw_payload(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"driver": "VER", "team": "Red Bull"}]
        driver = Driver(session, "VER")
        driver._prefetched_lap_data = {"lap": [1, 2], "time": [91.0, 89.5]}

        with patch.object(
            session, "_get_telemetry_df_for_ref", return_value=pd.DataFrame({"Speed": [100, 200]})
        ):
            result = driver.get_fastest_lap_tel()
            assert isinstance(result, pd.DataFrame)


class TestDriverGetFastestLap:
    """Test Driver.get_fastest_lap with empty laps."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_empty_laps(self, mock_fetch):
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver = Driver(session, "VER")
        driver._laps = pd.DataFrame()
        result = driver.get_fastest_lap()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_all_invalid_laps(self, mock_fetch):
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        driver = Driver(session, "VER")
        driver._laps = pd.DataFrame({"LapTime": ["None", "None"], "LapNumber": [1, 2]})
        result = driver.get_fastest_lap()
        assert len(result) == 0


class TestEnsureNestedLoopSupport:
    """Test _ensure_nested_loop_support."""

    def test_no_running_loop(self):
        from tif1.core import _ensure_nested_loop_support

        # Should not raise when no loop is running
        _ensure_nested_loop_support("test")

    def test_with_running_loop_nest_asyncio_available(self):
        import asyncio

        from tif1.core import _ensure_nested_loop_support

        async def _inner():
            _ensure_nested_loop_support("test")

        asyncio.run(_inner())

    def test_with_running_loop_nest_asyncio_missing(self, monkeypatch):
        import asyncio
        import sys

        from tif1 import core as core_module

        # Reset the global flag
        original_flag = core_module._NEST_ASYNCIO_APPLIED
        core_module._NEST_ASYNCIO_APPLIED = False

        try:
            # Hide nest_asyncio
            monkeypatch.setitem(sys.modules, "nest_asyncio", None)

            async def _inner():
                with pytest.raises(RuntimeError, match="cannot run inside an active event loop"):
                    core_module._ensure_nested_loop_support("test")

            asyncio.run(_inner())
        finally:
            core_module._NEST_ASYNCIO_APPLIED = original_flag


class TestSessionCacheProbe:
    """Test Session cache probe logic."""

    def test_cache_probe_exception_fallback(self, monkeypatch):
        from tif1.cache import get_cache

        cache = get_cache()

        def failing_has_session_data(*args, **kwargs):
            raise RuntimeError("Cache probe failed")

        monkeypatch.setattr(cache, "has_session_data", failing_has_session_data)

        session = Session(2025, "Test GP", "Race", enable_cache=True)
        # Should fall back to True
        assert session._session_cache_available() is True


class TestScheduleBackgroundCacheFill:
    """Test _schedule_background_cache_fill."""

    def test_background_cache_fill_with_json_payloads(self):
        import time

        session = Session(2025, "Test GP", "Race", enable_cache=True)
        json_payloads = [("drivers.json", {"drivers": []})]

        session._schedule_background_cache_fill(json_payloads=json_payloads)
        time.sleep(0.1)  # Give thread time to run

    def test_background_cache_fill_with_telemetry(self):
        import time

        session = Session(2025, "Test GP", "Race", enable_cache=True)
        telemetry_payloads = [("VER", 1, {"speed": [100, 200]})]

        session._schedule_background_cache_fill(telemetry_payloads=telemetry_payloads)
        time.sleep(0.1)

    def test_background_cache_fill_with_non_dict_payload(self):
        import time

        session = Session(2025, "Test GP", "Race", enable_cache=True)
        json_payloads = [("drivers.json", [1, 2, 3])]  # Non-dict

        session._schedule_background_cache_fill(json_payloads=json_payloads)
        time.sleep(0.1)

    def test_background_cache_fill_exception_handling(self, monkeypatch):
        import time

        from tif1.cache import get_cache

        cache = get_cache()

        def failing_set(*args, **kwargs):
            raise RuntimeError("Cache set failed")

        monkeypatch.setattr(cache, "set", failing_set)

        session = Session(2025, "Test GP", "Race", enable_cache=True)
        json_payloads = [("drivers.json", {"drivers": []})]

        # Should not raise, just log
        session._schedule_background_cache_fill(json_payloads=json_payloads)
        time.sleep(0.1)


class TestFetchJsonUnvalidated:
    """Test _fetch_json_unvalidated."""

    def test_returns_dict_from_cdn_fast(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        def fake_fetch(path):
            return {"data": "value"}

        monkeypatch.setattr(session, "_fetch_from_cdn_fast", fake_fetch)
        result = session._fetch_json_unvalidated("test.json")
        assert result == {"data": "value"}

    def test_handles_response_object_with_json(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        class FakeResponse:
            status_code = 200

            def json(self):
                return {"data": "value"}

            def raise_for_status(self):
                pass

        def fake_fetch(path):
            return FakeResponse()

        monkeypatch.setattr(session, "_fetch_from_cdn_fast", fake_fetch)
        result = session._fetch_json_unvalidated("test.json")
        assert result == {"data": "value"}

    def test_handles_404_response(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        class FakeResponse:
            status_code = 404

            def json(self):
                return {}

            def raise_for_status(self):
                pass

        def fake_fetch(path):
            return FakeResponse()

        monkeypatch.setattr(session, "_fetch_from_cdn_fast", fake_fetch)

        with pytest.raises(DataNotFoundError):
            session._fetch_json_unvalidated("test.json")

    def test_handles_non_dict_json_response(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        class FakeResponse:
            status_code = 200

            def json(self):
                return [1, 2, 3]

            def raise_for_status(self):
                pass

        def fake_fetch(path):
            return FakeResponse()

        monkeypatch.setattr(session, "_fetch_from_cdn_fast", fake_fetch)

        with pytest.raises(InvalidDataError, match="Expected dict"):
            session._fetch_json_unvalidated("test.json")

    def test_handles_non_dict_non_response_result(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        def fake_fetch(path):
            return "not a dict"

        monkeypatch.setattr(session, "_fetch_from_cdn_fast", fake_fetch)

        with pytest.raises(InvalidDataError, match="Expected dict"):
            session._fetch_json_unvalidated("test.json")


class TestFetchFromCdnFast:
    """Test _fetch_from_cdn_fast."""

    def test_successful_fetch(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        class FakeResponse:
            status_code = 200

            def json(self):
                return {"data": "value"}

            def raise_for_status(self):
                pass

        class FakeCdnManager:
            def try_sources(self, _year, _gp, _session_name, _path, fetch_fn):
                return fetch_fn("http://example.com/test.json")

        def fake_get(url, timeout):
            return FakeResponse()

        monkeypatch.setattr("tif1.core.get_cdn_manager", lambda: FakeCdnManager())
        monkeypatch.setattr("tif1.core._get_session", lambda: MagicMock(get=fake_get))

        result = session._fetch_from_cdn_fast("test.json")
        assert result == {"data": "value"}

    def test_404_raises_data_not_found(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        class FakeResponse:
            status_code = 404

            def raise_for_status(self):
                pass

        class FakeCdnManager:
            def try_sources(self, _year, _gp, _session_name, _path, fetch_fn):
                return fetch_fn("http://example.com/test.json")

        def fake_get(url, timeout):
            return FakeResponse()

        monkeypatch.setattr("tif1.core.get_cdn_manager", lambda: FakeCdnManager())
        monkeypatch.setattr("tif1.core._get_session", lambda: MagicMock(get=fake_get))

        with pytest.raises(DataNotFoundError):
            session._fetch_from_cdn_fast("test.json")

    def test_non_dict_response_raises_invalid_data(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        class FakeResponse:
            status_code = 200

            def json(self):
                return [1, 2, 3]

            def raise_for_status(self):
                pass

        class FakeCdnManager:
            def try_sources(self, _year, _gp, _session_name, _path, fetch_fn):
                return fetch_fn("http://example.com/test.json")

        def fake_get(url, timeout):
            return FakeResponse()

        monkeypatch.setattr("tif1.core.get_cdn_manager", lambda: FakeCdnManager())
        monkeypatch.setattr("tif1.core._get_session", lambda: MagicMock(get=fake_get))

        with pytest.raises(InvalidDataError, match="Expected dict"):
            session._fetch_from_cdn_fast("test.json")


class TestFetchFromCdn:
    """Test _fetch_from_cdn with retry logic."""

    def test_successful_fetch_with_retry(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        class FakeResponse:
            status_code = 200

            def json(self):
                return {"data": "value"}

            def raise_for_status(self):
                pass

        class FakeCdnManager:
            def try_sources(self, _year, _gp, _session_name, _path, fetch_fn):
                return fetch_fn("http://example.com/test.json")

        def fake_get(url, timeout):
            return FakeResponse()

        monkeypatch.setattr("tif1.core.get_cdn_manager", lambda: FakeCdnManager())
        monkeypatch.setattr("tif1.core._get_session", lambda: MagicMock(get=fake_get))

        result = session._fetch_from_cdn("test.json")
        assert result == {"data": "value"}

    def test_404_raises_data_not_found_with_retry(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        class FakeResponse:
            status_code = 404

            def raise_for_status(self):
                pass

        class FakeCdnManager:
            def try_sources(self, _year, _gp, _session_name, _path, fetch_fn):
                return fetch_fn("http://example.com/test.json")

        def fake_get(url, timeout):
            return FakeResponse()

        monkeypatch.setattr("tif1.core.get_cdn_manager", lambda: FakeCdnManager())
        monkeypatch.setattr("tif1.core._get_session", lambda: MagicMock(get=fake_get))

        with pytest.raises(DataNotFoundError):
            session._fetch_from_cdn("test.json")

    def test_non_dict_response_raises_invalid_data_with_retry(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)

        class FakeResponse:
            status_code = 200

            def json(self):
                return [1, 2, 3]

            def raise_for_status(self):
                pass

        class FakeCdnManager:
            def try_sources(self, _year, _gp, _session_name, _path, fetch_fn):
                return fetch_fn("http://example.com/test.json")

        def fake_get(url, timeout):
            return FakeResponse()

        monkeypatch.setattr("tif1.core.get_cdn_manager", lambda: FakeCdnManager())
        monkeypatch.setattr("tif1.core._get_session", lambda: MagicMock(get=fake_get))

        with pytest.raises(InvalidDataError, match="Expected dict"):
            session._fetch_from_cdn("test.json")


class TestExtractFastestRefFromFastestLaps:
    """Test _extract_fastest_ref_from_fastest_laps."""

    def test_empty_dataframe(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        fastest_laps = pd.DataFrame()
        result = session._extract_fastest_ref_from_fastest_laps(fastest_laps)
        assert result is None

    def test_missing_columns(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        fastest_laps = pd.DataFrame({"Driver": ["VER"]})
        result = session._extract_fastest_ref_from_fastest_laps(fastest_laps)
        assert result is None

    def test_valid_fastest_lap(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        fastest_laps = pd.DataFrame(
            {
                "Driver": ["VER", "HAM"],
                "LapNumber": [5, 10],
                "LapTime": [89.5, 90.0],
            }
        )
        result = session._extract_fastest_ref_from_fastest_laps(fastest_laps)
        assert result == ("VER", 5)


class TestFindTelemetryDfForRef:
    """Test _find_telemetry_df_for_ref."""

    def test_empty_tels_list(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        result = session._find_telemetry_df_for_ref([], ("VER", 5))
        assert result is None

    def test_matching_ref_found(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        tel_df = pd.DataFrame({"Driver": ["VER"], "LapNumber": [5], "Speed": [100]})
        tels = [tel_df]
        result = session._find_telemetry_df_for_ref(tels, ("VER", 5))
        assert result is tel_df

    def test_no_matching_ref(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        tel_df = pd.DataFrame({"Driver": ["HAM"], "LapNumber": [3], "Speed": [100]})
        tels = [tel_df]
        result = session._find_telemetry_df_for_ref(tels, ("VER", 5))
        assert result is None


class TestHydrateFastestLapTelFromBatch:
    """Test _hydrate_fastest_lap_tel_from_batch."""

    def test_no_fastest_ref(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._hydrate_fastest_lap_tel_from_batch([], None)
        # Should not raise, just return early

    def test_tel_found_in_batch(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        tel_df = pd.DataFrame({"Driver": ["VER"], "LapNumber": [5], "Speed": [100]})
        tels = [tel_df]
        session._hydrate_fastest_lap_tel_from_batch(tels, ("VER", 5))
        assert session._fastest_lap_tel_ref == ("VER", 5)
        assert session._fastest_lap_tel_df is tel_df

    def test_tel_not_found_in_batch(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        tel_df = pd.DataFrame({"Driver": ["HAM"], "LapNumber": [3], "Speed": [100]})
        tels = [tel_df]
        session._hydrate_fastest_lap_tel_from_batch(tels, ("VER", 5))
        # Should not set anything when not found
        assert not hasattr(session, "_fastest_lap_tel_ref") or session._fastest_lap_tel_ref is None


class TestCoreHighYieldCoverage:
    """Target dense uncovered branches in Session telemetry/lap internals."""

    def test_lazy_telemetry_dict_load_and_miss(self):
        class FakeLaps(pd.DataFrame):
            @property
            def _constructor(self):
                return FakeLaps

            @property
            def telemetry(self):
                return pd.DataFrame({"Speed": [300]})

        laps = FakeLaps(
            {
                "Driver": ["VER", "HAM"],
                "LapNumber": [1, 1],
                "LapTime": [90.0, 91.0],
            }
        )
        session = SimpleNamespace(
            _drivers_data=[{"dn": "1", "driver": "VER"}, {"dn": "44", "driver": "HAM"}],
            laps=laps,
        )
        lazy = LazyTelemetryDict(session)
        loaded = lazy[1]
        assert isinstance(loaded, pd.DataFrame)
        with pytest.raises(KeyError):
            _ = lazy["XXX"]

    def test_coerce_driver_code_paths(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = [{"dn": "1", "driver": "VER"}]

        assert session._coerce_driver_code({"driver": " HAM "}) == "HAM"
        assert session._coerce_driver_code(" VER ") == "VER"
        assert session._coerce_driver_code(1) == "VER"
        assert session._coerce_driver_code(SimpleNamespace(driver="ALO")) == "ALO"
        assert session._coerce_driver_code(SimpleNamespace(Abbreviation="LEC")) == "LEC"
        with pytest.raises(TypeError):
            session._coerce_driver_code(object())

    def test_process_fastest_lap_refs_sort_and_backfill(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=True)
        scheduled = []
        monkeypatch.setattr(
            session, "_schedule_background_cache_fill", lambda **k: scheduled.append(k)
        )
        monkeypatch.setattr(session, "_should_backfill_ultra_cold_cache", lambda enabled: enabled)

        refs = session._process_fastest_lap_refs_from_payloads(
            [({"driver": "VER"}, "VER/laptimes.json"), ({"driver": "HAM"}, "HAM/laptimes.json")],
            [{"lap": [1, 2], "time": [91.0, 89.0]}, {"lap": [1], "time": [90.0]}],
            [("drivers.json", {"drivers": [{"driver": "VER"}]})],
            [("VER/laptimes.json", {"lap": [1], "time": [90.0]})],
            ultra_cold=True,
        )
        assert refs == [("VER", 2), ("HAM", 1)]
        assert scheduled

    def test_fastest_lap_reference_sync_and_async(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._laps = pd.DataFrame({"Driver": ["VER"], "LapNumber": [7], "LapTime": [88.8]})
        first = session._get_fastest_lap_reference()
        second = session._get_fastest_lap_reference()
        assert first == ("VER", 7)
        assert second == ("VER", 7)

        session._laps = None
        session._fastest_lap_ref = None
        session._fastest_lap_ref_driver_source_id = None
        monkeypatch.setattr(
            session,
            "_load_drivers_for_fastest_lap_reference",
            lambda ultra_cold=False: ([{"driver": "VER"}], []),  # noqa: ARG005
        )
        monkeypatch.setattr(
            session,
            "_find_fastest_lap_reference_from_raw",
            lambda drivers, ultra_cold=False: ("VER", 5),  # noqa: ARG005
        )
        assert session._get_fastest_lap_reference() == ("VER", 5)

        async def _run():
            monkeypatch.setattr(
                session,
                "_find_fastest_lap_reference_from_raw_async",
                lambda drivers, ultra_cold=False: asyncio.sleep(0, result=("VER", 6)),  # noqa: ARG005
            )
            return await session._get_fastest_lap_reference_async()

        assert asyncio.run(_run()) == ("VER", 6)

    def test_fetch_telemetry_batch_from_refs_sync_and_async(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=True)
        session._session_cache_available = lambda: True
        session._remember_telemetry_payload("VER", 1, {"speed": [300.0]})
        remembered = []
        monkeypatch.setattr(
            session,
            "_remember_telemetry_payload",
            lambda d, lap_num, payload: remembered.append((d, lap_num, payload)),
        )

        class FakeCache:
            def get_telemetry_batch(self, *_args, **_kwargs):
                return {("HAM", 2): {"speed": [290.0]}}

            async def get_telemetry_batch_async(self, *_args, **_kwargs):
                return {("HAM", 2): {"speed": [290.0]}}

        monkeypatch.setattr("tif1.core.get_cache", lambda: FakeCache())

        requests, lap_info, tels = session._fetch_telemetry_batch_from_refs(
            [("VER", 1), ("HAM", 2)]
        )
        assert requests == []
        assert lap_info == []
        assert len(tels) == 2
        assert remembered

        async def _run_ok():
            return await session._fetch_telemetry_batch_from_refs_async([("VER", 1), ("HAM", 2)])

        arequests, alap_info, atels = asyncio.run(_run_ok())
        assert arequests == []
        assert alap_info == []
        assert len(atels) == 2

    def test_collect_prefetch_precompute_and_background(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=True)
        session._laps = pd.DataFrame(
            {
                "Driver": ["VER", "VER", "HAM", 1],
                "LapNumber": [1, 1, 2, "x"],
                "LapTime": [90.0, 90.0, 91.0, 92.0],
            }
        )
        refs = session._collect_lap_refs_from_loaded_laps()
        assert refs == [("VER", 1), ("HAM", 2)]

        writes = []
        session._remember_telemetry_payload = lambda d, lap_num, payload: writes.append(
            (d, lap_num, payload)
        )
        session._mark_session_cache_populated = lambda: writes.append(("marked",))

        class FakeCache:
            def set_telemetry(self, *_args, **_kwargs):
                writes.append(("cache",))

        monkeypatch.setattr("tif1.core.get_cache", lambda: FakeCache())
        session._memoize_prefetched_telemetry_payloads(
            [("VER", 1), ("HAM", 2)],
            [{"tel": {"speed": [300.0]}}, {"tel": {"speed": [290.0]}}],
            ultra_cold=False,
        )
        assert any(item[0] == "VER" for item in writes if len(item) > 1)

        session._telemetry_payloads = {
            ("VER", 1): {"time": [0.0, 0.1], "speed": [300.0, 301.0]},
            ("HAM", 2): {"time": [0.0], "speed": [290.0]},
        }
        session._precompute_telemetry_dfs()
        assert ("VER", 1) in session._telemetry_df_cache
        assert ("HAM", 2) in session._telemetry_df_cache

        started = []

        class FakeThread:
            def __init__(self, target, kwargs, name, daemon):
                _ = (target, kwargs, name, daemon)

            def start(self):
                started.append(True)

        monkeypatch.setattr("tif1.core.threading.Thread", FakeThread)
        monkeypatch.setitem(config._config, "prefetch_all_telemetry_after_laps_load", True)
        monkeypatch.setitem(config._config, "prefetch_all_telemetry_on_first_lap_request", True)
        session._telemetry_background_prefetch_started = False
        session._telemetry_bulk_prefetch_done = False
        session._telemetry_bulk_prefetch_attempted = False
        session._resolve_telemetry_ultra_cold_mode = lambda _: False
        session._maybe_start_background_telemetry_prefetch()
        assert started

    def test_fetch_batch_process_results_and_public_fastest_tels(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._drivers = [
            {"driver": "VER", "team": "Red Bull"},
            {"driver": "HAM", "team": "Mercedes"},
        ]

        fastest_laps = pd.DataFrame({"Driver": ["VER", "HAM"], "LapNumber": ["1", "bad"]})
        requests, lap_info, _ = session._fetch_telemetry_batch(fastest_laps)
        assert len(requests) == 1
        assert lap_info == [("VER", 1)]

        session._should_backfill_ultra_cold_cache = lambda enabled: enabled
        scheduled = []
        session._schedule_background_cache_fill = lambda **k: scheduled.append(k)
        out = session._process_telemetry_results(
            [{"tel": {"speed": [300.0]}}, {"tel": {}}],
            [("VER", 1), ("HAM", 2)],
            [],
            ultra_cold=True,
        )
        assert len(out) == 1
        assert scheduled

        session._resolve_ultra_cold_mode = lambda _: False
        session._session_cache_available = lambda: True
        session._is_fastest_lap_tel_cold_start = lambda: False
        session._get_fastest_laps_from_raw = (
            lambda _drivers=None, _ultra_cold=False, _by_driver=True: pd.DataFrame()
        )
        session.get_fastest_laps = lambda by_driver=True, drivers=None: pd.DataFrame(  # noqa: ARG005
            {"Driver": ["VER"], "LapNumber": [1], "LapTime": [89.0]}
        )
        session._fetch_telemetry_batch = lambda fl, skip_cache=False: (  # noqa: ARG005
            [],
            [],
            [pd.DataFrame({"Driver": ["VER"], "LapNumber": [1], "Speed": [300.0]})],
        )
        df = session.get_fastest_laps_tels(by_driver=True)
        assert not df.empty

        async def _run_async():
            session._get_fastest_lap_refs_from_raw_async = (
                lambda drivers=None, ultra_cold=False: asyncio.sleep(0, result=[])  # noqa: ARG005
            )
            session.get_fastest_laps_async = lambda by_driver=True, drivers=None: asyncio.sleep(  # noqa: ARG005
                0,
                result=pd.DataFrame({"Driver": ["VER"], "LapNumber": [1], "LapTime": [89.0]}),
            )
            session._fetch_telemetry_batch_async = lambda fl, skip_cache=False: asyncio.sleep(  # noqa: ARG005
                0,
                result=(
                    [],
                    [],
                    [pd.DataFrame({"Driver": ["VER"], "LapNumber": [1], "Speed": [300.0]})],
                ),
            )
            return await session.get_fastest_laps_tels_async(by_driver=True)

        async_df = asyncio.run(_run_async())
        assert not async_df.empty

    def test_drivers_results_tables_and_aliases(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
        session._drivers = [
            {
                "driver": "VER",
                "dn": "1",
                "team": "Red Bull",
                "fn": "Max",
                "ln": "Verstappen",
                "tc": "3671C6",
                "url": "",
            },
            {
                "driver": "HAM",
                "dn": "44",
                "team": "Mercedes",
                "fn": "Lewis",
                "ln": "Hamilton",
                "tc": "00D2BE",
                "url": "",
            },
        ]
        session._refresh_driver_indices()

        assert session.drivers == ["1", "44"]
        assert session.driver_list == ["1", "44"]
        drivers_df = session.drivers_df
        assert {"Driver", "Team", "DriverNumber"}.issubset(drivers_df.columns)

        results = session.results
        assert len(results) == 2
        assert session.pos_data is session.car_data
        assert session.total_laps is None
        assert session.session_start_time is pd.NaT
        assert session.t0_date is pd.NaT
        info = session.session_info
        assert info["Year"] == 2025
        assert session.name == "Race"

        dirty = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        dirty._drivers = [{"dn": "16"}, "bad-row"]
        assert dirty.drivers == ["16"]
        assert dirty.driver_list == ["16"]

        event_series = pd.Series({"Session1": "Race", "Session1Date": "2025-03-01 12:30:00+00:00"})
        monkeypatch.setattr(type(session), "event", property(lambda _self: event_series))
        assert session.date == pd.Timestamp("2025-03-01 12:30:00")

        monkeypatch.setattr(
            type(session), "event", property(lambda _self: pd.Series({"Session1": "FP1"}))
        )
        assert pd.isna(session.date)

    def test_race_control_weather_and_track_status_branches(self):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._prefetch_session_tables = lambda: None
        session._load_session_table = lambda _path, _rename: pd.DataFrame(
            {"Time": ["2025-06-29T12:20:01.000000000"], "Status": ["GREEN"]}
        )
        rcm = session.race_control_messages
        assert not rcm.empty
        assert rcm["Time"].dtype == "datetime64[ns]"
        assert session.session_status is rcm

        weather_session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        weather_session._prefetch_session_tables = lambda: None
        weather_session._load_session_table = lambda _path, _rename: pd.DataFrame(
            {"Time": [12.5], "AirTemp": [24.0]}
        )
        weather = weather_session.weather
        assert weather["Time"].dtype.kind == "m"
        assert weather_session.weather_data is weather

        err_session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        err_session._prefetch_session_tables = lambda: None
        err_session._load_session_table = lambda path, _rename: (_ for _ in ()).throw(
            NetworkError(url=path)
        )
        assert err_session.race_control_messages.empty
        assert err_session.weather.empty

        session._laps = pd.DataFrame({"Driver": ["VER"], "TrackStatus": ["14"], "LapNumber": [1]})
        assert list(session.track_status) == ["14"]
        session._laps = pd.DataFrame({"Driver": ["VER"], "LapNumber": [1]})
        assert session.track_status.empty
        session._laps = pd.DataFrame()
        assert session.track_status.empty
        assert session.get_circuit_info() is not None

    def test_prefetch_driver_lookup_and_laps_branches(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
        session._drivers = None
        session._resolve_ultra_cold_mode = lambda _: False
        session._session_cache_available = lambda: True

        async def fake_fetch(requests, **kwargs):
            return [
                {"drivers": [{"driver": "VER", "team": "Red Bull"}]},
                {"lap": [1], "time": [90.0]},
            ]

        monkeypatch.setattr("tif1.core.fetch_multiple_async", fake_fetch)
        prefetched = session._prefetch_driver_lookup_and_laps("VER")
        assert prefetched == {"lap": [1], "time": [90.0]}
        assert session._has_driver_code("VER") is True
        assert session._get_driver_info("VER")["team"] == "Red Bull"

        async def compat_fetch(_requests, **kwargs):
            if "use_cache" in kwargs:
                raise TypeError("unexpected keyword argument")
            return [{"drivers": [{"driver": "HAM"}]}, {"lap": [2], "time": [91.0]}]

        session._drivers = None
        monkeypatch.setattr("tif1.core.fetch_multiple_async", compat_fetch)
        out = session._prefetch_driver_lookup_and_laps("HAM")
        assert out == {"lap": [2], "time": [91.0]}


class TestCoreCoverageSecondPass:
    """Second-pass targeted coverage for deep core branches."""

    def test_get_fastest_laps_from_raw_sync_and_async(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")

        drivers = [{"driver": "VER", "team": "Red Bull"}, {"driver": "HAM", "team": "Mercedes"}]
        reqs = [(drivers[0], "VER/laptimes.json"), (drivers[1], "HAM/laptimes.json")]
        payloads = [{"lap": [1, 2], "time": [91.0, 89.0]}, {"lap": [1], "time": [90.0]}]
        scheduled: list[dict] = []

        monkeypatch.setattr(
            session,
            "_load_drivers_for_fastest_lap_reference",
            lambda ultra_cold=False: (drivers, [("drivers.json", {"drivers": drivers})]),  # noqa: ARG005
        )
        monkeypatch.setattr(
            session,
            "_build_driver_laptime_requests",
            lambda driver_pool=None, drivers_filter=None: reqs,  # noqa: ARG005
        )
        monkeypatch.setattr(
            session,
            "_fetch_laptime_payloads",
            lambda _r, operation, ultra_cold=False: (payloads, [("cache.json", {"ok": True})]),  # noqa: ARG005
        )

        async def fake_fetch_async(_r, operation, ultra_cold=False):
            return payloads, [("cache.json", {"ok": True})]

        monkeypatch.setattr(session, "_fetch_laptime_payloads_async", fake_fetch_async)
        monkeypatch.setattr(session, "_should_backfill_ultra_cold_cache", lambda enabled: enabled)
        monkeypatch.setattr(
            session, "_schedule_background_cache_fill", lambda **kwargs: scheduled.append(kwargs)
        )

        sync_df = session._get_fastest_laps_from_raw(by_driver=True, ultra_cold=True)
        assert not sync_df.empty
        assert "Driver" in sync_df.columns

        sync_overall = session._get_fastest_laps_from_raw(by_driver=False, ultra_cold=True)
        assert len(sync_overall) == 1

        async_df = asyncio.run(
            session._get_fastest_laps_from_raw_async(by_driver=True, ultra_cold=True)
        )
        assert not async_df.empty

        async_overall = asyncio.run(
            session._get_fastest_laps_from_raw_async(by_driver=False, ultra_cold=True)
        )
        assert len(async_overall) == 1
        assert scheduled

    def test_fastest_ref_resolution_helpers(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        drivers = [{"driver": "VER"}, {"driver": "HAM"}, {"driver": 1}]

        monkeypatch.setattr(
            session,
            "_build_driver_laptime_requests",
            lambda driver_pool=None, drivers_filter=None: [  # noqa: ARG005
                ({"driver": "VER"}, "VER/laptimes.json"),
                ({"driver": "HAM"}, "HAM/laptimes.json"),
                ({"driver": 1}, "BAD/laptimes.json"),
            ],
        )
        monkeypatch.setattr(
            session,
            "_fetch_laptime_payloads",
            lambda _r, operation, ultra_cold=False: (  # noqa: ARG005
                [{"lap": [1, 2], "time": [91.0, 89.2]}, {"lap": [1], "time": [89.8]}, None],
                [("a.json", {"x": 1})],
            ),
        )

        async def fake_fetch_async(_r, operation, ultra_cold=False):
            return [{"lap": [1], "time": [90.0]}, {"lap": [2], "time": [89.7]}, None], [
                ("b.json", {"y": 1})
            ]

        monkeypatch.setattr(session, "_fetch_laptime_payloads_async", fake_fetch_async)
        monkeypatch.setattr(session, "_should_backfill_ultra_cold_cache", lambda enabled: enabled)
        scheduled: list[dict] = []
        monkeypatch.setattr(
            session, "_schedule_background_cache_fill", lambda **kwargs: scheduled.append(kwargs)
        )

        out = session._find_fastest_lap_reference_from_raw(drivers, ultra_cold=True)
        assert out == ("VER", 2)

        out_async = asyncio.run(
            session._find_fastest_lap_reference_from_raw_async(drivers, ultra_cold=True)
        )
        assert out_async == ("HAM", 2)

        monkeypatch.setattr(
            session,
            "_load_drivers_for_fastest_lap_reference",
            lambda ultra_cold=False: (drivers, [("drivers.json", {"drivers": drivers})]),  # noqa: ARG005
        )
        refs = session._get_fastest_lap_refs_from_raw(ultra_cold=True)
        assert refs[0] in {("VER", 2), ("HAM", 1)}
        refs_async = asyncio.run(session._get_fastest_lap_refs_from_raw_async(ultra_cold=True))
        assert refs_async
        assert scheduled

    def test_laps_property_polars_and_laps_async_branches(self, monkeypatch):
        pl = pytest.importorskip("polars")
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="polars")

        async def fake_laps_async():
            return pl.DataFrame({"Driver": ["VER"], "LapNumber": [1], "LapTime": [90.0]})

        monkeypatch.setattr(session, "laps_async", fake_laps_async)
        laps = session.laps
        assert laps.height == 1

        empty_session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        empty_session._drivers = []
        out = asyncio.run(empty_session.laps_async())
        assert out.empty

        no_req_session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        no_req_session._drivers = [{"driver": "VER", "team": "Red Bull"}]
        monkeypatch.setattr(
            no_req_session,
            "_build_driver_laptime_requests",
            lambda driver_pool=None: [],  # noqa: ARG005
        )
        no_req_out = asyncio.run(no_req_session.laps_async())
        assert no_req_out.empty

    def test_get_telemetry_df_for_ref_branches(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
        session._session_cache_available = lambda: True
        session._should_skip_telemetry_fetch = lambda _driver: False
        session._prefetch_all_loaded_laps_telemetry = lambda ultra_cold=False: None  # noqa: ARG005
        session._fetch_json = lambda path: {}  # noqa: ARG005

        class FakeCache:
            def get_telemetry(self, year, gp, session_name, driver, lap_num):  # noqa: ARG002
                if driver == "HAM":
                    return {"speed": [290.0]}
                return None

            def set_telemetry(self, year, gp, session_name, driver, lap_num, payload):  # noqa: ARG002
                return None

        monkeypatch.setattr("tif1.core.get_cache", lambda: FakeCache())

        tel_df = session._get_telemetry_df_for_ref("VER", 1, ultra_cold=False)
        assert tel_df.empty

        session._remember_telemetry_payload("VER", 2, {"speed": [300.0]})
        tel2 = session._get_telemetry_df_for_ref("VER", 2, ultra_cold=False)
        assert not tel2.empty
        ham_df = session._get_telemetry_df_for_ref("HAM", 3, ultra_cold=False)
        assert not ham_df.empty

        session._should_skip_telemetry_fetch = lambda _driver: True
        skipped = session._get_telemetry_df_for_ref("ALO", 4, ultra_cold=False)
        assert skipped.empty

        session._should_skip_telemetry_fetch = lambda _driver: False
        session._fetch_json = lambda path: {"tel": {"speed": [301.0]}}  # noqa: ARG005
        fetched = session._get_telemetry_df_for_ref("NOR", 5, ultra_cold=False)
        assert not fetched.empty

        scheduled: list[dict] = []
        session._fetch_json_unvalidated = lambda path: {"tel": {"speed": [302.0]}}  # noqa: ARG005
        session._should_backfill_ultra_cold_cache = lambda enabled: enabled
        session._schedule_background_cache_fill = lambda **kwargs: scheduled.append(kwargs)
        ultra = session._get_telemetry_df_for_ref("PIA", 6, ultra_cold=True)
        assert not ultra.empty
        assert scheduled

        session._fetch_json = lambda path: {}  # noqa: ARG005
        empty_payload = session._get_telemetry_df_for_ref("SAI", 7, ultra_cold=False)
        assert empty_payload.empty

    def test_get_fastest_lap_tel_branches(self):
        session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
        session._resolve_ultra_cold_mode = lambda value: bool(value)
        session._session_cache_available = lambda: False
        session._is_fastest_lap_tel_cold_start = lambda: True

        session._get_fastest_lap_reference = lambda ultra_cold=False: None  # noqa: ARG005
        empty_first = session.get_fastest_lap_tel()
        assert empty_first.empty
        assert session._fastest_lap_tel_ref is None

        session._get_fastest_lap_reference = lambda ultra_cold=False: ("VER", 1)  # noqa: ARG005
        session._get_telemetry_df_for_ref = lambda driver, lap_num, ultra_cold=False: pd.DataFrame()  # noqa: ARG005
        empty_ref = session.get_fastest_lap_tel()
        assert empty_ref.empty
        assert session._fastest_lap_tel_ref is None

        non_empty = pd.DataFrame({"Driver": ["VER"], "LapNumber": [1], "Speed": [300.0]})
        session._get_telemetry_df_for_ref = (
            lambda driver, lap_num, ultra_cold=False: non_empty  # noqa: ARG005
        )
        first = session.get_fastest_lap_tel()
        second = session.get_fastest_lap_tel()
        assert first is second

    def test_get_fastest_lap_tel_async_branches(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
        session._resolve_ultra_cold_mode = lambda value: bool(value)
        session._session_cache_available = lambda: False
        session._is_fastest_lap_tel_cold_start = lambda: True

        async def no_ref(ultra_cold=False):
            return None

        session._get_fastest_lap_reference_async = no_ref
        first_empty = asyncio.run(session.get_fastest_lap_tel_async())
        assert first_empty.empty

        async def with_ref(ultra_cold=False):
            return ("VER", 1)

        session._get_fastest_lap_reference_async = with_ref
        session._fetch_telemetry_batch_from_refs = lambda refs, skip_cache=False: (  # noqa: ARG005
            [(2025, "Test GP", "Race", "VER/1_tel.json")],
            [("VER", 1)],
            [],
        )
        session._process_telemetry_results = lambda results, lap_info, tels, ultra_cold=False: [  # noqa: ARG005
            pd.DataFrame({"Driver": ["VER"], "LapNumber": [1], "Speed": [300.0]})
        ]

        async def compat_fetch(requests, **kwargs):
            if "use_cache" in kwargs:
                raise TypeError("unexpected keyword argument")
            return [{"tel": {"speed": [300.0]}}]

        monkeypatch.setattr("tif1.core.fetch_multiple_async", compat_fetch)
        async_tel = asyncio.run(session.get_fastest_lap_tel_async())
        assert not async_tel.empty

        async def failing_fetch(requests, **kwargs):
            raise RuntimeError("boom")

        session._fetch_telemetry_batch_from_refs = lambda refs, skip_cache=False: (  # noqa: ARG005
            [(2025, "Test GP", "Race", "VER/1_tel.json")],
            [("VER", 1)],
            [],
        )
        session._fastest_lap_tel_ref = None
        session._fastest_lap_tel_df = None
        monkeypatch.setattr("tif1.core.fetch_multiple_async", failing_fetch)
        with pytest.raises(RuntimeError, match="boom"):
            asyncio.run(session.get_fastest_lap_tel_async(ultra_cold=True))

    def test_fetch_all_laps_telemetry_async_paths(self, monkeypatch):
        session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
        session._resolve_telemetry_ultra_cold_mode = lambda value: bool(value)

        async def empty_laps():
            return pd.DataFrame()

        session.laps_async = empty_laps
        assert asyncio.run(session.fetch_all_laps_telemetry_async()) == {}

        async def laps_with_rows():
            return pd.DataFrame(
                {
                    "Driver": ["VER", "HAM"],
                    "LapNumber": [1, 2],
                }
            )

        session.laps_async = laps_with_rows
        cached_tel = pd.DataFrame({"Driver": ["VER"], "LapNumber": [1], "Speed": [300.0]})

        async def fake_batch(refs, skip_cache=False):
            return [(2025, "Test GP", "Race", "HAM/2_tel.json")], [("HAM", 2)], [cached_tel]

        session._fetch_telemetry_batch_from_refs_async = fake_batch
        remembered: list[tuple[str, int]] = []
        session._remember_telemetry_payload = lambda driver, lap_num, _payload: remembered.append(
            (driver, lap_num)
        )

        class CacheWriter:
            def set_telemetry(self, year, gp, session_name, driver, lap_num, data):  # noqa: ARG002
                return None

        monkeypatch.setattr("tif1.core.get_cache", lambda: CacheWriter())

        async def fake_fetch_multiple_async(requests, **kwargs):
            return [{"tel": {"speed": [290.0]}}]

        monkeypatch.setattr("tif1.async_fetch.fetch_multiple_async", fake_fetch_multiple_async)
        tel_map = asyncio.run(session.fetch_all_laps_telemetry_async())
        assert ("VER", 1) in tel_map
        assert ("HAM", 2) in tel_map
        assert ("HAM", 2) in remembered
