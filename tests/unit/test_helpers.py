"""Tests for core_utils helpers and json_utils modules."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from tif1.core_utils import helpers as helpers_module
from tif1.core_utils.helpers import (
    _apply_categorical,
    _check_cached_telemetry,
    _create_empty_df,
    _create_telemetry_df,
    _encode_url_component,
    _filter_valid_laptimes,
    _get_lap_number,
    _is_empty_df,
    _normalize_row_iteration,
    _rename_columns,
    _validate_drivers_list,
    _validate_lap_number,
    _validate_string_param,
    _validate_year,
)
from tif1.core_utils.json_utils import json_dumps, json_loads, parse_response_json


class TestValidateYear:
    """Tests for _validate_year."""

    def test_valid_year(self):
        _validate_year(2020, 2018, 2025)

    def test_min_boundary(self):
        _validate_year(2018, 2018, 2025)

    def test_max_boundary(self):
        _validate_year(2025, 2018, 2025)

    def test_below_min(self):
        with pytest.raises(ValueError, match="must be between"):
            _validate_year(2017, 2018, 2025)

    def test_above_max(self):
        with pytest.raises(ValueError, match="must be between"):
            _validate_year(2026, 2018, 2025)


class TestValidateDriversList:
    """Tests for _validate_drivers_list."""

    def test_none_is_valid(self):
        _validate_drivers_list(None)

    def test_valid_list(self):
        _validate_drivers_list(["VER", "HAM"])

    def test_not_a_list(self):
        with pytest.raises(TypeError, match="must be a list"):
            _validate_drivers_list("VER")  # type: ignore[arg-type]

    def test_empty_list(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            _validate_drivers_list([])

    def test_list_with_empty_string(self):
        with pytest.raises(ValueError, match="non-empty strings"):
            _validate_drivers_list(["VER", ""])

    def test_list_with_non_string(self):
        with pytest.raises(ValueError, match="non-empty strings"):
            _validate_drivers_list(["VER", 123])  # type: ignore[list-item]


class TestValidateLapNumber:
    """Tests for _validate_lap_number."""

    def test_valid_lap(self):
        _validate_lap_number(1)

    def test_not_int(self):
        with pytest.raises(TypeError, match="must be an integer"):
            _validate_lap_number(1.5)  # type: ignore[arg-type]

    def test_zero(self):
        with pytest.raises(ValueError, match="must be positive"):
            _validate_lap_number(0)

    def test_negative(self):
        with pytest.raises(ValueError, match="must be positive"):
            _validate_lap_number(-1)


class TestValidateStringParam:
    """Tests for _validate_string_param."""

    def test_valid_string(self):
        _validate_string_param("hello", "param")

    def test_not_a_string(self):
        with pytest.raises(TypeError, match="must be a string"):
            _validate_string_param(123, "param")  # type: ignore[arg-type]

    def test_empty_string(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            _validate_string_param("", "param")

    def test_whitespace_only(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            _validate_string_param("   ", "param")


class TestEncodeUrlComponent:
    """Tests for _encode_url_component."""

    def test_simple_string(self):
        assert _encode_url_component("hello") == "hello"

    def test_space(self):
        assert _encode_url_component("hello world") == "hello%20world"

    def test_special_chars(self):
        assert _encode_url_component("a/b") == "a%2Fb"

    def test_unicode(self):
        result = _encode_url_component("São Paulo")
        assert "S" in result
        assert "%C3%A3o" in result

    def test_caching(self):
        result1 = _encode_url_component("cached")
        result2 = _encode_url_component("cached")
        assert result1 == result2


class TestIsEmptyDf:
    """Tests for _is_empty_df."""

    def test_empty_pandas(self):
        assert _is_empty_df(pd.DataFrame(), "pandas") is True

    def test_non_empty_pandas(self):
        dataframe = pd.DataFrame({"a": [1]})
        assert _is_empty_df(dataframe, "pandas") is False

    def test_empty_pandas_with_polars_backend(self):
        assert _is_empty_df(pd.DataFrame(), "polars") is True

    def test_non_empty_pandas_with_polars_backend(self):
        dataframe = pd.DataFrame({"a": [1]})
        assert _is_empty_df(dataframe, "polars") is False


class TestCreateEmptyDf:
    """Tests for _create_empty_df."""

    def test_pandas(self):
        empty_dataframe = _create_empty_df("pandas")
        assert isinstance(empty_dataframe, pd.DataFrame)
        assert empty_dataframe.empty


class TestFilterValidLaptimes:
    """Tests for _filter_valid_laptimes."""

    def test_valid_laptimes_pandas(self):
        laps = pd.DataFrame({"LapTime": ["90.5", "91.2", "92.0"], "Driver": ["A", "B", "C"]})
        result = _filter_valid_laptimes(laps, "pandas")
        assert len(result) == 3
        assert pd.api.types.is_timedelta64_ns_dtype(result["LapTime"])
        assert result["LapTimeSeconds"].tolist() == [90.5, 91.2, 92.0]

    def test_empty_result_pandas(self):
        laps = pd.DataFrame({"LapTime": ["None", "invalid"], "Driver": ["A", "B"]})
        result = _filter_valid_laptimes(laps, "pandas")
        assert result.empty

    def test_mixed_valid_invalid_pandas(self):
        laps = pd.DataFrame({"LapTime": ["90.5", "None", "92.0"], "Driver": ["A", "B", "C"]})
        result = _filter_valid_laptimes(laps, "pandas")
        assert len(result) == 2
        assert pd.api.types.is_timedelta64_ns_dtype(result["LapTime"])
        assert result["LapTimeSeconds"].tolist() == [90.5, 92.0]

    def test_mixed_valid_invalid_polars(self):
        pl = pytest.importorskip("polars")
        laps = pl.DataFrame({"LapTime": ["90.5", "None", "92.0"], "Driver": ["A", "B", "C"]})

        result = _filter_valid_laptimes(laps, "polars")

        assert len(result) == 2
        assert result["LapTimeSeconds"].dtype == pl.Float64

    def test_numeric_laptimes_polars(self):
        pl = pytest.importorskip("polars")
        laps = pl.DataFrame({"LapTime": [90.5, 91.2, 92.0], "Driver": ["A", "B", "C"]})

        result = _filter_valid_laptimes(laps, "polars")

        assert len(result) == 3
        assert result["LapTimeSeconds"].dtype == pl.Float64

    def test_mixed_type_laptimes_polars(self):
        pl = pytest.importorskip("polars")
        laps = pl.DataFrame(
            {
                "LapTime": ["89.123", 90.5, None, "None", "invalid"],
                "Driver": ["A", "B", "C", "D", "E"],
            },
            strict=False,
        )

        result = _filter_valid_laptimes(laps, "polars")

        assert len(result) == 2
        assert result["LapTimeSeconds"].to_list() == [89.123, 90.5]

    def test_polars_reload_after_stale_module_state(self, monkeypatch):
        pl = pytest.importorskip("polars")
        monkeypatch.setattr(helpers_module, "pl", None, raising=True)
        monkeypatch.setattr(helpers_module, "POLARS_AVAILABLE", False, raising=True)
        laps = pl.DataFrame({"LapTime": ["90.5", None], "Driver": ["A", "B"]})

        result = helpers_module._filter_valid_laptimes(laps, "polars")

        assert len(result) == 1
        assert helpers_module.POLARS_AVAILABLE is True
        assert helpers_module.pl is not None


class TestRenameColumns:
    """Tests for _rename_columns."""

    def test_matching_columns_pandas(self):
        lap_dataframe = pd.DataFrame({"time": [1], "speed": [200]})
        result = _rename_columns(lap_dataframe, {"time": "Time", "speed": "Speed"}, "pandas")
        assert list(result.columns) == ["Time", "Speed"]

    def test_no_matching_columns_pandas(self):
        lap_dataframe = pd.DataFrame({"a": [1], "b": [2]})
        result = _rename_columns(lap_dataframe, {"time": "Time"}, "pandas")
        assert list(result.columns) == ["a", "b"]

    def test_partial_match_pandas(self):
        lap_dataframe = pd.DataFrame({"time": [1], "b": [2]})
        result = _rename_columns(lap_dataframe, {"time": "Time", "speed": "Speed"}, "pandas")
        assert list(result.columns) == ["Time", "b"]


class TestApplyCategorical:
    """Tests for _apply_categorical."""

    def test_pandas_categorical(self):
        driver_dataframe = pd.DataFrame({"Driver": ["VER", "HAM"], "Speed": [300, 290]})
        result = _apply_categorical(driver_dataframe, ["Driver"], "pandas")
        assert result["Driver"].dtype.name == "category"
        assert result["Speed"].dtype != "category"

    def test_nonexistent_column_pandas(self):
        driver_dataframe = pd.DataFrame({"Driver": ["VER"]})
        result = _apply_categorical(driver_dataframe, ["Team"], "pandas")
        assert list(result.columns) == ["Driver"]


class TestGetLapNumber:
    """Tests for _get_lap_number."""

    def test_lap_number_key(self):
        assert _get_lap_number({"LapNumber": 5}) == 5

    def test_lap_key(self):
        assert _get_lap_number({"lap": "10"}) == 10

    def test_none_value(self):
        with pytest.raises(ValueError, match="No lap number found"):
            _get_lap_number({})

    def test_invalid_value(self):
        with pytest.raises(ValueError, match="Invalid lap number"):
            _get_lap_number({"LapNumber": "abc"})


class TestCreateTelemetryDf:
    """Tests for _create_telemetry_df."""

    def test_valid_data_pandas(self):
        tel_data = {"speed": [200, 210], "rpm": [10000, 11000]}
        result = _create_telemetry_df(tel_data, "VER", 1, "pandas")
        assert result is not None
        assert "Driver" in result.columns
        assert "LapNumber" in result.columns
        assert "Speed" in result.columns
        assert "RPM" in result.columns
        assert (result["Driver"] == "VER").all()
        assert (result["LapNumber"] == 1).all()

    def test_renames_extended_telemetry_columns(self):
        tel_data = {
            "time": [0.0, 0.1],
            "speed": [200.0, 210.0],
            "driver_ahead": ["VER", "VER"],
            "distance_to_driver_ahead": [12.0, 11.6],
            "data_key": ["k1", "k2"],
        }
        result = _create_telemetry_df(tel_data, "VER", 1, "pandas")
        assert result is not None
        assert "DriverAhead" in result.columns
        assert "DistanceToDriverAhead" in result.columns
        assert "DataKey" in result.columns
        assert result["DriverAhead"].tolist() == ["VER", "VER"]
        assert result["DistanceToDriverAhead"].tolist() == [12.0, 11.6]
        assert result["DataKey"].tolist() == ["k1", "k2"]

    def test_empty_data(self):
        result = _create_telemetry_df({}, "VER", 1, "pandas")
        assert result is None

    def test_none_data(self):
        result = _create_telemetry_df(None, "VER", 1, "pandas")
        assert result is None

    def test_mismatched_lengths_are_normalized(self):
        result = _create_telemetry_df({"a": [1], "b": [1, 2]}, "VER", 1, "pandas")
        assert result is not None
        assert len(result) == 2
        assert result["a"].iloc[0] == 1
        assert pd.isna(result["a"].iloc[1])


class TestCheckCachedTelemetry:
    """Tests for _check_cached_telemetry."""

    def test_cache_hit(self):
        cache = MagicMock()
        cache.get_telemetry.return_value = {"speed": [200], "rpm": [10000]}
        result = _check_cached_telemetry(cache, 2024, "Monaco", "Race", "VER", 1, "pandas")
        assert result is not None
        assert "Speed" in result.columns
        cache.get_telemetry.assert_called_once_with(2024, "Monaco", "Race", "VER", 1)

    def test_cache_miss(self):
        cache = MagicMock()
        cache.get_telemetry.return_value = None
        result = _check_cached_telemetry(cache, 2024, "Monaco", "Race", "VER", 1, "pandas")
        assert result is None


class TestNormalizeRowIteration:
    """Tests for _normalize_row_iteration."""

    def test_pandas_iteration(self):
        table = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        rows = list(_normalize_row_iteration(table, "pandas"))
        assert len(rows) == 2
        assert rows[0]["a"] == 1
        assert rows[1]["b"] == 4


class TestJsonLoads:
    """Tests for json_loads."""

    def test_str_input(self):
        assert json_loads('{"key": "value"}') == {"key": "value"}

    def test_bytes_input(self):
        assert json_loads(b'{"key": "value"}') == {"key": "value"}

    def test_bytearray_input(self):
        assert json_loads(bytearray(b'{"key": 1}')) == {"key": 1}

    def test_memoryview_input(self):
        data = b'{"key": true}'
        assert json_loads(memoryview(data)) == {"key": True}

    def test_list_input(self):
        assert json_loads("[1, 2, 3]") == [1, 2, 3]


class TestJsonDumps:
    """Tests for json_dumps."""

    def test_dict(self):
        result = json_dumps({"key": "value"})
        assert isinstance(result, str)
        assert '"key"' in result
        assert '"value"' in result

    def test_list(self):
        result = json_dumps([1, 2, 3])
        assert isinstance(result, str)

    def test_roundtrip(self):
        data = {"a": 1, "b": [2, 3], "c": True}
        assert json_loads(json_dumps(data)) == data


class TestParseResponseJson:
    """Tests for parse_response_json."""

    def test_bytes_content(self):
        response = MagicMock()
        response.content = b'{"result": 42}'
        assert parse_response_json(response) == {"result": 42}

    def test_fallback_to_json_method(self):
        response = MagicMock()
        response.content = None
        response.json.return_value = {"fallback": True}
        assert parse_response_json(response) == {"fallback": True}
        response.json.assert_called_once()

    def test_no_content_attribute(self):
        response = MagicMock(spec=[])
        response.json = MagicMock(return_value={"no_content": True})
        assert parse_response_json(response) == {"no_content": True}
