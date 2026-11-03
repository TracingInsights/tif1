"""Contract tests for Delphi D7: the pandas ``Driver`` fast paths.

``get_driver(d)`` reads the driver's results row by position (the values
``BlockManager.fast_xs`` interleaves, never a boolean take of the frame),
``Driver.laps`` slices the session table when the driver's laps are one run
of rows, and ``Driver.get_fastest_lap`` picks the lap by ``argmin`` on the
valid lap times. Each must return exactly what the generic pandas path
returns: same values and scalar types, dtypes, index, name and class.
"""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from tif1 import models
from tif1.core_utils.ergast_results import finalize_results_frame
from tif1.core_utils.helpers import _filter_valid_laptimes, _not_deleted_mask
from tif1.models import (
    Driver,
    DriverResult,
    Laps,
    SessionResults,
    _driver_laps_frame,
    _driver_row_index,
    _driver_rows_mask,
    _fastest_lap_frame,
    _results_row_data,
)

# -- fixtures -----------------------------------------------------------------


def _results_frame() -> SessionResults:
    frame = SessionResults(
        finalize_results_frame(
            [
                {
                    "DriverNumber": "1",
                    "Abbreviation": "VER",
                    "Position": 1.0,
                    "Time": pd.Timedelta(seconds=5400.5),
                    "Points": 25.0,
                    "Status": "Finished",
                },
                {
                    "DriverNumber": "16",
                    "Abbreviation": "LEC",
                    "Position": 2.0,
                    "Time": pd.Timedelta(seconds=5412.25),
                    "Points": 18.0,
                    "Status": "Finished",
                },
                {"DriverNumber": "10", "Abbreviation": "fry", "Position": 3.0},
                {"DriverNumber": "12", "Abbreviation": "fry", "Position": 7.0},
            ]
        )
    )
    return frame


def _reference_row_data(results: pd.DataFrame, driver: str, driver_number: str):
    """The boolean-take reference: ``results[mask].iloc[0]`` and ``row.items()``."""
    mask = (results["Abbreviation"] == driver).to_numpy()
    numbers = results["DriverNumber"].astype(str).to_numpy() == driver_number
    if mask.sum() > 1:
        mask = mask & numbers
    elif not mask.any() and driver_number:
        mask = numbers
    if not mask.any():
        return None
    row = results[mask].iloc[0]
    return row.name, {str(key): value for key, value in row.items()}


def _assert_same_row_data(got, expected) -> None:
    assert got is not None
    assert expected is not None
    assert got[0] == expected[0]
    assert type(got[0]) is type(expected[0])
    assert list(got[1]) == list(expected[1])
    for key, value in expected[1].items():
        other = got[1][key]
        assert type(other) is type(value), key
        assert other is value or other == value or (pd.isna(other) and pd.isna(value)), key


def _laps_frame(
    *,
    unit: str = "ns",
    deleted: str = "boolean",
    driver_dtype: str = "category",
    drivers: list[str] | None = None,
) -> Laps:
    drivers = drivers or ["VER"] * 3 + ["LEC"] * 4 + ["VER"] * 0 + ["NOR"] * 2
    n = len(drivers)
    seconds = np.array([91.5, 90.25, np.nan, 90.25, 89.75, 92.0, 89.75, 95.0, 93.0][:n])
    lap_time = pd.to_timedelta(seconds, unit="s").astype(f"timedelta64[{unit}]")
    if deleted == "boolean":
        deleted_col = pd.array([False, False, True, False, True, None, False, None, False][:n])
    elif deleted == "bool":
        deleted_col = np.array([False, False, True, False, True, False, False, False, False][:n])
    else:
        deleted_col = [False, False, True, False, True, None, False, None, False][:n]
    frame = pd.DataFrame(
        {
            "Driver": pd.Series(drivers, dtype=driver_dtype),
            "LapNumber": np.arange(1, n + 1, dtype=float),
            "LapTime": lap_time,
            "Deleted": deleted_col,
            "Compound": pd.Series(["SOFT", "MEDIUM", "HARD"] * 3, dtype=object)[:n].to_numpy(),
            "Stint": np.ones(n, dtype="int64"),
        }
    )
    laps = Laps(frame)
    laps.session = SimpleNamespace(lib="pandas")
    return laps


def _reference_fastest(laps: pd.DataFrame) -> pd.DataFrame:
    valid = _filter_valid_laptimes(laps, "pandas", exclude_deleted=True)
    if valid.empty:
        return pd.DataFrame()
    return valid.nsmallest(1, "LapTimeSeconds").reset_index(drop=True)


def _assert_same_frame(got: pd.DataFrame, expected: pd.DataFrame) -> None:
    assert type(got) is type(expected)
    assert list(got.columns) == list(expected.columns)
    assert got.index.equals(expected.index)
    assert type(got.index) is type(expected.index)
    pd.testing.assert_frame_equal(got, expected, check_exact=True)
    for column in expected.columns:
        assert got[column].dtype == expected[column].dtype, column


# -- the results row ----------------------------------------------------------


class TestResultsRowData:
    @pytest.mark.parametrize(
        ("driver", "number"),
        [("VER", "1"), ("LEC", "16"), ("fry", "12"), ("fry", "10"), ("ZZZ", "16"), ("VER", "")],
    )
    def test_matches_the_boolean_take(self, driver: str, number: str) -> None:
        results = _results_frame()
        session = SimpleNamespace(results=results)
        _assert_same_row_data(
            _results_row_data(session, driver, number), _reference_row_data(results, driver, number)
        )

    def test_scalar_types_are_the_interleaved_ones(self) -> None:
        results = _results_frame()
        _, row = _results_row_data(SimpleNamespace(results=results), "VER", "1")
        assert type(row["Position"]) is np.float64
        assert type(row["Time"]) is pd.Timedelta
        assert row["Q1"] is pd.NaT
        assert type(row["Abbreviation"]) is str

    def test_unknown_driver_and_number(self) -> None:
        results = _results_frame()
        assert _results_row_data(SimpleNamespace(results=results), "ZZZ", "99") is None
        assert _results_row_data(SimpleNamespace(results=results), "ZZZ", "") is None

    def test_no_results(self) -> None:
        assert _results_row_data(SimpleNamespace(results=pd.DataFrame()), "VER", "1") is None
        assert _results_row_data(SimpleNamespace(results=None), "VER", "1") is None

    def test_pandas_takes_the_row_when_the_fast_path_is_off(self, monkeypatch) -> None:
        results = _results_frame()
        monkeypatch.setattr(models, "_FAST_LAP_ROWS", False)
        session = SimpleNamespace(results=results)
        _assert_same_row_data(
            _results_row_data(session, "LEC", "16"), _reference_row_data(results, "LEC", "16")
        )

    def test_single_block_frame_goes_through_pandas(self) -> None:
        results = pd.DataFrame(
            {"DriverNumber": ["1", "2"], "Abbreviation": ["VER", "LEC"], "Status": ["a", "b"]},
            index=pd.Index(["1", "2"]),
        )
        session = SimpleNamespace(results=results)
        _assert_same_row_data(
            _results_row_data(session, "LEC", "2"), _reference_row_data(results, "LEC", "2")
        )

    def test_results_column_reads_keep_the_driver_result_class(self) -> None:
        results = _results_frame()
        results.session = "session"
        column = results["Abbreviation"]
        assert type(column) is DriverResult
        assert column.name == "Abbreviation"
        assert column.session == "session"
        taken = results.iloc[[1]]
        assert type(taken) is SessionResults
        assert taken.session == "session"
        row = results.iloc[1]
        assert type(row) is DriverResult
        assert row.name == results.index[1]


# -- the Driver Series --------------------------------------------------------


class TestDriverSeries:
    def _session(self):
        results = _results_frame()
        return SimpleNamespace(
            lib="pandas",
            results=results,
            _laps=None,
            _get_driver_info=lambda code: {
                "driver": code,
                "dn": {"VER": "1", "LEC": "16"}.get(code, ""),
                "fn": "Max",
                "ln": "Verstappen",
                "team": "Red Bull",
                "tc": "3671C6",
            },
        )

    def test_matches_the_dict_constructor(self) -> None:
        session = self._session()
        driver = Driver(session, "VER")
        label, data = _results_row_data(session, "VER", "1")
        # What ``Driver.__init__`` hands the constructor: the row, with the
        # identity filling the values the classification leaves empty.
        info = session._get_driver_info("VER")
        identity = {
            "DriverNumber": "1",
            "Abbreviation": "VER",
            "TeamName": info["team"],
            "TeamColor": info["tc"],
            "FirstName": info["fn"],
            "LastName": info["ln"],
            "FullName": "Max Verstappen",
            "HeadshotUrl": "",
        }
        for key, value in identity.items():
            current = data.get(key)
            if key not in data or current is None or (isinstance(current, str) and not current):
                data[key] = value
        expected = pd.Series(data, name=label)
        assert driver.name == expected.name
        assert driver.dtype == expected.dtype
        assert driver.index.equals(expected.index)
        assert driver.index.dtype == expected.index.dtype
        assert list(driver.index) == list(expected.index)
        for value, other in zip(driver.array, expected.array, strict=True):
            assert type(value) is type(other)
            assert value is other or value == other or (pd.isna(value) and pd.isna(other))
        assert driver["TeamName"] == "Red Bull"
        assert driver["Position"] == 1.0
        assert driver.session is session
        assert driver.driver == "VER"
        assert driver._laps is None
        assert driver._lap_index_map is None

    def test_index_name_is_not_shared_between_drivers(self) -> None:
        session = self._session()
        first = Driver(session, "VER")
        second = Driver(session, "LEC")
        first.index.name = "field"
        assert second.index.name is None
        assert Driver(session, "VER").index.name is None
        assert _driver_row_index(("a", "b")).equals(pd.Index(("a", "b")))
        assert _driver_row_index(("a", "b")) is not _driver_row_index(("a", "b"))

    def test_reconstructed_series_resets_the_lookup_state(self) -> None:
        session = self._session()
        driver = Driver(session, "VER")
        driver._lap_index_map = {1: 0}
        subset = driver[["Abbreviation", "Position"]]
        assert type(subset) is Driver
        assert subset.session is session
        assert subset.driver == "VER"
        assert subset._lap_index_map is None
        assert subset._laps is None


# -- the driver's laps --------------------------------------------------------


class TestDriverLapsFrame:
    @pytest.mark.parametrize("driver_dtype", ["category", "object", "str"])
    @pytest.mark.parametrize("code", ["VER", "LEC", "NOR", "PIA"])
    def test_matches_the_boolean_take(self, driver_dtype: str, code: str) -> None:
        laps = _laps_frame(driver_dtype=driver_dtype)
        expected_mask = (laps["Driver"] == code).to_numpy(dtype=bool, na_value=False)
        np.testing.assert_array_equal(_driver_rows_mask(laps, code), expected_mask)
        _assert_same_frame(
            _driver_laps_frame(laps, code), laps[expected_mask].reset_index(drop=True)
        )

    def test_rows_spread_over_the_table(self) -> None:
        drivers = ["VER", "LEC", "VER", "LEC", "NOR", "VER", "NOR", "LEC", "VER"]
        laps = _laps_frame(drivers=drivers)
        for code in ("VER", "LEC", "NOR"):
            mask = (laps["Driver"] == code).to_numpy()
            _assert_same_frame(_driver_laps_frame(laps, code), laps[mask].reset_index(drop=True))

    def test_the_slice_is_isolated_from_the_table(self) -> None:
        laps = _laps_frame()
        sliced = _driver_laps_frame(laps, "LEC")
        assert type(sliced) is Laps
        sliced.loc[0, "LapNumber"] = 99.0
        sliced["Extra"] = 1
        assert laps["LapNumber"].tolist()[3] == 4.0
        assert "Extra" not in laps.columns
        assert sliced.index.equals(pd.RangeIndex(4))

    def test_driver_laps_property_uses_the_session_table(self) -> None:
        table = _laps_frame()
        session = SimpleNamespace(
            lib="pandas",
            results=_results_frame(),
            _laps=table,
            _get_driver_info=lambda code: {"driver": code, "dn": "16", "team": "Ferrari"},
            _driver_has_timing_data=lambda _code: True,
            _ergast_only=False,
        )
        driver = Driver(session, "LEC")
        laps = driver.laps
        assert type(laps) is Laps
        assert laps.session is session
        assert laps["Driver"].tolist() == ["LEC"] * 4
        assert laps.index.equals(pd.RangeIndex(4))
        assert driver.laps is laps


# -- the fastest lap ----------------------------------------------------------


class TestFastestLapFrame:
    @pytest.mark.parametrize("unit", ["ns", "us", "ms", "s"])
    @pytest.mark.parametrize("deleted", ["boolean", "bool", "object"])
    @pytest.mark.parametrize("code", ["VER", "LEC", "NOR"])
    def test_matches_the_filter_and_nsmallest(self, unit: str, deleted: str, code: str) -> None:
        laps = _driver_laps_frame(_laps_frame(unit=unit, deleted=deleted), code)
        got = _fastest_lap_frame(laps)
        assert got is not None
        _assert_same_frame(got, _reference_fastest(laps))

    def test_ties_go_to_the_first_row(self) -> None:
        laps = _driver_laps_frame(_laps_frame(), "LEC")
        # LEC: 90.25 (ok), 89.75 (deleted), 92.0 (Deleted NA: counts), 89.75 (ok)
        got = _fastest_lap_frame(laps)
        assert got["LapNumber"].tolist() == [7.0]
        assert got["LapTimeSeconds"].tolist() == [89.75]
        assert got["LapTimeSeconds"].dtype == "float64"
        _assert_same_frame(got, _reference_fastest(laps))

    @pytest.mark.parametrize("unit", ["ns", "ms"])
    def test_seconds_are_the_installed_total_seconds(self, unit: str) -> None:
        """``LapTimeSeconds`` follows ``total_seconds()`` bit for bit (pandas 3.1 rounds it
        through a divmod split, earlier versions as one division)."""
        rng = np.random.default_rng(7)
        for _ in range(50):
            ticks = rng.integers(80_000_000_000, 120_000_000_000, 6)
            lap_time = pd.Series(ticks.astype("m8[ns]")).astype(f"m8[{unit}]")
            laps = Laps({"LapTime": lap_time, "LapNumber": np.arange(1.0, 7.0)})
            got = _fastest_lap_frame(laps)
            assert got is not None
            best = int(lap_time.to_numpy().argmin())
            expected = lap_time.dt.total_seconds().to_numpy()[best]
            assert got["LapTimeSeconds"].to_numpy()[0] == expected

    def test_without_a_deleted_column(self) -> None:
        laps = _driver_laps_frame(_laps_frame(), "VER").drop(columns=["Deleted"])
        got = _fastest_lap_frame(laps)
        assert got["LapNumber"].tolist() == [2.0]
        _assert_same_frame(got, _reference_fastest(laps))

    def test_no_valid_lap_is_an_empty_frame(self) -> None:
        laps = _driver_laps_frame(_laps_frame(), "VER")
        laps["LapTime"] = pd.to_timedelta([np.nan] * 3, unit="s")
        got = _fastest_lap_frame(laps)
        assert got is not None
        assert got.empty
        # A datetime column (what a bare ``pd.NaT`` assignment makes) is not
        # a lap time: the generic filter decides.
        laps["LapTime"] = pd.NaT
        assert _fastest_lap_frame(laps) is None
        laps = _driver_laps_frame(_laps_frame(), "VER")
        laps["Deleted"] = True
        got = _fastest_lap_frame(laps)
        assert got is not None
        assert got.empty

    def test_numeric_lap_times_go_through_the_filter(self) -> None:
        laps = pd.DataFrame({"LapTime": [90.5, 91.0], "LapNumber": [1, 2]})
        assert _fastest_lap_frame(laps) is None
        assert _fastest_lap_frame(pd.DataFrame({"LapNumber": [1, 2]})) is None

    def test_the_row_is_isolated_from_the_laps(self) -> None:
        laps = _driver_laps_frame(_laps_frame(), "VER")
        row = _fastest_lap_frame(laps)
        row.loc[0, "LapNumber"] = 42.0
        assert laps["LapNumber"].tolist() == [1.0, 2.0, 3.0]
        assert "LapTimeSeconds" not in laps.columns

    def test_driver_get_fastest_lap(self) -> None:
        session = SimpleNamespace(
            lib="pandas",
            results=_results_frame(),
            _laps=_laps_frame(),
            _get_driver_info=lambda code: {"driver": code, "dn": "16", "team": "Ferrari"},
            _driver_has_timing_data=lambda _code: True,
            _ergast_only=False,
            _lap_time_sort_column=lambda _laps: "LapTimeSeconds",
        )
        driver = Driver(session, "LEC")
        fastest = driver.get_fastest_lap()
        assert type(fastest) is Laps
        assert fastest.session is session
        assert fastest["LapNumber"].tolist() == [7.0]
        _assert_same_frame(fastest, _reference_fastest(driver.laps))


class TestNotDeletedMask:
    def test_nullable_flags_match_the_fillna_path(self) -> None:
        laps = pd.DataFrame({"Deleted": pd.array([True, False, None, True])})
        expected = ~laps["Deleted"].astype("boolean").fillna(False).to_numpy(dtype=bool)
        np.testing.assert_array_equal(_not_deleted_mask(laps), expected)
        assert _not_deleted_mask(laps).dtype == bool
        assert _not_deleted_mask(pd.DataFrame({"LapNumber": [1]})) is None
