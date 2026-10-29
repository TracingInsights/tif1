"""fastf1 parity of the laps operations on both backends.

``get_weather_data`` (fastf1's per-lap rule on ``session.weather_data``),
``split_qualifying_sessions`` (fastf1's session-status algorithm and its
qualifying-session guard), ``iterlaps`` (every lap by default), the ``pick_*``
selectors (any iterable, fastf1's case handling), ``pick_fastest`` (personal
bests only) and the polars ``df.f1`` session guards and result-row names.
Every case runs on the pandas and the polars classes with the same synthetic
data and must give the same answer. Where fastf1 is installed its own
``Laps``/``Lap`` classes run on the same frames as the reference.
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pytest

from tif1.exceptions import DataNotFoundError, DriverNotFoundError
from tif1.models import Lap, Laps
from tif1.models_polars import (
    PolarsDriver,
    PolarsDriverResult,
    PolarsLap,
    PolarsLaps,
    PolarsSessionResults,
    PolarsTelemetry,
)

NS = pl.Duration("ns")
WEATHER_KEYS = [
    "Time",
    "AirTemp",
    "Humidity",
    "Pressure",
    "Rainfall",
    "TrackTemp",
    "WindDirection",
    "WindSpeed",
]


def _fastf1_core() -> Any:
    return pytest.importorskip("fastf1.core")


def _td(seconds: list[float | None]) -> pd.Series:
    values = [np.nan if value is None else value for value in seconds]
    return pd.Series(pd.to_timedelta(values, unit="s")).astype("timedelta64[ns]")


def _pl_td(seconds: list[float | None]) -> pl.Series:
    return pl.Series([None if s is None else dt.timedelta(seconds=s) for s in seconds]).cast(NS)


def _to_polars(frame: pd.DataFrame) -> pl.DataFrame:
    """The same laps as a polars frame (timedeltas as ``Duration("ns")``)."""
    columns: dict[str, pl.Series] = {}
    for name in frame.columns:
        values = frame[name]
        if pd.api.types.is_timedelta64_dtype(values):
            seconds = [None if pd.isna(v) else v.total_seconds() for v in values]
            columns[name] = _pl_td(seconds).alias(name)
        elif name == "LapNumber":  # Int64 in a polars session's laps
            columns[name] = pl.Series(name, [int(v) for v in values], dtype=pl.Int64)
        elif pd.api.types.is_bool_dtype(values) or str(values.dtype) == "boolean":
            columns[name] = pl.Series(
                name, [None if pd.isna(v) else bool(v) for v in values], dtype=pl.Boolean
            )
        else:
            columns[name] = pl.Series(name, [None if pd.isna(v) else v for v in values])
    return pl.DataFrame(columns)


def _both(frame: pd.DataFrame, session_pd: Any, session_pl: Any) -> tuple[Laps, PolarsLaps]:
    return Laps(frame, session=session_pd), PolarsLaps._from_frame(_to_polars(frame), session_pl)


# -- weather ---------------------------------------------------------------------------------


def _weather_pd(times: list[float | None]) -> pd.DataFrame:
    n = len(times)
    return pd.DataFrame(
        {
            "Time": _td(times),
            "AirTemp": [20.0 + i for i in range(n)],
            "Humidity": [50.0] * n,
            "Pressure": [1000.0] * n,
            "Rainfall": pd.array([False] * n, dtype="boolean"),
            "TrackTemp": [30.0] * n,
            "WindDirection": pd.array([90] * n, dtype="Int64"),
            "WindSpeed": [2.0] * n,
        }
    )


def _weather_pl(times: list[float | None]) -> pl.DataFrame:
    n = len(times)
    return pl.DataFrame(
        {
            "Time": _pl_td(times).cast(pl.Duration("ms")),  # the session table's unit
            "AirTemp": [20.0 + i for i in range(n)],
            "Humidity": [50.0] * n,
            "Pressure": [1000.0] * n,
            "Rainfall": [False] * n,
            "TrackTemp": [30.0] * n,
            "WindDirection": [90] * n,
            "WindSpeed": [2.0] * n,
        }
    )


#: Samples (AirTemp 20, 21, 22, 23, 24) at 90, 120, 150, 400 s and one without a time.
WEATHER_TIMES: list[float | None] = [90.0, 120.0, 150.0, 400.0, None]


def _weather_laps() -> pd.DataFrame:
    """Lap 1: a sample within (the first wins); 2: none within, the last before;
    3: nothing before its end; 4: no start (the last before its end); 5: no end;
    6: misaligned own weather columns that the session table overrides.
    """
    return pd.DataFrame(
        {
            "Driver": ["VER"] * 6,
            "LapNumber": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "LapStartTime": _td([100.0, 195.0, 0.0, None, 380.0, 395.0]).to_numpy(),
            "Time": _td([195.0, 290.0, 50.0, 410.0, None, 405.0]).to_numpy(),
            "LapTime": _td([95.0, 95.0, 50.0, None, 30.0, 10.0]).to_numpy(),
            "WeatherTime": _td([10.0] * 6).to_numpy(),
            "AirTemp": [99.0] * 6,
            "Humidity": [99.0] * 6,
            "Pressure": [99.0] * 6,
            "Rainfall": [True] * 6,
            "TrackTemp": [99.0] * 6,
            "WindDirection": [1] * 6,
            "WindSpeed": [9.0] * 6,
        },
        index=[10, 20, 30, 40, 50, 60],
    )


#: AirTemp of each lap of ``_weather_laps`` under fastf1's rule (``None``: no sample).
EXPECTED_AIR = [21.0, 22.0, None, 23.0, None, 23.0]
EXPECTED_TIME = [120.0, 150.0, None, 400.0, None, 400.0]


def _weather_sessions() -> tuple[Any, Any]:
    return (
        SimpleNamespace(weather_data=_weather_pd(WEATHER_TIMES)),
        SimpleNamespace(weather_data=_weather_pl(WEATHER_TIMES)),
    )


class TestWeather:
    def test_laps_follow_fastf1s_rule_on_the_session_table(self):
        laps_pd, laps_pl = _both(_weather_laps(), *_weather_sessions())
        weather_pd = laps_pd.get_weather_data()
        weather_pl = laps_pl.get_weather_data()
        assert list(weather_pd.columns) == WEATHER_KEYS == weather_pl.columns
        assert weather_pd.index.tolist() == [10, 20, 30, 40, 50, 60]  # the laps' index
        assert weather_pl.height == 6
        air_pd = [None if pd.isna(v) else v for v in weather_pd["AirTemp"]]
        assert air_pd == EXPECTED_AIR == weather_pl["AirTemp"].to_list()
        times_pd = [None if pd.isna(v) else v.total_seconds() for v in weather_pd["Time"]]
        times_pl = [None if v is None else v.total_seconds() for v in weather_pl["Time"]]
        assert times_pd == EXPECTED_TIME == times_pl
        assert weather_pl.schema["Time"] == NS
        # the session table's dtypes survive a lap without a sample
        assert str(weather_pd["Rainfall"].dtype) == "boolean"
        assert str(weather_pd["WindDirection"].dtype) == "Int64"

    def test_matches_fastf1s_lap_rule(self):
        ff = _fastf1_core()
        session_pd, _ = _weather_sessions()
        frame = _weather_laps()
        got = Laps(frame, session=session_pd).get_weather_data()
        for label, row in frame.iterrows():
            lap = ff.Lap(row)
            object.__setattr__(lap, "session", session_pd)
            expected = lap.get_weather_data()
            assert pd.isna(got.loc[label, "Time"]) == pd.isna(expected["Time"])
            if not pd.isna(expected["Time"]):
                assert got.loc[label, "Time"] == expected["Time"]
                assert got.loc[label, "AirTemp"] == expected["AirTemp"]

    def test_unsorted_table_and_duplicate_times(self):
        times = [400.0, 120.0, 150.0, 120.0]
        session_pd = SimpleNamespace(weather_data=_weather_pd(times))
        session_pl = SimpleNamespace(weather_data=_weather_pl(times))
        frame = pd.DataFrame(
            {"LapStartTime": _td([100.0, 130.0]), "Time": _td([195.0, 140.0])}, index=[0, 1]
        )
        laps_pd, laps_pl = _both(frame, session_pd, session_pl)
        # first within [100, 195]: the first 120 s row (AirTemp 21); none within
        # [130, 140]: the last row at or before 140 s is the second 120 s row (23).
        assert laps_pd.get_weather_data()["AirTemp"].tolist() == [21.0, 23.0]
        assert laps_pl.get_weather_data()["AirTemp"].to_list() == [21.0, 23.0]

    def test_without_a_session_table_the_laps_columns_answer(self):
        laps_pd, laps_pl = _both(_weather_laps(), None, None)
        assert laps_pd.get_weather_data()["AirTemp"].tolist() == [99.0] * 6
        assert laps_pl.get_weather_data()["AirTemp"].to_list() == [99.0] * 6
        empty_table = SimpleNamespace(weather_data=_weather_pd([]))
        assert (
            Laps(_weather_laps(), session=empty_table).get_weather_data()["AirTemp"].tolist()
            == [99.0] * 6
        )

    def test_without_any_weather_every_lap_has_a_typed_missing_row(self):
        frame = _weather_laps()[["Driver", "LapNumber", "LapStartTime", "Time"]]
        laps_pd, laps_pl = _both(frame, None, None)
        weather_pd = laps_pd.get_weather_data()
        weather_pl = laps_pl.get_weather_data()
        assert weather_pd.index.tolist() == frame.index.tolist()
        assert weather_pd.isna().all().all()
        assert weather_pd["Time"].dtype == "timedelta64[ns]"
        assert weather_pl.height == 6  # one row per lap, not a single Null row
        assert weather_pl.null_count().row(0) == (6,) * 8
        assert weather_pl.schema["AirTemp"] == pl.Float64
        assert weather_pl.schema["Rainfall"] == pl.Boolean

    def test_one_lap_returns_a_series_or_dict(self):
        laps_pd, laps_pl = _both(_weather_laps(), *_weather_sessions())
        row = laps_pd.loc[10].get_weather_data()
        assert isinstance(row, pd.Series)
        assert list(row.index) == WEATHER_KEYS
        assert row.name == 10
        assert row["AirTemp"] == 21.0
        sample = laps_pl.pick_laps(1).pick_fastest(only_by_time=True).get_weather_data()
        assert list(sample) == WEATHER_KEYS
        assert sample["AirTemp"] == 21.0
        assert sample["Time"] == dt.timedelta(seconds=120)

    def test_one_lap_without_a_sample_is_nan_or_none(self):
        laps_pd, laps_pl = _both(_weather_laps(), *_weather_sessions())
        row = laps_pd.loc[30].get_weather_data()
        assert list(row.index) == WEATHER_KEYS
        assert row.isna().all()
        assert row.dtype == "float64"  # fastf1's ``pd.Series(index=columns)``
        sample = laps_pl.pick_laps(3).pick_fastest(only_by_time=True).get_weather_data()
        assert sample == dict.fromkeys(WEATHER_KEYS)

    def test_a_lap_without_a_start_takes_the_last_sample_before_its_end(self):
        weather = SimpleNamespace(weather_data=_weather_pd([60.0, 120.0, 180.0, 240.0]))
        lap = Laps(
            pd.DataFrame({"LapStartTime": _td([None]), "Time": _td([230.0])}), session=weather
        ).iloc[0]
        lap.session = weather
        assert lap.get_weather_data()["Time"] == pd.Timedelta(seconds=180)


# -- split_qualifying_sessions -------------------------------------------------------------


def _quali_frame(starts: list[float], ends: list[float], pit_out: list[bool]) -> pd.DataFrame:
    n = len(starts)
    return pd.DataFrame(
        {
            "Driver": ["AAA"] * n,
            "DriverNumber": ["1"] * n,
            "LapNumber": [float(i + 1) for i in range(n)],
            "LapStartTime": _td(starts),
            "Time": _td(ends),
            "LapTime": _td([e - s for s, e in zip(starts, ends, strict=True)]),
            "PitOutTime": _td([s if out else None for s, out in zip(starts, pit_out, strict=True)]),
        }
    )


def _status_sessions(rows: list[tuple[float, str]], name: str = "Qualifying") -> tuple[Any, Any]:
    times = [time for time, _ in rows]
    states = [state for _, state in rows]
    return (
        SimpleNamespace(
            name=name, session_status=pd.DataFrame({"Time": _td(times), "Status": states})
        ),
        SimpleNamespace(
            name=name, session_status=pl.DataFrame({"Time": _pl_td(times), "Status": states})
        ),
    )


def _segments_pd(parts: list[Any]) -> list[list[float] | None]:
    return [None if part is None else part["LapNumber"].tolist() for part in parts]


def _segments_pl(parts: list[Any]) -> list[list[float] | None]:
    return [None if part is None else [float(n) for n in part["LapNumber"]] for part in parts]


def _split_both(frame: pd.DataFrame, session_pd: Any, session_pl: Any) -> list[list[float] | None]:
    laps_pd, laps_pl = _both(frame, session_pd, session_pl)
    got_pd = _segments_pd(laps_pd.split_qualifying_sessions())
    got_pl = _segments_pl(laps_pl.split_qualifying_sessions())
    assert got_pd == got_pl
    return got_pd


#: Q1 ends under a red flag and is not resumed; fastf1 resets on ``Finished``.
RED_FLAG_ENDED_Q1 = [
    (100.0, "Started"),
    (500.0, "Aborted"),
    (1100.0, "Finished"),
    (1200.0, "Started"),
    (2000.0, "Finished"),
    (2100.0, "Started"),
    (2700.0, "Finished"),
]


class TestSplitQualifyingSessions:
    @pytest.mark.parametrize("name", ["Race", "Sprint", "Practice 1"])
    def test_non_qualifying_sessions_raise(self, name):
        frame = _quali_frame([200.0], [290.0], [False])
        laps_pd, laps_pl = _both(
            frame.assign(QualifyingSession=["Q1"]),
            SimpleNamespace(name=name),
            SimpleNamespace(name=name),
        )
        for laps in (laps_pd, laps_pl):
            with pytest.raises(ValueError, match="Session is not a qualifying session!"):
                laps.split_qualifying_sessions()

    @pytest.mark.parametrize("name", ["Qualifying", "Sprint Qualifying", "Sprint Shootout"])
    def test_qualifying_like_sessions_use_the_marker_first(self, name):
        frame = _quali_frame([200.0, 1300.0], [290.0, 1390.0], [False, False])
        frame["QualifyingSession"] = ["Q1", "Q3"]
        sessions = _status_sessions(RED_FLAG_ENDED_Q1, name)
        assert _split_both(frame, *sessions) == [[1.0], None, [2.0]]

    def test_a_red_flag_ended_segment_is_cleared_by_finished(self):
        frame = _quali_frame([200.0, 1300.0, 2200.0], [290.0, 1390.0, 2290.0], [False] * 3)
        assert _split_both(frame, *_status_sessions(RED_FLAG_ENDED_Q1)) == [[1.0], [2.0], [3.0]]

    def test_matches_fastf1s_algorithm(self):
        ff = _fastf1_core()
        frame = _quali_frame([200.0, 1300.0, 2200.0], [290.0, 1390.0, 2290.0], [False] * 3)
        session_pd, _ = _status_sessions(RED_FLAG_ENDED_Q1)
        reference = SimpleNamespace(
            name="Qualifying",
            _QUALI_LIKE_SESSIONS=("Qualifying",),
            _session_split_times=None,
            session_status=session_pd.session_status,
        )
        expected = _segments_pd(ff.Laps(frame, session=reference).split_qualifying_sessions())
        assert _split_both(frame, *_status_sessions(RED_FLAG_ENDED_Q1)) == expected

    def test_a_restart_after_a_red_flag_continues_the_segment_and_early_laps_move(self):
        status = [
            (0.0, "Started"),
            (1000.0, "Finished"),
            (1500.0, "Started"),
            (1700.0, "Aborted"),
            (1900.0, "Started"),  # the red-flag restart: still Q2
            (2500.0, "Finished"),
            (3000.0, "Started"),
            (3600.0, "Finished"),
        ]
        # lap 4 is an out-lap that starts before Q3 and ends after it: Q3.
        frame = _quali_frame(
            [100.0, 1550.0, 1950.0, 2950.0, 3100.0],
            [190.0, 1640.0, 2040.0, 3050.0, 3190.0],
            [False, False, False, True, False],
        )
        assert _split_both(frame, *_status_sessions(status)) == [
            [1.0],
            [2.0, 3.0],
            [4.0, 5.0],
        ]

    def test_session_split_times_take_precedence(self):
        frame = _quali_frame([200.0, 1300.0, 2200.0], [290.0, 1390.0, 2290.0], [False] * 3)
        session_pd, session_pl = _status_sessions(RED_FLAG_ENDED_Q1)
        for session in (session_pd, session_pl):
            session._session_split_times = [pd.Timedelta(seconds=150), pd.Timedelta(seconds=2150)]
        assert _split_both(frame, session_pd, session_pl) == [[1.0, 2.0], [3.0], None]

    def test_gap_fallback_without_session_status_is_the_same_on_both_backends(self):
        # gaps of 6, 14 and 20 minutes: the two longest split.
        starts = [0.0, 360.0, 1200.0, 2400.0]
        frame = _quali_frame(starts, [s + 90.0 for s in starts], [False] * 4)
        named = (SimpleNamespace(name="Qualifying"), SimpleNamespace(name="Qualifying"))
        assert _split_both(frame, *named) == [[1.0, 2.0], [3.0], [4.0]]
        assert _split_both(frame, None, None) == [[1.0, 2.0], [3.0], [4.0]]
        short = _quali_frame([0.0, 100.0], [90.0, 190.0], [False, False])
        assert _split_both(short, None, None) == [[1.0, 2.0], None, None]


# -- iterlaps ----------------------------------------------------------------------------------


def _iter_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Driver": ["VER", "VER", "HAM"],
            "LapNumber": [1.0, 2.0, 1.0],
            "LapTime": _td([90.0, None, 91.0]),
            "PitInTime": _td([None, 100.0, None]),
        }
    )


class TestIterlaps:
    @pytest.mark.parametrize("require", [None, []])
    def test_every_lap_without_a_requirement(self, require):
        laps_pd, laps_pl = _both(_iter_frame(), None, None)
        got_pd = list(laps_pd.iterlaps(require))
        got_pl = list(laps_pl.iterlaps(require))
        assert [i for i, _ in got_pd] == [0, 1, 2] == [i for i, _ in got_pl]
        # every column stays on each lap, missing values included (iterrows rows)
        assert isinstance(got_pd[1][1], Lap)
        assert pd.isna(got_pd[1][1]["LapTime"])
        assert pd.isna(got_pd[0][1]["PitInTime"])
        assert got_pl[1][1]["LapTime"] is None

    def test_require_filters_and_a_missing_column_yields_nothing(self):
        laps_pd, laps_pl = _both(_iter_frame(), None, None)
        assert [i for i, _ in laps_pd.iterlaps(["LapTime"])] == [0, 2]
        assert [i for i, _ in laps_pl.iterlaps(["LapTime"])] == [0, 2]
        assert [i for i, _ in laps_pd.iterlaps(["PitInTime", "Driver"])] == [1]
        assert [i for i, _ in laps_pl.iterlaps(["PitInTime", "Driver"])] == [1]
        assert list(laps_pd.iterlaps(["Missing"])) == []
        assert list(laps_pl.iterlaps(["Missing"])) == []

    def test_matches_fastf1(self):
        ff = _fastf1_core()
        frame = _iter_frame()
        for require in (None, [], ["LapTime"], ["Missing"]):
            expected = [i for i, _ in ff.Laps(frame).iterlaps(require)]
            assert [i for i, _ in Laps(frame).iterlaps(require)] == expected


# -- pick_* ------------------------------------------------------------------------------------


def _pick_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Driver": ["VER", "VER", "HAM", "LEC"],
            "DriverNumber": ["1", "1", "44", "16"],
            "Team": ["Red Bull Racing", "Red Bull Racing", "Ferrari", "Ferrari"],
            "LapNumber": [1.0, 2.0, 1.0, 1.0],
            "LapTime": _td([92.0, 90.0, 91.0, 93.0]),
            "Compound": ["SOFT", "MEDIUM", "SOFT", "HARD"],
            "IsPersonalBest": [True, False, True, True],
            "Deleted": pd.array([False, False, False, False], dtype="boolean"),
        }
    )


def _heights(laps_pd: Laps, laps_pl: PolarsLaps, call: Any) -> int:
    got_pd = len(call(laps_pd))
    got_pl = call(laps_pl).height
    assert got_pd == got_pl
    return got_pd


class TestPickSelectors:
    @pytest.mark.parametrize(
        "identifiers",
        [
            ["VER", "HAM"],
            ("VER", "HAM"),
            {"VER", "HAM"},
            np.array(["VER", "HAM"]),
            pd.Series(["VER", "HAM"]),
            pl.Series(["VER", "HAM"]),
        ],
        ids=["list", "tuple", "set", "ndarray", "pd.Series", "pl.Series"],
    )
    def test_pick_drivers_accepts_any_iterable(self, identifiers):
        laps_pd, laps_pl = _both(_pick_frame(), None, None)
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_drivers(identifiers)) == 3

    def test_pick_drivers_accepts_a_generator_and_mixed_numbers(self):
        laps_pd, laps_pl = _both(_pick_frame(), None, None)
        assert len(laps_pd.pick_drivers(code for code in ["ver", 44])) == 3
        assert laps_pl.pick_drivers(code for code in ["ver", 44]).height == 3
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_drivers(np.int64(16))) == 1

    def test_case_handling_follows_fastf1(self):
        laps_pd, laps_pl = _both(_pick_frame(), None, None)
        # pick_drivers upper-cases codes; the deprecated pick_driver does not.
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_drivers("ver")) == 2
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_driver("ver")) == 0
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_driver("VER")) == 2
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_driver(44)) == 1
        # compounds are upper-cased, team names are not
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_compounds("soft")) == 2
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_compounds(["soft", "hard"])) == 3
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_tyre("medium")) == 1
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_teams("ferrari")) == 0
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_team("Ferrari")) == 2
        teams = (team for team in ["Ferrari", "Red Bull Racing"])
        assert len(laps_pd.pick_teams(teams)) == 4
        assert laps_pl.pick_teams(pl.Series(["Ferrari"])).height == 2

    def test_case_handling_matches_fastf1(self):
        ff = _fastf1_core()
        frame = _pick_frame()
        reference = ff.Laps(frame)
        laps = Laps(frame)
        assert len(laps.pick_drivers("ver")) == len(reference.pick_drivers("ver"))
        assert len(laps.pick_compounds("soft")) == len(reference.pick_compounds("soft"))
        assert len(laps.pick_teams("ferrari")) == len(reference.pick_teams("ferrari"))
        series = pd.Series(["VER", "HAM"])
        assert len(laps.pick_drivers(series)) == len(reference.pick_drivers(series))

    def test_pick_laps_takes_floats_and_rejects_fractions(self):
        laps_pd, laps_pl = _both(_pick_frame(), None, None)
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_laps(1.0)) == 3
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_laps(n for n in [2])) == 1
        for laps in (laps_pd, laps_pl):
            with pytest.raises(ValueError, match=r"Invalid value 1\.5"):
                laps.pick_laps([1.5])

    def test_pick_quicklaps_is_strictly_below_the_threshold(self):
        laps_pd, laps_pl = _both(_pick_frame(), None, None)
        # best 90 s: 1.0 keeps nothing, 92 / 90 keeps 90 and 91 (fastf1's "<").
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_quicklaps(1.0)) == 0
        assert _heights(laps_pd, laps_pl, lambda laps: laps.pick_quicklaps(92 / 90)) == 2


# -- pick_fastest ------------------------------------------------------------------------------


class TestPickFastest:
    def test_only_personal_bests_are_candidates(self):
        laps_pd, laps_pl = _both(_pick_frame(), None, None)
        # VER's 90.0 is not a personal best: HAM's 91.0 is the fastest.
        assert laps_pd.pick_fastest()["Driver"] == "HAM" == laps_pl.pick_fastest()["Driver"]
        assert laps_pd.pick_fastest(only_by_time=True)["LapTime"] == pd.Timedelta(seconds=90)
        assert laps_pl.pick_fastest(only_by_time=True)["LapTime"] == dt.timedelta(seconds=90)
        ver_pd = laps_pd.pick_drivers("VER").pick_fastest()
        ver_pl = laps_pl.pick_drivers("VER").pick_fastest()
        assert ver_pd["LapNumber"] == 1.0 == ver_pl["LapNumber"]
        assert ver_pd.name == 0

    def test_no_personal_best_is_none(self):
        frame = _pick_frame().assign(IsPersonalBest=pd.array([None] * 4, dtype="boolean"))
        laps_pd, laps_pl = _both(frame, None, None)
        assert laps_pd.pick_fastest() is None
        assert laps_pl.pick_fastest() is None
        assert laps_pd.pick_fastest(only_by_time=True)["LapNumber"] == 2.0
        assert laps_pl.pick_fastest(only_by_time=True)["LapNumber"] == 2.0

    def test_without_the_column_deleted_laps_are_skipped(self):
        frame = _pick_frame().drop(columns=["IsPersonalBest"])
        frame["Deleted"] = pd.array([False, True, False, False], dtype="boolean")
        laps_pd, laps_pl = _both(frame, None, None)
        assert laps_pd.pick_fastest()["Driver"] == "HAM" == laps_pl.pick_fastest()["Driver"]

    def test_matches_fastf1(self):
        ff = _fastf1_core()
        frame = _pick_frame()
        for only_by_time in (False, True):
            expected = ff.Laps(frame).pick_fastest(only_by_time)
            got = Laps(frame).pick_fastest(only_by_time)
            assert got.name == expected.name
            assert got["LapTime"] == expected["LapTime"]


# -- polars df.f1 session guards and result rows ----------------------------------------------


class _TelemetrySession:
    """In-memory session serving one telemetry frame per lap."""

    name = "Race"

    def __init__(self) -> None:
        self.fetches: list[tuple[str, int]] = []
        self.track_status = pl.DataFrame({"Time": _pl_td([0.0, 100.0]), "Status": ["1", "4"]})

    def _resolve_telemetry_ultra_cold_mode(self, ultra_cold: Any) -> bool:  # noqa: ARG002
        return False

    def _get_telemetry_df_for_ref(self, driver, lap, *, ultra_cold, allow_prefetch=True):  # noqa: ARG002
        self.fetches.append((driver, lap))
        frame = pl.DataFrame(
            {
                "SessionTime": _pl_td([100.0 * lap + 50.0, 100.0 * lap + 50.5]),
                "Time": _pl_td([0.0, 0.5]),
                "Speed": [200.0, 210.0],
                "Driver": [driver] * 2,
                "LapNumber": [lap] * 2,
            }
        )
        return PolarsTelemetry._from_frame(frame, self, driver)

    def _record_telemetry_failure(self, driver, lap, error):  # pragma: no cover
        raise AssertionError((driver, lap, error))


class TestPolarsNamespaceGuards:
    def _plain(self) -> tuple[pl.DataFrame, _TelemetrySession]:
        session = _TelemetrySession()
        laps = PolarsLaps._from_frame(_to_polars(_pick_frame()), session)
        plain = laps.filter(pl.col("Driver") == "VER")
        assert type(plain) is pl.DataFrame
        return plain, session

    @pytest.mark.parametrize(
        "call",
        [
            lambda plain: plain.f1.pick_fastest().get_telemetry(),
            lambda plain: plain.f1.pick_fastest().telemetry,
            lambda plain: plain.f1.pick_fastest().get_car_data(pad=1),
            lambda plain: plain.f1.pick_drivers("VER").get_telemetry(),
            lambda plain: plain.f1.pick_drivers("VER").get_pos_data(),
            lambda plain: plain.f1.pick_drivers("VER").pick_fastest().telemetry,
            lambda plain: next(iter(plain.f1.iterlaps())).lap.telemetry,
            lambda plain: plain.f1.as_laps().pick_laps(1).f1.get_telemetry(),
        ],
        ids=[
            "pick_fastest.get_telemetry",
            "pick_fastest.telemetry",
            "pick_fastest.get_car_data",
            "pick_drivers.get_telemetry",
            "pick_drivers.get_pos_data",
            "pick_drivers.pick_fastest.telemetry",
            "iterlaps.lap.telemetry",
            "as_laps.pick_laps.f1",
        ],
    )
    def test_telemetry_of_laps_derived_without_a_session_raises(self, call):
        plain, session = self._plain()
        with pytest.raises(ValueError, match=r"needs the session.*session="):
            call(plain)
        assert session.fetches == []

    def test_with_session_the_same_calls_answer(self):
        plain, session = self._plain()
        assert plain.f1.pick_fastest(session=session).get_telemetry().height == 2
        assert plain.f1.pick_drivers("VER", session=session).telemetry.height == 4
        assert next(iter(plain.f1.iterlaps(session=session))).lap.telemetry.height == 2
        # non-telemetry members and detached class instances are unchanged
        assert plain.f1.pick_fastest()["LapNumber"] == 1
        detached = PolarsLaps._from_frame(plain, None)
        assert detached.pick_fastest().telemetry.height == 0
        assert PolarsLap(plain.head(1)).telemetry.height == 0

    def test_add_track_status_without_a_session_raises(self):
        session = _TelemetrySession()
        tel = PolarsTelemetry._from_frame(
            session._get_telemetry_df_for_ref("VER", 1, ultra_cold=False), session, "VER"
        )
        plain = tel.filter(pl.col("Speed") > 0)
        assert type(plain) is pl.DataFrame
        with pytest.raises(ValueError, match=r"add_track_status.*session="):
            plain.f1.add_track_status()
        assert plain.f1.add_track_status(session=session)["TrackStatus"].to_list() == ["4", "4"]
        kept = plain.with_columns(pl.lit("9").alias("TrackStatus"))
        assert kept.f1.add_track_status(drop_existing=False)["TrackStatus"].to_list() == ["9"] * 2
        assert plain.clear().f1.add_track_status().height == 0


def _results_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "DriverNumber": ["1", "44", "2", "2"],
            "BroadcastName": ["M VERSTAPPEN", "L HAMILTON", "", "S MOSS"],
            "Abbreviation": ["VER", "HAM", "FAN", "MOS"],
            "FullName": ["Max Verstappen", "Lewis Hamilton", "", "Stirling Moss"],
            "Status": ["Finished", "Finished", "Finished", "Finished"],
        }
    )


class TestPolarsResultRows:
    def test_names_are_the_pandas_index_labels(self):
        session = SimpleNamespace()
        results = PolarsSessionResults._from_frame(_results_frame(), session)
        session.results = results
        assert results.row_for("VER").name == "1"
        assert results.get_driver(44).name == "44"
        assert results.row_for("FAN").name == "2"
        assert results.row_for("MOS").name == "2/MOS"  # a shared car's later driver
        assert results.row_for("2/MOS").Abbreviation == "MOS"
        assert PolarsDriver.from_session(session, "MOS").name == "2/MOS"
        assert PolarsDriver.from_session(session, "HAM").name == "44"
        # a row position (the core get_driver path) becomes the label too
        row = results.slice(3, 1)
        assert PolarsDriverResult(row, session=session, name=3).name == "2/MOS"
        assert PolarsDriver(row, session=session, name=3, driver="MOS").name == "2/MOS"
        assert PolarsDriverResult(results.slice(0, 1), session=None, name=0).name == "1"

    @pytest.mark.parametrize("identifier", ["", "   ", "\t"])
    def test_blank_identifiers_name_no_driver(self, identifier):
        results = PolarsSessionResults._from_frame(_results_frame(), None)
        with pytest.raises(DriverNotFoundError):
            results.row_for(identifier)
        with pytest.raises(DriverNotFoundError):
            results.get_driver(identifier)

    def test_a_failing_results_table_keeps_the_number(self):
        class Session:
            @property
            def results(self):
                raise DataNotFoundError(year=2024)

        frame = _results_frame().slice(3, 1)
        assert PolarsDriverResult(frame, session=Session(), name=3).name == "2"
