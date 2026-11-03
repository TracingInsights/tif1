"""Elis E7: the small warm pandas fast paths give pandas' own answers.

``Laps.iloc[i]`` boxes a multi-column timedelta block in one call,
``pick_driver`` slices a driver's run of rows, the qualifying split reads
its labels once per distinct value, the nanosecond clock helpers view the
column, and the Ergast plan/answer/round classes are plain ``__slots__``
classes with the dataclass constructor, repr and equality.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tif1 import models
from tif1.ergast.router import ExecutedQuery, QueryPlan
from tif1.ergast.stats import RoundInfo
from tif1.models import Laps


def _laps(drivers: list[str], *, categorical: bool = True, unit: str = "ns") -> Laps:
    n = len(drivers)
    times = pd.to_timedelta(np.arange(n) * 1500, unit="ms").as_unit(unit)
    frame = pd.DataFrame(
        {
            "Time": times,
            "Driver": pd.Categorical(drivers) if categorical else pd.Series(drivers, dtype=object),
            "DriverNumber": [str(10 + ord(d[0]) % 7) for d in drivers],
            "LapTime": pd.to_timedelta(np.arange(n) + 90.0, unit="s").as_unit(unit),
            "LapNumber": np.arange(n, dtype="float64") + 1,
            "PitOutTime": pd.Series([pd.NaT] * n, dtype=f"m8[{unit}]"),
            "Sector1Time": pd.to_timedelta(np.arange(n) + 30.0, unit="s").as_unit(unit),
            "LapStartTime": (times - pd.Timedelta(seconds=90)).as_unit(unit),
            "LapStartDate": pd.Timestamp("2025-04-13 15:00") + times,
            "Compound": pd.array(["SOFT"] * n, dtype="str"),
            "Position": pd.array(list(range(n)), dtype="Int64"),
            "IsAccurate": np.ones(n, dtype=bool),
        }
    )
    frame.loc[1, "LapTime"] = pd.NaT
    return Laps(frame)


def _assert_same_frame(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    assert type(actual) is type(expected)
    assert type(actual.index) is type(expected.index)
    if isinstance(expected.index, pd.RangeIndex):
        assert (actual.index.start, actual.index.stop, actual.index.step) == (
            expected.index.start,
            expected.index.stop,
            expected.index.step,
        )
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert [str(b.dtype) for b in actual._mgr.blocks] == [
        str(b.dtype) for b in expected._mgr.blocks
    ]


@pytest.mark.parametrize("unit", ["ns", "ms"])
def test_iloc_row_matches_pandas_row(unit: str) -> None:
    laps = _laps(["VER", "VER", "LEC", "LEC"], unit=unit)
    plain = pd.DataFrame(laps)
    timedelta_blocks = [b for b in laps._mgr.blocks if b.dtype.kind == "m"]
    assert any(len(b.mgr_locs) > 1 for b in timedelta_blocks)  # the vectorized branch runs
    for i in (*range(len(laps)), -1):
        row = laps.iloc[i]
        expected = plain.iloc[i]
        assert isinstance(row, models.Lap)
        assert row.name == expected.name
        assert list(row.index) == list(expected.index)
        for column in expected.index:
            got, want = row[column], expected[column]
            assert type(got) is type(want), column
            if want is pd.NaT:
                assert got is pd.NaT
            else:
                assert got == want, column
                if isinstance(want, pd.Timedelta):
                    assert got.unit == want.unit


def test_pick_driver_slices_a_run_and_matches_boolean_take() -> None:
    laps = _laps(["VER", "VER", "VER", "LEC", "LEC", "NOR"])
    for code in ("VER", "LEC", "NOR", "XXX", "ver"):
        _assert_same_frame(laps.pick_driver(code), laps[laps["Driver"] == code])
    assert laps._driver_run("LEC") == slice(3, 5)
    picked = laps.pick_driver("LEC")
    assert np.shares_memory(picked["LapNumber"].to_numpy(), laps["LapNumber"].to_numpy())
    # a write to the pick leaves the session table alone (copy-on-write)
    picked.loc[picked.index[0], "LapNumber"] = 99.0
    assert laps["LapNumber"].iloc[3] == 4.0


def test_pick_driver_keeps_the_boolean_path_otherwise() -> None:
    spread = _laps(["VER", "LEC", "VER"])
    assert spread._driver_run("VER") is None
    _assert_same_frame(spread.pick_driver("VER"), spread[spread["Driver"] == "VER"])
    plain = _laps(["VER", "VER", "LEC"], categorical=False)
    assert plain._driver_run("VER") is None
    _assert_same_frame(plain.pick_driver("VER"), plain[plain["Driver"] == "VER"])
    relabelled = _laps(["VER", "VER", "LEC"])
    relabelled.index = pd.Index([10, 20, 30])
    assert relabelled._driver_run("VER") is None
    _assert_same_frame(relabelled.pick_driver("VER"), relabelled[relabelled["Driver"] == "VER"])
    sub = _laps(["VER", "VER", "LEC", "LEC"]).iloc[2:]
    _assert_same_frame(sub.pick_driver("LEC"), sub[sub["Driver"] == "LEC"])


def _split_reference(laps: Laps, monkeypatch: pytest.MonkeyPatch) -> list:
    with monkeypatch.context() as patch:
        patch.setattr(models, "_qualifying_marks", lambda _labels: None)
        return laps.split_qualifying_sessions()


@pytest.mark.parametrize(
    "labels",
    [
        ["Q1", "Q1", "Q2", "Q3", "Q3", "Q2"],
        ["q1", " Q1 ", "Q2\t", "session 3", "Q3", "Q1"],
        ["Q1", "Q1", "Q1", "Q2", "Q2", "Q2"],  # no Q3
    ],
)
def test_split_marks_match_the_pandas_expression(
    labels: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    laps = _laps(["VER"] * len(labels))
    laps["QualifyingSession"] = labels
    marks = models._qualifying_marks(laps["QualifyingSession"])
    assert marks is not None
    got = laps.split_qualifying_sessions()
    want = _split_reference(laps, monkeypatch)
    assert len(got) == len(want) == 3
    for part, ref in zip(got, want, strict=True):
        if ref is None:
            assert part is None
            continue
        _assert_same_frame(part, ref)
        assert part.session is ref.session


@pytest.mark.parametrize(
    "labels",
    [
        ["Q1", None, "Q2"],  # a missing label: the pandas comparison decides
        ["Q1", "Q2", "X"],  # an unmarked label
        ["Q1", "Q2", "Q³"],  # not ASCII
        [1, 2, 3],  # not strings
    ],
)
def test_split_marks_defer_to_pandas(labels: list) -> None:
    assert models._qualifying_marks(pd.Series(labels, dtype=object)) is None


def test_split_marks_from_a_category_column(monkeypatch: pytest.MonkeyPatch) -> None:
    laps = _laps(["VER"] * 4)
    laps["QualifyingSession"] = pd.Categorical(["Q1", "Q2", "Q2", "Q3"])
    for part, ref in zip(
        laps.split_qualifying_sessions(), _split_reference(laps, monkeypatch), strict=True
    ):
        _assert_same_frame(part, ref)


def test_nanosecond_clock_views_match_the_coercing_path() -> None:
    td = pd.Series(pd.to_timedelta([1, None, 3], unit="s").as_unit("ns"))
    dt = pd.Series(pd.to_datetime(["2025-01-01", None, "2025-01-02"]).as_unit("ns"))
    fast_td = models._timedelta_ns(td)
    slow_td = td.astype("timedelta64[ns]").to_numpy().view("i8")
    np.testing.assert_array_equal(fast_td, slow_td)
    assert fast_td.dtype == np.int64
    assert not fast_td.flags.writeable
    fast_dt = models._clock_ns_values(dt)
    np.testing.assert_array_equal(fast_dt, dt.astype("datetime64[ns]").to_numpy().view("i8"))
    assert fast_dt[1] == models._NAT_NS
    assert not fast_dt.flags.writeable
    # other resolutions still convert to nanoseconds
    ms = pd.Series(pd.to_timedelta([1, 2], unit="s").as_unit("ms"))
    np.testing.assert_array_equal(models._timedelta_ns(ms), [10**9, 2 * 10**9])


def test_ergast_plain_classes_keep_the_dataclass_behaviour() -> None:
    plan = QueryPlan("results", "RaceTable", {"a": 1}, None)
    assert plan.selectors == {}
    assert plan.limit is None
    assert plan.source == "auto"
    assert plan.selectors is not QueryPlan("results", "RaceTable", {}, None).selectors
    assert repr(plan) == (
        "QueryPlan(endpoint='results', table='RaceTable', category={'a': 1}, "
        "subcategory=None, selectors={}, limit=None, offset=None, source='auto')"
    )
    same = QueryPlan(endpoint="results", table="RaceTable", category={"a": 1}, subcategory=None)
    assert plan == same
    assert plan != QueryPlan("laps", "RaceTable", {"a": 1}, None)
    assert plan.__eq__(object()) is NotImplemented
    with pytest.raises(TypeError):
        hash(plan)
    with pytest.raises(AttributeError):
        plan.other = 1  # type: ignore[attr-defined]
    match plan:
        case QueryPlan(endpoint, table):
            assert (endpoint, table) == ("results", "RaceTable")
    executed = ExecutedQuery({"MRData": {}}, "stats", 1.5)
    assert repr(executed) == "ExecutedQuery(payload={'MRData': {}}, source='stats', fetched_at=1.5)"
    assert executed == ExecutedQuery({"MRData": {}}, "stats", 1.5)
    info = RoundInfo(1, "Bahrain Grand Prix", "bahrain-grand-prix", {"Sprint": {}})
    assert repr(info) == (
        "RoundInfo(round=1, race_name='Bahrain Grand Prix', slug='bahrain-grand-prix', "
        "race={'Sprint': {}})"
    )
    assert info == RoundInfo(1, "Bahrain Grand Prix", "bahrain-grand-prix", {"Sprint": {}})
    assert info.has_sprint
