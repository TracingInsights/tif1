"""Contract tests for Pergamon Q7: per-row ``Lap`` construction.

``Laps.iterlaps`` builds each ``Lap`` through the Series constructor's own
steps with the column Index shared per null pattern, and ``Laps._ixs``
(``laps.iloc[i]``) runs ``fast_xs`` with the interleaved dtype memoized. Both
must return exactly what the public pandas paths return: same values and
element types, index, name, dtype, session and copy-on-write isolation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tif1 import models
from tif1.models import Lap, Laps


def _reference_iterlaps(laps: Laps, require=None):
    """The pre-Q7 algorithm (Argos A5): public ``Lap(...)`` constructor per row."""
    required_columns = ["LapTime", "Driver"] if require is None else list(require)
    columns = laps.columns
    null_mask = laps.isna().to_numpy(dtype=bool)
    required_positions = np.concatenate(
        [np.flatnonzero(columns == column) for column in required_columns]
    )
    row_values = laps.to_numpy(dtype=object)
    for position in range(len(laps)):
        null_row = null_mask[position]
        if null_row[required_positions].any():
            continue
        if null_row.any():
            keep = ~null_row
            lap = Lap(row_values[position][keep], index=columns[keep], name=laps.index[position])
        else:
            lap = Lap(row_values[position], index=columns, name=laps.index[position])
        lap.session = laps.session
        yield laps.index[position], lap


def _assert_same_lap(got, expected) -> None:
    assert type(got) is type(expected) is Lap
    assert got.dtype == expected.dtype
    assert got.equals(expected)
    assert got.index.equals(expected.index)
    assert got.index.dtype == expected.index.dtype
    assert list(got.index) == list(expected.index)
    assert got.name == expected.name
    assert type(got.name) is type(expected.name)
    assert got.session is expected.session
    assert set(vars(got)) == set(vars(expected))
    assert got.attrs == expected.attrs
    for value, other in zip(got.array, expected.array, strict=True):
        assert type(value) is type(other)
        assert value is other or value == other or (pd.isna(value) and pd.isna(other))
    assert len(got._mgr.blocks) == 1
    assert type(got._mgr.blocks[0]) is type(expected._mgr.blocks[0])
    assert got._mgr.blocks[0].refs.has_reference() == expected._mgr.blocks[0].refs.has_reference()


def _laps_frame(session=None) -> Laps:
    n = 9
    frame = pd.DataFrame(
        {
            "Time": pd.to_timedelta(np.arange(n), unit="s"),
            "Driver": pd.Categorical(["VER", "HAM", "LEC"] * 3),
            "DriverNumber": ["1", "44", "16"] * 3,
            "LapTime": pd.to_timedelta([90.0, np.nan, 91.0, 92.0, 93.0, np.nan, 94.0, 95, 96], "s"),
            "LapNumber": np.arange(1.0, n + 1),
            "Stint": pd.array([1, 1, None, 2, 2, 2, 3, 3, 3], dtype="Int64"),
            "Sector1Time": pd.to_timedelta([30.0, 31, np.nan, 32, 33, 34, np.nan, 35, 36], "s"),
            "SpeedI1": [300.0, np.nan, 301, 302, 303, np.nan, 304, 305, 306],
            "IsPersonalBest": pd.array([True, False, None, True, False, True, False, True, False]),
            "Compound": pd.Categorical(["SOFT", "MEDIUM", None] * 3),
            "Team": pd.array(["Red Bull", "Ferrari", None] * 3, dtype="str"),
            "LapStartDate": pd.date_range("2026-06-07 13:00", periods=n, freq="90s"),
            "Position": np.arange(n, dtype="int64"),
            "Deleted": [False, True, False] * 3,
            "DeletedReason": [None, "track limits", None] * 3,
        },
        index=pd.Index(np.arange(10, 10 + n), dtype="int64"),
    )
    laps = Laps(frame, session=session)
    assert len(laps._mgr.blocks) > 2
    return laps


@pytest.mark.parametrize("require", [None, ["LapTime"], ["Driver", "Sector1Time"]])
def test_iterlaps_matches_the_public_constructor_per_row(require):
    session = object()
    laps = _laps_frame(session)
    got = list(laps.iterlaps(require))
    expected = list(_reference_iterlaps(laps, require))
    assert len(got) == len(expected) > 0
    for (index, lap), (expected_index, expected_lap) in zip(got, expected, strict=True):
        assert index == expected_index
        assert type(index) is type(expected_index)
        _assert_same_lap(lap, expected_lap)
        assert lap.session is session


def test_iterlaps_keeps_the_constructors_dtype_inference():
    # Rows whose kept values are all timedeltas / all strings are inferred to a
    # non-object dtype by the Series constructor; the fast path must agree.
    laps = _laps_frame()
    timedeltas = laps[["Time", "LapTime", "Sector1Time"]]
    strings = laps[["DriverNumber", "Team"]]
    for frame, require in ((timedeltas, ["Time"]), (strings, ["DriverNumber"])):
        got = list(frame.iterlaps(require))
        expected = list(_reference_iterlaps(frame, require))
        assert {str(lap.dtype) for _, lap in expected} != {"object"}
        for (_, lap), (_, expected_lap) in zip(got, expected, strict=True):
            _assert_same_lap(lap, expected_lap)


def test_iterlaps_shares_one_index_per_null_pattern_and_isolates_rows():
    laps = _laps_frame()
    got = [lap for _, lap in laps.iterlaps(["Time"])]
    by_columns: dict[tuple, set[int]] = {}
    for lap in got:
        by_columns.setdefault(tuple(lap.index), set()).add(id(lap.index))
    assert all(len(ids) == 1 for ids in by_columns.values())
    first = got[0]
    first.iloc[0] = pd.Timedelta(-1)
    assert got[3].iloc[0] != pd.Timedelta(-1)
    assert laps.iloc[0]["Time"] == pd.Timedelta(0)


def test_iterlaps_with_an_empty_requirement_raises_like_before():
    laps = _laps_frame()
    with pytest.raises(ValueError, match="at least one array"):
        list(laps.iterlaps([]))
    with pytest.raises(ValueError, match="at least one array"):
        list(_reference_iterlaps(laps, []))


def test_iloc_rows_match_pandas_fast_xs_path():
    session = object()
    laps = _laps_frame(session)
    for position in [*range(len(laps)), -1, -len(laps)]:
        got = laps.iloc[position]
        expected = pd.DataFrame._ixs(laps, position)
        _assert_same_lap(got, expected)
        assert got.session is session
    with pytest.raises(IndexError):
        laps.iloc[len(laps)]


def test_iloc_follows_frame_mutations_and_keeps_cow_isolation():
    laps = _laps_frame()
    before = laps.iloc[2]
    laps.loc[laps.index[2], "LapNumber"] = 99.0
    after = laps.iloc[2]
    assert before["LapNumber"] == 3.0
    assert after["LapNumber"] == 99.0
    after["LapNumber"] = 5.0
    assert laps.iloc[2]["LapNumber"] == 99.0
    # dtype changes produce a new block-dtype tuple: still pandas' answer
    laps["Extra"] = pd.Timedelta(1)
    laps["Position"] = laps["Position"].astype("Int64")
    for position in range(len(laps)):
        _assert_same_lap(laps.iloc[position], pd.DataFrame._ixs(laps, position))


def test_iloc_non_object_and_single_block_frames_use_pandas():
    laps = _laps_frame()
    floats = laps[["LapNumber", "SpeedI1"]]
    timedeltas = laps[["Time", "LapTime"]]
    numbers = laps[["LapNumber", "Position"]]  # float64 + int64 -> float64 rows
    for frame in (floats, timedeltas, numbers):
        for position in range(len(frame)):
            got = frame.iloc[position]
            _assert_same_lap(got, pd.DataFrame._ixs(frame, position))
    assert numbers.iloc[0].dtype == np.float64


def test_public_paths_are_used_when_pandas_internals_are_unavailable(monkeypatch):
    laps = _laps_frame()
    expected_rows = [lap for _, lap in laps.iterlaps()]
    expected_iloc = [laps.iloc[i] for i in range(len(laps))]
    monkeypatch.setattr(models, "_FAST_LAP_ROWS", False)
    for lap, expected in zip((lap for _, lap in laps.iterlaps()), expected_rows, strict=True):
        _assert_same_lap(lap, expected)
    for position, expected in enumerate(expected_iloc):
        _assert_same_lap(laps.iloc[position], expected)


def test_interleaved_dtype_cache_is_bounded(monkeypatch):
    monkeypatch.setattr(models, "_ROW_DTYPE_CACHE", {})
    monkeypatch.setattr(models, "_ROW_DTYPE_CACHE_MAX", 2)
    dtypes = [
        (np.dtype("float64"), np.dtype("O")),
        (np.dtype("int64"), pd.StringDtype(na_value=np.nan)),
        (np.dtype("bool"), np.dtype("m8[ns]")),
    ]
    for group in dtypes:
        assert models._row_interleaved_dtype(group) == np.dtype("O")
    assert len(models._ROW_DTYPE_CACHE) <= 2
    assert models._row_interleaved_dtype((np.dtype("float64"), np.dtype("int64"))) == np.float64
