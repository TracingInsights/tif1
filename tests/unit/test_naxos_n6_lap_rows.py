"""Naxos N6: the per-lap layers of ``Laps.iterlaps`` and ``Lap.telemetry``.

``iterlaps`` skips the Series constructor's object inference for frames whose
rows always stay object dtype (a non-nullable numpy int or bool column) and
builds each row block over one shared placement; the row interleave runs
without cyclic GC; ``Lap.telemetry`` reads ``Driver``/``LapNumber`` directly
for unique labels. Every result must equal the public paths'.
"""

from __future__ import annotations

import gc
from typing import Any

import numpy as np
import pandas as pd
import pytest

from tif1 import models
from tif1.models import Lap, Laps


def _laps_frame(session: Any = None) -> Laps:
    n = 6
    frame = pd.DataFrame(
        {
            "Time": pd.to_timedelta(np.arange(n), unit="s"),
            "Driver": pd.Categorical(["VER", "HAM", "LEC"] * 2),
            "LapTime": pd.to_timedelta([90.0, np.nan, 91.0, 92.0, 93.0, np.nan], "s"),
            "LapNumber": np.arange(1.0, n + 1),
            "Stint": pd.array([1, 1, None, 2, 2, 2], dtype="Int64"),
            "Team": pd.array(["Red Bull", "Ferrari", None] * 2, dtype="str"),
            "LapStartDate": pd.date_range("2026-06-07 13:00", periods=n, freq="90s"),
            "Position": np.arange(n, dtype="int64"),
            "Deleted": [False, True, False] * 2,
            "DeletedReason": [None, "track limits", None] * 2,
        },
        index=pd.Index(np.arange(20, 20 + n), dtype="int64"),
    )
    return Laps(frame, session=session)


def _reference_rows(laps: pd.DataFrame) -> list[Lap]:
    values = laps.to_numpy(dtype=object)
    rows = []
    for position in range(len(laps)):
        lap = Lap(values[position].copy(), index=laps.columns, name=laps.index[position])
        lap.session = getattr(laps, "session", None)
        rows.append(lap)
    return rows


def _assert_same_row(got: Lap, expected: Lap) -> None:
    assert type(got) is type(expected) is Lap
    assert got.dtype == expected.dtype
    assert got.equals(expected)
    assert got.name == expected.name
    assert type(got.name) is type(expected.name)
    assert got.session is expected.session
    assert set(vars(got)) == set(vars(expected))
    for value, other in zip(got.array, expected.array, strict=True):
        assert type(value) is type(other)
        assert value is other or value == other or (pd.isna(value) and pd.isna(other))
    block, expected_block = got._mgr.blocks[0], expected._mgr.blocks[0]
    assert type(block) is type(expected_block)
    assert block.mgr_locs.as_slice == expected_block.mgr_locs.as_slice
    assert block.refs.has_reference() == expected_block.refs.has_reference()


def test_object_rows_probe_holds_on_the_installed_pandas():
    assert models._probe_object_rows()
    assert models._OBJECT_ROW_BLOCK_CLASS is not None


@pytest.mark.parametrize(
    ("columns", "fast"),
    [
        (None, True),
        (["Time", "LapTime", "Position"], True),
        (["Deleted", "Team"], True),
        (["Time", "LapTime"], False),
        (["LapNumber", "Stint", "Team"], False),
        (["Driver", "DeletedReason"], False),
    ],
)
def test_object_row_placement_needs_a_numpy_int_or_bool_column(columns, fast):
    laps = _laps_frame()
    frame = laps if columns is None else laps[columns]
    placement = models._object_row_placement(frame)
    assert (placement is not None) is fast
    if fast:
        assert placement.as_slice == slice(0, frame.shape[1], 1)


def test_iterlaps_skips_the_inference_and_matches_the_constructor(monkeypatch):
    session = object()
    laps = _laps_frame(session)
    calls: list[int] = []
    real = models._maybe_convert_objects

    def spy(values: Any, **kwargs: Any) -> Any:
        calls.append(len(values))
        return real(values, **kwargs)

    monkeypatch.setattr(models, "_maybe_convert_objects", spy)
    got = [lap for _, lap in laps.iterlaps()]
    assert calls == []
    expected = _reference_rows(laps)
    assert len(got) == len(expected) == len(laps)
    for lap, expected_lap in zip(got, expected, strict=True):
        _assert_same_row(lap, expected_lap)
    assert len({id(lap._mgr.blocks[0].mgr_locs) for lap in got}) == 1


def test_iterlaps_rows_stay_independent_over_the_shared_placement():
    laps = _laps_frame()
    got = [lap for _, lap in laps.iterlaps()]
    got[0]["Position"] = 99
    got[1].iloc[0] = pd.Timedelta(-1)
    assert got[2]["Position"] == 2
    assert got[0]["Time"] == pd.Timedelta(0)
    assert laps["Position"].tolist() == list(range(len(laps)))
    assert laps["Time"].iloc[1] == pd.Timedelta(seconds=1)
    # a write that changes the row's dtype stays local too
    got[3]["Driver"] = 1.5
    assert got[4]["Driver"] == "HAM"
    assert laps["Driver"].iloc[3] == "VER"


def test_iterlaps_keeps_the_inference_for_rows_that_may_convert(monkeypatch):
    laps = _laps_frame()
    frame = laps[["Time", "LapTime"]]
    calls: list[int] = []
    real = models._maybe_convert_objects

    def spy(values: Any, **kwargs: Any) -> Any:
        calls.append(len(values))
        return real(values, **kwargs)

    monkeypatch.setattr(models, "_maybe_convert_objects", spy)
    got = [lap for _, lap in frame.iterlaps()]
    assert len(calls) == len(frame)
    for lap, expected in zip(got, _reference_rows(frame), strict=True):
        _assert_same_row(lap, expected)
    assert {str(lap.dtype) for lap in got} != {"object"}


def test_iterlaps_infers_every_row_when_the_probe_fails(monkeypatch):
    laps = _laps_frame()
    expected = _reference_rows(laps)
    monkeypatch.setattr(models, "_OBJECT_ROWS_OK", False)
    assert models._object_row_placement(laps) is None
    for (_, lap), expected_lap in zip(laps.iterlaps(), expected, strict=True):
        _assert_same_row(lap, expected_lap)


def test_object_rows_probe_rejects_a_converting_inference(monkeypatch):
    def converting(values: Any, **_kwargs: Any) -> Any:
        return values.copy()

    monkeypatch.setattr(models, "_maybe_convert_objects", converting)
    assert models._probe_object_rows() is False
    monkeypatch.setattr(models, "_FAST_LAP_ROWS", False)
    assert models._probe_object_rows() is False


def test_iterlaps_builds_the_row_values_without_cyclic_gc(monkeypatch):
    laps = _laps_frame()
    states: list[bool] = []
    real = Laps.to_numpy

    def spy(self: Any, *args: Any, **kwargs: Any) -> Any:
        states.append(gc.isenabled())
        return real(self, *args, **kwargs)

    monkeypatch.setattr(Laps, "to_numpy", spy)
    assert gc.isenabled()
    rows = list(laps.iterlaps())
    assert states == [False]
    assert gc.isenabled()
    assert len(rows) == len(laps)


def test_driver_and_lap_number_match_series_get(monkeypatch):
    monkeypatch.setattr(models, "_LAP_KEY_LOCS", (None, None, None))
    laps = _laps_frame()
    rows = [lap for _, lap in laps.iterlaps()]
    for lap in rows:
        assert models._driver_and_lap_number(lap) == (lap.get("Driver"), lap.get("LapNumber"))
    assert models._LAP_KEY_LOCS[0] is rows[0].index
    # the cached positions follow the values, not a copy of them
    rows[2]["Driver"] = "NOR"
    rows[2]["LapNumber"] = 42.0
    assert models._driver_and_lap_number(rows[2]) == ("NOR", 42.0)
    # another index (other column order) is resolved on its own
    other = laps[["LapNumber", "Driver", "Time"]].iloc[1]
    assert models._driver_and_lap_number(other) == ("HAM", 2.0)
    assert models._driver_and_lap_number(rows[0]) == ("VER", 1.0)


def test_driver_and_lap_number_fall_back_to_get(monkeypatch):
    monkeypatch.setattr(models, "_LAP_KEY_LOCS", (None, None, None))
    missing = Lap(["VER", 3.0], index=["Driver", "Stint"])
    assert models._driver_and_lap_number(missing) == ("VER", None)
    duplicated = Lap(["VER", "HAM", 7.0], index=["Driver", "Driver", "LapNumber"])
    driver, lap_number = models._driver_and_lap_number(duplicated)
    assert driver.equals(duplicated.get("Driver"))
    assert lap_number == 7.0
    numbered = pd.Series([5.0, "VER"], index=pd.Index([0, 1]))
    assert models._driver_and_lap_number(numbered) == (None, None)


def test_lap_telemetry_asks_the_session_for_the_driver_and_lap():
    calls: list[tuple[str, int]] = []
    frame = models.Telemetry({"Speed": [1.0]})

    class _Session:
        def _resolve_telemetry_ultra_cold_mode(self, _ultra_cold: Any) -> bool:
            return False

        def _get_telemetry_df_for_ref(self, driver: str, lap: int, **_kwargs: Any) -> Any:
            calls.append((driver, lap))
            return frame

    laps = _laps_frame(_Session())
    got = [lap.telemetry for _, lap in laps.iterlaps()]
    assert calls == [("VER", 1), ("HAM", 2), ("LEC", 3), ("VER", 4), ("HAM", 5), ("LEC", 6)]
    assert all(tel is frame for tel in got)
    no_driver = Lap(np.array(["", 1.0], dtype=object), index=["Driver", "LapNumber"])
    no_driver.session = _Session()
    assert no_driver.telemetry.empty
    assert len(calls) == len(laps)
