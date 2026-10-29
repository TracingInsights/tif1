"""Contract tests for Pergamon Q19: the fast per-driver laps build and the vectorized
classification summary must reproduce the pipeline / row loop exactly."""

from __future__ import annotations

import random
from typing import Any

import numpy as np
import pandas as pd
import pytest

from tif1 import core
from tif1.core import Driver, Session
from tif1.core_utils.driver_laps import build_driver_laps_frame
from tif1.core_utils.helpers import _create_lap_df, _process_lap_df


def _pipeline(payload: dict, driver: str, team: str) -> pd.DataFrame:
    return _process_lap_df(_create_lap_df(payload, driver, team, "pandas"), "pandas")


def _assert_identical(ref: pd.DataFrame, got: pd.DataFrame) -> None:
    assert list(ref.columns) == list(got.columns)
    assert ref.columns.dtype == got.columns.dtype
    assert ref.dtypes.equals(got.dtypes)
    assert ref.index.equals(got.index)
    assert type(ref.index) is type(got.index)
    assert ref.equals(got)
    assert sorted((str(b.dtype), tuple(b.mgr_locs.as_array)) for b in ref._mgr.blocks) == sorted(
        (str(b.dtype), tuple(b.mgr_locs.as_array)) for b in got._mgr.blocks
    )
    for column in ref.columns:
        if ref[column].dtype == object:
            # DataFrame.equals treats None and NaN alike; the representation must match too.
            assert [type(v) for v in ref[column].tolist()] == [
                type(v) for v in got[column].tolist()
            ], column
        if isinstance(ref[column].dtype, pd.CategoricalDtype):
            assert ref[column].cat.codes.dtype == got[column].cat.codes.dtype


def _race_payload(n: int = 6) -> dict[str, Any]:
    """2026-style raw laptime payload: short keys, "None" sentinels, weather columns."""
    return {
        "time": [95.1, "None", 80.25, 79.9, 81.0, 120.5][:n],
        "lap": list(range(1, n + 1)),
        "compound": ["MEDIUM", "MEDIUM", "None", "HARD", "HARD", "HARD"][:n],
        "stint": [1, 1, 1, 2, 2, "None"][:n],
        "s1": ["None", 20.968, 21.0, 20.5, 20.7, 22.0][:n],
        "s2": [41.5, 41.2, 40.9, 40.8, 41.0, 50.0][:n],
        "s3": [22.2, 22.1, 22.0, 21.9, 22.3, 30.0][:n],
        "ms1": ["111111", "711111", "None", "710111", "111111", "111111"][:n],
        "life": [1, 2, 3, 1, 2, 3][:n],
        "pos": [3, 3, 2, "None", 2, 2][:n],
        "status": ["12", "1", "1", "124", "1", "4"][:n],
        "pb": [False, True, "None", True, False, False][:n],
        "sesT": [502.6, 582.8, 662.7, 742.6, 823.6, 944.1][:n],
        "drv": ["LEC"] * n,
        "dNum": ["16"] * n,
        "pout": [400.1, "None", "None", 650.0, "None", "None"][:n],
        "pin": ["None", "None", 640.0, "None", "None", "None"][:n],
        "vi1": [250.0, 251.0, None, 249.5, 250.5, 248.0][:n],
        "fresh": [True, True, True, False, False, False][:n],
        "team": ["Ferrari"] * n,
        "lST": [407.3, 502.6, 582.8, 662.7, 742.6, 823.6][:n],
        "lSD": [
            "2026-06-07T13:03:11.904000000",
            "2026-06-07T13:04:37.639000000",
            "None",
            "2026-06-07T13:05:56.192000000",
            "2026-06-07T13:07:16.192000000",
            "2026-06-07T13:08:36.192000000",
        ][:n],
        "del": [False, None, False, True, False, "None"][:n],
        "delR": ["None", "None", "None", "TRACK LIMITS", "None", "None"][:n],
        "ff1G": [False] * n,
        "iacc": [True, False, True, True, True, False][:n],
        "wT": [442.6, 542.6, 642.6, 742.6, 842.6, 942.6][:n],
        "wAT": [21.5] * n,
        "wR": [False] * n,
        "wWD": [270, 271, "None", 268, 270, 269][:n],
        "wWS": [1.2, 1.1, 1.3, 1.0, 1.2, 1.4][:n],
    }


@pytest.mark.parametrize("n", [1, 2, 6])
def test_fast_build_matches_the_pipeline_on_a_race_payload(n):
    payload = _race_payload(n)
    got = build_driver_laps_frame(payload, "LEC", "Ferrari")
    assert got is not None
    _assert_identical(_pipeline(payload, "LEC", "Ferrari"), got)


def test_fast_build_matches_the_pipeline_on_validated_long_keys():
    payload = {
        "time": [90.5, 91.0],
        "lap": [1, 2],
        "compound": ["SOFT", None],
        "status": ["1", "1"],
        "deleted": [None, None],
        "deleted_reason": [None, None],
        "qualifying_session": ["Q1", "Q2"],
        "wind_direction": [None, 12],
    }
    got = build_driver_laps_frame(payload, "VER", "Red Bull Racing")
    assert got is not None
    _assert_identical(_pipeline(payload, "VER", "Red Bull Racing"), got)


@pytest.mark.parametrize(
    "mutation",
    [
        {"lap": [1, 2, True]},  # bool in a float column
        {"s2": ["1.5", 2.0, 3.0]},  # numeric string
        {"pb": ["yes", False, True]},  # non-null-like string in a bool column
        {"wWD": [1.5, 2, 3]},  # float in the Int64 column
        {"compound": [1, "SOFT", "SOFT"]},  # non-string in a categorical
        {"time": [float("inf"), 1.0, 2.0]},  # timedelta cast pandas has to decide
    ],
)
def test_fast_build_defers_unusual_values_to_the_pipeline(mutation):
    payload = _race_payload(3)
    payload.update(mutation)
    assert build_driver_laps_frame(payload, "LEC", "Ferrari") is None


_POOL = [None, "None", " none ", "NULL", "", "nan", "NaN", "abc", "1.5", 1, 2.5, True, False]
_POOL += [10**20, float("inf"), "2026-06-07T13:03:11.904000000", "12", "SOFT", 0, -3]


def test_fast_build_is_exact_or_defers_on_randomized_payloads():
    rng = random.Random(19)
    built = 0
    for _ in range(400):
        payload = _race_payload(rng.randint(1, 6))
        for _ in range(rng.randint(1, 4)):
            key = rng.choice(list(payload))
            column = list(payload[key])
            roll = rng.random()
            if roll < 0.2:
                column = [None] * len(column)
            elif roll < 0.35:
                column = [rng.choice(_POOL)] * len(column)
            elif roll < 0.45:
                column = column[:-1]  # short column: padded with None
            else:
                column[rng.randrange(len(column))] = rng.choice(_POOL)
            payload[key] = column
        team = rng.choice(["Ferrari", "", "None"])
        try:
            ref: pd.DataFrame | Exception = _pipeline(payload, "LEC", team)
        except Exception as exc:
            ref = exc
        try:
            got = build_driver_laps_frame(payload, "LEC", team)
        except (OverflowError, TypeError, ValueError):
            got = None  # Driver.laps then runs the pipeline
        if got is None:
            continue
        assert not isinstance(ref, Exception), f"pipeline raised {ref!r}, fast build did not"
        _assert_identical(ref, got)
        built += 1
    assert built > 100


def _offline_session(payload: dict) -> Session:
    session = Session(2025, "Pre-Season Testing", "Race", enable_cache=False)
    session._drivers = [{"driver": "LEC", "dn": "16", "team": "Ferrari"}]
    session._refresh_driver_indices()
    session._remember_local_payload("LEC/laptimes.json", payload)
    return session


def test_driver_laps_uses_the_fast_build_without_refetching(monkeypatch):
    payload = _race_payload()
    session = _offline_session(payload)

    def _no_fetch(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("memoized payload must not go through _fetch_laptime_payloads")

    # Warm mode (the shortcut is skipped under ultra-cold, whose backfill consumes the
    # derived payloads).
    monkeypatch.setattr(session, "_resolve_ultra_cold_mode", lambda _ultra_cold: False)
    monkeypatch.setattr(session, "_fetch_laptime_payloads", _no_fetch)
    laps = Driver(session, "LEC").laps
    assert type(laps).__name__ == "Laps"
    assert laps.session is session
    _assert_identical(_pipeline(payload, "LEC", "Ferrari"), pd.DataFrame(laps))

    # The pipeline fallback returns the same frame.
    monkeypatch.setattr("tif1.models.build_driver_laps_frame", lambda *_a, **_k: None)
    fallback = Driver(session, "LEC").laps
    _assert_identical(pd.DataFrame(fallback), pd.DataFrame(laps))


def _summary_loop(laps: pd.DataFrame) -> dict:
    original = core._summarize_pandas_laps_vectorized
    core._summarize_pandas_laps_vectorized = lambda _laps: None  # ty: ignore[invalid-assignment]
    try:
        return core._summarize_laps_for_classification(laps, "pandas")
    finally:
        core._summarize_pandas_laps_vectorized = original


def _assert_same_summary(laps: pd.DataFrame) -> None:
    fast = core._summarize_pandas_laps_vectorized(laps)
    assert fast is not None
    assert list(fast.items()) == list(_summary_loop(laps).items())
    assert core._summarize_laps_for_classification(laps, "pandas") == fast


def test_vectorized_classification_summary_matches_the_row_loop():
    laps = pd.DataFrame(
        {
            "Driver": pd.Categorical(["VER", "HAM", "VER", None, "HAM", "VER", "None", "LEC"]),
            "DriverNumber": ["None", "44", "1", "5", 44.0, "1", "7", None],
            "LapNumber": [1.0, 1.0, 2.0, 3.0, 2.0, 2.0, 1.0, -1.0],
            "Position": [2.0, 1.0, np.nan, 4.0, 1.0, 3.0, 1.0, 5.0],
            "LapTimeSeconds": [80.0, 79.5, 78.0, 70.0, np.nan, 77.0, 60.0, 90.0],
            "Deleted": pd.array([False, None, True, False, False, False, False, False], "boolean"),
        }
    )
    _assert_same_summary(laps)


def test_vectorized_classification_summary_on_randomized_frames():
    rng = np.random.default_rng(19)
    codes = np.array(["VER", "HAM", "LEC", "None", ""], dtype=object)
    for _ in range(50):
        n = int(rng.integers(1, 60))
        laps = pd.DataFrame(
            {
                "Driver": pd.Series(rng.choice(codes, n), dtype=object),
                "DriverNumber": pd.Series(rng.choice(["1", "44", None, "None"], n), dtype=object),
                "LapNumber": np.where(rng.random(n) < 0.1, np.nan, rng.integers(-1, 5, n) * 1.0),
                "Position": np.where(rng.random(n) < 0.2, np.nan, rng.integers(1, 20, n) * 1.0),
                "LapTimeSeconds": np.where(rng.random(n) < 0.2, np.nan, rng.random(n) * 10 + 70),
                "Deleted": pd.array(rng.choice([True, False, None], n).tolist(), "boolean"),
            }
        )
        _assert_same_summary(laps)


def test_vectorized_summary_defers_on_missing_or_non_float_columns():
    laps = pd.DataFrame({"Driver": ["VER"], "LapNumber": pd.array([1], "Int64")})
    assert core._summarize_pandas_laps_vectorized(laps) is None
    assert core._summarize_laps_for_classification(laps, "pandas")["VER"]["laps"] == 1
