"""Olympia H8 contracts: a session's first laps load (no laps frame tier row).

The column-by-column pandas build of the merged laps dict, the one-plan polars
processing, the C-level null-like coercion, the batched per-driver payload
derivation and the event-loop-free load of a warm session must give exactly
what the frame-wide pipeline and the async load gave.
"""

from __future__ import annotations

import asyncio
import random
import sys
import warnings
from typing import Any

import numpy as np
import pandas as pd
import pytest

from tif1 import core
from tif1.cache import clear_lap_cache, get_cache
from tif1.config import get_config
from tif1.core_utils import driver_laps
from tif1.core_utils.driver_laps import _iso_ns_dates, build_laps_frame
from tif1.core_utils.helpers import (
    _NULL_LIKE_TOKEN_PROBE,
    _coerce_null_like_payload,
    _normalize_lap_payload,
    _normalize_lap_payload_polars,
    _process_lap_df,
    _process_lap_df_stepwise,
    _process_polars_laps,
)
from tif1.exceptions import _coerce_null_like_string_list

pl = pytest.importorskip("polars")


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
            assert [type(v) for v in ref[column].tolist()] == [
                type(v) for v in got[column].tolist()
            ], column
        if isinstance(ref[column].dtype, pd.CategoricalDtype):
            assert ref[column].cat.codes.dtype == got[column].cat.codes.dtype


def _driver_payload(rng: random.Random, n: int, drv: str) -> dict[str, Any]:
    """2025-style raw laptime payload with "None" sentinels in the usual places."""
    return {
        "time": [rng.choice([95.1, 80.25, "None", 79.9]) for _ in range(n)],
        "lap": list(range(1, n + 1)),
        "compound": [rng.choice(["SOFT", "MEDIUM", "None"]) for _ in range(n)],
        "stint": [1] * n,
        "s1": [rng.choice(["None", 20.968, 21.5]) for _ in range(n)],
        "ms1": [rng.choice(["1111", "0111", "None"]) for _ in range(n)],
        "pos": [rng.choice([3, 4, 5]) for _ in range(n)],
        "status": [rng.choice(["1", "12", "4"]) for _ in range(n)],
        "pb": [rng.choice([True, False]) for _ in range(n)],
        "sesT": [500.0 + 80 * i for i in range(n)],
        "drv": [drv] * n,
        "dNum": ["16"] * n,
        "pout": [rng.choice(["None", 400.5]) for _ in range(n)],
        "vfl": [rng.choice(["None", 280.0, 290]) for _ in range(n)],
        "fresh": [rng.choice([True, False]) for _ in range(n)],
        "team": ["Ferrari"] * n,
        "lST": [420.0 + 80 * i for i in range(n)],
        "lSD": [
            rng.choice(["2026-06-07T13:03:11.904000000", "2026-06-07T13:04:37.639000001", "None"])
            for _ in range(n)
        ],
        "del": [rng.choice([False, True]) for _ in range(n)],
        "delR": [rng.choice(["None", "TRACK LIMITS"]) for _ in range(n)],
        "wWD": [rng.choice([270, 12]) for _ in range(n)],
        "wR": [False] * n,
    }


_POOL = [None, "None", " none ", "NULL", "", "nan", "abc", "1.5", 1, 2.5, True, False, 10**20]
_POOL += [float("inf"), "2026-06-07T13:03:11.904000000", "2026-06-07T13:03:11.904", "12", -3]


def _merged_dict(rng: random.Random, *, mutate: bool) -> dict[str, list]:
    entries = []
    for code, team in (("LEC", "Ferrari"), ("VER", rng.choice(["Red Bull", "", "None"]))):
        payload = _driver_payload(rng, rng.randint(1, 5), code)
        if rng.random() < 0.3:
            payload.pop(rng.choice(list(payload)))  # a column one driver lacks: None padding
        if mutate:
            for _ in range(rng.randint(0, 2)):
                key = rng.choice(list(payload))
                column = list(payload[key])
                roll = rng.random()
                if roll < 0.2:
                    column = [None] * len(column)
                elif roll < 0.35:
                    column = [rng.choice(_POOL)] * len(column)
                else:
                    column[rng.randrange(len(column))] = rng.choice(_POOL)
                payload[key] = column
        entries.append((payload, code, team))
    return core._merge_lap_payloads(
        [(_normalize_lap_payload(payload), code, team) for payload, code, team in entries]
    )


def test_merged_fast_build_matches_the_pipeline_on_clean_sessions():
    rng = random.Random(8)
    for _ in range(60):
        merged = _merged_dict(rng, mutate=False)
        got = build_laps_frame(merged)
        assert got is not None
        _assert_identical(_process_lap_df(pd.DataFrame(merged, copy=False), "pandas"), got)


@pytest.mark.filterwarnings("ignore::RuntimeWarning")  # the pipeline casting inf to Int64
def test_merged_fast_build_is_exact_or_defers_on_randomized_sessions():
    rng = random.Random(88)
    built = 0
    for _ in range(400):
        merged = _merged_dict(rng, mutate=True)
        try:
            ref: pd.DataFrame | Exception = _process_lap_df(
                pd.DataFrame(merged, copy=False), "pandas"
            )
        except Exception as exc:
            ref = exc
        try:
            got = build_laps_frame(merged)
        except (OverflowError, TypeError, ValueError):
            got = None  # the session laps then run the pipeline
        if got is None:
            continue
        assert not isinstance(ref, Exception), f"pipeline raised {ref!r}, fast build did not"
        _assert_identical(ref, got)
        built += 1
    assert built > 80


def test_session_laps_from_merged_falls_back_to_the_pipeline(monkeypatch):
    merged = _merged_dict(random.Random(3), mutate=False)
    monkeypatch.setattr(driver_laps, "build_laps_frame", lambda _m: None)
    fallback = core._pandas_laps_from_merged(merged)
    monkeypatch.setattr(
        driver_laps, "build_laps_frame", lambda _m: (_ for _ in ()).throw(OverflowError("x"))
    )
    _assert_identical(fallback, core._pandas_laps_from_merged(merged))
    _assert_identical(fallback, _process_lap_df(pd.DataFrame(merged, copy=False), "pandas"))


@pytest.mark.parametrize(
    "values",
    [
        ["2025-04-13T15:03:49.174000000", "2025-04-13T15:05:36.030000001"],
        ["2025-04-13T15:03:49.174000000", None, "2025-04-13T15:05:36.030000001", None],
        ["1999-12-31T23:59:59.999999999"],
    ],
)
def test_canonical_nanosecond_dates_match_pandas(values):
    got = _iso_ns_dates(values)
    ref = pd.to_datetime(pd.Series(values), errors="coerce", utc=False, format="ISO8601")
    assert got is not None
    assert ref.dtype == got.dtype
    assert np.array_equal(ref.to_numpy().view("i8"), got.view("i8"))


@pytest.mark.parametrize(
    "values",
    [
        ["2025-04-13T15:03:49.174000"],  # microseconds: pandas infers [us]
        ["2025-04-13T15:03:49.17400000Z"],  # offset (same length): tz-aware in pandas
        ["2025-02-30T15:03:49.174000000"],  # invalid date
        ["2025-04-13 15:03:49.174000000"],  # space separator: not the canonical spelling
        [None, None],
        [],
    ],
)
def test_other_date_spellings_are_left_to_pandas(values):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert _iso_ns_dates(values) is None


def _reference_coerce(data: dict) -> dict:
    """The pre-H8 ``_coerce_null_like_payload``."""
    if data and any(
        isinstance(v, list | tuple)
        and any(isinstance(x, str) and x in _NULL_LIKE_TOKEN_PROBE for x in v)
        for v in data.values()
    ):
        return {
            k: _coerce_null_like_string_list(list(v)) if isinstance(v, list | tuple) else v
            for k, v in data.items()
        }
    return data


def test_null_like_coercion_matches_the_per_item_rule():
    rng = random.Random(5)
    pool = [*_POOL, ["nested"], {"k": 1}, (1, 2)]
    for _ in range(500):
        data: dict[str, Any] = {
            f"c{i}": [rng.choice(pool) for _ in range(rng.randint(0, 6))]
            for i in range(rng.randint(0, 4))
        }
        if rng.random() < 0.2:
            data["scalar"] = rng.choice(pool)
        if rng.random() < 0.2:
            data["tup"] = tuple(rng.choice(pool) for _ in range(3))
        ref = _reference_coerce(data)
        got = _coerce_null_like_payload(data)
        assert got == ref
        assert {k: [type(x) for x in v] for k, v in got.items() if isinstance(v, list)} == {
            k: [type(x) for x in v] for k, v in ref.items() if isinstance(v, list)
        }
        assert (got is data) == (ref is data)


def _polars_frame(rng: random.Random, *, mutate: bool) -> tuple[Any, dict]:
    entries = []
    for code, team in (("LEC", "Ferrari"), ("VER", rng.choice(["Red Bull", "", "None"]))):
        payload = _driver_payload(rng, rng.randint(0, 5), code)
        if mutate:
            key = rng.choice(list(payload))
            payload[key] = [
                rng.choice([None, "None", " none ", "", "x", 1.5]) for _ in payload[key]
            ]
        entries.append((_normalize_lap_payload_polars(payload), code, team))
    merged = core._merge_lap_payloads(entries)
    return pl.DataFrame(merged, strict=False), merged


@pytest.mark.parametrize("categorical", [False, True])
def test_one_plan_polars_processing_matches_the_steps(monkeypatch, categorical):
    monkeypatch.setitem(get_config()._config, "polars_lap_categorical", categorical)
    rng = random.Random(13)
    compared = 0
    for _ in range(150):
        frame, merged = _polars_frame(rng, mutate=rng.random() < 0.5)
        try:
            ref = _process_lap_df_stepwise(frame, "polars")
        except Exception:
            with pytest.raises(Exception):  # noqa: B017, PT011
                _process_polars_laps(frame, merged)
            continue
        for got in (_process_polars_laps(frame, merged), _process_polars_laps(frame)):
            assert got.schema == ref.schema
            assert got.equals(ref, null_equal=True)
        compared += 1
    assert compared > 100


def test_polars_processing_handles_raw_frames_and_empty_frames():
    raw = pl.DataFrame(
        {
            "time": [90.5, None],
            "lap": [1, 2],
            "compound": ["SOFT", "None"],
            "Driver": ["VER", "VER"],
            "Team": ["Red Bull", ""],
            "pb": [True, None],
            "del": [None, None],
            "lSD": ["2026-06-07T13:03:11.904000000", None],
        },
        strict=False,
    )
    for frame in (raw, raw.head(0), raw.select("time")):
        ref = _process_lap_df_stepwise(frame, "polars")
        got = _process_lap_df(frame, "polars")
        assert got.schema == ref.schema
        assert got.columns == ref.columns
        assert got.equals(ref, null_equal=True)


def test_requested_payloads_are_derived_in_one_pass():
    session = core.Session(2026, "Monaco Grand Prix", "Race", False, "pandas")
    table = {"drv": ["A", "A", "B", "C", "C"], "lap": [1, 2, 1, 1, 2], "scalar": 3}
    session._remember_local_payload("session_laptimes.json", table)
    requests = [({"driver": d}, f"{d}/laptimes.json") for d in ("C", "A", "X")]
    derived = session._derive_requested_laptime_payloads(requests)
    assert set(derived) == {"C/laptimes.json", "A/laptimes.json"}
    reference = core.Session(2026, "Monaco Grand Prix", "Race", False, "pandas")
    reference._remember_local_payload("session_laptimes.json", table)
    for code in ("C", "A", "X"):
        reference._get_or_derive_driver_laptime_payload(code)
    assert session._memo._json_payloads == reference._memo._json_payloads
    # The full-table pass reuses those objects and slices only the others.
    again = dict(session._derive_driver_laptime_payloads_from_session(table, derived=derived))
    assert again["A/laptimes.json"] is derived["A/laptimes.json"]
    assert again["B/laptimes.json"] == {"drv": ["B"], "lap": [1], "scalar": 3}
    assert session._derive_requested_laptime_payloads(requests) == {}  # all memoized now


GP = "Monaco%20Grand%20Prix"
PREFIX = f"2026/{GP}/Race/"
DRIVERS = {
    "drivers": [
        {"driver": "VER", "team": "Red Bull Racing", "dn": "1"},
        {"driver": "LEC", "team": "Ferrari", "dn": "16"},
    ]
}


def _session_laptimes() -> dict[str, list]:
    rng = random.Random(21)
    ver = _driver_payload(rng, 4, "VER")
    lec = _driver_payload(rng, 3, "LEC")
    return {key: ver[key] + lec[key] for key in ver}


def _warm_session(lib: str) -> core.Session:
    clear_lap_cache()
    cache = get_cache()
    cache.set(PREFIX + "drivers.json", DRIVERS)
    cache.set(PREFIX + "session_laptimes.json", _session_laptimes())
    session = core.Session(2026, "Monaco Grand Prix", "Race", True, lib)
    session._memo.has_session_data = True
    return session


def _drop_laps_tier() -> None:
    cache = get_cache()
    for table in ("laps_frames", "laps_frames_pl"):
        cache.conn.execute(f"DELETE FROM {table}")
    cache.conn.commit()


@pytest.mark.parametrize("lib", ["pandas", "polars"])
def test_warm_first_laps_load_needs_no_event_loop_and_matches_laps_async(monkeypatch, lib):
    _drop_laps_tier()
    session = _warm_session(lib)

    async def _no_fetch(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a warm first laps load must not start the async pipeline")

    with monkeypatch.context() as patched:
        patched.setattr(core, "fetch_multiple_async", _no_fetch)
        patched.setattr(
            core,
            "_run_coroutine_sync",
            lambda *_a, **_k: pytest.fail("a warm first laps load must not run a coroutine"),
        )
        patched.delitem(sys.modules, "asyncio", raising=False)
        laps = session.laps
        assert "asyncio" not in sys.modules
    assert laps.session is session
    assert session._session_tables_prefetched

    _drop_laps_tier()
    reference = _warm_session(lib)
    ref = asyncio.run(reference.laps_async())
    if lib == "pandas":
        _assert_identical(pd.DataFrame(ref), pd.DataFrame(laps))
    else:
        assert laps.schema == ref.schema
        assert laps.base_class_view.equals(ref.base_class_view, null_equal=True)
    # Both loads wrote the same frame to the laps tier.
    tier = get_cache().get_laps_frame(2026, GP, "Race", lib=lib)
    assert tier is not None
    assert tier.shape == laps.shape


def test_warm_load_with_a_missing_wave_table_keeps_the_async_wave(monkeypatch):
    _drop_laps_tier()
    session = _warm_session("pandas")
    session._requested_session_tables.add("weather.json")  # not in the cache
    assert session._laps_from_cached_payloads() is None
    assert session._get_local_payload("drivers.json") is None  # nothing memoized
    assert not session._session_tables_prefetched


def test_cold_session_skips_the_cached_laps_build():
    session = core.Session(2026, "Monaco Grand Prix", "Race", True, "pandas")
    session._memo.has_session_data = False
    assert session._laps_from_cached_payloads() is None
