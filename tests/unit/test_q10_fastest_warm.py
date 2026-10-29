"""Q10 contracts: the fastest-lap convenience paths on a warm cache.

On a session already in the persistent cache, ``get_fastest_lap_tel``,
``get_fastest_laps_tels`` and ``get_driver(...).get_fastest_lap_tel`` read the
cached payloads synchronously (no event loop, no ``asyncio`` import) and pick
the same laps as before; a cold session keeps the async fetch pipeline.
"""

from __future__ import annotations

import sys

import pandas as pd
import pytest

from tif1 import core
from tif1.cache import clear_lap_cache, get_cache
from tif1.core_utils.helpers import _create_telemetry_df

GP = "Monaco%20Grand%20Prix"
PREFIX = f"2026/{GP}/Race/"

DRIVERS = {
    "drivers": [
        {"driver": "VER", "team": "Red Bull Racing", "dn": "1"},
        {"driver": "LEC", "team": "Ferrari", "dn": "16"},
    ]
}
# LEC lap 2 is the quickest time but deleted; VER lap 3 ties LEC lap 3 and
# VER is first in drivers.json, so VER lap 3 is the overall fastest lap.
SESSION_LAPTIMES = {
    "drv": ["VER", "VER", "VER", "LEC", "LEC", "LEC"],
    "lap": [1, 2, 3, 1, 2, 3],
    "time": [80.5, 80.4, 80.1, 81.0, 79.0, 80.1],
    "del": [False, False, False, False, True, False],
    "team": ["Red Bull Racing"] * 3 + ["Ferrari"] * 3,
}


def _tel_payload(offset: float) -> dict:
    return {
        "Time": [0.0, 0.1, 0.2],
        "Speed": [200.0 + offset, 201.0 + offset, 202.0 + offset],
        "RPM": [10000.0, 10001.0, 10002.0],
        "nGear": [7, 7, 7],
        "Throttle": [100.0, 100.0, 100.0],
        "Brake": [0, 0, 0],
        "DRS": [0, 0, 0],
    }


def _session(*, warm: bool) -> core.Session:
    clear_lap_cache()
    cache = get_cache()
    if warm:
        cache.set(PREFIX + "drivers.json", DRIVERS)
        cache.set(PREFIX + "session_laptimes.json", SESSION_LAPTIMES)
        frames = []
        for driver, lap, offset in (("VER", 3, 0.0), ("LEC", 3, 5.0), ("LEC", 1, 9.0)):
            payload = _tel_payload(offset)
            cache.set_telemetry(2026, GP, "Race", driver, lap, payload)
            frame = _create_telemetry_df(payload, driver, lap, "pandas")
            frames.append((driver, lap, frame))
        cache.set_telemetry_frames_batch(2026, GP, "Race", frames)
        cache.conn.commit()
    session = core.Session(2026, "Monaco Grand Prix", "Race", True, "pandas")
    session._memo.has_session_data = warm
    return session


def _forbid_async(monkeypatch):
    async def _no_fetch(*_args, **_kwargs):
        raise AssertionError("warm fastest-lap path must not start the async pipeline")

    monkeypatch.setattr(core, "fetch_multiple_async", _no_fetch)
    monkeypatch.delitem(sys.modules, "asyncio", raising=False)


@pytest.mark.parametrize(
    ("call", "laps_tier"),
    [
        (lambda s: s.get_fastest_lap_tel(), False),
        (lambda s: s.get_fastest_laps_tels(by_driver=False), False),
        # ``get_driver`` builds the results row; without an official
        # classification that reads the laps, from the frame tier when warm.
        (lambda s: s.get_driver("VER").get_fastest_lap_tel(), True),
    ],
)
def test_warm_fastest_lap_tel_is_asyncio_free_and_skips_deleted_laps(monkeypatch, call, laps_tier):
    session = _session(warm=True)
    if laps_tier:
        _store_laps_frame()
    _forbid_async(monkeypatch)
    tel = call(session)
    assert "asyncio" not in sys.modules
    assert list(tel["Speed"]) == [200.0, 201.0, 202.0]  # VER lap 3, not deleted LEC lap 2


def test_warm_fastest_laps_tels_by_driver_is_asyncio_free(monkeypatch):
    session = _session(warm=True)
    _forbid_async(monkeypatch)
    tel = session.get_fastest_laps_tels(by_driver=True)
    assert "asyncio" not in sys.modules
    assert list(zip(tel["Driver"], tel["LapNumber"], strict=True)) == [
        ("VER", 3),
        ("VER", 3),
        ("VER", 3),
        ("LEC", 3),
        ("LEC", 3),
        ("LEC", 3),
    ]
    assert session._fastest_lap_tel_ref == ("VER", 3)


def test_warm_driver_lookup_reads_cached_payloads_synchronously(monkeypatch):
    session = _session(warm=True)
    _store_laps_frame()  # the results row reads the laps (no official classification)
    _forbid_async(monkeypatch)
    driver = session.get_driver("LEC")
    assert "asyncio" not in sys.modules
    assert driver._prefetched_lap_data["lap"] == [1, 2, 3]


def _store_laps_frame(*, deleted=True) -> None:
    frame = pd.DataFrame(
        {
            "Driver": pd.Categorical(SESSION_LAPTIMES["drv"]),
            "LapNumber": pd.array(SESSION_LAPTIMES["lap"], dtype="Int64"),
            "LapTime": pd.to_timedelta(SESSION_LAPTIMES["time"], unit="s"),
        }
    )
    if deleted:
        frame["Deleted"] = SESSION_LAPTIMES["del"]
    get_cache().set_laps_frame(2026, GP, "Race", frame)


def test_warm_laps_frame_tier_answers_the_fastest_lap_lookup(monkeypatch):
    session = _session(warm=True)
    _store_laps_frame()
    _forbid_async(monkeypatch)
    monkeypatch.setattr(
        session,
        "_fetch_laptime_payloads",
        lambda *_a, **_k: pytest.fail("the laps frame tier must answer the lookup"),
    )
    assert session._get_fastest_lap_reference() == ("VER", 3)  # tie -> drivers.json order
    assert session._get_fastest_lap_refs_from_raw() == [("VER", 3), ("LEC", 3)]
    assert session._get_fastest_lap_refs_from_raw(drivers=["LEC"]) == [("LEC", 3)]
    assert list(session.get_fastest_lap_tel()["Speed"]) == [200.0, 201.0, 202.0]
    assert "asyncio" not in sys.modules


def test_laps_frame_tier_selection_matches_the_payload_scan():
    session = _session(warm=True)
    requests = session._build_driver_laptime_requests()
    payloads, _ = session._fetch_laptime_payloads(requests, operation="test")
    raw = [
        session._extract_fastest_lap_candidate(info["driver"], payload)
        for (info, _path), payload in zip(requests, payloads, strict=True)
    ]
    _store_laps_frame()
    assert session._fastest_lap_candidates_from_laps_tier(requests) == raw
    _store_laps_frame(deleted=False)  # without the flag the deleted LEC lap 2 counts
    tier = session._fastest_lap_candidates_from_laps_tier(requests)
    assert tier is not None
    assert tier[1][:2] == ("LEC", 2)


def test_laps_frame_tier_is_skipped_for_cold_or_polars_sessions():
    _store_laps_frame()
    cold = _session(warm=False)
    assert cold._fastest_lap_candidates_from_laps_tier([({"driver": "VER"}, "x")]) is None
    polars_session = core.Session(2026, "Monaco Grand Prix", "Race", True, "polars")
    polars_session._memo.has_session_data = True
    assert polars_session._fastest_lap_candidates_from_laps_tier([({"driver": "VER"}, "x")]) is None


def test_cold_session_keeps_the_async_fetch_pipeline(monkeypatch):
    session = _session(warm=False)
    calls: list[list[str]] = []

    async def _fake_fetch(requests, **_kwargs):
        calls.append([request[3] for request in requests])
        return [
            SESSION_LAPTIMES if path.endswith("laptimes.json") else None for *_, path in requests
        ]

    monkeypatch.setattr(core, "fetch_multiple_async", _fake_fetch)
    monkeypatch.setattr(session, "_fetch_json_unvalidated", lambda _path: DRIVERS)
    monkeypatch.setattr(
        session,
        "_get_telemetry_df_for_ref",
        lambda driver, lap, **_kw: pd.DataFrame({"Driver": [driver], "LapNumber": [lap]}),
    )
    tel = session.get_fastest_lap_tel()
    assert calls == [["session_laptimes.json"]]
    assert (tel["Driver"].iloc[0], tel["LapNumber"].iloc[0]) == ("VER", 3)


def test_partial_cache_hit_falls_back_to_the_async_fetch(monkeypatch):
    session = _session(warm=False)
    session._memo.has_session_data = True  # drivers cached, laptimes missing
    get_cache().set(PREFIX + "drivers.json", DRIVERS)
    assert session._cached_payloads_sync(["drivers.json", "session_laptimes.json"]) is None
    assert session._cached_payloads_sync(["drivers.json"]) == [DRIVERS]


@pytest.mark.parametrize(
    "source",
    [
        ["A", "A", "B", "B", "B", "C"],  # contiguous runs
        ["A", "B", "A", "C", "B", "A"],  # interleaved rows
    ],
)
def test_driver_payload_derivation_matches_the_row_gather(source):
    session = core.Session(2026, "Monaco Grand Prix", "Race", False, "pandas")
    payload = {
        "drv": source,
        "lap": list(range(1, 7)),
        "short": [1, 2, 3, 4],  # shorter than the table: padded with None
        "tup": tuple("abcdef"),
        "scalar": 7,
    }
    derived = dict(session._derive_driver_laptime_payloads_from_session(payload))
    for driver in set(source):
        positions = [i for i, d in enumerate(source) if d == driver]
        expected = {
            key: [value[i] if i < len(value) else None for i in positions]
            if isinstance(value, list | tuple)
            else value
            for key, value in payload.items()
        }
        got = derived[f"{driver}/laptimes.json"]
        assert got == expected
        assert all(type(got[k]) is list for k in ("drv", "lap", "short", "tup"))
