"""Contract tests for the Thebes performance series.

TH1 - typed-columnar frame-tier codec (``core_utils.frame_codec``) and its
      wiring into the SQLite frame tier (legacy pickled rows stay readable,
      corrupt rows degrade to misses, unsupported frames fall back to pickle).
TH2 - direct block assembly in ``_typed_telemetry_frame`` (same frame as the
      public constructor path, same consolidated block layout).
"""

from __future__ import annotations

import pickle

import numpy as np
import pandas as pd
import pytest

from tif1.cache import Cache, _compress_frame_blob
from tif1.core_utils import frame_codec, helpers
from tif1.core_utils.helpers import _create_telemetry_df, _frame_from_arrays

_PAYLOAD = {
    "time": [0.0, 0.016, 0.256, 0.5],
    "rpm": [10500.0, 10620.0, 10700.0, 10800.0],
    "speed": [250.0, 251.0, 252.0, 253.5],
    "gear": [7, 7, 8, 8],
    "throttle": [100.0, 100.0, 99.0, 98.0],
    "brake": [0, 0, 1, 1],
    "drs": [12, 12, 8, 8],
    "distance": [0.0, 1.1, 2.2, 3.3],
    "rel_distance": [0.0, 0.001, 0.002, 0.003],
    "DriverAhead": ["1", "1", "None", "44"],
    "DistanceToDriverAhead": ["None", 12.5, 12.0, "None"],
    "acc_x": [0.1, 0.2, 0.3, 0.4],
    "x": [1.0, 2.0, 3.0, 4.0],
    "y": [1.0, 2.0, 3.0, 4.0],
    "z": [0.0, 0.0, 0.0, 0.0],
    "dataKey": "2026-Monaco Grand Prix-Race-VER-1",
}


def _frame(payload=_PAYLOAD, driver="VER", lap=1) -> pd.DataFrame:
    frame = _create_telemetry_df(payload, driver, lap, "pandas")
    assert frame is not None
    return frame


def _assert_same_frame(left: pd.DataFrame, right: pd.DataFrame) -> None:
    assert list(left.columns) == list(right.columns)
    assert left.dtypes.equals(right.dtypes)
    assert left.index.equals(right.index)
    assert left.equals(right)
    assert len(left._mgr.blocks) == len(right._mgr.blocks)


# -- TH2: direct block assembly ----------------------------------------------


def test_direct_block_assembly_matches_public_constructor_path():
    frame = _frame()
    columns = list(frame.columns)
    arrays = [
        frame[c].array
        if isinstance(frame[c].dtype, pd.api.extensions.ExtensionDtype)
        else frame[c].to_numpy()
        for c in columns
    ]
    public = _frame_from_arrays(arrays, columns, len(frame))
    direct = helpers._direct_frame_from_columns(arrays, columns, len(frame))
    _assert_same_frame(direct, public)
    _assert_same_frame(frame, public)


def test_direct_block_assembly_consolidates_float_columns_into_one_block():
    frame = _frame()
    float_blocks = [b for b in frame._mgr.blocks if b.dtype == np.dtype("float64")]
    assert len(float_blocks) == 1
    assert float_blocks[0].shape[0] >= 8


def test_direct_block_assembly_shares_the_column_index_between_frames():
    first, second = _frame(lap=1), _frame(lap=2)
    assert first.columns is second.columns


def test_direct_block_frames_are_writable_and_independent():
    first, second = _frame(lap=1), _frame(lap=2)
    first.loc[0, "Speed"] = -1.0
    first.loc[0, "nGear"] = 1
    assert second.loc[0, "Speed"] == 250.0
    assert second.loc[0, "nGear"] == 7


def test_frame_from_columns_falls_back_when_direct_path_unavailable(monkeypatch):
    monkeypatch.setattr(helpers, "_DIRECT_BLOCKS_OK", False)
    frame = _frame()
    _assert_same_frame(frame, _frame())


def test_frame_from_columns_falls_back_on_direct_build_error(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise ValueError("drift")

    monkeypatch.setattr(helpers, "_direct_frame_from_columns", _boom)
    frame = _frame()
    assert list(frame.columns)[-2:] == ["Driver", "LapNumber"]


def test_probe_rejects_drifted_internals(monkeypatch):
    monkeypatch.setattr(helpers, "_DIRECT_BLOCKS_AVAILABLE", False)
    assert helpers._probe_direct_blocks() is False


# -- TH1: frame codec ---------------------------------------------------------


def test_codec_round_trip_is_identical():
    frame = _frame()
    blob = frame_codec.encode_frame(frame)
    assert blob is not None
    assert frame_codec.is_encoded_frame(blob)
    decoded = frame_codec.decode_frame(blob)
    _assert_same_frame(decoded, frame)


def test_codec_round_trip_all_sentinel_string_column():
    payload = dict(_PAYLOAD, DriverAhead=["None", "None", "None", "None"])
    frame = _frame(payload)
    assert frame["DriverAhead"].dtype == object
    decoded = frame_codec.decode_frame(frame_codec.encode_frame(frame))
    _assert_same_frame(decoded, frame)
    assert decoded["DriverAhead"].tolist() == [None] * 4


def test_codec_round_trip_clean_string_column_keeps_str_dtype():
    payload = dict(_PAYLOAD, DriverAhead=["1", "1", "16", "44"])
    frame = _frame(payload)
    assert str(frame["DriverAhead"].dtype) == "str"
    decoded = frame_codec.decode_frame(frame_codec.encode_frame(frame))
    _assert_same_frame(decoded, frame)


def test_codec_round_trip_nullable_int_with_missing_values():
    frame = _frame()
    frame["nGear"] = pd.array([7, None, 8, 8], dtype="Int64")
    decoded = frame_codec.decode_frame(frame_codec.encode_frame(frame))
    _assert_same_frame(decoded, frame)
    assert decoded["nGear"].isna().tolist() == [False, True, False, False]


def test_codec_decoded_frames_are_writable_and_pickle_like_the_original():
    frame = _frame()
    decoded = frame_codec.decode_frame(frame_codec.encode_frame(frame))
    decoded.loc[0, "Speed"] = 1.0
    decoded.loc[0, "DRS"] = 0
    assert decoded.loc[0, "Speed"] == 1.0
    assert pickle.loads(pickle.dumps(decoded)).equals(decoded)


def test_codec_declines_unsupported_frames():
    frame = _frame()
    frame["Extra"] = pd.array([1.5, 2.5, 3.5, 4.5], dtype="Float64")
    assert frame_codec.encode_frame(frame) is None
    assert frame_codec.encode_frame(frame.iloc[1:]) is None  # non-zero-based index


def test_codec_rejects_malformed_blobs():
    with pytest.raises(ValueError, match="typed-columnar"):
        frame_codec.decode_frame(b"not a frame")
    blob = frame_codec.encode_frame(_frame())
    with pytest.raises(ValueError, match="corrupt frame"):
        frame_codec.decode_frame(blob[:40])


def test_frame_tier_writes_codec_rows_and_reads_legacy_pickles(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        frame = _frame()
        assert cache.set_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1, frame)]) == 1
        row = cache.conn.execute("SELECT frame FROM telemetry_frames").fetchone()[0]
        from tif1.cache import _decompress_frame_blob

        assert frame_codec.is_encoded_frame(_decompress_frame_blob(row))
        # A row written by an earlier release (plain pickle) reads too.
        cache.conn.execute(
            "INSERT OR REPLACE INTO telemetry_frames VALUES (?, ?, ?, ?, ?, ?)",
            (2026, "GP", "Race", "VER", 2, _compress_frame_blob(pickle.dumps(frame, protocol=5))),
        )
        cache.conn.execute(
            "INSERT OR REPLACE INTO telemetry_frames VALUES (?, ?, ?, ?, ?, ?)",
            (2026, "GP", "Race", "VER", 3, _compress_frame_blob(b"TF1\x01garbage")),
        )
        cache.conn.commit()
        hits = cache.get_telemetry_frames_batch(
            2026, "GP", "Race", [("VER", 1), ("VER", 2), ("VER", 3)]
        )
        assert set(hits) == {("VER", 1), ("VER", 2)}
        _assert_same_frame(hits[("VER", 1)], frame)
        _assert_same_frame(hits[("VER", 2)], frame)
    finally:
        cache.close()


def test_frame_tier_falls_back_to_pickle_for_unsupported_frames(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        frame = _frame()
        frame["Extra"] = pd.array([1.5, 2.5, 3.5, 4.5], dtype="Float64")
        assert cache.set_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1, frame)]) == 1
        hits = cache.get_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1)])
        assert hits[("VER", 1)].equals(frame)
    finally:
        cache.close()


# -- TH3: latency-aware CDN routing + straggler hedging -----------------------

import asyncio  # noqa: E402

from tif1.cdn import CDNManager, CDNSource  # noqa: E402
from tif1.exceptions import DataNotFoundError, NetworkError  # noqa: E402


def _manager(hedge_max_delay: float = 0.05) -> CDNManager:
    manager = CDNManager()
    manager.sources = [
        CDNSource(name="primary", base_url="https://primary.test", priority=1),
        CDNSource(name="backup", base_url="https://backup.test", priority=2),
    ]
    manager.reset()
    manager._failure_counts = {s.name: 0 for s in manager.sources}
    manager._hedge_enabled = True
    manager._hedge_max_delay = hedge_max_delay
    return manager


def _fetch_func(
    latencies: dict[str, float], failures: dict[str, Exception] | None = None, calls=None
):
    failures = failures or {}

    async def fetch(source, url):
        if calls is not None:
            calls.append(source.name)
        await asyncio.sleep(latencies.get(source.name, 0.0))
        if source.name in failures:
            raise failures[source.name]
        return {"from": source.name, "url": url}

    return fetch


def test_hedge_fires_only_for_stragglers_and_backup_wins():
    manager = _manager(hedge_max_delay=0.05)
    calls: list[str] = []
    fetch = _fetch_func({"primary": 0.5, "backup": 0.01}, calls=calls)
    result = asyncio.run(manager.try_sources_async(2026, "GP", "Race", "VER/1_tel.json", fetch))
    assert result["from"] == "backup"
    assert calls == ["primary", "backup"]
    # The abandoned primary request counts as a sample at the hedge delay.
    assert manager.latency_estimate("primary") == pytest.approx(0.05, abs=0.01)
    assert manager.latency_estimate("backup") is not None


def test_fast_primary_never_hedges():
    manager = _manager(hedge_max_delay=0.5)
    calls: list[str] = []
    fetch = _fetch_func({"primary": 0.0, "backup": 0.0}, calls=calls)
    result = asyncio.run(manager.try_sources_async(2026, "GP", "Race", "VER/1_tel.json", fetch))
    assert result["from"] == "primary"
    assert calls == ["primary"]


def test_backup_refusal_does_not_decide_the_request():
    manager = _manager(hedge_max_delay=0.02)
    fetch = _fetch_func(
        {"primary": 0.1, "backup": 0.0},
        failures={"backup": DataNotFoundError(year=2026, event="GP", session="Race")},
    )
    result = asyncio.run(manager.try_sources_async(2026, "GP", "Race", "VER/1_tel.json", fetch))
    assert result["from"] == "primary"
    assert manager._failure_counts["backup"] == 0


def test_primary_refusal_after_hedge_still_raises_data_not_found():
    manager = _manager(hedge_max_delay=0.02)
    fetch = _fetch_func(
        {"primary": 0.1, "backup": 0.05},
        failures={
            "primary": DataNotFoundError(year=2026, event="GP", session="Race"),
            "backup": DataNotFoundError(year=2026, event="GP", session="Race"),
        },
    )
    with pytest.raises(DataNotFoundError):
        asyncio.run(manager.try_sources_async(2026, "GP", "Race", "VER/1_tel.json", fetch))


def test_primary_transport_failure_falls_through_to_backup_result():
    manager = _manager(hedge_max_delay=0.02)
    fetch = _fetch_func(
        {"primary": 0.1, "backup": 0.2},
        failures={"primary": NetworkError(url="x", status_code=503)},
    )
    result = asyncio.run(manager.try_sources_async(2026, "GP", "Race", "VER/1_tel.json", fetch))
    assert result["from"] == "backup"
    assert manager._failure_counts["primary"] == 1


def test_hedging_can_be_disabled():
    manager = _manager(hedge_max_delay=0.02)
    manager._hedge_enabled = False
    calls: list[str] = []
    fetch = _fetch_func({"primary": 0.1, "backup": 0.0}, calls=calls)
    result = asyncio.run(manager.try_sources_async(2026, "GP", "Race", "VER/1_tel.json", fetch))
    assert result["from"] == "primary"
    assert calls == ["primary"]


def test_slow_primary_is_reranked_behind_a_measured_backup():
    manager = _manager()
    for _ in range(8):
        manager.record_latency("primary", 3.0)
        manager.record_latency("backup", 0.5)
    assert [s.name for s in manager.get_sources()] == ["backup", "primary"]
    manager.reset()
    assert [s.name for s in manager.get_sources()] == ["primary", "backup"]


def test_rerank_needs_enough_samples_on_both_sources():
    manager = _manager()
    for _ in range(8):
        manager.record_latency("primary", 3.0)
    manager.record_latency("backup", 0.1)
    assert [s.name for s in manager.get_sources()] == ["primary", "backup"]


def test_sync_fallback_records_latency():
    manager = _manager()
    manager.try_sources(2026, "GP", "Race", "drivers.json", lambda _url: {"ok": True})
    assert manager.latency_estimate("primary") is not None


# -- TH6: laps session tables in one wave -------------------------------------


def test_laps_wave_fetches_drivers_and_laptimes_together(monkeypatch):
    from tif1 import core

    session = core.Session(2026, "Monaco Grand Prix", "Race", enable_cache=False)
    session._requested_session_tables.update({"weather.json", "rcm.json"})
    seen: list[list[str]] = []

    async def fake_fetch_multiple(requests, **_kwargs):
        seen.append([r[3] for r in requests])
        return [{"payload": r[3]} for r in requests]

    monkeypatch.setattr(core, "fetch_multiple_async", fake_fetch_multiple)
    asyncio.run(session._prefetch_laps_wave_async(False))
    assert seen == [["drivers.json", "session_laptimes.json", "weather.json", "rcm.json"]]
    assert session._get_local_payload("session_laptimes.json") == {
        "payload": "session_laptimes.json"
    }
    assert session._session_tables_prefetched is True
    # A second call has nothing left to fetch.
    asyncio.run(session._prefetch_laps_wave_async(False))
    assert len(seen) == 1


def test_laps_wave_skips_single_missing_table(monkeypatch):
    from tif1 import core

    session = core.Session(2026, "Monaco Grand Prix", "Race", enable_cache=False)
    session._remember_local_payload("drivers.json", {"drivers": []})
    called = []

    async def fake_fetch_multiple(requests, **_kwargs):
        called.append(requests)
        return [None] * len(requests)

    monkeypatch.setattr(core, "fetch_multiple_async", fake_fetch_multiple)
    asyncio.run(session._prefetch_laps_wave_async(False))
    assert called == []


# -- TH7: stale-while-revalidate schedule -------------------------------------


def test_stale_schedule_row_is_served_while_a_background_refresh_runs(monkeypatch):
    import threading
    import time

    from tif1 import events

    year = 2099
    raw_year = {"events": []}
    monkeypatch.setattr(
        events, "_read_cached_f1schedule_year", lambda _y: (time.time() - 10**6, raw_year)
    )
    monkeypatch.setattr(
        events, "_convert_f1schedule_year", lambda raw, _y: {"events": [], "raw": raw}
    )
    monkeypatch.setattr(events, "_schedule_ttl_seconds", lambda: 3600.0)
    refreshed = threading.Event()

    def slow_fetch(_year):
        time.sleep(0.2)
        refreshed.set()
        return {"events": []}

    monkeypatch.setattr(events, "_fetch_f1schedule_year", slow_fetch)
    monkeypatch.setattr(
        events.get_config(), "_config", dict(events.get_config()._config, offline_mode=False)
    )
    t0 = time.perf_counter()
    payload, expires_at = events._load_live_year_payload(year, time.time())
    elapsed = time.perf_counter() - t0
    assert payload["raw"] is raw_year
    assert elapsed < 0.15
    assert expires_at <= time.time() + events._F1SCHEDULE_STALE_GRACE_SECONDS + 1
    assert refreshed.wait(2.0)


def test_no_cached_row_still_fetches_inline(monkeypatch):
    import time

    from tif1 import events

    monkeypatch.setattr(events, "_read_cached_f1schedule_year", lambda _y: None)
    monkeypatch.setattr(events, "_fetch_f1schedule_year", lambda _y: {"events": ["live"]})
    monkeypatch.setattr(
        events.get_config(), "_config", dict(events.get_config()._config, offline_mode=False)
    )
    payload, _ = events._load_live_year_payload(2099, time.time())
    assert payload == {"events": ["live"]}
