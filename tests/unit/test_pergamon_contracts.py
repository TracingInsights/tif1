"""Contract tests for the Pergamon series (warm-path cache usage).

P1  fast frame-decode primitives (shared placements/indexes, direct block classes)
P2  GIL-released parallel lz4 decompression through pyarrow
P3  per-driver typed-columnar blobs with view slicing + legacy upgrade
P4  batched polars IPC read (one conversion for the session)
P5  asyncio-free warm path
P6  single-lap access served from the frame tier (whole driver memoized)
P10 merged driver telemetry straight from the driver blob
"""

from __future__ import annotations

import pickle
import sys
import time

import pandas as pd
import pytest

from tif1.cache import (
    Cache,
    _compress_frame_blob,
    _decompress_frame_rows,
    _join_frame_writers,
    _lz4_frame_content_size,
    clear_lap_cache,
)
from tif1.core_utils import frame_codec, helpers
from tif1.core_utils.helpers import _create_telemetry_df

GP = "Monaco%20Grand%20Prix"


def _payload(n: int, ahead: list | None = None, start: float = 0.0) -> dict:
    return {
        "Time": [start + 0.1 * i for i in range(n)],
        "RPM": [10000.0 + i for i in range(n)],
        "Speed": [200.0 + i for i in range(n)],
        "nGear": [7] * n,
        "Throttle": [100.0] * n,
        "Brake": [0] * n,
        "DRS": [0] * n,
        "DriverAhead": ahead if ahead is not None else ["44"] * n,
    }


def _frame(driver="VER", lap=1, n=4, ahead=None) -> pd.DataFrame:
    frame = _create_telemetry_df(_payload(n, ahead), driver, lap, "pandas")
    assert frame is not None
    return frame


def _same(left: pd.DataFrame, right: pd.DataFrame) -> None:
    assert list(left.columns) == list(right.columns)
    assert left.dtypes.equals(right.dtypes)
    assert left.index.equals(right.index)
    assert left.equals(right)
    assert len(left._mgr.blocks) == len(right._mgr.blocks)
    assert sorted(type(b).__name__ for b in left._mgr.blocks) == sorted(
        type(b).__name__ for b in right._mgr.blocks
    )


# -- P1: decode primitives -----------------------------------------------------


def test_shared_placements_and_range_index_are_cached():
    assert helpers._placement_for(3) is helpers._placement_for(3)
    assert helpers._placement_for((1, 2, 4)) is helpers._placement_for((1, 2, 4))
    assert helpers._range_index(675) is helpers._range_index(675)
    assert helpers._range_index(5).equals(pd.RangeIndex(5))


def test_decoded_frame_matches_constructor_frame_and_is_writable():
    frame = _frame(n=6, ahead=["44", None, "16", "16", None, "1"])
    blob = frame_codec.encode_frame(frame)
    assert blob is not None
    decoded = frame_codec.decode_frame(bytearray(blob))
    _same(decoded, frame)
    decoded.iloc[0, decoded.columns.get_loc("Speed")] = -1.0  # views must be writable
    assert decoded["Speed"].iloc[0] == -1.0
    # Read-only input is copied once and still writable afterwards.
    decoded_ro = frame_codec.decode_frame(blob)
    decoded_ro.iloc[0, decoded_ro.columns.get_loc("Speed")] = -2.0
    assert decoded_ro["Speed"].iloc[0] == -2.0


def test_decode_builds_requested_frame_class():
    from tif1.models import Telemetry

    frame = _frame()
    decoded = frame_codec.decode_frame(bytearray(frame_codec.encode_frame(frame)), Telemetry)
    assert type(decoded) is Telemetry
    assert decoded.session is None  # _metadata initialised
    assert decoded.driver is None
    _same(decoded, frame)


# -- P2: parallel decompression ---------------------------------------------------


def test_lz4_content_size_is_read_from_the_frame_header():
    blob = _compress_frame_blob(b"x" * 1000)
    assert _lz4_frame_content_size(blob) == 1000
    assert _lz4_frame_content_size(b"not lz4") == -1


def test_parallel_decompression_matches_serial_and_drops_unreadable_rows():
    payloads = [pickle.dumps({"i": i, "pad": b"p" * 5000}, protocol=5) for i in range(70)]
    rows = [("D", i, _compress_frame_blob(p)) for i, p in enumerate(payloads)]
    rows.append(("D", 999, b"\x04\x22\x4d\x18garbage-not-a-frame"))  # lz4 magic, corrupt
    serial = {}
    for chunk in _decompress_frame_rows(rows, workers=1):
        serial.update({(d, lap): bytes(raw) for d, lap, raw in chunk})
    parallel = {}
    for chunk in _decompress_frame_rows(rows, workers=4, min_parallel=2):
        parallel.update({(d, lap): bytes(raw) for d, lap, raw in chunk})
    assert serial == parallel
    assert len(serial) == 70
    assert ("D", 999) not in serial
    assert all(serial[("D", i)] == payloads[i] for i in range(70))


# -- P3: per-driver blobs ---------------------------------------------------------


def test_driver_blob_round_trips_every_lap_with_layout_groups():
    # Two layouts: DriverAhead with strings (str dtype) and all-None (object).
    frames = {
        1: _frame(lap=1, n=4, ahead=["44", "44", None, "16"]),
        2: _frame(lap=2, n=3, ahead=[None, None, None]),
        3: _frame(lap=3, n=5, ahead=["1"] * 5),
        7: _frame(lap=7, n=2, ahead=[None, None]),
    }
    blob, leftovers = frame_codec.encode_driver_frames(list(frames.items()))
    assert blob is not None
    assert leftovers == []
    assert frame_codec.is_driver_blob(blob)
    assert sorted(frame_codec.driver_blob_laps(blob)) == [1, 2, 3, 7]
    decoded = frame_codec.decode_driver_frames(bytearray(blob))
    assert set(decoded) == {1, 2, 3, 7}
    for lap, frame in frames.items():
        _same(decoded[lap], frame)
    only = frame_codec.decode_driver_frames(bytearray(blob), {3})
    assert set(only) == {3}
    _same(only[3], frames[3])


def test_driver_blob_frames_do_not_share_writable_cells():
    blob, _ = frame_codec.encode_driver_frames([(1, _frame(lap=1)), (2, _frame(lap=2))])
    decoded = frame_codec.decode_driver_frames(bytearray(blob))
    decoded[1].iloc[0, decoded[1].columns.get_loc("Speed")] = -5.0
    assert decoded[2]["Speed"].iloc[0] == 200.0
    assert int(decoded[2]["LapNumber"].iloc[0]) == 2


def test_driver_blob_hands_back_unsupported_frames():
    frame = _frame(lap=1)
    odd = _frame(lap=2)
    odd["Extra"] = pd.array([1.5, 2.5, 3.5, 4.5], dtype="Float64")
    blob, leftovers = frame_codec.encode_driver_frames([(1, frame), (2, odd)])
    assert blob is not None
    assert [lap for lap, _ in leftovers] == [2]
    assert frame_codec.driver_blob_laps(blob) == [1]


def test_driver_blob_rejects_malformed_input():
    with pytest.raises(ValueError, match="driver frame blob"):
        frame_codec.decode_driver_frames(b"not a driver blob")
    blob, _ = frame_codec.encode_driver_frames([(1, _frame())])
    with pytest.raises(ValueError, match="corrupt driver blob"):
        frame_codec.decode_driver_frames(blob[:30])


def test_frame_tier_writes_driver_rows_and_merges_partial_batches(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        assert cache.set_telemetry_frames_batch(2026, GP, "Race", [("VER", 1, _frame(lap=1))]) == 1
        assert cache.set_telemetry_frames_batch(2026, GP, "Race", [("VER", 3, _frame(lap=3))]) == 1
        rows = cache.conn.execute(
            "SELECT driver, laps FROM telemetry_frames_drv WHERE year = 2026"
        ).fetchall()
        assert rows == [("VER", "[1,3]")]
        hits = cache.get_telemetry_frames_batch(2026, GP, "Race", [("VER", 1), ("VER", 3)])
        assert set(hits) == {("VER", 1), ("VER", 3)}
        _same(hits[("VER", 1)], _frame(lap=1))
        _same(hits[("VER", 3)], _frame(lap=3))
        # whole_driver returns the driver's other laps too; a lap the tier
        # does not hold is absent either way.
        extra = cache.get_telemetry_frames_batch(
            2026, GP, "Race", [("VER", 1), ("VER", 9)], whole_driver=True
        )
        assert set(extra) == {("VER", 1), ("VER", 3)}
        assert cache.get_telemetry_frames_batch(2026, GP, "Race", [("VER", 9)]) == {}
        assert cache.get_telemetry_frames_batch(2026, GP, "Race", [("HAM", 1)]) == {}
    finally:
        cache.close()


def test_frame_tier_unsupported_frames_fall_back_to_per_frame_rows(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        odd = _frame(lap=2)
        odd["Extra"] = pd.array([1.5, 2.5, 3.5, 4.5], dtype="Float64")
        written = cache.set_telemetry_frames_batch(
            2026, GP, "Race", [("VER", 1, _frame(lap=1)), ("VER", 2, odd)]
        )
        assert written == 2
        assert cache.conn.execute("SELECT COUNT(*) FROM telemetry_frames").fetchone()[0] == 1
        hits = cache.get_telemetry_frames_batch(2026, GP, "Race", [("VER", 1), ("VER", 2)])
        assert hits[("VER", 2)].equals(odd)
        _same(hits[("VER", 1)], _frame(lap=1))
    finally:
        cache.close()


def test_legacy_rows_are_served_then_upgraded_in_the_background(tmp_path, monkeypatch):
    import tif1.cache as cache_module

    monkeypatch.setattr(cache_module, "_LEGACY_UPGRADE_MIN_FRAMES", 2)
    monkeypatch.setattr(cache_module, "_LEGACY_UPGRADE_DELAY_S", 0.0)
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = {lap: _frame(lap=lap) for lap in (1, 2, 3)}
        for lap, frame in frames.items():
            blob = frame_codec.encode_frame(frame) if lap != 2 else pickle.dumps(frame, protocol=5)
            cache.conn.execute(
                "INSERT OR REPLACE INTO telemetry_frames VALUES (?, ?, ?, ?, ?, ?)",
                (2026, GP, "Race", "VER", lap, _compress_frame_blob(blob)),
            )
        cache.conn.commit()
        hits = cache.get_telemetry_frames_batch(
            2026, GP, "Race", [("VER", lap) for lap in (1, 2, 3)]
        )
        assert set(hits) == {("VER", 1), ("VER", 2), ("VER", 3)}
        _join_frame_writers()
        assert cache.conn.execute("SELECT COUNT(*) FROM telemetry_frames").fetchone()[0] == 0
        assert (
            cache.conn.execute("SELECT laps FROM telemetry_frames_drv").fetchone()[0] == "[1,2,3]"
        )
        again = cache.get_telemetry_frames_batch(
            2026, GP, "Race", [("VER", lap) for lap in (1, 2, 3)]
        )
        for lap, frame in frames.items():
            _same(again[("VER", lap)], frame)
    finally:
        cache.close()


def test_corrupt_driver_row_degrades_to_a_miss(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        cache.conn.execute(
            "INSERT OR REPLACE INTO telemetry_frames_drv VALUES (?, ?, ?, ?, ?, ?)",
            (2026, GP, "Race", "VER", "[1]", _compress_frame_blob(b"TF1\x02garbage")),
        )
        cache.conn.commit()
        assert cache.get_telemetry_frames_batch(2026, GP, "Race", [("VER", 1)]) == {}
        assert cache.get_driver_merged_frame(2026, GP, "Race", "VER", [1]) is None
    finally:
        cache.close()


def test_invalidate_and_clear_drop_driver_rows(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        cache.set_telemetry_frames_batch(2026, GP, "Race", [("VER", 1, _frame())])
        cache.invalidate("telemetry")
        assert cache.conn.execute("SELECT COUNT(*) FROM telemetry_frames_drv").fetchone()[0] == 0
        cache.set_telemetry_frames_batch(2026, GP, "Race", [("VER", 1, _frame())])
        cache.clear()
        assert cache.conn.execute("SELECT COUNT(*) FROM telemetry_frames_drv").fetchone()[0] == 0
    finally:
        cache.close()


# -- P10: merged driver frame ----------------------------------------------------


def test_merged_frame_equals_concat_of_lap_frames_in_requested_order(tmp_path):
    frames = {
        1: _frame(lap=1, n=4, ahead=["44"] * 4),
        2: _frame(lap=2, n=3, ahead=[None] * 3),  # other layout group
        3: _frame(lap=3, n=5, ahead=["1"] * 5),
        4: _frame(lap=4, n=2, ahead=["1", None]),
    }
    cache = Cache(cache_dir=tmp_path)
    try:
        cache.set_telemetry_frames_batch(
            2026, GP, "Race", [("VER", lap, frame) for lap, frame in frames.items()]
        )
        for laps in ([1, 2, 3, 4], [3, 4], [4, 1], [2]):
            merged = cache.get_driver_merged_frame(2026, GP, "Race", "VER", laps)
            expected = pd.concat([frames[lap] for lap in laps], ignore_index=True)
            assert merged is not None
            assert list(merged.columns) == list(expected.columns)
            assert merged.dtypes.equals(expected.dtypes)
            assert merged.equals(expected)
        assert cache.get_driver_merged_frame(2026, GP, "Race", "VER", [1, 9]) is None
        assert cache.get_driver_merged_frame(2026, GP, "Race", "HAM", [1]) is None
    finally:
        cache.close()


# -- P4: polars batched read ------------------------------------------------------


def test_polars_tier_reads_all_drivers_in_one_conversion(tmp_path):
    pl = pytest.importorskip("polars")
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = []
        for driver in ("VER", "LEC", "HAM", "SAI", "NOR"):
            for lap in (1, 2):
                frame = _create_telemetry_df(_payload(3 + lap), driver, lap, "polars")
                frames.append((driver, lap, frame))
        assert cache.set_telemetry_frames_batch(2026, GP, "Race", frames, lib="polars") == 10
        refs = [(d, lap) for d, lap, _ in frames]
        hits = cache.get_telemetry_frames_batch(2026, GP, "Race", refs, lib="polars")
        assert set(hits) == set(refs)
        for driver, lap, frame in frames:
            assert isinstance(hits[(driver, lap)], pl.DataFrame)
            assert hits[(driver, lap)].equals(frame)
            assert hits[(driver, lap)].schema == frame.schema
        partial = cache.get_telemetry_frames_batch(2026, GP, "Race", [("HAM", 2)], lib="polars")
        assert set(partial) == {("HAM", 2)}
    finally:
        cache.close()


# -- P5 / P6: session warm paths ------------------------------------------------


def _warm_session(tmp_path, monkeypatch):
    """A Session whose laps and telemetry frames are all in the frame tier."""
    from tif1 import core
    from tif1.models import Laps

    clear_lap_cache()  # the process-global laps LRU would hand back another test's session
    cache = Cache(cache_dir=tmp_path)
    frames = [(d, lap, _frame(driver=d, lap=lap)) for d in ("VER", "LEC") for lap in (1, 2)]
    cache.set_telemetry_frames_batch(2026, GP, "Race", frames)
    laps = pd.DataFrame(
        {
            "Driver": ["VER", "VER", "LEC", "LEC"],
            "LapNumber": pd.array([1, 2, 1, 2], dtype="Int64"),
            "LapTime": pd.to_timedelta([80.0, 81.0, 82.0, 83.0], unit="s"),
        }
    )
    cache.set_laps_frame(2026, GP, "Race", laps)
    cache.conn.commit()
    monkeypatch.setattr(core, "get_cache", lambda: cache)
    import tif1.models as models_module

    monkeypatch.setattr(models_module, "get_cache", lambda: cache)
    session = core.Session(2026, "Monaco Grand Prix", "Race", True, "pandas")
    session._memo.has_session_data = True
    session._resolve_ultra_cold_mode = lambda *_a, **_k: False
    session._resolve_telemetry_ultra_cold_mode = lambda *_a, **_k: False
    return session, cache, frames, Laps


def test_warm_load_never_imports_asyncio(tmp_path, monkeypatch):
    session, cache, frames, _ = _warm_session(tmp_path, monkeypatch)
    try:
        monkeypatch.delitem(sys.modules, "asyncio", raising=False)
        laps = session.laps
        assert len(laps) == 4
        tel = session.fetch_all_laps_telemetry()
        assert set(tel) == {(d, lap) for d, lap, _ in frames}
        assert "asyncio" not in sys.modules
        for d, lap, frame in frames:
            _same(tel[(d, lap)], frame)
    finally:
        cache.close()


def test_single_lap_access_reads_the_frame_tier_and_memoizes_the_driver(tmp_path, monkeypatch):
    from tif1.models import Telemetry

    session, cache, _frames, _ = _warm_session(tmp_path, monkeypatch)
    try:
        laps = session.laps
        lap = laps.pick_driver("LEC").iloc[1]
        tel = lap.telemetry
        assert isinstance(tel, Telemetry)
        assert tel.session is session
        assert tel.driver == "LEC"
        assert session._memo.get("telemetry_df", ("LEC", 1)) is not None  # whole driver memoized
        assert session._memo.get("telemetry_df", ("VER", 1)) is None
        t0 = time.perf_counter()
        again = lap.telemetry
        assert time.perf_counter() - t0 < 0.01
        assert again is session._memo.get("telemetry_df", ("LEC", 2))
    finally:
        cache.close()


def test_driver_telemetry_comes_from_the_merged_driver_row(tmp_path, monkeypatch):
    from tif1.models import Telemetry

    session, cache, frames, _ = _warm_session(tmp_path, monkeypatch)
    try:
        laps = session.laps
        tel = laps.pick_driver("VER").telemetry
        expected = pd.concat([f for d, _, f in frames if d == "VER"], ignore_index=True)
        assert isinstance(tel, Telemetry)
        assert tel.session is session
        assert tel.driver == "VER"
        assert tel.equals(expected)
        assert tel.dtypes.equals(expected.dtypes)
        assert laps.pick_driver("VER").pick_lap(2).telemetry.equals(frames[1][2])
    finally:
        cache.close()
