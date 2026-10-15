"""Contract tests for the Pergamon series (warm-path cache usage).

P1  fast frame-decode primitives (shared placements/indexes, direct block classes)
P2  GIL-released parallel lz4 decompression through pyarrow
P3  per-driver typed-columnar blobs with view slicing + legacy upgrade
P4  batched polars IPC read (one conversion for the session)
P5  asyncio-free warm path
P6  single-lap access served from the frame tier (whole driver memoized)
P10 merged driver telemetry straight from the driver blob
Q4  Laps/Lap results wrap the operation's fresh manager (no subclass __init__ copy)
Q6  polars tier: string_view rows, streamed cursor, no rechunk, legacy rows readable
Q13 driver-blob encode into one preallocated buffer + pooled lz4 compress
Q15 parallel payload-tier decode + synchronous payload-tier warm load
Q2  background preload of numpy/pandas/pyarrow in interactive interpreters
"""

from __future__ import annotations

import os
import pickle
import subprocess
import sys
import time
import types

import pandas as pd
import pytest

import tif1.cache as cache_mod
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
    pytest.importorskip("lz4.frame")  # no lz4 wheels on 3.15: the tier writes zstd there
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


# ---------------------------------------------------------------------------
# Q2  background preload of numpy/pandas/pyarrow in interactive interpreters
# ---------------------------------------------------------------------------

_PRELOAD_STATE = (
    "import sys, threading, tif1; th = tif1._preload_thread; th and th.join(30); "
    "print('thread' if th else 'none', 'pandas' in sys.modules, "
    "sorted(m for m in sys.modules if m.startswith('tif1.')))"
)
_IPYTHON_STUB = "import sys, types; sys.modules['IPython'] = types.ModuleType('IPython'); "


def _python(
    code: str, *flags: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess:
    full_env = dict(os.environ)
    full_env.pop("TIF1_PRELOAD_IMPORTS", None)
    full_env.update(env or {})
    return subprocess.run(
        [sys.executable, *flags, "-c", code],
        capture_output=True,
        text=True,
        env=full_env,
        timeout=120,
        stdin=subprocess.DEVNULL,
        check=False,
    )


def test_preload_decision_is_interactive_only_unless_overridden(monkeypatch):
    import tif1

    monkeypatch.delenv(tif1._PRELOAD_ENV, raising=False)
    monkeypatch.delitem(sys.modules, "IPython", raising=False)
    monkeypatch.delattr(sys, "ps1", raising=False)
    assert tif1._preload_wanted() is False
    monkeypatch.setitem(sys.modules, "IPython", types.ModuleType("IPython"))
    assert tif1._preload_wanted() is True
    monkeypatch.setenv(tif1._PRELOAD_ENV, "0")
    assert tif1._preload_wanted() is False
    monkeypatch.delitem(sys.modules, "IPython")
    monkeypatch.setenv(tif1._PRELOAD_ENV, "yes")
    assert tif1._preload_wanted() is True
    # pandas is already imported in the test process: nothing to warm, no thread.
    assert tif1._start_preload() is None
    assert tif1._PRELOAD_MODULES == ("numpy", "pandas", "pyarrow")
    assert not any(name.startswith("tif1") for name in tif1._PRELOAD_MODULES)


def test_preload_never_starts_in_a_non_interactive_process():
    proc = _python(_PRELOAD_STATE)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "none False ['tif1.exceptions']"


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(((), _IPYTHON_STUB, None), id="ipython-kernel"),
        pytest.param((("-i",), "", None), id="python-i"),
        pytest.param(((), "", "1"), id="env-override"),
    ],
)
def test_preload_warms_only_third_party_modules_in_interactive_interpreters(case):
    flags, prelude, override = case
    env = {"TIF1_PRELOAD_IMPORTS": override} if override else None
    proc = _python(prelude + _PRELOAD_STATE, *flags, env=env)
    assert proc.returncode == 0, proc.stderr
    # pandas was imported by the thread; no tif1 submodule (config, core, ...) was.
    assert proc.stdout.strip() == "thread True ['tif1.exceptions']"


def test_preload_override_off_wins_over_interactive_detection():
    proc = _python(_IPYTHON_STUB + _PRELOAD_STATE, env={"TIF1_PRELOAD_IMPORTS": "0"})
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "none False ['tif1.exceptions']"


@pytest.mark.parametrize(
    "main_thread",
    ["import tif1.core", "import pyarrow; import tif1.core", "import numpy; import tif1.core"],
)
def test_preload_does_not_deadlock_against_an_immediate_core_import(main_thread):
    code = (
        f"import tif1; {main_thread}; tif1._preload_thread.join(60); "
        "assert not tif1._preload_thread.is_alive(); import sys; "
        "print('pandas' in sys.modules, 'tif1.core' in sys.modules)"
    )
    proc = _python(code, env={"TIF1_PRELOAD_IMPORTS": "1"})
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "True True"


def test_preload_thread_exits_cleanly_with_warnings_as_errors():
    for code in ("import tif1", "import tif1, sys; sys.exit(3)"):
        proc = _python(code, "-W", "error", env={"TIF1_PRELOAD_IMPORTS": "1"})
        assert proc.returncode == (3 if "exit" in code else 0), proc.stderr
        assert proc.stdout == ""
        assert proc.stderr == ""


# -- Q11: frame-tier read path (shared pool, per-row tasks, streamed cursor) ------


def test_decompression_pool_is_shared_and_forgotten_after_fork():
    from tif1 import cache as cache_module

    pool = cache_module._decompress_pool()
    assert cache_module._decompress_pool() is pool
    assert pool._thread_name_prefix == cache_module._DECOMPRESS_THREAD_PREFIX
    cache_module._reset_decompress_pool()
    assert cache_module._DECOMPRESS_POOL is None
    assert cache_module._decompress_pool() is not pool


def test_big_rows_get_one_task_each_and_small_rows_stay_chunked():
    big = _compress_frame_blob(os.urandom(512 * 1024))  # incompressible, 512 KB per row
    big_rows = [("D", i, big) for i in range(12)]
    chunks = list(_decompress_frame_rows(big_rows, workers=4, min_parallel=2))
    assert sorted(len(chunk) for chunk in chunks) == [1] * 12
    small = _compress_frame_blob(b"x" * 1024)
    small_rows = [("D", i, small) for i in range(40)]
    chunks = list(_decompress_frame_rows(small_rows, workers=4, min_parallel=2))
    assert sorted(len(chunk) for chunk in chunks) == [8] * 5


def test_streamed_rows_match_serial_and_run_inline_from_a_pool_worker():
    from tif1 import cache as cache_module

    payloads = [bytes([i]) * (2000 + i) for i in range(40)]
    rows = [("D", i, _compress_frame_blob(p)) for i, p in enumerate(payloads)]
    rows.insert(5, ("D", 999, b"\x04\x22\x4d\x18garbage-not-a-frame"))  # lz4 magic, corrupt
    pending = cache_module._submit_frame_rows(iter(rows))
    assert len(pending) == 41
    assert hasattr(pending[0], "result")
    streamed = {(d, lap): bytes(raw) for d, lap, raw in cache_module._completed_frame_rows(pending)}
    assert streamed == {("D", i): p for i, p in enumerate(payloads)}
    assert ("D", 999) not in streamed
    # Submitted from one of the pool's own workers the rows decompress inline
    # (a worker waiting on the pool could deadlock the pool).
    inline = cache_module._decompress_pool().submit(cache_module._submit_frame_rows, rows).result()
    assert len(inline) == 40
    assert not hasattr(inline[0], "result")
    assert {(d, lap): bytes(raw) for d, lap, raw in cache_module._completed_frame_rows(inline)} == (
        streamed
    )
    assert list(cache_module._completed_frame_rows([])) == []


def test_driver_rows_streamed_through_the_pool_read_the_same_frames(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = {
            driver: {lap: _frame(driver=driver, lap=lap, n=3 + lap) for lap in (1, 2, 5)}
            for driver in ("VER", "HAM", "LEC", "NOR")
        }
        batch = [(d, lap, f) for d, laps in frames.items() for lap, f in laps.items()]
        assert cache.set_telemetry_frames_batch(2026, GP, "Race", batch) == 12
        refs = [(d, lap) for d, laps in frames.items() for lap in laps]
        hits = cache.get_telemetry_frames_batch(2026, GP, "Race", refs)
        assert set(hits) == set(refs)
        for driver, lap in refs:
            _same(hits[(driver, lap)], frames[driver][lap])
        # Partial batches still skip rows holding none of the wanted laps and
        # a corrupt row is a miss for its driver only.
        cache.conn.execute(
            "UPDATE telemetry_frames_drv SET frame = ? WHERE driver = 'LEC'",
            (_compress_frame_blob(b"TF1\x02garbage"),),
        )
        cache.conn.commit()
        partial = cache.get_telemetry_frames_batch(
            2026, GP, "Race", [("VER", 5), ("LEC", 1), ("NOR", 9), ("ALO", 1)]
        )
        assert set(partial) == {("VER", 5)}
        _same(partial[("VER", 5)], frames["VER"][5])
    finally:
        cache.close()


# -- Q1: lazy lap-frame materialization -------------------------------------------


def test_driver_frame_source_builds_the_same_frames_on_demand():
    frames = [
        (lap, _frame(lap=lap, n=3 + lap, ahead=[None] * (3 + lap) if lap == 2 else None))
        for lap in (1, 2, 3)
    ]
    blob, unsupported = frame_codec.encode_driver_frames(frames)
    assert not unsupported
    eager = frame_codec.decode_driver_frames(blob, None, None)
    source = frame_codec.DriverFrameSource(blob)
    assert sorted(source.laps) == [1, 2, 3]
    assert 2 in source
    assert 9 not in source
    for lap, frame in frames:
        _same(source.frame(lap), frame)
        _same(source.frame(lap), eager[lap])
    assert set(source.frames()) == {1, 2, 3}
    assert set(source.frames({3})) == {3}
    with pytest.raises(KeyError):
        source.frame(9)
    with pytest.raises(ValueError, match="corrupt driver blob"):
        frame_codec.DriverFrameSource(frame_codec.DRIVER_CODEC_MAGIC + b"\x05\x00\x00\x00junk")


def test_session_memo_materializes_pending_frames_on_access():
    from tif1.cache import SessionMemo

    frames = [(lap, _frame(lap=lap)) for lap in (1, 2)]
    blob, _ = frame_codec.encode_driver_frames(frames)
    source = frame_codec.DriverFrameSource(blob)
    memo = SessionMemo()
    adopted: list[tuple[str, int]] = []
    memo.frame_adopter = lambda frame, driver: adopted.append((driver, int(frame["LapNumber"][0])))
    memo.set("telemetry_df", ("VER", 2), "already built")
    assert memo.add_telemetry_source("VER", source) == [("VER", 1)]
    assert memo.contains("telemetry_df", ("VER", 1))
    assert memo.kind_size("telemetry_df") == 2
    assert memo._telemetry_dfs == {("VER", 2): "already built"}  # nothing built yet
    built = memo.get("telemetry_df", ("VER", 1))
    _same(built, frames[0][1])
    assert adopted == [("VER", 1)]
    assert memo.get("telemetry_df", ("VER", 1)) is built  # memoized, built once
    assert not memo._telemetry_sources
    assert memo.get("telemetry_df", ("VER", 3)) is None
    # a pending frame is replaced by an explicit set, and items() forces the rest
    memo.add_telemetry_source("LEC", source)
    memo.set("telemetry_df", ("LEC", 1), "explicit")
    assert ("LEC", 1) not in memo._telemetry_sources
    assert {k: type(v).__name__ for k, v in memo.items("telemetry_df")} == {
        ("VER", 1): "DataFrame",
        ("VER", 2): "str",
        ("LEC", 1): "str",
        ("LEC", 2): "DataFrame",
    }
    memo.add_telemetry_source("HAM", source)
    memo.clear()
    assert memo.kind_size("telemetry_df") == 0


def test_warm_load_defers_frame_construction_until_access(tmp_path, monkeypatch):
    from tif1.models import Telemetry

    session, cache, frames, _ = _warm_session(tmp_path, monkeypatch)
    try:
        session.load(weather=False, messages=False)
        memo = session._memo
        assert set(memo._telemetry_sources) == {(d, lap) for d, lap, _ in frames}
        assert not memo._telemetry_dfs  # no frame built by load()
        lap = session.laps.pick_driver("LEC").iloc[1]
        tel = lap.telemetry
        assert isinstance(tel, Telemetry)
        assert tel.session is session
        assert tel.driver == "LEC"
        _same(tel, frames[3][2])
        assert set(memo._telemetry_dfs) == {("LEC", 2)}
        assert session.laps.pick_driver("LEC").telemetry.equals(
            pd.concat([f for d, _, f in frames if d == "LEC"], ignore_index=True)
        )
        # the public batch contract: real frames for every lap, nothing left pending
        all_tel = session.fetch_all_laps_telemetry()
        assert set(all_tel) == {(d, lap) for d, lap, _ in frames}
        assert not memo._telemetry_sources
        for d, lap, frame in frames:
            assert isinstance(all_tel[(d, lap)], Telemetry)
            assert all_tel[(d, lap)].session is session
            _same(all_tel[(d, lap)], frame)
        assert all_tel[("LEC", 2)] is tel
    finally:
        cache.close()


def test_warm_load_falls_back_to_the_batch_path_for_laps_outside_the_tier(tmp_path, monkeypatch):
    session, cache, frames, _ = _warm_session(tmp_path, monkeypatch)
    try:
        laps = pd.DataFrame(
            {
                "Driver": ["VER", "VER", "LEC", "LEC", "HAM"],
                "LapNumber": pd.array([1, 2, 1, 2, 1], dtype="Int64"),
                "LapTime": pd.to_timedelta([80.0, 81.0, 82.0, 83.0, 84.0], unit="s"),
            }
        )
        cache.set_laps_frame(2026, GP, "Race", laps)
        cache.conn.commit()
        clear_lap_cache()
        calls: list[str] = []
        monkeypatch.setattr(
            session, "fetch_all_laps_telemetry", lambda **_k: calls.append("batch") or {}
        )
        session.load(weather=False, messages=False)
        assert calls == ["batch"]  # HAM lap 1 is not in the tier
        assert set(session._memo._telemetry_sources) == {(d, lap) for d, lap, _ in frames}
    finally:
        cache.close()


def test_first_lap_touch_holds_the_driver_row_lazily(tmp_path, monkeypatch):
    session, cache, frames, _ = _warm_session(tmp_path, monkeypatch)
    try:
        tel = session.laps.pick_driver("VER").iloc[0].telemetry
        _same(tel, frames[0][2])
        memo = session._memo
        assert set(memo._telemetry_dfs) == {("VER", 1)}
        assert set(memo._telemetry_sources) == {("VER", 2)}  # held, not built
        assert memo.get("telemetry_df", ("LEC", 1)) is None
        second = session.laps.pick_driver("VER").iloc[1].telemetry
        assert second.session is session
        assert second.driver == "VER"
        _same(second, frames[1][2])
        assert not memo._telemetry_sources
    finally:
        cache.close()


# -- Q4: Laps/Lap results wrap pandas' fresh manager directly --------------------


def _q4_laps():
    from tif1.models import Laps

    frame = pd.DataFrame(
        {
            "Driver": pd.Categorical(["VER", "NOR", "VER", "LEC"]),
            "DriverNumber": ["1", "4", "1", "16"],
            "LapNumber": [1.0, 1.0, 2.0, 1.0],
            "LapTime": pd.to_timedelta([80.0, 81.0, None, 79.5], unit="s"),
            "Compound": pd.Categorical(["SOFT", "HARD", "SOFT", "SOFT"]),
            "Team": pd.array(["Red Bull", "McLaren", "Red Bull", "Ferrari"], dtype="str"),
        }
    )
    session = object()
    return Laps(frame, session=session), frame, session


def test_laps_selections_skip_the_subclass_init_and_keep_the_contract(monkeypatch):
    from tif1.models import Lap, Laps

    laps, frame, session = _q4_laps()
    calls = {"laps": 0, "lap": 0}
    laps_init, lap_init = Laps.__init__, Lap.__init__

    def counting_laps_init(self, *args, **kwargs):
        calls["laps"] += 1
        laps_init(self, *args, **kwargs)

    def counting_lap_init(self, *args, **kwargs):
        # pandas' own ``self._constructor(ndarray, index=...)`` (isin/isna)
        # builds from a fresh array; only re-wrapping a Series copies a manager.
        if args and isinstance(args[0], pd.Series):
            calls["lap"] += 1
        lap_init(self, *args, **kwargs)

    monkeypatch.setattr(Laps, "__init__", counting_laps_init)
    monkeypatch.setattr(Lap, "__init__", counting_lap_init)

    picked = laps.pick_driver("VER")
    column = laps["LapTime"]
    mask = column.notna()
    row = laps.iloc[3]
    fastest = laps.pick_fastest()
    assert calls == {"laps": 0, "lap": 0}  # no shallow manager copy per selection

    for obj, cls, name in (
        (picked, Laps, None),
        (column, Lap, "LapTime"),
        (mask, Lap, "LapTime"),
        (row, Lap, 3),
        (fastest, Lap, 3),
    ):
        assert type(obj) is cls
        assert obj.session is session
        if name is not None:
            assert obj.name == name
    assert picked.equals(frame[frame["Driver"] == "VER"])
    assert list(picked.dtypes) == list(frame.dtypes)
    assert row.equals(frame.iloc[3])

    # a selection owns its manager: writes never reach the source frame
    assert picked._mgr is not laps._mgr
    picked["LapNumber"] = -1.0
    column.iloc[0] = pd.NaT
    assert laps["LapNumber"].min() == 1.0
    assert laps["LapTime"].iloc[0] == pd.Timedelta(seconds=80)


def test_laps_constructor_hooks_reset_session_without_finalize():
    from tif1.models import Lap, Laps

    laps, _, _ = _q4_laps()
    mgr = laps._mgr.copy(deep=False)
    bare = laps._constructor_from_mgr(mgr, axes=mgr.axes)
    assert type(bare) is Laps
    assert bare.session is None
    col_mgr = laps._mgr.iget(0)
    ser = laps._constructor_sliced_from_mgr(col_mgr, axes=col_mgr.axes)
    same = ser._constructor_from_mgr(ser._mgr, axes=ser._mgr.axes)
    for obj in (ser, same):
        assert type(obj) is Lap
        assert obj.session is None
        assert obj.name is None


# -- Q13: cheaper, parallel driver-blob write -------------------------------------


def _q13_frames() -> dict[int, pd.DataFrame]:
    """Laps covering every encoder branch: groups, empty and strided frames, varying Int64."""
    varying = _frame(lap=3, n=5, ahead=["1"] * 5)
    varying["nGear"] = pd.array([1, 2, None, 4, 5], dtype="Int64")
    renamed = _frame(lap=5, n=6)
    renamed["DRS"] = pd.array([8] * 6, dtype="Int64")
    renamed["Driver"] = "HAM"  # str dtype: a second layout group
    strided = _frame(lap=6, n=8).iloc[::2].reset_index(drop=True)
    return {
        1: _frame(lap=1, n=4, ahead=["44", "44", None, "16"]),
        2: _frame(lap=2, n=3, ahead=[None, None, None]),
        3: varying,
        4: _frame(lap=4).iloc[:0],
        5: renamed,
        6: strided,
    }


def test_q13_encoder_round_trips_every_branch_into_one_writable_buffer():
    frames = _q13_frames()
    assert any(
        not b.values.flags["C_CONTIGUOUS"]
        for b in frames[6]._mgr.blocks
        if hasattr(b.values, "flags")
    )
    blob, leftovers = frame_codec.encode_driver_frames(list(frames.items()))
    assert leftovers == []
    assert isinstance(blob, memoryview)
    assert not blob.readonly
    assert frame_codec.is_driver_blob(blob)
    decoded = frame_codec.decode_driver_frames(bytearray(blob))
    assert set(decoded) == set(frames)
    for lap, frame in frames.items():
        _same(decoded[lap], frame)
    # Compressing the view directly keeps the lz4 content size pyarrow needs.
    compressed = _compress_frame_blob(blob)
    if cache_mod._lz4_frame is not None:
        assert _lz4_frame_content_size(compressed) == len(blob)


def test_q13_encoder_body_layout_is_the_concatenation_of_the_group_blocks():
    import numpy as np

    frames = [(1, _frame(lap=1, n=4)), (2, _frame(lap=2, n=3)), (3, _frame(lap=3, n=2))]
    frames[1][1]["nGear"] = pd.array([3, None, 5], dtype="Int64")
    blob, _ = frame_codec.encode_driver_frames(frames)
    assert blob is not None
    header, body_start = frame_codec._driver_header(memoryview(blob))
    assert len(header) == 1
    body = bytes(blob[body_start:])
    specs = {tuple(s[3] if s[0] == "np" else s[1]): s for s in header[0][3]}
    float_spec = specs[(1, 2, 4)]
    expected = np.concatenate(
        [f[["RPM", "Speed", "Throttle"]].to_numpy().T for _, f in frames], axis=1
    )
    assert body[float_spec[4] : float_spec[4] + float_spec[5]] == expected.tobytes()
    gear = specs[(3,)]
    assert gear[0] == "Int64"
    data = np.concatenate([f["nGear"].array._data for _, f in frames])
    mask = np.concatenate([f["nGear"].array._mask for _, f in frames])
    assert body[gear[2] : gear[3]] == data.tobytes()
    assert body[gear[3] : gear[3] + mask.nbytes] == mask.tobytes()
    assert specs[(9,)] == ("Int64c", [9], [1, 2, 3])  # LapNumber: one constant per frame
    assert specs[(8,)][2] == [("const", "VER")]


def test_q13_column_memo_never_mixes_indexes_or_hides_duplicates():
    frame = _frame(lap=1)
    dup = _frame(lap=2)
    dup.columns = [*dup.columns[:-1], "Driver"]  # duplicate label
    blob, leftovers = frame_codec.encode_driver_frames([(1, frame), (2, dup)])
    assert blob is not None
    assert [lap for lap, _ in leftovers] == [2]
    assert frame_codec.driver_blob_laps(blob) == [1]
    seen: dict = {}
    first = frame_codec._frame_parts(frame, seen)
    again = frame_codec._frame_parts(_frame(lap=3), seen)
    assert first is not None
    assert again is not None
    assert first[0] == again[0] == [str(c) for c in frame.columns]


def test_q13_multi_driver_write_compresses_on_a_pool(tmp_path, monkeypatch):
    import tif1.cache as cache_mod

    pools: list = []
    real = cache_mod._frame_compress_pool

    def spy(n: int):
        pool = real(n)
        pools.append((n, pool))
        return pool

    monkeypatch.setattr(cache_mod, "_frame_compress_pool", spy)
    cache = Cache(cache_dir=tmp_path)
    try:
        batch = [
            (d, lap, _frame(driver=d, lap=lap)) for d in ("VER", "HAM", "LEC") for lap in (1, 2)
        ]
        assert cache.set_telemetry_frames_batch(2026, GP, "Race", batch) == 6
        assert cache.set_telemetry_frames_batch(2026, GP, "Race", [("VER", 3, _frame(lap=3))]) == 1
        assert [n for n, _ in pools] == [3, 1]
        # Only lz4 releases the GIL, so the zstd fallback (3.15) compresses serially.
        assert (pools[0][1] is not None) == (cache_mod._lz4_frame is not None)
        assert pools[1][1] is None
        refs = [(d, lap) for d, lap, _ in batch] + [("VER", 3)]
        hits = cache.get_telemetry_frames_batch(2026, GP, "Race", refs)
        assert set(hits) == set(refs)
        for d, lap, frame in batch:
            _same(hits[(d, lap)], frame)
        _same(hits[("VER", 3)], _frame(lap=3))
    finally:
        cache.close()


# -- Q6: polars tier read (string_view rows, streamed cursor, no rechunk) --------


def _large_string_ipc(blob: bytes) -> bytes:
    """Rewrite an IPC row the way releases before Q6 stored it (large_string)."""
    import pyarrow as pa
    import pyarrow.ipc as pa_ipc

    table = pa_ipc.open_file(pa.BufferReader(blob)).read_all()
    fields = [
        f.with_type(pa.large_string()) if pa.types.is_string_view(f.type) else f
        for f in table.schema
    ]
    sink = pa.BufferOutputStream()
    options = pa_ipc.IpcWriteOptions(compression="lz4")
    with pa_ipc.new_file(sink, pa.schema(fields), options=options) as writer:
        writer.write_table(table.cast(pa.schema(fields)))
    return sink.getvalue().to_pybytes()


def test_polars_tier_rows_store_string_view_and_read_back_single_chunk(tmp_path):
    pl = pytest.importorskip("polars")
    import pyarrow as pa
    import pyarrow.ipc as pa_ipc

    from tif1.cache import _POLARS_IPC_TABLE

    cache = Cache(cache_dir=tmp_path)
    try:
        frames = [
            (driver, lap, _create_telemetry_df(_payload(3 + lap), driver, lap, "polars"))
            for driver in ("VER", "LEC", "HAM", "SAI", "NOR")
            for lap in (1, 2, 3)
        ]
        assert cache.set_telemetry_frames_batch(2026, GP, "Race", frames, lib="polars") == 15
        blobs = dict(
            cache.conn.execute(f"SELECT driver, frame FROM {_POLARS_IPC_TABLE}").fetchall()
        )
        schema = pa_ipc.open_file(pa.BufferReader(blobs["VER"])).schema
        assert pa.types.is_string_view(schema.field("Driver").type)

        # Legacy large_string rows for two drivers: two schema groups in one read.
        for driver in ("HAM", "NOR"):
            cache.conn.execute(
                f"UPDATE {_POLARS_IPC_TABLE} SET frame = ? WHERE driver = ?",
                (_large_string_ipc(blobs[driver]), driver),
            )
        refs = [(d, lap) for d, lap, _ in frames]
        for wanted in (refs, [("HAM", 2), ("VER", 3)]):  # pooled, then serial path
            hits = cache.get_telemetry_frames_batch(2026, GP, "Race", wanted, lib="polars")
            assert set(hits) == set(wanted)
            for driver, lap, frame in frames:
                if (driver, lap) not in wanted:
                    continue
                hit = hits[(driver, lap)]
                assert isinstance(hit, pl.DataFrame)
                assert hit.schema == frame.schema
                assert hit.equals(frame)
                assert hit.n_chunks() == 1
    finally:
        cache.close()


def test_polars_tier_corrupt_row_is_a_miss_on_the_streamed_path(tmp_path):
    pytest.importorskip("polars")
    from tif1.cache import _POLARS_IPC_TABLE

    cache = Cache(cache_dir=tmp_path)
    try:
        frames = [
            (driver, 1, _create_telemetry_df(_payload(4), driver, 1, "polars"))
            for driver in ("VER", "LEC", "HAM", "SAI")
        ]
        cache.set_telemetry_frames_batch(2026, GP, "Race", frames, lib="polars")
        cache.conn.execute(
            f"UPDATE {_POLARS_IPC_TABLE} SET frame = ? WHERE driver = 'LEC'", (b"garbage",)
        )
        refs = [(d, 1) for d, _, _ in frames]
        hits = cache.get_telemetry_frames_batch(2026, GP, "Race", refs, lib="polars")
        assert set(hits) == {("VER", 1), ("HAM", 1), ("SAI", 1)}
    finally:
        cache.close()


# -- Q16: sync APIs inside a running event loop (Jupyter) ------------------------


def test_cold_bulk_telemetry_declares_nested_loop_support_before_asyncio_run(tmp_path, monkeypatch):
    """Laps from the frame tier never touch asyncio, so the telemetry wrapper must
    itself apply nested-loop support before ``asyncio.run`` (else a Jupyter cell
    raises "asyncio.run() cannot be called from a running event loop")."""
    import asyncio

    from tif1 import core

    session, cache, _frames, _ = _warm_session(tmp_path, monkeypatch)
    try:
        laps = session.laps
        extra = laps.iloc[[0]].copy()
        extra["LapNumber"] = pd.array([3], dtype="Int64")  # not in the frame tier
        session._laps = core.Laps(pd.concat([laps, extra], ignore_index=True))
        events: list[str] = []
        monkeypatch.setattr(core, "_ensure_nested_loop_support", events.append)

        def fake_run(coro):
            coro.close()
            events.append("asyncio.run")
            return {}

        monkeypatch.setattr(asyncio, "run", fake_run)
        session.fetch_all_laps_telemetry()
        assert events == ["fetch_all_laps_telemetry", "asyncio.run"]
    finally:
        cache.close()


def test_sync_payload_loader_honours_everywhere_missing_verdicts(tmp_path, monkeypatch):
    """A single-ref sync fetch (``Laps.telemetry`` of a driver with one missing lap)
    must not walk the CDNs again for a payload the negative tier knows is missing."""
    import tif1.cache as cache_module
    from tif1.exceptions import DataNotFoundError
    from tif1.payload_loader import PayloadLoader

    cache = Cache(cache_dir=tmp_path)
    monkeypatch.setattr(cache_module, "get_cache", lambda: cache)
    fetched: list[str] = []

    def fetch(path, *, fast):
        fetched.append(path)
        return {"tel": {"Time": [0.0]}}

    loader = PayloadLoader(2026, GP, "Race", fetch=fetch, cache_get=lambda _k: None)
    try:
        cache.record_missing(loader.cache_key("LEC/66_tel.json"))
        with pytest.raises(DataNotFoundError):
            loader.get("LEC/66_tel.json", validate=False)
        assert fetched == []
        loader.get("LEC/65_tel.json", validate=False)  # unknown refs still fetch
        loader.get("LEC/66_tel.json", validate=False, use_cache=False)  # explicit bypass
        assert fetched == ["LEC/65_tel.json", "LEC/66_tel.json"]
    finally:
        cache.close()


# -- Q15: parallel payload-tier decode + synchronous payload-tier warm load ------


def _payload_tier_cache(tmp_path, n_refs: int = 80, rows: int = 300):
    """A cache with payload-tier rows only (zstd rows for payloads >= 4 KiB)."""
    cache = Cache(cache_dir=tmp_path)
    refs = [(d, lap) for d in ("VER", "LEC") for lap in range(1, n_refs // 2 + 1)]
    for i, (d, lap) in enumerate(refs):
        ahead = (["44", None] * rows)[:rows] if i % 3 else ["16"] * rows
        cache.set_telemetry(2026, GP, "Race", d, lap, _payload(rows, ahead, start=float(lap)))
    cache.conn.commit()
    cache._memory_telemetry_cache.clear()
    cache._parsed_telemetry_cache.clear()
    return cache, refs


def _payload_rows(cache: Cache) -> list[tuple]:
    return cache.conn.execute("SELECT driver, lap, data FROM telemetry_cache").fetchall()


@pytest.mark.parametrize("native", [True, False])
def test_parallel_payload_decode_matches_serial(tmp_path, monkeypatch, native):
    cache, refs = _payload_tier_cache(tmp_path)
    try:
        if not native:
            monkeypatch.setattr(cache_mod, "NATIVE_TELEMETRY_DECODER_AVAILABLE", False)
        rows = _payload_rows(cache)
        assert any(isinstance(data, bytes) for _, _, data in rows)  # zstd rows exercised
        serial = {
            (d, lap): parsed
            for chunk in cache_mod._decode_payload_rows(rows, workers=1)
            for d, lap, _raw, parsed in chunk
        }
        parallel = {
            (d, lap): (raw, parsed)
            for chunk in cache_mod._decode_payload_rows(rows, workers=4, min_parallel=1)
            for d, lap, raw, parsed in chunk
        }
        assert set(parallel) == set(serial) == set(refs)
        for ref, (raw, parsed) in parallel.items():
            assert isinstance(raw, bytes | str)
            left = _create_telemetry_df(parsed, ref[0], ref[1], "pandas")
            right = _create_telemetry_df(serial[ref], ref[0], ref[1], "pandas")
            assert left is not None
            assert right is not None
            _same(left, right)
    finally:
        cache.close()


def test_parallel_payload_decode_surfaces_corrupt_rows(tmp_path):
    cache, _refs = _payload_tier_cache(tmp_path)
    try:
        rows = _payload_rows(cache)
        rows[5] = (rows[5][0], rows[5][1], cache_mod._ZSTD_MAGIC + b"\xff" * 64)
        with pytest.raises(ValueError, match="Corrupt compressed cache entry"):
            for _chunk in cache_mod._decode_payload_rows(rows, workers=4, min_parallel=1):
                pass
    finally:
        cache.close()


def test_threadsafe_zstd_decompress_from_many_threads():
    from concurrent.futures import ThreadPoolExecutor

    compress, decompress = cache_mod._zstd_compress, cache_mod._zstd_decompress_mt
    assert compress is not None
    assert decompress is not None
    blobs = [(b'{"x": %d}' % i) * 4000 for i in range(64)]
    packed = [compress(blob) for blob in blobs]
    with ThreadPoolExecutor(8) as pool:
        assert list(pool.map(decompress, packed * 4)) == blobs * 4


def test_batch_read_uses_the_parallel_decoder_and_fills_memory_tiers(tmp_path, monkeypatch):
    cache, refs = _payload_tier_cache(tmp_path)
    calls = []
    real = cache_mod._decode_payload_rows

    def spy(rows, *args, **kwargs):
        calls.append(len(rows))
        yield from real(rows, *args, min_parallel=1, **kwargs)

    monkeypatch.setattr(cache_mod, "_decode_payload_rows", spy)
    try:
        got = cache.get_telemetry_batch(2026, GP, "Race", [*refs, ("HAM", 1)])
        assert calls == [len(refs)]
        assert set(got) == set(refs)  # the uncached ref is simply absent
        key = (2026, GP, "Race", *refs[0])
        assert cache._parsed_telemetry_cache.get(key) is got[refs[0]]
        assert cache._memory_telemetry_cache.get(key) is not None
    finally:
        cache.close()


def test_payload_tier_warm_load_is_synchronous_and_materializes_frames(tmp_path, monkeypatch):
    """Frame tiers empty: the payload tier serves fetch_all without asyncio."""
    from tif1 import core
    from tif1.core_utils.json_utils import decode_telemetry_payload

    clear_lap_cache()
    cache, refs = _payload_tier_cache(tmp_path, n_refs=8)
    laps = pd.DataFrame(
        {
            "Driver": [d for d, _ in refs],
            "LapNumber": pd.array([lap for _, lap in refs], dtype="Int64"),
            "LapTime": pd.to_timedelta([80.0 + i for i in range(len(refs))], unit="s"),
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
    try:
        monkeypatch.delitem(sys.modules, "asyncio", raising=False)
        assert len(session.laps) == len(refs)
        tel = session.fetch_all_laps_telemetry()
        assert "asyncio" not in sys.modules
        assert list(tel) == refs
        stored = {(d, lap): data for d, lap, data in _payload_rows(cache)}
        for d, lap in refs:
            parsed = decode_telemetry_payload(cache_mod._decode_sqlite_value(stored[(d, lap)]))
            expected = _create_telemetry_df(parsed, d, lap, "pandas")
            assert expected is not None
            _same(tel[(d, lap)], expected)
            assert session._memo.get("telemetry_df", (d, lap)) is tel[(d, lap)]
        _join_frame_writers()
        assert set(cache.get_telemetry_frames_batch(2026, GP, "Race", refs)) == set(refs)
    finally:
        cache.close()


# -- Q18: per-lap frames over the group arrays -----------------------------------


def _q18_frames() -> dict[int, pd.DataFrame]:
    frames = {
        1: _frame(lap=1, n=4, ahead=["44", "44", None, "16"]),
        2: _frame(lap=2, n=3, ahead=[None, None, None]),
        3: _frame(lap=3, n=5, ahead=["1", "1", "44", None, "1"]),
        5: _frame(lap=5, n=1, ahead=["16"]),
        7: _frame(lap=7, n=2, ahead=[None, None]),
    }
    # A masked, non-constant Int64 channel next to the per-lap constants.
    frames[1]["nGear"] = pd.array([1, 2, None, 4], dtype="Int64")
    frames[3]["nGear"] = pd.array([5, None, 6, 7, 8], dtype="Int64")
    frames[5]["nGear"] = pd.array([3], dtype="Int64")
    return frames


def _block_signature(frame: pd.DataFrame) -> list[tuple]:
    return [
        (
            type(block).__name__,
            tuple(block.mgr_locs.as_array.tolist()),
            type(block.values).__name__,
            str(block.values.dtype),
            sorted(vars(block.values)) if hasattr(block.values, "__dict__") else None,
        )
        for block in frame._mgr.blocks
    ]


def _decode_both(blob: bytes, monkeypatch, wanted=None, frame_class=None):
    fast = frame_codec.decode_driver_frames(bytearray(blob), wanted, frame_class)
    with monkeypatch.context() as patch:
        patch.setattr(frame_codec, "_FAST_LAP_FRAMES", False)
        generic = frame_codec.decode_driver_frames(bytearray(blob), wanted, frame_class)
    return fast, generic


@pytest.mark.parametrize("use_telemetry_class", [False, True])
def test_fast_lap_frames_match_the_generic_loop(monkeypatch, use_telemetry_class):
    from tif1.models import Telemetry

    cls = Telemetry if use_telemetry_class else None
    frames = _q18_frames()
    blob, leftovers = frame_codec.encode_driver_frames(list(frames.items()))
    assert blob is not None
    assert leftovers == []
    assert frame_codec._FAST_LAP_FRAMES
    for wanted in (None, {3, 7}):
        fast, generic = _decode_both(blob, monkeypatch, wanted, cls)
        assert set(fast) == set(generic) == (wanted or set(frames))
        for lap, frame in fast.items():
            assert type(frame) is type(generic[lap])
            _same(frame, generic[lap])
            _same(frame, frames[lap])
            assert _block_signature(frame) == _block_signature(generic[lap])
            for name in getattr(type(frame), "_metadata", ()):
                assert getattr(frame, name) is None
            _same(pickle.loads(pickle.dumps(frame)), frames[lap])


def test_fast_lap_arrays_match_the_public_slicing_route():
    assert frame_codec._fast_arrays() == (True, True)
    frames = _q18_frames()
    blob, _ = frame_codec.encode_driver_frames(list(frames.items()))
    decoded = frame_codec.decode_driver_frames(bytearray(blob))
    reference = frames[3]["DriverAhead"].array
    got = decoded[3]["DriverAhead"].array
    assert type(got) is type(reference)
    assert list(vars(got)) == list(vars(reference[0:5]))
    assert got._readonly is False
    assert got.dtype == reference.dtype
    assert got.tolist() == reference.tolist()
    time_array = decoded[3]["Time"].array
    assert type(time_array) is pd.arrays.TimedeltaArray
    assert time_array.freq is None
    assert time_array.equals(frames[3]["Time"].array)


def test_fast_lap_frames_are_writable_and_mutation_isolated():
    frames = _q18_frames()
    blob, _ = frame_codec.encode_driver_frames(list(frames.items()))
    decoded = frame_codec.decode_driver_frames(bytearray(blob))
    # Laps 1, 3 and 5 share one layout group, so their columns slice the same arrays.
    for column in ("LapNumber", "DRS", "nGear", "DriverAhead", "Time", "Speed"):
        before = {lap: decoded[lap][column].copy() for lap in (3, 5)}
        value = -1.0 if column == "Speed" else None
        decoded[1].loc[0, column] = value
        decoded[1].loc[3, column] = value
        for lap, series in before.items():
            assert decoded[lap][column].equals(series), (column, lap)
    lap_numbers = decoded[3]["LapNumber"]
    assert lap_numbers.array._data.flags.writeable
    assert lap_numbers.tolist() == [3] * 5
    assert decoded[1]["LapNumber"].isna().tolist() == [True, False, False, True]


def test_fast_lap_frames_fall_back_when_the_array_probes_fail(monkeypatch):
    frames = _q18_frames()
    blob, _ = frame_codec.encode_driver_frames(list(frames.items()))
    monkeypatch.setattr(frame_codec, "_FAST_ARRAYS", (False, False))
    fast, generic = _decode_both(blob, monkeypatch)
    for lap, frame in fast.items():
        _same(frame, generic[lap])
        assert _block_signature(frame) == _block_signature(generic[lap])
