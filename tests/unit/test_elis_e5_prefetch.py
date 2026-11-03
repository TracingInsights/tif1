"""Elis E5: the second lazy driver row reads the session's other rows ahead (pandas).

A warm ``load()`` holds a lazy stand-in per driver (Delphi D5). The first
driver a script touches reads its row alone; the second reads its own row
alone too and, unless it is the driver after the first in the laps' order
(a loop over the laps, which batches as in D5), starts reading every other
lazy row ahead in the pool (``Cache.prefetch_telemetry_frame_rows``). These
tests pin the trigger (once, never for one driver, not for an in-order
walk), the hand-over of the read rows, the inline read of a row no worker
started, and the lifecycle: a cache closed mid-prefetch leaks no reader and
the interpreter exit never waits for the queued reads.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pandas as pd
import pytest

import tif1
import tif1.cache as cache_mod
from tif1.cache import Cache, clear_lap_cache
from tif1.core_utils.helpers import _create_telemetry_df

GP = "Monaco%20Grand%20Prix"
# The laps' order (and so the order the lazy sources are held in).
DRIVERS = ("PIA", "RUS", "NOR", "LEC", "VER")

pytestmark = pytest.mark.skipif(
    not cache_mod._can_decompress_in_parallel(cache_mod._pa_lz4_codec()),
    reason="the reads ahead need the parallel row path (pyarrow lz4, two workers)",
)


def _payload(n: int, offset: float) -> dict:
    return {
        "Time": [0.1 * i for i in range(n)],
        "RPM": [10000.0 + offset + i for i in range(n)],
        "Speed": [200.0 + offset + i for i in range(n)],
        "nGear": [7] * n,
        "Throttle": [100.0] * n,
        "Brake": [0] * n,
        "DRS": [0] * n,
        "DriverAhead": ["44"] * n,
    }


def _frames() -> dict[tuple[str, int], pd.DataFrame]:
    frames = {}
    for i, driver in enumerate(DRIVERS):
        for lap in (1, 2):
            frame = _create_telemetry_df(_payload(3 + lap, 10.0 * i + lap), driver, lap, "pandas")
            assert frame is not None
            frames[(driver, lap)] = frame
    return frames


def _same(left: pd.DataFrame, right: pd.DataFrame) -> None:
    assert list(left.columns) == list(right.columns)
    assert left.dtypes.equals(right.dtypes)
    assert left.index.equals(right.index)
    assert left.equals(right)


def _seed_cache(tmp_path: Path) -> tuple[Cache, dict[tuple[str, int], pd.DataFrame]]:
    cache = Cache(cache_dir=tmp_path)
    frames = _frames()
    cache.set_telemetry_frames_batch(
        2026, GP, "Race", [(d, lap, frame) for (d, lap), frame in frames.items()]
    )
    cache.conn.commit()
    return cache, frames


def _warm_session(tmp_path, monkeypatch):
    """A Session over five drivers whose laps and telemetry frames are all in the frame tier."""
    from tif1 import core

    clear_lap_cache()
    cache, frames = _seed_cache(tmp_path)
    laps = pd.DataFrame(
        {
            "Driver": [d for d in DRIVERS for _ in (1, 2)],
            "LapNumber": pd.array([1, 2] * len(DRIVERS), dtype="Int64"),
            "LapTime": pd.to_timedelta([80.0 + i for i in range(2 * len(DRIVERS))], unit="s"),
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
    return session, cache, frames


def _count_reads(cache, monkeypatch) -> tuple[list[list[str]], list[list[str]]]:
    """Record the driver lists of every on-demand row read and of every read-ahead start."""
    reads: list[list[str]] = []
    prefetches: list[list[str]] = []
    real_read = cache.stream_telemetry_frame_rows
    real_prefetch = cache.prefetch_telemetry_frame_rows

    def counted_read(year, gp, sess, drivers, **kwargs):
        reads.append(sorted(drivers))
        return real_read(year, gp, sess, drivers, **kwargs)

    def counted_prefetch(year, gp, sess, drivers):
        prefetches.append(list(drivers))
        return real_prefetch(year, gp, sess, drivers)

    monkeypatch.setattr(cache, "stream_telemetry_frame_rows", counted_read)
    monkeypatch.setattr(cache, "prefetch_telemetry_frame_rows", counted_prefetch)
    return reads, prefetches


def _lap_tel(session, driver: str, lap: int):
    return session.laps.pick_driver(driver).iloc[lap - 1].telemetry


def test_lazy_sources_follow_the_laps_order(tmp_path, monkeypatch):
    session, cache, _ = _warm_session(tmp_path, monkeypatch)
    try:
        session.load(weather=False, messages=False)
        assert list(session._memo._driver_sources) == list(DRIVERS)  # SQL order is alphabetical
    finally:
        cache.close()


def test_one_driver_never_starts_the_reads_ahead(tmp_path, monkeypatch):
    session, cache, frames = _warm_session(tmp_path, monkeypatch)
    try:
        session.load(weather=False, messages=False)
        reads, prefetches = _count_reads(cache, monkeypatch)
        for lap in (1, 2):
            _same(_lap_tel(session, "VER", lap), frames[("VER", lap)])
        assert reads == [["VER"]]
        assert prefetches == []
        assert not cache._prefetches
    finally:
        cache.close()


def test_second_driver_reads_the_others_ahead_once(tmp_path, monkeypatch):
    session, cache, frames = _warm_session(tmp_path, monkeypatch)
    try:
        session.load(weather=False, messages=False)
        memo = session._memo
        reads, prefetches = _count_reads(cache, monkeypatch)
        _same(_lap_tel(session, "VER", 1), frames[("VER", 1)])
        _same(_lap_tel(session, "LEC", 2), frames[("LEC", 2)])  # not the driver after VER
        # Both touched rows were read alone; the others are read ahead in laps order.
        assert reads == [["VER"], ["LEC"]]
        assert prefetches == [["PIA", "RUS", "NOR"]]
        held = {driver: memo.driver_source(driver) for driver in ("PIA", "RUS", "NOR")}
        assert all(source._prefetched is not None and source.hydrated for source in held.values())
        assert all(source.decodable for source in held.values())
        for driver in ("NOR", "PIA", "RUS"):
            for lap in (2, 1):
                _same(_lap_tel(session, driver, lap), frames[(driver, lap)])
        assert reads == [["VER"], ["LEC"]]  # every other row came from the reads ahead
        assert prefetches == [["PIA", "RUS", "NOR"]]
        assert all(source._prefetched is None for source in held.values())
        assert not memo._telemetry_sources or all(
            driver in ("VER", "LEC") for driver, _ in memo._telemetry_sources
        )
    finally:
        cache.close()


def test_a_walk_in_laps_order_batches_instead(tmp_path, monkeypatch):
    """A loop over the laps (PIA then RUS) pays the D5 batch while it waits, never a read ahead."""
    session, cache, frames = _warm_session(tmp_path, monkeypatch)
    try:
        session.load(weather=False, messages=False)
        reads, prefetches = _count_reads(cache, monkeypatch)
        built = [lap.telemetry for _, lap in session.laps.iterlaps()]
        for tel, ((driver, lap), frame) in zip(built, frames.items(), strict=True):
            assert (tel.driver, int(tel["LapNumber"].iloc[0])) == (driver, lap)
            _same(tel, frame)
        assert reads == [["PIA"], sorted(DRIVERS[1:])]
        assert prefetches == []
    finally:
        cache.close()


def test_bulk_reader_takes_the_rows_read_ahead(tmp_path, monkeypatch):
    session, cache, frames = _warm_session(tmp_path, monkeypatch)
    try:
        session.load(weather=False, messages=False)
        reads, prefetches = _count_reads(cache, monkeypatch)
        _lap_tel(session, "VER", 1)
        _lap_tel(session, "NOR", 1)
        assert prefetches == [["PIA", "RUS", "LEC"]]
        all_tel = session.fetch_all_laps_telemetry()
        assert reads == [["VER"], ["NOR"]]
        assert set(all_tel) == set(frames)
        for ref, frame in frames.items():
            _same(all_tel[ref], frame)
        assert not session._memo._telemetry_sources
    finally:
        cache.close()


def test_a_row_read_ahead_that_turned_bad_falls_through(tmp_path, monkeypatch):
    session, cache, frames = _warm_session(tmp_path, monkeypatch)
    try:
        session.load(weather=False, messages=False)
        cache.conn.execute(
            f"UPDATE {cache_mod._PANDAS_DRIVER_TABLE} SET frame = ? WHERE driver = ?",
            (b"not a row", "PIA"),
        )
        cache.conn.commit()
        monkeypatch.setattr(session, "_get_telemetry_payload_for_ref", lambda *_a, **_k: None)
        _lap_tel(session, "VER", 1)
        _lap_tel(session, "LEC", 1)
        assert _lap_tel(session, "PIA", 1).empty  # no row, no payload: the ordinary miss
        assert not any(driver == "PIA" for driver, _ in session._memo._telemetry_sources)
        _same(_lap_tel(session, "RUS", 1), frames[("RUS", 1)])
    finally:
        cache.close()


class _StalledReads:
    """Hold the pool's row reads until released (reads on the calling thread pass)."""

    def __init__(self, monkeypatch) -> None:
        self.release = threading.Event()
        self.started = threading.Event()
        real_read = cache_mod._read_frame_row

        def stalled(*args):
            if threading.current_thread().name.startswith(cache_mod._DECOMPRESS_THREAD_PREFIX):
                self.started.set()
                assert self.release.wait(10)
            return real_read(*args)

        monkeypatch.setattr(cache_mod, "_read_frame_row", stalled)
        monkeypatch.setattr(cache_mod, "_PREFETCH_LANES", 1)


def test_a_row_no_worker_started_is_read_by_the_caller(tmp_path, monkeypatch):
    cache, _ = _seed_cache(tmp_path)
    stalled = _StalledReads(monkeypatch)
    try:
        handles = cache.prefetch_telemetry_frame_rows(2026, GP, "Race", list(DRIVERS))
        assert handles is not None
        assert list(handles) == list(DRIVERS)
        assert stalled.started.wait(10)  # the single lane holds PIA's read
        row = handles["VER"].take()  # queued behind it: read here instead
        assert row is not None
        assert row[0] == "VER"
        assert handles["VER"].future.cancelled()
        stalled.release.set()
        for driver in ("PIA", "RUS", "NOR", "LEC"):
            row = handles[driver].take()
            assert row is not None
            assert row[0] == driver
    finally:
        stalled.release.set()
        cache.close()


def test_a_cache_closed_mid_prefetch_leaks_no_reader(tmp_path, monkeypatch):
    cache, _ = _seed_cache(tmp_path)
    stalled = _StalledReads(monkeypatch)
    opened: list[sqlite3.Connection] = []
    real_open = cache._open_reader

    def tracked_open():
        conn = real_open()
        if conn is not None:
            opened.append(conn)
        return conn

    monkeypatch.setattr(cache, "_open_reader", tracked_open)
    try:
        handles = cache.prefetch_telemetry_frame_rows(2026, GP, "Race", list(DRIVERS))
        assert handles is not None
        assert stalled.started.wait(10)
        cache.close()
        queued = [handles[driver].future for driver in DRIVERS[1:]]
        assert all(future.cancelled() for future in queued)
        stalled.release.set()
        first = handles["PIA"].future.result(timeout=10)  # the read in flight finishes
        assert first is None  # the cache closed under it: no reader, no main connection
        assert not cache._idle_readers
        for conn in opened:
            with pytest.raises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
        assert cache.prefetch_telemetry_frame_rows(2026, GP, "Race", ["VER"]) is None
    finally:
        stalled.release.set()
        cache.close()


def test_a_forked_child_never_waits_on_the_parents_reads(tmp_path, monkeypatch):
    """The lanes stay in the parent: a child reads inline and leaves the futures alone."""
    cache, _ = _seed_cache(tmp_path)
    stalled = _StalledReads(monkeypatch)
    try:
        handles = cache.prefetch_telemetry_frame_rows(2026, GP, "Race", list(DRIVERS))
        assert handles is not None
        assert stalled.started.wait(10)  # PIA's read is running, the rest queued
        queued = [handles[driver].future for driver in DRIVERS[1:]]
        # As seen from a forked child (another pid than the one that started them).
        for handle in handles.values():
            handle.pid = -1
        cache._prefetch_pid = -1
        row = handles["PIA"].take()  # would block forever on the running future
        assert row is not None
        assert row[0] == "PIA"
        assert not handles["PIA"].future.done()
        cache._cancel_prefetches()
        assert not cache._prefetches
        assert not any(future.cancelled() for future in queued)  # untouched
        cache._prefetch_pid = os.getpid()
        for future in queued:
            future.cancel()
    finally:
        stalled.release.set()
        cache.close()


def test_exit_hook_cancels_the_queued_reads(tmp_path, monkeypatch):
    cache, _ = _seed_cache(tmp_path)
    stalled = _StalledReads(monkeypatch)
    try:
        handles = cache.prefetch_telemetry_frame_rows(2026, GP, "Race", list(DRIVERS))
        assert handles is not None
        assert stalled.started.wait(10)
        assert cache_mod._PREFETCH_EXIT_HOOK_REGISTERED
        cache_mod._cancel_all_frame_prefetches()
        assert all(handles[driver].future.cancelled() for driver in DRIVERS[1:])
        assert not handles["PIA"].future.cancelled()  # running: it finishes
        stalled.release.set()
        assert handles["PIA"].future.result(timeout=10) is not None
        assert not cache._prefetches
    finally:
        stalled.release.set()
        cache.close()


_EXIT_CHILD = r"""
import atexit, json, sys, time
import tif1.cache as cache_mod
from tests_e5_seed import seed

cache = seed(sys.argv[1])
real_read = cache_mod._read_frame_row

def slow(*args):
    time.sleep(0.4)
    return real_read(*args)

cache_mod._read_frame_row = slow
handles = cache.prefetch_telemetry_frame_rows(2026, sys.argv[2], "Race", sys.argv[3].split(","))
assert handles, "no reads ahead"
time.sleep(0.05)  # the lanes are reading their first rows
end = time.perf_counter()
# atexit handlers run after threading's exit hooks, i.e. after the pool joined.
atexit.register(lambda: print(json.dumps({"join_s": time.perf_counter() - end})))
"""

_EXIT_SEED = """
from pathlib import Path

from tif1.cache import Cache
from tif1.core_utils.helpers import _create_telemetry_df


def seed(path):
    cache = Cache(cache_dir=Path(path))
    rows = []
    for i in range(8):
        payload = {"Time": [0.0, 0.1], "RPM": [1.0, 2.0], "Speed": [float(i), 1.0],
                   "nGear": [7, 7], "Throttle": [1.0, 1.0], "Brake": [0, 0], "DRS": [0, 0],
                   "DriverAhead": ["44", "44"]}
        rows.append((f"D{i}", 1, _create_telemetry_df(payload, f"D{i}", 1, "pandas")))
    cache.set_telemetry_frames_batch(2026, "Monaco%20Grand%20Prix", "Race", rows)
    cache.conn.commit()
    return cache
"""


def test_interpreter_exit_never_waits_for_the_queued_reads(tmp_path):
    """Eight 0.4 s reads on two lanes would hold the exit 1.6 s; only the two in flight finish."""
    (tmp_path / "tests_e5_seed.py").write_text(_EXIT_SEED, encoding="utf-8")
    package_root = str(Path(tif1.__file__).resolve().parent.parent)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [package_root, str(tmp_path), *filter(None, [env.get("PYTHONPATH")])]
    )
    env["HOME"] = str(tmp_path)
    env["USERPROFILE"] = str(tmp_path)
    env["TIF1_CACHE_DIR"] = str(tmp_path / "cache")
    drivers = ",".join(f"D{i}" for i in range(8))
    started = time.perf_counter()
    completed = subprocess.run(
        [sys.executable, "-c", _EXIT_CHILD, str(tmp_path / "cache"), GP, drivers],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        cwd=tmp_path,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout.strip().splitlines()[-1])
    assert report["join_s"] < 1.0, (report, time.perf_counter() - started)
