"""Delphi D9: driver-row reads of the pandas frame tier run in the workers.

The main connection answers one index query (driver, rowid, length); each
row is then read on a reader connection by a decompression worker
(``Connection.blobopen``, GIL released, inside one read transaction that
re-checks the key) and decompressed there. A row the reader's snapshot
does not hold as the main connection described it (written in this
process and not committed, or replaced in between) is read on the main
connection under the cache lock; readers close with the cache; the serial
path (no pool) still reads through the cursor.
"""

from __future__ import annotations

import sqlite3
import threading

import pandas as pd
import pytest

import tif1.cache as cache_mod
from tif1.cache import _PANDAS_DRIVER_TABLE, Cache

GP = "Bahrain%20Grand%20Prix"
KEY = (2026, GP, "Race")
DRIVERS = ("VER", "HAM", "LEC", "NOR", "PIA")


def _payload(n: int) -> dict:
    return {
        "Time": [0.1 * i for i in range(n)],
        "RPM": [10000.0 + i for i in range(n)],
        "Speed": [200.0 + i for i in range(n)],
        "nGear": [7] * n,
        "Throttle": [100.0] * n,
        "Brake": [0] * n,
        "DRS": [0] * n,
        "DriverAhead": ["44"] * n,
    }


def _frame(driver: str, lap: int, n: int = 4) -> pd.DataFrame:
    from tif1.core_utils.helpers import _create_telemetry_df

    frame = _create_telemetry_df(_payload(n), driver, lap, "pandas")
    assert frame is not None
    return frame


def _same(left: pd.DataFrame, right: pd.DataFrame) -> None:
    assert list(left.columns) == list(right.columns)
    assert left.dtypes.equals(right.dtypes)
    assert left.index.equals(right.index)
    assert left.equals(right)


def _fill(cache: Cache, drivers=DRIVERS, laps=(1, 2, 5)) -> dict[tuple[str, int], pd.DataFrame]:
    frames = {(d, lap): _frame(d, lap, n=3 + lap) for d in drivers for lap in laps}
    batch = [(d, lap, f) for (d, lap), f in frames.items()]
    assert cache.set_telemetry_frames_batch(*KEY, batch) == len(batch)
    return frames


def _count_main_reads(cache: Cache) -> list[str]:
    """Record the drivers whose row the main-connection fallback read."""
    main_reads: list[str] = []
    original = cache._read_frame_blob_locked

    def counting(table, session_key, driver):
        main_reads.append(driver)
        return original(table, session_key, driver)

    cache._read_frame_blob_locked = counting
    return main_reads


@pytest.fixture
def parallel_pool(monkeypatch):
    """The parallel path even on a one-core CI box (the pool stays shared)."""
    if cache_mod._pa_lz4_codec() is None:
        pytest.skip("pyarrow without lz4")
    monkeypatch.setattr(cache_mod, "_decompress_workers", lambda: 2)


def test_rows_are_read_by_the_workers_on_reader_connections(tmp_path, parallel_pool):
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = _fill(cache)
        cache.conn.commit()
        main_reads = _count_main_reads(cache)
        sources = cache.get_telemetry_frame_sources(*KEY, list(DRIVERS))
        assert set(sources) == set(DRIVERS)
        for (driver, lap), frame in frames.items():
            _same(sources[driver].frame(lap), frame)
        # Every row came through a reader: the main connection read no blob
        # and the readers are back in the idle pool for the next read, with
        # no transaction left open.
        assert main_reads == []
        assert 1 <= len(cache._idle_readers) <= cache._reader_cap
        for reader in cache._idle_readers:
            assert reader.execute("PRAGMA query_only").fetchone() == (1,)
            assert not reader.in_transaction
        # The batch path reads through the same tasks (whole rows or wanted laps).
        refs = [(d, lap) for d in DRIVERS for lap in (1, 5)]
        hits = cache.get_telemetry_frames_batch(*KEY, refs)
        assert set(hits) == set(refs)
        for ref in refs:
            _same(hits[ref], frames[ref])
        whole = cache.get_telemetry_frames_batch(*KEY, [("VER", 1), ("HAM", 9)], whole_driver=True)
        assert set(whole) == {
            ("VER", 1),
            ("VER", 2),
            ("VER", 5),
            ("HAM", 1),
            ("HAM", 2),
            ("HAM", 5),
        }
        assert main_reads == []
    finally:
        cache.close()


def test_reader_cap_never_exceeds_the_worker_count(tmp_path, parallel_pool, monkeypatch):
    monkeypatch.setattr(cache_mod, "_FRAME_READER_CONNECTIONS", 8)
    cache = Cache(cache_dir=tmp_path)
    try:
        assert cache._reader_cap == 2  # two workers, see the fixture
    finally:
        cache.close()


def test_uncommitted_rows_are_served_through_the_main_connection(tmp_path, parallel_pool):
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = _fill(cache)
        assert cache._pending_writes > 0  # batched write, not yet committed
        main_reads = _count_main_reads(cache)
        sources = cache.get_telemetry_frame_sources(*KEY, list(DRIVERS))
        assert set(sources) == set(DRIVERS)
        for (driver, lap), frame in frames.items():
            _same(sources[driver].frame(lap), frame)
        # The readers' snapshot predates the rows: every one fell back.
        assert sorted(main_reads) == sorted(DRIVERS)
        cache.conn.commit()
        main_reads.clear()
        assert set(cache.get_telemetry_frame_sources(*KEY, ["VER"])) == {"VER"}
        assert main_reads == []
    finally:
        cache.close()


def test_a_row_replaced_at_its_rowid_before_commit_is_read_from_the_main_connection(
    tmp_path, parallel_pool
):
    """A rowid the reader's snapshot maps to older content of the same key must
    not be served from the reader (the main connection's length disagrees).

    ``INSERT OR REPLACE`` never reuses a rowid (the new rowid is allocated
    before the conflicting row is deleted), so the case is built by hand:
    the uncommitted replacement is moved onto the old rowid.
    """
    cache = Cache(cache_dir=tmp_path)
    try:
        _fill(cache, drivers=("VER",), laps=(1, 2))
        cache.conn.commit()
        (old_rowid,) = cache.conn.execute(f"SELECT rowid FROM {_PANDAS_DRIVER_TABLE}").fetchone()
        # Warm a reader on the committed row, then rewrite the row uncommitted.
        assert set(cache.get_telemetry_frame_sources(*KEY, ["VER"])) == {"VER"}
        new_frames = _fill(cache, drivers=("VER",), laps=(1, 2, 7))
        assert cache._pending_writes > 0
        (new_rowid,) = cache.conn.execute(f"SELECT rowid FROM {_PANDAS_DRIVER_TABLE}").fetchone()
        assert new_rowid != old_rowid
        cache.conn.execute(
            f"UPDATE {_PANDAS_DRIVER_TABLE} SET rowid = ? WHERE rowid = ?", (old_rowid, new_rowid)
        )
        main_reads = _count_main_reads(cache)
        sources = cache.get_telemetry_frame_sources(*KEY, ["VER"])
        assert main_reads == ["VER"]
        assert sorted(sources["VER"].laps) == [1, 2, 7]
        _same(sources["VER"].frame(7), new_frames[("VER", 7)])
    finally:
        cache.close()


def test_a_row_replaced_between_the_index_query_and_the_read_is_served_current(
    tmp_path, parallel_pool, monkeypatch
):
    cache = Cache(cache_dir=tmp_path)
    try:
        _fill(cache)
        cache.conn.commit()
        newer = {}
        original = cache_mod._reader_frame_blob
        rewritten = threading.Event()

        def rewriting_reader(conn, table, session_key, driver, rowid, nbytes):
            # Another thread's committed rewrite lands after the index query.
            if driver == "LEC" and not rewritten.is_set():
                newer.update(_fill(cache, drivers=("LEC",), laps=(1, 2, 5, 9)))
                cache.conn.commit()
                rewritten.set()
            return original(conn, table, session_key, driver, rowid, nbytes)

        monkeypatch.setattr(cache_mod, "_reader_frame_blob", rewriting_reader)
        main_reads = _count_main_reads(cache)
        sources = cache.get_telemetry_frame_sources(*KEY, list(DRIVERS))
        assert set(sources) == set(DRIVERS)
        assert main_reads == ["LEC"]
        assert sorted(sources["LEC"].laps) == [1, 2, 5, 9]
        _same(sources["LEC"].frame(9), newer[("LEC", 9)])
    finally:
        cache.close()


def test_a_reader_that_cannot_read_falls_back_to_the_main_connection(
    tmp_path, parallel_pool, monkeypatch
):
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = _fill(cache)
        cache.conn.commit()

        def failing_reader(conn, table, session_key, driver, rowid, nbytes):
            raise sqlite3.OperationalError("cannot open blob")

        monkeypatch.setattr(cache_mod, "_reader_frame_blob", failing_reader)
        main_reads = _count_main_reads(cache)
        sources = cache.get_telemetry_frame_sources(*KEY, list(DRIVERS))
        assert set(sources) == set(DRIVERS)
        assert sorted(main_reads) == sorted(DRIVERS)
        for (driver, lap), frame in frames.items():
            _same(sources[driver].frame(lap), frame)
        # The readers were returned to the pool despite the failure.
        assert 1 <= len(cache._idle_readers) <= cache._reader_cap
    finally:
        cache.close()


def test_reader_frame_blob_closes_its_transaction(tmp_path, parallel_pool):
    cache = Cache(cache_dir=tmp_path)
    try:
        _fill(cache, drivers=("VER",), laps=(1,))
        cache.conn.commit()
        rowid, nbytes = cache.conn.execute(
            f"SELECT rowid, length(frame) FROM {_PANDAS_DRIVER_TABLE}"
        ).fetchone()
        reader = cache._open_reader()
        assert reader is not None
        try:
            blob = cache_mod._reader_frame_blob(
                reader, _PANDAS_DRIVER_TABLE, KEY, "VER", rowid, nbytes
            )
            assert len(blob) == nbytes
            assert not reader.in_transaction
            # A disagreeing index (other rowid, other length, other driver) reads nothing.
            for args in ((rowid + 1, nbytes), (rowid, nbytes - 1)):
                assert (
                    cache_mod._reader_frame_blob(reader, _PANDAS_DRIVER_TABLE, KEY, "VER", *args)
                    is None
                )
                assert not reader.in_transaction
            assert (
                cache_mod._reader_frame_blob(
                    reader, _PANDAS_DRIVER_TABLE, KEY, "HAM", rowid, nbytes
                )
                is None
            )
            assert not reader.in_transaction
            with pytest.raises(sqlite3.OperationalError):
                cache_mod._reader_frame_blob(reader, "no_such_table", KEY, "VER", rowid, nbytes)
            assert not reader.in_transaction
        finally:
            cache._release_reader(reader)
    finally:
        cache.close()


def test_readers_close_with_the_cache_and_a_late_return_closes_too(tmp_path, parallel_pool):
    cache = Cache(cache_dir=tmp_path)
    _fill(cache, drivers=("VER",), laps=(1,))
    cache.conn.commit()
    assert cache.get_telemetry_frame_sources(*KEY, ["VER"])
    lent = cache._open_reader()
    assert lent is not None
    idle_before = list(cache._idle_readers)
    cache.close()
    assert cache._readers_closed
    assert cache._idle_readers == []
    for reader in idle_before:
        with pytest.raises(sqlite3.ProgrammingError):
            reader.execute("SELECT 1")
    cache._release_reader(lent)
    with pytest.raises(sqlite3.ProgrammingError):
        lent.execute("SELECT 1")
    assert cache._open_reader() is None
    # A worker whose reader is gone reads nothing from a closed cache.
    assert cache._read_frame_blob_locked(_PANDAS_DRIVER_TABLE, KEY, "VER") is None
    assert cache_mod._read_frame_row(cache, _PANDAS_DRIVER_TABLE, KEY, "VER", 1, 0, None) is None


def test_reader_pool_never_grows_past_the_cap(tmp_path, parallel_pool, monkeypatch):
    monkeypatch.setattr(cache_mod, "_FRAME_READER_CONNECTIONS", 2)
    cache = Cache(cache_dir=tmp_path)
    try:
        _fill(cache)
        cache.conn.commit()
        peak = 0
        seen = threading.Lock()
        original = cache._open_reader

        def counting_open():
            conn = original()
            with seen:
                nonlocal peak
                peak = max(peak, cache._reader_cap - cache._reader_slots._value)
            return conn

        cache._open_reader = counting_open
        for _ in range(3):
            assert len(cache.get_telemetry_frame_sources(*KEY, list(DRIVERS))) == 5
        assert peak <= 2
        assert len(cache._idle_readers) <= 2
        # Returning a reader to a full idle pool closes it instead.
        while len(cache._idle_readers) < 2:
            cache._idle_readers.append(sqlite3.connect(str(cache.db_path), check_same_thread=False))
        extra = sqlite3.connect(str(cache.db_path), check_same_thread=False)
        cache._release_reader(extra)
        assert len(cache._idle_readers) == 2
        with pytest.raises(sqlite3.ProgrammingError):
            extra.execute("SELECT 1")
    finally:
        cache.close()


def test_serial_path_reads_the_same_rows_through_the_cursor(tmp_path, monkeypatch):
    monkeypatch.setattr(cache_mod, "_can_decompress_in_parallel", lambda _codec: False)
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = _fill(cache)
        assert (
            cache_mod._submit_frame_row_reads(cache, _PANDAS_DRIVER_TABLE, KEY, [("VER", 1, 0)])
            is None
        )
        sources = cache.get_telemetry_frame_sources(*KEY, list(DRIVERS))
        assert set(sources) == set(DRIVERS)
        for (driver, lap), frame in frames.items():
            _same(sources[driver].frame(lap), frame)
        hits = cache.get_telemetry_frames_batch(*KEY, [("LEC", 2), ("NOR", 9)])
        assert set(hits) == {("LEC", 2)}
        _same(hits[("LEC", 2)], frames[("LEC", 2)])
        assert cache._idle_readers == []
    finally:
        cache.close()


def test_a_corrupt_row_and_a_missing_driver_are_misses_on_the_worker_path(tmp_path, parallel_pool):
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = _fill(cache)
        cache.conn.execute(
            f"UPDATE {_PANDAS_DRIVER_TABLE} SET frame = ? WHERE driver = 'LEC'",
            (cache_mod._compress_frame_blob(b"TF1\x02garbage"),),
        )
        cache.conn.commit()
        sources = cache.get_telemetry_frame_sources(*KEY, [*DRIVERS, "ALO"])
        assert set(sources) == set(DRIVERS) - {"LEC"}
        _same(sources["VER"].frame(5), frames[("VER", 5)])
        assert cache.get_telemetry_frame_sources(*KEY, ["ALO"]) == {}
    finally:
        cache.close()
