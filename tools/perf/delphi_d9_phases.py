"""Delphi D9 probe: where the first driver-row read of a process spends its time.

Fresh process, warm cache, pandas imported up front. Measures, in order,
on the same ``Cache``:

1. ``index``: the ``(driver, rowid)`` query on the main connection;
2. ``raw read``: the 20 blobs read through ``blobopen`` on ``--readers``
   reader threads (no decompression), first call and a warm repeat;
3. ``read+lz4``: the pool path of the tree (``_submit_frame_row_reads`` +
   ``_completed_frame_rows``), rows discarded;
4. ``sources``: ``get_telemetry_frame_sources`` (read + lz4 + decode).

Each step is run on a fresh ``Cache`` so the file map and the readers start
cold, except where ``warm`` is printed. Run from the repository root with
``PYTHONPATH`` pointing at the D9 tree.
"""

from __future__ import annotations

import argparse
import os
import resource
import sys
import threading
import time

GP = "Bahrain%20Grand%20Prix"


def _timed(label: str, fn) -> None:
    before = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    out = fn()
    dt = (time.perf_counter() - t0) * 1e3
    after = resource.getrusage(resource.RUSAGE_SELF)
    sys_ms = (after.ru_stime - before.ru_stime) * 1e3
    print(
        f"{label:28s} {dt:7.1f} ms  minflt {after.ru_minflt - before.ru_minflt:6d}  "
        f"sys {sys_ms:5.1f} ms  -> {out}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--readers", type=int, default=2)
    args = parser.parse_args()
    if not os.environ.get("TIF1_CACHE_DIR"):
        sys.exit("set TIF1_CACHE_DIR to a warm cache directory")

    import pandas as pd  # noqa: F401

    import tif1.cache as cache_mod

    cache_mod._FRAME_READER_CONNECTIONS = args.readers
    key = (2025, GP, "Race")
    table = cache_mod._PANDAS_DRIVER_TABLE

    cache = cache_mod.Cache()
    conn = cache.conn
    drivers = [
        row[0]
        for row in conn.execute(
            f"SELECT driver FROM {table} WHERE year = ? AND gp = ? AND session = ?", key
        ).fetchall()
    ]

    def index():
        return len(
            conn.execute(
                f"SELECT driver, rowid FROM {table} WHERE year = ? AND gp = ? AND session = ?",
                key,
            ).fetchall()
        )

    _timed("index query", index)
    _timed("index query (warm)", index)

    stored = conn.execute(
        f"SELECT driver, rowid FROM {table} WHERE year = ? AND gp = ? AND session = ?", key
    ).fetchall()

    def raw_read():
        todo = list(stored)
        lock = threading.Lock()
        total = [0]

        def work():
            with cache._reader_slots:
                reader = cache._open_reader()
                try:
                    while True:
                        with lock:
                            if not todo:
                                return
                            _driver, rowid = todo.pop()
                        handle = reader.blobopen(table, "frame", rowid, readonly=True)
                        try:
                            n = len(handle.read())
                        finally:
                            handle.close()
                        with lock:
                            total[0] += n
                finally:
                    cache._release_reader(reader)

        threads = [threading.Thread(target=work) for _ in range(args.readers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return f"{total[0] / 1e6:.1f} MB"

    _timed(f"raw read x{args.readers} (cold map)", raw_read)
    _timed(f"raw read x{args.readers} (warm)", raw_read)
    cache.close()

    cache = cache_mod.Cache()

    def read_lz4():
        pending = cache_mod._submit_frame_row_reads(cache, table, stored)
        n = 0
        for _driver, raw in cache_mod._completed_frame_rows(pending):
            n += len(raw)
        return f"{n / 1e6:.1f} MB decompressed"

    _timed("read+lz4 (cold)", read_lz4)
    _timed("read+lz4 (warm)", read_lz4)
    cache.close()

    cache = cache_mod.Cache()
    _timed("sources (cold)", lambda: len(cache.get_telemetry_frame_sources(*key, drivers)))
    _timed("sources (warm)", lambda: len(cache.get_telemetry_frame_sources(*key, drivers)))
    cache.close()

    # Decode alone: the rows already decompressed on the main thread.
    cache = cache_mod.Cache()
    rows = list(
        cache_mod._completed_frame_rows(cache_mod._submit_frame_row_reads(cache, table, stored))
    )
    from tif1.core_utils.frame_codec import DriverFrameSource

    def decode():
        with cache_mod._suspend_gc():
            return len([DriverFrameSource(raw) for _d, raw in rows])

    _timed("decode only (20 rows)", decode)
    _timed("decode only (warm)", decode)
    cache.close()


if __name__ == "__main__":
    main()
