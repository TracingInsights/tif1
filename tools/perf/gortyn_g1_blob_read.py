"""Gortyn G1 probe: raw cost of reading a race's frame-tier driver rows.

Fresh process, the 2025 Bahrain race rows of ``telemetry_frames_drv`` read on one
read-only connection with ``blobopen``: ``Blob.read()`` into fresh ``bytes``
(the E4 base) against chunked reads copied into a huge-page buffer, serially
and on 4 threads (the reader cap). Prints wall, minor faults, sys/user ms.

Usage (repo root)::

    PYTHONPATH=native/target/site:src TIF1_CACHE_DIR=/tmp/tif1-gortyn-warm-G1 \
        python tools/perf/gortyn_g1_blob_read.py
"""

from __future__ import annotations

import ctypes
import os
import resource
import sqlite3
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

MODES = ["whole", "view256k", "mm256k", "mm1m", "view64k", "mm64k"]


def _rows(conn: sqlite3.Connection) -> list[int]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT rowid FROM telemetry_frames_drv WHERE year = 2025 AND session = 'Race' "
            "AND length(frame) > 2097152"
        )
    ]


def _read(conn: sqlite3.Connection, rowid: int, mode: str) -> object:
    from tif1 import cache

    handle = conn.blobopen("telemetry_frames_drv", "frame", rowid, readonly=True)
    try:
        if mode == "whole":
            return handle.read()
        size = len(handle)
        chunk = {"256k": 262144, "1m": 1048576, "64k": 65536}[mode[-4:].lstrip("wmv")]
        buffer = cache._huge_page_buffer(size)
        base = ctypes.addressof(ctypes.c_char.from_buffer(buffer))
        pos = 0
        while pos < size:
            data = handle.read(chunk)
            n = len(data)
            if mode.startswith("mm"):
                ctypes.memmove(base + pos, data, n)
            else:
                buffer[pos : pos + n] = data
            pos += n
        return buffer
    finally:
        handle.close()


def _child(mode: str, threads: int) -> None:
    db = Path(os.environ["TIF1_CACHE_DIR"]) / "cache.sqlite"
    conns = [
        sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True, check_same_thread=False)
        for _ in range(threads)
    ]
    for c in conns:
        c.execute("PRAGMA mmap_size = 1073741824")
    rows = _rows(conns[0])
    r0 = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    if threads == 1:
        out = [_read(conns[0], r, mode) for r in rows]
    else:
        with ThreadPoolExecutor(threads) as pool:
            out = list(
                pool.map(lambda ir: _read(conns[ir[0] % threads], ir[1], mode), enumerate(rows))
            )
    wall = (time.perf_counter() - t0) * 1e3
    r1 = resource.getrusage(resource.RUSAGE_SELF)
    assert len(out) == len(rows)
    print(
        f"{wall:.2f} {r1.ru_minflt - r0.ru_minflt} {(r1.ru_stime - r0.ru_stime) * 1e3:.1f} "
        f"{(r1.ru_utime - r0.ru_utime) * 1e3:.1f} {len(rows)}"
    )


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        _child(sys.argv[2], int(sys.argv[3]))
        return
    for threads in [int(t) for t in (sys.argv[1:] or ["1", "4"])]:
        res: dict[str, list[list[float]]] = {m: [] for m in MODES}
        for _ in range(7):
            for mode in MODES:
                out = subprocess.run(
                    [sys.executable, __file__, "--child", mode, str(threads)],
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.split()
                res[mode].append([float(x) for x in out])
        for mode, runs in res.items():
            med = [statistics.median(c) for c in zip(*runs, strict=True)]
            print(
                f"threads={threads} {mode:9} wall {med[0]:6.1f} faults {med[1]:6.0f} "
                f"sys {med[2]:5.1f} user {med[3]:5.1f} rows {med[4]:.0f}"
            )


if __name__ == "__main__":
    main()
