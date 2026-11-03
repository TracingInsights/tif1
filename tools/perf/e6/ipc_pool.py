"""E6 probe: pooled pyarrow-free IPC row reads (pl.read_ipc vs the eager stream reader).

Interleaved reps over the 20 race rows of a cache; also the serial single-row
cost. Usage: python tools/perf/e6/ipc_pool.py CACHE_DIR [WORKERS]
"""

from __future__ import annotations

import sqlite3
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
from polars._plr import PyDataFrame

conn = sqlite3.connect((Path(sys.argv[1]) / "cache.sqlite").as_posix())
workers = int(sys.argv[2]) if len(sys.argv) > 2 else 8
blobs = [
    bytes(b)
    for (b,) in conn.execute(
        "SELECT frame FROM telemetry_frames_pl_ipc WHERE year = 2025 AND session = 'Race'"
    )
]


def stream(blob: bytes) -> pl.DataFrame:
    return pl.DataFrame._from_pydf(
        PyDataFrame.read_ipc_stream(blob[8:], None, None, None, None, False)
    )


def file_read(blob: bytes) -> pl.DataFrame:
    return pl.read_ipc(blob)


readers = {"read_ipc": file_read, "stream": stream}
walls: dict[str, list[float]] = {name: [] for name in readers}
cpus: dict[str, list[float]] = {name: [] for name in readers}
with ThreadPoolExecutor(workers) as pool:
    list(pool.map(file_read, blobs))
    list(pool.map(stream, blobs))
    for _ in range(12):
        for name, fn in readers.items():
            c0, t0 = time.process_time(), time.perf_counter()
            list(pool.map(fn, blobs))
            walls[name].append((time.perf_counter() - t0) * 1e3)
            cpus[name].append((time.process_time() - c0) * 1e3)
for name in readers:
    print(
        f"pool{workers} {name:9s} wall median {statistics.median(walls[name]):6.1f} ms "
        f"min {min(walls[name]):6.1f}  cpu median {statistics.median(cpus[name]):6.1f} ms"
    )
for name, fn in readers.items():
    t0 = time.perf_counter()
    for blob in blobs:
        fn(blob)
    print(f"serial {name:9s} {(time.perf_counter() - t0) * 1e3:6.1f} ms for {len(blobs)} rows")
