"""E6 probe: polars-tier IPC row readers (pyarrow, pl.read_ipc, eager stream reader).

Usage: python tools/perf/e6/ipc_read.py CACHE_DIR [--no-arrow] [--driver VER]

``--no-arrow`` times only the pyarrow-free readers in a process that never
imports pyarrow (the warm polars situation).
"""

from __future__ import annotations

import io
import sqlite3
import sys
import time
from pathlib import Path

import polars as pl
from polars._plr import PyDataFrame

cache_dir = Path(sys.argv[1])
no_arrow = "--no-arrow" in sys.argv
driver = sys.argv[sys.argv.index("--driver") + 1] if "--driver" in sys.argv else "VER"
conn = sqlite3.connect((cache_dir / "cache.sqlite").as_posix())
rows = conn.execute(
    "SELECT driver, laps, frame FROM telemetry_frames_pl_ipc WHERE year = 2025 AND session = 'Race'"
).fetchall()
blobs = {d: bytes(b) for d, _, b in rows}
blob = blobs[driver]
print(f"{len(blobs)} rows, {driver} {len(blob) / 1e6:.2f} MB, head {blob[:8]!r} tail {blob[-6:]!r}")


def timed(label, fn, reps=10):
    t0 = time.perf_counter()
    out = fn()
    first = (time.perf_counter() - t0) * 1e3
    t0 = time.perf_counter()
    for _ in range(reps):
        fn()
    steady = (time.perf_counter() - t0) * 1e3 / reps
    print(f"{label:48s} first {first:7.2f} ms  steady {steady:7.2f} ms")
    return out


def stream_read(data: bytes) -> pl.DataFrame:
    # An IPC file is the 8-byte magic, the stream messages (schema,
    # dictionaries, batches, end-of-stream marker), the footer and the magic.
    return pl.DataFrame._from_pydf(
        PyDataFrame.read_ipc_stream(io.BytesIO(memoryview(data)[8:]), None, None, None, None, False)
    )


def stream_read_bytes(data: bytes) -> pl.DataFrame:
    return pl.DataFrame._from_pydf(
        PyDataFrame.read_ipc_stream(data[8:], None, None, None, None, False)
    )


a = timed("pl.read_ipc(bytes)", lambda: pl.read_ipc(blob))
b = timed("PyDataFrame.read_ipc_stream(BytesIO(mv[8:]))", lambda: stream_read(blob))
c = timed("PyDataFrame.read_ipc_stream(bytes[8:])", lambda: stream_read_bytes(blob))
print(
    "equal read_ipc vs stream:",
    a.equals(b),
    a.schema == b.schema,
    a.n_chunks("all"),
    b.n_chunks("all"),
)
print("schema", dict(b.schema))
if not no_arrow:
    import pyarrow as pa
    import pyarrow.ipc as pa_ipc

    def arrow_read(data):
        return pa_ipc.open_file(pa.BufferReader(data)).read_all()

    t = timed("pyarrow open_file.read_all", lambda: arrow_read(blob))
    d = timed("pl.from_arrow(table)", lambda: pl.from_arrow(t, rechunk=False))
    print("equal arrow vs stream:", d.equals(b), d.schema == b.schema)

# whole race: 20 rows serial, then pooled
timed("20 rows read_ipc serial", lambda: [pl.read_ipc(x) for x in blobs.values()], reps=3)
timed("20 rows stream serial", lambda: [stream_read(x) for x in blobs.values()], reps=3)
from concurrent.futures import ThreadPoolExecutor  # noqa: E402

with ThreadPoolExecutor(8) as pool:
    timed("20 rows read_ipc pooled(8)", lambda: list(pool.map(pl.read_ipc, blobs.values())), reps=3)
    timed("20 rows stream pooled(8)", lambda: list(pool.map(stream_read, blobs.values())), reps=3)
print("pyarrow imported:", "pyarrow" in sys.modules, "numpy:", "numpy" in sys.modules)
