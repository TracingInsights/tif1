"""Delphi D9 probe: raw read of a session's pandas driver rows from the frame tier.

Compares, in one process, the control read (one cursor on the caller's
thread, ``SELECT driver, frame ... WHERE year/gp/session``) with the D9
read (one index query, then ``blobopen().read()`` per row on reader
connections in ``k`` worker threads). Prints the median of ``--reps``
runs and the minor page faults of one run.

    TIF1_CACHE_DIR=/path/to/warm/cache python tools/perf/delphi_d9_rowread.py \\
        --year 2025 --gp "Bahrain%20Grand%20Prix" --session Race --readers 1,2,4,8

The first run of a process faults the file map in; ``--reps`` above 1
also shows the steady state. Run from the repository root.
"""

from __future__ import annotations

import argparse
import os
import queue
import resource
import sqlite3
import statistics
import sys
import threading
import time
from pathlib import Path

TABLE = "telemetry_frames_drv"
MMAP_BYTES = 2**31 - 2**16


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=30.0)
    conn.execute("PRAGMA query_only=1")
    if sys.maxsize > 2**32:
        conn.execute(f"PRAGMA mmap_size={MMAP_BYTES}")
    return conn


def _control(conn: sqlite3.Connection, key: tuple[int, str, str]) -> int:
    rows = conn.execute(
        f"SELECT driver, frame FROM {TABLE} WHERE year = ? AND gp = ? AND session = ?", key
    ).fetchall()
    return sum(len(row[1]) for row in rows)


def _workers(
    conn: sqlite3.Connection, readers: list[sqlite3.Connection], key: tuple[int, str, str]
) -> int:
    ids = conn.execute(
        f"SELECT driver, rowid FROM {TABLE} WHERE year = ? AND gp = ? AND session = ?", key
    ).fetchall()
    todo: queue.SimpleQueue = queue.SimpleQueue()
    for row in ids:
        todo.put(row)
    sizes: list[int] = []
    lock = threading.Lock()

    def work(reader: sqlite3.Connection) -> None:
        total = 0
        while True:
            try:
                _driver, rowid = todo.get_nowait()
            except queue.Empty:
                break
            handle = reader.blobopen(TABLE, "frame", rowid, readonly=True)
            try:
                total += len(handle.read())
            finally:
                handle.close()
        with lock:
            sizes.append(total)

    threads = [threading.Thread(target=work, args=(reader,)) for reader in readers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return sum(sizes)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--gp", default="Bahrain%20Grand%20Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--readers", default="1,2,4,8")
    parser.add_argument("--reps", type=int, default=10)
    args = parser.parse_args()
    cache_dir = os.environ.get("TIF1_CACHE_DIR")
    if not cache_dir:
        sys.exit("set TIF1_CACHE_DIR to a warm cache directory")
    db_path = Path(cache_dir) / "cache.sqlite"
    key = (args.year, args.gp, args.session)
    conn = _connect(db_path)
    n_rows = conn.execute(
        f"SELECT count(*) FROM {TABLE} WHERE year = ? AND gp = ? AND session = ?", key
    ).fetchone()[0]
    print(f"rows: {n_rows}")

    def measure(label: str, fn) -> None:
        times: list[float] = []
        faults = 0
        for i in range(args.reps):
            before = resource.getrusage(resource.RUSAGE_SELF).ru_minflt
            t0 = time.perf_counter()
            n_bytes = fn()
            times.append((time.perf_counter() - t0) * 1e3)
            if i == 0:
                faults = resource.getrusage(resource.RUSAGE_SELF).ru_minflt - before
        print(
            f"{label:22s} {n_bytes / 1e6:6.1f} MB  median {statistics.median(times):6.1f} ms  "
            f"first {times[0]:6.1f} ms  minflt(first) {faults}"
        )

    measure("control cursor", lambda: _control(conn, key))
    for k in (int(part) for part in args.readers.split(",")):
        readers = [_connect(db_path) for _ in range(k)]
        measure(f"workers k={k}", lambda readers=readers: _workers(conn, readers, key))
        for reader in readers:
            reader.close()


if __name__ == "__main__":
    main()
