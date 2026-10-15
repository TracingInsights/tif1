"""Fresh-process frame-tier *write* timing of one cached session (Pergamon Q13).

Loads the session's pandas frames from a primed cache, then times the
background writer (``Cache.set_telemetry_frames_batch``) into a scratch cache
whose driver rows are deleted before every run (full write) and the write of
a few frames into the populated rows (partial write). Prints JSON with the
per-run wall times plus a parity digest (sha256 of the decompressed driver
blobs, and ``equals``/block-count checks of the re-read frames).

Usage:
    uv run python tools/pergamon_q13_write_bench.py --cache /tmp/tif1-q/cache.sqlite \
        [--src-dir SRC] [--runs 5] [--partial 10]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

YEAR, GP, SESSION = 2026, "Monaco%20Grand%20Prix", "Race"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--cache", required=True, help="primed cache.sqlite (read only)")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--partial", type=int, default=10)
    parser.add_argument("--no-parity", action="store_true")
    parser.add_argument("--dump", default=None, help="write decompressed driver blobs here")
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    scratch = Path(tempfile.mkdtemp(prefix="tif1-q13-"))
    os.environ["TIF1_CACHE_DIR"] = str(scratch)
    shutil.copy(args.cache, scratch / "cache.sqlite")
    conn = sqlite3.connect(scratch / "cache.sqlite")
    refs: set[tuple[str, int]] = set()
    for driver, laps in conn.execute(
        "SELECT driver, laps FROM telemetry_frames_drv WHERE year=? AND gp=? AND session=?",
        (YEAR, GP, SESSION),
    ):
        refs.update((driver, int(lap)) for lap in json.loads(laps))
    conn.close()

    from tif1.cache import Cache, _decompress_frame_blob

    cache = Cache(cache_dir=scratch)
    frames_map = cache.get_telemetry_frames_batch(YEAR, GP, SESSION, sorted(refs))
    frames = [(driver, lap, frame) for (driver, lap), frame in sorted(frames_map.items())]
    assert cache.conn is not None

    def clear_rows() -> None:
        with cache._sqlite_lock:
            cache.conn.execute(
                "DELETE FROM telemetry_frames_drv WHERE year=? AND gp=? AND session=?",
                (YEAR, GP, SESSION),
            )
            cache.conn.execute(
                "DELETE FROM telemetry_frames WHERE year=? AND gp=? AND session=?",
                (YEAR, GP, SESSION),
            )
            cache.conn.commit()

    full_ms: list[float] = []
    partial_ms: list[float] = []
    partial = frames[: args.partial]
    for _ in range(args.runs):
        clear_rows()
        t0 = time.perf_counter()
        written = cache.set_telemetry_frames_batch(YEAR, GP, SESSION, frames, lib="pandas")
        full_ms.append((time.perf_counter() - t0) * 1000)
        assert written == len(frames), written
        t0 = time.perf_counter()
        written = cache.set_telemetry_frames_batch(YEAR, GP, SESSION, partial, lib="pandas")
        partial_ms.append((time.perf_counter() - t0) * 1000)
        assert written == len(partial), written

    out = {
        "frames": len(frames),
        "full_ms": [round(v, 1) for v in full_ms],
        "partial_ms": [round(v, 2) for v in partial_ms],
    }
    if not args.no_parity:
        digest = hashlib.sha256()
        with cache._sqlite_lock:
            rows = cache.conn.execute(
                "SELECT driver, laps, frame FROM telemetry_frames_drv WHERE year=? AND gp=? "
                "AND session=? ORDER BY driver",
                (YEAR, GP, SESSION),
            ).fetchall()
        stored_bytes = 0
        for driver, laps, blob in rows:
            raw = bytes(_decompress_frame_blob(bytes(blob)))
            stored_bytes += len(blob)
            digest.update(driver.encode())
            digest.update(laps.encode())
            digest.update(raw)
            if args.dump:
                Path(args.dump).mkdir(parents=True, exist_ok=True)
                (Path(args.dump) / f"{driver}.bin").write_bytes(raw)
        reread = cache.get_telemetry_frames_batch(YEAR, GP, SESSION, sorted(refs))
        mismatches = 0
        for driver, lap, frame in frames:
            other = reread.get((driver, lap))
            if (
                other is None
                or not frame.equals(other)
                or list(frame.columns) != list(other.columns)
                or list(map(str, frame.dtypes)) != list(map(str, other.dtypes))
                or len(frame._mgr.blocks) != len(other._mgr.blocks)
            ):
                mismatches += 1
        out.update(
            {
                "rows": len(rows),
                "stored_bytes": stored_bytes,
                "blob_sha256": digest.hexdigest(),
                "reread_mismatches": mismatches,
            }
        )
    cache.close()
    shutil.rmtree(scratch, ignore_errors=True)
    print(json.dumps(out))


if __name__ == "__main__":
    main()
