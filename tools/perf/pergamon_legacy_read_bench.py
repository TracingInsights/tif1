"""Fresh-process frame-tier read of one cached session (Pergamon legacy A/B).

Usage:
    TIF1_CACHE_DIR=/tmp/tif1-legacy uv run python tools/perf/pergamon_legacy_read_bench.py \
        --year 2024 --gp "Monaco%20Grand%20Prix" --session Race [--src-dir SRC]
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--gp", required=True, help="URL-encoded event name as stored")
    parser.add_argument("--session", required=True)
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    cache_dir = Path(os.environ["TIF1_CACHE_DIR"])
    conn = sqlite3.connect(cache_dir / "cache.sqlite")
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    refs: set[tuple[str, int]] = set()
    for driver, lap in conn.execute(
        "SELECT driver, lap FROM telemetry_frames WHERE year=? AND gp=? AND session=?",
        (args.year, args.gp, args.session),
    ):
        refs.add((driver, int(lap)))
    if "telemetry_frames_drv" in tables:
        for driver, laps in conn.execute(
            "SELECT driver, laps FROM telemetry_frames_drv WHERE year=? AND gp=? AND session=?",
            (args.year, args.gp, args.session),
        ):
            refs.update((driver, int(lap)) for lap in json.loads(laps))
    conn.close()
    from tif1.cache import Cache

    cache = Cache(cache_dir=cache_dir)
    t0 = time.perf_counter()
    frames = cache.get_telemetry_frames_batch(args.year, args.gp, args.session, sorted(refs))
    elapsed = time.perf_counter() - t0
    cache.close()
    print(
        json.dumps({"refs": len(refs), "frames": len(frames), "read_ms": round(elapsed * 1000, 1)})
    )


if __name__ == "__main__":
    main()
