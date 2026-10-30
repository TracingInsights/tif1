"""In-process best-of-N decode of a session's driver rows (Pergamon Q18).

Usage:
    uv run python tools/perf/q18_decode_bench.py --db /tmp/x/cache.sqlite --year 2026 \
        --gp "Monaco%20Grand%20Prix" --session Race [--src-dir SRC] [--reps 30] [--profile]
"""

from __future__ import annotations

import argparse
import gc
import json
import sqlite3
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--db", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--gp", default="Monaco%20Grand%20Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--reps", type=int, default=30)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    from tif1.cache import _decompress_frame_blob
    from tif1.core_utils.frame_codec import decode_driver_frames
    from tif1.models import Telemetry

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT driver, frame FROM telemetry_frames_drv WHERE year=? AND gp=? AND session=?",
        (args.year, args.gp, args.session),
    ).fetchall()
    conn.close()
    raws = [bytes(_decompress_frame_blob(blob)) for _, blob in rows]

    def run(bufs: list[bytearray]) -> int:
        count = 0
        for buf in bufs:
            count += len(decode_driver_frames(buf, None, Telemetry))
        return count

    times = []
    frames = 0
    gc.disable()
    for _ in range(args.reps):
        bufs = [bytearray(r) for r in raws]
        t0 = time.perf_counter()
        frames = run(bufs)
        times.append(time.perf_counter() - t0)
    gc.enable()
    times.sort()
    print(
        json.dumps(
            {
                "src": args.src_dir,
                "year": args.year,
                "drivers": len(raws),
                "frames": frames,
                "best_ms": round(times[0] * 1000, 2),
                "median_ms": round(times[len(times) // 2] * 1000, 2),
                "us_per_frame_best": round(times[0] * 1e6 / max(frames, 1), 2),
            }
        )
    )
    if args.profile:
        import cProfile
        import pstats

        bufs = [bytearray(r) for r in raws]
        prof = cProfile.Profile()
        prof.enable()
        run(bufs)
        prof.disable()
        pstats.Stats(prof).sort_stats("tottime").print_stats(25)


if __name__ == "__main__":
    main()
