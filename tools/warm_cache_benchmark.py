"""Warm-cache benchmark: full telemetry load from a pre-warmed persistent cache.

Companion to ``monaco_telemetry_cold_benchmark.py``: one "run" first performs a
cold fetch (throwaway cache) into a *persistent* cache directory, then measures
a second, fresh process loading the same session from that warm cache.

Phases (warm process):
- ``get_session_s`` — schedule fetch + session construction
- ``laps_s``        — session.laps (drivers + laptimes from cache)
- ``telemetry_s``  — session.fetch_all_laps_telemetry() (all from cache)
- ``total_s``      — the warm-load pipeline

Usage:
    uv run python tools/warm_cache_benchmark.py            # warm run
    uv run python tools/warm_cache_benchmark.py --prime   # prime the cache
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

WARM_CACHE_DIR = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--prime", action="store_true", help="prime the persistent cache only")
    args = parser.parse_args()

    if args.src_dir:
        import sys

        sys.path.insert(0, str(Path(args.src_dir).resolve()))

    Path(WARM_CACHE_DIR).mkdir(parents=True, exist_ok=True)
    os.environ["TIF1_CACHE_DIR"] = WARM_CACHE_DIR

    import tif1

    if args.prime:
        session = tif1.get_session(args.year, args.gp, args.session)
        session.load()
        n = len(session.fetch_all_laps_telemetry())
        print(json.dumps({"primed": True, "telemetry_frames": n, "cache_dir": WARM_CACHE_DIR}))
        return

    t0 = time.perf_counter()
    session = tif1.get_session(args.year, args.gp, args.session)
    t_get = time.perf_counter() - t0

    t0 = time.perf_counter()
    laps = session.laps
    t_laps = time.perf_counter() - t0

    t0 = time.perf_counter()
    tel = session.fetch_all_laps_telemetry()
    t_tel = time.perf_counter() - t0

    result = {
        "get_session_s": round(t_get, 4),
        "laps_s": round(t_laps, 4),
        "telemetry_s": round(t_tel, 4),
        "total_s": round(t_get + t_laps + t_tel, 4),
        "laps_rows": len(laps),
        "telemetry_frames": len(tel),
    }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
