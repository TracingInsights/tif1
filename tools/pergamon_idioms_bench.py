"""Fresh-process timings of FastF1-style idioms on a warm cache (Pergamon series).

Prints one JSON line: first single-lap telemetry without a prior full load,
the next lap of the same driver, a driver's merged telemetry, and 50 laps
touched one by one through ``laps.iloc[i].telemetry``.

Usage:
    TIF1_CACHE_DIR=/tmp/tif1-warm-p3 uv run python tools/pergamon_idioms_bench.py [--src-dir SRC]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--driver", default="ANT")
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    import tif1

    session = tif1.get_session(args.year, args.gp, args.session)
    laps = session.laps
    driver_laps = laps.pick_driver(args.driver)
    t0 = time.perf_counter()
    first = driver_laps.iloc[3].telemetry
    t_first = time.perf_counter() - t0
    t0 = time.perf_counter()
    second = driver_laps.iloc[4].telemetry
    t_second = time.perf_counter() - t0
    t0 = time.perf_counter()
    merged = driver_laps.telemetry
    t_merged = time.perf_counter() - t0
    t0 = time.perf_counter()
    touched = [laps.iloc[i].telemetry for i in range(200, 250)]
    t_fifty = time.perf_counter() - t0
    print(
        json.dumps(
            {
                "first_lap_ms": round(t_first * 1000, 2),
                "next_lap_same_driver_ms": round(t_second * 1000, 2),
                "driver_merged_ms": round(t_merged * 1000, 2),
                "fifty_laps_ms": round(t_fifty * 1000, 2),
                "rows": [len(first), len(second), len(merged), sum(len(t) for t in touched)],
            }
        )
    )


if __name__ == "__main__":
    main()
