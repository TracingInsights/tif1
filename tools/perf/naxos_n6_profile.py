"""Naxos N6 probe: profile the pandas per-lap telemetry loops on the warm cache.

Usage (from the tree root, offline, warm cache):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_profile.py [loop|car|both] [--profile]
"""

from __future__ import annotations

import cProfile
import pstats
import sys
import time


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "loop"
    profile = "--profile" in sys.argv
    import tif1

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    if mode in ("loop", "both"):
        session.load(weather=True, messages=True)
    laps = session.laps
    prof = cProfile.Profile() if profile else None
    t0 = time.perf_counter()
    if prof:
        prof.enable()
    if mode in ("loop", "both"):
        out = [lap.telemetry for _, lap in laps.iterlaps()]
    else:
        drv = laps.pick_driver("VER")
        out = [lap.get_car_data() for _, lap in drv.iterlaps()]
    if prof:
        prof.disable()
    dt = (time.perf_counter() - t0) * 1000
    print(f"{mode}: {len(out)} frames in {dt:.1f} ms")
    if prof:
        stats = pstats.Stats(prof)
        stats.sort_stats("cumulative").print_stats(45)
        stats.sort_stats("tottime").print_stats(30)


if __name__ == "__main__":
    main()
