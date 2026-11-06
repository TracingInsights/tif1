"""Naxos N10: where the warm ``fetch_all_laps_telemetry()`` time goes.

Fresh process, offline, warm cache (``TIF1_CACHE_DIR``). Runs ``session.laps``
then ``fetch_all_laps_telemetry()`` under cProfile (``--mode profile``) or a
plain timer (``--mode time``) and prints the top functions by cumulative and
own time.

    TIF1_CACHE_DIR=/tmp/naxos/warm-N10 TIF1_OFFLINE_MODE=true \
        python tools/perf/naxos_n10_profile.py --lib pandas --mode profile
"""

from __future__ import annotations

import argparse
import cProfile
import pstats
import time


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--mode", default="profile", choices=["profile", "time"])
    parser.add_argument("--top", type=int, default=40)
    parser.add_argument("--workload", default="all_tel", choices=["all_tel", "merged_x4"])
    args = parser.parse_args()

    import tif1

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=args.lib)
    if args.workload == "merged_x4":
        session.load(weather=True, messages=True)
    laps = session.laps

    def run():
        if args.workload == "merged_x4":
            return [laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")]
        return session.fetch_all_laps_telemetry()

    if args.mode == "time":
        t0 = time.perf_counter()
        out = run()
        print(f"{args.workload}: {(time.perf_counter() - t0) * 1000:.1f} ms, {len(out)} items")
        return
    prof = cProfile.Profile()
    t0 = time.perf_counter()
    prof.enable()
    out = run()
    prof.disable()
    print(f"{args.workload}: {(time.perf_counter() - t0) * 1000:.1f} ms, {len(out)} items")
    stats = pstats.Stats(prof)
    stats.sort_stats("cumulative").print_stats(args.top)
    stats.sort_stats("tottime").print_stats(args.top)


if __name__ == "__main__":
    main()
