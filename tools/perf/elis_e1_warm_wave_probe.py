"""Elis E1 probe: cost of the per-driver lazy waves on a warm polars session.

Usage (warm cache, offline):
    TIF1_CACHE_DIR=... TIF1_OFFLINE_MODE=true python tools/perf/elis_e1_warm_wave_probe.py polars
"""

from __future__ import annotations

import cProfile
import pstats
import sys
import time

import tif1


def main() -> None:
    lib = sys.argv[1] if len(sys.argv) > 1 else "polars"
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=lib)
    session.load(telemetry="lazy", weather=True, messages=True)
    laps = session.laps
    drivers = list(session._lazy_telemetry_refs)
    print("pending drivers", len(drivers))
    t = time.perf_counter()
    session._ensure_lazy_telemetry([(drivers[0], 1)])
    print(f"first driver wave {1000 * (time.perf_counter() - t):.1f} ms")
    prof = cProfile.Profile()
    prof.enable()
    t = time.perf_counter()
    _ = [lap.telemetry for _, lap in laps.iterlaps()]
    prof.disable()
    print(f"iterlaps loop {1000 * (time.perf_counter() - t):.1f} ms")
    pstats.Stats(prof).sort_stats("cumulative").print_stats(18)


if __name__ == "__main__":
    main()
