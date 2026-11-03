"""Elis E7: ``.telemetry`` of a sliced vs a boolean-taken VER pick, same process."""

from __future__ import annotations

import cProfile
import pstats
import statistics
import sys
import time

import tif1

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
laps = session.laps
mask = laps["Driver"] == "VER"


def sliced():
    return laps.iloc[laps._driver_run("VER")]


def taken():
    return laps[mask]


for name, make in (("taken", taken), ("sliced", sliced)) * 3:
    vals = []
    for _ in range(20):
        picked = make()
        t0 = time.perf_counter()
        picked.telemetry  # noqa: B018
        vals.append((time.perf_counter() - t0) * 1000)
    print(f"{name} median={statistics.median(vals):.3f}")
if len(sys.argv) > 1:
    make = sliced if sys.argv[1] == "sliced" else taken
    prof = cProfile.Profile()
    for _ in range(20):
        picked = make()
        prof.enable()
        picked.telemetry  # noqa: B018
        prof.disable()
    pstats.Stats(prof).sort_stats("tottime").print_stats(25)
