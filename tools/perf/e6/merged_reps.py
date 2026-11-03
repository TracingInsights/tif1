"""E6 probe: in-process medians of polars steps after a warm load (numpy loaded).

Usage: TIF1_CACHE_DIR=... TIF1_OFFLINE_MODE=true python tools/perf/e6/merged_reps.py
Runs against whichever tree PYTHONPATH points at.
"""

from __future__ import annotations

import statistics
import time

import numpy as np  # noqa: F401  (the notebook has it loaded by then)

import tif1

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
session.load(weather=True, messages=True)
laps = session.laps


def merged():
    return [laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")]


def picks():
    return [
        laps.pick_driver("VER"),
        laps.pick_drivers(["VER", "LEC"]),
        laps.pick_quicklaps(),
        laps.pick_fastest(),
        laps.pick_laps([5, 6]),
        laps.pick_compounds(["SOFT"]),
        laps.pick_wo_box(),
        laps.pick_track_status("1"),
    ]


def fastest():
    return session.get_fastest_laps()


for name, fn in (("merged_x4", merged), ("picks_x8", picks), ("get_fastest_laps", fastest)):
    times = []
    for _ in range(15):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1e3)
    print(f"{name:18s} median {statistics.median(times):7.2f} ms  min {min(times):7.2f}")
