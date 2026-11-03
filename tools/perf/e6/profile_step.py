"""E6 probe: cProfile one polars step in a fresh process (bench shape: no laps read first).

Usage: TIF1_CACHE_DIR=... TIF1_OFFLINE_MODE=true python tools/perf/e6/profile_step.py STEP [N]
STEP: fastest_lap_tel | all_tel | merged_x4 (after load + numpy) | merged_x4_np
"""

from __future__ import annotations

import cProfile
import pstats
import sys
import time

import tif1

step = sys.argv[1]
top = int(sys.argv[2]) if len(sys.argv) > 2 else 30
session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
if step == "fastest_lap_tel":
    fn = session.get_fastest_lap_tel
elif step == "all_tel":
    _ = session.laps
    fn = session.fetch_all_laps_telemetry
elif step.startswith("merged_x4"):
    session.load(weather=True, messages=True)
    laps = session.laps
    import numpy as np  # noqa: F401  (the notebook has it loaded by then)

    laps.pick_quicklaps()

    def fn():
        return [laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")]

else:
    raise SystemExit(f"unknown step {step}")
profiler = cProfile.Profile()
t0 = time.perf_counter()
profiler.enable()
out = fn()
profiler.disable()
print(f"{step}: {(time.perf_counter() - t0) * 1e3:.1f} ms")
print("numpy", sys.modules.get("numpy") is not None, "pyarrow", "pyarrow" in sys.modules)
pstats.Stats(profiler).sort_stats("cumulative").print_stats(top)
