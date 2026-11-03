"""Elis E7: cProfile one warm pandas phase in a fresh process.

Usage (worktree root, primed cache, offline):
    python tools/perf/elis_e7_fresh.py results|split|driver_tel|car_data_loop|merged_x4 [N]
"""

from __future__ import annotations

import cProfile
import pstats
import sys
import time

import tif1

phase = sys.argv[1]
top = int(sys.argv[2]) if len(sys.argv) > 2 else 40
session = tif1.get_session(
    2025, "Bahrain Grand Prix", "Qualifying" if phase == "split" else "Race", lib="pandas"
)
if phase == "merged_x4":
    session.load(weather=True, messages=True)
laps = session.laps
drv = laps.pick_driver("VER") if phase == "car_data_loop" else None
fns = {
    "results": lambda: session.results,
    "split": laps.split_qualifying_sessions,
    "driver_tel": lambda: laps.pick_driver("VER").telemetry,
    "car_data_loop": lambda: [lap.get_car_data() for _, lap in drv.iterlaps()],
    "car_data_total": lambda: [lap.get_car_data() for _, lap in laps.pick_driver("VER").iterlaps()],
    "merged_x4": lambda: [laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")],
}
fn = fns[phase]
if top == 0:
    t0 = time.perf_counter()
    fn()
    print(f"{phase} {(time.perf_counter() - t0) * 1000:.2f} ms")
    sys.exit(0)
prof = cProfile.Profile()
prof.enable()
fn()
prof.disable()
stats = pstats.Stats(prof).sort_stats(sys.argv[3] if len(sys.argv) > 3 else "cumulative")
if len(sys.argv) > 4:
    stats.print_callers(sys.argv[4])
else:
    stats.print_stats(top)
