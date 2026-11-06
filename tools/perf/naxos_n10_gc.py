"""Naxos N10: cyclic-GC collections (generation, ms, objects) inside a warm workload.

    TIF1_CACHE_DIR=/tmp/naxos/warm-N10 TIF1_OFFLINE_MODE=true \
        python tools/perf/naxos_n10_gc.py pandas all_tel
"""

from __future__ import annotations

import gc
import sys
import time

import tif1

lib = sys.argv[1] if len(sys.argv) > 1 else "pandas"
workload = sys.argv[2] if len(sys.argv) > 2 else "all_tel"
session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=lib)
if workload == "notebook":
    session.load(weather=True, messages=True)
laps = session.laps
events: list[tuple[int, float, int]] = []
start: list[float] = []


SHOW_STACK = "--stack" in sys.argv


def cb(phase, info):
    if phase == "start":
        if SHOW_STACK and info["generation"] >= 1:
            import threading
            import traceback

            print("--- gen", info["generation"], "thread", threading.current_thread().name)
            traceback.print_stack(limit=12)
        start.append(time.perf_counter())
    else:
        elapsed = (time.perf_counter() - start.pop()) * 1000
        events.append((info["generation"], elapsed, info["collected"]))


gc.callbacks.append(cb)
print("counts before", gc.get_count())
t0 = time.perf_counter()
if workload == "notebook":
    out = [lap.telemetry for _, lap in laps.iterlaps()]
    out += [laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")]
elif workload == "car_data_loop":
    drv = laps.pick_driver("VER")
    out = [lap.get_car_data() for _, lap in drv.iterlaps()]
else:
    out = session.fetch_all_laps_telemetry()
total = (time.perf_counter() - t0) * 1000
gc.callbacks.remove(cb)
print(
    f"{workload} {lib}: {total:.1f} ms; collections:", [(g, round(ms, 2), c) for g, ms, c in events]
)
print("gc ms total", round(sum(ms for _, ms, _ in events), 2), "counts after", gc.get_count())
