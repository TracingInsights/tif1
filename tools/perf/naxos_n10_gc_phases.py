"""Naxos N10: cyclic-GC time per phase of a whole warm workload process (import included).

    TIF1_CACHE_DIR=/tmp/naxos/warm-N10 TIF1_OFFLINE_MODE=true \
        python tools/perf/naxos_n10_gc_phases.py pandas all_tel
"""

from __future__ import annotations

import gc
import sys
import time

events: list[tuple[str, int, float]] = []
_start: list[float] = []
_phase = ["import"]


_STACKS = "--stack" in sys.argv


def _cb(phase, info):
    if phase == "start":
        if _STACKS and info["generation"] >= 1:
            import traceback

            print("--- gen", info["generation"], "in", _phase[0])
            for frame in traceback.extract_stack():
                if "tif1" in frame.filename or "site-packages" in frame.filename:
                    print("   ", frame.filename.split("/")[-2:], frame.lineno, frame.name)
        _start.append(time.perf_counter())
    else:
        events.append((_phase[0], info["generation"], (time.perf_counter() - _start.pop()) * 1000))


gc.callbacks.append(_cb)
t_proc = time.perf_counter()
walls: dict[str, float] = {}


def step(name, fn):
    _phase[0] = name
    t0 = time.perf_counter()
    out = fn()
    walls[name] = (time.perf_counter() - t0) * 1000
    return out


lib = sys.argv[1] if len(sys.argv) > 1 else "pandas"
workload = sys.argv[2] if len(sys.argv) > 2 else "all_tel"
tif1 = step("import", lambda: __import__("tif1"))
session = step("get_session", lambda: tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=lib))
if workload == "notebook":
    step("load", lambda: session.load(weather=True, messages=True))
laps = step("laps", lambda: session.laps)
if workload == "all_tel":
    step("all_tel", session.fetch_all_laps_telemetry)
elif workload == "car_data_loop":
    drv = laps.pick_driver("VER")
    step("car_data_loop", lambda: [lap.get_car_data() for _, lap in drv.iterlaps()])
elif workload == "driver_tel":
    step("driver_tel", lambda: laps.pick_driver("VER").telemetry)
elif workload == "notebook":
    step("iterlaps_tel_loop", lambda: [lap.telemetry for _, lap in laps.iterlaps()])
    step(
        "driver_merged_x4",
        lambda: [laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")],
    )
gc.callbacks.remove(_cb)
by_phase: dict[str, list[tuple[int, float]]] = {}
for name, generation, ms in events:
    by_phase.setdefault(name, []).append((generation, ms))
for name, wall in walls.items():
    collections = by_phase.get(name, [])
    gc_ms = sum(ms for _, ms in collections)
    big = [(g, round(ms, 2)) for g, ms in collections if ms > 0.5]
    print(f"{name:18s} wall {wall:7.1f}  gc {gc_ms:6.2f} ms  n={len(collections):3d}  big={big}")
