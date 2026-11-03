"""Gortyn G4 probe: the notebook's ``telemetry_analysis_4`` step split into sub-steps.

Runs the notebook up to the analysis (load, the eight picks, the iterlaps
telemetry loop, iloc, merged x4, the per-driver fastest laps) and times each
call of the analysis. ``--numpy-first`` imports numpy before the session (the
control's state at that point, where the picks had imported it).

Usage (from the repo root):
    TIF1_CACHE_DIR=/tmp/tif1-gortyn-warm-G4 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/gortyn_g4/analysis_split.py [--numpy-first]
"""

from __future__ import annotations

import json
import sys
import time

if "--numpy-first" in sys.argv:
    import numpy as np  # noqa: F401

import tif1

out: dict[str, float] = {}


def step(name, fn):
    t0 = time.perf_counter()
    r = fn()
    out[name] = round(out.get(name, 0.0) + (time.perf_counter() - t0) * 1000, 2)
    return r


session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
session.load(weather=True, messages=True)
laps = session.laps
step(
    "picks_x8",
    lambda: [
        laps.pick_driver("VER"),
        laps.pick_drivers(["VER", "LEC"]),
        laps.pick_quicklaps(),
        laps.pick_fastest(),
        laps.pick_laps([5, 6]),
        laps.pick_compounds(["SOFT"]),
        laps.pick_wo_box(),
        laps.pick_track_status("1"),
    ],
)
step("iterlaps_tel_loop", lambda: [lap.telemetry for _, lap in laps.iterlaps()])
[laps.row(i, named=True) for i in range(200)]
[laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")]
fastest = []
for d in laps["Driver"].unique().to_list():
    lap = laps.pick_driver(d).pick_fastest()
    if lap is not None:
        fastest.append(lap)
    if len(fastest) == 4:
        break
step("numpy_import", lambda: __import__("numpy"))
tels = []
for i, lap in enumerate(fastest):
    tel = step("get_telemetry", lap.get_telemetry)
    tel = step("add_distance", tel.add_distance)
    tel = step("add_differential_distance", tel.add_differential_distance)
    step(f"resample_channels_{i}", lambda tel=tel: tel.resample_channels(rule="0.1s"))
    step("fill_missing", tel.fill_missing)
    step(
        "slice_by_time",
        lambda tel=tel: tel.slice_by_time(tel["Time"][0], tel["Time"][len(tel) // 2]),
    )
    tels.append(tel)
step("merge_channels", lambda: tels[0].merge_channels(tels[1]))
print(json.dumps(out))
