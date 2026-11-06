"""Gortyn G4: per-method in-process cost of the polars selections (numpy masks vs Series masks).

Run once per tree (``PYTHONPATH=<tree>/src``) with the same warm cache; numpy is
imported up front so both trees are timed in steady state and only the mask
kernels differ. Prints one JSON object of medians (microseconds) per method.

Usage (from the repo root):
    TIF1_CACHE_DIR=/tmp/tif1-gortyn-warm-G4 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/gortyn_g4/method_bench.py [--reps 20] [--no-numpy]
"""

from __future__ import annotations

import json
import statistics
import sys
import time

REPS = int(sys.argv[sys.argv.index("--reps") + 1]) if "--reps" in sys.argv else 20
if "--no-numpy" not in sys.argv:
    import numpy as np  # noqa: F401

import tif1  # noqa: E402

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
laps = session.laps
results = session.results

CASES = {
    "pick_driver": lambda: laps.pick_driver("VER"),
    "pick_drivers_2": lambda: laps.pick_drivers(["VER", "LEC"]),
    "pick_drivers_6": lambda: laps.pick_drivers(["VER", "LEC", "NOR", "PIA", "HAM", "RUS"]),
    "pick_drivers_num": lambda: laps.pick_drivers(["1", "16"]),
    "pick_quicklaps": lambda: laps.pick_quicklaps(),
    "pick_fastest": lambda: laps.pick_fastest(),
    "pick_fastest_time": lambda: laps.pick_fastest(only_by_time=True),
    "pick_laps_2": lambda: laps.pick_laps([5, 6]),
    "pick_laps_10": lambda: laps.pick_laps(list(range(1, 11))),
    "pick_laps_slice": lambda: laps.pick_laps(slice(3, 9)),
    "pick_lap": lambda: laps.pick_lap(5),
    "pick_compounds": lambda: laps.pick_compounds(["SOFT"]),
    "pick_compounds_2": lambda: laps.pick_compounds(["SOFT", "MEDIUM"]),
    "pick_tyre": lambda: laps.pick_tyre("SOFT"),
    "pick_team": lambda: laps.pick_team("McLaren"),
    "pick_teams": lambda: laps.pick_teams(["McLaren", "Ferrari"]),
    "pick_wo_box": lambda: laps.pick_wo_box(),
    "pick_box_laps": lambda: laps.pick_box_laps(),
    "pick_track_status": lambda: laps.pick_track_status("1"),
    "pick_track_status_contains": lambda: laps.pick_track_status("2", how="contains"),
    "pick_track_status_none": lambda: laps.pick_track_status("4", how="none"),
    "pick_not_deleted": lambda: laps.pick_not_deleted(),
    "pick_accurate": lambda: laps.pick_accurate(),
    "iterlaps_require": lambda: list(laps.iterlaps(require=["LapTime", "PitInTime"])),
    "iterlaps_require_none_kept": lambda: list(laps.iterlaps(require=["LapTime"])),
    "results_position_for": lambda: results._position_for("VER"),
    "results_position_name": lambda: results._position_for("lewis hamilton"),
    "notebook_picks_x8": lambda: [
        laps.pick_driver("VER"),
        laps.pick_drivers(["VER", "LEC"]),
        laps.pick_quicklaps(),
        laps.pick_fastest(),
        laps.pick_laps([5, 6]),
        laps.pick_compounds(["SOFT"]),
        laps.pick_wo_box(),
        laps.pick_track_status("1"),
    ],
}

out: dict[str, float] = {}
for name, fn in CASES.items():
    fn()  # first call outside the timing (lazy imports, thread pools)
    times = []
    for _ in range(REPS):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1e6)
    out[name] = round(statistics.median(times), 1)
print(json.dumps(out))
