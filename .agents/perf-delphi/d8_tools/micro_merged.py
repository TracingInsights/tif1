"""In-process micro benchmark of ``laps.pick_driver(d).telemetry`` after ``load()``.

Usage: ``python -I micro_merged.py <control|candidate> [reps]``.
Each repetition rebuilds the session (a cold memo) and runs ``load()``
first, then times the merged telemetry of VER, LEC, NOR and PIA. Prints
the per-driver median over the repetitions and the first-call values.
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time

TREES = {
    "control": ("/tmp/tif1-delphi-control/src", "/tmp/tif1-delphi-warm"),
    "candidate": ("/tmp/tif1-delphi/D8/src", "/tmp/tif1-delphi-warm-D8"),
}
tree = sys.argv[1] if len(sys.argv) > 1 else "candidate"
REPS = int(sys.argv[2]) if len(sys.argv) > 2 else 10
src, cache_dir = TREES[tree]
sys.path.insert(0, src)
os.environ["TIF1_OFFLINE_MODE"] = "true"
os.environ["TIF1_CACHE_DIR"] = cache_dir

import tif1  # noqa: E402
from tif1.cache import clear_lap_cache  # noqa: E402

DRIVERS = ("VER", "LEC", "NOR", "PIA")
times: dict[str, list[float]] = {d: [] for d in DRIVERS}
load_times: list[float] = []
for _ in range(REPS):
    clear_lap_cache()
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    t0 = time.perf_counter()
    session.load(weather=False, messages=False)
    load_times.append((time.perf_counter() - t0) * 1000)
    laps = session.laps
    for driver in DRIVERS:
        picked = laps.pick_driver(driver)
        t0 = time.perf_counter()
        tel = picked.telemetry
        times[driver].append((time.perf_counter() - t0) * 1000)
        assert len(tel) > 1000
out = {
    "tree": tree,
    "reps": REPS,
    "load_median_ms": round(statistics.median(load_times), 1),
    "per_driver_median_ms": {d: round(statistics.median(v), 2) for d, v in times.items()},
    "per_driver_first_ms": {d: round(v[0], 2) for d, v in times.items()},
    "four_drivers_median_ms": round(
        statistics.median(
            [sum(times[d][i] for d in DRIVERS) for i in range(REPS)],
        ),
        1,
    ),
}
print(json.dumps(out))
