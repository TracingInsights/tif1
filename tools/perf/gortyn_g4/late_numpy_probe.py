"""Gortyn G4 probe: the cost of importing numpy late in a polars notebook, with and without gc.

Runs the notebook's load, a pick and the iterlaps telemetry loop (numpy stays
unloaded on this tree), then times ``import numpy`` with the collector on
(default) or off (``--no-gc``) and prints the gc collections the import
triggered. ``--early`` times the import in a bare interpreter instead.

Usage (from the repo root):
    TIF1_CACHE_DIR=/tmp/tif1-gortyn-warm-G4 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/gortyn_g4/late_numpy_probe.py [--no-gc] [--early]
"""

from __future__ import annotations

import gc
import json
import sys
import time

if "--early" in sys.argv:
    t0 = time.perf_counter()
    import numpy as np

    print(json.dumps({"early_import_ms": round((time.perf_counter() - t0) * 1000, 2)}))
    sys.exit(0)

import tif1

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
session.load(weather=True, messages=True)
laps = session.laps
laps.pick_quicklaps()
[lap.telemetry for _, lap in laps.iterlaps()]
assert "numpy" not in sys.modules
if "--no-gc" in sys.argv:
    gc.disable()
before = [stats["collections"] for stats in gc.get_stats()]
t0 = time.perf_counter()
import numpy as np  # noqa: E402, F401

ms = (time.perf_counter() - t0) * 1000
after = [stats["collections"] for stats in gc.get_stats()]
print(
    json.dumps(
        {
            "late_import_ms": round(ms, 2),
            "gc_collections": [b - a for a, b in zip(before, after, strict=True)],
            "tracked_objects": len(gc.get_objects()),
        }
    )
)
