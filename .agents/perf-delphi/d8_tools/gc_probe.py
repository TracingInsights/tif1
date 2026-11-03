"""Where do the cyclic GC passes land during a no-load ``Laps.telemetry``?

Usage: ``python -I gc_probe.py <control|candidate> [driver] [load]``.
Logs every collection (generation, duration, objects) with the stage it
interrupted, in a fresh process.
"""

from __future__ import annotations

import gc
import os
import sys
import time

TREES = {
    "control": ("/tmp/tif1-delphi-control/src", "/tmp/tif1-delphi-warm"),
    "candidate": ("/tmp/tif1-delphi/D8/src", "/tmp/tif1-delphi-warm-D8"),
}
tree = sys.argv[1] if len(sys.argv) > 1 else "candidate"
DRIVER = sys.argv[2] if len(sys.argv) > 2 else "VER"
LOAD = "load" in sys.argv[3:]
src, cache_dir = TREES[tree]
sys.path.insert(0, src)
os.environ["TIF1_OFFLINE_MODE"] = "true"
os.environ["TIF1_CACHE_DIR"] = cache_dir

stage = ["startup"]
events: list[tuple[str, int, float, int]] = []
_t: list[float] = [0.0]


def _cb(phase: str, info: dict) -> None:
    if phase == "start":
        _t[0] = time.perf_counter()
    else:
        events.append(
            (stage[0], info["generation"], (time.perf_counter() - _t[0]) * 1000, info["collected"])
        )


gc.callbacks.append(_cb)

import tif1  # noqa: E402

stage[0] = "get_session"
session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
if LOAD:
    stage[0] = "load"
    session.load(weather=False, messages=False)
stage[0] = "laps"
laps = session.laps
stage[0] = "pick_driver"
drv = laps.pick_driver(DRIVER)
stage[0] = "driver_tel"
print("gc counts before driver_tel", gc.get_count(), "threshold", gc.get_threshold())
t0 = time.perf_counter()
tel = drv.telemetry
print("driver_tel ms", round((time.perf_counter() - t0) * 1000, 2), "rows", len(tel))
print("gc counts after", gc.get_count(), "tracked objects", len(gc.get_objects()))
for st, gen, dur, collected in events:
    if st in ("driver_tel", "load", "laps", "pick_driver"):
        print(f"  gc gen{gen} during {st}: {dur:.2f} ms, collected {collected}")
summary = {}
for st, gen, dur, _c in events:
    key = (st, gen)
    n, total = summary.get(key, (0, 0.0))
    summary[key] = (n + 1, total + dur)
for (st, gen), (n, total) in sorted(summary.items()):
    print(f"{st:12s} gen{gen}: {n} passes, {total:.2f} ms")
