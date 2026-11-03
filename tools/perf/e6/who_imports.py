"""E6 probe: print the stack that first imports numpy / pyarrow / pandas on a polars workload.

Usage: TIF1_CACHE_DIR=... TIF1_OFFLINE_MODE=true python tools/perf/e6/who_imports.py driver_tel
       [--block]   (numpy made unimportable after ``session.laps``)
"""

from __future__ import annotations

import sys
import time
import traceback

WATCH = ("numpy", "pyarrow", "pandas")
_seen: set[str] = set()
ARMED = [False]


class _Finder:
    @staticmethod
    def find_spec(fullname, _path=None, _target=None):
        root = fullname.split(".")[0]
        if ARMED[0] and root in WATCH and root not in _seen and fullname not in sys.modules:
            _seen.add(root)
            print(f"=== first import of {root}", file=sys.stderr)
            traceback.print_stack(limit=18, file=sys.stderr)


sys.meta_path.insert(0, _Finder)

import tif1  # noqa: E402

workload = sys.argv[1]
sess_name = "Qualifying" if workload == "quali_split" else "Race"
session = tif1.get_session(2025, "Bahrain Grand Prix", sess_name, lib="polars")
laps = session.laps
ARMED[0] = True
if "--block" in sys.argv:
    sys.modules["numpy"] = None  # type: ignore[assignment]
t0 = time.perf_counter()
r = None
if workload == "driver_tel":
    r = laps.pick_driver("VER").telemetry
elif workload == "fastest_lap_tel":
    r = session.get_fastest_lap_tel()
elif workload == "car_data_loop":
    drv = laps.pick_driver("VER")
    r = [lap.get_car_data() for _, lap in drv.iterlaps()]
elif workload == "all_tel":
    r = session.fetch_all_laps_telemetry()
elif workload == "quali_split":
    r = laps.split_qualifying_sessions()
elif workload == "pick_driver":
    r = laps.pick_driver("VER")
print(f"{workload}: {(time.perf_counter() - t0) * 1000:.1f} ms", file=sys.stderr)
if isinstance(r, list):
    shape = [*(getattr(x, "shape", None) for x in r[:3]), len(r)]
elif isinstance(r, dict):
    shape = len(r)
else:
    shape = getattr(r, "shape", r)
print("result shape:", shape, file=sys.stderr)
print(
    sys.modules.get("numpy") is not None,
    sys.modules.get("pyarrow") is not None,
    sys.modules.get("pandas") is not None,
    file=sys.stderr,
)
