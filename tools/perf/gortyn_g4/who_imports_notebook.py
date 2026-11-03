"""Gortyn G4 probe: which notebook step first imports numpy on a polars session.

Runs the ``notebook`` workload of ``delphi_warm_bench.py`` step by step (plus
``iterlaps(require=...)`` and the rest of the ``pick_*`` family) and prints the
step and the stack of numpy's first import.

Usage (from the repo root):
    TIF1_CACHE_DIR=/tmp/tif1-gortyn-warm-G4 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/gortyn_g4/who_imports_notebook.py [--stack]
"""

from __future__ import annotations

import sys
import time
import traceback

STEP = ["import"]
SHOW_STACK = "--stack" in sys.argv


class _Finder:
    @staticmethod
    def find_spec(fullname, _path=None, _target=None):
        if fullname == "numpy" and "numpy" not in sys.modules:
            print(f"=== numpy first imported during step {STEP[0]!r}", file=sys.stderr)
            if SHOW_STACK:
                traceback.print_stack(limit=14, file=sys.stderr)


sys.meta_path.insert(0, _Finder)

import tif1  # noqa: E402

BLOCK = "--block" in sys.argv


def step(name, fn):
    STEP[0] = name
    t0 = time.perf_counter()
    try:
        r = fn()
    except BaseException:  # ImportError, or pyo3's PanicException from to_numpy
        # --block: report the tif1 frame that needed numpy and carry on.
        frames = [f for f in traceback.extract_tb(sys.exc_info()[2]) if "/tif1/" in f.filename]
        where = f"{frames[-1].filename.split('/tif1/')[-1]}:{frames[-1].lineno}" if frames else "?"
        print(f"{name:28s} NEEDS NUMPY at {where} ({frames[-1].line if frames else ''})")
        return None
    print(f"{name:28s} {(time.perf_counter() - t0) * 1000:8.2f} ms", file=sys.stderr)
    return r


session = step(
    "get_session", lambda: tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
)
step("load", lambda: session.load(weather=True, messages=True))
laps = session.laps
if BLOCK:
    sys.modules["numpy"] = None  # type: ignore[assignment]
step("pick_driver", lambda: laps.pick_driver("VER"))
step("pick_drivers", lambda: laps.pick_drivers(["VER", "LEC"]))
step("pick_quicklaps", lambda: laps.pick_quicklaps())
step("pick_fastest", lambda: laps.pick_fastest())
step("pick_laps", lambda: laps.pick_laps([5, 6]))
step("pick_compounds", lambda: laps.pick_compounds(["SOFT"]))
step("pick_wo_box", lambda: laps.pick_wo_box())
step("pick_track_status", lambda: laps.pick_track_status("1"))
step("pick_drivers_x6", lambda: laps.pick_drivers(["VER", "LEC", "NOR", "PIA", "HAM", "RUS"]))
step("pick_team", lambda: laps.pick_team("McLaren"))
step("pick_teams", lambda: laps.pick_teams(["McLaren", "Ferrari"]))
step("pick_box_laps", lambda: laps.pick_box_laps())
step("pick_not_deleted", lambda: laps.pick_not_deleted())
step("pick_accurate", lambda: laps.pick_accurate())
step("pick_lap", lambda: laps.pick_lap(5))
step("pick_laps_slice", lambda: laps.pick_laps(slice(3, 9)))
step("iterlaps_require", lambda: list(laps.iterlaps(require=["LapTime", "PitInTime"])))
step("iterlaps_tel_loop", lambda: [lap.telemetry for _, lap in laps.iterlaps()])
step("iloc_200", lambda: [laps.row(i, named=True) for i in range(200)])
step(
    "driver_merged_x4",
    lambda: [laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")],
)
fastest = []
for d in laps["Driver"].unique(maintain_order=True).to_list():
    lap = step(f"pick_fastest[{d}]", lambda d=d: laps.pick_driver(d).pick_fastest())
    if lap is not None:
        fastest.append(lap)
    if len(fastest) == 4:
        break
step("tel_add_distance", lambda: fastest[0].get_telemetry().add_distance())
step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
step(
    "driver_fastest_lap_x4",
    lambda: [session.get_driver(d).get_fastest_lap() for d in ("VER", "LEC", "NOR", "PIA")],
)
step("results", lambda: session.results)
step("weather", lambda: session.weather_data)
print("numpy loaded at exit:", sys.modules.get("numpy") is not None, file=sys.stderr)
