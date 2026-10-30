"""Warm-cache phase profile + notebook-workflow micro-benchmarks (Argos series).

Loads the Monaco race from the primed persistent cache (``TIF1_CACHE_DIR``
must point at it *before* importing tif1) and times, in a fresh process:

- get_session / laps / fetch_all_laps_telemetry (the warm pipeline), with an
  optional cProfile of the telemetry phase (``--profile``)
- the FastF1-idiom notebook workflow on real data: get_driver, get_lap,
  lap.telemetry, get_car_data().add_distance(), add_relative_distance(),
  get_telemetry() (driver-ahead), pick_driver().pick_fastest(),
  pick_driver().telemetry, pick_quicklaps, results, weather, race control

Usage:
    TIF1_CACHE_DIR=/tmp/tif1-warm-cache uv run python tools/perf/argos_profile_warm.py [--src-dir src] [--profile]
"""

from __future__ import annotations

import argparse
import cProfile
import io
import json
import os
import pstats
import sys
import time
from pathlib import Path


def _t(fn, reps: int = 1):
    best = 1e9
    out = None
    for _ in range(reps):
        t0 = time.perf_counter()
        out = fn()
        best = min(best, time.perf_counter() - t0)
    return best, out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--driver", default="VER")
    parser.add_argument("--skip-fetch-all", action="store_true")
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    os.environ.setdefault("TIF1_CACHE_DIR", "/tmp/tif1-warm-cache")

    res: dict[str, float] = {}
    t0 = time.perf_counter()
    import tif1

    res["import_tif1_s"] = time.perf_counter() - t0
    t0 = time.perf_counter()
    session = tif1.get_session(args.year, args.gp, args.session)
    res["get_session_s"] = time.perf_counter() - t0
    t0 = time.perf_counter()
    laps = session.laps
    res["laps_s"] = time.perf_counter() - t0

    pr = cProfile.Profile() if args.profile else None
    if pr:
        pr.enable()
    t0 = time.perf_counter()
    tel = {} if args.skip_fetch_all else session.fetch_all_laps_telemetry()
    res["telemetry_s"] = time.perf_counter() - t0
    if pr:
        pr.disable()
    res["telemetry_frames"] = len(tel)
    res["total_s"] = res["get_session_s"] + res["laps_s"] + res["telemetry_s"]

    # --- notebook workflow (ms) ---
    d = args.driver
    res["results_ms"], _ = _t(lambda: session.results)
    res["weather_ms"], _ = _t(lambda: session.weather_data)
    res["rcm_ms"], _ = _t(lambda: session.race_control_messages)
    res["get_driver_first_ms"], drv = _t(lambda: session.get_driver(d))
    res["driver_laps_first_ms"], dlaps = _t(lambda: drv.laps)
    res["get_driver_repeat_ms"], _ = _t(lambda: session.get_driver(d).laps, reps=5)
    lap_no = int(dlaps["LapNumber"].dropna().iloc[min(9, len(dlaps) - 1)])
    res["get_lap_ms"], lap = _t(lambda: drv.get_lap(lap_no), reps=20)
    res["lap_telemetry_ms"], _ = _t(lambda: lap.telemetry, reps=20)
    res["get_car_data_add_distance_ms"], _ = _t(lambda: lap.get_car_data().add_distance(), reps=20)
    res["add_relative_distance_ms"], _ = _t(
        lambda: lap.get_car_data().add_relative_distance(), reps=20
    )
    res["get_telemetry_driver_ahead_ms"], _ = _t(lambda: lap.get_telemetry(), reps=20)
    res["pick_driver_ms"], dl = _t(lambda: laps.pick_driver(d), reps=20)
    res["pick_fastest_ms"], fl = _t(lambda: dl.pick_fastest(), reps=20)
    res["pick_quicklaps_ms"], _ = _t(lambda: dl.pick_quicklaps(), reps=20)
    res["pick_wo_box_ms"], _ = _t(lambda: dl.pick_wo_box(), reps=20)
    res["fastest_lap_telemetry_ms"], _ = _t(lambda: fl.telemetry, reps=20)
    res["fastest_lap_get_car_data_add_distance_ms"], _ = _t(
        lambda: fl.get_car_data().add_distance(), reps=20
    )
    res["driver_laps_telemetry_first_ms"], _ = _t(lambda: dl.telemetry)
    res["driver_laps_telemetry_repeat_ms"], _ = _t(lambda: dl.telemetry, reps=5)
    res["iterlaps_ms"], _ = _t(lambda: list(dl.iterlaps()), reps=3)
    res["get_fastest_laps_tels_ms"], _ = _t(lambda: session.get_fastest_laps_tels(by_driver=True))
    res["get_fastest_lap_tel_ms"], _ = _t(lambda: session.get_fastest_lap_tel(), reps=3)
    res["circuit_info_ms"], _ = _t(lambda: session.get_circuit_info())
    for k in list(res):
        if k.endswith("_ms"):
            res[k] = round(res[k] * 1000, 3)
        elif k.endswith("_s"):
            res[k] = round(res[k], 4)
    print(json.dumps(res))
    if pr:
        s = io.StringIO()
        pstats.Stats(pr, stream=s).sort_stats("cumtime").print_stats(25)
        print(s.getvalue()[:6000], file=sys.stderr)


if __name__ == "__main__":
    main()
