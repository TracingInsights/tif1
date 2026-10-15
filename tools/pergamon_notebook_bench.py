"""Fresh-process "analysis notebook" benchmark on a warm cache (Pergamon wave 2).

Times the steps a typical notebook runs after the data is cached: session
load, lap selections, per-lap loops, driver telemetry, Telemetry analysis
methods and the Driver / fastest-lap helpers. Prints one JSON line of
milliseconds per step. Run offline (``TIF1_OFFLINE_MODE=true``) so no arm
touches the network.

Usage:
    TIF1_CACHE_DIR=... TIF1_OFFLINE_MODE=true uv run python tools/pergamon_notebook_bench.py \
        [--src-dir SRC]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=None)
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    t_all = time.perf_counter()
    import tif1

    out: dict[str, float] = {}

    def step(name, fn):
        t0 = time.perf_counter()
        result = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        return result

    session = step("get_session", lambda: tif1.get_session(2026, "Monaco Grand Prix", "Race"))
    step("load", lambda: session.load(weather=True, messages=True))
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
    step("iterlaps_telemetry_loop", lambda: [lap.telemetry for _, lap in laps.iterlaps()])
    step("iloc_200", lambda: [laps.iloc[i] for i in range(200)])
    step(
        "driver_merged_x4",
        lambda: [laps.pick_driver(d).telemetry for d in ("ANT", "VER", "NOR", "PIA")],
    )

    fastest = []
    for d in laps["Driver"].unique():
        lap = laps.pick_driver(d).pick_fastest()
        if lap is not None:
            fastest.append(lap)
        if len(fastest) == 4:
            break

    def analysis():
        tels = []
        for lap in fastest:
            tel = lap.get_telemetry().add_distance()
            tel = tel.add_differential_distance()
            tel.resample_channels(rule="0.1s")
            tel.fill_missing()
            tel.slice_by_time(tel["Time"].iloc[0], tel["Time"].iloc[len(tel) // 2])
            tels.append(tel)
        tels[0].merge_channels(tels[1])
        return tels

    step("telemetry_analysis_4_drivers", analysis)
    step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
    step(
        "driver_fastest_lap_x4",
        lambda: [session.get_driver(d).get_fastest_lap() for d in ("ANT", "VER", "NOR", "PIA")],
    )
    session2 = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    step("new_session_driver_laps", lambda: session2.get_driver("LEC").laps)
    out["total_wall"] = round((time.perf_counter() - t_all) * 1000, 2)
    print(json.dumps(out))


if __name__ == "__main__":
    main()
