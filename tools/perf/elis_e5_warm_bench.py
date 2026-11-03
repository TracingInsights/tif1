"""Elis E5: the Delphi warm bench plus the ``load_then_N_drivers`` workloads.

A copy of ``delphi_warm_bench.py`` (every workload unchanged) with three
workloads for the lazy-row prefetch: ``load()`` with its defaults, then the
fastest lap's car data of the first N of VER, LEC, NOR, PIA
(``load_then_1_driver``, ``load_then_2_drivers``, ``load_then_4_drivers``).
``elis_e5_warm_ab.py`` (same folder) runs it.

Usage:
    TIF1_CACHE_DIR=/tmp/tif1-elis-warm-E5 python tools/perf/elis_e5_warm_bench.py \
        --lib pandas --reps 7 [--src-dir SRC] [--workloads laps,all_tel] [--out FILE]
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
LOAD_THEN_DRIVERS = {
    "load_then_1_driver": 1,
    "load_then_2_drivers": 2,
    "load_then_4_drivers": 4,
}
WORKLOADS = (
    "import_get_session",
    "laps",
    "load_only",
    "results",
    "state",
    "fastest_lap_tel",
    "driver_fastest_tel",
    "driver_tel",
    "car_data_loop",
    "all_tel",
    "notebook",
    "quali_split",
    *LOAD_THEN_DRIVERS,
)


def _child(workload: str, lib: str) -> None:
    out: dict[str, float] = {}
    t_proc = time.perf_counter()

    def step(name, fn):
        t0 = time.perf_counter()
        r = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        return r

    import tif1

    out["import"] = round((time.perf_counter() - t_proc) * 1000, 2)
    sess_name = "Qualifying" if workload == "quali_split" else SESSION
    session = step("get_session", lambda: tif1.get_session(YEAR, GP, sess_name, lib=lib))
    if workload == "import_get_session":
        pass
    elif workload == "laps":
        step("laps", lambda: session.laps)
    elif workload == "load_only":
        # fastf1 habit: load() with its defaults, then only laps and results.
        step("load", lambda: session.load())
        step("laps", lambda: session.laps)
        step("results", lambda: session.results)
    elif workload in LOAD_THEN_DRIVERS:
        drivers = ("VER", "LEC", "NOR", "PIA")[: LOAD_THEN_DRIVERS[workload]]
        step("load", lambda: session.load())
        step(
            "access",
            lambda: [session.laps.pick_driver(d).pick_fastest().get_car_data() for d in drivers],
        )
    elif workload == "results":
        step("laps", lambda: session.laps)
        step("results", lambda: session.results)
    elif workload == "state":
        step("laps", lambda: session.laps)
        step("weather", lambda: session.weather_data)
        step("rcm", lambda: session.race_control_messages)
        step("track_status", lambda: session.track_status)
        step("session_status", lambda: session.session_status)
        step("t0_date", lambda: session.t0_date)
    elif workload == "fastest_lap_tel":
        step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
    elif workload == "driver_fastest_tel":
        step("driver_fastest_tel", lambda: session.get_driver("VER").get_fastest_lap_tel())
    elif workload == "driver_tel":
        step("laps", lambda: session.laps)
        step("driver_tel", lambda: session.laps.pick_driver("VER").telemetry)
    elif workload == "car_data_loop":
        step("laps", lambda: session.laps)
        drv = session.laps.pick_driver("VER")
        step("car_data_loop", lambda: [lap.get_car_data() for _, lap in drv.iterlaps()])
    elif workload == "all_tel":
        step("laps", lambda: session.laps)
        step("all_tel", lambda: session.fetch_all_laps_telemetry())
    elif workload == "notebook":
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
        step("iterlaps_tel_loop", lambda: [lap.telemetry for _, lap in laps.iterlaps()])
        if lib == "polars":
            step("iloc_200", lambda: [laps.row(i, named=True) for i in range(200)])
        else:
            step("iloc_200", lambda: [laps.iloc[i] for i in range(200)])
        step(
            "driver_merged_x4",
            lambda: [laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")],
        )
        fastest = []
        drivers = laps["Driver"].unique()
        for d in drivers if lib == "pandas" else drivers.to_list():
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
                if lib == "polars":
                    tel.slice_by_time(tel["Time"][0], tel["Time"][len(tel) // 2])
                else:
                    tel.slice_by_time(tel["Time"].iloc[0], tel["Time"].iloc[len(tel) // 2])
                tels.append(tel)
            tels[0].merge_channels(tels[1])
            return tels

        step("telemetry_analysis_4", analysis)
        step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
        step(
            "driver_fastest_lap_x4",
            lambda: [session.get_driver(d).get_fastest_lap() for d in ("VER", "LEC", "NOR", "PIA")],
        )
        step("results", lambda: session.results)
        step("weather", lambda: session.weather_data)
    elif workload == "quali_split":
        step("laps", lambda: session.laps)
        step("split", lambda: session.laps.split_qualifying_sessions())
        step("results", lambda: session.results)
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    print(json.dumps(out))


def _run_child(workload: str, lib: str, src_dir: str | None) -> dict:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true")
    if src_dir:
        env["PYTHONPATH"] = str(Path(src_dir).resolve())
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--reps", type=int, default=7)
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--out", default=None)
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib)
        return
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        recs = [_run_child(workload, args.lib, args.src_dir) for _ in range(args.reps)]
        phases = {}
        for key in recs[0]:
            vals = [r[key] for r in recs]
            phases[key] = {"median": round(statistics.median(vals), 1), "min": round(min(vals), 1)}
        summary[workload] = phases
        line = "  ".join(f"{k}={v['median']:.0f}/{v['min']:.0f}" for k, v in phases.items())
        print(f"[{args.lib}] {workload:20s} {line}", flush=True)
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {"lib": args.lib, "label": args.label, "reps": args.reps, "summary": summary},
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
