"""Elis E10: fresh-process cold (empty cache, live CDN) workload benchmark.

A copy of E1's ``elis_cold_bench.py`` (itself a copy of ``delphi_cold_bench.py``)
with per-arm environment variables: ``--control-env K=V`` / ``--candidate-env
K=V`` (repeatable) set config keys such as ``TIF1_LAZY_TELEMETRY_LOOKAHEAD``
for one arm only, so stream variants of the same tree can run interleaved.

Workloads (2025 Bahrain race): ``laps``, ``fastest_lap_tel``, ``driver_tel``,
``notebook``, ``all_tel``, ``notebook_4drivers`` (``load(weather, messages)``,
four drivers' merged telemetry, their fastest laps' car data, a 4-lap analysis
and the results), ``notebook_lazy`` (the same after ``load(telemetry="lazy")``),
``iterlaps_all`` / ``iterlaps_all_lazy`` (every lap's ``lap.telemetry``),
``fetch_all_lazy`` (a lazy load, one driver touched, then
``fetch_all_laps_telemetry()``). ``--control-workload`` runs another workload
in the control arm.

Each run is a fresh interpreter with a throwaway cache directory; phases are
timed in-process and the parent measures the whole-process wall time. The
parent interleaves control/candidate (ABAB..., the order flipping every pair).

Usage:
    python tools/perf/elis_e10_cold_bench.py --lib pandas --reps 5 \
        --workloads iterlaps_all_lazy --control-workload iterlaps_all \
        --control-src /tmp/ctl/src --candidate-src src \
        [--candidate-env TIF1_LAZY_TELEMETRY_LOOKAHEAD=-1] [--out FILE]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
WORKLOADS = ("laps", "fastest_lap_tel", "driver_tel", "notebook", "all_tel")
FOUR_DRIVERS = ("VER", "LEC", "NOR", "PIA")


def _notebook_4drivers(session, lib: str, step, telemetry) -> None:
    step("load", lambda: session.load(telemetry=telemetry, weather=True, messages=True))
    laps = session.laps
    for driver in FOUR_DRIVERS:
        step(f"merged_{driver}", lambda d=driver: laps.pick_driver(d).telemetry)
    fastest = [laps.pick_driver(d).pick_fastest() for d in FOUR_DRIVERS]
    step("fastest_car_data_x4", lambda: [lap.get_car_data() for lap in fastest])

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
    step("results", lambda: session.results)


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
    session = step("get_session", lambda: tif1.get_session(YEAR, GP, SESSION, lib=lib))
    if workload == "laps":
        step("laps", lambda: session.laps)
    elif workload == "fastest_lap_tel":
        step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
    elif workload == "driver_tel":
        step("laps", lambda: session.laps)
        step("driver_tel", lambda: session.laps.pick_driver("VER").telemetry)
    elif workload == "all_tel":
        step("laps", lambda: session.laps)
        step("all_tel", lambda: session.fetch_all_laps_telemetry())
    elif workload == "notebook":
        step("load", lambda: session.load(weather=True, messages=True))
        laps = session.laps
        step("picks", lambda: [laps.pick_driver("VER"), laps.pick_quicklaps(), laps.pick_fastest()])
        step("driver_tel_x2", lambda: [laps.pick_driver(d).telemetry for d in ("VER", "LEC")])
        step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
        step("results", lambda: session.results)
        step("weather", lambda: session.weather_data)
    elif workload == "notebook_4drivers":
        _notebook_4drivers(session, lib, step, True)
    elif workload == "notebook_lazy":
        _notebook_4drivers(session, lib, step, "lazy")
    elif workload in ("iterlaps_all", "iterlaps_all_lazy"):
        # The lazy load's worst case: every lap touched, one driver at a time.
        lazy = workload == "iterlaps_all_lazy"
        step("load", lambda: session.load(telemetry="lazy" if lazy else True))
        laps = session.laps
        step("iterlaps_tel_loop", lambda: [lap.telemetry for _, lap in laps.iterlaps()])
    elif workload == "fetch_all_lazy":
        step("load", lambda: session.load(telemetry="lazy"))
        laps = session.laps
        step("driver_tel", lambda: laps.pick_driver("VER").telemetry)
        step("all_tel", lambda: session.fetch_all_laps_telemetry())
    else:
        raise SystemExit(f"unknown workload {workload!r}")
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    print(json.dumps(out), flush=True)


def _run_child(workload: str, lib: str, src_dir: str | None, extra_env: dict[str, str]) -> dict:
    cache_dir = tempfile.mkdtemp(prefix="tif1-elis-cold-")
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir)
    env.pop("TIF1_OFFLINE_MODE", None)
    env.update(extra_env)
    if src_dir:
        env["PYTHONPATH"] = str(Path(src_dir).resolve())
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    shutil.rmtree(cache_dir, ignore_errors=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    # Time from the last in-process step to process exit (atexit joins, teardown).
    rec["exit_tail"] = round(wall - rec["in_process_total"], 2)
    rec["warnings_403"] = proc.stderr.count("403")
    rec["stderr_warnings"] = proc.stderr.count("WARNING")
    return rec


def _summarize(recs: list[dict]) -> dict:
    phases = {}
    for key in recs[0]:
        vals = [r[key] for r in recs if key in r]
        phases[key] = {"median": round(statistics.median(vals), 1), "min": round(min(vals), 1)}
    return phases


def _env_pairs(values: list[str]) -> dict[str, str]:
    return dict(value.split("=", 1) for value in values)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--control-src", default=None)
    parser.add_argument("--candidate-src", default=None)
    parser.add_argument("--control-workload", default=None)
    parser.add_argument("--control-env", action="append", default=[])
    parser.add_argument("--candidate-env", action="append", default=[])
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--out", default=None)
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib)
        return
    control_env = _env_pairs(args.control_env)
    candidate_env = _env_pairs(args.candidate_env)
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        if args.control_src:
            control_workload = args.control_workload or workload
            ctl, cand = [], []
            for i in range(args.reps):
                order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
                for arm in order:
                    if arm == "control":
                        ctl.append(
                            _run_child(control_workload, args.lib, args.control_src, control_env)
                        )
                    else:
                        cand.append(
                            _run_child(workload, args.lib, args.candidate_src, candidate_env)
                        )
            wins = sum(
                1 for a, b in zip(ctl, cand, strict=True) if b["process_wall"] < a["process_wall"]
            )
            summary[workload] = {
                "control_workload": control_workload,
                "control_env": control_env,
                "candidate_env": candidate_env,
                "control": _summarize(ctl),
                "candidate": _summarize(cand),
                "candidate_faster_pairs": f"{wins}/{args.reps}",
                "control_runs": ctl,
                "candidate_runs": cand,
            }
            c, d = (
                summary[workload]["control"]["process_wall"],
                summary[workload]["candidate"]["process_wall"],
            )
            print(
                f"[{args.lib}] {workload:18s} (control {control_workload}) control "
                f"{c['median']:.0f}/{c['min']:.0f}  candidate {d['median']:.0f}/{d['min']:.0f}"
                f"  faster {wins}/{args.reps}",
                flush=True,
            )
        else:
            recs = [
                _run_child(workload, args.lib, args.candidate_src, candidate_env)
                for _ in range(args.reps)
            ]
            summary[workload] = _summarize(recs)
            line = "  ".join(
                f"{k}={v['median']:.0f}/{v['min']:.0f}" for k, v in summary[workload].items()
            )
            print(f"[{args.lib}] {workload:18s} {line}", flush=True)
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
