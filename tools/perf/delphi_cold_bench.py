"""Delphi round: fresh-process cold (empty cache, live CDN) workload benchmark.

Each run is a fresh interpreter with a throwaway cache directory; phases are
timed in-process and the parent measures the whole-process wall time. The
parent interleaves control/candidate trees when ``--control-src`` is given
(ABAB...) so CDN drift hits both arms.

Usage:
    uv run python tools/perf/delphi_cold_bench.py --lib pandas --reps 5 \
        [--workloads laps,fastest_lap_tel,driver_tel,notebook] [--control-src /tmp/ctl/src] [--out FILE]
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
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    print(json.dumps(out))


def _run_child(workload: str, lib: str, src_dir: str | None) -> dict:
    cache_dir = tempfile.mkdtemp(prefix="tif1-delphi-cold-")
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir)
    env.pop("TIF1_OFFLINE_MODE", None)
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
    rec["warnings_403"] = proc.stderr.count("403")
    return rec


def _summarize(recs: list[dict]) -> dict:
    phases = {}
    for key in recs[0]:
        vals = [r[key] for r in recs]
        phases[key] = {"median": round(statistics.median(vals), 1), "min": round(min(vals), 1)}
    return phases


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--control-src", default=None)
    parser.add_argument("--candidate-src", default=None)
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--out", default=None)
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib)
        return
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        if args.control_src:
            ctl, cand = [], []
            for i in range(args.reps):
                order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
                for arm in order:
                    src = args.control_src if arm == "control" else args.candidate_src
                    (ctl if arm == "control" else cand).append(_run_child(workload, args.lib, src))
            wins = sum(
                1 for a, b in zip(ctl, cand, strict=True) if b["process_wall"] < a["process_wall"]
            )
            summary[workload] = {
                "control": _summarize(ctl),
                "candidate": _summarize(cand),
                "candidate_faster_pairs": f"{wins}/{args.reps}",
            }
            c, d = (
                summary[workload]["control"]["process_wall"],
                summary[workload]["candidate"]["process_wall"],
            )
            print(
                f"[{args.lib}] {workload:16s} control {c['median']:.0f}/{c['min']:.0f}"
                f"  candidate {d['median']:.0f}/{d['min']:.0f}  faster {wins}/{args.reps}",
                flush=True,
            )
        else:
            recs = [_run_child(workload, args.lib, args.candidate_src) for _ in range(args.reps)]
            summary[workload] = _summarize(recs)
            line = "  ".join(
                f"{k}={v['median']:.0f}/{v['min']:.0f}" for k, v in summary[workload].items()
            )
            print(f"[{args.lib}] {workload:16s} {line}", flush=True)
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
