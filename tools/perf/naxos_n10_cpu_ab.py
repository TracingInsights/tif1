"""Naxos N10: interleaved control/candidate A/B reporting wall AND main-thread CPU per phase.

A copy of ``delphi_warm_bench.py``'s workloads (fresh process, offline, warm
cache) that also records ``time.thread_time()`` per phase. On a box shared
with other agents (load 30-60 on 32 cores) the walls swing by 2x while the
main thread's CPU time, which is where cyclic-GC passes run, stays readable.

    TIF1_CACHE_DIR=/tmp/naxos/warm-N10 python tools/perf/naxos_n10_cpu_ab.py \
        --control-src /tmp/naxos/control/src --candidate-src src --lib pandas \
        --workloads laps,all_tel,car_data_loop,notebook --pairs 12 --out out.json
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from statistics import median

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"


def _child(workload: str, lib: str) -> None:
    out: dict[str, float] = {}
    t_proc = time.perf_counter()
    c_proc = time.thread_time()

    def step(name, fn):
        t0 = time.perf_counter()
        c0 = time.thread_time()
        r = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        out[name + ".cpu"] = round((time.thread_time() - c0) * 1000, 2)
        return r

    import tif1

    out["import"] = round((time.perf_counter() - t_proc) * 1000, 2)
    session = step("get_session", lambda: tif1.get_session(YEAR, GP, SESSION, lib=lib))
    if workload == "laps":
        step("laps", lambda: session.laps)
    elif workload == "all_tel":
        step("laps", lambda: session.laps)
        step("all_tel", session.fetch_all_laps_telemetry)
    elif workload == "car_data_loop":
        step("laps", lambda: session.laps)
        drv = session.laps.pick_driver("VER")
        step("car_data_loop", lambda: [lap.get_car_data() for _, lap in drv.iterlaps()])
    elif workload == "driver_tel":
        step("laps", lambda: session.laps)
        step("driver_tel", lambda: session.laps.pick_driver("VER").telemetry)
    elif workload == "notebook":
        step("load", lambda: session.load(weather=True, messages=True))
        laps = session.laps
        step("iterlaps_tel_loop", lambda: [lap.telemetry for _, lap in laps.iterlaps()])
        step(
            "driver_merged_x4",
            lambda: [laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")],
        )

        def analysis():
            tels = []
            for d in ("VER", "LEC", "NOR", "PIA"):
                tel = laps.pick_driver(d).pick_fastest().get_telemetry().add_distance()
                tel.resample_channels(rule="0.1s")
                tels.append(tel)
            return tels

        step("telemetry_analysis_4", analysis)
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    out["in_process_total.cpu"] = round((time.thread_time() - c_proc) * 1000, 2)
    print(json.dumps(out))


def _run(src: str, workload: str, lib: str) -> dict:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", PYTHONPATH=str(Path(src).resolve()))
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-2000:])
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--control-src")
    parser.add_argument("--candidate-src")
    parser.add_argument("--workloads", default="laps,all_tel")
    parser.add_argument("--pairs", type=int, default=12)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib)
        return
    summary: dict[str, dict[str, dict[str, object]]] = {}
    raw: dict[str, dict[str, list[dict]]] = {}
    for workload in args.workloads.split(","):
        arms: dict[str, list[dict]] = {"control": [], "candidate": []}
        for i in range(args.pairs):
            order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
            for arm in order:
                src = args.control_src if arm == "control" else args.candidate_src
                arms[arm].append(_run(src, workload, args.lib))
        raw[workload] = arms
        phases = list(arms["control"][0])
        summary[workload] = {}
        for phase in phases:
            c = [r[phase] for r in arms["control"]]
            k = [r[phase] for r in arms["candidate"]]
            won = sum(b < a for a, b in zip(c, k, strict=True))
            summary[workload][phase] = {
                "control_median": median(c),
                "candidate_median": median(k),
                "candidate_faster_pairs": f"{won}/{args.pairs}",
            }
            print(
                f"[{args.lib}] {workload:14s} {phase:28s} {median(c):8.1f} -> {median(k):8.1f}"
                f"  {won}/{args.pairs}",
                flush=True,
            )
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {"lib": args.lib, "pairs": args.pairs, "summary": summary, "raw": raw}, indent=1
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
