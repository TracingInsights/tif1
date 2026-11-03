"""Delphi round: interleaved fresh-process warm A/B, control tree vs candidate tree.

Each pair runs every workload of ``delphi_warm_bench.py`` once per arm
(ABAB..., the order flipped every pair) against each arm's own primed cache
directory (``TIF1_CACHE_DIR``), offline. Reports, per workload and phase,
the control/candidate medians and the number of pairs the candidate won on
the whole-process wall time.

Usage:
    uv run python tools/perf/delphi_warm_ab.py --control-src /tmp/tif1-delphi-control/src \
        --candidate-src /tmp/tif1-delphi/D4/src --control-cache /tmp/tif1-delphi-warm \
        --candidate-cache /tmp/tif1-delphi-warm-D4 --lib pandas --pairs 6 \
        --workloads laps,all_tel [--out .agents/perf-delphi/D4_ab_pandas.json]
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

BENCH = Path(__file__).resolve().parent / "delphi_warm_bench.py"


def _run_child(workload: str, lib: str, src_dir: str, cache_dir: str) -> dict:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", TIF1_CACHE_DIR=cache_dir)
    env["PYTHONPATH"] = str(Path(src_dir).resolve())
    cmd = [sys.executable, str(BENCH), "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} ({src_dir}) failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--control-cache", required=True)
    parser.add_argument("--candidate-cache", required=True)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--pairs", type=int, default=6)
    parser.add_argument("--workloads", default="laps,all_tel,notebook")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    arms = {
        "control": (args.control_src, args.control_cache),
        "candidate": (args.candidate_src, args.candidate_cache),
    }
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        recs: dict[str, list[dict]] = {"control": [], "candidate": []}
        for i in range(args.pairs):
            order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
            for arm in order:
                recs[arm].append(_run_child(workload, args.lib, *arms[arm]))
        phases: dict[str, dict] = {}
        for key in recs["control"][0]:
            if key not in recs["candidate"][0]:
                continue
            c = [r[key] for r in recs["control"]]
            d = [r[key] for r in recs["candidate"]]
            wins = sum(1 for a, b in zip(c, d, strict=True) if b < a)
            phases[key] = {
                "control_median": round(statistics.median(c), 1),
                "candidate_median": round(statistics.median(d), 1),
                "control_min": round(min(c), 1),
                "candidate_min": round(min(d), 1),
                "candidate_faster_pairs": f"{wins}/{args.pairs}",
            }
        summary[workload] = phases
        w = phases["process_wall"]
        print(
            f"[{args.lib}] {workload:20s} wall control {w['control_median']:.0f}/{w['control_min']:.0f}"
            f"  candidate {w['candidate_median']:.0f}/{w['candidate_min']:.0f}"
            f"  faster {w['candidate_faster_pairs']}  | "
            + "  ".join(
                f"{k}={v['control_median']:.0f}->{v['candidate_median']:.0f}"
                for k, v in phases.items()
                if k not in ("process_wall", "in_process_total", "import")
            ),
            flush=True,
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {
                    "lib": args.lib,
                    "pairs": args.pairs,
                    "control_src": args.control_src,
                    "candidate_src": args.candidate_src,
                    "summary": summary,
                },
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
