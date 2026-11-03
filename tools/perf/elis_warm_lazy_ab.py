"""Elis E1: warm eager-vs-lazy A/B on one tree (``TIF1_LOAD_TELEMETRY_LAZY`` per arm).

A copy of ``delphi_warm_ab.py`` whose two arms run the same tree and cache
and differ only in the environment: the control arm loads eagerly, the
candidate arm sets ``TIF1_LOAD_TELEMETRY_LAZY=true`` so ``load()`` means
``load(telemetry="lazy")``. Workloads are those of ``delphi_warm_bench.py``.

Usage:
    python tools/perf/elis_warm_lazy_ab.py --src /tmp/tif1-elis/E1/src \
        --cache /tmp/tif1-elis-warm-E1 --bench /path/to/delphi_warm_bench.py \
        --lib polars --pairs 6 --workloads load_only,notebook [--out FILE]
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


def _run_child(bench: str, workload: str, lib: str, src: str, cache: str, lazy: bool) -> dict:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", TIF1_CACHE_DIR=cache)
    env["PYTHONPATH"] = str(Path(src).resolve())
    env["TIF1_LOAD_TELEMETRY_LAZY"] = "true" if lazy else "false"
    cmd = [sys.executable, bench, "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} (lazy={lazy}) failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--bench", required=True)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--pairs", type=int, default=6)
    parser.add_argument("--workloads", default="load_only,notebook")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        recs: dict[bool, list[dict]] = {False: [], True: []}
        for i in range(args.pairs):
            for lazy in (False, True) if i % 2 == 0 else (True, False):
                recs[lazy].append(
                    _run_child(args.bench, workload, args.lib, args.src, args.cache, lazy)
                )
        phases: dict[str, dict] = {}
        for key in recs[False][0]:
            c = [r[key] for r in recs[False]]
            d = [r[key] for r in recs[True]]
            wins = sum(1 for a, b in zip(c, d, strict=True) if b < a)
            phases[key] = {
                "control_median": round(statistics.median(c), 1),
                "candidate_median": round(statistics.median(d), 1),
                "candidate_faster_pairs": f"{wins}/{args.pairs}",
            }
        summary[workload] = phases
        w = phases["process_wall"]
        print(
            f"[{args.lib}] {workload:20s} wall eager {w['control_median']:.0f}"
            f"  lazy {w['candidate_median']:.0f}  faster {w['candidate_faster_pairs']}  | "
            + "  ".join(
                f"{k}={v['control_median']:.0f}->{v['candidate_median']:.0f}"
                for k, v in phases.items()
                if k not in ("process_wall", "in_process_total", "import")
            ),
            flush=True,
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps({"lib": args.lib, "pairs": args.pairs, "summary": summary}, indent=1) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
