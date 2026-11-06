"""Gortyn G5: ``delphi_warm_ab.py`` with an extra site directory per arm.

The interleaved fresh-process warm A/B of the Delphi round (same pairs, same
order flips, same report), plus ``--control-site``/``--candidate-site``: a
directory put on ``PYTHONPATH`` ahead of the arm's source tree, so one arm
imports other upstream versions (``pip install --target DIR pandas==3.1.0
...``) than the interpreter's own site-packages. ``--bench`` points at the
main repository's ``tools/perf/delphi_warm_bench.py``.

Usage (from the repo root):
    python tools/perf/gortyn_g5_warm_ab.py --bench /path/to/delphi_warm_bench.py \
        --control-src /tmp/tif1-gortyn-control/src --control-cache /tmp/tif1-delphi-warm \
        --candidate-src src --candidate-cache /tmp/tif1-gortyn-warm-G5 \
        --candidate-site .newpkgs --lib pandas --pairs 6 --workloads laps,all_tel \
        --out .agents/perf-gortyn/G5_ab_versions_pandas.json
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


def _run_child(
    bench: str, workload: str, lib: str, src_dir: str, cache_dir: str, site: str | None
) -> dict:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", TIF1_CACHE_DIR=cache_dir)
    paths = [str(Path(site).resolve())] if site else []
    paths.append(str(Path(src_dir).resolve()))
    env["PYTHONPATH"] = os.pathsep.join(paths)
    cmd = [sys.executable, bench, "--child", workload, "--lib", lib]
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
    parser.add_argument("--bench", required=True)
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--control-cache", required=True)
    parser.add_argument("--candidate-cache", required=True)
    parser.add_argument("--control-site", default=None)
    parser.add_argument("--candidate-site", default=None)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--pairs", type=int, default=6)
    parser.add_argument("--workloads", default="laps,all_tel,notebook")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    arms = {
        "control": (args.control_src, args.control_cache, args.control_site),
        "candidate": (args.candidate_src, args.candidate_cache, args.candidate_site),
    }
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        recs: dict[str, list[dict]] = {"control": [], "candidate": []}
        for i in range(args.pairs):
            order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
            for arm in order:
                recs[arm].append(_run_child(args.bench, workload, args.lib, *arms[arm]))
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
                f"{k}={v['control_median']:.1f}->{v['candidate_median']:.1f}"
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
                    "control_site": args.control_site,
                    "candidate_site": args.candidate_site,
                    "summary": summary,
                },
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
