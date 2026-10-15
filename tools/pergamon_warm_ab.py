"""Interleaved fresh-process warm A/B: control tree vs candidate tree (Pergamon series).

Each pair runs ``tools/warm_cache_benchmark.py`` once per arm (ABAB...), each
arm with its own primed cache directory, and prints per-phase medians plus
the per-pair win count.

Usage:
    uv run python tools/pergamon_warm_ab.py --control-src /tmp/tif1-control/src \
        --control-cache /tmp/tif1-warm-cache --candidate-cache /tmp/tif1-warm-p3 \
        --pairs 8 [--lib polars] [--out .agents/perf-pergamon/warm_ab.json]
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

PHASES = ("get_session_s", "laps_s", "telemetry_s", "total_s")


def run_arm(src_dir: str | None, cache_dir: str, lib: str) -> dict:
    env = dict(os.environ, TIF1_WARM_CACHE_DIR=cache_dir)
    cmd = [sys.executable, "tools/warm_cache_benchmark.py", "--lib", lib]
    if src_dir:
        cmd += ["--src-dir", src_dir]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env).stdout
    return json.loads(out.strip().splitlines()[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--control-cache", required=True)
    parser.add_argument("--candidate-cache", required=True)
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    pairs: list[dict] = []
    for i in range(args.pairs):
        order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
        result: dict = {}
        for arm in order:
            if arm == "control":
                result[arm] = run_arm(args.control_src, args.control_cache, args.lib)
            else:
                result[arm] = run_arm(None, args.candidate_cache, args.lib)
        pairs.append(result)
        print(
            f"pair {i + 1}: control total {result['control']['total_s']:.3f} "
            f"tel {result['control']['telemetry_s']:.3f} | candidate total "
            f"{result['candidate']['total_s']:.3f} tel {result['candidate']['telemetry_s']:.3f}",
            flush=True,
        )
    summary = {}
    for phase in PHASES:
        ctl = [p["control"][phase] for p in pairs]
        cand = [p["candidate"][phase] for p in pairs]
        wins = sum(1 for p in pairs if p["candidate"][phase] < p["control"][phase])
        summary[phase] = {
            "control_median": round(statistics.median(ctl), 4),
            "control_min": round(min(ctl), 4),
            "candidate_median": round(statistics.median(cand), 4),
            "candidate_min": round(min(cand), 4),
            "candidate_faster_pairs": f"{wins}/{len(pairs)}",
        }
        print(
            f"{phase:14s} control {summary[phase]['control_median']:.4f}/{summary[phase]['control_min']:.4f}"
            f"  candidate {summary[phase]['candidate_median']:.4f}/{summary[phase]['candidate_min']:.4f}"
            f"  faster {summary[phase]['candidate_faster_pairs']}"
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps({"lib": args.lib, "pairs": pairs, "summary": summary}, indent=1) + "\n"
        )


if __name__ == "__main__":
    main()
