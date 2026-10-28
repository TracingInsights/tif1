"""Interleaved fresh-process chart A/B (Pergamon Q14): control tree vs candidate tree.

Each pair runs ``tools/q14_chart_probe.py`` once per arm (alternating which
arm goes first) and reports medians plus per-pair win counts for the
"import + first chart + savefig" wall and the second chart in the process.

Usage:
    uv run python tools/q14_chart_ab.py --control-src /tmp/tif1-pergamon-control/src \
        --cache /tmp/tif1-q-Q14/a --pairs 8 --charts speed_traces,gear_shifts \
        [--out /tmp/tif1-q-Q14/ab.json]
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

PHASES = ("import_charts_s", "chart0_s", "first_total_s", "chart1_s", "total_s")


def run_arm(src_dir: str | None, cache_dir: str, charts: str) -> dict:
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir, MPLBACKEND="Agg", PYTHONWARNINGS="ignore")
    cmd = [sys.executable, "tools/q14_chart_probe.py", "--charts", charts]
    if src_dir:
        cmd += ["--src-dir", src_dir]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env).stdout
    return json.loads(out.strip().splitlines()[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--charts", default="speed_traces,gear_shifts")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    pairs: list[dict] = []
    for i in range(args.pairs):
        order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
        result: dict = {}
        for arm in order:
            src = args.control_src if arm == "control" else None
            result[arm] = run_arm(src, args.cache, args.charts)
        pairs.append(result)
        print(
            f"pair {i + 1}: control first {result['control']['first_total_s']:.3f} "
            f"second {result['control']['chart1_s']:.3f} | candidate first "
            f"{result['candidate']['first_total_s']:.3f} second "
            f"{result['candidate']['chart1_s']:.3f}",
            flush=True,
        )
    summary = {}
    for phase in PHASES:
        ctl = [p["control"][phase] for p in pairs]
        cand = [p["candidate"][phase] for p in pairs]
        wins = sum(1 for p in pairs if p["candidate"][phase] < p["control"][phase])
        summary[phase] = {
            "control_median": round(statistics.median(ctl), 4),
            "candidate_median": round(statistics.median(cand), 4),
            "candidate_faster_pairs": f"{wins}/{len(pairs)}",
        }
        print(
            f"{phase:16s} control {summary[phase]['control_median']:.4f}"
            f"  candidate {summary[phase]['candidate_median']:.4f}"
            f"  faster {summary[phase]['candidate_faster_pairs']}"
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps({"charts": args.charts, "pairs": pairs, "summary": summary}, indent=1)
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
