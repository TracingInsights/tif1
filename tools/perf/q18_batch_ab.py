"""Interleaved fresh-process A/B of ``Cache.get_telemetry_frames_batch`` (Pergamon Q18).

Runs ``tools/perf/pergamon_legacy_read_bench.py`` alternately against the control
and candidate source trees over the SAME cache directory (the read does not
write), alternating which arm goes first, and reports medians + win counts.

Usage:
    uv run python tools/perf/q18_batch_ab.py --control-src /tmp/tif1-pergamon-control/src \
        --cache-dir /tmp/x --year 2026 --gp "Monaco%20Grand%20Prix" --session Race --pairs 8
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _run(src: str, args: argparse.Namespace) -> float:
    env = dict(os.environ, TIF1_CACHE_DIR=args.cache_dir)
    out = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "perf" / "pergamon_legacy_read_bench.py"),
            "--src-dir",
            src,
            "--year",
            str(args.year),
            "--gp",
            args.gp,
            "--session",
            args.session,
        ],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return float(json.loads(out.strip().splitlines()[-1])["read_ms"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", default=str(ROOT / "src"))
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--gp", default="Monaco%20Grand%20Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--pairs", type=int, default=8)
    args = parser.parse_args()
    control: list[float] = []
    candidate: list[float] = []
    for i in range(args.pairs):
        if i % 2:
            candidate.append(_run(args.candidate_src, args))
            control.append(_run(args.control_src, args))
        else:
            control.append(_run(args.control_src, args))
            candidate.append(_run(args.candidate_src, args))
    wins = sum(1 for c, k in zip(control, candidate) if k < c)
    print(
        json.dumps(
            {
                "year": args.year,
                "gp": args.gp,
                "session": args.session,
                "control_ms": control,
                "candidate_ms": candidate,
                "control_median": statistics.median(control),
                "candidate_median": statistics.median(candidate),
                "candidate_faster": f"{wins}/{args.pairs}",
            }
        )
    )


if __name__ == "__main__":
    main()
