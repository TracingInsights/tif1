"""Gortyn G4: run ``method_bench.py`` interleaved on the control and candidate trees.

Prints per method the median over rounds of each tree's per-process median (us)
and writes them to ``--out``.

Usage (from the repo root):
    python tools/perf/gortyn_g4/method_bench_ab.py --control-src /tmp/tif1-gortyn-control/src \
        --candidate-src src --cache /tmp/tif1-gortyn-warm-G4 --rounds 3 [--no-numpy] --out FILE
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _run(src: str, cache: str, extra: list[str]) -> dict[str, float]:
    env = dict(
        os.environ,
        TIF1_CACHE_DIR=cache,
        TIF1_OFFLINE_MODE="true",
        PYTHONPATH=str(Path(src).resolve()),
    )
    proc = subprocess.run(
        [sys.executable, str(HERE / "method_bench.py"), *extra],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--reps", type=int, default=20)
    parser.add_argument("--no-numpy", action="store_true")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    extra = ["--reps", str(args.reps)] + (["--no-numpy"] if args.no_numpy else [])
    runs: dict[str, list[dict[str, float]]] = {"control": [], "candidate": []}
    for _ in range(args.rounds):
        runs["control"].append(_run(args.control_src, args.cache, extra))
        runs["candidate"].append(_run(args.candidate_src, args.cache, extra))
    summary = {}
    for name in runs["control"][0]:
        control = statistics.median(r[name] for r in runs["control"])
        candidate = statistics.median(r[name] for r in runs["candidate"])
        summary[name] = {"control_us": control, "candidate_us": candidate}
        print(f"{name:30s} control {control:9.1f} µs  candidate {candidate:9.1f} µs")
    if args.out:
        Path(args.out).write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
