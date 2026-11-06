"""Naxos N10: interleaved control/candidate runs of ``naxos_n10_gc.py`` (GC ms and phase ms).

    TIF1_CACHE_DIR=/tmp/naxos/warm-N10 python tools/perf/naxos_n10_gc_ab.py \
        --control-src /tmp/naxos/control/src --candidate-src src --lib pandas \
        --workload all_tel --reps 8
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from statistics import median

PROBE = Path(__file__).with_name("naxos_n10_gc.py")


def run(src: str, lib: str, workload: str) -> tuple[float, float]:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", PYTHONPATH=str(Path(src).resolve()))
    out = subprocess.run(
        [sys.executable, str(PROBE), lib, workload],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    ).stdout
    phase = float(re.search(r": ([\d.]+) ms; collections", out).group(1))
    gc_ms = float(re.search(r"gc ms total ([\d.]+)", out).group(1))
    return phase, gc_ms


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--workload", default="all_tel")
    parser.add_argument("--reps", type=int, default=8)
    args = parser.parse_args()
    arms: dict[str, list[tuple[float, float]]] = {"control": [], "candidate": []}
    for _ in range(args.reps):
        arms["control"].append(run(args.control_src, args.lib, args.workload))
        arms["candidate"].append(run(args.candidate_src, args.lib, args.workload))
    for name, runs in arms.items():
        phases = [p for p, _ in runs]
        gcs = [g for _, g in runs]
        print(
            f"{name:9s} phase median {median(phases):7.2f} min {min(phases):7.2f} | "
            f"gc median {median(gcs):6.2f} all {[round(g, 1) for g in gcs]}"
        )
    won = sum(c[0] < k[0] for k, c in zip(arms["control"], arms["candidate"], strict=True))
    print(f"candidate phase won {won}/{args.reps}")


if __name__ == "__main__":
    main()
