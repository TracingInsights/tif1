"""Naxos N2: the lone-wave cap of a cached lazy polars load, interleaved against control.

Runs ``naxos_n2_warm_bench.py`` children (``load()`` then N drivers' fastest
lap car data) for the control tree and the candidate tree with
``N2_LONE_WAVES`` set to each cap (a measurement-only override of
``core._LAZY_CACHED_LONE_WAVES``), the arms rotated every rep, and prints
the median of ``load + access`` (in-process ms) and of the process wall.

Usage:
    python tools/perf/naxos_n2_lone_cap_probe.py --cache /tmp/naxos/warm-N2 --reps 8 \
        --control /tmp/naxos/control/src --candidate /tmp/naxos/N2/src --caps 1,2,4
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

BENCH = Path(__file__).resolve().parent / "naxos_n2_warm_bench.py"
WORKLOADS = (
    "load_then_2_drivers",
    "load_then_top2",
    "load_then_4_drivers",
    "load_then_6_shuffled",
    "load_then_drivers_loop",
)


def _run(src: str, cache: str, workload: str, cap: str | None) -> tuple[float, float]:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", TIF1_CACHE_DIR=cache, PYTHONPATH=src)
    if cap is not None:
        env["N2_LONE_WAVES"] = cap
    t = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, str(BENCH), "--child", workload, "--lib", "polars"],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    wall = (time.perf_counter() - t) * 1000
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    return rec["load"] + rec["access"], wall


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", required=True)
    parser.add_argument("--control", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--caps", default="1,2,4")
    parser.add_argument("--reps", type=int, default=8)
    args = parser.parse_args()
    arms: list[tuple[str, str, str | None]] = [("control", args.control, None)]
    arms += [(f"cap{cap}", args.candidate, cap) for cap in args.caps.split(",")]
    for workload in WORKLOADS:
        recs: dict[str, list[tuple[float, float]]] = {name: [] for name, _, _ in arms}
        for i in range(args.reps):
            rotated = arms[i % len(arms) :] + arms[: i % len(arms)]
            for name, src, cap in rotated:
                recs[name].append(_run(src, args.cache, workload, cap))
        line = "  ".join(
            f"{name} {statistics.median(r[0] for r in runs):.0f}"
            f"/{statistics.median(r[1] for r in runs):.0f}"
            for name, runs in recs.items()
        )
        print(f"{workload:22s} (load+access / wall) {line}", flush=True)


if __name__ == "__main__":
    main()
