"""Naxos N6 probe: interleaved control-vs-candidate timing of the per-lap loops.

Each run is a fresh interpreter (offline, warm cache). ``loop`` is the
notebook's ``load(weather, messages)`` + ``iterlaps`` telemetry loop over
every lap; ``car`` is ``car_data_loop`` (VER's laps, ``get_car_data()``); ``load`` times
the notebook's ``load(weather, messages)`` alone.

Usage (from the tree root):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 python tools/perf/naxos_n6_loop_ab.py \
        --control /tmp/naxos/control/src --candidate src --pairs 8 [--mode loop|car]
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

CHILD = r"""
import json, sys, time
import tif1
mode = sys.argv[1]
s = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
if mode == "load":
    t0 = time.perf_counter()
    s.load(weather=True, messages=True)
    out = [0]
elif mode == "picks":
    s.load(weather=True, messages=True)
    laps = s.laps
    t0 = time.perf_counter()
    out = [
        laps.pick_driver("VER"), laps.pick_drivers(["VER", "LEC"]), laps.pick_quicklaps(),
        laps.pick_fastest(), laps.pick_laps([5, 6]), laps.pick_compounds(["SOFT"]),
        laps.pick_wo_box(), laps.pick_track_status("1"),
    ]
elif mode == "loop":
    s.load(weather=True, messages=True)
    laps = s.laps
    t0 = time.perf_counter()
    out = [lap.telemetry for _, lap in laps.iterlaps()]
else:
    laps = s.laps
    drv = laps.pick_driver("VER")
    t0 = time.perf_counter()
    out = [lap.get_car_data() for _, lap in drv.iterlaps()]
print(json.dumps({"ms": (time.perf_counter() - t0) * 1000, "n": len(out)}))
"""


def _run(src: str, mode: str) -> float:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", PYTHONPATH=str(Path(src).resolve()))
    proc = subprocess.run(
        [sys.executable, "-c", CHILD, mode], capture_output=True, text=True, env=env, check=True
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])["ms"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--mode", default="loop", choices=["loop", "car", "load", "picks"])
    args = parser.parse_args()
    control: list[float] = []
    candidate: list[float] = []
    for i in range(args.pairs):
        order = [("control", args.control), ("candidate", args.candidate)]
        if i % 2:
            order.reverse()
        for label, src in order:
            (control if label == "control" else candidate).append(_run(src, args.mode))
        time.sleep(0.05)
    won = sum(1 for a, b in zip(control, candidate, strict=True) if b < a)
    print(
        f"{args.mode}: control median {statistics.median(control):.1f} "
        f"(min {min(control):.1f})  candidate median {statistics.median(candidate):.1f} "
        f"(min {min(candidate):.1f})  pairs won {won}/{args.pairs}"
    )


if __name__ == "__main__":
    main()
