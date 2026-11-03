"""Elis E5 probe: lap-loop and N-driver access time by prefetch lane count, interleaved.

Each rep runs, in a fresh process per arm (arms interleaved, the order
rotated every rep): ``load()``, then either the loop over every lap's
telemetry (``--shape loop``) or the fastest lap's car data of the first N
of VER, LEC, NOR, PIA (``--shape N``). Arms: ``control`` (the control tree)
and ``lanes=K`` (this tree with ``cache._PREFETCH_LANES = K``). Prints the
median and min of the measured phase per arm.

Usage (from the repo root):
    python tools/perf/elis_e5_lanes.py --reps 8 --lanes 1,2,4,8 --shape loop
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

CONTROL_SRC = "/tmp/tif1-elis-control/src"
CONTROL_CACHE = "/tmp/tif1-delphi-warm"
CANDIDATE_SRC = str(Path(__file__).resolve().parents[2] / "src")
CANDIDATE_CACHE = "/tmp/tif1-elis-warm-E5"


def _child(shape: str, lanes: int) -> None:
    import tif1
    from tif1 import cache as cache_mod

    if lanes and hasattr(cache_mod, "_PREFETCH_LANES"):
        cache_mod._PREFETCH_LANES = lanes
    switch = os.environ.get("E5_SWITCH_INTERVAL")
    if switch:  # mechanism probe: GIL hand-off latency
        sys.setswitchinterval(float(switch))
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
    session.load()
    laps = session.laps
    t0 = time.perf_counter()
    if shape == "loop":
        _ = [lap.telemetry for _, lap in laps.iterlaps()]
    else:
        drivers = ("VER", "LEC", "NOR", "PIA")[: int(shape)]
        _ = [laps.pick_driver(d).pick_fastest().get_car_data() for d in drivers]
    print(json.dumps({"phase": (time.perf_counter() - t0) * 1000}))


def _run(arm: str, shape: str) -> tuple[float, float]:
    control = arm == "control"
    lanes = 0 if control else int(arm.split("=")[1])
    env = dict(
        os.environ,
        TIF1_OFFLINE_MODE="true",
        TIF1_CACHE_DIR=CONTROL_CACHE if control else CANDIDATE_CACHE,
        PYTHONPATH=CONTROL_SRC if control else CANDIDATE_SRC,
    )
    cmd = [sys.executable, __file__, "--child", shape, "--child-lanes", str(lanes)]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
    wall = (time.perf_counter() - t0) * 1000
    return json.loads(proc.stdout.strip().splitlines()[-1])["phase"], wall


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--child-lanes", type=int, default=0)
    parser.add_argument("--reps", type=int, default=8)
    parser.add_argument("--lanes", default="1,2,4,8")
    parser.add_argument("--shape", default="loop")
    parser.add_argument("--switch-interval", default="")
    args = parser.parse_args()
    if args.switch_interval:
        os.environ["E5_SWITCH_INTERVAL"] = args.switch_interval
    if args.child:
        _child(args.child, args.child_lanes)
        return
    arms = ["control", *(f"lanes={k}" for k in args.lanes.split(","))]
    phase: dict[str, list[float]] = {arm: [] for arm in arms}
    wall: dict[str, list[float]] = {arm: [] for arm in arms}
    for rep in range(args.reps):
        order = arms[rep % len(arms) :] + arms[: rep % len(arms)]
        for arm in order:
            p, w = _run(arm, args.shape)
            phase[arm].append(p)
            wall[arm].append(w)
    for arm in arms:
        print(
            f"{args.shape:5s} {arm:9s} phase median {statistics.median(phase[arm]):6.1f} "
            f"min {min(phase[arm]):6.1f} | wall median {statistics.median(wall[arm]):6.1f}"
        )


if __name__ == "__main__":
    main()
