"""Naxos N2: where a polars loop over every lap spends its time after ``load()``.

Times ``load()``, the first lap's telemetry (driver 1), the first lap of the
second driver, and the rest of the loop, in a fresh interpreter (warm cache,
offline). Run per tree with ``PYTHONPATH``.

Usage:
    python tools/perf/naxos_n2_loop_probe.py --cache /tmp/naxos/warm-N2 --reps 5 \
        /tmp/naxos/control/src /tmp/naxos/N2/src
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


def _child() -> None:
    import tif1

    out: dict[str, float] = {}
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
    t = time.perf_counter()
    session.load()
    out["load"] = (time.perf_counter() - t) * 1000
    laps = session.laps
    if os.environ.get("N2_PROBE_GENERATOR"):
        # The notebook's loop: laps consumed one at a time.
        t = time.perf_counter()
        _ = [lap.telemetry for _, lap in laps.iterlaps()]
        out["loop"] = (time.perf_counter() - t) * 1000
        print(json.dumps({k: round(v, 1) for k, v in out.items()}))
        return
    rows = list(laps.iterlaps())
    first_driver = rows[0][1]["Driver"]
    second = next(i for i, (_, lap) in enumerate(rows) if lap["Driver"] != first_driver)
    t = time.perf_counter()
    _ = rows[0][1].telemetry
    out["first_lap"] = (time.perf_counter() - t) * 1000
    t = time.perf_counter()
    for _, lap in rows[1:second]:
        _ = lap.telemetry
    out["rest_of_driver_1"] = (time.perf_counter() - t) * 1000
    t = time.perf_counter()
    _ = rows[second][1].telemetry
    out["second_driver_first_lap"] = (time.perf_counter() - t) * 1000
    t = time.perf_counter()
    for _, lap in rows[second + 1 :]:
        _ = lap.telemetry
    out["rest"] = (time.perf_counter() - t) * 1000
    print(json.dumps({k: round(v, 1) for k, v in out.items()}))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--cache", default=None)
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("srcs", nargs="*")
    args = parser.parse_args()
    if args.child:
        _child()
        return
    recs: dict[str, list[dict[str, float]]] = {src: [] for src in args.srcs}
    for i in range(args.reps):
        for src in args.srcs if i % 2 == 0 else args.srcs[::-1]:
            env = dict(
                os.environ, TIF1_OFFLINE_MODE="true", TIF1_CACHE_DIR=args.cache, PYTHONPATH=src
            )
            proc = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--child"],
                capture_output=True,
                text=True,
                env=env,
                check=True,
            )
            recs[src].append(json.loads(proc.stdout.strip().splitlines()[-1]))
    for src, runs in recs.items():
        medians = {key: round(statistics.median(r[key] for r in runs), 1) for key in runs[0]}
        print(src, json.dumps(medians))


if __name__ == "__main__":
    main()
