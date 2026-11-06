"""Naxos N2: the notebook workload's first phases with the frame-tier reads timed.

Runs ``load(weather, messages)``, the eight picks and the ``iterlaps``
telemetry loop of ``delphi_warm_bench.py``'s notebook (polars), in a fresh
interpreter per rep, and reports each phase plus the time spent inside
``Cache.get_telemetry_frames_batch`` (reads and their count) per phase.
Arms interleave.

Usage:
    python tools/perf/naxos_n2_notebook_probe.py --cache /tmp/naxos/warm-N2 --reps 6 \
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
    from tif1.cache import Cache

    out: dict[str, float] = {}
    tier = {"ms": 0.0, "n": 0}
    read = Cache.get_telemetry_frames_batch

    def timed(self, *args, **kwargs):
        t = time.perf_counter()
        try:
            return read(self, *args, **kwargs)
        finally:
            tier["ms"] += (time.perf_counter() - t) * 1000
            tier["n"] += 1

    Cache.get_telemetry_frames_batch = timed  # type: ignore[method-assign]

    def step(name, fn):
        before_ms, before_n = tier["ms"], tier["n"]
        t = time.perf_counter()
        fn()
        out[name] = (time.perf_counter() - t) * 1000
        out[f"{name}.tier_ms"] = tier["ms"] - before_ms
        out[f"{name}.tier_reads"] = tier["n"] - before_n

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
    step("load", lambda: session.load(weather=True, messages=True))
    laps = session.laps
    shuffled = os.environ.get("N2_PROBE_DRIVERS")
    if shuffled:
        # load() then each driver's fastest-lap car data, one phase per driver.
        for code in shuffled.split(","):
            step(code, lambda code=code: laps.pick_drivers(code).pick_fastest().get_car_data())
        print(json.dumps({k: round(v, 1) for k, v in out.items()}))
        return
    step(
        "picks_x8",
        lambda: [
            laps.pick_driver("VER"),
            laps.pick_drivers(["VER", "LEC"]),
            laps.pick_quicklaps(),
            laps.pick_fastest(),
            laps.pick_laps([5, 6]),
            laps.pick_compounds(["SOFT"]),
            laps.pick_wo_box(),
            laps.pick_track_status("1"),
        ],
    )
    step("loop", lambda: [lap.telemetry for _, lap in laps.iterlaps()])
    print(json.dumps({k: round(v, 1) for k, v in out.items()}))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--cache", default=None)
    parser.add_argument("--reps", type=int, default=6)
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
