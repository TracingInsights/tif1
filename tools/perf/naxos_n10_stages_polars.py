"""Naxos N10: per-stage wall time of a warm polars ``fetch_all_laps_telemetry()``.

TIF1_CACHE_DIR=/tmp/naxos/warm-N10 python tools/perf/naxos_n10_stages_polars.py --reps 5
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from statistics import median


def _child() -> None:
    import tif1

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
    _ = session.laps

    import polars as pl

    from tif1 import cache as cache_mod
    from tif1 import core

    main = threading.get_ident()
    totals: dict[str, float] = {}

    def wrap(owner, name, label):
        original = getattr(owner, name)

        def timed(*args, **kwargs):
            if threading.get_ident() != main:
                return original(*args, **kwargs)
            t0 = time.perf_counter()
            try:
                return original(*args, **kwargs)
            finally:
                totals[label] = totals.get(label, 0.0) + (time.perf_counter() - t0) * 1000

        setattr(owner, name, timed)

    wrap(cache_mod.Cache, "_get_polars_ipc_frames", "get_ipc_frames")
    wrap(core.Session, "_adopt_telemetry_frame", "adopt")
    wrap(core.Session, "_memoized_telemetry", "memoized_telemetry")
    wrap(pl, "concat", "pl_concat")
    wrap(pl.DataFrame, "slice", "slices")
    wrap(core, "_in_lap_order", "in_lap_order")
    wrap(core.Session, "_telemetry_lap_refs", "lap_refs")

    t0 = time.perf_counter()
    out = session.fetch_all_laps_telemetry()
    totals["all_tel"] = (time.perf_counter() - t0) * 1000
    totals["frames"] = len(out)
    print(json.dumps({k: round(v, 2) for k, v in totals.items()}))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--src", default=None)
    args = parser.parse_args()
    if args.child:
        _child()
        return
    env = dict(os.environ, TIF1_OFFLINE_MODE="true")
    if args.src:
        env["PYTHONPATH"] = str(Path(args.src).resolve())
    runs = []
    for _ in range(args.reps):
        proc = subprocess.run(
            [sys.executable, __file__, "--child"],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        runs.append(json.loads(proc.stdout.strip().splitlines()[-1]))
    for key in sorted({k for run in runs for k in run}):
        values = [run[key] for run in runs if key in run]
        print(f"{key:22s} median {median(values):8.2f}  min {min(values):8.2f}")


if __name__ == "__main__":
    main()
