"""Naxos N10: per-stage wall time of a warm pandas ``fetch_all_laps_telemetry()``.

Fresh process, offline, warm cache (``TIF1_CACHE_DIR``). Wraps the stages of
the frame-tier read with perf_counter timers (main thread only) and prints
them with the phase total, so the stages can be compared without cProfile's
overhead (which also mixes the worker threads in on CPython 3.12+).

    TIF1_CACHE_DIR=/tmp/naxos/warm-N10 TIF1_OFFLINE_MODE=true \
        python tools/perf/naxos_n10_stages.py [--reps 5]
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

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    _ = session.laps

    from tif1 import cache as cache_mod
    from tif1 import core
    from tif1.core_utils import frame_codec, pandas_blocks

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

    wrap(frame_codec.DriverFrameSource, "__init__", "source_init")
    wrap(frame_codec, "_group_parts", "group_parts")
    wrap(frame_codec, "_decode_str", "decode_str")
    wrap(frame_codec, "_decode_object", "decode_object")
    wrap(frame_codec.DriverFrameSource, "frames", "frames_build")
    wrap(frame_codec, "_writable_body", "writable_body")
    wrap(pandas_blocks, "_probe_direct_blocks", "probe_direct_blocks")
    wrap(cache_mod, "_submit_frame_row_reads", "submit_reads")
    wrap(cache_mod.Cache, "_get_driver_frames", "get_driver_frames")
    wrap(core.Session, "_adopt_telemetry_frame", "adopt")
    wrap(core, "_in_lap_order", "in_lap_order")

    original_completed = cache_mod._completed_frame_rows

    def completed(pending):
        it = original_completed(pending)
        while True:
            t0 = time.perf_counter()
            try:
                item = next(it)
            except StopIteration:
                totals["wait_rows"] = (
                    totals.get("wait_rows", 0.0) + (time.perf_counter() - t0) * 1000
                )
                return
            totals["wait_rows"] = totals.get("wait_rows", 0.0) + (time.perf_counter() - t0) * 1000
            yield item

    cache_mod._completed_frame_rows = completed

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
    keys = sorted({k for run in runs for k in run})
    for key in keys:
        values = [run[key] for run in runs if key in run]
        print(f"{key:22s} median {median(values):8.2f}  min {min(values):8.2f}")


if __name__ == "__main__":
    main()
