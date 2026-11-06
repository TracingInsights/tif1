"""Corinth C5: wall vs main-thread CPU of the frame-tier decode stages during a warm read.

Wraps ``frame_codec._decode_str``, ``_group_parts`` and ``_writable_body``
with perf_counter + thread_time accumulators, runs a warm pandas
``fetch_all_laps_telemetry()`` in a fresh process per rep, and prints the
medians. Wall well above CPU means the main thread waited (GIL or faults).

    TIF1_CACHE_DIR=/tmp/corinth/warm-C5 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/corinth_c5_contention.py --reps 7
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from statistics import median

STAGES = ("_decode_str", "_group_parts", "_writable_body")


def _child() -> None:
    import tif1

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    _ = session.laps
    from tif1.core_utils import frame_codec

    acc: dict[str, list[float]] = {s: [0.0, 0.0, 0] for s in STAGES}  # type: ignore[misc]

    def wrap(name: str) -> None:
        fn = getattr(frame_codec, name)

        def wrapper(*a, **k):
            t0 = time.perf_counter()
            c0 = time.thread_time()
            try:
                return fn(*a, **k)
            finally:
                rec = acc[name]
                rec[0] += time.perf_counter() - t0
                rec[1] += time.thread_time() - c0
                rec[2] += 1

        setattr(frame_codec, name, wrapper)

    for s in STAGES:
        wrap(s)
    t0 = time.perf_counter()
    c0 = time.thread_time()
    session.fetch_all_laps_telemetry()
    out = {
        "all_tel": (time.perf_counter() - t0) * 1000,
        "all_tel.cpu": (time.thread_time() - c0) * 1000,
    }
    for s, (w, c, n) in acc.items():
        out[s] = w * 1000
        out[s + ".cpu"] = c * 1000
        out[s + ".n"] = n
    print(json.dumps(out))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--reps", type=int, default=7)
    args = parser.parse_args()
    if args.child:
        _child()
        return
    recs = []
    for _ in range(args.reps):
        proc = subprocess.run(
            [sys.executable, __file__, "--child"], capture_output=True, text=True, check=True
        )
        recs.append(json.loads(proc.stdout.strip().splitlines()[-1]))
    print(sys.version.split()[0])
    for key in recs[0]:
        vals = [r[key] for r in recs]
        print(f"  {key:22s} median {median(vals):8.2f}  min {min(vals):8.2f}")


if __name__ == "__main__":
    main()
