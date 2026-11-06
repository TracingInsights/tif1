"""Naxos N2: the polars IPC batch read alone, after a lone row read, and the lone read itself.

Fresh interpreter per variant (warm cache, offline); prints medians.

Usage:
    python tools/perf/naxos_n2_batch_probe.py --cache /tmp/naxos/warm-N2 --src /tmp/naxos/N2/src
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

VARIANTS = ("batch20", "lone_then_batch19", "lone4_then_batch16")


def _child(variant: str) -> None:
    import gc

    import tif1
    from tif1.cache import get_cache

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
    laps = session.laps
    refs = session._telemetry_lap_refs(laps)
    drivers = list(dict.fromkeys(d for d, _ in refs))
    cache = get_cache()
    out: dict[str, float] = {}

    def read(rs):
        return cache.get_telemetry_frames_batch(
            session.year, session.gp, session.session, rs, lib="polars"
        )

    if variant.endswith("gc_off"):
        gc.disable()
    alone = 4 if variant.startswith("lone4") else 1 if variant.startswith("lone") else 0
    for i in range(alone):
        t = time.perf_counter()
        read([r for r in refs if r[0] == drivers[i]])
        out[f"lone{i + 1}"] = (time.perf_counter() - t) * 1000
    rest = [r for r in refs if r[0] not in drivers[:alone]]
    t = time.perf_counter()
    read(rest)
    out["batch"] = (time.perf_counter() - t) * 1000
    print(json.dumps(out))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--cache", default=None)
    parser.add_argument("--src", default=None)
    parser.add_argument("--reps", type=int, default=5)
    args = parser.parse_args()
    if args.child:
        _child(args.child)
        return
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", TIF1_CACHE_DIR=args.cache, PYTHONPATH=args.src)
    recs: dict[str, list[dict[str, float]]] = {v: [] for v in VARIANTS}
    for _ in range(args.reps):
        for variant in VARIANTS:
            proc = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--child", variant],
                capture_output=True,
                text=True,
                env=env,
                check=True,
            )
            recs[variant].append(json.loads(proc.stdout.strip().splitlines()[-1]))
    for variant, runs in recs.items():
        medians = {key: round(statistics.median(r[key] for r in runs), 1) for key in runs[0]}
        print(variant, json.dumps(medians))


if __name__ == "__main__":
    main()
