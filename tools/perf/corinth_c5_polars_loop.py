"""Corinth C5: the polars notebook's iterlaps telemetry loop, control vs candidate, interleaved.

Fresh process per run (``--python``, offline, warm cache): get_session, load(weather, messages),
then ``[lap.telemetry for _, lap in laps.iterlaps()]`` timed (wall, main-thread CPU, GC ms and
collections, ``promote_young_objects`` calls and actual promotions).

    TIF1_CACHE_DIR=/tmp/corinth/warm-C5 python tools/perf/corinth_c5_polars_loop.py \
        --python /tmp/corinth/venv314/bin/python --a /tmp/corinth/control/src --b src --pairs 10
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from statistics import median


def _child(lib: str) -> None:
    import gc

    import tif1

    s = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=lib)
    s.load(weather=True, messages=True)
    laps = s.laps
    from tif1.core_utils import gc_control

    stats = {"promote_calls": 0, "promotions": 0}
    real = gc_control.promote_young_objects

    def counting() -> None:
        stats["promote_calls"] += 1
        before = gc.get_count()[0]
        real()
        stats["promotions"] += gc.get_count()[0] < before

    gc_control.promote_young_objects = counting
    gc_ms = [0.0, 0.0, 0]

    def on_gc(phase, info):
        if phase == "start":
            gc_ms[1] = time.perf_counter()
        else:
            gc_ms[0] += time.perf_counter() - gc_ms[1]
            gc_ms[2] += 1

    gc.callbacks.append(on_gc)
    t0 = time.perf_counter()
    c0 = time.thread_time()
    [lap.telemetry for _, lap in laps.iterlaps()]
    out = {
        "loop": (time.perf_counter() - t0) * 1000,
        "loop.cpu": (time.thread_time() - c0) * 1000,
        "loop.gc": gc_ms[0] * 1000,
        "loop.collections": gc_ms[2],
        **stats,
    }
    print(json.dumps(out))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--a", default="/tmp/corinth/control/src")
    parser.add_argument("--b", default="src")
    parser.add_argument("--lib", default="polars")
    parser.add_argument("--pairs", type=int, default=10)
    args = parser.parse_args()
    if args.child:
        _child(args.child)
        return
    recs: dict[str, list[dict]] = {"a": [], "b": []}
    for i in range(args.pairs):
        for arm in ("a", "b") if i % 2 == 0 else ("b", "a"):
            src = str(Path(getattr(args, arm)).resolve())
            env = dict(os.environ, PYTHONPATH=src, TIF1_OFFLINE_MODE="true")
            proc = subprocess.run(
                [args.python, __file__, "--child", args.lib],
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )
            recs[arm].append(json.loads(proc.stdout.strip().splitlines()[-1]))
    for key in recs["a"][0]:
        a = [r[key] for r in recs["a"]]
        b = [r[key] for r in recs["b"]]
        won = sum(y < x for x, y in zip(a, b, strict=True))
        print(f"{key:18s} a {median(a):8.2f}  b {median(b):8.2f}  b lower {won}/{args.pairs}")


if __name__ == "__main__":
    main()
