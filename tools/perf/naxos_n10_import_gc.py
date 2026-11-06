"""Naxos N10: ``import pandas`` (or polars) with cyclic GC on vs suspended+promoted, fresh processes.

python tools/perf/naxos_n10_import_gc.py --module pandas --reps 10
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from statistics import median

CHILD = """
import gc, sys, time
mode = sys.argv[1]
module = sys.argv[2]
t0 = time.perf_counter()
if mode == "promote":
    gc.disable()
    __import__(module)
    gc.freeze(); gc.unfreeze()
    gc.enable()
else:
    __import__(module)
t1 = time.perf_counter()
# a burst of later work: allocations that trigger the young collections
x = [[i] for i in range(200000)]
t2 = time.perf_counter()
print((t1 - t0) * 1000, (t2 - t1) * 1000)
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--module", default="pandas")
    parser.add_argument("--reps", type=int, default=10)
    args = parser.parse_args()
    res: dict[str, list[tuple[float, float]]] = {"plain": [], "promote": []}
    for _ in range(args.reps):
        for mode, runs in res.items():
            out = subprocess.run(
                [sys.executable, "-c", CHILD, mode, args.module],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.split()
            runs.append((float(out[0]), float(out[1])))
    for mode, runs in res.items():
        print(
            f"{mode:8s} import median {median(r[0] for r in runs):7.1f} ms, "
            f"later 200k allocations {median(r[1] for r in runs):6.1f} ms"
        )
    won = sum(p[0] + p[1] < c[0] + c[1] for c, p in zip(res["plain"], res["promote"], strict=True))
    print(f"promote faster (import + later) in {won}/{args.reps}")


if __name__ == "__main__":
    main()
