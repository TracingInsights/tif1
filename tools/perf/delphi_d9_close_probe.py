"""Delphi D9 side probe: what closing the reader connections costs at exit.

Fresh process per run: read the 20 race rows (``get_telemetry_frame_sources``),
then time ``_close_readers()`` (candidate tree only) and ``close()``, and the
whole process wall of that script, for the control and the candidate tree,
alternating. Run from the repository root with ``TIF1_CACHE_DIR`` at a warm
cache; ``--control-src`` and ``--candidate-src`` name the two trees.
"""

from __future__ import annotations

import argparse
import os
import statistics
import subprocess
import sys
import time

CHILD = """
import time
import pandas as pd
import tif1.cache as cache_mod
cache = cache_mod.Cache()
key = (2025, "Bahrain%20Grand%20Prix", "Race")
table = cache_mod._PANDAS_DRIVER_TABLE
drivers = [
    r[0]
    for r in cache.conn.execute(
        f"SELECT driver FROM {table} WHERE year=? AND gp=? AND session=?", key
    )
]
s = cache.get_telemetry_frame_sources(*key, drivers)
n_readers = len(getattr(cache, "_idle_readers", []))
t0 = time.perf_counter()
if hasattr(cache, "_close_readers"):
    cache._close_readers()
t1 = time.perf_counter()
cache.close()
t2 = time.perf_counter()
print(n_readers, (t1 - t0) * 1e3, (t2 - t1) * 1e3)
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--reps", type=int, default=6)
    args = parser.parse_args()
    if not os.environ.get("TIF1_CACHE_DIR"):
        sys.exit("set TIF1_CACHE_DIR to a warm cache directory")
    arms = {"control": args.control_src, "candidate": args.candidate_src}
    stats: dict[str, list[tuple[float, ...]]] = {arm: [] for arm in arms}
    walls: dict[str, list[float]] = {arm: [] for arm in arms}
    for _ in range(args.reps):
        for arm, src in arms.items():
            env = dict(os.environ, PYTHONPATH=src)
            t0 = time.perf_counter()
            out = subprocess.run(
                [sys.executable, "-c", CHILD], env=env, capture_output=True, text=True, check=True
            ).stdout.split()
            walls[arm].append((time.perf_counter() - t0) * 1e3)
            stats[arm].append(tuple(float(x) for x in out))
    for arm, rows in stats.items():
        print(
            f"{arm:9s} readers {rows[0][0]:.0f}  "
            f"close_readers median {statistics.median(r[1] for r in rows):5.2f} ms  "
            f"close() median {statistics.median(r[2] for r in rows):5.2f} ms  "
            f"process wall median {statistics.median(walls[arm]):6.1f} ms  min {min(walls[arm]):6.1f}"
        )


if __name__ == "__main__":
    main()
