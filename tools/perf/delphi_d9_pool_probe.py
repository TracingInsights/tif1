"""Delphi D9 side probe: pyarrow memory pool for the decompressed driver rows.

Fresh process per run (the decompressed buffers of a session stay alive, so
only the first call of a process matters): ``get_telemetry_frame_sources``
on the 20 race rows with the system pool (the tree's default) and with
jemalloc, alternating, ``--reps`` runs each. Run from the repository root
with ``PYTHONPATH`` pointing at the D9 tree and ``TIF1_CACHE_DIR`` at a warm
cache.
"""

from __future__ import annotations

import argparse
import os
import statistics
import subprocess
import sys

CHILD = """
import resource, sys, time
import pandas
import pyarrow as pa
import tif1.cache as cache_mod
if sys.argv[1] == "jemalloc":
    cache_mod._pa_lz4_codec()
    cache_mod._PA_LZ4_POOL[0] = pa.jemalloc_memory_pool()
cache = cache_mod.Cache()
key = (2025, "Bahrain%20Grand%20Prix", "Race")
table = cache_mod._PANDAS_DRIVER_TABLE
drivers = [
    r[0]
    for r in cache.conn.execute(
        f"SELECT driver FROM {table} WHERE year=? AND gp=? AND session=?", key
    )
]
b = resource.getrusage(resource.RUSAGE_SELF)
t0 = time.perf_counter()
s = cache.get_telemetry_frame_sources(*key, drivers)
dt = (time.perf_counter() - t0) * 1e3
a = resource.getrusage(resource.RUSAGE_SELF)
print(dt, a.ru_minflt - b.ru_minflt, (a.ru_stime - b.ru_stime) * 1e3, len(s))
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--reps", type=int, default=6)
    args = parser.parse_args()
    if not os.environ.get("TIF1_CACHE_DIR"):
        sys.exit("set TIF1_CACHE_DIR to a warm cache directory")
    results: dict[str, list[tuple[float, ...]]] = {"system": [], "jemalloc": []}
    for _ in range(args.reps):
        for pool, rows in results.items():
            out = subprocess.run(
                [sys.executable, "-c", CHILD, pool], capture_output=True, text=True, check=True
            ).stdout.split()
            rows.append(tuple(float(x) for x in out))
    for pool, rows in results.items():
        times = [r[0] for r in rows]
        print(
            f"{pool:9s} sources(cold) median {statistics.median(times):6.1f} ms  "
            f"min {min(times):6.1f}  faults median {statistics.median(r[1] for r in rows):.0f}  "
            f"sys median {statistics.median(r[2] for r in rows):.0f} ms"
        )


if __name__ == "__main__":
    main()
