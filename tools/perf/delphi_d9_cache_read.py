"""Delphi D9 probe: ``Cache.get_telemetry_frame_sources`` on a warm frame tier.

Times the library's own driver-row read (read + decompress + decode of a
session's rows) in a fresh process: the first call (cold file map, cold
pool, cold reader connections) and the median of the following ``--reps``
calls. ``--readers`` sets ``cache._FRAME_READER_CONNECTIONS`` before the
cache opens (ignored by a tree without it, e.g. the control tree).

    TIF1_CACHE_DIR=/path/to/warm/cache python tools/perf/delphi_d9_cache_read.py \\
        --readers 2 --reps 10 --drivers 20

Run from the repository root with ``PYTHONPATH`` pointing at the tree to
measure.
"""

from __future__ import annotations

import argparse
import os
import resource
import statistics
import sys
import time

GP = "Bahrain%20Grand%20Prix"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--gp", default=GP)
    parser.add_argument("--session", default="Race")
    parser.add_argument("--readers", type=int, default=None)
    parser.add_argument("--reps", type=int, default=10)
    parser.add_argument("--drivers", type=int, default=20, help="how many drivers to read")
    parser.add_argument("--batch", action="store_true", help="time get_telemetry_frames_batch")
    args = parser.parse_args()
    if not os.environ.get("TIF1_CACHE_DIR"):
        sys.exit("set TIF1_CACHE_DIR to a warm cache directory")

    import pandas as pd  # noqa: F401  (the frame codec needs it; keep it out of the timings)

    import tif1.cache as cache_mod

    if args.readers is not None and hasattr(cache_mod, "_FRAME_READER_CONNECTIONS"):
        cache_mod._FRAME_READER_CONNECTIONS = args.readers
    cache = cache_mod.Cache()
    key = (args.year, args.gp, args.session)
    drivers = [
        row[0]
        for row in cache.conn.execute(
            f"SELECT driver FROM {cache_mod._PANDAS_DRIVER_TABLE} "
            "WHERE year = ? AND gp = ? AND session = ? ORDER BY driver",
            key,
        ).fetchall()
    ][: args.drivers]

    def read() -> int:
        if args.batch:
            refs = [(driver, 1) for driver in drivers]
            return len(cache.get_telemetry_frames_batch(*key, refs, whole_driver=True))
        return len(cache.get_telemetry_frame_sources(*key, drivers))

    times: list[float] = []
    faults: list[int] = []
    for _ in range(args.reps + 1):
        before = resource.getrusage(resource.RUSAGE_SELF)
        t0 = time.perf_counter()
        n = read()
        times.append((time.perf_counter() - t0) * 1e3)
        after = resource.getrusage(resource.RUSAGE_SELF)
        faults.append(after.ru_minflt - before.ru_minflt)
    label = f"readers={args.readers}" if args.readers is not None else "tree default"
    print(
        f"{label:14s} drivers={len(drivers):2d} n={n:4d}  first {times[0]:6.1f} ms "
        f"(minflt {faults[0]})  median(rest) {statistics.median(times[1:]):6.1f} ms "
        f"(minflt {int(statistics.median(faults[1:]))})"
    )
    cache.close()


if __name__ == "__main__":
    main()
