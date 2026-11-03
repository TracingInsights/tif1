"""Elis E4 probe: faults and system time of a warm ``fetch_all_laps_telemetry()``.

One fresh process per run (``--child``), 2025 Bahrain race, pandas, offline;
prints the phase wall, the minor faults and the system/user CPU of the
process during the call, and how many rows took the huge-page path.

Usage (repo root; the cache must be primed)::

    TIF1_CACHE_DIR=/tmp/tif1-elis-warm-E4 python tools/perf/elis_e4_alltel_probe.py --runs 6
"""

from __future__ import annotations

import argparse
import os
import resource
import statistics
import subprocess
import sys
import time


def _child() -> None:
    os.environ.setdefault("TIF1_OFFLINE_MODE", "true")
    import tif1
    from tif1 import cache

    huge = [0]
    original = cache._huge_page_decompress

    def counted(src: object, size: int) -> object:
        out = original(src, size)
        huge[0] += out is not None
        return out

    cache._huge_page_decompress = counted
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
    session.load(laps=True, telemetry=False, weather=False, messages=False)
    r0 = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    frames = session.fetch_all_laps_telemetry()
    wall = (time.perf_counter() - t0) * 1e3
    r1 = resource.getrusage(resource.RUSAGE_SELF)
    print(
        f"{wall:.2f} {r1.ru_minflt - r0.ru_minflt} {(r1.ru_stime - r0.ru_stime) * 1e3:.1f} "
        f"{(r1.ru_utime - r0.ru_utime) * 1e3:.1f} {huge[0]} {len(frames)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=6)
    parser.add_argument("--child", action="store_true")
    args = parser.parse_args()
    if args.child:
        _child()
        return
    arms = {"native": {}, "disabled": {"TIF1_DISABLE_NATIVE": "1"}}
    results: dict[str, list[list[float]]] = {arm: [] for arm in arms}
    for _ in range(args.runs):
        for arm, extra in arms.items():
            out = subprocess.run(
                [sys.executable, __file__, "--child"],
                capture_output=True,
                text=True,
                check=True,
                env={**os.environ, **extra},
            ).stdout.split()
            results[arm].append([float(x) for x in out])
    print(f"{'arm':10} {'wall ms':>8} {'faults':>8} {'sys ms':>8} {'user ms':>8} huge laps")
    for arm, runs in results.items():
        med = [statistics.median(col) for col in zip(*runs, strict=True)]
        print(
            f"{arm:10} {med[0]:8.1f} {med[1]:8.0f} {med[2]:8.1f} {med[3]:8.1f} "
            f"{med[4]:.0f} {med[5]:.0f}"
        )


if __name__ == "__main__":
    main()
