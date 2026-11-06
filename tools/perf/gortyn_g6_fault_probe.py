"""Gortyn G6 probe: faults and CPU of a warm ``fetch_all_laps_telemetry()``, per arm.

G1's ``gortyn_g1_fault_probe.py`` plus a per-arm overlay: an arm's
``SITE=DIR`` extra goes ahead of its source tree on ``PYTHONPATH`` (after the
shared ``--site`` entries), so two tif1-native builds can be compared on one
tree::

    TIF1_CACHE_DIR=/tmp/tif1-gortyn-warm-G6 python tools/perf/gortyn_g6_fault_probe.py \
        --arm lz4_flex=src,SITE=/tmp/tif1-gortyn/G1/native/target/site \
        --arm liblz4=src,SITE=native/target/site --runs 12

G1's docstring: an interleaved version of Elis E4's ``elis_e4_alltel_probe.py`` whose arms are
source trees (and environments) instead of native on/off. Each run is a fresh
process (2025 Bahrain race, pandas, offline, the cache primed): the phase
wall, the process's minor faults and system/user CPU during the call, how
many rows decompressed into a huge-page buffer and how many blobs were read
into one (``cache._read_blob``, absent before G1 step 4). ``--route harness``
loads the laps as the warm bench's ``all_tel`` does (``session.laps``).

Usage (repo root; ``--site`` puts an unpacked wheel ahead of every arm)::

    TIF1_CACHE_DIR=/tmp/tif1-gortyn-warm-G1 python tools/perf/gortyn_g1_fault_probe.py \
        --site native/target/site --arm base=native/target/g1-base/src --arm chunked=src \
        --runs 8
"""

from __future__ import annotations

import argparse
import os
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path


def _child() -> None:
    os.environ.setdefault("TIF1_OFFLINE_MODE", "true")
    import mmap

    import tif1
    from tif1 import cache

    huge = [0, 0]
    decompress = cache._huge_page_decompress

    def counted_decompress(src: object, size: int) -> object:
        out = decompress(src, size)
        huge[0] += out is not None
        return out

    cache._huge_page_decompress = counted_decompress
    read_blob = getattr(cache, "_read_blob", None)
    if read_blob is not None:

        def counted_read(handle: object) -> object:
            out = read_blob(handle)
            huge[1] += isinstance(getattr(out, "obj", None), mmap.mmap)
            return out

        cache._read_blob = counted_read
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
    if os.environ.get("G1_PROBE_ROUTE") == "harness":
        _ = session.laps  # the warm-bench ``all_tel`` route (implicit load)
    else:
        session.load(laps=True, telemetry=False, weather=False, messages=False)
    r0 = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    frames = session.fetch_all_laps_telemetry()
    wall = (time.perf_counter() - t0) * 1e3
    r1 = resource.getrusage(resource.RUSAGE_SELF)
    print(
        f"{wall:.2f} {r1.ru_minflt - r0.ru_minflt} {(r1.ru_stime - r0.ru_stime) * 1e3:.1f} "
        f"{(r1.ru_utime - r0.ru_utime) * 1e3:.1f} {huge[0]} {huge[1]} {len(frames)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=8)
    parser.add_argument("--site", action="append", default=[])
    parser.add_argument(
        "--arm", action="append", default=[], help="NAME=SRC[,KEY=VALUE...] (repeatable)"
    )
    parser.add_argument(
        "--route", default="load", choices=["load", "harness"], help="how the laps are loaded"
    )
    parser.add_argument("--child", action="store_true")
    args = parser.parse_args()
    if args.child:
        _child()
        return
    arms: dict[str, dict[str, str]] = {}
    for spec in args.arm:
        name, _, rest = spec.partition("=")
        src, *extras = rest.split(",")
        env = dict(item.split("=", 1) for item in extras)
        own_site = [env.pop("SITE")] if "SITE" in env else []
        env["PYTHONPATH"] = os.pathsep.join(
            [*(str(Path(s).resolve()) for s in [*args.site, *own_site]), str(Path(src).resolve())]
        )
        arms[name] = env
    results: dict[str, list[list[float]]] = {arm: [] for arm in arms}
    names = list(arms)
    for i in range(args.runs):
        for arm in names if i % 2 == 0 else names[::-1]:
            out = subprocess.run(
                [sys.executable, __file__, "--child"],
                capture_output=True,
                text=True,
                check=True,
                env={**os.environ, **arms[arm], "G1_PROBE_ROUTE": args.route},
            ).stdout.split()
            results[arm].append([float(x) for x in out])
    first = results[names[0]]
    print(
        f"{'arm':10} {'wall ms':>8} {'faults':>8} {'sys ms':>8} {'user ms':>8} "
        f"huge-rows huge-blobs laps  faster-than-{names[0]}"
    )
    for arm, runs in results.items():
        med = [statistics.median(col) for col in zip(*runs, strict=True)]
        wins = sum(1 for a, b in zip(first, runs, strict=True) if b[0] < a[0])
        print(
            f"{arm:10} {med[0]:8.1f} {med[1]:8.0f} {med[2]:8.1f} {med[3]:8.1f} "
            f"{med[4]:9.0f} {med[5]:10.0f} {med[6]:4.0f}  {wins}/{args.runs}"
        )


if __name__ == "__main__":
    main()
