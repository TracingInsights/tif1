"""Naxos N8: fresh-process cost of building one driver's 57 polars lap frames.

The post-wave part of a cold polars ``pick_driver("VER").telemetry``: each
lap's native-decoded payload becomes a polars frame through
``helpers._create_telemetry_df``. The payloads come from the payload tier of
a warm cache (offline, read only), so the measurement has no network. Each
run is a fresh interpreter; the arms (``--src`` trees) are interleaved.

Usage:
    TIF1_CACHE_DIR=/tmp/naxos/warm-N8 python tools/perf/naxos_n8_polars_build.py \
        --src /tmp/naxos/control/src --src src --reps 10
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

GP = "Bahrain%20Grand%20Prix"


def _child() -> None:
    import polars as pl  # noqa: F401  (a cold polars session has polars and numpy loaded)

    from tif1.cache import get_cache
    from tif1.core_utils.helpers import _create_telemetry_df

    refs = [("VER", lap) for lap in range(1, 58)]
    payloads = get_cache().get_telemetry_batch(2025, GP, "Race", refs)
    t0 = time.perf_counter()
    frames = [
        _create_telemetry_df(payloads[ref], ref[0], ref[1], "polars", clock=(ref[1] * 10**11, 0))
        for ref in refs
    ]
    build = (time.perf_counter() - t0) * 1000
    assert all(f is not None and f.height > 0 for f in frames)
    digest = [
        (f.columns, [str(d) for d in f.dtypes], f["DriverAhead"].to_list()[:50]) for f in frames
    ]
    print(json.dumps({"build_ms": build, "digest": hash(repr(digest))}))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--src", action="append", default=[])
    parser.add_argument("--reps", type=int, default=10)
    args = parser.parse_args()
    if args.child:
        _child()
        return
    results: dict[str, list[float]] = {src: [] for src in args.src}
    digests: dict[str, set] = {src: set() for src in args.src}
    for i in range(args.reps):
        order = args.src if i % 2 == 0 else args.src[::-1]
        for src in order:
            env = dict(
                os.environ,
                PYTHONPATH=str(Path(src).resolve()),
                TIF1_OFFLINE_MODE="true",
                PYTHONHASHSEED="0",
            )
            proc = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--child"],
                capture_output=True,
                text=True,
                env=env,
                check=True,
            )
            rec = json.loads(proc.stdout.strip().splitlines()[-1])
            results[src].append(rec["build_ms"])
            digests[src].add(rec["digest"])
    for src, vals in results.items():
        print(
            f"{src}: build median {statistics.median(vals):.1f} ms min {min(vals):.1f} "
            f"digests {sorted(digests[src])}"
        )


if __name__ == "__main__":
    main()
