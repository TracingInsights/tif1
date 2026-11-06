"""Naxos N6 probe: the one-time costs a fresh process pays before its first lap frame.

Usage (from the tree root, offline, warm cache):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_probe_cost.py
"""

from __future__ import annotations

import sys
import time


def main() -> None:
    import tif1

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    _ = session.laps
    before = set(sys.modules)
    from tif1.core_utils import frame_codec, pandas_blocks

    t0 = time.perf_counter()
    pandas_blocks._direct_blocks_ok()
    t1 = time.perf_counter()
    mods = sorted(set(sys.modules) - before)
    t2 = time.perf_counter()
    frame_codec._fast_arrays()
    t3 = time.perf_counter()
    print(f"direct block probe {1000 * (t1 - t0):.2f} ms; new modules: {mods}")
    print(f"fast arrays probe {1000 * (t3 - t2):.2f} ms")


if __name__ == "__main__":
    main()
