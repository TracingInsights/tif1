"""Naxos N6 probe: stage timers of the warm ``car_data_loop`` workload (VER's laps).

Usage (from the tree root, offline, warm cache):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_car_loop.py
"""

from __future__ import annotations

import time
from collections import defaultdict

TOTALS: dict[str, float] = defaultdict(float)
COUNTS: dict[str, int] = defaultdict(int)


def _wrap(owner, name, label):
    fn = getattr(owner, name)

    def wrapper(*args, **kwargs):
        t0 = time.perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            TOTALS[label] += time.perf_counter() - t0
            COUNTS[label] += 1

    setattr(owner, name, wrapper)


def main() -> None:
    import tif1
    from tif1 import cache, core
    from tif1.core_utils import frame_codec

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    laps = session.laps
    drv = laps.pick_driver("VER")
    _wrap(core.Session, "_cached_payload_telemetry_df", "first lap from payload")
    _wrap(cache.Cache, "get_telemetry_frame_sources", "row read+decode")
    _wrap(frame_codec.DriverFrameSource, "__init__", "  decode")
    _wrap(frame_codec._LapBuilder, "build", "build")
    t0 = time.perf_counter()
    out = [lap.get_car_data() for _, lap in drv.iterlaps()]
    dt = (time.perf_counter() - t0) * 1000
    print(f"car_data_loop {len(out)} laps {dt:.1f} ms")
    for key, total in TOTALS.items():
        print(f"  {key:28s} {total * 1000:7.2f} ms n={COUNTS[key]}")


if __name__ == "__main__":
    main()
