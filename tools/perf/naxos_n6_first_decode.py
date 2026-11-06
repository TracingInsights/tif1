"""Naxos N6 probe: what the process's first ``DriverFrameSource`` decode pays.

Usage (from the tree root, offline, warm cache):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_first_decode.py [--profile]
"""

from __future__ import annotations

import cProfile
import pstats
import sys
import time


def main() -> None:
    import tif1
    from tif1.cache import get_cache
    from tif1.core_utils import frame_codec

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    laps = session.laps
    cache = get_cache()
    rows = cache.get_telemetry_frame_rows(session.year, session.gp, session.session, ["VER", "LEC"])
    prof = cProfile.Profile() if "--profile" in sys.argv else None
    if prof:
        prof.enable()
    t0 = time.perf_counter()
    src = frame_codec.DriverFrameSource(rows["VER"], tif1.models.Telemetry)
    t1 = time.perf_counter()
    src.frame(src.laps[1])
    t2 = time.perf_counter()
    src.frame(src.laps[2])
    t3 = time.perf_counter()
    src2 = frame_codec.DriverFrameSource(rows["LEC"], tif1.models.Telemetry)
    t4 = time.perf_counter()
    if prof:
        prof.disable()
    print(
        f"first decode {1000 * (t1 - t0):.2f}  first build {1000 * (t2 - t1):.2f}  "
        f"second build {1000 * (t3 - t2):.3f}  second decode {1000 * (t4 - t3):.2f} ms"
    )
    _ = (laps, src2)
    if prof:
        pstats.Stats(prof).sort_stats("cumulative").print_stats(30)


if __name__ == "__main__":
    main()
