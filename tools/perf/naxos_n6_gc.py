"""Naxos N6 probe: cyclic GC passes inside the pandas ``iterlaps`` telemetry loop.

Usage (from the tree root, offline, warm cache):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_gc.py [car]
"""

from __future__ import annotations

import gc
import sys
import time


def main() -> None:
    import tif1

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    car = "car" in sys.argv
    if not car:
        session.load(weather=True, messages=True)
    laps = session.laps
    events: list[tuple[int, float]] = []
    start: list[float] = [0.0]

    def callback(phase, info):
        if phase == "start":
            start[0] = time.perf_counter()
        else:
            events.append((info["generation"], (time.perf_counter() - start[0]) * 1000))

    gc.callbacks.append(callback)
    t0 = time.perf_counter()
    if car:
        drv = laps.pick_driver("VER")
        out = [lap.get_car_data() for _, lap in drv.iterlaps()]
    else:
        out = [lap.telemetry for _, lap in laps.iterlaps()]
    dt = (time.perf_counter() - t0) * 1000
    gc.callbacks.remove(callback)
    by_gen = {
        g: (sum(1 for e in events if e[0] == g), sum(e[1] for e in events if e[0] == g))
        for g in (0, 1, 2)
    }
    print(
        f"loop {len(out)} {dt:.1f} ms; gc by generation (count, ms): {by_gen}; objects {len(gc.get_objects())}"
    )


if __name__ == "__main__":
    main()
