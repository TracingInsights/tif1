"""Naxos N6 probe: which notebook phase each cyclic GC pass lands in.

Usage (from the tree root, offline, warm cache; PYTHONPATH picks the tree):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_gc_phases.py
"""

from __future__ import annotations

import gc
import time


def main() -> None:
    phase = ["import"]
    log: dict[str, list[tuple[int, float]]] = {}
    start = [0.0]

    def callback(event, info):
        if event == "start":
            start[0] = time.perf_counter()
        else:
            log.setdefault(phase[0], []).append(
                (info["generation"], round((time.perf_counter() - start[0]) * 1000, 2))
            )

    gc.callbacks.append(callback)
    import tif1

    phase[0] = "get_session"
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    phase[0] = "load"
    session.load(weather=True, messages=True)
    laps = session.laps
    phase[0] = "picks"
    _ = [laps.pick_driver("VER"), laps.pick_quicklaps(), laps.pick_wo_box()]
    phase[0] = "loop"
    _ = [lap.telemetry for _, lap in laps.iterlaps()]
    phase[0] = "iloc"
    _ = [laps.iloc[i] for i in range(200)]
    gc.callbacks.remove(callback)
    for name, events in log.items():
        big = [e for e in events if e[0] > 0]
        print(f"{name:12s} gen0 x{sum(1 for e in events if e[0] == 0)}  gen1/2: {big}")


if __name__ == "__main__":
    main()
