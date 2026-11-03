"""Elis E5 probe: the lazy-row touch order and per-access timings after a warm ``load()``.

Usage (from the repo root, warm cache):
    TIF1_CACHE_DIR=/tmp/tif1-elis-warm-E5 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/elis_e5_probe.py
"""

from __future__ import annotations

import time

import tif1
from tif1.cache import LazyDriverFrameSource


def main() -> None:
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
    session.load()
    laps = session.laps
    memo = session._memo
    sql_order = list(
        dict.fromkeys(
            src.driver
            for src in memo._telemetry_sources.values()
            if isinstance(src, LazyDriverFrameSource)
        )
    )
    lap_order = list(dict.fromkeys(laps["Driver"]))
    print("sql order ", sql_order)
    print("laps order", lap_order)
    seen: set[str] = set()
    t_loop = time.perf_counter()
    t_prev = t_loop
    for _, lap in laps.iterlaps():
        driver = lap["Driver"]
        _ = lap.telemetry
        if driver not in seen:
            seen.add(driver)
            now = time.perf_counter()
            print(
                f"{driver} first lap at {(now - t_loop) * 1000:7.1f} ms (+{(now - t_prev) * 1000:.1f})"
            )
            t_prev = now
    print(f"loop {(time.perf_counter() - t_loop) * 1000:.1f} ms")


if __name__ == "__main__":
    main()
