"""Elis E7: in-process timers for the small warm pandas items.

Run from the worktree root with ``TIF1_CACHE_DIR`` on a primed cache and
``TIF1_OFFLINE_MODE=true``. Prints the median/min of each item (ms) over
``--reps`` repetitions; ``--profile ITEM`` runs cProfile over one item.

Items:
    pick_driver: ``laps.pick_driver("VER")``.
    driver_tel: ``laps.pick_driver("VER").telemetry`` (merged frame memo
        cleared each time, so the join runs every repetition).
    join: ``_join_lap_telemetry`` on VER's merged frame.
    results: ``session.results`` on a fresh session (ergast merge).
    split: ``laps.split_qualifying_sessions()`` on the qualifying laps.
    iloc_200: ``[laps.iloc[i] for i in range(200)]``.
"""

from __future__ import annotations

import argparse
import cProfile
import pstats
import statistics
import time

import tif1
from tif1 import models


def _timed(fn, reps: int) -> list[float]:
    out = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        out.append((time.perf_counter() - t0) * 1000)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reps", type=int, default=20)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--items", default="pick_driver,driver_tel,join,results,split,iloc_200")
    args = parser.parse_args()

    race = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    laps = race.laps
    quali = tif1.get_session(2025, "Bahrain Grand Prix", "Qualifying", lib="pandas")
    qlaps = quali.laps

    def driver_tel():
        picked = laps.pick_driver("VER")
        return picked.telemetry

    ver = laps.pick_driver("VER")
    tel = ver.telemetry
    start = models._first_lap_start_ns(ver)

    def join():
        return models._join_lap_telemetry(tel, start, multi_driver=False)

    def results():
        s = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
        _ = s.laps
        t0 = time.perf_counter()
        s.results  # noqa: B018
        return (time.perf_counter() - t0) * 1000

    items = {
        "pick_driver": lambda: laps.pick_driver("VER"),
        "driver_tel": driver_tel,
        "join": join,
        "results": results,
        "split": qlaps.split_qualifying_sessions,
        "iloc_200": lambda: [laps.iloc[i] for i in range(200)],
    }
    if args.profile:
        fn = items[args.profile]
        prof = cProfile.Profile()
        prof.enable()
        for _ in range(args.reps):
            fn()
        prof.disable()
        pstats.Stats(prof).sort_stats("cumulative").print_stats(40)
        return
    for name in args.items.split(","):
        fn = items[name]
        if name == "results":
            vals = [fn() for _ in range(args.reps)]
        else:
            vals = _timed(fn, args.reps)
        print(
            f"{name:12s} median={statistics.median(vals):7.3f} min={min(vals):7.3f} "
            f"first={vals[0]:7.3f}"
        )


if __name__ == "__main__":
    main()
