"""Naxos N6 probe: modules imported and time spent in the notebook's ``load`` and picks.

Mirrors ``delphi_warm_bench.py``'s notebook steps up to ``picks_x8``.

Usage (from the tree root, offline, warm cache; PYTHONPATH picks the tree):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_load_imports.py
"""

from __future__ import annotations

import sys
import time


def main() -> None:
    import tif1

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    before = set(sys.modules)
    t0 = time.perf_counter()
    session.load(weather=True, messages=True)
    t1 = time.perf_counter()
    in_load = sorted(set(sys.modules) - before)
    laps = session.laps
    before = set(sys.modules)
    t2 = time.perf_counter()
    _ = [
        laps.pick_driver("VER"),
        laps.pick_drivers(["VER", "LEC"]),
        laps.pick_quicklaps(),
        laps.pick_fastest(),
        laps.pick_laps([5, 6]),
        laps.pick_compounds(["SOFT"]),
        laps.pick_wo_box(),
        laps.pick_track_status("1"),
    ]
    t3 = time.perf_counter()
    in_picks = sorted(set(sys.modules) - before)
    print(f"load {1000 * (t1 - t0):.1f} ms imports {in_load}")
    print(f"picks {1000 * (t3 - t2):.1f} ms imports {in_picks}")


if __name__ == "__main__":
    main()
