"""Elis E5 check: how long the exit joins the pool with queued reads ahead, hook on or off.

Seeds a throwaway cache with eight tiny driver rows, slows each worker row
read to 0.4 s, starts the reads ahead on two lanes and exits; an atexit
handler (atexit runs after threading's exit hooks, i.e. after the pool's
join) prints how long the join took. ``--no-hook`` replaces the cancel hook
by a no-op: the join then waits for every queued read.

Usage (from the repo root):
    PYTHONPATH=src python tools/perf/elis_e5_exit_check.py [--no-hook]
"""

from __future__ import annotations

import atexit
import sys
import tempfile
import time
from pathlib import Path

import tif1.cache as cache_mod
from tif1.cache import Cache
from tif1.core_utils.helpers import _create_telemetry_df


def main() -> None:
    if "--no-hook" in sys.argv:
        cache_mod._cancel_all_frame_prefetches = lambda: None
    cache = Cache(cache_dir=Path(tempfile.mkdtemp(prefix="e5-exit-")))
    rows = []
    for i in range(8):
        payload = {
            "Time": [0.0, 0.1],
            "RPM": [1.0, 2.0],
            "Speed": [float(i), 1.0],
            "nGear": [7, 7],
            "Throttle": [1.0, 1.0],
            "Brake": [0, 0],
            "DRS": [0, 0],
            "DriverAhead": ["44", "44"],
        }
        rows.append((f"D{i}", 1, _create_telemetry_df(payload, f"D{i}", 1, "pandas")))
    cache.set_telemetry_frames_batch(2026, "GP", "Race", rows)
    cache.conn.commit()
    real_read = cache_mod._read_frame_row

    def slow(*args):
        time.sleep(0.4)
        return real_read(*args)

    cache_mod._read_frame_row = slow
    assert cache.prefetch_telemetry_frame_rows(2026, "GP", "Race", [d for d, _, _ in rows])
    time.sleep(0.05)
    end = time.perf_counter()
    atexit.register(lambda: print(f"pool join at exit: {time.perf_counter() - end:.2f} s"))


if __name__ == "__main__":
    main()
