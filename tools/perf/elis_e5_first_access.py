"""Elis E5 probe: where the first lazy lap access of a warm ``load()`` spends its time.

Usage (from the repo root, warm cache):
    TIF1_CACHE_DIR=/tmp/tif1-elis-warm-E5 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/elis_e5_first_access.py
"""

from __future__ import annotations

import time

import tif1
from tif1 import cache as cache_mod

TIMES: dict[str, list] = {"rows": [], "decode": []}
_orig_load = cache_mod.LazyDriverFrameSource._load
_orig_rows = cache_mod.Cache.get_telemetry_frame_rows


def _rows(self, *args, **kwargs):
    t0 = time.perf_counter()
    out = _orig_rows(self, *args, **kwargs)
    TIMES["rows"].append(round((time.perf_counter() - t0) * 1000, 2))
    return out


def _load(self):
    t0 = time.perf_counter()
    out = _orig_load(self)
    TIMES["decode"].append((self.driver, round((time.perf_counter() - t0) * 1000, 2)))
    return out


def main() -> None:
    cache_mod.Cache.get_telemetry_frame_rows = _rows
    cache_mod.LazyDriverFrameSource._load = _load
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
    session.load()
    laps = session.laps
    it = laps.iterlaps()
    t0 = time.perf_counter()
    _, lap = next(it)
    t1 = time.perf_counter()
    _ = lap.telemetry
    t2 = time.perf_counter()
    _, lap = next(it)
    _ = lap.telemetry
    t3 = time.perf_counter()
    print(f"first next() {(t1 - t0) * 1000:.1f} ms, first lap.telemetry {(t2 - t1) * 1000:.1f}")
    print(f"second lap {(t3 - t2) * 1000:.1f} ms")
    print("lone row read (get_telemetry_frame_rows)", TIMES["rows"])
    print("first _load (read + decode)", TIMES["decode"][:2])


if __name__ == "__main__":
    main()
