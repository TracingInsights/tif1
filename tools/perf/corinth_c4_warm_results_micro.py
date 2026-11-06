"""Corinth C4: in-process warm ``session.results`` timing (Ergast memo cleared per iteration).

Usage: TIF1_CACHE_DIR=... PYTHONPATH=<tree>/src python tools/perf/corinth_c4_warm_results_micro.py
"""

from __future__ import annotations

import statistics
import sys
import time

import tif1
from tif1.ergast import payloads, stats

event = sys.argv[1] if len(sys.argv) > 1 else "British Grand Prix"
_ = tif1.get_session(2026, event, "R").results  # imports and first-touch costs
times = []
for _ in range(40):
    payloads.clear_memo()
    stats.reset_stats_source()
    session = tif1.get_session(2026, event, "R")
    _ = session.laps
    start = time.perf_counter()
    _ = session.results
    times.append((time.perf_counter() - start) * 1000)
print(f"median {statistics.median(times):.3f} ms  min {min(times):.3f} ms")
