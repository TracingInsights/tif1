"""Profile get_session + laps warm-load phases."""

from __future__ import annotations

import cProfile
import io
import os
import pstats
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/perf-v1"

import tif1

pr = cProfile.Profile()
pr.enable()
t0 = time.perf_counter()
session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
t_get = time.perf_counter() - t0
t0 = time.perf_counter()
laps = session.laps
t_laps = time.perf_counter() - t0
pr.disable()

print(f"get_session={t_get:.3f}s laps={t_laps:.3f}s")
out = io.StringIO()
stats = pstats.Stats(pr, stream=out).sort_stats("cumulative")
stats.print_stats(35)
print(out.getvalue())
