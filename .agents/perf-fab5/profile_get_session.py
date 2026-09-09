"""Profile the get_session + laps phases (schedule fetch, session construction)."""

from __future__ import annotations

import cProfile
import io
import os
import pstats
import tempfile
import time

os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-probe-")

import tif1

t0 = time.perf_counter()
session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
t1 = time.perf_counter()
print(f"get_session: {t1 - t0:.3f}s")

laps = session.laps
t2 = time.perf_counter()
print(f"laps: {t2 - t1:.3f}s rows={len(laps)}")

# second session (warm) for comparison
t0 = time.perf_counter()
s2 = tif1.get_session(2026, "Monaco Grand Prix", "Race")
print(f"get_session warm: {time.perf_counter() - t0:.3f}s")

pr = cProfile.Profile()
pr.enable()
s3 = tif1.get_session(2026, "Monaco Grand Prix", "Race")
pr.disable()
s = io.StringIO()
pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats(16)
print(s.getvalue())
