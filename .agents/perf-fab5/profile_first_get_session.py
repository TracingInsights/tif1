"""cProfile the FIRST get_session call in a fresh process."""

from __future__ import annotations

import cProfile
import io
import os
import pstats
import tempfile
import time

os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-sched-")

import tif1

pr = cProfile.Profile()
t0 = time.perf_counter()
pr.enable()
session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
pr.disable()
print(f"first get_session: {time.perf_counter() - t0:.3f}s")
s = io.StringIO()
pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats(18)
print(s.getvalue())
