"""KR-10 workload: polars session, time first `session.laps` access."""

import os
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

import tif1

s = tif1.get_session(2026, "Monaco Grand Prix", "Race", lib="polars")
t0 = time.perf_counter()
laps = s.laps
dt = time.perf_counter() - t0
print(f"LAPSPL_MS {dt * 1000:.1f} rows={laps.height}")
