"""Kroton: results first-touch + laps dtype-pass sub-costs."""

import cProfile
import io
import os
import pstats
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

import tif1

s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
s.load(telemetry=False, weather=True, messages=True)

# results first-touch
for label, attr in (("results", "results"),):
    t0 = time.perf_counter()
    r = getattr(s, attr)
    print(f"first-touch {label}: {(time.perf_counter() - t0)*1000:.1f} ms")

# laps first-touch with sub-profile
pr = cProfile.Profile()
pr.enable()
laps = s.laps
pr.disable()
out = io.StringIO()
pstats.Stats(pr, stream=out).sort_stats("cumulative").print_stats(20)
for ln in out.getvalue().splitlines():
    if "tif1" in ln or "seconds" in ln or "pandas" in ln or "to_datetime" in ln:
        print(ln)
