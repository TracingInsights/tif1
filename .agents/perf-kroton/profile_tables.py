"""Component profile of warm get_session / laps / weather / rcm table loads."""

import cProfile
import io
import os
import pstats
import sys
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"
sys.path.insert(0, "src")

import tif1

t0 = time.perf_counter()
session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
print(f"get_session: {time.perf_counter()-t0:.4f} s")

for attr, label in (("laps", "laps"), ("weather", "weather"), ("race_control_messages", "rcm")):
    t0 = time.perf_counter()
    obj = getattr(session, attr)
    dt = time.perf_counter() - t0
    print(f"{label}: {dt:.4f} s  rows={len(obj) if obj is not None else 0}")

pr = cProfile.Profile()
s2 = tif1.get_session(2026, "Monaco Grand Prix", "Race")
pr.enable()
l2 = s2.laps
pr.disable()
buf = io.StringIO()
pstats.Stats(pr, stream=buf).sort_stats("cumulative").print_stats(18)
print(buf.getvalue())

pr = cProfile.Profile()
s3 = tif1.get_session(2026, "Monaco Grand Prix", "Race")
pr.enable()
w3 = s3.weather
r3 = s3.race_control_messages
pr.disable()
buf = io.StringIO()
pstats.Stats(pr, stream=buf).sort_stats("cumulative").print_stats(18)
print(buf.getvalue())
