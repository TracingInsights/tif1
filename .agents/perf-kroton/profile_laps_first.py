"""cProfile the FIRST laps access in a fresh process (the real 66ms path)."""

import cProfile
import io
import os
import pstats
import sys

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"
sys.path.insert(0, "src")

import tif1

session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
pr = cProfile.Profile()
pr.enable()
laps = session.laps
pr.disable()
buf = io.StringIO()
pstats.Stats(pr, stream=buf).sort_stats("cumulative").print_stats(22)
print(buf.getvalue())
