"""KR-3 probe: cProfile the real laps first-touch (payload path, no tier)."""

import cProfile
import io
import os
import pstats

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

import tif1  # noqa: E402

pr = cProfile.Profile()
pr.enable()
s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
laps = s.laps
pr.disable()
out = io.StringIO()
pstats.Stats(pr, stream=out).sort_stats("cumulative").print_stats(22)
for ln in out.getvalue().splitlines():
    if "tif1" in ln or "seconds" in ln or "ncalls" in ln or "pandas" in ln:
        print(ln)
