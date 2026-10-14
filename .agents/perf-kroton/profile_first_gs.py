"""Kroton: profile the first (cold) get_session call — the 334ms schedule load."""

import cProfile
import io
import os
import pstats

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

import tif1

pr = cProfile.Profile()
pr.enable()
s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
pr.disable()
out = io.StringIO()
pstats.Stats(pr, stream=out).sort_stats("cumulative").print_stats(25)
text = out.getvalue()
for ln in text.splitlines():
    if "tif1" in ln or "seconds" in ln or "calls" in ln:
        print(ln)
