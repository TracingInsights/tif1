"""Detailed warm laps-phase profile (env set before tif1 import)."""

from __future__ import annotations

import os

os.environ["TIF1_CACHE_DIR"] = "/tmp/perf-v1"

import cProfile
import io
import pstats

import tif1

session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
pr = cProfile.Profile()
pr.enable()
laps = session.laps
pr.disable()
out = io.StringIO()
stats = pstats.Stats(pr, stream=out).sort_stats("cumulative")
stats.print_stats(30)
print(out.getvalue())
