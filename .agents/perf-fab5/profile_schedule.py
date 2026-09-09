"""Profile the schedule payload load (the cold cost inside get_session)."""

from __future__ import annotations

import cProfile
import io
import os
import pstats
import tempfile
import time

os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-sched-")

import tif1.events as ev

t0 = time.perf_counter()
events = ev.get_events(2026)
t1 = time.perf_counter()
print(f"get_events cold: {t1 - t0:.3f}s events={len(events)}")

t0 = time.perf_counter()
events = ev.get_events(2026)
print(f"get_events warm: {time.perf_counter() - t0:.3f}s")

# profile a fresh interpreter-equivalent: clear caches and re-run
ev._events_cache.clear() if hasattr(ev, "_events_cache") else None

pr = cProfile.Profile()
pr.enable()
ev._load_schedule_payload() if hasattr(ev, "_load_schedule_payload") else None
pr.disable()
s = io.StringIO()
pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats(16)
print(s.getvalue())
