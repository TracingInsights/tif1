"""Naxos N10: replicate ``Cache._get_driver_frames`` step by step with timers (fresh process)."""

from __future__ import annotations

import sys
import time

import tif1
from tif1 import cache as cache_mod

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
laps = session.laps
refs = session._telemetry_lap_refs(laps)
frame_class = session._telemetry_frame_class()
c = cache_mod.get_cache()
T = {}
t = time.perf_counter()


def mark(name):
    global t
    now = time.perf_counter()
    T[name] = T.get(name, 0) + (now - t) * 1000
    t = now


if "--probe-first" in sys.argv:
    from tif1.core_utils import pandas_blocks

    pandas_blocks._direct_blocks_ok()
    mark("probe")
from tif1.core_utils.frame_codec import DriverFrameSource  # noqa: E402 - timed import
from tif1.core_utils.json_utils import json_loads  # noqa: E402

mark("import codec")
wanted = {}
for driver, lap in refs:
    wanted.setdefault(driver, set()).add(int(lap))
placeholders = ", ".join("?" for _ in wanted)
stored = c.conn.execute(
    f"SELECT driver, laps, rowid, length(frame) FROM {cache_mod._PANDAS_DRIVER_TABLE} "
    f"WHERE year = ? AND gp = ? AND session = ? AND driver IN ({placeholders})",
    (2025, session.gp, session.session, *wanted),
).fetchall()
useful = [(d, r, n) for d, lj, r, n in stored if wanted[d].intersection(json_loads(lj))]
mark("index query")
pending = cache_mod._submit_frame_row_reads(
    c, cache_mod._PANDAS_DRIVER_TABLE, (2025, session.gp, session.session), useful
)
mark("submit")
results = {}
with cache_mod._suspend_gc():
    it = cache_mod._completed_frame_rows(pending)
    if "--collect-first" in sys.argv:
        it = iter(list(it))
        mark("collect all rows")
    while True:
        try:
            driver, raw = next(it)
        except StopIteration:
            break
        mark("wait")
        src = DriverFrameSource(raw, frame_class)
        mark("source")
        frames = src.frames(wanted[driver])
        mark("frames")
        for lap, frame in frames.items():
            results[(driver, lap)] = frame
        mark("dict")
mark("gc resume")
print({k: round(v, 2) for k, v in T.items()}, len(results))
