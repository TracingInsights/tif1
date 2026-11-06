"""Corinth C4: trace one current-season script (get_session, laps, results).

Logs every tif1/niquests/urllib3 debug line with its offset and thread, then
the step timings. Run with ``TIF1_SCHEDULE_TTL_HOURS=0 TIF1_ERGAST_TTL_HOURS=0``
to see the expired-TTL refresh path.

Usage: python tools/perf/corinth_c4_trace.py <lib> [event] [session]
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time

if os.environ.get("C4_COLD") == "1":
    # Cold: a fresh empty cache directory per run.
    os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="c4cold-", dir=os.environ["HOME"])

t0 = time.perf_counter()
logging.basicConfig(
    level=logging.DEBUG,
    format="%(relativeCreated)7.1f %(threadName)-26s %(name)s: %(message)s",
    stream=sys.stderr,
)
for name in ("asyncio", "matplotlib", "PIL"):
    logging.getLogger(name).setLevel(logging.WARNING)

import tif1  # noqa: E402

out: dict[str, int] = {}


def step(name, func):
    start = time.perf_counter()
    result = func()
    out[name] = round((time.perf_counter() - start) * 1000)
    print(f"=== {name} done in {out[name]} ms", file=sys.stderr)
    return result


lib = sys.argv[1]
event = sys.argv[2] if len(sys.argv) > 2 else "Mexico City Grand Prix"
session_name = sys.argv[3] if len(sys.argv) > 3 else "R"
session = step("get_session", lambda: tif1.get_session(2026, event, session_name, lib=lib))
step("laps", lambda: session.laps)
step("results", lambda: session.results)
out["total"] = round((time.perf_counter() - t0) * 1000)
print(lib, out)
