"""Fresh-process cost of the str column gather, control vs candidate, no wrappers.

Usage: ``python -I str_cost.py <control|candidate> [driver]``.
"""

from __future__ import annotations

import os
import sys
import time

TREES = {
    "control": ("/tmp/tif1-delphi-control/src", "/tmp/tif1-delphi-warm"),
    "candidate": ("/tmp/tif1-delphi/D8/src", "/tmp/tif1-delphi-warm-D8"),
}
tree = sys.argv[1] if len(sys.argv) > 1 else "candidate"
DRIVER = sys.argv[2] if len(sys.argv) > 2 else "VER"
src, cache_dir = TREES[tree]
sys.path.insert(0, src)
os.environ["TIF1_OFFLINE_MODE"] = "true"
os.environ["TIF1_CACHE_DIR"] = cache_dir

import tif1  # noqa: E402
from tif1.cache import get_cache  # noqa: E402
from tif1.core_utils import frame_codec  # noqa: E402


def ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000, 2)


session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
laps = session.laps
drv = laps.pick_driver(DRIVER)
lap_numbers = [int(n) for n in drv["LapNumber"]]
cache = get_cache()
conn = cache.conn
row = conn.execute(
    "SELECT frame FROM telemetry_frames_drv WHERE year = ? AND gp = ? "
    "AND session = ? AND driver = ?",
    (session.year, session.gp, session.session, DRIVER),
).fetchone()
from tif1 import cache as cache_mod  # noqa: E402

t0 = time.perf_counter()
raw = cache_mod._decompress_row((DRIVER, row[0]), cache_mod._pa_lz4_codec())[-1]
print("decompress", ms(t0))
t0 = time.perf_counter()
header, body = frame_codec._writable_body(raw)
print("writable_body", ms(t0))
group_columns, raw_laps, lengths, specs = header[0]
total = int(sum(lengths))
t0 = time.perf_counter()
parts = frame_codec._group_parts(body, [int(x) for x in raw_laps], total, specs)
print("group_parts", ms(t0))
str_part = next(p for p in parts if p[0] == "str")
ea = str_part[2]
# 57-ish spans: drop one row per lap boundary.
starts = [0]
for length in lengths:
    starts.append(starts[-1] + int(length))
spans = [(starts[0], starts[1])] + [(starts[i] + 1, starts[i + 1]) for i in range(1, len(lengths))]
print("spans", len(spans))
for label in ("first", "second", "third"):
    t0 = time.perf_counter()
    pieces = [ea[b:e] for b, e in spans]
    t_get = ms(t0)
    t0 = time.perf_counter()
    joined = type(ea)._concat_same_type(pieces)
    print(f"control str path ({label}): getitem x{len(spans)} {t_get} + concat {ms(t0)}")
t0 = time.perf_counter()
one = ea[spans[0][0] : spans[0][1]]
print("single getitem", ms(t0))
if hasattr(frame_codec, "_str_spans"):
    for label in ("first", "second"):
        t0 = time.perf_counter()
        out = frame_codec._str_spans(ea, spans)
        print(f"candidate _str_spans ({label})", ms(t0))
