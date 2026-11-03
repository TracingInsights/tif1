"""Fresh-process probe of the first-call costs on the candidate's merged path.

Usage: ``python -I str_probe.py [driver]`` (candidate tree, warm cache D8).
Times, one after another in a fresh process: the SQLite read + decompress,
the source decode, ``_fast_arrays()`` (its two probes), the str gather of
the merged spans, the numpy gather (with the minor page faults it causes),
then the same gathers again (warm).
"""

from __future__ import annotations

import os
import resource
import sys
import time

sys.path.insert(0, "/tmp/tif1-delphi/D8/src")
os.environ["TIF1_OFFLINE_MODE"] = "true"
os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-delphi-warm-D8"
DRIVER = sys.argv[1] if len(sys.argv) > 1 else "VER"

import numpy as np  # noqa: E402

import tif1  # noqa: E402
from tif1.cache import get_cache  # noqa: E402
from tif1.core_utils import frame_codec  # noqa: E402


def ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000, 2)


def faults() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_minflt


session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
laps = session.laps
drv = laps.pick_driver(DRIVER)
lap_numbers = [int(n) for n in drv["LapNumber"]]

cache = get_cache()
t0 = time.perf_counter()
f0 = faults()
sources = cache.get_telemetry_frame_sources(
    session.year, session.gp, session.session, [DRIVER], frame_class=None
)
print("read+decompress+decode", ms(t0), "faults", faults() - f0)
source = sources[DRIVER]
print("groups", len(source._groups), "laps", len(source.laps))

t0 = time.perf_counter()
frame_codec._probe_str_slice()
print("_probe_str_slice (first)", ms(t0))
t0 = time.perf_counter()
frame_codec._probe_timedelta()
print("_probe_timedelta (first)", ms(t0))
t0 = time.perf_counter()
frame_codec._fast_arrays()
print("_fast_arrays (memoized)", ms(t0))

# Spans as merged(drop_repeats=True) computes them.
g = source._where[lap_numbers[0]][0]
columns, _laps, offsets, parts = source._groups[g]
clock = source._clock(g)
spans = []
stretch = offsets[0]
for j in range(1, len(offsets) - 1):
    row = offsets[j]
    if clock[row] == clock[row - 1]:
        spans.append((stretch, row))
        stretch = row + 1
spans.append((stretch, offsets[-1]))
print("spans", len(spans), "rows", sum(e - s for s, e in spans))

str_part = next(p for p in parts if p[0] == "str")
ea = str_part[2]
for label in ("first", "second"):
    t0 = time.perf_counter()
    out = frame_codec._str_spans(ea, spans)
    print(f"_str_spans ({label})", ms(t0), "chunks", out._pa_array.num_chunks)
import pyarrow as pa  # noqa: E402

t0 = time.perf_counter()
chunks = [c for b, e in spans for c in ea._pa_array.slice(b, e - b).chunks]
print("  slice chunks", ms(t0))
t0 = time.perf_counter()
ca = pa.chunked_array(chunks, type=ea._pa_array.type)
print("  pa.chunked_array", ms(t0))
t0 = time.perf_counter()
ea._from_pyarrow_array(ca)
print("  _from_pyarrow_array", ms(t0))
t0 = time.perf_counter()
type(ea)(ca)
print("  ArrowStringArray(ca)", ms(t0))
t0 = time.perf_counter()
type(ea)._concat_same_type([ea[b:e] for b, e in spans])
print("  control: getitem x spans + _concat_same_type", ms(t0))

np_part = next(p for p in parts if p[0] == "np" and p[2].dtype == np.float64)
values = np_part[2]
print("float block", values.shape, values.nbytes // 1024, "KiB")
for label in ("first", "second", "third"):
    f0 = faults()
    t0 = time.perf_counter()
    out = frame_codec._gather_spans(values, spans)
    print(f"_gather_spans ({label})", ms(t0), "faults", faults() - f0)
    del out
f0 = faults()
t0 = time.perf_counter()
buf = np.empty(values.shape, dtype=values.dtype)
buf.fill(0)
print("np.empty+fill same size", ms(t0), "faults", faults() - f0)
# In-place move (control's _take_spans) on a copy of the block.
work = values.copy()
f0 = faults()
t0 = time.perf_counter()
begin = spans[0][0]
end = begin
for start, stop in spans:
    length = stop - start
    if start != end:
        work[..., end : end + length] = work[..., start:stop]
    end += length
print("in-place move", ms(t0), "faults", faults() - f0)
