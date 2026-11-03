"""Fresh-process stage timer for one driver's ``Laps.telemetry``.

Usage: ``python -I stages.py <control|candidate> [driver] [load]``.
The tree picks the source tree and its warm cache (control:
/tmp/tif1-delphi-control + /tmp/tif1-delphi-warm; candidate: this worktree
+ /tmp/tif1-delphi-warm-D8). ``load`` runs ``load()`` first (the notebook's
driver_merged path). Prints JSON: stage -> ms (cumulative over calls).
"""

from __future__ import annotations

import functools
import json
import os
import sys
import time

TREES = {
    "control": ("/tmp/tif1-delphi-control/src", "/tmp/tif1-delphi-warm"),
    "candidate": ("/tmp/tif1-delphi/D8/src", "/tmp/tif1-delphi-warm-D8"),
}
tree = sys.argv[1] if len(sys.argv) > 1 else "candidate"
DRIVER = sys.argv[2] if len(sys.argv) > 2 else "VER"
LOAD = "load" in sys.argv[3:]
src, cache_dir = TREES[tree]
sys.path.insert(0, src)
os.environ["TIF1_OFFLINE_MODE"] = "true"
os.environ["TIF1_CACHE_DIR"] = cache_dir

timings: dict[str, float] = {}


def timed(name):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*a, **k):
            t0 = time.perf_counter()
            try:
                return fn(*a, **k)
            finally:
                timings[name] = timings.get(name, 0.0) + (time.perf_counter() - t0) * 1000

        return wrapper

    return deco


t_proc = time.perf_counter()
import tif1  # noqa: E402

timings["import"] = (time.perf_counter() - t_proc) * 1000
assert tif1.__file__.startswith(src), tif1.__file__

from tif1 import cache as cache_mod  # noqa: E402
from tif1 import models  # noqa: E402
from tif1.core_utils import frame_codec  # noqa: E402

# Shared stages.
cache_mod._decompress_frame_rows = timed("decompress_rows")(cache_mod._decompress_frame_rows)
cache_mod._submit_frame_rows = timed("submit_rows")(cache_mod._submit_frame_rows)
cache_mod._completed_frame_rows = timed("completed_rows")(cache_mod._completed_frame_rows)
frame_codec._group_parts = timed("group_parts")(frame_codec._group_parts)
frame_codec._decode_str = timed("decode_str")(frame_codec._decode_str)
frame_codec._frame_from_blocks = timed("frame_from_blocks")(frame_codec._frame_from_blocks)
models._join_lap_telemetry = timed("join_lap_telemetry")(models._join_lap_telemetry)
models._with_session_clock = timed("with_session_clock")(models._with_session_clock)
models._first_lap_start_ns = timed("first_lap_start_ns")(models._first_lap_start_ns)
cache_mod.Cache.get_driver_merged_frame = timed("cache.get_driver_merged_frame")(
    cache_mod.Cache.get_driver_merged_frame
)
cache_mod.Cache.get_telemetry_frame_sources = timed("cache.get_telemetry_frame_sources")(
    cache_mod.Cache.get_telemetry_frame_sources
)
cache_mod.SessionMemo.add_telemetry_source = timed("memo.add_telemetry_source")(
    cache_mod.SessionMemo.add_telemetry_source
)
if hasattr(frame_codec, "decode_driver_merged"):
    frame_codec.decode_driver_merged = timed("decode_driver_merged")(
        frame_codec.decode_driver_merged
    )
if hasattr(frame_codec.DriverFrameSource, "merged"):
    frame_codec.DriverFrameSource.merged = timed("source.merged")(
        frame_codec.DriverFrameSource.merged
    )
    frame_codec.DriverFrameSource.__init__ = timed("source.__init__")(
        frame_codec.DriverFrameSource.__init__
    )
    frame_codec.DriverFrameSource._joined_pieces = timed("source._joined_pieces")(
        frame_codec.DriverFrameSource._joined_pieces
    )
    frame_codec.DriverFrameSource._piece_frame = timed("source._piece_frame")(
        frame_codec.DriverFrameSource._piece_frame
    )
if hasattr(models.Laps, "_merged_from_driver_source"):
    models.Laps._merged_from_driver_source = staticmethod(
        timed("laps._merged_from_driver_source")(models.Laps._merged_from_driver_source)
    )
if hasattr(frame_codec, "_take_spans"):
    frame_codec._take_spans = timed("take_spans")(frame_codec._take_spans)
if hasattr(frame_codec, "_gather_spans"):
    frame_codec._gather_spans = timed("gather_spans")(frame_codec._gather_spans)
if hasattr(frame_codec, "_str_spans"):
    frame_codec._str_spans = timed("str_spans")(frame_codec._str_spans)
    frame_codec._str_pieces = timed("str_pieces")(frame_codec._str_pieces)
_pd_concat = frame_codec.pd.concat
frame_codec.pd.concat = timed("pd.concat")(_pd_concat)
if hasattr(frame_codec, "_fast_arrays"):
    frame_codec._fast_arrays = timed("fast_arrays")(frame_codec._fast_arrays)
    frame_codec._probe_str_slice = timed("probe_str_slice")(frame_codec._probe_str_slice)
    frame_codec._probe_timedelta = timed("probe_timedelta")(frame_codec._probe_timedelta)
import pyarrow as pa  # noqa: E402

pa.chunked_array = timed("pa.chunked_array")(pa.chunked_array)
_asa = frame_codec.pd.arrays.ArrowStringArray
_asa.__getitem__ = timed("ArrowStringArray.__getitem__")(_asa.__getitem__)
_asa.__init__ = timed("ArrowStringArray.__init__")(_asa.__init__)
_asa._concat_same_type = classmethod(
    timed("ArrowStringArray._concat_same_type")(_asa._concat_same_type.__func__)
)

t0 = time.perf_counter()
session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
timings["get_session"] = (time.perf_counter() - t0) * 1000
if LOAD:
    t0 = time.perf_counter()
    session.load(weather=False, messages=False)
    timings["load"] = (time.perf_counter() - t0) * 1000
t0 = time.perf_counter()
laps = session.laps
timings["laps"] = (time.perf_counter() - t0) * 1000
t0 = time.perf_counter()
drv = laps.pick_driver(DRIVER)
timings["pick_driver"] = (time.perf_counter() - t0) * 1000
t0 = time.perf_counter()
tel = drv.telemetry
timings["driver_tel"] = (time.perf_counter() - t0) * 1000
timings["rows"] = len(tel)
print(json.dumps({k: round(v, 2) for k, v in timings.items()}))
