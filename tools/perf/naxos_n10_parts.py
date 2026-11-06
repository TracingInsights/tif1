"""Naxos N10: per block kind cost of ``_group_parts`` over a race's 20 driver rows (no workers).

The rows are read and decompressed first; then each row's header/body split
and every block kind of ``_group_parts`` is timed on the main thread alone.

    TIF1_CACHE_DIR=/tmp/naxos/warm-N10 TIF1_OFFLINE_MODE=true \
        python tools/perf/naxos_n10_parts.py
"""

from __future__ import annotations

import time

import numpy as np

import tif1
from tif1 import cache as cache_mod
from tif1.core_utils import frame_codec as fc
from tif1.core_utils import pandas_blocks

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
laps = session.laps
drivers = sorted({d for d, _ in session._telemetry_lap_refs(laps)})
rows = cache_mod.get_cache().get_telemetry_frame_rows(2025, session.gp, session.session, drivers)
pandas_blocks._direct_blocks_ok()
fc._decode_str(["a"], np.zeros(3, dtype="int32"))  # warm pyarrow's take
T: dict[str, float] = {}


def add(name, t0):
    T[name] = T.get(name, 0.0) + (time.perf_counter() - t0) * 1000


for raw in rows.values():
    t0 = time.perf_counter()
    header, body = fc._writable_body(raw)
    add("writable_body", t0)
    for _columns, glaps, lengths, specs in header:
        total = int(sum(lengths))
        for spec in specs:
            t0 = time.perf_counter()
            fc._group_parts(body, [int(x) for x in glaps], total, [spec])
            add(spec[0], t0)
for k, v in T.items():
    print(f"{k:14s} {v:7.2f} ms")
