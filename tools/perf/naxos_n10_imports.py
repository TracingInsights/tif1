"""Naxos N10: modules first imported inside a warm pandas ``fetch_all`` and per-call costs."""

from __future__ import annotations

import sys
import time

import tif1

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
session.laps
before = set(sys.modules)
from tif1.core_utils import frame_codec, pandas_blocks  # noqa: E402

calls: list[float] = []
orig = frame_codec._decode_str


def timed(*a):
    t0 = time.perf_counter()
    try:
        return orig(*a)
    finally:
        calls.append((time.perf_counter() - t0) * 1000)


frame_codec._decode_str = timed
t0 = time.perf_counter()
pandas_blocks._direct_blocks_ok()
print("probe ms", round((time.perf_counter() - t0) * 1000, 2))
mid = set(sys.modules)
print("imported by probe:", sorted(mid - before))
t0 = time.perf_counter()
session.fetch_all_laps_telemetry()
print("fetch_all ms", round((time.perf_counter() - t0) * 1000, 2))
print("imported by fetch_all:", sorted(set(sys.modules) - mid))
print("decode_str ms per call:", [round(c, 2) for c in calls])
