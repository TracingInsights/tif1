"""Instrument the candidate cold telemetry phase: fetch wave vs flush vs assembly."""

from __future__ import annotations

import os
import tempfile
import time

os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-phase-")

import tif1
import tif1.async_fetch as af
from tif1.core_utils.helpers import _create_telemetry_df

T0 = time.perf_counter()
marks: list[tuple[str, float]] = []


def mark(name: str) -> None:
    marks.append((name, time.perf_counter() - T0))


_orig_flush = af._flush_telemetry_writes
_flush_total = [0.0]


def _timed_flush(cache, entries):
    t = time.perf_counter()
    _orig_flush(cache, entries)
    _flush_total[0] += time.perf_counter() - t


af._flush_telemetry_writes = _timed_flush

_orig_create = _create_telemetry_df
_create_total = [0.0]
_create_count = [0]


def _timed_create(tel_data, driver, lap_num, lib):
    t = time.perf_counter()
    out = _orig_create(tel_data, driver, lap_num, lib)
    _create_total[0] += time.perf_counter() - t
    _create_count[0] += 1
    return out


from tif1.core_utils import helpers  # noqa: E402

helpers._create_telemetry_df = _timed_create
from tif1 import core  # noqa: E402

core._create_telemetry_df = _timed_create

session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
mark("get_session")
_ = session.laps
mark("laps")
tel = session.fetch_all_laps_telemetry()
mark("telemetry_done")

print(f"frames: {len(tel)}")
print(f"flush (deferred cache writes): {_flush_total[0]:.2f} s")
print(f"assembly (_create_telemetry_df x{_create_count[0]}): {_create_total[0]:.2f} s")
for name, t in marks:
    print(f"  @{t:6.2f}s  {name}")
