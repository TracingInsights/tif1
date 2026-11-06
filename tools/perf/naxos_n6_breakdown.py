"""Naxos N6 probe: where the pandas ``iterlaps`` telemetry loop spends its time.

Wraps the loop's stages with perf_counter timers (no cProfile), runs the
notebook's loop once after ``load(weather, messages)`` and once more (every
lap a memo hit), and prints the time per stage.

Usage (from the tree root, offline, warm cache):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_breakdown.py
"""

from __future__ import annotations

import time
from collections import defaultdict

TOTALS: dict[str, float] = defaultdict(float)
COUNTS: dict[str, int] = defaultdict(int)
DEPTH: dict[str, int] = defaultdict(int)


def _wrap(owner, name, label):
    fn = getattr(owner, name)

    def wrapper(*args, **kwargs):
        if DEPTH[label]:
            return fn(*args, **kwargs)
        DEPTH[label] += 1
        t0 = time.perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            TOTALS[label] += time.perf_counter() - t0
            COUNTS[label] += 1
            DEPTH[label] -= 1

    setattr(owner, name, wrapper)


def main() -> None:
    import tif1
    from tif1 import cache, core, models
    from tif1.core_utils import frame_codec

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    session.load(weather=True, messages=True)
    laps = session.laps

    _wrap(models, "_lap_from_row", "iterlaps._lap_from_row")
    _wrap(cache, "hydrate_lazy_sources", "hydrate_batch")
    _wrap(cache.Cache, "get_telemetry_frame_rows", "lone_row_read")
    _wrap(frame_codec.DriverFrameSource, "__init__", "DriverFrameSource.__init__")
    _wrap(frame_codec._LapBuilder, "build", "_LapBuilder.build")
    _wrap(frame_codec.DriverFrameSource, "_builder", "DriverFrameSource._builder")
    _wrap(core.Session, "_adopt_telemetry_frame", "adopt")
    _wrap(core.Session, "_session_telemetry", "session_telemetry")
    _wrap(core.Session, "_get_telemetry_df_for_ref", "get_telemetry_df_for_ref")
    _wrap(cache.SessionMemo, "_materialize_telemetry", "materialize")
    orig_tel = models.Lap.telemetry.fget

    def tel(self):
        t0 = time.perf_counter()
        try:
            return orig_tel(self)
        finally:
            TOTALS["Lap.telemetry"] += time.perf_counter() - t0
            COUNTS["Lap.telemetry"] += 1

    models.Lap.telemetry = property(tel)
    orig_to_numpy = models.Laps.to_numpy

    def to_numpy(self, *a, **k):
        t0 = time.perf_counter()
        try:
            return orig_to_numpy(self, *a, **k)
        finally:
            TOTALS["Laps.to_numpy"] += time.perf_counter() - t0
            COUNTS["Laps.to_numpy"] += 1

    models.Laps.to_numpy = to_numpy

    for label in ("first", "second"):
        TOTALS.clear()
        COUNTS.clear()
        t0 = time.perf_counter()
        out = [lap.telemetry for _, lap in laps.iterlaps()]
        dt = (time.perf_counter() - t0) * 1000
        print(f"== {label} loop: {len(out)} frames, {dt:.1f} ms")
        for key in sorted(TOTALS, key=lambda k: -TOTALS[k]):
            print(f"  {key:32s} {TOTALS[key] * 1000:8.2f} ms  n={COUNTS[key]}")
    t0 = time.perf_counter()
    rows = list(laps.iterlaps())
    print(f"== iterlaps alone: {(time.perf_counter() - t0) * 1000:.1f} ms ({len(rows)})")


if __name__ == "__main__":
    main()
