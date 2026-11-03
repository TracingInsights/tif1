"""Elis E7: ``laps.pick_driver("VER")`` and its ``.telemetry`` timed apart (fresh process)."""

from __future__ import annotations

import time

import tif1

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
laps = session.laps
t0 = time.perf_counter()
picked = laps.pick_driver("VER")
t1 = time.perf_counter()
tel = picked.telemetry
t2 = time.perf_counter()
print(f"pick {1000 * (t1 - t0):.2f} tel {1000 * (t2 - t1):.2f}")
