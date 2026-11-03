"""Elis E5 probe: timeline of the reads ahead against the lap loop after a warm ``load()``.

Records, per driver row read in a worker, when it started and ended (ms from
the loop's start) and how long each first lap waited in ``_PrefetchedRow.take``.

Usage (from the repo root, warm cache):
    TIF1_CACHE_DIR=/tmp/tif1-elis-warm-E5 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/elis_e5_timeline.py
"""

from __future__ import annotations

import threading
import time

import tif1
from tif1 import cache as cache_mod

T0 = [0.0]
READS: list[tuple[str, str, float, float]] = []
TAKES: list[tuple[str, float, bool]] = []


def _ms() -> float:
    return (time.perf_counter() - T0[0]) * 1000


def main() -> None:
    orig_read = cache_mod._read_frame_row

    def read(cache, table, key, driver, *rest):
        start = _ms()
        out = orig_read(cache, table, key, driver, *rest)
        READS.append((driver, threading.current_thread().name[-3:], start, _ms()))
        return out

    cache_mod._read_frame_row = read
    orig_take = cache_mod._PrefetchedRow.take

    def take(self):
        start = time.perf_counter()
        started = self.future.running() or self.future.done()
        out = orig_take(self)
        TAKES.append((self.args[3], (time.perf_counter() - start) * 1000, started))
        return out

    cache_mod._PrefetchedRow.take = take
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
    session.load()
    laps = session.laps
    T0[0] = time.perf_counter()
    firsts = {}
    for _, lap in laps.iterlaps():
        _ = lap.telemetry
        firsts.setdefault(lap["Driver"], _ms())
    print(f"loop {_ms():.1f} ms")
    for driver, thread, start, end in sorted(READS, key=lambda r: r[2]):
        print(f"read {driver} [{thread}] {start:6.1f} -> {end:6.1f} ({end - start:4.1f})")
    for driver, wait, started in TAKES:
        print(f"take {driver} wait {wait:5.1f} started={started} first lap {firsts[driver]:6.1f}")


if __name__ == "__main__":
    main()
