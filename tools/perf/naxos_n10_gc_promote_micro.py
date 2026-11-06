"""Naxos N10: cost of the first collections after a GC-suspended burst, with and without promotion.

Pure Python, any interpreter. Builds a heap of long-lived containers (a
stand-in for pandas' import), then a burst of kept containers with GC off
(a stand-in for 1128 lap frames), re-enables GC and times the collections
the next allocations trigger. ``promote`` moves the young objects into the
oldest generation with ``gc.freeze(); gc.unfreeze()`` before re-enabling.

    python tools/perf/naxos_n10_gc_promote_micro.py
"""

from __future__ import annotations

import gc
import sys
import time


class Node:
    __slots__ = ("a", "b")

    def __init__(self, a, b):
        self.a = a
        self.b = b


def run(promote: bool, heap: int, burst: int) -> tuple[float, float, int]:
    gc.collect()
    keep = [{"k": [i]} for i in range(heap)]  # long-lived heap
    gc.collect()
    events: list[float] = []
    start: list[float] = []

    def cb(phase, info):
        if phase == "start":
            start.append(time.perf_counter())
        else:
            events.append((time.perf_counter() - start.pop()) * 1000)

    gc.disable()
    built = [Node([i], {"x": (i,)}) for i in range(burst)]
    t0 = time.perf_counter()
    if promote:
        gc.freeze()
        gc.unfreeze()
    promote_ms = (time.perf_counter() - t0) * 1000
    gc.callbacks.append(cb)
    gc.enable()
    t0 = time.perf_counter()
    tail = [[i] for i in range(20000)]  # allocations after the burst
    after_ms = (time.perf_counter() - t0) * 1000
    gc.callbacks.remove(cb)
    del keep, built, tail
    return promote_ms, after_ms, len(events)


def main() -> None:
    print(
        sys.version.split()[0],
        "GIL" if getattr(sys, "_is_gil_enabled", lambda: True)() else "noGIL",
    )
    for heap, burst in ((300_000, 60_000), (1_000_000, 60_000)):
        for promote in (False, True, False, True):
            p, a, n = run(promote, heap, burst)
            print(
                f"heap {heap:>9} burst {burst} promote={promote!s:5} "
                f"freeze+unfreeze {p:6.2f} ms, next 20k allocations {a:6.2f} ms ({n} collections)"
            )


if __name__ == "__main__":
    main()
