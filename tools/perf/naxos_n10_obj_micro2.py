"""Naxos N10: split of the constant object block cost (alloc + None prefill vs the fill)."""

from __future__ import annotations

import time

import numpy as np

N = 43_536
REPS = 20


def bench(label, fn):
    keep = []
    t0 = time.perf_counter()
    for _ in range(REPS):
        keep.append(fn())
    print(f"{label:28s} {(time.perf_counter() - t0) * 1000:6.2f} ms / {REPS}")
    return keep


for _ in range(2):
    bench("np.empty int64 (3,N)", lambda: np.empty((3, N), dtype="int64"))
    bench("np.zeros int64 (3,N) touched", lambda: np.zeros((3, N), dtype="int64") + 0)
    bench("np.empty object (3,N)", lambda: np.empty((3, N), dtype=object))
    pre = bench("prefill only", lambda: np.empty((3, N), dtype=object))

    def fill(arr=iter(pre)):
        a = next(arr)
        a[0, :] = "merged"
        a[1, :] = "OnTrack"
        a[2, :] = "HAM"
        return a

    bench("fill only (pre-allocated)", fill)
