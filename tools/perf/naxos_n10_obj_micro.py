"""Naxos N10: ways to build a driver row's constant object block (3 x ~43 500, fresh memory)."""

from __future__ import annotations

import time

import numpy as np

N = 43_536
CONSTS = ["merged", "OnTrack", "HAM"]


def current():
    values = np.empty((len(CONSTS), N), dtype=object)
    for row, c in enumerate(CONSTS):
        values[row, :] = c
    return values


def repeat():
    return np.array(CONSTS, dtype=object).reshape(-1, 1).repeat(N, axis=1)


def broadcast_assign():
    values = np.empty((len(CONSTS), N), dtype=object)
    values[...] = np.array(CONSTS, dtype=object).reshape(-1, 1)
    return values


def main() -> None:
    for fn in (current, repeat, broadcast_assign) * 2:
        times = []
        keep = []
        for _ in range(20):  # 20 drivers, all kept: fresh memory each time
            t0 = time.perf_counter()
            keep.append(fn())
            times.append((time.perf_counter() - t0) * 1000)
        assert all((k == current()).all() for k in keep[:1])
        print(f"{fn.__name__:18s} 20 drivers {sum(times):6.2f} ms (median {sorted(times)[10]:.3f})")


if __name__ == "__main__":
    main()
