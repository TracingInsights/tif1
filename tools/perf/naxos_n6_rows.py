"""Naxos N6 probe: the cost of ``iterlaps``' row interleave and ``Lap`` construction.

Usage (from the tree root, offline, warm cache):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_rows.py
"""

from __future__ import annotations

import time


def _t(label, fn, reps=5):
    best = float("inf")
    out = None
    for _ in range(reps):
        t0 = time.perf_counter()
        out = fn()
        best = min(best, time.perf_counter() - t0)
    print(f"  {label:44s} {best * 1000:8.3f} ms")
    return out


def main() -> None:
    import numpy as np

    import tif1
    from tif1 import models

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    laps = session.laps
    print(f"laps {laps.shape}")
    for blk in laps._mgr.blocks:
        print(f"  block {blk.dtype!s:28s} ncols={len(blk.mgr_locs)} {type(blk).__name__}")
    t0 = time.perf_counter()
    laps.to_numpy(dtype=object)
    print(f"first to_numpy(object) {(time.perf_counter() - t0) * 1000:.2f} ms")
    _t("to_numpy(object)", lambda: laps.to_numpy(dtype=object))
    for blk in laps._mgr.blocks:
        _t(f"block {blk.dtype} astype(object)", lambda blk=blk: blk.get_values(np.dtype(object)))
    values = laps.to_numpy(dtype=object)
    cols = laps.columns
    sess = laps.session
    idx = laps.index
    _t(
        "_lap_from_row x n",
        lambda: [
            models._lap_from_row(values[i].copy(), cols, idx[i], sess) for i in range(len(laps))
        ],
    )
    lap_rows = [models._lap_from_row(values[i].copy(), cols, idx[i], sess) for i in range(10)]
    _t(
        "maybe_convert_objects x n",
        lambda: [
            models._maybe_convert_objects(
                values[i],
                convert_numeric=False,
                convert_non_numeric=True,
                convert_to_nullable_dtype=False,
                dtype_if_all_nat=models._DTYPE_IF_ALL_NAT,
            )
            for i in range(len(laps))
        ],
    )
    lap = lap_rows[3]
    _t(
        "lap.get Driver+LapNumber x n",
        lambda: [(lap.get("Driver"), lap.get("LapNumber")) for _ in range(len(laps))],
    )
    _t(
        "ultra_cold resolve x n",
        lambda: [session._resolve_telemetry_ultra_cold_mode(None) for _ in range(len(laps))],
    )
    print("row types:", [type(v).__name__ for v in values[3]])


if __name__ == "__main__":
    main()
