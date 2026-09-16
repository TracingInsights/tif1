"""R1/R3 micro-benchmark: full 1452-frame read loop variants.

Variants (all measured on real primed-cache frames):
- A  baseline loop: per-frame zstd + pickle.loads (current implementation shape)
- B  same with gc.disable() around the loop  (R3)
- C  one-blob pickle of list-of-dict-of-arrays + per-frame pd.DataFrame(...)  (R1 v2a)
- D  one-blob pickle + direct BlockManager construction  (R1 v2b)
- E  per-frame pickled dict-of-arrays (arrays pickle ~ as cheap as frames?)
"""

from __future__ import annotations

import gc
import json
import pickle
import sqlite3
import time

import numpy as np
import pandas as pd
import zstandard

WARM = "/tmp/tif1-warm-cache"
N_REPEAT = 3


def bench(fn, n=N_REPEAT):
    best = 1e9
    for _ in range(n):
        gc.collect()
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return round(best * 1000, 1)


def main() -> None:
    conn = sqlite3.connect(f"{WARM}/cache.sqlite")
    rows = conn.execute(
        "SELECT driver, lap, frame FROM telemetry_frames ORDER BY driver, lap"
    ).fetchall()
    dctx = zstandard.ZstdDecompressor()
    _zctx = zstandard.ZstdCompressor(level=1)  # write-side reference
    raw = [dctx.decompress(b) for _, _, b in rows]
    _keys = [tuple(r[:2]) for r in rows]  # ref list for later loops
    out: dict = {"n_frames": len(rows)}

    # A: baseline
    def loop_a():
        return [pickle.loads(r) for r in raw]

    out["A_baseline_loop_ms"] = bench(loop_a)

    # B: gc disabled
    def loop_b():
        gc.disable()
        try:
            return [pickle.loads(r) for r in raw]
        finally:
            gc.enable()

    out["B_gc_disabled_ms"] = bench(loop_b)

    # C: one-blob dict-of-arrays + pd.DataFrame construction
    frames = [pickle.loads(r) for r in raw]

    def frame_to_arrays(f: pd.DataFrame) -> dict:
        d = {}
        for c in f.columns:
            v = f[c].values
            if isinstance(v, pd.arrays.ArrowStringArray):
                d[c] = np.asarray(v.astype(object)) if False else v.to_numpy(dtype=object)
            else:
                d[c] = np.asarray(v)
        return d

    arr_dicts = [frame_to_arrays(f) for f in frames]
    meta = {"columns": list(frames[0].columns), "dtypes": [str(t) for t in frames[0].dtypes]}
    blob = pickle.dumps((meta, arr_dicts), protocol=5)
    out["C_blob_kb"] = len(blob) // 1024

    def loop_c():
        _, dicts = pickle.loads(blob)
        return [pd.DataFrame(d, copy=False) for d in dicts]

    out["C_onelob_df_construct_ms"] = bench(loop_c)

    # D: one-blob + direct BlockManager
    from pandas.core.internals.blocks import new_block_2d
    from pandas.core.internals.managers import BlockManager

    def build_fast(d):
        from pandas.core.internals.blocks import BlockPlacement

        n = len(d["Speed"])
        axes = [pd.Index(meta["columns"]), pd.RangeIndex(n)]
        blocks = []
        seen = set()
        for c in meta["columns"]:
            if c in seen:
                continue
            v = d[c]
            kind = v.dtype.kind
            cols = [x for x in meta["columns"] if d[x].dtype.kind == kind and x not in seen]
            for x in cols:
                seen.add(x)
            arr2d = np.empty((len(cols), n), dtype=v.dtype)
            for i, x in enumerate(cols):
                arr2d[i] = d[x]
            placement = BlockPlacement([meta["columns"].index(x) for x in cols])
            blocks.append(new_block_2d(arr2d, placement))
        mgr = BlockManager(tuple(blocks), axes)
        return pd.DataFrame(mgr, copy=False)

    def loop_d():
        _, dicts = pickle.loads(blob)
        return [build_fast(d) for d in dicts]

    try:
        out["D_onelob_blockmgr_ms"] = bench(loop_d)
        # parity spot-check D
        _, dicts = pickle.loads(blob)
        f0 = build_fast(dicts[0])
        same = list(f0.columns) == list(frames[0].columns) and all(
            str(a) == str(b) for a, b in zip(f0.dtypes, frames[0].dtypes)
        )
        out["D_columns_dtypes_match"] = bool(same)
        out["D_values_match"] = bool(f0.equals(frames[0]))
    except Exception as e:
        out["D_error"] = repr(e)

    # E: per-frame pickled dict-of-arrays (keep per-row storage shape)
    per_frame_blobs = [pickle.dumps(frame_to_arrays(f), protocol=5) for f in frames]
    out["E_perframe_blob_kb"] = sum(len(b) for b in per_frame_blobs) // 1024

    def loop_e():
        return [pd.DataFrame(pickle.loads(b), copy=False) for b in per_frame_blobs]

    out["E_perframe_arrays_construct_ms"] = bench(loop_e)

    # C parity spot-check
    _, dicts = pickle.loads(blob)
    c0 = pd.DataFrame(dicts[0], copy=False)
    out["C_columns_dtypes_match"] = list(c0.columns) == list(frames[0].columns)
    out["C_dtypes_match"] = all(str(a) == str(b) for a, b in zip(c0.dtypes, frames[0].dtypes))
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
