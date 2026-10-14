"""Offline telemetry-assembly benchmark + parity gate (Argos series).

Builds every frame of the corpus with ``_create_telemetry_df`` from the
source tree on ``sys.path`` (``--src-dir``), reports best-of-N wall time for
the full loop, and optionally dumps a parity fingerprint (columns, dtypes,
value hashes per frame) so a candidate tree can be compared against the
control tree frame by frame.

Usage:
    uv run python tools/argos_assembly_bench.py --src-dir /tmp/tif1-control/src --dump /tmp/argos_ctl.pkl
    uv run python tools/argos_assembly_bench.py --src-dir src --compare /tmp/argos_ctl.pkl
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import pickle
import sys
import time
from pathlib import Path


def fingerprint(df) -> tuple:
    import numpy as np
    import pandas as pd

    cols = tuple(df.columns)
    dtypes = tuple(str(t) for t in df.dtypes)
    h = hashlib.sha256()
    for col in df.columns:
        s = df[col]
        if str(s.dtype) in ("object", "str", "string"):
            vals = [
                "<NA>"
                if (v is None or (isinstance(v, float) and np.isnan(v)) or v is pd.NA)
                else str(v)
                for v in s.tolist()
            ]
            h.update(repr(vals).encode())
        elif str(s.dtype) in ("Int64", "boolean"):
            h.update(np.asarray(s.to_numpy(dtype="float64", na_value=np.nan)).tobytes())
        else:
            h.update(np.ascontiguousarray(s.to_numpy()).view("u1").tobytes())
    return (cols, dtypes, len(df), h.hexdigest(), len(df._mgr.blocks))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default="src")
    parser.add_argument("--corpus", default="/tmp/argos_corpus.pkl")
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--dump", default=None)
    parser.add_argument("--compare", default=None)
    parser.add_argument("--lib", default="pandas")
    args = parser.parse_args()
    sys.path.insert(0, str(Path(args.src_dir).resolve()))

    from tif1.core_utils.helpers import _create_telemetry_df

    with open(args.corpus, "rb") as f:
        corpus = pickle.load(f)
    refs = corpus["refs"]
    # The telemetry tier stores the inner channel dict; older dumps wrap it in {"tel": ...}.
    payloads = [
        (d, lap, corpus["parsed"][(d, lap)].get("tel", corpus["parsed"][(d, lap)]))
        for d, lap in refs
    ]

    best = 1e9
    frames = None
    for _ in range(args.reps):
        gc.collect()
        gc.disable()
        t0 = time.perf_counter()
        out = [(d, lap, _create_telemetry_df(p, d, lap, args.lib)) for d, lap, p in payloads]
        dt = time.perf_counter() - t0
        gc.enable()
        best = min(best, dt)
        frames = out
    n_ok = sum(1 for _, _, f in frames if f is not None)
    print(
        f"src={args.src_dir} frames={n_ok}/{len(frames)} best={best * 1000:.0f} ms ({best / len(frames) * 1e6:.0f} us/frame)"
    )

    if args.dump or args.compare:
        fps = {(d, lap): (fingerprint(f) if f is not None else None) for d, lap, f in frames}
        if args.dump:
            with open(args.dump, "wb") as f:
                pickle.dump(fps, f)
            print(f"dumped fingerprints -> {args.dump}")
        if args.compare:
            with open(args.compare, "rb") as f:
                ref = pickle.load(f)
            mism = []
            for k, v in fps.items():
                r = ref.get(k)
                if r is None and v is None:
                    continue
                if r is None or v is None or r[:4] != v[:4]:
                    mism.append((k, r[:3] if r else None, v[:3] if v else None))
            blocks_ref = sum(r[4] for r in ref.values() if r)
            blocks_new = sum(v[4] for v in fps.values() if v)
            print(
                f"parity: {len(fps) - len(mism)}/{len(fps)} frames identical (cols, dtypes, rows, values); blocks {blocks_ref} -> {blocks_new}"
            )
            for m in mism[:5]:
                print("  mismatch", m)


if __name__ == "__main__":
    main()
