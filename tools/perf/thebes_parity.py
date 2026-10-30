"""Cross-season telemetry-frame parity gate (Thebes series).

Builds every frame of a raw-bytes corpus (``{(year, gp, session, driver, lap):
bytes}`` or the Argos ``{"refs", "raw"}`` shape) with ``_create_telemetry_df``
from the source tree on ``sys.path`` and writes a fingerprint per frame
(columns, dtypes, row count, sha256 of the values, block count). A candidate
tree is then compared frame by frame against the control dump.

Usage:
    uv run python tools/perf/thebes_parity.py --src-dir /tmp/tif1-control/src \
        --corpus /tmp/tif1-perf/corpus_all_raw.pkl --dump /tmp/tif1-perf/parity_ctl.pkl
    uv run python tools/perf/thebes_parity.py --src-dir src \
        --corpus /tmp/tif1-perf/corpus_all_raw.pkl --compare /tmp/tif1-perf/parity_ctl.pkl
"""

from __future__ import annotations

import argparse
import hashlib
import pickle
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

_SRC_DIR: str | None = None
_LIB = "pandas"


def _init(src_dir: str, lib: str) -> None:
    global _SRC_DIR, _LIB
    _SRC_DIR = src_dir
    _LIB = lib
    sys.path.insert(0, src_dir)


def fingerprint(df) -> tuple:
    import numpy as np
    import pandas as pd

    if df is None:
        return ("<None>",)
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


def _polars_fingerprint(df) -> tuple:
    if df is None:
        return ("<None>",)
    h = hashlib.sha256()
    for name, dtype in df.schema.items():
        h.update(name.encode())
        h.update(str(dtype).encode())
        h.update(repr(df.get_column(name).to_list()).encode())
    return (tuple(df.columns), tuple(str(t) for t in df.dtypes), len(df), h.hexdigest(), 0)


def _work(item: tuple[tuple, bytes]) -> tuple[tuple, tuple]:
    import orjson

    from tif1.core_utils.helpers import _create_telemetry_df

    key, blob = item
    try:
        from tif1.core_utils.json_utils import decode_telemetry_payload as _loads
    except ImportError:  # control trees without the Thebes decoder seam
        _loads = orjson.loads
    data = _loads(blob)
    tel = data.get("tel", data) if isinstance(data, dict) else data
    driver, lap = str(key[-2]), int(key[-1])
    try:
        df = _create_telemetry_df(tel, driver, lap, _LIB)
    except Exception as e:
        return key, ("<raise>", type(e).__name__)
    fp = _polars_fingerprint(df) if _LIB == "polars" else fingerprint(df)
    return key, fp


def load_corpus(path: str) -> dict[tuple, bytes]:
    with open(path, "rb") as f:
        corpus = pickle.load(f)
    if isinstance(corpus, dict) and "raw" in corpus and "refs" in corpus:
        gp = corpus.get("gp", "corpus")
        return {("corpus", gp, "", r[0], r[1]): corpus["raw"][r] for r in corpus["refs"]}
    return corpus


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", required=True)
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--dump")
    parser.add_argument("--compare")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    corpus = load_corpus(args.corpus)
    items = sorted(corpus.items())
    if args.limit:
        items = items[: args.limit]
    src_dir = str(Path(args.src_dir).resolve())
    t0 = time.perf_counter()
    with ProcessPoolExecutor(args.workers, initializer=_init, initargs=(src_dir, args.lib)) as ex:
        results = dict(ex.map(_work, items, chunksize=64))
    print(f"fingerprinted {len(results)} frames in {time.perf_counter() - t0:.1f}s from {src_dir}")

    if args.dump:
        with open(args.dump, "wb") as f:
            pickle.dump(results, f, protocol=5)
        print(f"dumped -> {args.dump}")
    if args.compare:
        with open(args.compare, "rb") as f:
            control = pickle.load(f)
        bad = [k for k in results if control.get(k) != results[k]]
        print(f"parity: {len(results) - len(bad)}/{len(results)} identical, {len(bad)} mismatches")
        for k in bad[:10]:
            print(
                "  ", k, "control:", control.get(k, "<missing>")[:3], "candidate:", results[k][:3]
            )
        if bad:
            sys.exit(1)


if __name__ == "__main__":
    main()
