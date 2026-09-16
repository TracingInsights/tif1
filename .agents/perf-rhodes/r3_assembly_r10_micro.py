"""R3-assembly + R10 offline micro on real Monaco payloads (no network).

- assembly loop: _create_telemetry_df x1452 with GC enabled vs suspended
- write macro: two separate passes (JSON flush shape: orjson.dumps+zstd+SQL
  per blob; frame pass: pickle.dumps+zstd+executemany) vs one combined pass
  sharing one transaction.
"""

from __future__ import annotations

import gc
import json
import os
import pickle
import sqlite3
import time

os.environ.setdefault("TIF1_CACHE_DIR", "/tmp/tif1-warm-cache")

import pandas as pd  # noqa: E402
import zstandard  # noqa: E402

WARM = "/tmp/tif1-warm-cache"
ZC = zstandard.ZstdCompressor(level=1)
ZD = zstandard.ZstdDecompressor()


def bench(fn, n=3):
    best = 1e9
    for _ in range(n):
        gc.collect()
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return round(best * 1000, 1)


def main() -> None:
    import tif1
    from tif1.core_utils.helpers import _create_telemetry_df

    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    laps = session.laps
    refs_frame = laps[["Driver", "LapNumber"]].dropna()
    refs = list(zip(refs_frame["Driver"].astype(str), refs_frame["LapNumber"].astype(int)))
    cache = tif1.cache.get_cache()
    payloads = cache.get_telemetry_batch(session.year, session.gp, session.session, refs)
    print("payloads:", len(payloads))
    items = [(d, l, p.get("tel", p)) for (d, l), p in payloads.items()]

    def assemble_gc_on():
        return [_create_telemetry_df(p, d, l, "pandas") for d, l, p in items]

    def assemble_gc_off():
        gc.disable()
        try:
            return [_create_telemetry_df(p, d, l, "pandas") for d, l, p in items]
        finally:
            gc.enable()

    out: dict = {}
    frames = assemble_gc_on()
    out["assembly_gc_on_ms"] = bench(assemble_gc_on)
    out["assembly_gc_off_ms"] = bench(assemble_gc_off)
    frames = assemble_gc_off()

    # ---- write macro ----
    # Current cold-path shape: fetch pipeline flush = per-payload JSON dumps+zstd
    # then executemany-ish writes (set_raw), then N1 materialization pass =
    # per-frame pickle.dumps+zstd + executemany. Two separate passes+transactions.
    def json_pass():
        blobs = [(d, l, ZC.compress(json.dumps(p).encode())) for d, l, p in items]
        return blobs

    def frame_pass():
        blobs = [
            (d, l, ZC.compress(pickle.dumps(f, protocol=5)))
            for (d, l, _), f in zip(items, frames)
        ]
        return blobs

    def combined_pass():
        out_rows: list = []
        frame_rows: list = []
        for (d, l, p), f in zip(items, frames):
            out_rows.append((d, l, ZC.compress(json.dumps(p).encode())))
            frame_rows.append((d, l, ZC.compress(pickle.dumps(f, protocol=5))))
        return out_rows, frame_rows

    out["json_dumps_zstd_ms"] = bench(json_pass)
    out["frame_dumps_zstd_ms"] = bench(frame_pass)
    out["combined_dumps_zstd_ms"] = bench(combined_pass)

    # SQLite side: two transactions vs one
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE j (d TEXT, l INT, b BLOB)")
    conn.execute("CREATE TABLE f (d TEXT, l INT, b BLOB)")
    jb = json_pass()
    fb = frame_pass()

    def two_tx():
        with conn:
            conn.executemany("INSERT INTO j VALUES (?,?,?)", jb)
        with conn:
            conn.executemany("INSERT INTO f VALUES (?,?,?)", fb)

    def one_tx():
        with conn:
            conn.executemany("INSERT INTO j VALUES (?,?,?)", jb)
            conn.executemany("INSERT INTO f VALUES (?,?,?)", fb)

    out["two_transactions_ms"] = bench(two_tx, n=5)
    out["one_transaction_ms"] = bench(one_tx, n=5)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
