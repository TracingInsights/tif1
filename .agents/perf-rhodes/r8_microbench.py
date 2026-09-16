"""R8 micro: zstd per-call overhead variants + raw (uncompressed) storage read."""

from __future__ import annotations

import gc
import json
import pickle
import sqlite3
import time

import zstandard

WARM = "/tmp/tif1-warm-cache"


def bench(fn, n=5):
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
    out = {"n": len(rows)}
    dctx = zstandard.ZstdDecompressor()

    # current shape: dctx.decompress per blob
    out["zstd_dctx_decompress_ms"] = bench(lambda: [dctx.decompress(b) for _, _, b in rows])

    # decompressobj per blob
    def obj_variant():
        res = []
        for _, _, b in rows:
            res.append(dctx.decompressobj().decompress(b))
        return res

    out["zstd_decompressobj_ms"] = bench(obj_variant)

    # read_size hint / max_output_size? dctx.decompress(b, max_output_size=...)
    out["zstd_maxout_ms"] = bench(
        lambda: [dctx.decompress(b, max_output_size=1 << 20) for _, _, b in rows]
    )

    # gc off around zstd loop
    def zstd_gc_off():
        gc.disable()
        try:
            return [dctx.decompress(b) for _, _, b in rows]
        finally:
            gc.enable()

    out["zstd_dctx_gc_off_ms"] = bench(zstd_gc_off)

    # combined zstd+loads with gc off (the candidate read loop)
    def combined():
        gc.disable()
        try:
            return [pickle.loads(dctx.decompress(b)) for _, _, b in rows]
        finally:
            gc.enable()

    out["combined_zstd_loads_gc_off_ms"] = bench(combined)

    # raw storage: SQLite read of uncompressed blobs (simulate table with raw pickle)
    raw_blobs = [dctx.decompress(b) for _, _, b in rows]
    conn.execute("CREATE TEMP TABLE rawframes (driver TEXT, lap INT, frame BLOB)")
    conn.executemany(
        "INSERT INTO rawframes VALUES (?, ?, ?)",
        [(d, l, p) for (d, l, _), p in zip(rows, raw_blobs)],
    )
    conn.commit()
    out["raw_sql_read_ms"] = bench(
        lambda: conn.execute("SELECT driver, lap, frame FROM rawframes").fetchall()
    )

    def raw_loads():
        rws = conn.execute("SELECT driver, lap, frame FROM rawframes").fetchall()
        gc.disable()
        try:
            return [pickle.loads(b) for _, _, b in rws]
        finally:
            gc.enable()

    out["raw_read_plus_loads_gc_off_ms"] = bench(raw_loads)

    # zstd write side: compressor per blob (current) vs one compressor
    zctx = zstandard.ZstdCompressor(level=1)
    out["zstd_compress_ms"] = bench(lambda: [zctx.compress(p) for p in raw_blobs])
    out["raw_total_mb"] = round(sum(len(b) for b in raw_blobs) / 1e6, 1)
    out["zstd_total_mb"] = round(sum(len(b) for _, _, b in rows) / 1e6, 1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
