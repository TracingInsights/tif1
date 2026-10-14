"""Interleaved per-frame codec bakeoff on the real 1452-frame corpus, best-of-5."""

import gc
import os
import pickle
import sys
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"
sys.path.insert(0, "src")

import lz4.frame as lzf
import zstandard as zstd

from tif1.cache import get_cache

c = get_cache()
rows = c.conn.execute(
    "SELECT driver, lap, frame FROM telemetry_frames WHERE year=2026"
    " AND gp='Monaco%20Grand%20Prix' AND session='Race'"
).fetchall()

pickles = [lzf.decompress(r[2]) for r in rows]
zstd_c = zstd.ZstdCompressor(level=1)
zstd_d = zstd.ZstdDecompressor()
zstd_blobs = [zstd_c.compress(p) for p in pickles]
lz4_blobs = [lzf.compress(p, compression_level=0) for p in pickles]


def bench(fn):
    gc.collect()
    t0 = time.perf_counter()
    out = fn()
    dt = time.perf_counter() - t0
    del out
    return dt


r: dict[str, list[float]] = {}
for _ in range(5):
    r.setdefault("lz4_frame0", []).append(bench(lambda: [lzf.decompress(b) for b in lz4_blobs]))
    r.setdefault("zstd1_perframe", []).append(
        bench(lambda: [zstd_d.decompress(b) for b in zstd_blobs])
    )
    r.setdefault("zstd1_batch", []).append(
        bench(lambda: zstd_d.decompress(zstd_c.compress(b"".join(pickles))))
    )
    r.setdefault("lz4_batch", []).append(
        bench(lambda: lzf.decompress(lzf.compress(b"".join(pickles), compression_level=0)))
    )
    r.setdefault("unpickle_only", []).append(bench(lambda: [pickle.loads(p) for p in pickles]))

for k, v in r.items():
    print(f"{k}: best {min(v) * 1000:.0f} ms  median {sorted(v)[len(v) // 2] * 1000:.0f} ms")
print(
    f"sizes: lz4 {sum(map(len, lz4_blobs)) / 1e6:.1f} MB"
    f" zstd {sum(map(len, zstd_blobs)) / 1e6:.1f} MB raw {sum(map(len, pickles)) / 1e6:.1f} MB"
)

t0 = time.perf_counter()
_ = [lzf.compress(p, compression_level=0) for p in pickles]
print(f"enc lz4: {(time.perf_counter() - t0) * 1000:.0f} ms")
t0 = time.perf_counter()
_ = [zstd_c.compress(p) for p in pickles]
print(f"enc zstd1: {(time.perf_counter() - t0) * 1000:.0f} ms")
