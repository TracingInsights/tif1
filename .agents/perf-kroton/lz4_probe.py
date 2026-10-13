"""Measure lz4 per-call overhead: per-frame decompress vs one batched stream."""

import os
import sys
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"
sys.path.insert(0, "src")
import lz4.frame as lzf
import numpy as np

from tif1.cache import get_cache

c = get_cache()
rows = c.conn.execute(
    "SELECT driver, lap, frame FROM telemetry_frames WHERE year=2026"
    " AND gp='Monaco%20Grand%20Prix' AND session='Race'"
).fetchall()
blobs = [r[2] for r in rows]
print(f"{len(blobs)} blobs, {sum(len(b) for b in blobs) / 1e6:.1f} MB compressed")

t0 = time.perf_counter()
raw_now = [lzf.decompress(b) for b in blobs]
t_a = time.perf_counter() - t0
print(f"per-frame lz4: {t_a * 1000:.0f} ms")

raw_all = b"".join(raw_now)
offsets = np.cumsum([0] + [len(r) for r in raw_now], dtype=np.int64)
batch_blob = lzf.compress(raw_all, compression_level=0)
print(
    f"batched blob: {len(batch_blob) / 1e6:.1f} MB (vs per-frame sum {sum(len(b) for b in blobs) / 1e6:.1f} MB)"
)
t0 = time.perf_counter()
big = lzf.decompress(batch_blob)
sliced = [big[o : o + n] for o, n in zip(offsets[:-1], np.diff(offsets))]
t_b = time.perf_counter() - t0
print(f"batched lz4: {t_b * 1000:.0f} ms")
assert sliced == raw_now

t0 = time.perf_counter()
_ = big[:]
t_c = time.perf_counter() - t0
print(f"memcpy floor: {t_c * 1000:.0f} ms, raw total {len(raw_all) / 1e6:.1f} MB")

import zstandard as zstd  # noqa: E402

t0 = time.perf_counter()
zblob = zstd.ZstdCompressor(level=1).compress(raw_all)
t_enc = time.perf_counter() - t0
t0 = time.perf_counter()
big2 = zstd.ZstdDecompressor().decompress(zblob)
t_d = time.perf_counter() - t0
print(
    f"zstd-1 batched: enc {t_enc * 1000:.0f} ms dec {t_d * 1000:.0f} ms size {len(zblob) / 1e6:.1f} MB"
)
assert big2 == raw_all
