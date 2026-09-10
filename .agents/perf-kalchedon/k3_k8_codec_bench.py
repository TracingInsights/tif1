"""K3/K8 offline codec+format bench on the 1452 real Monaco telemetry blobs.

Measures, per storage codec and payload encoding:
- compress time + ratio (cold-write cost)
- decompress time (warm-read cost)
- decode time (orjson JSON parse vs msgpack unpack)

Mirrors how the SQLite tier uses the codec today: blobs >= 4 KB are compressed.
"""

from __future__ import annotations

import pickle
import time
import zlib

import msgpack
import orjson
import zstandard

with open("/tmp/monaco_all_tel.pkl", "rb") as f:
    dump = pickle.load(f)

raw: dict[tuple[str, int], bytes] = dump["raw"]
blobs = list(raw.values())
print(f"payloads: {len(blobs)}, total {sum(len(b) for b in blobs) / 1e6:.1f} MB")


def bench(fn, n=3):
    best = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        best.append(time.perf_counter() - t0)
    return min(best)


# --- parse/decode layer (on raw JSON bytes: what a warm JSON-tier read pays) ---
parsed = [orjson.loads(b) for b in blobs]

t_json = bench(lambda: [orjson.loads(b) for b in blobs])
t_msgpack_pack = bench(lambda: [msgpack.packb(p, use_bin_type=True) for p in parsed])
mp_blobs = [msgpack.packb(p, use_bin_type=True) for p in parsed]
t_msgpack_unpack = bench(lambda: [msgpack.unpackb(b, raw=False) for b in mp_blobs])

print(f"orjson.loads 1452 payloads:      {t_json * 1000:.0f} ms")
print(f"msgpack.packb 1452 payloads:     {t_msgpack_pack * 1000:.0f} ms")
print(f"msgpack.unpackb 1452 payloads:   {t_msgpack_unpack * 1000:.0f} ms")
print(
    f"msgpack size vs json:            {sum(len(b) for b in mp_blobs) / sum(len(b) for b in blobs) * 100:.1f}%"
)

# --- compression layer (SQLite tier: blobs >= 4 KB) ---
big = [b for b in blobs if len(b) >= 4096]
print(f"compressible payloads (>=4KB):   {len(big)}")

t_zlib3_c = bench(lambda: [zlib.compress(b, 3) for b in big])
z3 = [zlib.compress(b, 3) for b in big]
t_zlib3_d = bench(lambda: [zlib.decompress(b) for b in z3])

zc1 = zstandard.ZstdCompressor(level=1)
zc3 = zstandard.ZstdCompressor(level=3)
t_zstd1_c = bench(lambda: [zc1.compress(b) for b in big])
t_zstd3_c = bench(lambda: [zc3.compress(b) for b in big])
zs1 = [zc1.compress(b) for b in big]
zs3 = [zc3.compress(b) for b in big]
zd = zstandard.ZstdDecompressor()
t_zstd1_d = bench(lambda: [zd.decompress(b) for b in zs1])
t_zstd3_d = bench(lambda: [zd.decompress(b) for b in zs3])

ratio_z3 = sum(len(b) for b in z3) / sum(len(b) for b in big)
ratio_zs1 = sum(len(b) for b in zs1) / sum(len(b) for b in big)
ratio_zs3 = sum(len(b) for b in zs3) / sum(len(b) for b in big)

print(
    f"zlib-3  compress: {t_zlib3_c * 1000:.0f} ms  decompress: {t_zlib3_d * 1000:.0f} ms  ratio: {ratio_z3 * 100:.1f}%"
)
print(
    f"zstd-1  compress: {t_zstd1_c * 1000:.0f} ms  decompress: {t_zstd1_d * 1000:.0f} ms  ratio: {ratio_zs1 * 100:.1f}%"
)
print(
    f"zstd-3  compress: {t_zstd3_c * 1000:.0f} ms  decompress: {t_zstd3_d * 1000:.0f} ms  ratio: {ratio_zs3 * 100:.1f}%"
)

# --- combined warm-read cost per codec (decompress + JSON parse) ---
print()
print("warm-read cost per codec (decompress + orjson parse):")
print(f"  zlib-3: {(t_zlib3_d + t_json) * 1000:.0f} ms")
print(f"  zstd-1: {(t_zstd1_d + t_json) * 1000:.0f} ms")

# msgpack rows compressed with zstd-1 (K3+K8 combined tier)
t_mp_zstd_c = bench(lambda: [zc1.compress(b) for b in mp_blobs])
mp_zs1 = [zc1.compress(b) for b in mp_blobs]
t_mp_zstd_d = bench(lambda: [zd.decompress(b) for b in mp_zs1])
print()
print("combined tier msgpack+zstd-1:")
print(
    f"  compress {t_mp_zstd_c * 1000:.0f} ms  decompress {t_mp_zstd_d * 1000:.0f} ms  "
    f"unpack {t_msgpack_unpack * 1000:.0f} ms  "
    f"warm-read total {(t_mp_zstd_d + t_msgpack_unpack) * 1000:.0f} ms  "
    f"ratio {sum(len(b) for b in mp_zs1) / sum(len(b) for b in blobs) * 100:.1f}%"
)

# round-trip parity: msgpack must reproduce the orjson object exactly
import itertools  # noqa: E402

for b, mp in itertools.islice(zip(blobs, mp_blobs), 5):
    assert msgpack.unpackb(mp, raw=False) == orjson.loads(b)
print("msgpack round-trip parity: OK (first 5 payloads)")
