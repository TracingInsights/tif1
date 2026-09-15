"""L5+L6 offline probes on the primed warm cache database.

L5: batch-read wall time with stock pragmas vs PRAGMA mmap_size.
L6: zstd-1 compress/decompress/ratio on the 1452 real telemetry blobs vs a
    zstd trained dictionary (trained on a disjoint sample).
"""

from __future__ import annotations

import os
import sqlite3
import statistics
import time
from pathlib import Path

import zstandard as zstd

_DB_DIR = Path(os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache"))
DB = _DB_DIR / "cache.sqlite"
if not DB.exists():
    for name in _DB_DIR.iterdir():
        print("cache dir contents:", name)
    raise SystemExit(1)

YEAR, GP, SESSION = 2026, "Monaco%20Grand%20Prix", "Race"


def load_blobs() -> list[tuple[str, int, bytes]]:
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=? AND gp=? AND session=?",
        (YEAR, GP, SESSION),
    ).fetchall()
    conn.close()
    # data column is compressed bytes (zstd-1) or legacy text
    out = []
    for driver, lap, data in rows:
        blob = data if isinstance(data, bytes) else data.encode()
        out.append((driver, lap, blob))
    return out


def bench_read(mmap_size: int | None, reps: int = 5) -> list[float]:
    times = []
    for _ in range(reps):
        conn = sqlite3.connect(DB)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA cache_size=-64000")
        if mmap_size:
            conn.execute(f"PRAGMA mmap_size={mmap_size}")
        keys = conn.execute(
            "SELECT driver, lap FROM telemetry_cache WHERE year=? AND gp=? AND session=?",
            (YEAR, GP, SESSION),
        ).fetchall()
        t0 = time.perf_counter()
        placeholders = ", ".join(["(?, ?)"] * len(keys))
        params: list = [YEAR, GP, SESSION]
        for d, lp in keys:
            params.extend([d, lp])
        query = (
            f"SELECT driver, lap, data FROM telemetry_cache WHERE year = ? AND gp = ? AND "
            f"session = ? AND (driver, lap) IN ({placeholders})"
        )
        conn.execute(query, params).fetchall()
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        conn.close()
    return times


def main() -> None:
    blobs = load_blobs()
    print(f"telemetry rows: {len(blobs)}")
    raw_total = sum(len(zstd.ZstdDecompressor().decompress(b)) for _, _, b in blobs)
    print(
        f"stored bytes: {sum(len(b) for _, _, b in blobs) / 1e6:.1f} MB, raw: {raw_total / 1e6:.1f} MB"
    )

    # L5: mmap
    stock = bench_read(None)
    mmapped = bench_read(1 << 30)
    print(
        f"L5 stock read:  median={statistics.median(stock):.3f}s runs={[round(t, 3) for t in stock]}"
    )
    print(
        f"L5 mmap read:   median={statistics.median(mmapped):.3f}s runs={[round(t, 3) for t in mmapped]}"
    )

    # L6: baseline zstd-1 (level shipped)
    raw_blobs = [zstd.ZstdDecompressor().decompress(b) for _, _, b in blobs]

    c1 = zstd.ZstdCompressor(level=1)
    t0 = time.perf_counter()
    for raw in raw_blobs:
        c1.compress(raw)
    t_c1 = time.perf_counter() - t0
    d = zstd.ZstdDecompressor()
    t0 = time.perf_counter()
    for _, _, b in blobs:
        d.decompress(b)
    t_d1 = time.perf_counter() - t0
    ratio1 = sum(len(b) for _, _, b in blobs) / raw_total
    print(f"L6 zstd-1: compress={t_c1:.3f}s decompress={t_d1:.3f}s ratio={ratio1:.3f}")

    # L6: trained dictionary (train on first 200 payloads, measure on the rest)
    train_samples = raw_blobs[:200]
    measure = raw_blobs[200:]
    t0 = time.perf_counter()
    dictionary = zstd.train_dictionary(112640, train_samples, level=1)
    t_train = time.perf_counter() - t0
    print(f"L6 dict trained in {t_train:.1f}s, size={len(dictionary.as_bytes())}")

    cd = zstd.ZstdCompressor(level=1, dict_data=dictionary)
    t0 = time.perf_counter()
    cd_blobs = [cd.compress(raw) for raw in measure]
    t_cd = time.perf_counter() - t0
    dd = zstd.ZstdDecompressor(dict_data=dictionary)
    t0 = time.perf_counter()
    for b in cd_blobs:
        dd.decompress(b)
    t_dd = time.perf_counter() - t0
    ratio_d = sum(len(b) for b in cd_blobs) / sum(len(r) for r in measure)
    print(
        f"L6 zstd-1+dict (n={len(measure)}): compress={t_cd:.3f}s decompress={t_dd:.3f}s "
        f"ratio={ratio_d:.3f}"
    )

    # roundtrip sanity
    assert dd.decompress(cd_blobs[0]) == measure[0]
    print("L6 roundtrip OK")


if __name__ == "__main__":
    main()
