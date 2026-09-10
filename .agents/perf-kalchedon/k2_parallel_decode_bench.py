"""K2 offline: parallel decompress+parse for the warm batch cache read.

The warm telemetry read (`Cache.get_telemetry_batch`) fetches all rows in one
SQL query, then decompresses (zlib) and parses (orjson) each blob serially.
zlib and orjson release the GIL, so a small thread pool should parallelize the
CPU-bound decode work. This is distinct from rejected F10 (GIL-bound pandas
assembly) and H3 (parse offload during the live fetch loop).

Bench: 1452 real zlib-3 blobs from the primed warm cache (SQLite rows), serial
vs ThreadPoolExecutor variants, including LRU-set overhead.
"""

from __future__ import annotations

import sqlite3
import time
import zlib
from concurrent.futures import ThreadPoolExecutor

import orjson

DB = "/tmp/tif1-warm-cache/cache.sqlite"


def load_rows() -> list[tuple[str, int, bytes]]:
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=2026 AND gp=? AND session='Race'",
        ("Monaco%20Grand%20Prix",),
    ).fetchall()
    conn.close()
    out = []
    for driver, lap, data in rows:
        blob = data if isinstance(data, bytes) else data.encode()
        out.append((driver, lap, blob))
    return out


def decode_one(row):
    driver, lap, blob = row
    return (driver, lap), orjson.loads(zlib.decompress(blob))


def bench(fn, n=3):
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        res = fn()
        times.append(time.perf_counter() - t0)
        assert len(res) == len(ROWS), (len(res), len(ROWS))
    return min(times), res


ROWS = load_rows()
print(f"rows: {len(ROWS)}, compressed {sum(len(r[2]) for r in ROWS) / 1e6:.1f} MB")


def serial():
    return [decode_one(r) for r in ROWS]


def pooled(workers):
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(decode_one, ROWS))


t_serial, res_serial = bench(serial)
print(f"serial:            {t_serial * 1000:.0f} ms")

for w in (2, 4, 8):
    t_p, res_p = bench(lambda w=w: pooled(w))
    same = res_p == res_serial
    print(f"pool({w}):           {t_p * 1000:.0f} ms  ({t_serial / t_p:.2f}x)  parity={same}")

# SQL fetch time separately (the part that stays serial under the sqlite lock)
t0 = time.perf_counter()
rows2 = load_rows()
t_sql = time.perf_counter() - t0
print(f"sql fetch only:    {t_sql * 1000:.0f} ms")
