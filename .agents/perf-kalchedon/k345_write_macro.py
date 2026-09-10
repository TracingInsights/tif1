"""K3/K4/K5 offline cache-write macro + warm-read macro on real payloads.

Variants (cold bulk write of 1452 telemetry payloads, mirroring the real flow):
  A current      : per-row set_raw (JSON tier, zlib-3) + per-row set_telemetry
                   (dumps + zlib-3 + insert), commit every 100   [ships today]
  B zstd         : same double-tier write, zstd-1 codec
  C zstd 1-tier  : telemetry table only (dumps + zstd-1 + insert), per-row
  D zstd 1-tier batch : executemany per 100, one lock/commit per chunk
  E = D + PRAGMA synchronous=OFF

Warm-read macro: batch SELECT all rows + decompress + orjson parse (serial),
the exact work Cache.get_telemetry_batch does on a warm load.
"""

from __future__ import annotations

import pickle
import sqlite3
import tempfile
import time
import zlib
from pathlib import Path

import orjson
import zstandard

with open("/tmp/monaco_all_tel.pkl", "rb") as f:
    dump = pickle.load(f)

raw: dict[tuple[str, int], bytes] = dump["raw"]
parsed: dict[tuple[str, int], dict] = dump["parsed"]
GP = dump["gp"]
ITEMS = list(parsed.items())
RAW = {k: raw[k] for k in parsed}
N = len(ITEMS)
print(f"payloads: {N}")


def make_db(tag: str, synchronous: str = "NORMAL") -> tuple[sqlite3.Connection, str]:
    d = tempfile.mkdtemp(prefix=f"tif1-k45-{tag}-")
    conn = sqlite3.connect(f"{d}/cache.sqlite", check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA synchronous={synchronous}")
    conn.execute("PRAGMA cache_size=-64000")
    conn.execute("CREATE TABLE cache (key TEXT PRIMARY KEY, data TEXT)")
    conn.execute(
        """CREATE TABLE telemetry_cache (
        year INTEGER, gp TEXT, session TEXT, driver TEXT, lap INTEGER, data TEXT,
        PRIMARY KEY (year, gp, session, driver, lap))"""
    )
    conn.commit()
    return conn, d


def json_key(driver, lap):
    return f"2026/{GP}/Race/{driver}/{lap}_tel.json"


zc = zstandard.ZstdCompressor(level=1)
zd = zstandard.ZstdDecompressor()


def enc_zlib(blob):
    return zlib.compress(blob, 3) if len(blob) >= 4096 else blob


def enc_zstd(blob):
    return zc.compress(blob) if len(blob) >= 4096 else blob


def bench(fn, n=2):
    best = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        best.append(time.perf_counter() - t0)
    return min(best)


def variant_a():
    conn, d = make_db("a")
    t0 = time.perf_counter()
    pending = 0
    for (drv, lap), tel in ITEMS:
        blob = RAW[(drv, lap)]
        conn.execute(
            "INSERT OR REPLACE INTO cache VALUES (?,?)",
            (json_key(drv, lap), enc_zlib(blob)),
        )
        inner = orjson.dumps(tel)
        conn.execute(
            "INSERT OR REPLACE INTO telemetry_cache VALUES (?,?,?,?,?,?)",
            (2026, GP, "Race", drv, lap, enc_zlib(inner)),
        )
        pending += 2
        if pending >= 100:
            conn.commit()
            pending = 0
    conn.commit()
    t = time.perf_counter() - t0
    conn.close()
    return t, d


def variant_b():
    conn, d = make_db("b")
    t0 = time.perf_counter()
    pending = 0
    for (drv, lap), tel in ITEMS:
        blob = RAW[(drv, lap)]
        conn.execute(
            "INSERT OR REPLACE INTO cache VALUES (?,?)", (json_key(drv, lap), enc_zstd(blob))
        )
        inner = orjson.dumps(tel)
        conn.execute(
            "INSERT OR REPLACE INTO telemetry_cache VALUES (?,?,?,?,?,?)",
            (2026, GP, "Race", drv, lap, enc_zstd(inner)),
        )
        pending += 2
        if pending >= 100:
            conn.commit()
            pending = 0
    conn.commit()
    t = time.perf_counter() - t0
    conn.close()
    return t, d


def variant_c():
    conn, d = make_db("c")
    t0 = time.perf_counter()
    pending = 0
    for (drv, lap), tel in ITEMS:
        inner = orjson.dumps(tel)
        conn.execute(
            "INSERT OR REPLACE INTO telemetry_cache VALUES (?,?,?,?,?,?)",
            (2026, GP, "Race", drv, lap, enc_zstd(inner)),
        )
        pending += 1
        if pending >= 100:
            conn.commit()
            pending = 0
    conn.commit()
    t = time.perf_counter() - t0
    conn.close()
    return t, d


def variant_d(synchronous="NORMAL", tag="d"):
    conn, d = make_db(tag, synchronous)
    t0 = time.perf_counter()
    rows = []
    for (drv, lap), tel in ITEMS:
        inner = orjson.dumps(tel)
        rows.append((2026, GP, "Race", drv, lap, enc_zstd(inner)))
    for i in range(0, len(rows), 100):
        chunk = rows[i : i + 100]
        conn.executemany("INSERT OR REPLACE INTO telemetry_cache VALUES (?,?,?,?,?,?)", chunk)
        conn.commit()
    t = time.perf_counter() - t0
    conn.close()
    return t, d


def warm_read(d, codec):
    conn = sqlite3.connect(f"file:{d}/cache.sqlite?mode=ro", uri=True)
    t0 = time.perf_counter()
    rows = conn.execute("SELECT driver, lap, data FROM telemetry_cache").fetchall()
    t1 = time.perf_counter() - t0
    out = {}
    for drv, lap, data in rows:
        blob = data if isinstance(data, bytes) else data.encode()
        if isinstance(blob, bytes) and len(blob) >= 4 and blob[:4] == b"\x28\xb5\x2f\xfd":
            blob = zd.decompress(blob)
        else:
            try:
                blob = zlib.decompress(blob)
            except zlib.error:
                pass
        out[(drv, lap)] = orjson.loads(blob)
    t2 = time.perf_counter() - t0
    conn.close()
    return t1, t2 - t1, len(out)


t_a, d_a = variant_a()
t_b, d_b = variant_b()
t_c, d_c = variant_c()
t_d, d_d = variant_d()
t_e, d_e = variant_d(synchronous="OFF", tag="e")

print(f"A current (zlib-3, 2 tiers, per-row):      {t_a:.2f} s")
print(f"B zstd-1, 2 tiers, per-row:                {t_b:.2f} s  ({t_a / t_b:.2f}x)")
print(f"C zstd-1, 1 tier, per-row:                 {t_c:.2f} s  ({t_a / t_c:.2f}x)")
print(f"D zstd-1, 1 tier, executemany/100:         {t_d:.2f} s  ({t_a / t_d:.2f}x)")
print(f"E = D + synchronous=OFF:                   {t_e:.2f} s  ({t_d / t_e:.2f}x vs D)")

for (
    name,
    d,
) in (("A(zlib-3)", d_a), ("B(zstd)", d_b), ("C(zstd-1tier)", d_c), ("D(zstd-batch)", d_d)):
    t_sql, t_decode, n = warm_read(d, name)
    print(
        f"warm read {name:16s}: sql {t_sql * 1000:.0f} ms + decode {t_decode * 1000:.0f} ms ({n} rows)"
    )

# DB sizes
for name, d in (("A", d_a), ("D", d_d)):
    size = Path(f"{d}/cache.sqlite").stat().st_size / 1e6
    print(f"db size {name}: {size:.0f} MB")
