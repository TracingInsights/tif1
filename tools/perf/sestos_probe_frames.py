"""L7 offline probe: pickle+zstd cost/size/load-time for 1452 real telemetry frames.

Loads the warm-cache payloads, assembles frames exactly like the library
does, then measures: pickle dump, pickle load, zstd(level 1) compress of the
pickles, zstd+unpickle round-trip (the prospective warm fast path), and a
one-blob variant (all frames in a single pickled dict).
"""

from __future__ import annotations

import os
import pickle
import sqlite3
import time

import zstandard as zstd

WARM_CACHE_DIR = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
DB = f"{WARM_CACHE_DIR}/cache.sqlite"
YEAR, GP, SESSION = 2026, "Monaco%20Grand%20Prix", "Race"


def main() -> None:
    import sys

    sys.path.insert(0, "src")
    from tif1.cache import _decode_sqlite_value
    from tif1.core_utils.helpers import _create_telemetry_df
    from tif1.core_utils.json_utils import json_loads

    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=? AND gp=? AND session=?",
        (YEAR, GP, SESSION),
    ).fetchall()
    conn.close()

    t0 = time.perf_counter()
    frames = []
    for driver, lap, data in rows:
        payload = json_loads(_decode_sqlite_value(data))
        frame = _create_telemetry_df(payload, driver, lap, "pandas")
        if frame is not None:
            frames.append((driver, lap, frame))
    t_assemble = time.perf_counter() - t0
    print(f"assemble from payloads: {t_assemble:.3f}s ({len(frames)} frames)")

    # per-frame pickle
    t0 = time.perf_counter()
    pickles = [pickle.dumps(f, protocol=5) for _, _, f in frames]
    t_dump = time.perf_counter() - t0
    t0 = time.perf_counter()
    _loaded = [pickle.loads(p) for p in pickles]
    t_load = time.perf_counter() - t0
    pickle_bytes = sum(len(p) for p in pickles)
    print(
        f"per-frame pickle: dump={t_dump:.3f}s load={t_load:.3f}s total={pickle_bytes / 1e6:.1f} MB"
    )

    c = zstd.ZstdCompressor(level=1)
    d = zstd.ZstdDecompressor()
    t0 = time.perf_counter()
    compressed = [c.compress(p) for p in pickles]
    t_compress = time.perf_counter() - t0
    t0 = time.perf_counter()
    restored = [pickle.loads(d.decompress(z)) for z in compressed]
    t_rt = time.perf_counter() - t0
    zbytes = sum(len(z) for z in compressed)
    print(
        f"per-frame zstd: compress={t_compress:.3f}s stored={zbytes / 1e6:.1f} MB "
        f"roundtrip(decompress+loads)={t_rt:.3f}s"
    )

    # parity check: sample
    parity = all(
        frames[i][2].equals(restored[i]) and str(frames[i][2].dtypes) == str(restored[i].dtypes)
        for i in range(0, len(frames), 50)
    )
    print(f"parity sampled: {parity}")

    # one-blob variant
    big = {(driver, lap): f for driver, lap, f in frames}
    t0 = time.perf_counter()
    big_p = pickle.dumps(big, protocol=5)
    t_bigdump = time.perf_counter() - t0
    t0 = time.perf_counter()
    big_z = c.compress(big_p)
    t_bigcompress = time.perf_counter() - t0
    t0 = time.perf_counter()
    big_back = pickle.loads(d.decompress(big_z))
    t_bigrt = time.perf_counter() - t0
    print(
        f"one-blob: pickle={len(big_p) / 1e6:.1f} MB zstd={len(big_z) / 1e6:.1f} MB "
        f"dump={t_bigdump:.3f}s compress={t_bigcompress:.3f}s roundtrip={t_bigrt:.3f}s "
        f"frames={len(big_back)}"
    )

    # write cost estimate (serial inserts, like the deferred flush)
    t0 = time.perf_counter()
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE telemetry_frames (year INTEGER, gp TEXT, session TEXT, driver TEXT, "
        "lap INTEGER, frame BLOB, PRIMARY KEY (year, gp, session, driver, lap))"
    )
    for (driver, lap, _), blob in zip(frames, compressed):
        conn.execute(
            "INSERT OR REPLACE INTO telemetry_frames VALUES (?, ?, ?, ?, ?, ?)",
            (YEAR, GP, SESSION, driver, lap, blob),
        )
    conn.commit()
    t_write = time.perf_counter() - t0
    conn.close()
    print(f"insert 1452 frame rows (memory db): {t_write:.3f}s")


if __name__ == "__main__":
    main()
