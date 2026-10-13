"""Phase-level profile of the warm-cache load path.

Breaks warm telemetry into: SQL read, lz4 decompress, unpickle, and post-loop
assembly. Also profiles get_session (schedule fetch) and laps phases.
"""

from __future__ import annotations

import os
import pickle
import sqlite3
import time

WARM_CACHE_DIR = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM_CACHE_DIR

import tif1  # noqa: E402


def t() -> float:
    return time.perf_counter()


def main() -> None:
    t0 = t()
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    t_get = t() - t0
    print(f"get_session: {t_get:.3f}s")

    t0 = t()
    laps = session.laps
    t_laps = t() - t0
    print(f"laps: {t_laps:.3f}s (rows={len(laps)})")

    # Instrument the frame-tier read directly to split SQL/decompress/unpickle.
    t0 = t()
    conn = sqlite3.connect(f"{WARM_CACHE_DIR}/cache.sqlite")
    rows = conn.execute(
        "SELECT driver, lap, frame FROM telemetry_frames WHERE year = 2026 "
        "AND gp = 'Monaco%20Grand%20Prix' AND session = 'Race'"
    ).fetchall()
    t_sql = t() - t0
    conn.close()
    print(f"raw SQL read: {t_sql:.3f}s rows={len(rows)}")

    from tif1.cache import _decompress_frame_blob

    t0 = t()
    blobs = [_decompress_frame_blob(r[2]) for r in rows]
    t_decomp = t() - t0
    print(f"decompress: {t_decomp:.3f}s ({sum(len(b) for b in blobs) / 1e6:.1f} MB raw)")

    t0 = t()
    n_frames = len([pickle.loads(b) for b in blobs])
    t_unpickle = t() - t0
    print(f"unpickle: {t_unpickle:.3f}s ({n_frames} frames)")

    t0 = t()
    tel = session.fetch_all_laps_telemetry()
    t_tel = t() - t0
    print(f"fetch_all_laps_telemetry (instrumented cache in place): {t_tel:.3f}s frames={len(tel)}")


if __name__ == "__main__":
    main()
