"""Profile the warm-load pipeline phase by phase."""

from __future__ import annotations

import cProfile
import io
import os
import pstats
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/perf-v1"

import tif1

t0 = time.perf_counter()
session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
t_get = time.perf_counter() - t0

t0 = time.perf_counter()
laps = session.laps
t_laps = time.perf_counter() - t0

import pickle  # noqa: E402

from tif1.cache import _decompress_frame_blob, get_cache  # noqa: E402

refs = list(zip(laps["Driver"].astype(str), laps["LapNumber"].astype(int).astype(int)))
cache = get_cache()
t0 = time.perf_counter()
with cache._sqlite_lock:
    placeholders = ", ".join(["(?, ?)"] * len(refs))
    params = [2026, "Monaco Grand Prix", "Race"]
    for d, lap in refs:
        params.extend([d, lap])
    rows = cache.conn.execute(
        f"SELECT driver, lap, frame FROM telemetry_frames WHERE year = ? AND gp = ? AND session = ? AND (driver, lap) IN ({placeholders})",
        params,
    ).fetchall()
t_sql = time.perf_counter() - t0

t0 = time.perf_counter()
blobs = [_decompress_frame_blob(b) for _, _, b in rows]
t_dec = time.perf_counter() - t0

t0 = time.perf_counter()
frames = [pickle.loads(b) for b in blobs]
t_pickle = time.perf_counter() - t0

print(
    f"get_session={t_get:.3f}s laps={t_laps:.3f}s sql={t_sql:.3f}s "
    f"decompress={t_dec:.3f}s unpickle={t_pickle:.3f}s frames={len(rows)}"
)

pr = cProfile.Profile()
pr.enable()
tel = session.fetch_all_laps_telemetry()
pr.disable()
out = io.StringIO()
stats = pstats.Stats(pr, stream=out).sort_stats("cumulative")
stats.print_stats(28)
print(out.getvalue())
