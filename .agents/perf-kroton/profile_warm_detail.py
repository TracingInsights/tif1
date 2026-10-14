"""Phase-level + cProfile breakdown of the current warm load path (fresh process)."""

import cProfile
import io
import os
import pstats
import sys
import time

WARM = "/tmp/tif1-warm-cache"
os.environ["TIF1_CACHE_DIR"] = WARM
sys.path.insert(0, "src")

t0 = time.perf_counter()
import tif1  # noqa: E402

t_import = time.perf_counter() - t0

t0 = time.perf_counter()
session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
t_get = time.perf_counter() - t0

t0 = time.perf_counter()
laps = session.laps
t_laps = time.perf_counter() - t0

# component timings inside fetch_all via manual batch-read reproduction
import pickle  # noqa: E402

from tif1.cache import _decompress_frame_blob, get_cache  # noqa: E402

gp = session.gp
t_sql = 0.0
t_lz4 = 0.0
t_unpickle = 0.0
rows = None
cache = get_cache()
t0 = time.perf_counter()
with cache._sqlite_lock:
    rows = cache.conn.execute(
        "SELECT driver, lap, frame FROM telemetry_frames WHERE year = ? AND gp = ? AND session = ?",
        (session.year, gp, session.session),
    ).fetchall()
t_sql = time.perf_counter() - t0
t0 = time.perf_counter()
blobs = [_decompress_frame_blob(r[2]) for r in rows]
t_lz4 = time.perf_counter() - t0
t0 = time.perf_counter()
frames = [pickle.loads(b) for b in blobs]
t_unpickle = time.perf_counter() - t0

t0 = time.perf_counter()
tel = session.fetch_all_laps_telemetry()
t_tel = time.perf_counter() - t0

# pandas import cost isolated (separate process measures it too; here it's inside get_session)
print(
    {
        "import_s": round(t_import, 4),
        "get_session_s": round(t_get, 4),
        "laps_s": round(t_laps, 4),
        "rows": len(rows),
        "sql_s": round(t_sql, 4),
        "lz4_s": round(t_lz4, 4),
        "unpickle_s": round(t_unpickle, 4),
        "fetch_all_s": round(t_tel, 4),
        "frames": len(tel),
    }
)

# cProfile the three phases in a FRESH-equivalent state: use a second session instance
pr = cProfile.Profile()
pr.enable()
s2 = tif1.get_session(2026, "Monaco Grand Prix", "Race")
l2 = s2.laps
tel2 = s2.fetch_all_laps_telemetry()
pr.disable()
s = io.StringIO()
pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats(30)
print(s.getvalue())
