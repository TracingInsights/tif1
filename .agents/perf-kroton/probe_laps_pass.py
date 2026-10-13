"""KR-3 probe: where does _process_lap_df spend time on the merged laps dict?"""

import cProfile
import io
import os
import pstats
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

import orjson  # noqa: E402
import pandas as pd  # noqa: E402

from tif1.cache import _decode_sqlite_value, get_cache  # noqa: E402
from tif1.core import _merge_lap_payloads  # noqa: E402
from tif1.core_utils.helpers import _normalize_lap_payload, _process_lap_df  # noqa: E402


def merged_dict():
    c = get_cache()
    rows = c.conn.execute(
        "SELECT key, data FROM cache WHERE key LIKE"
        " '2026/Monaco%20Grand%20Prix/Race/%/laptimes.json'"
    ).fetchall()
    entries = []
    for key, blob in rows:
        data = orjson.loads(_decode_sqlite_value(blob) if isinstance(blob, bytes) else blob)
        driver = key.split("/")[3]
        entries.append((_normalize_lap_payload(data), driver, "team"))
    return _merge_lap_payloads(entries)


merged = merged_dict()
print("merged cols:", len(merged), "rows:", len(next(iter(merged.values()))) if merged else 0)
frame = pd.DataFrame(merged, copy=False)

best = 1e9
for _ in range(7):
    f0 = frame.copy()
    t0 = time.perf_counter()
    _process_lap_df(f0, "pandas")
    best = min(best, time.perf_counter() - t0)
print(f"_process_lap_df best-of-7: {best * 1000:.1f} ms")

pr = cProfile.Profile()
pr.enable()
_process_lap_df(frame.copy(), "pandas")
pr.disable()
out = io.StringIO()
pstats.Stats(pr, stream=out).sort_stats("cumulative").print_stats(16)
for ln in out.getvalue().splitlines():
    if "tif1" in ln or "pandas" in ln or "seconds" in ln or "ncalls" in ln:
        print(ln)
