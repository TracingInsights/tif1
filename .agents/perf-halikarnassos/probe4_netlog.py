"""Log all network requests during a warm-cache load."""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

os.environ["TIF1_CACHE_DIR"] = "/tmp/perf-v1"

# Mirror probe3's sequence: an independent sqlite3 connection to the same DB
# opened before the warm load, in case it interferes with the Cache.
import sqlite3  # noqa: E402

import tif1  # noqa: E402
from tif1 import async_fetch, http_session  # noqa: E402

_probe_conn = sqlite3.connect("/tmp/perf-v1/cache.sqlite")
_probe_conn.execute("SELECT COUNT(*) FROM telemetry_frames").fetchone()

orig_get_session = http_session.get_session
counts: dict[str, int] = {}


def counting_session():
    sess = orig_get_session()
    orig_get = sess.get

    def logged_get(url, *args, **kwargs):
        key = str(url).split("?")[0]
        counts[key] = counts.get(key, 0) + 1
        return orig_get(url, *args, **kwargs)

    if not getattr(sess, "_tif1_counting", False):
        sess.get = logged_get
        sess._tif1_counting = True
    return sess


http_session.get_session = counting_session

orig_async = async_fetch._get_async_session


def counting_async():
    sess = orig_async()
    if not getattr(sess, "_tif1_counting", False):
        orig_get = sess.get

        async def logged_get(url, *args, **kwargs):
            key = str(url).split("?")[0]
            counts[key] = counts.get(key, 0) + 1
            return await orig_get(url, *args, **kwargs)

        sess.get = logged_get
        sess._tif1_counting = True
    return sess


async_fetch._get_async_session = counting_async

session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
laps = session.laps
tel = session.fetch_all_laps_telemetry()

print(f"network requests: {sum(counts.values())}")
for url, c in sorted(counts.items()):
    print(f"  {c:3d}x {url}")
