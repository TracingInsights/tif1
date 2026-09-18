"""Probe V8 (lz4 GIL release) and V10 (get_session non-import breakdown)."""

from __future__ import annotations

import os
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/perf-v1"

import sqlite3
import threading

import tif1

# --- V8: does lz4.frame.decompress release the GIL? (overlap potential)
conn = sqlite3.connect("/tmp/perf-v1/cache.sqlite")
blobs = [
    r[0]
    for r in conn.execute(
        "SELECT frame FROM telemetry_frames WHERE year=2026 AND gp='Monaco%20Grand%20Prix' AND session='Race'"
    ).fetchall()
]
from lz4 import frame as lz4_frame  # noqa: E402


def decompress_all():
    for b in blobs:
        lz4_frame.decompress(b)


t0 = time.perf_counter()
decompress_all()
t_single = time.perf_counter() - t0

# Python-loop control (GIL-bound by construction)
counter = [0]


def py_spin():
    x = 0
    for _ in range(3_000_000):
        x += 1
    counter[0] = x


t0 = time.perf_counter()
th = threading.Thread(target=decompress_all)
th.start()
py_spin()
th.join()
t_overlap = time.perf_counter() - t0

t0 = time.perf_counter()
py_spin()
t_py = time.perf_counter() - t0
t0 = time.perf_counter()
decompress_all()
t_dec = time.perf_counter() - t0
expected_serial = t_py + t_dec
print(
    f"V8 lz4 GIL: dec_alone={t_single * 1e3:.0f}ms overlap(dec+py)={t_overlap * 1e3:.0f}ms "
    f"serial_sum={expected_serial * 1e3:.0f}ms -> GIL_released={'yes' if t_overlap < expected_serial * 0.85 else 'no'}"
)

# --- V10: get_session non-import breakdown
import time as _time  # noqa: E402

from tif1.events import get_event_by_name, get_sessions  # noqa: E402

t0 = _time.perf_counter()
ev = get_event_by_name(2026, "Monaco Grand Prix", exact_match=False)
t_resolve = _time.perf_counter() - t0

t0 = _time.perf_counter()
ev2 = get_event_by_name(2026, "Monaco Grand Prix", exact_match=False)
t_resolve2 = _time.perf_counter() - t0

t0 = _time.perf_counter()
ss = get_sessions(2026, "Monaco Grand Prix")
t_sessions = _time.perf_counter() - t0

from tif1.config import get_config  # noqa: E402

cfg = get_config()
t0 = _time.perf_counter()
for _ in range(5):
    cfg.get("max_concurrent_requests", 20)
t_cfg = (_time.perf_counter() - t0) / 5

t0 = _time.perf_counter()
for _ in range(5):
    tif1.get_session(2026, "Monaco Grand Prix", "Race")
t_getsession = (_time.perf_counter() - t0) / 5

print(
    f"V10 get_session: full={t_getsession * 1e3:.1f}ms event_resolve={t_resolve * 1e3:.1f}ms "
    f"repeat_resolve={t_resolve2 * 1e3:.1f}ms get_sessions={t_sessions * 1e3:.1f}ms cfg_get={t_cfg * 1e6:.1f}us"
)
