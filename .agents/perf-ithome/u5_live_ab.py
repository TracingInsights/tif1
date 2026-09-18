"""Ithome U5 live A/B: background post-return frame-tier write vs inline write.

Each run is a fresh process with a throwaway cache dir (cold CDN fetch of the
2026 Monaco GP race telemetry). The cache dir env var is set BEFORE any tif1
import (the config singleton must not capture a different dir), and the
reporter is registered before cache initialization so atexit LIFO runs it
after tif1's cache close (which joins the background writer).
"""

from __future__ import annotations

import atexit
import json
import os
import sys
import tempfile
import time

cache_dir = tempfile.mkdtemp(prefix="tif1-u5-")
os.environ["TIF1_CACHE_DIR"] = cache_dir

CONTROL = os.environ.get("U5_CONTROL") == "1"
if CONTROL:
    import tif1.cache as _cache_mod

    _cache_mod.register_frame_writer = lambda fn: fn()  # synchronous control

import tif1  # noqa: E402


def main() -> None:
    wall_start = time.perf_counter()

    def _report() -> None:
        import sqlite3

        conn = sqlite3.connect(f"{cache_dir}/cache.sqlite")
        try:
            n_frames = conn.execute(
                "SELECT COUNT(*) FROM telemetry_frames WHERE year=2026 "
                "AND gp='Monaco%20Grand%20Prix' AND session='Race'"
            ).fetchone()[0]
        finally:
            conn.close()
        print(
            json.dumps(
                {
                    "control": CONTROL,
                    "telemetry_s": round(t_tel, 3),
                    "total_s": round(t_get + t_laps + t_tel, 3),
                    "wall_s": round(time.perf_counter() - wall_start, 3),
                    "frames": len(tel),
                    "frame_rows_at_exit": n_frames,
                }
            ),
            file=sys.stderr,
            flush=True,
        )

    atexit.register(_report)

    t0 = time.perf_counter()
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    t_get = time.perf_counter() - t0

    t0 = time.perf_counter()
    laps = session.laps
    t_laps = time.perf_counter() - t0
    assert laps is not None

    t0 = time.perf_counter()
    tel = session.fetch_all_laps_telemetry()
    t_tel = time.perf_counter() - t0


if __name__ == "__main__":
    main()
