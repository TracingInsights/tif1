"""Cross-season parity gate for the per-driver frame tier (Pergamon P3).

For every session whose frames sit in the legacy per-frame table of a cache
copy: read them through the current code (legacy path), let the background
upgrade write the driver rows, re-read through the driver rows, and compare
both against the original pickled frames (columns, dtypes, values, block
count). Run it on a COPY of a cache: the upgrade rewrites the tables.

Usage:
    uv run python tools/pergamon_legacy_gate.py /tmp/tif1-legacy [--out report.json]
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import sqlite3
import sys
import time
from pathlib import Path


def _same(a, b) -> bool:
    return bool(
        list(a.columns) == list(b.columns)
        and a.dtypes.equals(b.dtypes)
        and a.equals(b)
        and len(a._mgr.blocks) == len(b._mgr.blocks)
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cache_dir")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    cache_dir = Path(args.cache_dir)
    os.environ["TIF1_CACHE_DIR"] = str(cache_dir)
    from tif1.cache import Cache, _decompress_frame_blob

    db = cache_dir / "cache.sqlite"
    conn = sqlite3.connect(db)
    sessions = conn.execute(
        "SELECT year, gp, session, COUNT(*) FROM telemetry_frames GROUP BY 1, 2, 3"
    ).fetchall()
    conn.close()
    report = []
    for year, gp, sess, n in sessions:
        conn = sqlite3.connect(db)
        rows = conn.execute(
            "SELECT driver, lap, frame FROM telemetry_frames WHERE year=? AND gp=? AND session=?",
            (year, gp, sess),
        ).fetchall()
        conn.close()
        originals = {}
        for driver, lap, blob in rows:
            raw = _decompress_frame_blob(blob)
            if bytes(raw[:4]) == b"TF1\x01":
                from tif1.core_utils.frame_codec import decode_frame

                originals[(driver, lap)] = decode_frame(raw)
            else:
                originals[(driver, lap)] = pickle.loads(raw)
        refs = list(originals)
        cache = Cache(cache_dir=cache_dir)
        t0 = time.perf_counter()
        legacy = cache.get_telemetry_frames_batch(year, gp, sess, refs)
        t_legacy = time.perf_counter() - t0
        cache.close()  # joins the background upgrade writer
        cache = Cache(cache_dir=cache_dir)
        driver_rows = cache.conn.execute(
            "SELECT COUNT(*) FROM telemetry_frames_drv WHERE year=? AND gp=? AND session=?",
            (year, gp, sess),
        ).fetchone()[0]
        legacy_left = cache.conn.execute(
            "SELECT COUNT(*) FROM telemetry_frames WHERE year=? AND gp=? AND session=?",
            (year, gp, sess),
        ).fetchone()[0]
        t0 = time.perf_counter()
        upgraded = cache.get_telemetry_frames_batch(year, gp, sess, refs)
        t_driver = time.perf_counter() - t0
        cache.close()
        line = {
            "year": year,
            "gp": gp,
            "session": sess,
            "frames": n,
            "served_legacy": len(legacy),
            "served_driver": len(upgraded),
            "driver_rows": driver_rows,
            "legacy_rows_left": legacy_left,
            "mismatch_legacy": sum(
                1 for r in refs if r not in legacy or not _same(originals[r], legacy[r])
            ),
            "mismatch_driver": sum(
                1 for r in refs if r not in upgraded or not _same(originals[r], upgraded[r])
            ),
            "t_legacy_ms": round(t_legacy * 1000, 1),
            "t_driver_ms": round(t_driver * 1000, 1),
        }
        print(json.dumps(line), flush=True)
        report.append(line)
    total_bad = sum(r["mismatch_legacy"] + r["mismatch_driver"] for r in report)
    print(f"TOTAL frames {sum(r['frames'] for r in report)} mismatches {total_bad}")
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=1) + "\n", encoding="utf-8")
    sys.exit(1 if total_bad else 0)


if __name__ == "__main__":
    main()
