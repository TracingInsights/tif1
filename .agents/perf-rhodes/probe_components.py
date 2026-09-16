"""Component probes for the Rhodes series: where do warm-path milliseconds go?

Measures, on the primed warm cache (2026 Monaco Race):
- frame-tier batch read split (SQL / zstd / pickle.loads)
- refs extraction + memo-loop cost inside fetch_all_laps_telemetry
- repeat-call (L3 memo hit) residual
- GC activity during the batch read
- storage size of the frame tier
"""

from __future__ import annotations

import gc
import json
import os
import pickle
import sqlite3
import time

WARM = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM

import tif1  # noqa: E402

YEAR, GP, SESSION = 2026, "Monaco Grand Prix", "Race"


def ms(t: float) -> float:
    return round(t * 1000, 2)


def main() -> None:
    out: dict = {}

    session = tif1.get_session(YEAR, GP, SESSION)
    KEY = (session.year, session.gp, session.session)
    laps = session.laps
    # refs extraction (same code as fetch_all_laps_telemetry_async pandas branch)
    import pandas as pd

    t0 = time.perf_counter()
    refs_frame = laps[["Driver", "LapNumber"]].dropna()
    lap_refs = list(
        zip(refs_frame["Driver"].astype(str), refs_frame["LapNumber"].astype(int))
    )
    out["refs_extraction_ms"] = ms(time.perf_counter() - t0)
    out["n_refs"] = len(lap_refs)

    t0 = time.perf_counter()
    telemetry_map: dict = {}
    pending: list = []
    for ref in lap_refs:
        memoized_df = session._memo.get("telemetry_df", ref)
        if memoized_df is not None:
            telemetry_map[ref] = memoized_df
        else:
            pending.append(ref)
    out["memo_loop_ms"] = ms(time.perf_counter() - t0)
    out["n_pending_after_memo"] = len(pending)

    # raw frame-tier read split
    db = os.path.join(WARM, "cache.sqlite")
    conn = sqlite3.connect(db)
    placeholders = ", ".join(["(?, ?)"] * len(lap_refs))
    params: list = list(KEY)
    for d, l in lap_refs:
        params.extend([d, l])
    query = (
        "SELECT driver, lap, frame FROM telemetry_frames WHERE year = ? AND gp = ? "
        f"AND session = ? AND (driver, lap) IN ({placeholders})"
    )
    t0 = time.perf_counter()
    rows = conn.execute(query, params).fetchall()
    out["frames_sql_ms"] = ms(time.perf_counter() - t0)
    out["n_frame_rows"] = len(rows)
    out["frames_stored_mb"] = round(sum(len(b) for _, _, b in rows) / 1e6, 1)

    import zstandard

    dctx = zstandard.ZstdDecompressor()
    t0 = time.perf_counter()
    raw = [dctx.decompress(b) for _, _, b in rows]
    out["frames_zstd_ms"] = ms(time.perf_counter() - t0)
    out["frames_raw_mb"] = round(sum(len(r) for r in raw) / 1e6, 1)

    gc_counts_before = gc.get_count()
    t0 = time.perf_counter()
    frames = [pickle.loads(r) for r in raw]
    out["frames_pickle_ms"] = ms(time.perf_counter() - t0)
    out["gc_counts_during_pickle"] = gc.get_count()
    out["gc_counts_before"] = gc_counts_before
    out["gc_stats_after"] = [v["collections"] for v in gc.get_stats()]

    # what dtypes live in a frame?
    f0 = frames[0]
    out["frame_columns"] = {c: str(t) for c, t in f0.dtypes.items()}
    out["frame_rows"] = int(len(f0))

    # repeat-call residual (L3): everything memoized now
    for (d, l), f in zip([tuple(r[:2]) for r in rows], frames):
        session._memo.set("telemetry_df", (d, int(l)), f)
    t0 = time.perf_counter()
    tel2 = session.fetch_all_laps_telemetry()
    out["repeat_call_ms"] = ms(time.perf_counter() - t0)
    out["repeat_call_frames"] = len(tel2)

    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
