"""Offline phase profiling for the N-series experiments (no network).

Breaks down, on the pre-warmed persistent cache:
- warm payload-tier telemetry read (SQLite/zstd/orjson/assembly/materialize)
- warm frame-tier telemetry read (SQLite/zstd/unpickle)
"""

from __future__ import annotations

import json
import os
import pickle
import sqlite3
import time
from pathlib import Path

WARM = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM

import orjson  # noqa: E402
import zstandard  # noqa: E402

import tif1  # noqa: E402
from tif1.cache import Cache  # noqa: E402
from tif1.core_utils.helpers import _create_telemetry_df  # noqa: E402


def t() -> float:
    return time.perf_counter()


def main() -> None:
    out: dict[str, object] = {}

    t0 = t()
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    out["get_session_warm_s"] = round(t() - t0, 4)

    t0 = t()
    laps = session.laps
    out["laps_warm1_s"] = round(t() - t0, 4)
    out["laps_rows"] = len(laps)

    cache = Cache(Path(WARM))
    conn = sqlite3.connect(f"{WARM}/cache.sqlite")
    conn.row_factory = sqlite3.Row
    import urllib.parse as _up

    gp_key = _up.quote(session.gp)

    refs_frame = laps[["Driver", "LapNumber"]].dropna()
    refs = list(zip(refs_frame["Driver"].astype(str), refs_frame["LapNumber"].astype(int)))
    out["n_refs"] = len(refs)

    # ---- payload tier (what the 2nd load pays)
    t0 = t()
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=? AND gp=? AND session=?",
        (session.year, gp_key, session.session),
    ).fetchall()
    out["payload_sql_s"] = round(t() - t0, 4)
    out["payload_rows"] = len(rows)

    dec = zstandard.ZstdDecompressor()
    t0 = t()
    raws = [(r["driver"], r["lap"], dec.decompress(r["data"])) for r in rows]
    out["payload_zstd_s"] = round(t() - t0, 4)

    t0 = t()
    parsed = [(drv, lp, orjson.loads(blob)) for drv, lp, blob in raws]
    out["payload_orjson_s"] = round(t() - t0, 4)

    t0 = t()
    frames = [(drv, lp, _create_telemetry_df(pl, drv, lp, "pandas")) for drv, lp, pl in parsed]
    out["payload_assembly_s"] = round(t() - t0, 4)
    good = [(drv, lp, fr) for drv, lp, fr in frames if fr is not None and not fr.empty]
    out["payload_frames_ok"] = len(good)

    comp = zstandard.ZstdCompressor(level=1)
    t0 = t()
    blobs = [(drv, lp, comp.compress(pickle.dumps(fr, protocol=5))) for drv, lp, fr in good]
    out["materialize_pickle_zstd_s"] = round(t() - t0, 4)

    t0 = t()
    conn.executemany(
        "INSERT OR REPLACE INTO telemetry_frames VALUES (?,?,?,?,?,?)",
        [(session.year, session.gp, session.session, drv, lp, b) for drv, lp, b in blobs],
    )
    conn.commit()
    out["materialize_sql_s"] = round(t() - t0, 4)

    # ---- frame tier (what the 3rd+ load pays)
    t0 = t()
    frows = conn.execute(
        "SELECT driver, lap, frame FROM telemetry_frames WHERE year=? AND gp=? AND session=?",
        (session.year, gp_key, session.session),
    ).fetchall()
    out["frames_sql_s"] = round(t() - t0, 4)
    out["frames_rows"] = len(frows)

    t0 = t()
    funp = [(r["driver"], r["lap"], dec.decompress(r["frame"])) for r in frows]
    out["frames_zstd_s"] = round(t() - t0, 4)

    t0 = t()
    fframes = [(drv, lp, pickle.loads(blob)) for drv, lp, blob in funp]
    out["frames_unpickle_s"] = round(t() - t0, 4)
    out["frames_unpickled"] = len(fframes)

    t0 = t()
    hit = cache.get_telemetry_frames_batch(session.year, session.gp, session.session, refs)
    out["frames_batch_api_s"] = round(t() - t0, 4)
    out["frames_batch_hits"] = len(hit)

    t0 = t()
    miss = cache.get_telemetry_batch(session.year, session.gp, session.session, refs)
    out["payload_batch_api_s"] = round(t() - t0, 4)
    out["payload_batch_hits"] = len(miss)

    out["payload_bytes_mb"] = round(sum(len(r["data"]) for r in rows) / 1e6, 1)
    out["frame_bytes_mb"] = round(sum(len(r["frame"]) for r in frows) / 1e6, 1)

    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
