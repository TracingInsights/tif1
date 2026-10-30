"""N-series offline probes: assembly micro-optimizations and frame write batching.

- N4: numpy-based Int64 masked arrays vs pd.array in _typed_telemetry_frame
- N5: executemany vs per-row execute for telemetry_frames writes
"""

from __future__ import annotations

import os
import pickle
import sqlite3
import time
from pathlib import Path

WARM = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import zstandard  # noqa: E402

import tif1  # noqa: E402
from tif1.core_utils.helpers import _create_telemetry_df  # noqa: E402


def t() -> float:
    return time.perf_counter()


def main() -> None:
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    _ = session.laps
    conn = sqlite3.connect(f"{WARM}/cache.sqlite")
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=? AND gp=? AND session=?",
        (session.year, session.gp, session.session),
    ).fetchall()
    dec = zstandard.ZstdDecompressor()
    import orjson

    raws = [(r["driver"], int(r["lap"]), orjson.loads(dec.decompress(r["data"]))) for r in rows]
    print(f"payloads: {len(raws)}")

    # ---- N4: current per-frame assembly
    t0 = t()
    base = []
    for drv, lp, pl in raws:
        fr = _create_telemetry_df(pl, drv, lp, "pandas")
        if fr is not None:
            base.append(fr)
    cur_s = t() - t0
    print(f"current _create_telemetry_df loop: {cur_s:.4f}s ({len(base)} frames)")

    # ---- N4 variant: numpy-based Int64 + to_timedelta on ndarray
    def typed_variant(normalized_data: dict, max_len: int, driver: str, lap_num: int):
        frame_data: dict = {}
        for k, v in normalized_data.items():
            if k == "Time":
                frame_data[k] = pd.to_timedelta(np.asarray(v, dtype="float64"), unit="s")
            elif k == "Brake" and None not in v:
                frame_data[k] = np.asarray(v, dtype=bool)
            elif k in ("nGear", "DRS"):
                arr = np.asarray(v, dtype="int64")
                mask = np.zeros(len(v), dtype=bool)
                frame_data[k] = pd.arrays.IntegerArray(arr, mask)
            else:
                frame_data[k] = v
        frame_data["Driver"] = pd.Series(
            np.full(max_len, driver, dtype=object), dtype=object, copy=False
        )
        frame_data["LapNumber"] = pd.arrays.IntegerArray(
            np.full(max_len, lap_num, dtype="int64"), np.zeros(max_len, dtype=bool)
        )
        frame = pd.DataFrame(frame_data, copy=False)
        for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
            if col not in frame.columns:
                frame[col] = pd.NA
        return frame

    def variant_loop():
        out = []
        for drv, lp, pl in raws:
            if not isinstance(pl, dict) or not pl:
                continue
            col_data = {k: v for k, v in pl.items() if isinstance(v, list)}
            if not col_data:
                continue
            max_len = max(len(v) for v in col_data.values())
            if max_len == 0:
                continue
            normalized = {
                k: (v + [None] * (max_len - len(v))) if len(v) < max_len else v
                for k, v in col_data.items()
            }
            try:
                fr = typed_variant(normalized, max_len, drv, lp)
            except (TypeError, ValueError):
                fr = _create_telemetry_df(pl, drv, lp, "pandas")
            if fr is not None and not fr.empty:
                out.append(fr)
        return out

    t0 = t()
    variant = variant_loop()
    var_s = t() - t0
    print(f"variant loop: {var_s:.4f}s ({len(variant)} frames)")

    # parity
    mism = 0
    for a, b in zip(base, variant):
        if (
            list(a.columns) != list(b.columns)
            or list(a.dtypes) != list(b.dtypes)
            or len(a) != len(b)
        ):
            mism += 1
            if mism <= 3:
                print(
                    "  cols equal:",
                    list(a.columns) == list(b.columns),
                    "dtypes:",
                    list(a.dtypes) == list(b.dtypes),
                )
            continue
        for col in a.columns:
            av, bv = a[col], b[col]
            if av.dtype == object:
                if av.tolist() != bv.tolist():
                    mism += 1
                    break
            elif not np.array_equal(av.to_numpy(), bv.to_numpy(), equal_nan=True):
                mism += 1
                break
    print(f"variant parity mismatches: {mism}/{len(base)}")

    # ---- N5: frame write executemany vs execute-loop
    comp = zstandard.ZstdCompressor(level=1)
    blobs = [comp.compress(pickle.dumps(fr, protocol=5)) for fr in base]

    scratch = "/tmp/tif1-scratch-frames.db"
    if Path(scratch).exists():
        Path(scratch).unlink()
    sc = sqlite3.connect(scratch)
    sc.execute(
        "CREATE TABLE telemetry_frames (year INTEGER, gp TEXT, session TEXT, driver TEXT, lap INTEGER, frame BLOB)"
    )
    year, gp, ses = session.year, session.gp, session.session

    t0 = t()
    # execute loop (mirror of cache.set_telemetry_frames_batch)
    sc.execute("BEGIN")
    for idx, b in enumerate(blobs):
        fr = base[idx]
        drv = fr["Driver"].iloc[0] if len(fr) else "?"
        lp = fr["LapNumber"].iloc[0] if len(fr) else -1
        sc.execute(
            "INSERT OR REPLACE INTO telemetry_frames VALUES (?,?,?,?,?,?)",
            (year, gp, ses, drv, int(lp), b),
        )
    sc.commit()
    loop_s = t() - t0
    print(f"execute-loop write: {loop_s:.4f}s")

    sc.execute("DELETE FROM telemetry_frames")
    sc.commit()
    refs = [
        (
            year,
            gp,
            ses,
            fr["Driver"].iloc[0] if len(fr) else "?",
            int(fr["LapNumber"].iloc[0]) if len(fr) else -1,
            blobs[idx],
        )
        for idx, fr in enumerate(base)
    ]

    t0 = t()
    sc.execute("BEGIN")
    sc.executemany("INSERT OR REPLACE INTO telemetry_frames VALUES (?,?,?,?,?,?)", refs)
    sc.commit()
    many_s = t() - t0
    print(f"executemany write: {many_s:.4f}s")
    sc.close()


if __name__ == "__main__":
    main()
