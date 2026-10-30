"""N8 probe: Rust columnar parser vs orjson pipeline on real telemetry payloads."""

from __future__ import annotations

import os
import sqlite3
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-w5"

import numpy as np
import orjson
import pandas as pd
import zstandard
from telparse import parse_columns

import tif1
from tif1.core_utils.constants import TELEMETRY_RENAME_MAP
from tif1.core_utils.helpers import _create_telemetry_df


def bench(name: str, fn, reps: int = 3) -> float:
    best = 1e9
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    print(f"{name}: {best:.4f}s")
    return best


def typed_from_numpy(drv: str, lp: int, cols: dict):
    n = max((len(v) for v in cols.values()), default=0)
    if n == 0:
        return None
    frame_data: dict = {}
    for k, v in cols.items():
        rk = TELEMETRY_RENAME_MAP.get(k, k)
        if rk == "Time":
            frame_data[rk] = pd.to_timedelta(np.asarray(v, dtype="float64"), unit="s")
        elif rk == "Brake":
            frame_data[rk] = np.asarray(v, dtype=bool)
        elif rk in ("nGear", "DRS"):
            frame_data[rk] = pd.array(np.asarray(v, dtype="int64"), dtype="Int64")
        else:
            frame_data[rk] = v
    frame_data["Driver"] = pd.Series(np.full(n, drv, dtype=object), dtype=object, copy=False)
    frame_data["LapNumber"] = pd.array(np.full(n, lp, dtype="int64"), dtype="Int64")
    fr = pd.DataFrame(frame_data, copy=False)
    for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
        if col not in fr.columns:
            fr[col] = pd.NA
    return fr


def main() -> None:
    s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    _ = s.laps
    c = sqlite3.connect("/tmp/tif1-w5/cache.sqlite")
    c.row_factory = sqlite3.Row
    rows = c.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=? AND gp=? AND session=?",
        (s.year, s.gp, s.session),
    ).fetchall()
    dec = zstandard.ZstdDecompressor()
    raws = [(r["driver"], int(r["lap"]), dec.decompress(r["data"])) for r in rows]
    print(f"payloads: {len(raws)}, {sum(len(b) for _, _, b in raws) / 1e6:.1f} MB")

    bench("orjson.loads", lambda: [orjson.loads(b) for _, _, b in raws])
    bench("telparse.parse_columns", lambda: [parse_columns(b) for _, _, b in raws])

    def rust_pipeline():
        out = []
        for drv, lp, blob in raws:
            cols = parse_columns(blob)
            if cols:
                fr = typed_from_numpy(drv, lp, cols)
                if fr is not None:
                    out.append(fr)
        return out

    bench("rust parse + numpy typed assembly", rust_pipeline, reps=1)

    def py_pipeline():
        return [
            f
            for f in (_create_telemetry_df(orjson.loads(b), d, lp_, "pandas") for d, lp_, b in raws)
            if f is not None
        ]

    bench("orjson + _create_telemetry_df", py_pipeline, reps=1)

    rf = rust_pipeline()
    pf = py_pipeline()
    mism = 0
    for a, b in zip(pf, rf):
        if list(a.dtypes) != list(b.dtypes) or list(a.columns) != list(b.columns):
            mism += 1
            if mism <= 2:
                print(
                    "  dtype diff:",
                    list(a.dtypes) == list(b.dtypes),
                    list(a.columns) == list(b.columns),
                )
    print(f"pipeline frames: py={len(pf)} rust={len(rf)} mismatches={mism}")


if __name__ == "__main__":
    main()
