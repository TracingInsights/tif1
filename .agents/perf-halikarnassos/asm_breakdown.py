"""Line-level breakdown of _typed_telemetry_frame on real payloads."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path

os.environ["TIF1_CACHE_DIR"] = "/tmp/perf-v1"

import numpy as np
import orjson
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

import zstandard  # noqa: E402


def timed(fn, repeat: int = 5) -> float:
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best


def main() -> None:
    conn = sqlite3.connect("/tmp/perf-v1/cache.sqlite")
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=2026 AND gp='Monaco%20Grand%20Prix' AND session='Race' LIMIT 200"
    ).fetchall()
    payloads = []
    for _, _, blob in rows:
        if isinstance(blob, bytes) and blob[:4] == b"\x28\xb5\x2f\xfd":
            blob = zstandard.ZstdDecompressor().decompress(blob)  # noqa: PLW2901
        payloads.append(orjson.loads(blob))
    print(f"payloads: {len(payloads)}")

    from tif1.core_utils.constants import TELEMETRY_RENAME_MAP

    prepared = []
    for (d, lap, _), tel in zip(rows, payloads):
        col_data: dict[str, object] = {}
        for k, v in tel.items():
            if not isinstance(v, list):
                continue
            col_data[TELEMETRY_RENAME_MAP.get(k, k)] = v
        prepared.append((d, lap, col_data))

    max_lens = [max(len(v) for v in cd.values()) for _, _, cd in prepared]
    print(f"rows/frame avg: {sum(max_lens) / len(max_lens):.0f}")

    def stage_pad():
        out = []
        for _, _, col_data in prepared:
            n = max(len(v) for v in col_data.values())
            out.append(
                {k: (v + [None] * (n - len(v))) if len(v) < n else v for k, v in col_data.items()}
            )
        return out

    padded = stage_pad()
    t_pad = timed(stage_pad)

    def stage_time():
        for nd in padded:
            for k, v in nd.items():
                if k == "Time":
                    pd.to_timedelta(np.asarray(v, dtype="float64"), unit="s")

    t_time = timed(stage_time)

    def stage_brake():
        for nd in padded:
            for k, v in nd.items():
                if k == "Brake":
                    _probe_result = None not in v

    t_brake_scan = timed(stage_brake)

    def stage_int64_pdarray():
        for nd in padded:
            for k, v in nd.items():
                if k in ("nGear", "DRS"):
                    pd.array(v, dtype="Int64")

    t_int64 = timed(stage_int64_pdarray)

    def stage_int64_direct():
        for nd in padded:
            for k, v in nd.items():
                if k in ("nGear", "DRS"):
                    arr = np.asarray(v)
                    if arr.dtype.kind in "iu":
                        pd.arrays.IntegerArray(arr, np.zeros(len(arr), dtype=bool))
                    else:
                        pd.array(v, dtype="Int64")

    t_int64_direct = timed(stage_int64_direct)

    def stage_driver():
        out = []
        for (d, _, _), nd in zip(prepared, padded):
            n = max(len(v) for v in nd.values())
            out.append(pd.Series(np.full(n, d, dtype=object), dtype=object))
        return out

    t_driver = timed(stage_driver)

    def stage_lapnum():
        out = []
        for (_, lap, _), nd in zip(prepared, padded):
            n = max(len(v) for v in nd.values())
            out.append(pd.array([lap] * n, dtype="Int64"))
        return out

    t_lapnum = timed(stage_lapnum)

    def stage_lapnum_np():
        out = []
        for (_, lap, _), nd in zip(prepared, padded):
            n = max(len(v) for v in nd.values())
            out.append(
                pd.arrays.IntegerArray(np.full(n, lap, dtype="int64"), np.zeros(n, dtype=bool))
            )
        return out

    t_lapnum_np = timed(stage_lapnum_np)

    def stage_frame_dict():
        out = []
        for nd in padded:
            frame_data: dict[str, object] = {}
            for k, v in nd.items():
                if k == "Time":
                    try:
                        frame_data[k] = pd.to_timedelta(np.asarray(v, dtype="float64"), unit="s")
                    except (TypeError, ValueError):
                        frame_data[k] = pd.to_timedelta(v, unit="s")
                elif k == "Brake" and None not in v:
                    frame_data[k] = np.asarray(v, dtype=bool)
                elif k in ("nGear", "DRS"):
                    frame_data[k] = pd.array(v, dtype="Int64")
                else:
                    frame_data[k] = v
            n = max(len(v) for v in nd.values())
            frame_data["Driver"] = pd.Series(np.full(n, "VER", dtype=object), dtype=object)
            frame_data["LapNumber"] = pd.array([1] * n, dtype="Int64")
            out.append(frame_data)
        return out

    frame_dicts = stage_frame_dict()
    t_frame_dict = timed(stage_frame_dict)

    def stage_construct():
        for fd in frame_dicts:
            pd.DataFrame(fd, copy=False)

    t_construct = timed(stage_construct)

    def full_current():
        from tif1.core_utils.helpers import _create_telemetry_df

        for (d, lap, _), tel in zip(rows, payloads):
            _create_telemetry_df(tel, d, lap, "pandas")

    t_full = timed(full_current)

    n = len(prepared)
    print(
        f"pad={t_pad * 1e6 / n:.0f}us to_timedelta={t_time * 1e6 / n:.0f}us brake-scan={t_brake_scan * 1e6 / n:.0f}us"
    )
    print(f"int64 pd.array={t_int64 * 1e6 / n:.0f}us direct={t_int64_direct * 1e6 / n:.0f}us")
    print(
        f"driver={t_driver * 1e6 / n:.0f}us lapnum pd.array={t_lapnum * 1e6 / n:.0f}us direct={t_lapnum_np * 1e6 / n:.0f}us"
    )
    print(
        f"frame_dict build={t_frame_dict * 1e6 / n:.0f}us DataFrame construct={t_construct * 1e6 / n:.0f}us"
    )
    print(f"full _create_telemetry_df={t_full * 1e6 / n:.0f}us/frame")

    for nd in padded[:20]:
        for k, v in nd.items():
            if k in ("nGear", "DRS"):
                arr = np.asarray(v)
                if arr.dtype.kind in "iu":
                    direct = pd.arrays.IntegerArray(arr, np.zeros(len(arr), dtype=bool))
                    via_pdarray = pd.array(v, dtype="Int64")
                    assert (direct == via_pdarray).all(), k
                    assert str(direct.dtype) == str(via_pdarray.dtype), k
    print("int64 direct parity: OK")


if __name__ == "__main__":
    main()
