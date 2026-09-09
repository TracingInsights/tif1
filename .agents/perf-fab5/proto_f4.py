"""F4 prototype: fast float-seconds -> timedelta64[ns] cast, isolated and parity-checked.

Measures on dumped Monaco payloads:
  A. _apply_telemetry_dtypes on raw frames (current, uses pd.to_timedelta)
  B. same but Time column via numpy multiply + view cast
Parity: Time column dtype and values (incl. NaN->NaT) must be identical.
"""

from __future__ import annotations

import pickle
import time

import numpy as np
import pandas as pd

from tif1.core_utils.helpers import _apply_telemetry_dtypes


def fast_dtype_pass(df: pd.DataFrame) -> pd.DataFrame:
    """_apply_telemetry_dtypes with the Time column cast via multiply+view."""
    if "Time" in df.columns:
        col = df["Time"]
        arr = col.to_numpy(dtype="float64", na_value=np.nan) if hasattr(col, "to_numpy") else None
        if arr is None and isinstance(col, list):
            arr = np.asarray(col, dtype="float64")
        if arr is None:
            arr = np.asarray(col, dtype="float64")
        df = df.copy() if False else df
        df["Time"] = pd.Series(arr * 1e9, dtype="timedelta64[ns]", index=df.index)
    if "Brake" in df.columns and not df["Brake"].isna().any():
        df["Brake"] = df["Brake"].astype(bool)
    if "nGear" in df.columns:
        df["nGear"] = df["nGear"].astype("Int64")
    if "DRS" in df.columns:
        df["DRS"] = df["DRS"].astype("Int64")
    if "Driver" in df.columns:
        df["Driver"] = df["Driver"].astype(object)
    if "LapNumber" in df.columns:
        df["LapNumber"] = df["LapNumber"].astype("Int64")
    for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def raw_frames(entries):
    from tif1.core_utils.constants import TELEMETRY_RENAME_MAP

    frames = []
    for driver, lap, tel in entries:
        if not isinstance(tel, dict) or not tel:
            continue
        col_data = {
            TELEMETRY_RENAME_MAP.get(k, k): v for k, v in tel.items() if isinstance(v, list)
        }
        if not col_data:
            continue
        max_len = max(len(v) for v in col_data.values())
        if max_len == 0:
            continue
        normalized = {
            k: (v + [None] * (max_len - len(v))) if len(v) < max_len else v
            for k, v in col_data.items()
        }
        normalized["Driver"] = driver
        normalized["LapNumber"] = lap
        frames.append(pd.DataFrame(normalized, copy=False))
    return frames


def bench(fn, entries, n=3):
    best = float("inf")
    out = None
    for _ in range(n):
        frames = raw_frames(entries)  # fresh: _apply_telemetry_dtypes mutates in place
        t0 = time.perf_counter()
        out = [fn(f) for f in frames]
        best = min(best, time.perf_counter() - t0)
    return best, out


def main() -> None:
    with open("/tmp/monaco_tel_payloads.pkl", "rb") as f:
        entries = pickle.load(f)
    frames = raw_frames(entries)
    print(f"{len(frames)} raw frames")

    ta, out_a = bench(_apply_telemetry_dtypes, entries)
    tb, out_b = bench(fast_dtype_pass, entries)
    print(f"A current dtype pass: {ta:.3f}s ({ta / len(frames) * 1e3:.2f} ms/frame)")
    print(f"B fast-cast dtype pass: {tb:.3f}s ({tb / len(frames) * 1e3:.2f} ms/frame)")
    print(f"speedup: {ta / tb:.2f}x")

    # parity on Time (dtype + values incl NaT) and full-frame spot checks
    nat_mismatch = 0
    for fa, fb in zip(out_a, out_b):
        assert "Time" in fa.columns and "Time" in fb.columns
        assert fa["Time"].dtype == fb["Time"].dtype, (fa["Time"].dtype, fb["Time"].dtype)
        va, vb = fa["Time"].to_numpy(), fb["Time"].to_numpy()
        equal = (va == vb) | (pd.isna(va) & pd.isna(vb))
        if not equal.all():
            nat_mismatch += 1
    print(f"Time parity: {'OK' if nat_mismatch == 0 else f'{nat_mismatch} frames mismatch'}")
    fa, fb = out_a[0], out_b[0]
    for col in fa.columns:
        if col == "Time":
            continue
        assert fa[col].dtype == fb[col].dtype, (col, fa[col].dtype, fb[col].dtype)


if __name__ == "__main__":
    main()
