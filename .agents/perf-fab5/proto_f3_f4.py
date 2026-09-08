"""Offline prototype for F3 (merged result-map) and F4 (timedelta cast) on real payloads.

Measures on the 400 dumped Monaco payloads:
  A. current per-frame _create_telemetry_df loop (production path)
  B. merged-dict build + per-lap iloc slice views (parity-checked)
  C. merged build with fast timedelta cast (F4) instead of pd.to_timedelta
  D. current loop with fast timedelta cast in _apply_telemetry_dtypes

Parity: keys, columns, dtypes, and values compared variant-by-variant.
"""

from __future__ import annotations

import pickle
import time

import numpy as np
import pandas as pd

from tif1.core_utils.constants import COL_DRIVER, COL_LAP_NUMBER
from tif1.core_utils.helpers import (
    _apply_telemetry_dtypes,
    _create_telemetry_df,
    _merge_telemetry_payloads,
)


def fast_timedelta(seconds: pd.Series | list) -> pd.Series:
    """float seconds -> timedelta64[ns] via multiply+view (NaN -> NaT)."""
    arr = np.asarray(seconds, dtype="float64")
    return pd.Series(arr * 1e9, dtype="timedelta64[ns]")


def variant_a(entries):
    out = {}
    for driver, lap, tel in entries:
        df = _create_telemetry_df(tel, driver, lap, "pandas")
        if df is not None and not df.empty:
            out[(driver, lap)] = df
    return out


def _entry_rows(tel: dict) -> int | None:
    if not isinstance(tel, dict) or not tel:
        return None
    lists = [v for v in tel.values() if isinstance(v, list)]
    if not lists:
        return None
    n = max(len(v) for v in lists)
    return n or None


def _merged_frame(entries, fast: bool):
    merged = _merge_telemetry_payloads(entries)
    frame = pd.DataFrame(merged, copy=False)
    if fast:
        if "Time" in frame.columns:
            arr = frame["Time"].to_numpy(dtype="float64", na_value=np.nan)
            frame["Time"] = pd.Series(arr * 1e9, dtype="timedelta64[ns]", index=frame.index)
        if "Brake" in frame.columns and not frame["Brake"].isna().any():
            frame["Brake"] = frame["Brake"].astype(bool)
        if "nGear" in frame.columns:
            frame["nGear"] = frame["nGear"].astype("Int64")
        if "DRS" in frame.columns:
            frame["DRS"] = frame["DRS"].astype("Int64")
        if COL_DRIVER in frame.columns:
            frame[COL_DRIVER] = frame[COL_DRIVER].astype(object)
        if COL_LAP_NUMBER in frame.columns:
            frame[COL_LAP_NUMBER] = frame[COL_LAP_NUMBER].astype("Int64")
        for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
            if col not in frame.columns:
                frame[col] = pd.NA
        return frame
    return _apply_telemetry_dtypes(frame)


def variant_b(entries, fast: bool = False):
    frame = _merged_frame(entries, fast)
    out = {}
    start = 0
    for driver, lap, tel in entries:
        n = _entry_rows(tel)
        if n is None:
            continue
        out[(driver, lap)] = frame.iloc[start : start + n].reset_index(drop=True)
        start += n
    return out


def parity(a, b, label) -> None:
    assert set(a) == set(b), f"{label}: key mismatch"
    for k in a:
        fa, fb = a[k], b[k]
        assert list(fa.columns) == list(fb.columns), f"{label}: columns {k}"
        for col in fa.columns:
            da, db = fa[col].dtype, fb[col].dtype
            assert str(da) == str(db), f"{label}: dtype {k} {col}: {da} != {db}"
            va, vb = fa[col].to_numpy(), fb[col].to_numpy()
            if fa[col].dtype == object:
                assert (va == vb).all(), f"{label}: values {k} {col}"
            else:
                equal = (va == vb) | (pd.isna(va) & pd.isna(vb))
                assert equal.all(), f"{label}: values {k} {col}"
    print(f"parity OK: {label}")


def bench(fn, entries, n=3):
    best = float("inf")
    for _ in range(n):
        t0 = time.perf_counter()
        fn(entries)
        best = min(best, time.perf_counter() - t0)
    return best


def main() -> None:
    with open("/tmp/monaco_tel_payloads.pkl", "rb") as f:
        entries = pickle.load(f)
    rows = sum(_entry_rows(e[2]) or 0 for e in entries)
    print(f"{len(entries)} payloads, {rows} rows")

    a = variant_a(entries)
    b = variant_b(entries)
    c = variant_b(entries, fast=True)
    parity(a, b, "A vs B (merged)")
    parity(a, c, "A vs C (merged+fast timedelta)")
    parity(b, c, "B vs C")

    for name, fn in (
        ("A current per-frame", variant_a),
        ("B merged + slices", lambda e: variant_b(e, fast=False)),
        ("C merged + fast timedelta", lambda e: variant_b(e, fast=True)),
    ):
        print(f"{name}: {bench(fn, entries):.3f}s")

    # F4 isolated on the current per-frame path: time the dtype pass alone.
    t0 = time.perf_counter()
    for _driver, _lap, tel in entries[:200]:
        _apply_telemetry_dtypes(pd.DataFrame({k: v for k, v in tel.items() if isinstance(v, list)}))
    print(f"legacy _apply_telemetry_dtypes on 200 raw frames: {time.perf_counter() - t0:.3f}s")


if __name__ == "__main__":
    main()
