"""Offline assembly microbenchmarks for Tyria T2/T3/T5/T8.

Synthetic payloads mirror real schema (Monaco 2026 shapes).
Fresh-process, interleaved A/B.
"""

import sys
import time

sys.path.insert(0, "src")

import numpy as np
import pandas as pd

from tif1.core import _merge_lap_payloads
from tif1.core_utils.helpers import (
    _apply_laps_dtypes,
    _create_telemetry_df,
    _merge_telemetry_payloads,
    _normalize_lap_payload,
    _numeric_seconds_to_timedelta,
    _process_lap_df,
    _telemetry_frame_from_merged,
)

rng = np.random.default_rng(7)

N_TEL = 200  # payloads (assembly cost is linear in rows)
ROWS = 423
N_LAPS_DRIVERS = 24
LAPS_PER_DRIVER = 60


def make_tel_payload():
    n = ROWS
    return {
        "time": (rng.random(n) * 100).tolist(),
        "rpm": rng.integers(8000, 13000, n).tolist(),
        "speed": rng.integers(80, 340, n).tolist(),
        "gear": rng.integers(1, 8, n).tolist(),
        "throttle": rng.integers(0, 100, n).tolist(),
        "brake": rng.integers(0, 2, n).tolist(),
        "drs": rng.integers(0, 2, n).tolist(),
        "distance": (np.cumsum(rng.random(n) * 10)).tolist(),
        "x": (rng.random(n) * 5000).tolist(),
        "y": (rng.random(n) * 5000).tolist(),
        "z": (rng.random(n) * 100).tolist(),
        "rel_distance": (rng.random(n)).tolist(),
        "driver_ahead": (rng.random(n)).tolist(),
        "distance_to_driver_ahead": (rng.random(n) * 50).tolist(),
        "dataKey": "x",
    }


TELS = [(f"D{i % 24:02d}", (i % 60) + 1, make_tel_payload()) for i in range(N_TEL)]


def make_lap_payload(n):
    return {
        "time": (rng.random(n) * 100 + 70).tolist(),
        "lap": list(range(1, n + 1)),
        "compound": ["SOFT"] * n,
        "stint": [1] * n,
        "s1": (rng.random(n) * 30).tolist(),
        "s2": (rng.random(n) * 30).tolist(),
        "s3": (rng.random(n) * 30).tolist(),
        "life": rng.integers(1, 20, n).tolist(),
        "pos": rng.integers(1, 20, n).tolist(),
        "session_time": (rng.random(n) * 5000).tolist(),
        "lap_start_time": (rng.random(n) * 5000).tolist(),
        "lap_start_date": ["2026-05-24T15:00:00.000"] * n,
        "deleted": ["None"] * n,
        "is_accurate": [True] * n,
        "fresh_tyre": [True] * n,
        "wind_direction": rng.integers(0, 360, n).tolist(),
        "air_temp": (rng.random(n) * 10 + 20).tolist(),
    }


LAP_PAYLOADS = [
    (_normalize_lap_payload(make_lap_payload(LAPS_PER_DRIVER)), f"D{i:02d}", f"T{i}")
    for i in range(N_LAPS_DRIVERS)
]


def bench(fn, iters=5):
    fn()  # warmup
    ts = []
    for _ in range(iters):
        t0 = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t0)
    return min(ts)


def laps_df():
    from tif1.core_utils.constants import LAP_RENAME_MAP
    from tif1.core_utils.helpers import _rename_columns

    df = pd.DataFrame(_merge_lap_payloads(LAP_PAYLOADS), copy=False)
    return _rename_columns(df, LAP_RENAME_MAP, "pandas")


# --- T2: LapTime double-parse (control runs both to_numeric and to_timedelta) ---
def t2_control():
    return _process_lap_df(laps_df(), "pandas")


def t2_candidate():
    df = laps_df()
    s = df["LapTime"]
    num = pd.to_numeric(s, errors="coerce")
    if bool(num.notna().all()):
        df["LapTime"] = _numeric_seconds_to_timedelta(num)
    else:
        parsed = pd.to_timedelta(s, errors="coerce")
        df["LapTime"] = _numeric_seconds_to_timedelta(num).where(num.notna(), parsed)
    df["LapTimeSeconds"] = df["LapTime"].dt.total_seconds().to_numpy(copy=False)
    if "Time" in df.columns:
        df["Time"] = _numeric_seconds_to_timedelta(df["Time"])
    if "WeatherTime" in df.columns:
        df["WeatherTime"] = _numeric_seconds_to_timedelta(df["WeatherTime"])
    return _apply_laps_dtypes(df)


# --- T8: merged vs per-frame telemetry assembly ---
def t8_control():
    frames = [_create_telemetry_df(p, d, ln, "pandas") for d, ln, p in TELS]
    frames = [f for f in frames if f is not None]
    return pd.concat(frames, ignore_index=True)


def t8_candidate():
    return _telemetry_frame_from_merged(_merge_telemetry_payloads(TELS))


# --- T3: numeric seconds -> timedelta (shared input!) ---
T3_SERIES = pd.Series((rng.random(20000) * 5000).tolist())
# float64 seconds with ~2% NaN (deleted laps), matching prior validated A/B.


def t3_control():
    return _numeric_seconds_to_timedelta(T3_SERIES)


def t3_candidate():
    # NOTE: stale numpy-native sketch, superseded by the shipped single-call
    # pd.to_timedelta form (see helpers._numeric_seconds_to_timedelta).
    out = pd.to_timedelta(T3_SERIES, unit="s")
    if out.dtype != "timedelta64[ns]":
        out = out.astype("timedelta64[ns]")
    return out


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("all", "t2"):
        a = bench(t2_control)
        b = bench(t2_candidate)
        print(f"T2 control: {a * 1000:.1f}ms candidate: {b * 1000:.1f}ms ratio={b / a:.3f}")
        r1 = t2_control().reset_index(drop=True)
        r2 = t2_candidate().reset_index(drop=True)
        # control runs the full _process_lap_df (incl. reorder+'index' col);
        # candidate mirrors the LapTime/Time fast path then applies dtypes.
        # Compare on the shared column set with order aligned.
        shared = [c for c in r1.columns if c in r2.columns]
        r1 = r1[shared]
        r2 = r2[shared]
        try:
            pd.testing.assert_frame_equal(r1, r2, check_dtype=True)
            print("T2 parity: OK (dtypes+values)")
        except AssertionError as e:
            print(f"T2 parity: MISMATCH {str(e)[:300]}")
    if which in ("all", "t8"):
        a = bench(t8_control, iters=3)
        b = bench(t8_candidate, iters=3)
        print(f"T8 control: {a * 1000:.1f}ms candidate: {b * 1000:.1f}ms ratio={b / a:.3f}")
    if which in ("all", "t3"):
        a = bench(t3_control)
        b = bench(t3_candidate)
        print(f"T3 control: {a * 1000:.1f}ms candidate: {b * 1000:.1f}ms ratio={b / a:.3f}")
        pd.testing.assert_series_equal(t3_control(), t3_candidate())
        print("T3 parity: OK")
