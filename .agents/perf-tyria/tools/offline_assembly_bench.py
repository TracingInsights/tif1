"""Offline assembly microbenchmarks for Tyria T2/T3/T8.

Synthetic payloads mirror real schema (Monaco 2026 shapes).
Fresh-process, interleaved A/B.
"""

import sys
import time

sys.path.insert(0, "src")

import numpy as np
import pandas as pd

from tif1.core_utils.helpers import (
    _create_telemetry_df,
    _merge_telemetry_payloads,
    _numeric_seconds_to_timedelta,
    _telemetry_frame_from_merged,
)

rng = np.random.default_rng(7)

N_TEL = 200  # payloads (assembly cost is linear in rows)
ROWS = 423


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


def make_lap_time_series(n: int) -> pd.Series:
    """Realistic LapTime column: numeric seconds + ~2% None (object dtype)."""
    r = np.random.default_rng(11)
    secs = r.random(n) * 100 + 70
    return pd.Series(
        [None if r.random() < 0.02 else float(x) for x in secs], dtype=object
    )


def bench(fn, iters=5):
    fn()  # warmup
    ts = []
    for _ in range(iters):
        t0 = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t0)
    return min(ts)


# --- T2: LapTime block (control = pre-PR full-column double-parse;
# candidate = shipped subset-fallback parse, identical to _process_lap_df) ---
N_LAPTIME = 20000
T2_SERIES = make_lap_time_series(N_LAPTIME)


def _t2_frame() -> pd.DataFrame:
    return pd.DataFrame({"LapTime": T2_SERIES})


def t2_control():
    df = _t2_frame()
    s = df["LapTime"]
    num = pd.to_numeric(s, errors="coerce")
    parsed = pd.to_timedelta(s, errors="coerce")
    df["LapTime"] = _numeric_seconds_to_timedelta(num).where(num.notna(), parsed)
    return df


def t2_candidate():
    df = _t2_frame()
    s = df["LapTime"]
    num = pd.to_numeric(s, errors="coerce")
    td = _numeric_seconds_to_timedelta(num)
    missing = num.isna()
    if bool(missing.any()):
        parsed_subset = pd.to_timedelta(s[missing], errors="coerce")
        td = td.copy()
        td[missing] = parsed_subset
    df["LapTime"] = td
    return df


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
    # NOTE: rejected sketch, kept for the record — the single-call form breaks
    # the NaN-guard contract (RESULTS.md T3); shipped code stays masked.
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
        try:
            pd.testing.assert_series_equal(
                t2_control()["LapTime"], t2_candidate()["LapTime"], check_dtype=True
            )
            print("T2 parity: OK (dtypes+values on 20k numeric+None)")
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
