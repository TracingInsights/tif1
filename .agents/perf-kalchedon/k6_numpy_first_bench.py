"""K6 offline: numpy-first construction in _typed_telemetry_frame.

Current: canonical Int64 columns via pd.array(list); non-canonical columns
passed as raw Python lists (pandas sanitizes them element-by-element).
Candidate: pre-convert list columns to numpy arrays (np.asarray) when they are
None-free (pandas would infer the same dtype), keep pd.array for nullable
canonicals. Parity-gated: all 1452 frames must be identical (columns, dtypes,
values).
"""

from __future__ import annotations

import pickle
import time

import numpy as np
import pandas as pd

with open("/tmp/monaco_all_tel.pkl", "rb") as f:
    dump = pickle.load(f)
parsed: dict[tuple[str, int], dict] = dump["parsed"]

from tif1.core_utils.constants import TELEMETRY_RENAME_MAP  # noqa: E402
from tif1.core_utils.helpers import _typed_telemetry_frame  # noqa: E402


def normalize(tel_data, driver, lap_num):
    col_data = {}
    expected_len = None
    for k, v in tel_data.items():
        if not isinstance(v, list):
            continue
        col_data[TELEMETRY_RENAME_MAP.get(k, k)] = v
        if expected_len is None:
            expected_len = len(v)
    if not expected_len:
        return None
    max_len = max(len(v) for v in col_data.values())
    normalized = {}
    for k, v in col_data.items():
        if len(v) < max_len:
            normalized[k] = v + [None] * (max_len - len(v))
        else:
            normalized[k] = v
    return normalized, max_len


def numpy_first(normalized_data, max_len, driver, lap_num):
    try:
        frame_data = {}
        for k, v in normalized_data.items():
            if k == "Time":
                frame_data[k] = pd.to_timedelta(v, unit="s")
            elif k == "Brake" and None not in v:
                frame_data[k] = np.asarray(v, dtype=bool)
            elif k in ("nGear", "DRS"):
                # Nullable canonical: keep pd.array (None handling contract).
                if None not in v:
                    frame_data[k] = pd.array(np.asarray(v, dtype=np.int64), dtype="Int64")
                else:
                    frame_data[k] = pd.array(v, dtype="Int64")
            elif isinstance(v, list) and None not in v:
                # None-free column: np.asarray infers the same dtype pandas
                # would, without element-wise sanitize.
                arr = np.asarray(v)
                if arr.dtype.kind in "biuf":
                    frame_data[k] = arr
                else:
                    frame_data[k] = v
            else:
                frame_data[k] = v
        frame_data["Driver"] = pd.Series(np.full(max_len, driver, dtype=object), dtype=object)
        frame_data["LapNumber"] = pd.array(np.full(max_len, lap_num, dtype=np.int64), dtype="Int64")
        frame = pd.DataFrame(frame_data, copy=False)
    except (TypeError, ValueError):
        return None
    for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame


def frames_equal(a, b):
    if a is None or b is None:
        return a is b
    if list(a.columns) != list(b.columns):
        return False, "columns"
    for c in a.columns:
        if a[c].dtype != b[c].dtype:
            return False, f"dtype:{c}:{a[c].dtype}|{b[c].dtype}"
        if not a[c].equals(b[c]):
            return False, f"values:{c}"
    return True


work = []
for (d, lap), tel in parsed.items():
    n = normalize(tel, d, lap)
    if n:
        work.append((n[0], n[1], d, lap))
print(f"payloads: {len(work)}")


def run_current():
    out = {}
    for nd, ml, d, lap in work:
        out[(d, lap)] = _typed_telemetry_frame(nd, ml, d, lap)
    return out


def run_candidate():
    out = {}
    for nd, ml, d, lap in work:
        out[(d, lap)] = numpy_first(nd, ml, d, lap)
    return out


cur = run_current()
cand = run_candidate()

mismatch = 0
first_mismatches = []
for key in cur:
    ok = frames_equal(cur[key], cand[key])
    if ok is not True:
        mismatch += 1
        if len(first_mismatches) < 3:
            first_mismatches.append((key, ok))
print(f"parity: {len(cur) - mismatch}/{len(cur)} identical; mismatches: {first_mismatches}")


def bench(fn, n=3):
    best = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        best.append(time.perf_counter() - t0)
    return min(best)


t_cur = bench(run_current)
t_cand = bench(run_candidate)
print(f"current:   {t_cur:.3f} s ({t_cur / len(work) * 1000:.2f} ms/frame)")
print(
    f"candidate: {t_cand:.3f} s ({t_cand / len(work) * 1000:.2f} ms/frame)  speedup {t_cur / t_cand:.2f}x"
)
