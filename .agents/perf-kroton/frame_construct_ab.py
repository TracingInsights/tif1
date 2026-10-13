"""Kroton KR-2 probe: direct BlockManager construction + fast conversions in
the cold-path telemetry frame assembly, vs the current pd.DataFrame(dict) path.

Arms:
  current  = exact copy of _typed_telemetry_frame's typing loop + pd.DataFrame(dict)
  candidate = fast_td_ns Time replica + object-ndarray Driver + arrow string
             lists + direct BlockManager/new_block assembly.

Parity: assert_frame_equal on every frame (dtypes and values), then interleaved
best-of timing on the full build+assemble pipeline.
"""

import os
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

import numpy as np
import orjson
import pandas as pd
import pyarrow as pa
from pandas._libs.internals import BlockPlacement
from pandas.core.arrays import string_arrow
from pandas.core.construction import ensure_wrapped_if_datetimelike
from pandas.core.internals.blocks import ensure_block_shape, new_block
from pandas.core.internals.managers import BlockManager

from tif1.cache import _decode_sqlite_value, get_cache
from tif1.core_utils.constants import TELEMETRY_RENAME_MAP

_NAT = np.iinfo("int64").min
_STR_DTYPE = pd.array([], dtype="str").dtype
_NEEDED = ("Time", "Speed", "nGear", "X", "Y", "Z")


def load_payloads(limit=None):
    c = get_cache()
    q = (
        "SELECT data FROM telemetry_cache WHERE year=2026"
        " AND gp='Monaco%20Grand%20Prix' AND session='Race'"
    )
    if limit:
        q += f" LIMIT {limit}"
    rows = c.conn.execute(q).fetchall()
    payloads = []
    for r in rows:
        blob = _decode_sqlite_value(r[0])
        try:
            data = orjson.loads(blob)
        except Exception:
            continue
        if isinstance(data, dict) and any(isinstance(v, list) for v in data.values()):
            payloads.append(data)
    return payloads


def normalize(tel_data):
    col_data = {}
    for k, v in tel_data.items():
        if isinstance(v, list):
            col_data[TELEMETRY_RENAME_MAP.get(k, k)] = v
    max_len = max(len(v) for v in col_data.values())
    for k, v in col_data.items():
        if len(v) < max_len:
            col_data[k] = v + [None] * (max_len - len(v))
    return col_data, max_len


def fast_td_ns(vals):
    """Vectorized bit-exact replica of pd.to_timedelta(f8 ndarray, unit='s')."""
    with np.errstate(invalid="ignore"):
        base = vals.astype("int64")
        frac = np.round(vals - base, 9)
        out = base * 1000000000 + (frac * 1e9).astype("int64")
    out[np.isnan(vals)] = _NAT
    return out.view("timedelta64[ns]")


def build_current(normalized, max_len, driver, lap_num):
    """Exact copy of _typed_telemetry_frame's typing loop."""
    frame_data = {}
    for k, v in normalized.items():
        if k == "Time":
            try:
                frame_data[k] = pd.to_timedelta(np.asarray(v, dtype="float64"), unit="s")
            except (TypeError, ValueError):
                frame_data[k] = pd.to_timedelta(v, unit="s")
        elif k == "Brake" and None not in v:
            frame_data[k] = np.asarray(v, dtype=bool)
        elif k in ("nGear", "DRS"):
            arr = np.asarray(v)
            if arr.dtype.kind in "iu":
                frame_data[k] = pd.arrays.IntegerArray(arr, np.zeros(len(arr), dtype=bool))
            else:
                frame_data[k] = pd.array(v, dtype="Int64")
        else:
            first = v[0] if v else None
            if isinstance(first, (int, float)) and not isinstance(first, bool):
                arr = np.asarray(v)
                frame_data[k] = arr if arr.dtype.kind in "if" else v
            else:
                frame_data[k] = v
    frame_data["Driver"] = pd.Series(np.full(max_len, driver, dtype=object), dtype=object)
    frame_data["LapNumber"] = pd.arrays.IntegerArray(
        np.full(max_len, lap_num, dtype="int64"), np.zeros(max_len, dtype=bool)
    )
    return frame_data


def build_candidate(normalized, max_len, driver, lap_num):
    """Candidate typing loop: fast Time + raw Driver ndarray; lists stay raw."""
    frame_data = {}
    for k, v in normalized.items():
        if k == "Time":
            try:
                arr = np.asarray(v, dtype="float64")
            except (TypeError, ValueError):
                arr = None
            if arr is not None and np.isfinite(arr).all() and np.abs(arr).max(initial=0.0) < 9.0e9:
                frame_data[k] = fast_td_ns(arr)
            else:
                # Fallback: replicate current behavior incl. exceptions on inf.
                frame_data[k] = pd.to_timedelta(
                    arr if arr is not None else v, unit="s"
                )
        else:
            frame_data[k] = None  # placeholder replaced below
            frame_data[k] = _typed_other(k, v)
    frame_data["Driver"] = np.full(max_len, driver, dtype=object)
    frame_data["LapNumber"] = pd.arrays.IntegerArray(
        np.full(max_len, lap_num, dtype="int64"), np.zeros(max_len, dtype=bool)
    )
    return frame_data


def _typed_other(k, v):
    if k == "Brake" and None not in v:
        return np.asarray(v, dtype=bool)
    if k in ("nGear", "DRS"):
        arr = np.asarray(v)
        if arr.dtype.kind in "iu":
            return pd.arrays.IntegerArray(arr, np.zeros(len(arr), dtype=bool))
        return pd.array(v, dtype="Int64")
    first = v[0] if v else None
    if isinstance(first, (int, float)) and not isinstance(first, bool):
        arr = np.asarray(v)
        return arr if arr.dtype.kind in "if" else v
    return v


def assemble_current(frame_data, max_len):
    frame = pd.DataFrame(frame_data, copy=False)
    for col in _NEEDED:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame


def to_block_values(val):
    """Coerce a constructor input to block values shaped (1, n) for a 2-D mgr."""
    if isinstance(val, list):
        first = val[0] if val else None
        if isinstance(first, str):
            # Same class/dtype/values as pd.array(..., dtype='str') but ~2x faster.
            try:
                val = string_arrow.ArrowStringArray(
                    pa.chunked_array([pa.array(val)]), dtype=_STR_DTYPE
                )
            except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
                val = np.asarray(val, dtype=object)  # mixed -> object like DataFrame
        else:
            from pandas.api.types import infer_dtype

            kind = infer_dtype(val, skipna=True)
            if kind == "string":
                val = string_arrow.ArrowStringArray(
                    pa.chunked_array([pa.array(val)]), dtype=_STR_DTYPE
                )
            elif kind == "boolean" and None not in val:
                val = np.asarray(val, dtype=bool)
            elif kind == "empty" and not val:
                val = np.empty(0, dtype="float64")
            else:
                val = np.asarray(val, dtype=object)
    if isinstance(val, (pd.Series, pd.Index)):
        val = val._values
    if isinstance(val, np.ndarray) and val.dtype.kind in "mM":
        val = ensure_wrapped_if_datetimelike(val)
    return ensure_block_shape(val, ndim=2)


def assemble_candidate(frame_data, max_len):
    cols = list(frame_data.keys())
    for col in _NEEDED:
        if col not in cols:
            frame_data[col] = pd.array([pd.NA] * max_len, dtype="Int64")
            cols.append(col)
    blocks = [
        new_block(to_block_values(frame_data[name]), BlockPlacement(slice(i, i + 1)), ndim=2)
        for i, name in enumerate(cols)
    ]
    col_index = pd.Index(cols)
    row_index = pd.RangeIndex(max_len)
    mgr = BlockManager(blocks, [col_index, row_index], verify_integrity=False)
    return pd.DataFrame._from_mgr(mgr, axes=[col_index, row_index])


def run_arm(arm, payloads, repeat):
    """Time build+assemble for all payloads; return best-of-repeat total secs."""
    best = 1e9
    for _ in range(repeat):
        t0 = time.perf_counter()
        for p in payloads:
            normalized, max_len = normalize(p)
            fd = (build_current if arm == "current" else build_candidate)(
                normalized, max_len, "VER", 1
            )
            (assemble_current if arm == "current" else assemble_candidate)(fd, max_len)
        best = min(best, time.perf_counter() - t0)
    return best


def main():
    payloads = load_payloads(limit=None)
    print(f"{len(payloads)} payloads, cols sample: {sorted(normalize(payloads[0])[0])}")

    # Parity on every frame.
    str_cols = set()
    for p in payloads:
        normalized, max_len = normalize(p)
        for k, v in normalized.items():
            if v and isinstance(v[0], str):
                str_cols.add(k)
        fd_c = build_current(normalized, max_len, "VER", 1)
        fd_n = build_candidate(normalized, max_len, "VER", 1)
        a = assemble_current(dict(fd_c), max_len)
        b = assemble_candidate(dict(fd_n), max_len)
        pd.testing.assert_frame_equal(a, b)
    print(f"parity OK on all {len(payloads)} frames; string channels: {sorted(str_cols)}")

    # Interleaved timing.
    res = {}
    for arm in ("current", "candidate", "current", "candidate", "current", "candidate"):
        res.setdefault(arm, []).append(run_arm(arm, payloads, 3))
    for arm in ("current", "candidate"):
        best = min(res[arm])
        print(f"{arm:10} best-of {best:.4f}s for {len(payloads)} frames = {best/len(payloads)*1e6:.0f} us/frame")
    ratio = min(res["current"]) / min(res["candidate"])
    print(f"speedup: {ratio:.2f}x")


if __name__ == "__main__":
    main()
