"""Naxos N10: ``_decode_str`` sub-steps and alternatives on a real driver row's DriverAhead codes."""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pyarrow as pa

import tif1
from tif1 import cache as cache_mod
from tif1.core_utils import frame_codec as fc

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
laps = session.laps
drivers = sorted({d for d, _ in session._telemetry_lap_refs(laps)})
rows = cache_mod.get_cache().get_telemetry_frame_rows(2025, session.gp, session.session, drivers)
inputs = []
for raw in rows.values():
    header, body = fc._writable_body(raw)
    for _columns, _glaps, lengths, specs in header:
        total = int(sum(lengths))
        for spec in specs:
            if spec[0] == "str":
                _, _placement, cats, offset, _n = spec
                codes = np.frombuffer(body, dtype="int32", count=total, offset=offset)
                inputs.append((cats, codes))
print(len(inputs), "str columns; nulls in", sum(bool((c < 0).any()) for _, c in inputs))
DTYPE = pd.StringDtype("pyarrow", na_value=np.nan)


def via_buffers(cats, codes):
    """large_string buffers built with numpy, no Arrow take."""
    encoded = [c.encode() for c in cats]
    lens = np.array([len(b) for b in encoded] + [0], dtype="int64")
    width = max(1, int(lens.max()))
    table = np.zeros((len(encoded) + 1, width), dtype="uint8")
    for i, b in enumerate(encoded):
        table[i, : len(b)] = np.frombuffer(b, dtype="uint8")
    nulls = codes < 0
    idx = np.where(nulls, len(encoded), codes) if nulls.any() else codes
    row_lens = lens[idx]
    offsets = np.empty(len(codes) + 1, dtype="int64")
    offsets[0] = 0
    np.cumsum(row_lens, out=offsets[1:])
    if (lens[:-1] == width).all() or width == 1:
        data = table[idx][:, :width].reshape(-1) if width == lens[0] else None
    data = table[idx][np.arange(width) < row_lens[:, None]]
    validity = None
    null_count = 0
    if nulls.any():
        validity = pa.py_buffer(np.packbits(~nulls, bitorder="little"))
        null_count = int(nulls.sum())
    arr = pa.LargeStringArray.from_buffers(
        len(codes), pa.py_buffer(offsets), pa.py_buffer(data), validity, null_count
    )
    return pd.arrays.ArrowStringArray(arr, dtype=DTYPE)


def via_dictionary(cats, codes):
    nulls = codes < 0
    indices = pa.array(codes, type=pa.int32(), mask=nulls) if nulls.any() else pa.array(codes)
    arr = pa.DictionaryArray.from_arrays(indices, pa.array(cats, type=pa.large_string()))
    return pd.arrays.ArrowStringArray(arr.cast(pa.large_string()), dtype=DTYPE)


def take_only(cats, codes):
    nulls = codes < 0
    indices = pa.array(codes, type=pa.int32(), mask=nulls) if nulls.any() else pa.array(codes)
    return pa.array(cats, type=pa.large_string()).take(indices)


def wrap_only(strings):
    return pd.arrays.ArrowStringArray(strings, dtype=DTYPE)


for c, k in inputs[:3]:
    assert via_buffers(c, k)._pa_array.equals(fc._decode_str(c, k)._pa_array)
    assert via_dictionary(c, k)._pa_array.equals(fc._decode_str(c, k)._pa_array)
for _round in range(2):
    for name, fn in (
        ("current", fc._decode_str),
        ("buffers", via_buffers),
        ("dictionary", via_dictionary),
        ("take_only", take_only),
    ):
        t0 = time.perf_counter()
        out = [fn(c, k) for c, k in inputs]
        print(f"{name:10s} {(time.perf_counter() - t0) * 1000:7.2f} ms")
    taken = [take_only(c, k) for c, k in inputs]
    t0 = time.perf_counter()
    [wrap_only(s) for s in taken]
    print(f"{'wrap_only':10s} {(time.perf_counter() - t0) * 1000:7.2f} ms")
