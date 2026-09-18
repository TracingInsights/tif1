"""A/B null-like probe strategies on the real warm laps frame."""

from __future__ import annotations

import os
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/perf-v1"

import pandas as pd

import tif1
from tif1.core_utils.helpers import _NULL_LIKE_TOKEN_PROBE

session = tif1.get_session(2026, "Monaco Grand Prix", "Race")

laps_df = session.laps
print(f"laps frame: {laps_df.shape}")

# The probe runs on the PRE-dtype merged frame inside _process_lap_df;
# rebuild that exact frame from the warm payload tier.
import asyncio  # noqa: E402

from tif1.core_utils.helpers import _normalize_lap_payload  # noqa: E402

driver_requests = session._build_driver_laptime_requests(driver_pool=session._drivers_data)
payloads, _ = asyncio.run(
    session._fetch_laptime_payloads_async(driver_requests, operation="probe", ultra_cold=False)
)
merged = []
for (driver_info, _path), lap_data in zip(driver_requests, payloads):
    if not isinstance(lap_data, dict) or not lap_data:
        continue
    merged.append(
        (
            _normalize_lap_payload(lap_data),
            driver_info.get("driver", ""),
            driver_info.get("team", ""),
        )
    )
from tif1.core import _merge_lap_payloads  # noqa: E402

raw_df = pd.DataFrame(_merge_lap_payloads(merged), copy=False)
print(f"raw-ish frame for probe: {raw_df.shape}")


def timed(fn, repeat: int = 7) -> float:
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best


probe_list = _NULL_LIKE_TOKEN_PROBE
probe_arrow = pd.array(list(_NULL_LIKE_TOKEN_PROBE), dtype="str")


def variant_current():
    hits = []
    for col in raw_df.columns:
        s = raw_df[col]
        if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
            continue
        if s.isin(probe_list).any():
            hits.append(col)
    return hits


def variant_arrow_probe():
    hits = []
    for col in raw_df.columns:
        s = raw_df[col]
        if pd.api.types.is_object_dtype(s):
            if s.isin(probe_list).any():
                hits.append(col)
            continue
        if pd.api.types.is_string_dtype(s):
            if s.isin(probe_arrow).any():
                hits.append(col)
    return hits


def variant_unique_set():
    hits = []
    probe_set = set(probe_list)
    for col in raw_df.columns:
        s = raw_df[col]
        if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
            continue
        if probe_set & set(pd.unique(s.to_numpy() if hasattr(s, "to_numpy") else s)):
            hits.append(col)
    return hits


h0 = variant_current()
assert variant_arrow_probe() == h0, "arrow variant mismatch"
assert variant_unique_set() == h0, "unique variant mismatch"
t_a = timed(variant_current)
t_b = timed(variant_arrow_probe)
t_c = timed(variant_unique_set)
print(
    f"current={t_a * 1e3:.1f}ms arrow_probe={t_b * 1e3:.1f}ms unique_set={t_c * 1e3:.1f}ms hits={h0}"
)
