"""Profile the per-lap telemetry assembly loop on dumped real payloads."""

from __future__ import annotations

import cProfile
import io
import pickle
import pstats
import time

import pandas as pd  # noqa: F401

from tif1.core_utils.helpers import _create_telemetry_df

with open("/tmp/monaco_tel_payloads.pkl", "rb") as f:
    entries = pickle.load(f)
print(f"{len(entries)} payloads, rows={sum(len(e[2].get('Time', [])) for e in entries)}")

# warm imports/caches
for driver, lap, tel in entries[:20]:
    _create_telemetry_df(tel, driver, lap, "pandas")


def run() -> dict:
    out: dict = {}
    for driver, lap, tel in entries:
        df = _create_telemetry_df(tel, driver, lap, "pandas")
        if df is not None and not df.empty:
            out[(driver, lap)] = df
    return out


t0 = time.perf_counter()
run()
t1 = time.perf_counter()
print(f"baseline loop: {t1 - t0:.3f}s for {len(entries)} frames")

pr = cProfile.Profile()
pr.enable()
run()
pr.disable()
s = io.StringIO()
pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats(18)
print(s.getvalue())
