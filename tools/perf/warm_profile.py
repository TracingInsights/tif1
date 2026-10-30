"""Profile the warm-cache full-telemetry load phase by phase.

Runs against the primed /tmp/tif1-warm-cache. Reports wall time of the batch
cache read, the per-frame assembly loop, and a cProfile of the telemetry phase.
"""

from __future__ import annotations

import cProfile
import io
import json
import os
import pstats
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

import tif1

session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
_ = session.laps

# Time the batch cache read directly (what _fetch_telemetry_batch_from_refs_async does)
from tif1.cache import get_cache  # noqa: E402

cache = get_cache()
refs = []
for _, row in session.laps.iterrows():
    driver, lap_num = row.get("Driver"), row.get("LapNumber")
    if driver is not None and lap_num is not None:
        refs.append((str(driver), int(lap_num)))

t0 = time.perf_counter()
cached = cache.get_telemetry_batch(2026, "Monaco Grand Prix", "Race", refs)
t_batch = time.perf_counter() - t0

# Clear parsed/memory tiers so the next call re-reads SQLite (steady-state check)
t0 = time.perf_counter()
cached2 = cache.get_telemetry_batch(2026, "Monaco Grand Prix", "Race", refs)
t_batch2 = time.perf_counter() - t0

# Time the per-frame assembly on the cached payloads
from tif1.core_utils.helpers import _create_telemetry_df  # noqa: E402

payloads = [(d, lap, p) for (d, lap), p in cached.items() if p]
t0 = time.perf_counter()
frames = [_create_telemetry_df(p, d, lap, "pandas") for d, lap, p in payloads]
t_assembly = time.perf_counter() - t0
n_frames = sum(1 for f in frames if f is not None)

# Full end-to-end telemetry phase under cProfile
prof = cProfile.Profile()
t0 = time.perf_counter()
prof.enable()
tel = session.fetch_all_laps_telemetry()
prof.disable()
t_e2e = time.perf_counter() - t0

s = io.StringIO()
pstats.Stats(prof, stream=s).sort_stats("cumulative").print_stats(28)
print(
    json.dumps(
        {
            "batch_read_s": round(t_batch, 3),
            "batch_read_again_s": round(t_batch2, 3),
            "assembly_s": round(t_assembly, 3),
            "n_payloads": len(payloads),
            "n_frames": n_frames,
            "e2e_telemetry_s": round(t_e2e, 3),
            "e2e_frames": len(tel),
        },
        indent=1,
    )
)
print(s.getvalue())
