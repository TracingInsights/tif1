"""Kroton profiling round 2: per-payload validation cost, laps first-touch,
first get_session in a fresh process."""

import cProfile
import io
import os
import pstats
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

# First get_session in this fresh process (includes events schedule load).
import tif1

t0 = time.perf_counter()
s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
first_gs = time.perf_counter() - t0
t0 = time.perf_counter()
s2 = tif1.get_session(2026, "Monaco Grand Prix", "Race")
warm_gs = time.perf_counter() - t0
print(f"get_session first call: {first_gs*1000:.1f} ms, second: {warm_gs*1000:.1f} ms")

# Per-payload pydantic validation (imports warmed outside the profile).
import orjson

from tif1.cache import _decode_sqlite_value, get_cache
from tif1.validation import validate_telemetry_data

c = get_cache()
rows = c.conn.execute(
    "SELECT data FROM telemetry_cache WHERE year=2026 AND gp='Monaco%20Grand%20Prix'"
    " AND session='Race'"
).fetchall()
payloads = []
for r in rows:
    data = orjson.loads(_decode_sqlite_value(r[0]))
    payloads.append(data["tel"] if "tel" in data else data)

t0 = time.perf_counter()
ok = 0
for p in payloads:
    try:
        validate_telemetry_data(p, strict=False, normalize=False)
        ok += 1
    except Exception:
        pass
dt = time.perf_counter() - t0
print(f"validate_telemetry_data: {dt:.3f} s for {ok}/{len(payloads)} payloads")

# Laps first-touch.
s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
s.load(telemetry=False, weather=False, messages=False)
pr = cProfile.Profile()
pr.enable()
_ = s.laps
pr.disable()
out = io.StringIO()
pstats.Stats(pr, stream=out).sort_stats("cumulative").print_stats(14)
print(out.getvalue())
