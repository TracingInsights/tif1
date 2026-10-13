"""Kroton profiling: warm get_session breakdown, cold validation share,
laps dtype-pass detail. One script, three sections, cProfile output."""

import cProfile
import io
import os
import pstats

import orjson

import tif1
from tif1.cache import _decode_sqlite_value, get_cache
from tif1.validation import validate_telemetry_data

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"


def top(profiler, label, n=18, strip=True):
    s = io.StringIO()
    stats = pstats.Stats(profiler, stream=s)
    stats.sort_stats("cumulative").print_stats(n)
    text = s.getvalue()
    if strip:
        lines = text.splitlines()
        keep = [
            ln for ln in lines if "tif1" in ln or "pstats" in ln or "{" in ln or "seconds" in ln
        ]
        text = "\n".join(keep)
    print(f"===== {label} =====")
    print(text)


# Section A: warm get_session.
for _ in range(3):
    s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    s.load(telemetry=False, weather=False, messages=False)

pr = cProfile.Profile()
pr.enable()
for _ in range(10):
    s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
pr.disable()
top(pr, "get_session x10 (warm)")

c = get_cache()
rows = c.conn.execute(
    "SELECT data FROM telemetry_cache WHERE year=2026 AND gp='Monaco%20Grand%20Prix'"
    " AND session='Race'"
).fetchall()

pr = cProfile.Profile()
pr.enable()
payloads = [orjson.loads(_decode_sqlite_value(r[0])) for r in rows]
pr.disable()
top(pr, "payload-tier decode (orjson+zstd) x1452")

pr = cProfile.Profile()
pr.enable()

ok = 0
for p in payloads:
    try:
        if "tel" in p and isinstance(p["tel"], dict):
            validate_telemetry_data(p["tel"], strict=False, normalize=False)
        else:
            validate_telemetry_data(p, strict=False, normalize=False)
        ok += 1
    except Exception:
        pass
pr.disable()
print(f"validated {ok}/{len(payloads)}")
top(pr, "pydantic validate_payload x1452 (cold share)")

# Section C: laps first-touch dtype pass.
s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
s.load(telemetry=False, weather=False, messages=False)
s._laps_df = None  # drop any memo
pr = cProfile.Profile()
pr.enable()
_ = s.laps
pr.disable()
top(pr, "laps first-touch (payload tier)")
