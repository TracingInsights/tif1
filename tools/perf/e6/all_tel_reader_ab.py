"""E6 probe: fresh-process ``fetch_all_laps_telemetry()`` with the stream vs the file reader.

Same tree, same cache; the child pins the reader (``stream`` or ``file``) and
prints the phase. Parent alternates. Usage:
    TIF1_CACHE_DIR=... python tools/perf/e6/all_tel_reader_ab.py [PAIRS]
"""

from __future__ import annotations

import os
import statistics
import subprocess
import sys
import time

if len(sys.argv) > 1 and sys.argv[1] in {"stream", "file"}:
    import tif1
    from tif1 import cache as cache_mod

    if sys.argv[1] == "file":
        cache_mod._IPC_STREAM_READER = False
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
    _ = session.laps
    t0 = time.perf_counter()
    frames = session.fetch_all_laps_telemetry()
    print(f"{(time.perf_counter() - t0) * 1e3:.2f} {len(frames)}")
    raise SystemExit(0)

pairs = int(sys.argv[1]) if len(sys.argv) > 1 else 8
env = dict(os.environ, TIF1_OFFLINE_MODE="true")
results: dict[str, list[float]] = {"stream": [], "file": []}
for i in range(pairs):
    order = ("stream", "file") if i % 2 == 0 else ("file", "stream")
    for mode in order:
        out = subprocess.run(
            [sys.executable, __file__, mode], capture_output=True, text=True, env=env, check=True
        ).stdout.split()
        results[mode].append(float(out[0]))
wins = sum(s < f for s, f in zip(results["stream"], results["file"], strict=True))
for mode, values in results.items():
    print(f"{mode:6s} median {statistics.median(values):6.1f} ms  min {min(values):6.1f}")
print(f"stream faster in {wins}/{pairs} pairs")
