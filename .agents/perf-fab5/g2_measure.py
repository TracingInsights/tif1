"""G2 measurement: missing-file CDN-walk latency, sequential vs raced fallback.

Runs the production fetch path for a file that 404s on every CDN
(LEC/66_tel.json, 2026 Monaco Race) against the control tree (G5 tip) and the
candidate tree (G2 raced fallback), N repetitions each in fresh subprocesses.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile

SNIPPET = """
import asyncio, json, os, sys, time
sys.path.insert(0, {src!r})
os.environ["TIF1_CACHE_DIR"] = {cache!r}
import tif1
from tif1.async_fetch import fetch_json_async

async def one():
    try:
        await fetch_json_async(2026, "Monaco Grand Prix", "Race", "LEC/66_tel.json",
                               use_cache=False, write_cache=False, validate_payload=False)
    except Exception:
        pass

async def main():
    times = []
    for _ in range(6):
        t0 = time.perf_counter()
        await one()
        times.append(time.perf_counter() - t0)
    print(json.dumps([round(t, 3) for t in times]))

asyncio.run(main())
"""


def run(src: str) -> list[float]:
    cache = tempfile.mkdtemp(prefix="tif1-g2-")
    code = SNIPPET.format(src=src, cache=cache)
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        print(proc.stderr[-600:])
        raise SystemExit(f"walk snippet failed for {src}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def main() -> None:
    for label, src in (
        ("control(G5, sequential)", "/tmp/tif1-control/src"),
        ("candidate(G2, raced)", "src"),
    ):
        times = run(src)
        best = min(times)
        print(f"{label}: walks={[round(t, 2) for t in times]} best={best:.2f}s")


if __name__ == "__main__":
    main()
