"""G-series sizing probes: (1) cold session-table prefetch cost, (2) missing-file CDN-walk tax."""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-gprobe-")

import tif1


def probe_prefetch() -> None:
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    laps = session.laps  # triggers _prefetch_session_tables + laptimes wave
    print(f"laps phase (incl. weather+rcm prefetch + laptimes): {len(laps)} rows")

    # What did the prefetch actually drag in?
    for path in ("weather.json", "rcm.json", "drivers.json"):
        payload = session._get_local_payload(path)
        size = len(str(payload)) if payload else 0
        print(f"  local {path}: {'present' if payload else 'MISSING'} (~{size // 1024} KB str)")


async def probe_missing() -> None:
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    laps = session.laps
    refs = []
    for _, row in laps.iterrows():
        driver = row.get("Driver")
        lap_num = row.get("LapNumber")
        if driver is not None and lap_num is not None:
            refs.append((str(driver), int(lap_num)))

    from tif1.async_fetch import fetch_multiple_async

    requests = [(2026, "Monaco Grand Prix", "Race", f"{d}/{n}_tel.json") for d, n in refs]
    t0 = time.perf_counter()
    results = await fetch_multiple_async(
        requests, use_cache=False, write_cache=False, validate_payload=False
    )
    wall = time.perf_counter() - t0
    missing = [(req, res) for req, res in zip(requests, results) if res is None]
    print(f"\nbatch: {len(requests)} requests, {len(missing)} missing, wall={wall:.2f}s")
    for req, _res in missing:
        print(f"  missing: {req[3]}")

    # Time each missing file's full CDN walk in isolation (already edge-warm now).
    for req in [r for r, _ in missing][:3]:
        t0 = time.perf_counter()
        try:
            await fetch_multiple_async(
                [req], use_cache=False, write_cache=False, validate_payload=False
            )
        except Exception as e:
            print(f"  {req[3]}: raised {type(e).__name__}")
        dt = time.perf_counter() - t0
        print(f"  {req[3]}: 404-walk took {dt:.2f}s (warm edge)")


def main() -> None:
    probe_prefetch()
    asyncio.run(probe_missing())


if __name__ == "__main__":
    main()
