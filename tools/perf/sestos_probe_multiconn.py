"""L4 probe: multi-connection batch fetch vs one multiplexed connection.

Fetches 300 known-good telemetry files from jsDelivr (warm edge) with:
  A) 1 niquests session, 22 concurrent
  B) 2 sessions x 11 concurrent each
  C) 4 sessions x 6 concurrent each
  D) 2 sessions x 22 concurrent each
Round-robin interleaved, 3 rounds each, medians reported.
"""

from __future__ import annotations

import asyncio
import sqlite3
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import niquests

DB = "/tmp/tif1-warm-cache/cache.sqlite"
BASE = "https://cdn.jsdelivr.net/gh/TracingInsights/2026@main/Monaco%20Grand%20Prix/Race"


def known_good_urls(n: int) -> list[str]:
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT driver, lap FROM telemetry_cache WHERE year=2026 AND gp=? AND session='Race' "
        "ORDER BY driver, lap LIMIT ?",
        ("Monaco%20Grand%20Prix", n),
    ).fetchall()
    conn.close()
    return [f"{BASE}/{driver}/{lap}_tel.json" for driver, lap in rows]


def make_session() -> niquests.Session:
    s = niquests.Session(multiplexed=True)
    s.trust_env = False
    return s


async def fetch_config(urls: list[str], n_sessions: int, per_session_concurrency: int) -> float:
    """Fetch urls split across n_sessions, each limited to per_session_concurrency."""
    sessions = [make_session() for _ in range(n_sessions)]
    executor = ThreadPoolExecutor(max_workers=64)
    chunks = [urls[i::n_sessions] for i in range(n_sessions)]
    total_bytes = 0

    async def fetch_chunk(session, chunk):
        nonlocal total_bytes
        sem = asyncio.Semaphore(per_session_concurrency)

        async def one(url):
            async with sem:
                loop = asyncio.get_running_loop()
                resp = await loop.run_in_executor(executor, lambda: session.get(url, timeout=30))
                return len(resp.content)

        return await asyncio.gather(*[one(u) for u in chunk])

    t0 = time.perf_counter()
    results = await asyncio.gather(*[fetch_chunk(s, c) for s, c in zip(sessions, chunks)])
    elapsed = time.perf_counter() - t0
    for r in results:
        total_bytes += sum(r)
    for s in sessions:
        s.close()
    executor.shutdown(wait=False)
    print(f"    ({total_bytes / 1e6:.1f} MB)")
    return elapsed


async def main() -> None:
    urls = known_good_urls(300)
    print(f"{len(urls)} urls")
    configs = [
        ("1x22", 1, 22),
        ("2x11", 2, 11),
        ("4x6", 4, 6),
        ("2x22", 2, 22),
    ]
    times: dict[str, list[float]] = {name: [] for name, _, _ in configs}
    for round_num in range(3):
        for name, ns, pc in configs:
            t = await fetch_config(urls, ns, pc)
            times[name].append(t)
            print(f"round {round_num} {name}: {t:.2f}s")
    print("\n=== medians ===")
    for name, _, _ in configs:
        print(
            f"{name}: median={statistics.median(times[name]):.2f}s runs={[round(t, 2) for t in times[name]]}"
        )


if __name__ == "__main__":
    asyncio.run(main())
