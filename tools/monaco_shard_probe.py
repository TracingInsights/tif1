"""Probe: single-CDN vs sharded-CDN batch fetch throughput (same files, live).

Measures whether splitting a large batch across multiple CDN hosts (each host
gets its own connection + congestion window + edge cache) beats a single CDN
for the identical file set. Uses the production shared niquests session.

Usage: uv run python tools/monaco_shard_probe.py --files 300
"""

from __future__ import annotations

import argparse
import os
import statistics
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

os.environ.setdefault("TIF1_CACHE_DIR", tempfile.mkdtemp(prefix="tif1-probe-"))

JSDELIVR = "https://cdn.jsdelivr.net/gh/TracingInsights/{year}@main/{gp}/{session}/{path}"
STATICDELIVR = "https://cdn.staticdelivr.com/gh/TracingInsights/{year}/main/{gp}/{session}/{path}"


def build_urls(year: int, gp: str, session: str, paths: list[str]) -> dict[str, list[str]]:
    return {
        "jsdelivr": [JSDELIVR.format(year=year, gp=gp, session=session, path=p) for p in paths],
        "staticdelivr": [
            STATICDELIVR.format(year=year, gp=gp, session=session, path=p) for p in paths
        ],
    }


def fetch_batch(
    session: object,
    executor: ThreadPoolExecutor,
    urls: list[str],
    concurrency: int,
) -> tuple[float, int, int, float]:
    """Fetch urls through the shared session with a thread-pool semaphore."""

    import threading

    sem = threading.Semaphore(concurrency)
    total_bytes = 0
    ok = 0
    t0 = time.perf_counter()

    def get(url: str) -> None:
        nonlocal total_bytes, ok
        with sem:
            r = session.get(url, timeout=30)
            if r.status_code == 200:
                total_bytes += len(r.content)
                ok += 1

    list(executor.map(get, urls))
    return (
        time.perf_counter() - t0,
        ok,
        total_bytes,
        total_bytes / max(1e-9, time.perf_counter() - t0),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=22)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    args = parser.parse_args()

    import tif1  # config init
    from tif1.http_session import get_session

    session = tif1.get_session(args.year, args.gp, args.session)
    laps = session.laps
    paths: list[str] = []
    for _, row in laps.iterrows():
        driver = row.get("Driver")
        lap_num = row.get("LapNumber")
        if driver is not None and lap_num is not None:
            paths.append(f"{driver}/{int(lap_num)}_tel.json")
    # Skip the first 300 (already warmed by diagnostics) to probe cold-ish files.
    paths = paths[300 : 300 + args.files]
    if len(paths) < args.files:
        raise SystemExit(f"only {len(paths)} paths available")

    urls = build_urls(args.year, args.gp, args.session, paths)
    shared = get_session()
    executor = ThreadPoolExecutor(max_workers=64)

    def shard_round() -> dict[str, float]:
        # Interleave hosts: even index -> jsdelivr, odd -> staticdelivr.
        mixed: list[str] = []
        for i, _p in enumerate(paths):
            mixed.append(urls["jsdelivr"][i] if i % 2 == 0 else urls["staticdelivr"][i])
        t, ok, mb, _ = fetch_batch(shared, executor, mixed, args.concurrency)
        return {"wall_s": t, "ok": ok, "mb": mb / 1e6}

    def single_round(host: str) -> dict[str, float]:
        t, ok, mb, _ = fetch_batch(shared, executor, urls[host], args.concurrency)
        return {"wall_s": t, "ok": ok, "mb": mb / 1e6}

    # Round 0: warm each CDN's edge with a few files (both hosts touched).
    fetch_batch(shared, executor, urls["jsdelivr"][:4], 4)
    fetch_batch(shared, executor, urls["staticdelivr"][:4], 4)

    results: dict[str, list[dict]] = {"jsdelivr": [], "staticdelivr": [], "shard": []}
    for r in range(args.rounds):
        for name, fn in (
            ("jsdelivr", lambda: single_round("jsdelivr")),
            ("staticdelivr", lambda: single_round("staticdelivr")),
            ("shard", shard_round),
        ):
            res = fn()
            results[name].append(res)
            print(f"round {r + 1} {name}: {res}")

    print("\n== summary (median wall_s) ==")
    for name, runs in results.items():
        walls = [x["wall_s"] for x in runs]
        print(f"{name}: median={statistics.median(walls):.3f}s ok={sum(x['ok'] for x in runs)}")


if __name__ == "__main__":
    main()
