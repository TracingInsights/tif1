"""G3 probe: batch-scale single-host vs gcore-mirror-shard throughput.

Same production shared niquests session, thread-pool semaphore 22, 300 real
Monaco telemetry files (files 301-600, i.e. not the ones warmed by earlier
diagnostics). Variants: all cdn.jsdelivr.net / all gcore.jsdelivr.net /
50-50 shard. 2 rounds each, medians reported.
"""

from __future__ import annotations

import argparse
import os
import statistics
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

os.environ.setdefault("TIF1_CACHE_DIR", tempfile.mkdtemp(prefix="tif1-g3-"))

HOSTS = {
    "cdn": "https://cdn.jsdelivr.net/gh/TracingInsights/2026@main/Monaco%20Grand%20Prix/Race/{path}",
    "gcore": "https://gcore.jsdelivr.net/gh/TracingInsights/2026@main/Monaco%20Grand%20Prix/Race/{path}",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", type=int, default=300)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--concurrency", type=int, default=22)
    args = parser.parse_args()

    import tif1
    from tif1.http_session import get_session

    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    laps = session.laps
    paths: list[str] = []
    for _, row in laps.iterrows():
        driver = row.get("Driver")
        lap_num = row.get("LapNumber")
        if driver is not None and lap_num is not None:
            paths.append(f"{driver}/{int(lap_num)}_tel.json")
    paths = paths[301 : 301 + args.files]
    if len(paths) < args.files:
        raise SystemExit("not enough paths")

    shared = get_session()
    executor = ThreadPoolExecutor(max_workers=64)

    def fetch(urls: list[str]) -> tuple[float, int]:
        import threading

        sem = threading.Semaphore(args.concurrency)
        ok = 0

        def get(url: str) -> None:
            nonlocal ok
            with sem:
                r = shared.get(url, timeout=30)
                if r.status_code == 200:
                    ok += 1

        t0 = time.perf_counter()
        list(executor.map(get, urls))
        return time.perf_counter() - t0, ok

    def variant(name: str, urls: list[str]) -> float:
        wall, ok = fetch(urls)
        print(f"  {name}: {wall:.2f}s ok={ok}")
        return wall

    all_cdn = [HOSTS["cdn"].format(path=p) for p in paths]
    all_gcore = [HOSTS["gcore"].format(path=p) for p in paths]
    shard = [
        (HOSTS["cdn"] if i % 2 == 0 else HOSTS["gcore"]).format(path=p) for i, p in enumerate(paths)
    ]

    results: dict[str, list[float]] = {"cdn": [], "gcore": [], "shard": []}
    # Touch both hosts' edges with a few files first (same warm-up for all).
    fetch(all_cdn[:4])
    fetch(all_gcore[:4])

    for _ in range(args.rounds):
        for name, urls in (("cdn", all_cdn), ("gcore", all_gcore), ("shard", shard)):
            results[name].append(variant(name, urls))

    print("\n== medians ==")
    for name, walls in results.items():
        print(f"{name}: {statistics.median(walls):.2f}s")


if __name__ == "__main__":
    main()
