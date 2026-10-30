"""A11: deterministic simulation of intermittent jsDelivr 403s on a bulk fetch.

Models the live mechanism observed from this vantage: jsDelivr answers 403
("Package size exceeded the configured limit of 50 MB") for a fraction of the
files (its edge has not cached them) at ~10 ms, Hugging Face serves every
file at ~300 ms (redirect hop + xet CDN) and StaticDelivr at ~150 ms. Runs
``CDNManager.try_sources_async`` for ``--files`` payloads under a 22-wide
semaphore against the source tree on ``--src-dir`` and reports the wall time
plus how many payloads each mirror ended up serving.

Usage:
    uv run python tools/perf/argos_403_sim.py --src-dir /tmp/tif1-control/src
    uv run python tools/perf/argos_403_sim.py --src-dir src
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default="src")
    parser.add_argument("--files", type=int, default=300)
    parser.add_argument("--cold-fraction", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--concurrency", type=int, default=22)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(args.src_dir).resolve()))

    from tif1.cdn import CDNManager
    from tif1.exceptions import NetworkError

    latency = {"jsDelivr": 0.010, "HuggingFace": 0.300, "StaticDelivr": 0.150}
    rng = random.Random(args.seed)
    cold = {f"f{i}.json" for i in range(args.files) if rng.random() < args.cold_fraction}
    served: dict[str, int] = dict.fromkeys(latency, 0)

    manager = CDNManager()
    manager.reset()
    sources = manager.get_sources()
    names = [s.name for s in sources]

    async def fetch(source, url):
        name = source.name
        path = url.rsplit("/", 1)[-1]
        await asyncio.sleep(latency[name])
        if name == "jsDelivr" and path in cold:
            err = NetworkError(url=url, status_code=403)
            err.response = type("R", (), {"status_code": 403})()
            raise err
        served[name] += 1
        return {"ok": path}

    sem = asyncio.Semaphore(args.concurrency)

    async def one(i: int):
        async with sem:
            return await manager.try_sources_async(2026, "GP", "Race", f"f{i}.json", fetch)

    async def run():
        return await asyncio.gather(*(one(i) for i in range(args.files)), return_exceptions=True)

    t0 = time.perf_counter()
    results = asyncio.run(run())
    wall = time.perf_counter() - t0
    ok = sum(1 for r in results if isinstance(r, dict))
    print(
        json.dumps(
            {
                "src": args.src_dir,
                "sources": names,
                "files": args.files,
                "cold_on_jsdelivr": len(cold),
                "wall_s": round(wall, 3),
                "ok": ok,
                "served": served,
                "failure_counts": dict(manager._failure_counts),
            }
        )
    )


if __name__ == "__main__":
    main()
