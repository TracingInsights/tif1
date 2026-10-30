"""A10: bulk-fetch throughput probe — niquests (production) vs curl_cffi vs aiohttp.

Fetches the same ``--files`` Monaco 2026 telemetry URLs with each client at
22-wide concurrency, interleaved over ``--rounds`` rounds in fresh processes,
and reports wall time, bytes and non-200 counts. Run with:

    uv run --with curl_cffi --with aiohttp python tools/perf/argos_transport_probe.py --rounds 3
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CONCURRENCY = 22


def _urls(args: argparse.Namespace) -> list[str]:
    os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-a10-")
    import tif1

    session = tif1.get_session(args.year, args.gp, args.session)
    laps = session.laps
    refs = list(zip(laps["Driver"].astype(str), laps["LapNumber"].astype(int)))[: args.files]
    base = f"https://cdn.jsdelivr.net/gh/TracingInsights/{args.year}@main/{session.gp}/{session.session}"
    return [f"{base}/{d}/{n}_tel.json" for d, n in refs]


async def run_niquests(urls: list[str]) -> tuple[int, int]:
    from functools import partial

    from tif1.async_fetch import _get_executor
    from tif1.http_session import get_session

    http = get_session()
    loop = asyncio.get_running_loop()
    ex = _get_executor()
    sem = asyncio.Semaphore(CONCURRENCY)
    statuses: list[int] = []
    sizes: list[int] = []

    async def one(url: str) -> None:
        async with sem:
            resp = await loop.run_in_executor(ex, partial(http.get, url, timeout=30))
            content = await loop.run_in_executor(ex, lambda: resp.content)
            statuses.append(resp.status_code)
            sizes.append(len(content or b""))

    await asyncio.gather(*(one(u) for u in urls))
    return sum(1 for s in statuses if s != 200), sum(sizes)


async def run_curl_cffi(urls: list[str]) -> tuple[int, int]:
    from curl_cffi.requests import AsyncSession

    sem = asyncio.Semaphore(CONCURRENCY)
    statuses: list[int] = []
    sizes: list[int] = []
    async with AsyncSession(max_clients=CONCURRENCY) as s:

        async def one(url: str) -> None:
            async with sem:
                resp = await s.get(url, timeout=30)
                statuses.append(resp.status_code)
                sizes.append(len(resp.content))

        await asyncio.gather(*(one(u) for u in urls))
    return sum(1 for st in statuses if st != 200), sum(sizes)


async def run_aiohttp(urls: list[str]) -> tuple[int, int]:
    import aiohttp

    sem = asyncio.Semaphore(CONCURRENCY)
    statuses: list[int] = []
    sizes: list[int] = []
    conn = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=conn) as s:

        async def one(url: str) -> None:
            async with sem:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    body = await resp.read()
                    statuses.append(resp.status)
                    sizes.append(len(body))

        await asyncio.gather(*(one(u) for u in urls))
    return sum(1 for st in statuses if st != 200), sum(sizes)


RUNNERS = {"niquests": run_niquests, "curl_cffi": run_curl_cffi, "aiohttp": run_aiohttp}


def single(args: argparse.Namespace) -> None:
    urls = _urls(args)
    t0 = time.perf_counter()
    bad, nbytes = asyncio.run(RUNNERS[args.client](urls))
    wall = time.perf_counter() - t0
    print(
        json.dumps(
            {
                "client": args.client,
                "wall_s": round(wall, 3),
                "non200": bad,
                "mb": round(nbytes / 1e6, 2),
                "n": len(urls),
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--files", type=int, default=150)
    parser.add_argument("--rounds", type=int, default=0)
    parser.add_argument("--client", default="niquests")
    parser.add_argument("--out", default=".agents/perf-argos/a10_transport.json")
    args = parser.parse_args()
    if args.rounds == 0:
        single(args)
        return
    records = []
    order = list(RUNNERS)
    for rnd in range(args.rounds):
        clients = order if rnd % 2 == 0 else order[::-1]
        for client in clients:
            cmd = [sys.executable, __file__, "--client", client, "--files", str(args.files)]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            line = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
            rec = (
                json.loads(line)
                if line.startswith("{")
                else {"client": client, "error": proc.stderr[-400:]}
            )
            rec["round"] = rnd
            records.append(rec)
            print(rec, file=sys.stderr)
    summary = {
        c: {
            "median": statistics.median(
                [r["wall_s"] for r in records if r.get("client") == c and "wall_s" in r]
                or [float("nan")]
            )
        }
        for c in order
    }
    Path(args.out).write_text(
        json.dumps({"records": records, "summary": summary}, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
