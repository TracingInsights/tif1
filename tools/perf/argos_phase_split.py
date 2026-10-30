"""Cold telemetry pipeline phase split on a warm CDN edge (Argos).

Each invocation is a fresh process with a throwaway cache that resolves the
Monaco 2026 refs and then runs ONE of:

- ``raw``      : production niquests session, GET + ``.content`` only (no parse)
- ``parse``    : ``fetch_multiple_async`` with cache reads/writes and validation off
- ``cachewrite``: same plus ``write_cache=True, defer_telemetry_writes=True``
- ``full``     : ``session.fetch_all_laps_telemetry()`` (assembly + tiers)

Usage:
    for m in raw parse cachewrite full; do uv run python tools/perf/argos_phase_split.py --mode $m; done
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
from functools import partial
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="full")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--src-dir", default=None)
    args = parser.parse_args()
    if args.src_dir:
        import sys

        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-phase-")
    import tif1
    from tif1.async_fetch import _get_executor, fetch_multiple_async
    from tif1.http_session import get_session as http_session

    session = tif1.get_session(args.year, args.gp, args.session)
    laps = session.laps
    refs = list(zip(laps["Driver"].astype(str), laps["LapNumber"].astype(int)))
    requests = [(args.year, session.gp, session.session, f"{d}/{n}_tel.json") for d, n in refs]
    t_cpu0 = time.process_time()
    t0 = time.perf_counter()
    if args.mode == "raw":
        http = http_session()
        loop_ex = _get_executor()
        sem = asyncio.Semaphore(22)
        sizes = []

        async def one(req):
            url = f"https://cdn.jsdelivr.net/gh/TracingInsights/{req[0]}@main/{req[1]}/{req[2]}/{req[3]}"
            async with sem:
                loop = asyncio.get_running_loop()
                resp = await loop.run_in_executor(loop_ex, partial(http.get, url, timeout=30))
                content = await loop.run_in_executor(loop_ex, lambda: resp.content)
                sizes.append(len(content or b""))

        async def run():
            await asyncio.gather(*(one(r) for r in requests))

        asyncio.run(run())
        n_ok = len(sizes)
    elif args.mode == "parse":
        res = asyncio.run(
            fetch_multiple_async(
                requests, use_cache=False, write_cache=False, validate_payload=False
            )
        )
        n_ok = sum(1 for r in res if isinstance(r, dict))
    elif args.mode == "cachewrite":
        res = asyncio.run(
            fetch_multiple_async(
                requests,
                use_cache=False,
                write_cache=True,
                validate_payload=False,
                defer_telemetry_writes=True,
            )
        )
        n_ok = sum(1 for r in res if isinstance(r, dict))
    else:
        tel = session.fetch_all_laps_telemetry()
        n_ok = len(tel)
    wall = time.perf_counter() - t0
    cpu = time.process_time() - t_cpu0
    print(
        json.dumps(
            {
                "mode": args.mode,
                "wall_s": round(wall, 3),
                "cpu_s": round(cpu, 3),
                "ok": n_ok,
                "n": len(requests),
            }
        )
    )


if __name__ == "__main__":
    main()
