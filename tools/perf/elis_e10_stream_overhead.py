"""Elis E10 probe: a lazy wave through the stream vs a gather wave, fake network.

Each request sleeps ``--latency`` seconds on the event loop (no sockets).
Prints the median wave time of ``--requests`` requests at 22 in flight:
``gather`` is E1's wave (``asyncio.run`` of a semaphore-gated gather from the
calling thread), ``stream`` the E10 stream (one request per call, the caller
blocked on the futures).

Usage: python tools/perf/elis_e10_stream_overhead.py [--requests 57] [--reps 15]
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import statistics
import time

from tif1.core_utils.telemetry_stream import PriorityFetchStream


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=57)
    parser.add_argument("--reps", type=int, default=15)
    parser.add_argument("--latency", type=float, default=0.05)
    args = parser.parse_args()

    async def fetch(request):
        await asyncio.sleep(args.latency)
        return {"tel": {"x": [request]}}

    async def gather_wave(requests):
        semaphore = asyncio.Semaphore(22)

        async def one(request):
            async with semaphore:
                return await asyncio.gather(fetch(request))

        return await asyncio.gather(*(one(r) for r in requests))

    gather_times, stream_times = [], []
    for rep in range(args.reps):
        requests = [(rep, i) for i in range(args.requests)]
        t0 = time.perf_counter()
        asyncio.run(gather_wave(requests))
        gather_times.append((time.perf_counter() - t0) * 1000)

        stream = PriorityFetchStream(fetch, max_concurrent=22, background_concurrent=22)
        t0 = time.perf_counter()
        concurrent.futures.wait(stream.request((r, r) for r in requests))
        stream_times.append((time.perf_counter() - t0) * 1000)
        stream.close()
    ideal = -(-args.requests // 22) * args.latency * 1000
    print(
        f"ideal {ideal:.1f} ms  gather {statistics.median(gather_times):.1f} ms  "
        f"stream {statistics.median(stream_times):.1f} ms"
    )


if __name__ == "__main__":
    main()
