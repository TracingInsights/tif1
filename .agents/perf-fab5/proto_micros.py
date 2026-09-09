"""Offline micro-benchmarks for F5/F6/F8/F9: per-request fetch-path overheads.

Measures, without any network, the per-request costs these hypotheses target:
  F5: executor round-trips per request (get-hop + decode/parse-hop) vs one
      fused hop, measured as scheduling overhead on an echo workload.
  F6: http_session._track_request (lock + config.get + monotonic) per call,
      single- and multi-threaded contention.
  F8: function-level `from .x import y` on the hot path (sys.modules hit).
  F9: semaphore+gather vs bounded-worker pool for 1452 tiny coroutines.
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import tif1.http_session as hs

N = 1452


def bench(fn, n, repeat=5):
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn(n)
        best = min(best, time.perf_counter() - t0)
    return best


def f6_micro(n: int) -> None:
    """_track_request per-call cost, uncontended (single thread)."""
    for _ in range(n):
        hs._track_request(reused=True)


def f6_threaded(n: int) -> None:
    """_track_request under 22-thread contention."""
    with ThreadPoolExecutor(max_workers=22) as pool:
        list(
            pool.map(lambda _: [hs._track_request(reused=True) for _ in range(n // 22)], range(22))
        )


def f6_noop(n: int) -> None:
    for _ in range(n):
        pass


def f8_imports(n: int) -> None:
    """Three function-level imports per request (sys.modules lookups)."""
    for _ in range(n):
        from tif1.cdn import get_cdn_manager  # noqa: F401
        from tif1.http_session import _track_request  # noqa: F401
        from tif1.retry import get_circuit_breaker  # noqa: F401


def f8_noop(n: int) -> None:
    for _ in range(n):
        pass


def f5_roundtrips(n: int) -> None:
    """Two executor hops per request (echo payload) — scheduling cost only."""
    loop = asyncio.new_event_loop()
    executor = ThreadPoolExecutor(max_workers=64)
    try:

        async def one():
            r1 = await loop.run_in_executor(executor, lambda: "get")
            r2 = await loop.run_in_executor(executor, lambda: r1)
            return r2

        async def run():
            await asyncio.gather(*(one() for _ in range(n)))

        loop.run_until_complete(run())
    finally:
        loop.close()
        executor.shutdown(wait=True)


def f5_fused(n: int) -> None:
    """One fused executor hop per request."""
    loop = asyncio.new_event_loop()
    executor = ThreadPoolExecutor(max_workers=64)
    try:

        async def one():
            return await loop.run_in_executor(executor, lambda: "get" and "parse")

        async def run():
            await asyncio.gather(*(one() for _ in range(n)))

        loop.run_until_complete(run())
    finally:
        loop.close()
        executor.shutdown(wait=True)


def f9_semaphore(n: int) -> None:
    """Current shape: 1452 semaphore-wrapped coroutines + gather."""
    loop = asyncio.new_event_loop()
    try:
        sem = asyncio.Semaphore(22)

        async def one(i):
            async with sem:
                return i

        async def run():
            await asyncio.gather(*(one(i) for i in range(n)))

        loop.run_until_complete(run())
    finally:
        loop.close()


def f9_workers(n: int) -> None:
    """Bounded worker pool: 22 workers draining a queue of 1452."""
    loop = asyncio.new_event_loop()
    try:

        async def run():
            queue: asyncio.Queue = asyncio.Queue()
            for i in range(n):
                queue.put_nowait(i)
            stop = object()

            async def worker():
                while True:
                    item = await queue.get()
                    if item is stop:
                        return

            workers = [asyncio.ensure_future(worker()) for _ in range(22)]
            for _ in range(22):
                queue.put_nowait(stop)
            await asyncio.gather(*workers)

        loop.run_until_complete(run())
    finally:
        loop.close()


def f9_noop(n: int) -> None:
    loop = asyncio.new_event_loop()
    try:

        async def one(i):
            return i

        async def run():
            await asyncio.gather(*(one(i) for i in range(n)))

        loop.run_until_complete(run())
    finally:
        loop.close()


def main() -> None:
    print(f"N={N} per-request micros:")
    t = bench(f6_noop, N)
    t6 = bench(f6_micro, N)
    print(
        f"F6 _track_request: {(t6 - t) * 1e3:.2f} ms per {N} calls (loop baseline {(t) * 1e3:.2f} ms)"
    )
    t6t = bench(f6_threaded, N)
    print(f"F6 _track_request (22 threads): {(t6t) * 1e3:.2f} ms per {N} calls")
    t8 = bench(f8_imports, N)
    t8n = bench(f8_noop, N)
    print(f"F8 3x function-level imports: {(t8 - t8n) * 1e3:.2f} ms per {N} requests")
    t5a = bench(f5_roundtrips, N, repeat=3)
    t5b = bench(f5_fused, N, repeat=3)
    print(
        f"F5 two executor hops: {t5a * 1e3:.1f} ms | one fused hop: {t5b * 1e3:.1f} ms | delta {(t5a - t5b) * 1e3:.1f} ms"
    )
    t9a = bench(f9_semaphore, N, repeat=3)
    t9w = bench(f9_workers, N, repeat=3)
    t9n = bench(f9_noop, N, repeat=3)
    print(
        f"F9 semaphore+gather: {t9a * 1e3:.1f} ms | worker pool: {t9w * 1e3:.1f} ms | "
        f"bare gather: {t9n * 1e3:.1f} ms"
    )


if __name__ == "__main__":
    main()
