"""Corinth C2: trace the requests of a cold Ergast-only session load.

Usage (fresh process, empty ``TIF1_CACHE_DIR``)::

    python tools/perf/corinth_c2_trace.py 1998 Monaco R pandas

Prints every HTTP request (start/end ms from process start, thread, status,
bytes) and the phase times of ``get_session``, ``laps``, ``results``.
"""

from __future__ import annotations

import sys
import threading
import time

T0 = time.perf_counter()
LOG: list[tuple[float, float, str, str, str]] = []


def _ms() -> float:
    return (time.perf_counter() - T0) * 1000


def _wrap(cls: type, name: str) -> None:
    original = getattr(cls, name)

    def wrapper(self, url, *args, **kwargs):
        start = _ms()
        status = "ok"
        try:
            return original(self, url, *args, **kwargs)
        except Exception as exc:
            status = type(exc).__name__
            raise
        finally:
            LOG.append((start, _ms(), threading.current_thread().name, status, url))

    setattr(cls, name, wrapper)


def main() -> None:
    year, gp, session, lib = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    import tif1
    from tif1 import first_wave, payload_loader

    _wrap(payload_loader.NiquestsTransport, "get_json")
    _wrap(payload_loader.NiquestsTransport, "get_bytes")
    original_fetch = first_wave.fetch

    def traced_fetch(url, *args, **kwargs):
        start = _ms()
        try:
            return original_fetch(url, *args, **kwargs)
        finally:
            LOG.append((start, _ms(), threading.current_thread().name, "stdlib", url))

    first_wave.fetch = traced_fetch
    phases = {}
    t = time.perf_counter()
    s = tif1.get_session(int(year), gp, session, lib=lib)
    phases["get_session"] = (time.perf_counter() - t) * 1000
    t = time.perf_counter()
    laps = s.laps
    phases["laps"] = (time.perf_counter() - t) * 1000
    t = time.perf_counter()
    results = s.results
    phases["results"] = (time.perf_counter() - t) * 1000
    t = time.perf_counter()
    drivers = s.drivers
    phases["drivers"] = (time.perf_counter() - t) * 1000
    print({k: round(v) for k, v in phases.items()}, len(laps), len(results), len(drivers))
    for start, end, thread, status, url in sorted(LOG):
        print(f"{start:8.0f} {end:8.0f} {end - start:6.0f} {thread[:18]:18s} {status:10s} {url}")


if __name__ == "__main__":
    main()
