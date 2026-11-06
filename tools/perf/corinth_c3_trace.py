"""Corinth C3: trace the requests of cold Ergast queries (one query per process).

Usage: python tools/perf/corinth_c3_trace.py <query> [--lib pandas|polars]

Patches ``NiquestsTransport.get_json`` (and the stdlib first-wave fetch) to log
each request's thread, start, end and outcome relative to the query start, then
prints the query wall time and the timeline. Run with an empty
``TIF1_CACHE_DIR`` for a cold trace.
"""

from __future__ import annotations

import sys
import threading
import time

T0 = time.perf_counter()

if "--pre-import" in sys.argv:
    import pandas as pd  # noqa: F401

import tif1  # noqa: E402,F401
from tif1 import payload_loader  # noqa: E402

LOG: list[tuple[str, float, float, str, str]] = []
_orig = payload_loader.NiquestsTransport.get_json


def _traced(self, url, *, timeout=None):
    start = time.perf_counter()
    outcome = "ok"
    try:
        return _orig(self, url, timeout=timeout)
    except Exception as exc:
        outcome = f"{type(exc).__name__}:{getattr(exc, 'context', {}).get('status_code', '')}"
        raise
    finally:
        LOG.append((threading.current_thread().name, start, time.perf_counter(), url, outcome))


payload_loader.NiquestsTransport.get_json = _traced  # type: ignore[method-assign]

from tif1 import first_wave  # noqa: E402

_orig_status = first_wave.fetch_status


def _traced_status(url, *, timeout):
    start = time.perf_counter()
    status = None
    try:
        status, body = _orig_status(url, timeout=timeout)
        return status, body
    finally:
        LOG.append(
            (threading.current_thread().name, start, time.perf_counter(), url, f"std:{status}")
        )


first_wave.fetch_status = _traced_status

QUERIES = {
    "race_results_2024": lambda e: e.get_race_results(season=2024),
    "driver_standings_2024": lambda e: e.get_driver_standings(season=2024),
    "qualifying_2023_r5": lambda e: e.get_qualifying_results(season=2023, round=5),
    "sprint_2024": lambda e: e.get_sprint_results(season=2024),
    "schedule_2024": lambda e: e.get_race_schedule(season=2024),
    "laps_2024_r1": lambda e: e.get_lap_times(season=2024, round=1),
    "pitstops_2024_r1": lambda e: e.get_pit_stops(season=2024, round=1),
    "driver_info_2024": lambda e: e.get_driver_info(season=2024),
    "constructor_standings_2025_r10": lambda e: e.get_constructor_standings(season=2025, round=10),
    "race_results_2026": lambda e: e.get_race_results(season=2026),
}


def main() -> None:
    query = sys.argv[1]
    lib = sys.argv[sys.argv.index("--lib") + 1] if "--lib" in sys.argv else "pandas"
    from tif1.ergast import Ergast

    e = Ergast(result_type="polars") if lib == "polars" else Ergast()
    q0 = time.perf_counter()
    QUERIES[query](e)
    q1 = time.perf_counter()
    print(f"import {1000 * (q0 - T0):.0f} ms, query {query} {1000 * (q1 - q0):.0f} ms")
    for name, start, end, url, outcome in sorted(LOG, key=lambda r: r[1]):
        print(
            f"  {1000 * (start - q0):7.1f} -> {1000 * (end - q0):7.1f} "
            f"({1000 * (end - start):6.1f}) {name[:14]:14} {outcome:10} {url}"
        )
    print(f"requests: {len(LOG)}")


if __name__ == "__main__":
    main()
