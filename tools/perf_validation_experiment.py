"""Reproducible offline performance harness for the payload-validation experiment.

Measures two layers with zero network access:

- micro: ``tif1.async_fetch._validate_json_payload`` per payload type
  (drivers / session_laptimes / weather / rcm / telemetry), with the three
  validation toggles forced on.
- macro: the real async fetch pipeline (``fetch_multiple_async``) fed by an
  in-memory fake HTTP session, covering a realistic full-session payload set:
  drivers + session_laptimes + weather + rcm + one telemetry payload per
  driver-lap.

Usage:
    uv run python tools/perf_validation_experiment.py --label baseline --mode all
    uv run python tools/perf_validation_experiment.py --label no-pydantic --mode all

Emits a JSON object on stdout. Compare runs with ``jq``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from typing import Any

# Force validation on before tif1's config singleton initializes, so the
# macro run exercises the pydantic path even though shipped defaults are off.
# --validation-off removes these to reproduce the shipped default config.
os.environ.setdefault("TIF1_VALIDATE_DATA", "true")
os.environ.setdefault("TIF1_VALIDATE_LAP_TIMES", "true")
os.environ.setdefault("TIF1_VALIDATE_TELEMETRY", "true")

import orjson

DRIVER_CODES = [
    "VER",
    "HAM",
    "LEC",
    "NOR",
    "ALO",
    "SAI",
    "GAS",
    "OCO",
    "ALB",
    "TSU",
    "STR",
    "BOT",
    "ZHO",
    "MAG",
    "HUL",
    "PER",
    "RIC",
    "LAW",
    "PIA",
    "SAR",
]
DRIVER_NUMBERS = [
    "1",
    "44",
    "16",
    "4",
    "14",
    "55",
    "10",
    "31",
    "23",
    "22",
    "18",
    "77",
    "24",
    "20",
    "27",
    "11",
    "3",
    "30",
    "81",
    "2",
]
TEAMS = [
    "Red Bull Racing",
    "Mercedes",
    "Ferrari",
    "McLaren",
    "Aston Martin",
    "Williams",
    "Alpine",
    "AlphaTauri",
    "Alfa Romeo",
    "Haas",
] * 2

N_DRIVERS = 20
N_LAPS = 57
N_TEL_SAMPLES = 400
N_WEATHER = 130
N_RCM = 400


def build_drivers_payload() -> dict[str, Any]:
    return {
        "drivers": [
            {
                "driver": code,
                "team": TEAMS[i],
                "dn": DRIVER_NUMBERS[i],
                "fn": f"First{i}",
                "ln": f"Last{i}",
                "tc": "#12121z",
                "url": f"https://example.com/{code}",
            }
            for i, code in enumerate(DRIVER_CODES)
        ]
    }


def build_laptimes_payload() -> dict[str, Any]:
    rows = N_DRIVERS * N_LAPS
    idx = list(range(rows))
    compound_cycle = ["SOFT", "MEDIUM", "HARD"]
    status_cycle = ["Valid", "Valid", "Valid", "OUTLAP", "Valid", "INLAP"]

    def lap_no(i: int) -> int:
        return i % N_LAPS + 1

    def driver_idx(i: int) -> int:
        return i // N_LAPS

    return {
        "time": [90.0 + (i % 40) * 0.05 for i in idx],
        "lap": [lap_no(i) for i in idx],
        "compound": [compound_cycle[i % 3] for i in idx],
        "stint": [1 + lap_no(i) // 25 for i in idx],
        "s1": [30.0 + (i % 7) * 0.1 for i in idx],
        "s2": [32.0 + (i % 5) * 0.1 for i in idx],
        "s3": [28.0 + (i % 9) * 0.1 for i in idx],
        "life": [lap_no(i) % 25 for i in idx],
        "pos": [driver_idx(i) + 1 for i in idx],
        "status": [status_cycle[i % 6] for i in idx],
        "pb": [i % 4 == 0 for i in idx],
        "qs": [None] * rows,
        "sesT": [1000.0 + i * 95.0 for i in idx],
        "drv": [DRIVER_CODES[driver_idx(i)] for i in idx],
        "dNum": [DRIVER_NUMBERS[driver_idx(i)] for i in idx],
        "pout": [None if lap_no(i) % 25 else 500.0 + i for i in idx],
        "pin": [None if lap_no(i) % 25 != 24 else 900.0 + i for i in idx],
        "s1T": [1030.0 + i * 95.0 for i in idx],
        "s2T": [1062.0 + i * 95.0 for i in idx],
        "s3T": [1090.0 + i * 95.0 for i in idx],
        "vi1": [280.0 + (i % 20) for i in idx],
        "vi2": [290.0 + (i % 15) for i in idx],
        "vfl": [300.0 + (i % 25) for i in idx],
        "vst": [305.0 + (i % 10) for i in idx],
        "fresh": [i % 25 == 0 for i in idx],
        "team": [TEAMS[driver_idx(i)] for i in idx],
        "lST": [1000.0 + i * 95.0 for i in idx],
        "lSD": ["2025-03-01T10:00:00" for _ in idx],
        "del": [False for _ in idx],
        "delR": [None for _ in idx],
        "ff1G": [False for _ in idx],
        "iacc": [True for _ in idx],
        "wT": [1000.0 + i * 60.0 for i in idx],
        "wAT": [22.0 + (i % 10) * 0.1 for i in idx],
        "wH": [40.0 + (i % 20) * 0.2 for i in idx],
        "wP": [1010.0 + (i % 8) * 0.5 for i in idx],
        "wR": [False for _ in idx],
        "wTT": [30.0 + (i % 12) * 0.2 for i in idx],
        "wWD": [float(i % 360) for i in idx],
        "wWS": [1.5 + (i % 7) * 0.3 for i in idx],
    }


def build_tel_payload(drv: str, lap: int) -> dict[str, Any]:
    n = N_TEL_SAMPLES
    idx = list(range(n))
    return {
        "time": [i * 0.27 for i in idx],
        "speed": [150.0 + 90.0 * ((i % 37) / 37.0) for i in idx],
        "rpm": [6000.0 + 5000.0 * ((i % 41) / 41.0) for i in idx],
        "gear": [1 + (i % 8) for i in idx],
        "throttle": [float(i % 101) for i in idx],
        "brake": [i % 9 == 0 for i in idx],
        "drs": [i % 50 < 20 for i in idx],
        "distance": [i * 82.0 for i in idx],
        "rel_distance": [i / n for i in idx],
        "DriverAhead": [DRIVER_NUMBERS[(lap + i) % 20] if i % 5 else None for i in idx],
        "DistanceToDriverAhead": [float(i % 30) if i % 5 else None for i in idx],
        "x": [1000.0 + 300.0 * ((i % 53) / 53.0) for i in idx],
        "y": [500.0 + 200.0 * ((i % 47) / 47.0) for i in idx],
        "z": [10.0 + 5.0 * ((i % 19) / 19.0) for i in idx],
        "acc_x": [-8.0 + 16.0 * ((i % 29) / 29.0) for i in idx],
        "acc_y": [-12.0 + 24.0 * ((i % 23) / 23.0) for i in idx],
        "acc_z": [-2.0 + 4.0 * ((i % 17) / 17.0) for i in idx],
        "dataKey": [f"{drv}-{lap}" for _ in idx],
    }


def build_weather_payload() -> dict[str, Any]:
    idx = list(range(N_WEATHER))
    return {
        "wT": [600.0 + i * 60.0 for i in idx],
        "wAT": [21.5 + (i % 9) * 0.2 for i in idx],
        "wH": [42.0 + (i % 15) * 0.3 for i in idx],
        "wP": [1011.0 + (i % 6) * 0.4 for i in idx],
        "wR": [i % 40 == 0 for i in idx],
        "wTT": [29.0 + (i % 11) * 0.3 for i in idx],
        "wWD": [float(i % 360) for i in idx],
        "wWS": [2.0 + (i % 9) * 0.4 for i in idx],
    }


def build_rcm_payload() -> dict[str, Any]:
    idx = list(range(N_RCM))
    cats = ["Other", "Flag", "Drs", "CarEvent"]
    flags = ["GREEN", "YELLOW", "CLEAR", "CHEQUERED", None]
    scopes = ["Track", "Sector", "Driver"]
    return {
        "time": [60.0 + i * 30.0 for i in idx],
        "cat": [cats[i % 4] for i in idx],
        "msg": [f"Message {i}" for i in idx],
        "status": ["ENABLED" if i % 2 else "DISABLED" for i in idx],
        "flag": [flags[i % 5] for i in idx],
        "scope": [scopes[i % 3] for i in idx],
        "sector": [(i % 24) + 1 for i in idx],
        "dNum": [DRIVER_NUMBERS[i % 20] for i in idx],
        "lap": [(i % N_LAPS) + 1 for i in idx],
    }


def payload_set(tel_count: int) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {
        "drivers.json": build_drivers_payload(),
        "session_laptimes.json": build_laptimes_payload(),
        "weather.json": build_weather_payload(),
        "rcm.json": build_rcm_payload(),
    }
    for i in range(tel_count):
        drv = DRIVER_CODES[i % N_DRIVERS]
        lap = i // N_DRIVERS + 1
        payloads[f"{drv}/{lap}_tel.json"] = build_tel_payload(drv, lap)
    return payloads


def time_best(fn, reps: int) -> dict[str, float]:
    times: list[float] = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return {
        "best_ms": min(times) * 1000.0,
        "median_ms": statistics.median(times) * 1000.0,
        "all_ms": [round(t * 1000.0, 3) for t in times],
    }


def run_micro(reps: int) -> dict[str, Any]:
    from tif1.async_fetch import _validate_json_payload

    config_on = {"validate_data": True, "validate_lap_times": True, "validate_telemetry": True}
    config_off = {
        "validate_data": False,
        "validate_lap_times": False,
        "validate_telemetry": False,
    }

    tel = build_tel_payload("VER", 1)
    cases = [
        ("drivers.json", build_drivers_payload()),
        ("session_laptimes.json", build_laptimes_payload()),
        ("weather.json", build_weather_payload()),
        ("rcm.json", build_rcm_payload()),
        ("VER/1_tel.json", tel),
    ]

    results: dict[str, Any] = {
        "rows": len(build_laptimes_payload()["lap"]),
        "tel_samples": N_TEL_SAMPLES,
    }
    for path, payload in cases:
        results[path] = {
            "validation_on": time_best(
                lambda p=path, d=payload: _validate_json_payload(p, d, config_on), reps
            ),
        }
    # Dispatch overhead with toggles off (what default-config users pay today).
    results["dispatch_only_toggles_off"] = {
        path: time_best(lambda p=path, d=payload: _validate_json_payload(p, d, config_off), reps)
        for path, payload in cases
    }
    return results


class FakeResponse:
    status_code = 200

    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


class FakeHttpSession:
    """Duck-typed stand-in for the shared niquests session.

    ``latency_ms`` simulates network round-trip time by sleeping before
    responding (the sleep runs inside the executor thread, like a real
    blocking socket call would).
    """

    def __init__(self, blobs: dict[str, bytes], latency_ms: float = 0.0) -> None:
        self._blobs = blobs
        self._latency = latency_ms / 1000.0
        self.calls = 0

    def get(self, url: str, timeout: float | None = None) -> FakeResponse:  # noqa: ARG002
        if self._latency:
            time.sleep(self._latency)
        self.calls += 1
        for key, blob in self._blobs.items():
            if key in url:
                return FakeResponse(blob)
        raise FileNotFoundError(url)


def run_macro(
    reps: int,
    tel_count: int,
    *,
    cache: bool = False,
    latency_ms: float = 0.0,
    max_concurrent: int | None = None,
) -> dict[str, Any]:
    from tif1 import async_fetch
    from tif1.async_fetch import fetch_multiple_async

    payloads = payload_set(tel_count)
    blobs = {path: orjson.dumps(payload) for path, payload in payloads.items()}
    requests = [(2025, "Test GP", "Race", path) for path in payloads]

    cache_obj = None
    original_get_cache = async_fetch.get_cache
    if cache:
        import tempfile
        from pathlib import Path

        from tif1.cache import Cache

        cache_obj = Cache(Path(tempfile.mkdtemp(prefix="tif1-perf-")))
        async_fetch.get_cache = lambda: cache_obj

    async def run_once(*, use_cache: bool, write_cache: bool) -> list[dict[str, Any] | None]:
        async_fetch._async_session = FakeHttpSession(blobs, latency_ms=latency_ms)
        try:
            return await fetch_multiple_async(
                requests,
                use_cache=use_cache,
                write_cache=write_cache,
                validate_payload=True,
                max_concurrent_requests=max_concurrent,
            )
        finally:
            async_fetch._async_session = None
            async_fetch.cleanup_resources()

    def measure() -> dict[str, float]:
        # Warm-up once so lazy imports/executors are not attributed to the run.
        result = asyncio.run(run_once(use_cache=False, write_cache=False))
        assert len(result) == len(requests)
        assert all(r is not None for r in result), "a payload failed to fetch"
        return time_best(lambda: asyncio.run(run_once(use_cache=False, write_cache=False)), reps)

    try:
        if not cache:
            stats = measure()
            stats["payload_count"] = len(requests)
            stats["tel_payload_count"] = tel_count
            return stats

        # Cold: exactly one timed run against an empty cache (writes serialize).
        t0 = time.perf_counter()
        result = asyncio.run(run_once(use_cache=True, write_cache=True))
        cold_ms = (time.perf_counter() - t0) * 1000.0
        assert all(r is not None for r in result), "a payload failed to fetch"
        if cache_obj is not None and cache_obj.conn is not None:
            cache_obj.conn.commit()

        warm_memory = time_best(
            lambda: asyncio.run(run_once(use_cache=True, write_cache=True)), reps
        )

        # Warm disk: drop the in-memory tiers (blobs + parsed), keep SQLite
        # (fresh-process shape).
        cache_obj._memory_cache.clear()
        parsed = getattr(cache_obj, "_parsed_cache", None)
        if parsed is not None:
            parsed.clear()
        parsed_tel = getattr(cache_obj, "_parsed_telemetry_cache", None)
        if parsed_tel is not None:
            parsed_tel.clear()
        warm_disk = time_best(lambda: asyncio.run(run_once(use_cache=True, write_cache=True)), reps)

        return {
            "cold_write_ms": cold_ms,
            "warm_memory": warm_memory,
            "warm_disk": warm_disk,
            "payload_count": len(requests),
            "tel_payload_count": tel_count,
        }
    finally:
        async_fetch.get_cache = original_get_cache


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True, help="run label recorded in the output")
    parser.add_argument("--mode", choices=["micro", "macro", "all"], default="all")
    parser.add_argument("--reps", type=int, default=7, help="repetitions per measurement")
    parser.add_argument(
        "--tel-count",
        type=int,
        default=200,
        help="telemetry payloads in the macro run (0 disables)",
    )
    parser.add_argument(
        "--validation-off",
        action="store_true",
        help="drop the TIF1_VALIDATE_* env overrides (shipped default config)",
    )
    parser.add_argument(
        "--cache",
        action="store_true",
        help="macro mode: real SQLite cache; report cold-write/warm-memory/warm-disk",
    )
    parser.add_argument(
        "--latency-ms",
        type=float,
        default=0.0,
        help="simulated network round-trip per request in the macro run",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=None,
        help="max_concurrent_requests override for the macro run",
    )
    args = parser.parse_args()

    if args.validation_off:
        for key in ("TIF1_VALIDATE_DATA", "TIF1_VALIDATE_LAP_TIMES", "TIF1_VALIDATE_TELEMETRY"):
            os.environ.pop(key, None)

    out: dict[str, Any] = {
        "label": args.label,
        "python": sys.version.split()[0],
        "reps": args.reps,
    }
    if args.mode in ("micro", "all"):
        out["micro"] = run_micro(args.reps)
    if args.mode in ("macro", "all"):
        out["macro"] = run_macro(
            args.reps,
            args.tel_count,
            cache=args.cache,
            latency_ms=args.latency_ms,
            max_concurrent=args.max_concurrent,
        )

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
