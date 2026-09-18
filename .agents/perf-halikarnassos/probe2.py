"""Probe validation cost, cache probes, Cache init, and laps breakdown."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

import orjson  # noqa: E402
import zstandard  # noqa: E402


def timed(fn, repeat: int = 3) -> float:
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best


def main() -> None:
    conn = sqlite3.connect("/tmp/perf-v1/cache.sqlite")
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=2026 AND gp='Monaco%20Grand%20Prix' AND session='Race'"
    ).fetchall()
    payloads = []
    for _, _, blob in rows:
        if isinstance(blob, bytes) and blob[:4] == b"\x28\xb5\x2f\xfd":
            blob = zstandard.ZstdDecompressor().decompress(blob)  # noqa: PLW2901
        payloads.append(orjson.loads(blob))
    n = len(payloads)
    print(f"payloads={n}")

    # 1. validation cost per payload
    from tif1.validation import validate_telemetry_data

    t_val = timed(
        lambda: [validate_telemetry_data(p, strict=False, normalize=False) for p in payloads]
    )
    print(f"validate_telemetry_data: {t_val * 1e6 / n:.0f}us/payload total={t_val * 1e3:.0f}ms")

    # sanitize cost (runs after validation)
    def sanitize(payload):
        sanitized = {}
        for key, value in payload.items():
            if key == "tel":
                continue
            if isinstance(value, list) and not value:
                continue
            sanitized[key] = value
        return sanitized

    t_san = timed(lambda: [sanitize(p) for p in payloads])
    print(f"sanitize: {t_san * 1e6 / n:.0f}us/payload total={t_san * 1e3:.0f}ms")

    # 2. per-request cache probe cost on an EMPTY cache (cold path)
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        os.environ["TIF1_CACHE_DIR"] = td
        from tif1.cache import get_cache

        t0 = time.perf_counter()
        cache = get_cache()
        t_init = time.perf_counter() - t0
        print(f"Cache init: {t_init * 1e3:.0f}ms")

        def probe_all():
            for d, lap, _ in rows:
                cache.get_telemetry(2026, "Monaco Grand Prix", "Race", d, lap)

        t_probe = timed(probe_all)
        print(
            f"empty-cache get_telemetry probe: {t_probe * 1e6 / n:.0f}us/req total={t_probe * 1e3:.0f}ms"
        )

        # 3. frame-tier batch read loop overhead (with work stubbed out)
        frame_rows = conn.execute(
            "SELECT driver, lap, frame FROM telemetry_frames WHERE year=2026 AND gp='Monaco%20Grand%20Prix' AND session='Race'"
        ).fetchall()

        def loop_only():
            out = {}
            for d, lap, blob in frame_rows:
                out[(d, lap)] = blob
            return out

        t_loop = timed(loop_only)
        print(f"frames fetchall+loop-only: {t_loop * 1e3:.0f}ms for {len(frame_rows)}")

    # 4. import tree for tif1
    os.environ.pop("TIF1_CACHE_DIR", None)


if __name__ == "__main__":
    main()
