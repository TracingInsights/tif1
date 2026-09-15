"""L10 probe: where does get_session() spend its time on a warm cache?

Splits a fresh-process get_session(2026, Monaco, Race) into schedule payload
load (vendored parse + validate), event resolution, and Session construction.
"""

from __future__ import annotations

import os
import time

WARM_CACHE_DIR = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM_CACHE_DIR

t0 = time.perf_counter()
import tif1  # noqa: E402

t_import = time.perf_counter() - t0

from tif1 import events as ev  # noqa: E402


def timed(label: str, fn):
    t = time.perf_counter()
    out = fn()
    print(f"{label}: {time.perf_counter() - t:.4f}s")
    return out


def main() -> None:
    print(f"import tif1: {t_import:.4f}s")

    payload = timed("events._load_schedule_payload (vendored parse+validate)", ev._load_schedule_payload)
    print(f"  years in payload: {len(payload['years'])}")

    timed("events._load_schedule_payload (cached 2nd call)", ev._load_schedule_payload)

    t = time.perf_counter()
    tif1.get_session(2026, "Monaco Grand Prix", "Race")
    print(f"get_session (full, incl. above): {time.perf_counter() - t:.4f}s")

    # second get_session in-process (lru caches warm)
    t = time.perf_counter()
    tif1.get_session(2026, "Monaco Grand Prix", "Race")
    print(f"get_session (2nd, in-process): {time.perf_counter() - t:.4f}s")


if __name__ == "__main__":
    main()
