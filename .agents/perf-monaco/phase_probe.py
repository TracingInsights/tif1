"""Phase probe: time fetch_multiple_async vs post-processing inside the real path."""

import os
import resource
import tempfile
import time

os.environ.setdefault("TIF1_CACHE_DIR", tempfile.mkdtemp(prefix="tif1-phase-"))

import tif1
import tif1.async_fetch

orig = tif1.async_fetch.fetch_multiple_async
timings: list[float] = []


async def timed(*args, **kwargs):
    t0 = time.perf_counter()
    result = await orig(*args, **kwargs)
    timings.append(time.perf_counter() - t0)
    return result


tif1.async_fetch.fetch_multiple_async = timed


def rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def main() -> None:
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")

    t0 = time.perf_counter()
    laps = session.laps
    t_laps = time.perf_counter() - t0
    print(f"laps: {t_laps:.2f}s rows={len(laps)}")

    t0 = time.perf_counter()
    refs_done = 0
    tel = session.fetch_all_laps_telemetry()
    t_tel = time.perf_counter() - t0

    fetch_s = sum(timings)
    print(f"telemetry total: {t_tel:.2f}s")
    print(f"  fetch_multiple_async calls: {len(timings)} totaling {fetch_s:.2f}s")
    print(f"  post-processing (refs+frames): {t_tel - fetch_s:.2f}s")
    print(f"frames={len(tel)} rss={rss_mb():.0f}MB")


if __name__ == "__main__":
    main()
