"""Profile the warm-cache telemetry load phase (sestos-series recon).

Loads the Monaco race from the pre-warmed persistent cache and splits the
telemetry phase into: cache batch read, per-frame assembly, network tail
(missing-everywhere files), plus a cProfile top-N for the whole phase.
"""

from __future__ import annotations

import cProfile
import io
import os
import pstats
import time

WARM_CACHE_DIR = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM_CACHE_DIR

import tif1  # noqa: E402
from tif1.cache import get_cache  # noqa: E402
from tif1.core_utils.helpers import _create_telemetry_df  # noqa: E402


def main() -> None:
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    _ = session.laps

    cache = get_cache()
    refs: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    laps = session.laps
    for driver, lap in zip(laps["Driver"].tolist(), laps["LapNumber"].tolist()):
        try:
            ref = (str(driver), int(lap))
        except (TypeError, ValueError):
            continue
        if ref in seen:
            continue
        seen.add(ref)
        refs.append(ref)

    # Phase A: batch cache read (SQL + zstd + orjson)
    t0 = time.perf_counter()
    cached = cache.get_telemetry_batch(2026, "Monaco Grand Prix", "Race", refs)
    t_read = time.perf_counter() - t0
    print(f"batch_read_s={t_read:.3f} hits={len(cached)}/{len(refs)}")

    # Phase B: per-frame assembly of cached payloads
    t0 = time.perf_counter()
    n_frames = 0
    for (driver, lap), payload in cached.items():
        frame = _create_telemetry_df(payload, driver, lap, "pandas")
        if frame is not None:
            n_frames += 1
    t_asm = time.perf_counter() - t0
    print(f"assembly_s={t_asm:.3f} frames={n_frames}")

    # Phase C: missing refs — how many must go to network, and what does the
    # missing-file walk cost?
    missing = [r for r in refs if r not in cached]
    print(f"missing_refs={len(missing)}")

    # Full telemetry phase via the public API, with cProfile
    prof = cProfile.Profile()
    t0 = time.perf_counter()
    prof.enable()
    tel = session.fetch_all_laps_telemetry()
    prof.disable()
    t_full = time.perf_counter() - t0
    print(f"fetch_all_laps_telemetry_s={t_full:.3f} frames={len(tel)}")

    out = io.StringIO()
    stats = pstats.Stats(prof, stream=out)
    stats.sort_stats("cumulative").print_stats(28)
    print(out.getvalue())


if __name__ == "__main__":
    main()
