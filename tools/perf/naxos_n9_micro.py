"""Naxos N9: cost of the post-wave work of a cold ``fetch_all_laps_telemetry()``.

Reads the 1128 race telemetry payloads from a warm cache's payload tier
(native-decoded, as the cold wave hands them over) and times, serially and
in-process:

- the payload-tier flush pieces: ``json_dumps_bytes``, ``_encode_sqlite_value``
  and the whole ``_flush_telemetry_writes`` into an empty cache;
- the frame build of the cold path (``Session._frames_from_payloads``);
- the frame-tier write (``set_telemetry_frames_batch``) into an empty cache.

Usage (from the repo root; offline, warm cache)::

    TIF1_CACHE_DIR=/tmp/naxos/warm-N9 TIF1_OFFLINE_MODE=true \
        python tools/perf/naxos_n9_micro.py --lib pandas
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
import time
from pathlib import Path


def main() -> None:
    """Time each piece and print one line per piece."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    ap.add_argument("--skip", default="", help="comma list of pieces to skip")
    args = ap.parse_args()
    skip = set(args.skip.split(","))

    import tif1
    from tif1 import async_fetch
    from tif1 import cache as cache_mod
    from tif1.core_utils.json_utils import json_dumps_bytes

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=args.lib)
    laps = session.laps
    refs = session._telemetry_lap_refs(laps)
    warm = cache_mod.get_cache()
    t0 = time.perf_counter()
    payloads = warm.get_telemetry_batch(session.year, session.gp, session.session, refs)
    print(f"read {len(payloads)} payloads {1000 * (time.perf_counter() - t0):.0f} ms")
    cached = [(d, n, payloads[(d, n)]) for d, n in refs if (d, n) in payloads]

    if "dumps" not in skip:
        t0 = time.perf_counter()
        blobs = [json_dumps_bytes(p) for _, _, p in cached]
        t1 = time.perf_counter()
        comp = [cache_mod._encode_sqlite_value(b) for b in blobs]
        t2 = time.perf_counter()
        print(
            f"json_dumps_bytes {1000 * (t1 - t0):.0f} ms ({sum(map(len, blobs)) / 1e6:.1f} MB); "
            f"compress {1000 * (t2 - t1):.0f} ms ({sum(map(len, comp)) / 1e6:.1f} MB)"
        )

    tmp = Path(tempfile.mkdtemp(prefix="n9-micro-"))
    try:
        fresh = cache_mod.Cache(cache_dir=tmp)
        entries = [(session.year, session.gp, session.session, d, n, p) for d, n, p in cached]
        if "flush" not in skip:
            t0 = time.perf_counter()
            async_fetch._flush_telemetry_writes(fresh, entries)
            fresh._commit_if_needed(force=True)
            print(
                f"flush (dumps+compress+insert+commit) {1000 * (time.perf_counter() - t0):.0f} ms"
            )

        t0 = time.perf_counter()
        frames = session._frames_from_payloads(cached, {})
        print(f"frame build {len(frames)} frames {1000 * (time.perf_counter() - t0):.0f} ms")

        if "frames" not in skip:
            t0 = time.perf_counter()
            fresh.set_telemetry_frames_batch(
                session.year, session.gp, session.session, frames, lib=args.lib
            )
            fresh._commit_if_needed(force=True)
            print(f"frame-tier write {1000 * (time.perf_counter() - t0):.0f} ms")
        fresh.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
