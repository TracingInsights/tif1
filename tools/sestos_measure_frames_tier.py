"""L7 measurement: materialized-frame tier warm-load protocol + parity.

Steps (fresh process each):
  purge   — drop the telemetry_frames table rows (back to payload-only cache)
  warm1   — warm load that assembles from payloads and materializes frames
  warm2/3 — warm loads that should read the frames tier (fast path)
Parity: warm2 frames are compared against freshly assembled frames from the
payload tier (columns, dtypes, values via .equals on the full set).
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time

WARM_CACHE_DIR = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM_CACHE_DIR


def purge_frames() -> None:
    conn = sqlite3.connect(f"{WARM_CACHE_DIR}/cache.sqlite")
    try:
        conn.execute("DELETE FROM telemetry_frames")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # table not created yet
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["purge", "warm1", "warm2", "warm2-parity"])
    args = parser.parse_args()

    if args.mode == "purge":
        purge_frames()
        print('{"purged": true}')
        return

    import tif1

    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    _ = session.laps

    t0 = time.perf_counter()
    tel = session.fetch_all_laps_telemetry()
    t_tel = time.perf_counter() - t0

    result = {"mode": args.mode, "telemetry_s": round(t_tel, 3), "frames": len(tel)}

    if args.mode == "warm2-parity":
        # Reassemble every frame from the payload tier and compare.
        from tif1.cache import get_cache
        from tif1.core_utils.helpers import _create_telemetry_df

        cache = get_cache()
        refs = list(tel.keys())
        payloads = cache.get_telemetry_batch(2026, "Monaco%20Grand%20Prix", "Race", refs)
        mismatched = []
        for ref in refs:
            fresh = _create_telemetry_df(payloads.get(ref), ref[0], ref[1], "pandas")
            stored = tel[ref]
            if fresh is None or not fresh.equals(stored):
                mismatched.append(ref)
                continue
            if str(fresh.dtypes) != str(stored.dtypes):
                mismatched.append(ref)
        result["payload_refs"] = len(payloads)
        result["mismatched"] = len(mismatched)
        if mismatched:
            result["mismatched_refs"] = [str(r) for r in mismatched[:5]]

    print(result)


if __name__ == "__main__":
    main()
