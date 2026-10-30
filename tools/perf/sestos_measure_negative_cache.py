"""L2 measurement: missing-file network walk on warm loads (deterministic).

Fresh process per run, primed persistent cache. On a fully warm load the
ONLY network requests are the everywhere-missing telemetry files. Counts
HTTP GETs (shared-session request counter) and times the telemetry phase.

Modes:
  --no-verdicts : purge recorded verdicts first (control behavior)
  --verdicts    : ensure verdicts are recorded first (candidate behavior)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

WARM_CACHE_DIR = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM_CACHE_DIR

if len(sys.argv) > 1 and sys.argv[1].startswith("/"):
    sys.path.insert(0, str(Path(sys.argv[1]).resolve() / "src"))
    sys.argv = [sys.argv[0], *sys.argv[2:]]

import tif1  # noqa: E402
from tif1.http_session import get_connection_stats  # noqa: E402


def purge_verdicts() -> None:
    import sqlite3

    conn = sqlite3.connect(f"{WARM_CACHE_DIR}/cache.sqlite")
    try:
        conn.execute("DELETE FROM missing_payloads")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # table absent on control code
    conn.close()


def ensure_verdicts() -> None:
    """One warm load records the all-4xx verdicts for the missing files."""
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    _ = session.laps
    _ = session.fetch_all_laps_telemetry()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["no-verdicts", "verdicts"], required=True)
    args = parser.parse_args()

    if args.mode == "no-verdicts":
        purge_verdicts()
    else:
        purge_verdicts()
        ensure_verdicts()

    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    _ = session.laps

    before = get_connection_stats()["total_requests"]
    t0 = time.perf_counter()
    tel = session.fetch_all_laps_telemetry()
    t_tel = time.perf_counter() - t0
    after = get_connection_stats()["total_requests"]

    print(
        f'{{"mode": "{args.mode}", "telemetry_s": {round(t_tel, 3)}, '
        f'"http_gets": {after - before}, "frames": {len(tel)}}}'
    )


if __name__ == "__main__":
    main()
