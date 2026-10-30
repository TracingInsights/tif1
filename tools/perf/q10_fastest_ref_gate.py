"""Q10 parity gate: fastest-lap references from the laps frame tier vs the payload scan.

For every session of a cache copy that holds both a pandas laps frame and the
session laptime payload, compares the per-driver fastest ``(driver, lap)``
candidates (and the overall pick) chosen from the laps frame tier against the
raw-payload scan that cold sessions use. Run it on a COPY of a cache
(offline mode, nothing is fetched).

Usage:
    TIF1_CACHE_DIR=/tmp/copy TIF1_OFFLINE_MODE=1 uv run python tools/perf/q10_fastest_ref_gate.py
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from urllib.parse import unquote

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from tif1 import core
from tif1.cache import get_cache


def main() -> None:
    cache = get_cache()
    rows = cache.conn.execute("SELECT year, gp, session FROM laps_frames").fetchall()
    report = {"sessions": 0, "drivers": 0, "mismatches": [], "ties": 0, "skipped": []}
    for year, gp, session_name in rows:
        key = f"{year}/{gp}/{session_name}/session_laptimes.json"
        if cache.get(key) is None:
            report["skipped"].append(key)
            continue
        session = core.Session(year, unquote(gp), unquote(session_name), True, "pandas")
        session._memo.has_session_data = True
        requests = session._build_driver_laptime_requests()
        tier = session._fastest_lap_candidates_from_laps_tier(requests)
        if tier is None:
            report["skipped"].append(f"{key} (no usable tier frame)")
            continue
        payloads, _ = session._fetch_laptime_payloads(requests, operation="q10_gate")
        raw = []
        for (driver_info, _path), payload in zip(requests, payloads, strict=True):
            candidate = session._extract_fastest_lap_candidate(driver_info["driver"], payload)
            if candidate is not None:
                raw.append(candidate)
        report["sessions"] += 1
        report["drivers"] += len(raw)
        same = [(d, lap) for d, lap, _ in tier] == [(d, lap) for d, lap, _ in raw] and all(
            math.isclose(a[2], b[2], abs_tol=1e-6) for a, b in zip(tier, raw, strict=True)
        )
        overall_raw = min(raw, key=lambda c: c[2])[:2] if raw else None
        overall_tier = min(tier, key=lambda c: c[2])[:2] if tier else None
        sorted_raw = [c[:2] for c in sorted(raw, key=lambda c: c[2])]
        sorted_tier = [c[:2] for c in sorted(tier, key=lambda c: c[2])]
        times = [c[2] for c in raw]
        report["ties"] += len(times) - len(set(times))
        if not same or overall_raw != overall_tier or sorted_raw != sorted_tier:
            report["mismatches"].append(
                {"session": key, "raw": raw, "tier": tier, "overall": [overall_raw, overall_tier]}
            )
    print(json.dumps(report, indent=1, default=str))


if __name__ == "__main__":
    main()
