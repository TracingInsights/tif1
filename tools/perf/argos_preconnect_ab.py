"""A8: interleaved A/B of the CDN pre-connect hook on the cold ``laps`` phase.

Each run is a fresh process with a throwaway cache: ``get_session`` then
``session.laps`` (drivers.json + laptimes wave over the live CDN). The
candidate (``TIF1_CDN_PRECONNECT=true``) warms the shared HTTP session in a
daemon thread during ``get_session``; the control disables it. Reports
``get_session_s``, ``laps_s`` and their sum per run and the medians per arm.

Usage:
    uv run python tools/perf/argos_preconnect_ab.py --rounds 6 --out .agents/perf-argos/a8_preconnect.json
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def single(args: argparse.Namespace) -> None:
    os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-a8-")
    os.environ["TIF1_CDN_PRECONNECT"] = "true" if args.variant == "on" else "false"
    import tif1

    t0 = time.perf_counter()
    session = tif1.get_session(args.year, args.gp, args.session)
    t_get = time.perf_counter() - t0
    t0 = time.perf_counter()
    laps = session.laps
    t_laps = time.perf_counter() - t0
    print(
        json.dumps(
            {
                "variant": args.variant,
                "get_session_s": round(t_get, 4),
                "laps_s": round(t_laps, 4),
                "sum_s": round(t_get + t_laps, 4),
                "rows": len(laps),
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--rounds", type=int, default=0)
    parser.add_argument("--variant", default="on")
    parser.add_argument("--out", default=".agents/perf-argos/a8_preconnect.json")
    args = parser.parse_args()
    if args.rounds == 0:
        single(args)
        return
    records = []
    for rnd in range(args.rounds):
        for variant in ("off", "on") if rnd % 2 == 0 else ("on", "off"):
            cmd = [sys.executable, __file__, "--variant", variant]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            line = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
            rec = (
                json.loads(line)
                if line.startswith("{")
                else {"variant": variant, "error": proc.stderr[-400:]}
            )
            rec["round"] = rnd
            records.append(rec)
            print(rec, file=sys.stderr)
    summary = {}
    for variant in ("off", "on"):
        for key in ("get_session_s", "laps_s", "sum_s"):
            vals = [r[key] for r in records if r.get("variant") == variant and key in r]
            summary[f"{variant}_{key}_median"] = statistics.median(vals) if vals else None
    Path(args.out).write_text(
        json.dumps({"records": records, "summary": summary}, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
