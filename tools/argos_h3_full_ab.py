"""A7 (full session): interleaved cold-benchmark runs with HTTP/3 on vs off.

Alternates ``tools/monaco_telemetry_cold_benchmark.py`` single runs (fresh
process + throwaway cache each) with ``TIF1_HTTP_DISABLE_HTTP3`` unset (h3)
and ``true`` (h2 only); every other round flips the order so edge drift hits
both arms. Writes per-run records and per-arm medians.

Usage:
    uv run python tools/argos_h3_full_ab.py --rounds 3 --out .agents/perf-argos/a7_full_ab.json
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

BENCH = Path(__file__).resolve().parent / "monaco_telemetry_cold_benchmark.py"


def _run(variant: str, args: argparse.Namespace) -> dict:
    env = dict(os.environ)
    env.pop("TIF1_HTTP_DISABLE_HTTP3", None)
    if variant == "h2":
        env["TIF1_HTTP_DISABLE_HTTP3"] = "true"
    cmd = [
        sys.executable,
        str(BENCH),
        "--year",
        str(args.year),
        "--gp",
        args.gp,
        "--session",
        args.session,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    line = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
    rec = json.loads(line) if line.startswith("{") else {"error": proc.stderr[-500:]}
    rec["variant"] = variant
    rec["warnings_403"] = proc.stderr.count("403 Client Error")
    rec["timeouts"] = proc.stderr.count("timed out")
    cache_dir = rec.pop("cache_dir", None)
    if cache_dir:
        import shutil

        shutil.rmtree(cache_dir, ignore_errors=True)
    return rec


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--out", default=".agents/perf-argos/a7_full_ab.json")
    args = parser.parse_args()
    records = []
    for rnd in range(args.rounds):
        for variant in ("h3", "h2") if rnd % 2 == 0 else ("h2", "h3"):
            rec = _run(variant, args)
            rec["round"] = rnd
            records.append(rec)
            print(json.dumps(rec), file=sys.stderr)
    summary = {}
    for variant in ("h3", "h2"):
        for key in ("total_s", "telemetry_s"):
            vals = [r[key] for r in records if r.get("variant") == variant and key in r]
            summary[f"{variant}_{key}_median"] = statistics.median(vals) if vals else None
    Path(args.out).write_text(json.dumps({"records": records, "summary": summary}, indent=2))
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
