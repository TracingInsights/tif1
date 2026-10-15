"""Interleaved fresh-process A/B of the fastest-lap entry points on a warm cache (Q10).

For every pair and entry point, runs ``tools/q10_fastest_probe.py`` once against
the control tree and once against the candidate tree (order alternates per
pair), both on the SAME cache directory, and reports medians, minimums and
per-pair win counts plus asyncio / result-digest agreement.

Usage:
    uv run python tools/q10_fastest_ab.py --control-src /tmp/tif1-pergamon-control/src \
        --cache-dir /tmp/tif1-q-Q10/cache --pairs 8 [--out results.json]
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

PROBE = Path(__file__).with_name("q10_fastest_probe.py")
DEFAULT_ENTRIES = (
    "session_fastest_lap_tel",
    "session_fastest_laps_tels",
    "session_fastest_laps_tels_3drv",
    "session_fastest_laps_tels_overall",
    "driver_fastest_lap_tel",
    "driver_fastest_lap",
    "laps_pick_fastest_tel",
)


def _run(src: str, entry: str, cache_dir: str) -> dict:
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir, TIF1_OFFLINE_MODE="1")
    out = subprocess.run(
        [sys.executable, str(PROBE), "--src-dir", src, "--entry", entry, "--no-instrument"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    ).stdout
    return json.loads(out.strip().splitlines()[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", default=str(Path(__file__).parents[1] / "src"))
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--entries", default=",".join(DEFAULT_ENTRIES))
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    entries = args.entries.split(",")
    runs: dict[str, dict[str, list[dict]]] = {e: {"control": [], "candidate": []} for e in entries}
    arms = {"control": args.control_src, "candidate": args.candidate_src}
    for pair in range(args.pairs):
        order = ("control", "candidate") if pair % 2 == 0 else ("candidate", "control")
        for entry in entries:
            for arm in order:
                runs[entry][arm].append(_run(arms[arm], entry, args.cache_dir))
    summary = {}
    for entry in entries:
        ctl = [r["wall_ms"] for r in runs[entry]["control"]]
        cand = [r["wall_ms"] for r in runs[entry]["candidate"]]
        summary[entry] = {
            "control_median_ms": round(statistics.median(ctl), 2),
            "control_min_ms": round(min(ctl), 2),
            "candidate_median_ms": round(statistics.median(cand), 2),
            "candidate_min_ms": round(min(cand), 2),
            "candidate_faster": f"{sum(b < a for a, b in zip(ctl, cand))}/{len(ctl)}",
            "control_asyncio": sorted({r["asyncio_after"] for r in runs[entry]["control"]}),
            "candidate_asyncio": sorted({r["asyncio_after"] for r in runs[entry]["candidate"]}),
            "digest_equal": {r["digest"] for r in runs[entry]["control"]}
            == {r["digest"] for r in runs[entry]["candidate"]},
        }
        print(entry, json.dumps(summary[entry]), flush=True)
    if args.out:
        Path(args.out).write_text(json.dumps({"summary": summary, "runs": runs}, indent=1))


if __name__ == "__main__":
    main()
