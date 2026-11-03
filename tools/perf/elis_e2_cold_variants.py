"""Elis E2: cold (empty cache, live CDN) runs of control and several E2 variants, interleaved.

Each rep runs every arm once in a rotated order (so link drift spreads over
all arms) through ``delphi_cold_bench.py --child``. The candidate arms share
the worktree and select their variant through the ``.e2_variant`` file that a
measurement-only switch in ``_preconnect_cdn`` read (``a``: stdlib wave plus
the niquests import and warm-up in a thread; ``lazy``: stdlib wave, niquests
on demand, the shipped form; ``b``: niquests import and warm-up in a thread,
no stdlib wave; ``control``). The switch is not in the committed tree: put it
back to re-run the comparison. Run from the repository root:

    python tools/perf/elis_e2_cold_variants.py --lib pandas --reps 7 \
        --variants a,lazy --workloads laps,fastest_lap_tel,driver_tel --out FILE
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

BENCH = "/home/harish/Documents/tif1/tools/perf/delphi_cold_bench.py"
CONTROL_SRC = "/tmp/tif1-elis-control/src"
CANDIDATE_SRC = str(Path(__file__).resolve().parents[2] / "src")
VARIANT_FILE = Path(__file__).resolve().parents[2] / ".e2_variant"


EXTRA_ENV: dict[str, str] = {}


def run_child(workload: str, lib: str, src: str) -> dict:
    cache_dir = tempfile.mkdtemp(prefix="tif1-e2-cold-")
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir, PYTHONPATH=src, **EXTRA_ENV)
    env.pop("TIF1_OFFLINE_MODE", None)
    cmd = [sys.executable, BENCH, "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    shutil.rmtree(cache_dir, ignore_errors=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--reps", type=int, default=7)
    parser.add_argument("--variants", default="a,lazy")
    parser.add_argument("--workloads", default="laps,fastest_lap_tel,driver_tel")
    parser.add_argument("--out", default=None)
    parser.add_argument("--env", action="append", default=[], help="KEY=VALUE for every arm")
    args = parser.parse_args()
    EXTRA_ENV.update(item.split("=", 1) for item in args.env)
    arms = ["control", *args.variants.split(",")]
    summary: dict[str, dict] = {}
    try:
        for workload in args.workloads.split(","):
            recs: dict[str, list[dict]] = {arm: [] for arm in arms}
            for rep in range(args.reps):
                order = arms[rep % len(arms) :] + arms[: rep % len(arms)]
                for arm in order:
                    if arm == "control":
                        src = CONTROL_SRC
                    else:
                        VARIANT_FILE.write_text(arm + "\n", encoding="utf-8")
                        src = CANDIDATE_SRC
                    recs[arm].append(run_child(workload, args.lib, src))
            walls = {arm: [r["process_wall"] for r in recs[arm]] for arm in arms}
            entry: dict[str, dict] = {}
            line = [f"[{args.lib}] {workload:16s}"]
            for arm in arms:
                med = statistics.median(walls[arm])
                wins = sum(1 for c, v in zip(walls["control"], walls[arm], strict=True) if v < c)
                entry[arm] = {
                    "median": round(med, 1),
                    "min": round(min(walls[arm]), 1),
                    "walls": walls[arm],
                    "phases": {
                        k: round(statistics.median(r[k] for r in recs[arm]), 1)
                        for k in recs[arm][0]
                    },
                    "faster_than_control": f"{wins}/{args.reps}",
                }
                line.append(
                    f"{arm} {med:.0f}" + ("" if arm == "control" else f" ({wins}/{args.reps})")
                )
            summary[workload] = entry
            print("  ".join(line), flush=True)
    finally:
        VARIANT_FILE.unlink(missing_ok=True)
    if args.out:
        Path(args.out).write_text(
            json.dumps({"lib": args.lib, "reps": args.reps, "summary": summary}, indent=1) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
