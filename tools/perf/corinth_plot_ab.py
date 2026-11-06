"""Corinth C6: interleaved fresh-process A/B of the plotting workloads.

A copy of ``delphi_warm_ab.py`` (same ABAB order flipped every pair, medians
and pairs won on the whole-process wall) that runs ``corinth_plot_bench.py``
instead of ``delphi_warm_bench.py``. With ``--digest-dir`` the first pair of
every workload also writes each arm's figure digest and compares them.

Usage:
    python tools/perf/corinth_plot_ab.py --control-src /tmp/corinth/control/src \
        --candidate-src /tmp/corinth/C6/src --cache /tmp/corinth/warm-C6 --pairs 8 \
        --workloads import_plotting,team_color,speed_trace [--out FILE]
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

BENCH = Path(__file__).resolve().parent / "corinth_plot_bench.py"


def _run_child(workload: str, src_dir: str, cache_dir: str, digest: str | None) -> dict:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", TIF1_CACHE_DIR=cache_dir, MPLBACKEND="Agg")
    env["PYTHONPATH"] = str(Path(src_dir).resolve())
    env.pop("C6_DIGEST", None)
    if digest:
        env["C6_DIGEST"] = digest
    cmd = [sys.executable, str(BENCH), "--child", workload]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} ({src_dir}) failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--workloads", default="import_plotting,team_color,speed_trace")
    parser.add_argument("--digest-dir", default=None)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    arms = {"control": args.control_src, "candidate": args.candidate_src}
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        recs: dict[str, list[dict]] = {"control": [], "candidate": []}
        for i in range(args.pairs):
            order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
            for arm in order:
                digest = None
                if args.digest_dir and i == 0:
                    digest = str(Path(args.digest_dir) / f"{workload}_{arm}.json")
                recs[arm].append(_run_child(workload, arms[arm], args.cache, digest))
        parity = None
        if args.digest_dir:
            a = json.loads((Path(args.digest_dir) / f"{workload}_control.json").read_text("utf-8"))
            b = json.loads(
                (Path(args.digest_dir) / f"{workload}_candidate.json").read_text("utf-8")
            )
            a.pop("modules", None)
            b.pop("modules", None)
            parity = "equal" if a == b else "DIFFERENT"
        phases: dict[str, dict] = {}
        for key in recs["control"][0]:
            if key not in recs["candidate"][0]:
                continue
            c = [r[key] for r in recs["control"]]
            d = [r[key] for r in recs["candidate"]]
            wins = sum(1 for x, y in zip(c, d, strict=True) if y < x)
            phases[key] = {
                "control_median": round(statistics.median(c), 1),
                "candidate_median": round(statistics.median(d), 1),
                "control_min": round(min(c), 1),
                "candidate_min": round(min(d), 1),
                "candidate_faster_pairs": f"{wins}/{args.pairs}",
            }
        summary[workload] = {"phases": phases, "parity": parity}
        w = phases["process_wall"]
        print(
            f"{workload:24s} wall control {w['control_median']:.0f}/{w['control_min']:.0f}"
            f"  candidate {w['candidate_median']:.0f}/{w['candidate_min']:.0f}"
            f"  faster {w['candidate_faster_pairs']}  parity={parity} | "
            + "  ".join(
                f"{k}={v['control_median']:.1f}->{v['candidate_median']:.1f}"
                f"({v['candidate_faster_pairs']})"
                for k, v in phases.items()
                if k not in ("process_wall",)
            ),
            flush=True,
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {
                    "pairs": args.pairs,
                    "control_src": args.control_src,
                    "candidate_src": args.candidate_src,
                    "summary": summary,
                },
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
