"""Naxos N8: interleaved traced cold runs over several arms (src + env per arm).

Usage:
    python tools/perf/naxos_n8_arms.py --lib pandas --reps 6 --workload driver_tel \
        --arm "c22=/tmp/naxos/control/src" \
        --arm "c12=/tmp/naxos/control/src:TIF1_MAX_CONCURRENT_REQUESTS=12" \
        --out FILE

Prints per-run summaries, then per arm: median and p90 of the whole-process
wall and of every phase, and pairs won against the first arm.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import naxos_n8_trace as trace


def _p90(xs: list[float]) -> float:
    xs = sorted(xs)
    if len(xs) == 1:
        return xs[0]
    k = 0.9 * (len(xs) - 1)
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--reps", type=int, default=6)
    parser.add_argument("--workload", default="driver_tel")
    parser.add_argument("--arm", action="append", required=True)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    arms = []
    for spec in args.arm:
        label, rest = spec.split("=", 1)
        src, *envs = rest.split(":")
        env = dict(item.split("=", 1) for env_s in envs for item in env_s.split(",") if item)
        arms.append((label, src, env))
    recs: dict[str, list[dict]] = {label: [] for label, _, _ in arms}
    for i in range(args.reps):
        order = arms if i % 2 == 0 else arms[::-1]
        for label, src, env in order:
            rec = trace._run_child(args.workload, args.lib, src, env)
            recs[label].append(rec)
            print(f"[{args.lib} {args.workload} {label} {i}] {trace.summarize(rec)}", flush=True)
    base = arms[0][0]
    summary = {}
    for label, _, _ in arms:
        rows = recs[label]
        keys = ["process_wall", *rows[0]["phases"].keys()]
        stats = {}
        for key in keys:
            vals = [r["process_wall"] if key == "process_wall" else r["phases"][key] for r in rows]
            stats[key] = {
                "median": round(statistics.median(vals), 1),
                "p90": round(_p90(vals), 1),
                "min": round(min(vals), 1),
            }
        wins = sum(
            1
            for a, b in zip(recs[base], rows, strict=True)
            if b["process_wall"] < a["process_wall"]
        )
        summary[label] = {"stats": stats, "faster_than_first": f"{wins}/{len(rows)}"}
        w = stats["process_wall"]
        ph = stats.get(args.workload, stats["process_wall"])
        print(
            f"{label:10s} wall med {w['median']:.0f} p90 {w['p90']:.0f} | {args.workload} med "
            f"{ph['median']:.0f} p90 {ph['p90']:.0f} | faster than {base}: {wins}/{len(rows)}"
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps({"lib": args.lib, "summary": summary, "runs": recs}, default=str) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
