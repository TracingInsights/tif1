"""Interleaved CDN bake-off on the cold Monaco telemetry benchmark.

For each round, runs the dedicated cold benchmark once per CDN (as the sole
source via ``TIF1_CDNS``), interleaved so all CDNs see the same edge/network
conditions. All runs use the same source tree (``--src-dir``), fresh process
and throwaway cache dir per run (provided by the benchmark harness).

Prints per-CDN stats and per-round deltas vs the reference CDN, and writes a
JSON report.

Example:
    uv run python tools/monaco_cdn_bakeoff.py \
        --src-dir /tmp/tif1-pr63/src --rounds 5 \
        --out .agents/perf-monaco/cdn_bakeoff.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

BENCH = Path(__file__).resolve().parent / "monaco_telemetry_cold_benchmark.py"

CDNS = {
    "staticdelivr": "https://cdn.staticdelivr.com/gh/TracingInsights",
    "jsdelivr": "https://cdn.jsdelivr.net/gh/TracingInsights",
    "huggingface": "https://huggingface.co/buckets/tracinginsights",
}
MIN_COMPLETENESS = 0.9


def _spawn(src_dir: str, cdn_url: str, args: argparse.Namespace) -> dict:
    env = dict(os.environ)
    env["TIF1_CDNS"] = cdn_url
    cmd = [
        sys.executable,
        str(BENCH),
        "--src-dir",
        src_dir,
        "--year",
        str(args.year),
        "--gp",
        args.gp,
        "--session",
        args.session,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if proc.returncode != 0:
        raise RuntimeError(f"child failed rc={proc.returncode}: {proc.stderr[-1500:]}")
    run = json.loads(proc.stdout.strip().splitlines()[-1])
    run.pop("cache_dir", None)
    return run


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--src-dir", required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--reference", default="staticdelivr")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    records: dict[str, list[dict]] = {name: [] for name in CDNS}
    failures: list[dict] = []
    for rnd in range(1, args.rounds + 1):
        for name, url in CDNS.items():
            tries = 0
            while True:
                tries += 1
                try:
                    run = _spawn(args.src_dir, url, args)
                    if run["completeness"] < MIN_COMPLETENESS:
                        failures.append({"round": rnd, "cdn": name, "reason": "incomplete", **run})
                        print(
                            f"round {rnd} {name}: incomplete ({run['completeness']}), retry {tries}",
                            file=sys.stderr,
                        )
                    else:
                        records[name].append(run)
                        print(
                            f"round {rnd} {name}: total={run['total_s']}s "
                            f"telemetry={run['telemetry_s']}s n_tel={run['n_telemetry']}",
                            file=sys.stderr,
                        )
                        break
                except (RuntimeError, json.JSONDecodeError, KeyError) as e:
                    failures.append({"round": rnd, "cdn": name, "reason": str(e)})
                    print(f"round {rnd} {name}: FAILED ({e}), retry {tries}", file=sys.stderr)
                if tries >= 2:
                    print(f"round {rnd} {name}: giving up after {tries} tries", file=sys.stderr)
                    break

    def stats(name: str) -> dict:
        vals = [r["total_s"] for r in records[name]]
        tels = [r["telemetry_s"] for r in records[name]]
        if not vals:
            return {"n": 0}
        return {
            "n": len(vals),
            "total_s": {
                "mean": round(statistics.mean(vals), 3),
                "median": round(statistics.median(vals), 3),
                "min": round(min(vals), 3),
                "max": round(max(vals), 3),
                "values": vals,
            },
            "telemetry_s": {
                "median": round(statistics.median(tels), 3),
                "values": tels,
            },
        }

    ref = args.reference
    ref_vals = [r["total_s"] for r in records[ref]]
    deltas: dict[str, list] = {}
    for name in CDNS:
        if name == ref or not records[name] or not ref_vals:
            continue
        pairs = min(len(records[name]), len(ref_vals))
        deltas[name] = [round(records[name][i]["total_s"] - ref_vals[i], 3) for i in range(pairs)]

    out = {
        "timestamp": dt.datetime.now(dt.UTC).isoformat(),
        "src_dir": args.src_dir,
        "rounds": args.rounds,
        "cdns": {name: stats(name) for name in CDNS},
        "per_round_delta_vs_reference": {ref: 0.0, **deltas},
        "failures": failures,
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2) + "\n")
    print(f"wrote {out_path}")
    for name in CDNS:
        s = out["cdns"][name]
        if s.get("n"):
            print(
                f"{name:14} n={s['n']} median={s['total_s']['median']}s "
                f"min={s['total_s']['min']}s max={s['total_s']['max']}s"
            )
        else:
            print(f"{name:14} no valid runs")


if __name__ == "__main__":
    main()
