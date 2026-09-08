"""Interleaved A/B suite on the cold Monaco telemetry benchmark.

Alternates runs of ``tools/monaco_telemetry_cold_benchmark.py`` between a
control source tree (``--src-a``, typically the last accepted state) and a
candidate source tree (``--src-b``, typically the working tree), so both
variants see the same CDN edge/network conditions. Defaults: A/B order,
5 runs each, first run is A (re-warms the edge after idle).

Writes a JSON report with per-run records and per-variant stats.

Example:
    uv run python tools/monaco_ab_suite.py --label exp4-merged-frames \
        --src-a /tmp/tif1-control/src --src-b src \
        --out .agents/perf-monaco/exp4.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics
import subprocess
import sys
from pathlib import Path

BENCH = Path(__file__).resolve().parent / "monaco_telemetry_cold_benchmark.py"
MIN_COMPLETENESS = 0.9


def _spawn(src_dir: str, args: argparse.Namespace, variant: str) -> dict:
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
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"child ({variant}) failed rc={proc.returncode}: {proc.stderr[-1500:]}")
    run = json.loads(proc.stdout.strip().splitlines()[-1])
    run.pop("cache_dir", None)
    run["variant"] = variant
    return run


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--src-a", required=True, help="control source tree")
    parser.add_argument("--src-b", required=True, help="candidate source tree")
    parser.add_argument("--runs", type=int, default=5, help="runs per variant")
    parser.add_argument("--label", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    records: list[dict] = []
    failures: list[dict] = []
    # Interleave A,B,A,B...; retry incomplete/failed runs (max 3 per variant).
    for i in range(args.runs):
        for variant, src in (("a", args.src_a), ("b", args.src_b)):
            tries = 0
            while True:
                tries += 1
                try:
                    run = _spawn(src, args, variant)
                    if run["completeness"] < MIN_COMPLETENESS:
                        failures.append({"variant": variant, "reason": "incomplete", **run})
                        print(
                            f"{variant} run {i + 1}: incomplete "
                            f"({run['completeness']}), retry {tries}",
                            file=sys.stderr,
                        )
                    else:
                        records.append(run)
                        print(
                            f"{variant} run {i + 1}: total={run['total_s']}s "
                            f"telemetry={run['telemetry_s']}s",
                            file=sys.stderr,
                        )
                        break
                except (RuntimeError, json.JSONDecodeError, KeyError) as e:
                    failures.append({"variant": variant, "reason": str(e)})
                    print(f"{variant} run {i + 1}: FAILED ({e}), retry {tries}", file=sys.stderr)
                if tries >= 3:
                    print(f"{variant}: giving up after {tries} tries", file=sys.stderr)
                    break

    def stats(variant: str) -> dict:
        vals = [r["total_s"] for r in records if r["variant"] == variant]
        tels = [r["telemetry_s"] for r in records if r["variant"] == variant]
        if not vals:
            return {}
        return {
            "n": len(vals),
            "total_s": {
                "mean": round(statistics.mean(vals), 4),
                "median": round(statistics.median(vals), 4),
                "min": round(min(vals), 4),
                "max": round(max(vals), 4),
                "values": vals,
            },
            "telemetry_s": {
                "mean": round(statistics.mean(tels), 4),
                "median": round(statistics.median(tels), 4),
                "min": round(min(tels), 4),
                "max": round(max(tels), 4),
                "values": tels,
            },
        }

    a, b = stats("a"), stats("b")
    if a and b:
        delta_pct = round((b["total_s"]["median"] / a["total_s"]["median"] - 1) * 100, 2)
        tel_delta_pct = round(
            (b["telemetry_s"]["median"] / a["telemetry_s"]["median"] - 1) * 100, 2
        )
    else:
        delta_pct = tel_delta_pct = None

    out = {
        "label": args.label,
        "timestamp": dt.datetime.now(dt.UTC).isoformat(),
        "src_a": args.src_a,
        "src_b": args.src_b,
        "runs": records,
        "failures": failures,
        "stats": {"a": a, "b": b},
        "b_vs_a_total_pct": delta_pct,
        "b_vs_a_telemetry_pct": tel_delta_pct,
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    print(f"wrote {out_path}")
    if delta_pct is not None:
        print(
            f"{args.label}: A median total={a['total_s']['median']}s "
            f"B median total={b['total_s']['median']}s -> B vs A {delta_pct:+.2f}% "
            f"(telemetry {tel_delta_pct:+.2f}%)"
        )


if __name__ == "__main__":
    main()
