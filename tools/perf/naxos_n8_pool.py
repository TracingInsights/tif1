"""Naxos N8: pool the cold A/B result files (medians, p90, pairs won)."""

import json
import statistics
import sys
from pathlib import Path

DIR = Path(".agents/perf-naxos")
SETS = {
    "pandas": ["N8_cold_pandas.json", "N8_cold_pandas_dt16.json"],
    "polars-bridge-only": ["N8_cold_polars.json", "N8_cold_polars_dt16.json"],
    "polars-final": ["N8_cold_polars_final.json", "N8_cold_polars_final_dt16.json"],
    "all_tel": ["N8_cold_alltel_pandas.json", "N8_cold_alltel_polars.json"],
}


def p90(xs):
    xs = sorted(xs)
    k = 0.9 * (len(xs) - 1)
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def stat(rows, key):
    vals = [r[key] for r in rows]
    return f"{statistics.median(vals):.0f} (p90 {p90(vals):.0f})"


for label, files in SETS.items():
    pooled: dict[tuple[str, str], dict[str, list]] = {}
    for name in files:
        path = DIR / name
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        for workload, arms in data["raw"].items():
            slot = pooled.setdefault((data["lib"], workload), {"control": [], "candidate": []})
            for arm, rows in arms.items():
                slot[arm] += rows
    for (lib, workload), arms in pooled.items():
        ctl, cand = arms["control"], arms["candidate"]
        phase = {"driver_tel_x2": "driver_tel_2"}.get(workload, workload)
        wins = sum(b["process_wall"] < a["process_wall"] for a, b in zip(ctl, cand, strict=True))
        line = (
            f"{label:18s} {lib:6s} {workload:15s} n={len(ctl):2d} wall {stat(ctl, 'process_wall')}"
            f" -> {stat(cand, 'process_wall')} ({wins}/{len(ctl)}) | {phase} {stat(ctl, phase)}"
            f" -> {stat(cand, phase)}"
        )
        if workload == "driver_tel_x2":
            line += f" | 1st wave {stat(ctl, 'driver_tel')} -> {stat(cand, 'driver_tel')}"
        niq_c = sum(r.get("niquests_imported", 0) for r in ctl)
        niq_d = sum(r.get("niquests_imported", 0) for r in cand)
        line += f" | niquests imported {niq_c:.0f}/{len(ctl)} -> {niq_d:.0f}/{len(cand)}"
        print(line)
sys.exit(0)
