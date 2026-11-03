"""Markdown tables of the G5 A/B JSON files."""

import json
from pathlib import Path

ROOT = Path("/tmp/tif1-gortyn/G5/.agents/perf-gortyn")
SKIP = {"import", "in_process_total", "results", "weather"}
for name in (
    "versions_pandas",
    "versions_polars",
    "code_new_pandas",
    "code_new_polars",
    "code_locked_pandas",
    "code_locked_polars",
    "code_new_pandas_10",
    "versions_pandas_10",
):
    data = json.loads((ROOT / f"G5_ab_{name}.json").read_text(encoding="utf-8"))
    print(f"\n#### {name} ({data['pairs']} pairs)\n")
    print("| workload | phase | control | candidate | pairs won |")
    print("|---|---|---:|---:|---:|")
    for workload, phases in data["summary"].items():
        for phase, v in phases.items():
            if phase in SKIP:
                continue
            print(
                f"| {workload} | {phase} | {v['control_median']} | {v['candidate_median']} "
                f"| {v['candidate_faster_pairs']} |"
            )
