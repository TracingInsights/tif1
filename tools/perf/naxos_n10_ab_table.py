"""Naxos N10: markdown phase table from a ``delphi_warm_ab.py`` JSON (its ``summary``).

python tools/perf/naxos_n10_ab_table.py .agents/perf-naxos/N10_ab_pandas.json
"""

from __future__ import annotations

import sys
from pathlib import Path

import orjson


def main() -> None:
    for path in sys.argv[1:]:
        data = orjson.loads(Path(path).read_bytes())
        print(f"### {Path(path).name}: {data['lib']}, {data['pairs']} pairs")
        print("| workload | phase | control median | candidate median | pairs won |")
        print("|---|---|---|---|---|")
        for workload, phases in data["summary"].items():
            for phase, row in phases.items():
                print(
                    f"| {workload} | {phase} | {row['control_median']:.1f} | "
                    f"{row['candidate_median']:.1f} | {row['candidate_faster_pairs']} |"
                )


if __name__ == "__main__":
    main()
