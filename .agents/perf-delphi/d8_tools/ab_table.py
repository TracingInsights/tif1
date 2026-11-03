"""Print an A/B JSON of ``delphi_warm_ab.py`` as a per-phase table.

Usage: ``python -I ab_table.py <ab.json> [workload,...]``.
"""

from __future__ import annotations

import json
import sys

path = sys.argv[1]
only = sys.argv[2].split(",") if len(sys.argv) > 2 else None
with open(path, encoding="utf-8") as fh:
    data = json.load(fh)
for workload, phases in data["summary"].items():
    if only and workload not in only:
        continue
    print(f"== {workload} ({data['lib']}, {data['pairs']} pairs)")
    for phase, v in phases.items():
        print(
            f"  {phase:22s} ctl {v['control_median']:7.1f} (min {v['control_min']:7.1f})"
            f"  cand {v['candidate_median']:7.1f} (min {v['candidate_min']:7.1f})"
            f"  won {v['candidate_faster_pairs']}"
        )
