"""Gortyn G4: run ``analysis_split.py`` on control, candidate, and candidate with numpy first.

Usage (from the repo root):
    python tools/perf/gortyn_g4/analysis_split_ab.py [--reps 5]
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARMS = {
    "control": ("/tmp/tif1-gortyn-control/src", []),
    "candidate": (str(HERE.parent.parent.parent / "src"), []),
    "candidate_numpy_first": (str(HERE.parent.parent.parent / "src"), ["--numpy-first"]),
}
REPS = int(sys.argv[sys.argv.index("--reps") + 1]) if "--reps" in sys.argv else 5
if "--blas1" in sys.argv:  # every arm with a one-thread OpenBLAS
    os.environ["OPENBLAS_NUM_THREADS"] = "1"

recs: dict[str, list[dict[str, float]]] = {arm: [] for arm in ARMS}
for _ in range(REPS):
    for arm, (src, extra) in ARMS.items():
        env = dict(
            os.environ,
            TIF1_CACHE_DIR="/tmp/tif1-gortyn-warm-G4",
            TIF1_OFFLINE_MODE="true",
            PYTHONPATH=src,
        )
        proc = subprocess.run(
            [sys.executable, str(HERE / "analysis_split.py"), *extra],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        recs[arm].append(json.loads(proc.stdout.strip().splitlines()[-1]))
print("step".ljust(28) + "".join(arm.rjust(24) for arm in ARMS))
for key in recs["control"][0]:
    medians = [statistics.median(r[key] for r in recs[arm]) for arm in ARMS]
    print(key.ljust(28) + "".join(f"{m:24.1f}" for m in medians))
