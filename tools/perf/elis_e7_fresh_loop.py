"""Elis E7: run ``elis_e7_fresh.py <phase> 0`` in fresh processes and print the medians.

Usage: python tools/perf/elis_e7_fresh_loop.py SRC REPS phase[,phase...]
"""

from __future__ import annotations

import os
import statistics
import subprocess
import sys
from pathlib import Path

src, reps, phases = sys.argv[1], int(sys.argv[2]), sys.argv[3].split(",")
script = Path(__file__).with_name("elis_e7_fresh.py")
env = dict(os.environ, PYTHONPATH=src, TIF1_OFFLINE_MODE="true")
for phase in phases:
    vals = []
    for _ in range(reps):
        out = subprocess.run(
            [sys.executable, str(script), phase, "0"],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        ).stdout.split()
        vals.append(float(out[-2]))
    print(f"{phase:14s} median={statistics.median(vals):7.2f} min={min(vals):7.2f}")
