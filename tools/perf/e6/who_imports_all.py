"""Run who_imports.py for several workloads (fresh process each), filtered output."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
env = dict(os.environ, TIF1_OFFLINE_MODE="true")
for workload in sys.argv[1].split(","):
    print(f"##### {workload}", flush=True)
    proc = subprocess.run(
        [sys.executable, str(HERE / "who_imports.py"), workload],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    for line in proc.stderr.splitlines():
        if line.startswith("    ") or 'who_imports.py", line 2' in line:
            continue
        print(line)
