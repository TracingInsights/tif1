"""Run the Delphi parity digest of this worktree for one backend and compare it.

Usage: ``python -I parity.py <pandas|polars>``. Sets the environment the
brief prescribes (this worktree's warm cache, offline) and runs
``delphi_parity.py --lib <lib> --out /tmp/D8_parity_<lib>.json`` followed by
``--compare`` against the control digest ``/tmp/delphi_parity_<lib>.json``.
"""

from __future__ import annotations

import os
import subprocess
import sys

lib = sys.argv[1] if len(sys.argv) > 1 else "pandas"
tool = "/home/harish/Documents/tif1/tools/perf/delphi_parity.py"
python = "/home/harish/Documents/tif1/.venv/bin/python"
env = dict(
    os.environ,
    TIF1_CACHE_DIR="/tmp/tif1-delphi-warm-D8",
    TIF1_OFFLINE_MODE="true",
    PYTHONPATH="/tmp/tif1-delphi/D8/src",
)
out = f"/tmp/D8_parity_{lib}.json"
subprocess.run([python, tool, "--lib", lib, "--out", out], env=env, check=True)
subprocess.run([python, tool, "--compare", f"/tmp/delphi_parity_{lib}.json", out], check=True)
