"""Run tools/perf/perf_gate.py with the newest upstream wheels (``.newpkgs``) ahead of src.

The gate's children get ``PYTHONPATH=<repo>/src``; this wrapper prepends the
``pip install --target`` directory so they import pandas 3.1.0 & co. Extra
arguments go to the gate (e.g. ``--write-baseline`` is NOT wanted here).
"""

import os
import sys
from pathlib import Path

NEW = "/tmp/tif1-gortyn/G5/.newpkgs"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "perf"))

import perf_gate  # noqa: E402

_orig = perf_gate.child_env


def child_env(cache_dir, home, src):
    env = _orig(cache_dir, home, src)
    env["PYTHONPATH"] = NEW + os.pathsep + env.get("PYTHONPATH", "")
    return env


perf_gate.child_env = child_env
sys.argv = ["perf_gate.py", *sys.argv[1:]]
perf_gate.main()
