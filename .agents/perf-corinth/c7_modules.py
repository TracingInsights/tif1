"""C7 scratch: which heavy libraries a CLI command imports (fresh process per command)."""

import os
import subprocess
import sys

PY = "/home/harish/Documents/tif1/.venv/bin/python"
CODE = """
import atexit, sys
def report():
    names = ("pandas", "numpy", "polars", "pyarrow", "importlib.metadata", "niquests", "rich.progress")
    sys.__stderr__.write("MODULES " + ",".join(n for n in names if n in sys.modules) + "\\n")
atexit.register(report)
sys.argv = ["tif1", *sys.argv[1:]]
import tif1.cli
tif1.cli.app()
"""
for src in sys.argv[1:3]:
    for command in sys.argv[3:]:
        env = dict(
            os.environ,
            PYTHONPATH=src,
            HOME="/tmp/corinth/home-C7",
            TIF1_CACHE_DIR="/tmp/corinth/warm-C7",
            TIF1_OFFLINE_MODE="true",
        )
        r = subprocess.run(
            [PY, "-c", CODE, *command.split()], env=env, capture_output=True, text=True, check=False
        )
        line = [x for x in r.stderr.splitlines() if x.startswith("MODULES")]
        print(f"{src.split('/')[3]:8s} {command:32s} {line[-1] if line else r.stderr[-200:]}")
