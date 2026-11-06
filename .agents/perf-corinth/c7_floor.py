"""C7 scratch: fresh-process floors of the CLI's imports."""

import os
import subprocess
import sys
import time

PY = "/home/harish/Documents/tif1/.venv/bin/python"
ENV = dict(os.environ, PYTHONPATH=sys.argv[1], HOME="/tmp/corinth/home-C7",
           TIF1_CACHE_DIR="/tmp/corinth/warm-C7")
snippets = sys.argv[2:] or [
    "pass",
    "import typer",
    "import tif1",
    "import tif1; tif1.__version__",
    "import rich.console",
    "import typer, tif1, rich.console",
    "import tif1.cli",
    "import pandas",
    "import polars",
]
for code in snippets:
    walls = []
    for _ in range(7):
        t = time.perf_counter()
        subprocess.run([PY, "-c", code], env=ENV, check=True)
        walls.append((time.perf_counter() - t) * 1000)
    walls.sort()
    print(f"{code:45s} median {walls[3]:6.1f} min {walls[0]:6.1f}")
