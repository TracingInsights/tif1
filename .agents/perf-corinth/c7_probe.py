"""C7 scratch probe: timings and import profile of CLI commands."""

import os
import subprocess
import sys
import time
from pathlib import Path

PY = "/home/harish/Documents/tif1/.venv/bin/python"
TREE = Path("/tmp/corinth/C7")
SRC = sys.argv[1] if len(sys.argv) > 1 else str(TREE / "src")
ENV = dict(os.environ)
ENV.update(
    PYTHONPATH=SRC,
    HOME="/tmp/corinth/home-C7",
    TIF1_CACHE_DIR="/tmp/corinth/warm-C7",
    COLUMNS="120",
)
Path(ENV["HOME"]).mkdir(parents=True, exist_ok=True)

mode = sys.argv[2] if len(sys.argv) > 2 else "time"
cmds = [a.split() for a in sys.argv[3:]] or [["version"], ["--help"]]

if mode == "time":
    for cmd in cmds:
        walls = []
        for _ in range(5):
            t = time.perf_counter()
            subprocess.run([PY, "-m", "tif1.cli", *cmd], env=ENV, capture_output=True, check=False)
            walls.append((time.perf_counter() - t) * 1000)
        walls.sort()
        print(f"{' '.join(cmd):40s} median {walls[2]:.0f} ms  min {walls[0]:.0f}")
elif mode == "importtime":
    for cmd in cmds:
        r = subprocess.run(
            [PY, "-X", "importtime", "-m", "tif1.cli", *cmd],
            env=ENV,
            capture_output=True,
            text=True,
            check=False,
        )
        rows = []
        for line in r.stderr.splitlines():
            if not line.startswith("import time:") or "cumulative" in line:
                continue
            parts = line[len("import time:") :].split("|")
            rows.append((int(parts[1]), int(parts[0]), parts[2].rstrip()))
        print("==", " ".join(cmd), "total modules", len(rows))
        for cum, self_us, name in sorted(rows, reverse=True)[:40]:
            print(f"{cum / 1000:8.1f} {self_us / 1000:8.1f} {name}")
elif mode in ("inproc", "profile"):
    env = dict(ENV, PROFILE="1") if mode == "profile" else ENV
    for cmd in cmds:
        r = subprocess.run(
            [PY, str(TREE / ".agents/perf-corinth/c7_inproc.py"), *cmd],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        print("==", " ".join(cmd))
        print("\n".join(r.stderr.splitlines()[:60]))
elif mode == "show":
    for cmd in cmds:
        r = subprocess.run(
            [PY, "-m", "tif1.cli", *cmd], env=ENV, capture_output=True, text=True, check=False
        )
        print("== rc", r.returncode, cmd)
        print(r.stdout)
        print(r.stderr)
