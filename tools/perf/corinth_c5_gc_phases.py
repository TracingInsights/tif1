"""Corinth C5: GC time per phase of warm workloads on Python 3.13 and 3.14.

Runs ``naxos_n10_gc_phases.py`` in a fresh interpreter per (python, workload),
offline on the C5 warm cache.

    python tools/perf/corinth_c5_gc_phases.py <src dir> <lib> <workload> [<workload>...]
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PYTHONS = {
    "3.14": "/tmp/corinth/venv314/bin/python",
    "3.13": "/home/harish/Documents/tif1/.venv/bin/python",
}


def main() -> None:
    src, lib, *workloads = sys.argv[1:]
    only = os.environ.get("C5_PY")
    env = dict(
        os.environ,
        TIF1_CACHE_DIR=os.environ.get("TIF1_CACHE_DIR", "/tmp/corinth/warm-C5"),
        TIF1_OFFLINE_MODE="true",
        HOME="/tmp/corinth/home-C5",
        PYTHONPATH=src,
    )
    for label, py in PYTHONS.items():
        if only and only != label:
            continue
        for workload in workloads:
            print(f"== {label} {lib} {workload} ({src})", flush=True)
            subprocess.run(
                [py, str(HERE / "naxos_n10_gc_phases.py"), lib, workload], env=env, check=True
            )


if __name__ == "__main__":
    main()
