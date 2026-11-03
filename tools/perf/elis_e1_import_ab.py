"""Elis E1: interleaved ``import tif1.core`` + ``get_session`` time, control vs candidate.

Usage:
    python tools/perf/elis_e1_import_ab.py CONTROL_SRC CANDIDATE_SRC CACHE_DIR [ROUNDS]
"""

from __future__ import annotations

import os
import statistics
import subprocess
import sys

CODE = (
    "import time\n"
    "t = time.perf_counter()\n"
    "import tif1.core\n"
    "a = (time.perf_counter() - t) * 1000\n"
    "import tif1\n"
    "t = time.perf_counter()\n"
    "tif1.get_session(2025, 'Bahrain Grand Prix', 'Race')\n"
    "b = (time.perf_counter() - t) * 1000\n"
    "print(a, b)\n"
)


def main() -> None:
    trees = {"control": sys.argv[1], "candidate": sys.argv[2]}
    cache = sys.argv[3]
    rounds = int(sys.argv[4]) if len(sys.argv) > 4 else 20
    res: dict[str, list[tuple[float, float]]] = {k: [] for k in trees}
    for i in range(rounds):
        order = list(trees) if i % 2 == 0 else list(trees)[::-1]
        for arm in order:
            env = dict(
                os.environ, PYTHONPATH=trees[arm], TIF1_CACHE_DIR=cache, TIF1_OFFLINE_MODE="true"
            )
            out = subprocess.run(
                [sys.executable, "-c", CODE], capture_output=True, text=True, env=env, check=True
            ).stdout.split()
            res[arm].append((float(out[0]), float(out[1])))
    for arm, vals in res.items():
        imp = [v[0] for v in vals]
        gs = [v[1] for v in vals]
        print(
            f"{arm:9s} import tif1.core {statistics.median(imp):.1f} (min {min(imp):.1f})"
            f"  get_session {statistics.median(gs):.1f} (min {min(gs):.1f})"
        )


if __name__ == "__main__":
    main()
