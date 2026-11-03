"""Interleaved control/candidate measurement of tif1's own warm startup.

Each child pre-imports numpy and pandas, then times ``import tif1`` +
``get_session(2025, "Bahrain Grand Prix", "Race")`` (warm cache, offline).
The pandas import (~340 ms, +-100 ms under load) is excluded, so the
tif1-attributable startup is resolved to the millisecond. The parent
alternates the arms (ABBA) and reports medians, mins and pairs won.
"""

from __future__ import annotations

import os
import statistics
import subprocess
import sys

PY = "/home/harish/Documents/tif1/.venv/bin/python"
CHILD = r"""
import time
import numpy, pandas
t0 = time.perf_counter()
import tif1
t1 = time.perf_counter()
s = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
t2 = time.perf_counter()
extra = 0.0
mode = __import__("sys").argv[1]
if mode == "laps":
    t3 = time.perf_counter(); s.laps; extra = time.perf_counter() - t3
elif mode == "results":
    t3 = time.perf_counter(); s.laps; s.results; extra = time.perf_counter() - t3
elif mode == "tel":
    t3 = time.perf_counter(); s.get_fastest_lap_tel(); extra = time.perf_counter() - t3
print((t1 - t0) * 1000, (t2 - t1) * 1000, extra * 1000)
"""
ARMS = {
    "control": ("/tmp/tif1-delphi-control/src", "/tmp/tif1-delphi-warm"),
    "candidate": ("/tmp/tif1-delphi/D2/src", "/tmp/tif1-delphi-warm-D2"),
}


def run(arm: str, mode: str) -> tuple[float, float, float]:
    src, cache = ARMS[arm]
    env = dict(os.environ, PYTHONPATH=src, TIF1_CACHE_DIR=cache, TIF1_OFFLINE_MODE="true")
    out = subprocess.run(
        [PY, "-c", CHILD, mode], capture_output=True, text=True, env=env, check=True
    ).stdout
    a, b, c = (float(x) for x in out.split())
    return a, b, c


def main() -> None:
    pairs = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    mode = sys.argv[2] if len(sys.argv) > 2 else "none"
    recs: dict[str, list[tuple[float, float, float]]] = {"control": [], "candidate": []}
    for i in range(pairs):
        order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
        for arm in order:
            recs[arm].append(run(arm, mode))
    for idx, name in enumerate(("import tif1", "get_session", f"extra({mode})")):
        c = [r[idx] for r in recs["control"]]
        d = [r[idx] for r in recs["candidate"]]
        wins = sum(1 for a, b in zip(c, d, strict=True) if b < a)
        print(
            f"{name:16s} control med {statistics.median(c):6.2f} min {min(c):6.2f} | "
            f"candidate med {statistics.median(d):6.2f} min {min(d):6.2f} | "
            f"paired delta med {statistics.median([b - a for a, b in zip(c, d, strict=True)]):+6.2f} "
            f"| candidate faster {wins}/{pairs}"
        )
    tot_c = [r[1] + r[2] for r in recs["control"]]
    tot_d = [r[1] + r[2] for r in recs["candidate"]]
    wins = sum(1 for a, b in zip(tot_c, tot_d, strict=True) if b < a)
    print(
        f"{'get_session+extra':16s} control med {statistics.median(tot_c):6.2f} | "
        f"candidate med {statistics.median(tot_d):6.2f} | candidate faster {wins}/{pairs}"
    )


if __name__ == "__main__":
    main()
