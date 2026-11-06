"""Corinth C5: per-module import time of a warm get_session, Python 3.13 vs 3.14.

Runs ``-X importtime`` over ``import tif1; tif1.get_session(...)`` ``--runs``
times per interpreter (fresh process each, offline, warm cache), takes the
median self time of every module, and prints the largest 3.14 - 3.13 deltas
plus the per-top-level-package totals.

    python tools/perf/corinth_c5_importtime.py --src src --lib pandas --runs 5
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path
from statistics import median

PYTHONS = {
    "3.13": "/home/harish/Documents/tif1/.venv/bin/python",
    "3.14": "/tmp/corinth/venv314/bin/python",
}
_LINE = re.compile(r"import time:\s+(\d+) \|\s+(\d+) \|(\s*)(\S+)")


def _one(python: str, src: str, lib: str) -> dict[str, int]:
    env = dict(
        os.environ,
        PYTHONPATH=src,
        TIF1_OFFLINE_MODE="true",
        TIF1_CACHE_DIR=os.environ.get("TIF1_CACHE_DIR", "/tmp/corinth/warm-C5"),
        HOME="/tmp/corinth/home-C5",
    )
    code = f"import tif1; tif1.get_session(2025, 'Bahrain Grand Prix', 'Race', lib={lib!r})"
    proc = subprocess.run(
        [python, "-X", "importtime", "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    out: dict[str, int] = {}
    for line in proc.stderr.splitlines():
        m = _LINE.match(line)
        if m:
            out[m.group(4)] = out.get(m.group(4), 0) + int(m.group(1))
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", default="src")
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--top", type=int, default=40)
    args = parser.parse_args()
    runs: dict[str, list[dict[str, int]]] = {k: [] for k in PYTHONS}
    for _ in range(args.runs):
        for label, python in PYTHONS.items():
            runs[label].append(_one(python, str(Path(args.src).resolve()), args.lib))
    med: dict[str, dict[str, float]] = {}
    for label, recs in runs.items():
        names = set().union(*recs)
        med[label] = {n: median(r.get(n, 0) for r in recs) / 1000 for n in names}
    names = set(med["3.13"]) | set(med["3.14"])
    pkg: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0])
    for n in names:
        top = n.split(".")[0]
        pkg[top][0] += med["3.13"].get(n, 0)
        pkg[top][1] += med["3.14"].get(n, 0)
    tot = [sum(v[0] for v in pkg.values()), sum(v[1] for v in pkg.values())]
    print(f"total self ms: 3.13 {tot[0]:.1f}  3.14 {tot[1]:.1f}")
    print("-- packages (3.13 -> 3.14 ms)")
    for top, (a, b) in sorted(pkg.items(), key=lambda kv: -abs(kv[1][1] - kv[1][0]))[:25]:
        print(f"{top:30s} {a:7.1f} -> {b:7.1f}  ({b - a:+.1f})")
    print("-- modules")
    delta = sorted(names, key=lambda n: -abs(med["3.14"].get(n, 0) - med["3.13"].get(n, 0)))
    for n in delta[: args.top]:
        a, b = med["3.13"].get(n, 0), med["3.14"].get(n, 0)
        print(f"{n:50s} {a:7.2f} -> {b:7.2f}  ({b - a:+.2f})")


if __name__ == "__main__":
    main()
