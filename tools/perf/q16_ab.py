"""Q16 interleaved fresh-process A/B driver for ``tools/perf/q16_jupyter_probe.py``.

Each arm is ``name=probe args`` (``{src}`` expands to ``--src-dir``); every run
gets a fresh copy of ``--cache-master`` so both arms start from the same bytes.

Usage:
    uv run python tools/perf/q16_ab.py --cache-master /tmp/tif1-q-Q16/ctl_cache_master.sqlite \
        --pairs 8 --out /tmp/tif1-q-Q16/ab.json \
        --arm "control=--src-dir /tmp/tif1-pergamon-control/src --scenario notebook" \
        --arm "candidate=--scenario notebook"
"""

from __future__ import annotations

import argparse
import json
import shlex
import shutil
import statistics
import subprocess
import sys
from pathlib import Path


def run(cache_master: str, workdir: Path, probe_args: list[str]) -> dict:
    cache_dir = workdir / "cache"
    shutil.rmtree(cache_dir, ignore_errors=True)
    cache_dir.mkdir(parents=True)
    shutil.copy(cache_master, cache_dir / "cache.sqlite")
    cmd = [sys.executable, "tools/perf/q16_jupyter_probe.py", "--cache-dir", str(cache_dir)]
    out = subprocess.run(cmd + probe_args, check=True, capture_output=True, text=True).stdout
    return json.loads(out.strip().splitlines()[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-master", required=True)
    parser.add_argument("--arm", action="append", required=True)
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--workdir", default="/tmp/tif1-q-Q16/ab")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    arms = [(a.split("=", 1)[0], shlex.split(a.split("=", 1)[1])) for a in args.arm]
    runs: list[dict[str, dict]] = []
    for i in range(args.pairs):
        order = arms if i % 2 == 0 else list(reversed(arms))
        result = {name: run(args.cache_master, Path(args.workdir), a) for name, a in order}
        runs.append(result)
        print(f"pair {i + 1}: " + " | ".join(f"{n} {result[n]['total_s']:.3f}" for n, _ in arms))
    keys: list[str] = []
    for name, _ in arms:
        for c in runs[0][name]["calls"]:
            if c["call"] not in keys:
                keys.append(c["call"])
    keys += ["total_s", "sleep0_20k_s", "tasks_5k_s"]

    def value(r: dict, key: str) -> float | None:
        if key in r:
            return r[key]
        for c in r["calls"]:
            if c["call"] == key:
                return c["s"]
        return None

    first = arms[0][0]
    for key in keys:
        line = f"{key:26s}"
        base = [value(r[first], key) for r in runs]
        for name, _ in arms:
            vals = [value(r[name], key) for r in runs]
            if any(v is None for v in vals):
                line += f"  {name}: n/a"
                continue
            med = statistics.median(vals)
            wins = ""
            if name != first and None not in base:
                w = sum(1 for v, b in zip(vals, base) if v < b)
                wins = f" faster {w}/{len(vals)}"
            line += f"  {name}: med {med * 1000:9.3f} ms min {min(vals) * 1000:9.3f}{wins}"
        print(line)
    for name, _ in arms:
        r = runs[-1][name]
        print(name, "counters", r["counters"], "nest", r["nest_applied"], "frames", r["frames"])
        errs = [c for c in r["calls"] if "error" in c]
        if errs:
            print(name, "errors", errs)
    if args.out:
        Path(args.out).write_text(json.dumps(runs, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
