"""Corinth C7: interleaved control-vs-candidate wall time of CLI commands.

Each pair runs one command in a fresh process under the control tree then the
candidate tree (the order alternates per pair: ABAB, BABA), same interpreter
and cache. Reports the median, p90 and pairs won per command and writes JSON.

    python tools/perf/corinth_c7_cli_ab.py --control-src /tmp/corinth/control/src \
        --candidate-src src --cache /tmp/corinth/warm-C7 --offline --pairs 10 \
        --commands commands.txt --out result.json

Cold mode (``--cold``): each run gets a new empty cache directory (live CDN).
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import orjson


def _run(
    src: str, cache: str, home: str, offline: bool, argv: list[str]
) -> tuple[float, float, int]:
    """``(wall ms, child CPU ms (user + sys), exit code)`` of one fresh process.

    A blocking ``os.wait4``: ``subprocess.run(timeout=...)`` polls ``waitpid``
    with a back-off of up to 50 ms, which quantized the walls.
    """
    env = dict(os.environ)
    env.update(PYTHONPATH=src, TIF1_CACHE_DIR=cache, HOME=home, COLUMNS="120", TERM="dumb")
    if offline:
        env["TIF1_OFFLINE_MODE"] = "true"
    start = time.perf_counter()
    proc = subprocess.Popen(
        [sys.executable, "-m", "tif1.cli", *argv],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _, status, usage = os.wait4(proc.pid, 0)
    wall = (time.perf_counter() - start) * 1000.0
    proc.returncode = os.waitstatus_to_exitcode(status)
    return wall, (usage.ru_utime + usage.ru_stime) * 1000.0, proc.returncode


def _p90(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(0.9 * (len(ordered) - 1)))]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--cache", help="warm cache directory (both arms)")
    parser.add_argument("--cold", action="store_true", help="a new empty cache per run")
    parser.add_argument("--home", default=os.environ.get("HOME", ""))
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--pairs", type=int, default=10)
    parser.add_argument("--commands", required=True)
    parser.add_argument("--out")
    args = parser.parse_args()

    lines = Path(args.commands).read_text(encoding="utf-8").splitlines()
    commands = [shlex.split(line) for line in lines if line.strip() and not line.startswith("#")]
    report: dict[str, dict] = {}
    print(f"{'command':42s} {'control':>9s} {'cand':>9s} {'ratio':>7s} {'won':>6s}  p90 c/c")
    for argv in commands:
        label = " ".join(argv)
        # One unmeasured run per arm: page cache and .pyc files.
        for src in (args.control_src, args.candidate_src):
            if not args.cold:
                _run(src, args.cache, args.home, args.offline, argv)
        walls: dict[str, list[float]] = {"control": [], "candidate": []}
        cpus: dict[str, list[float]] = {"control": [], "candidate": []}
        codes: set[int] = set()
        for pair in range(args.pairs):
            order = ("control", "candidate") if pair % 2 == 0 else ("candidate", "control")
            for arm in order:
                src = args.control_src if arm == "control" else args.candidate_src
                if args.cold:
                    cache = tempfile.mkdtemp(prefix="c7-cold-")
                    try:
                        wall, cpu, code = _run(src, cache, args.home, args.offline, argv)
                    finally:
                        shutil.rmtree(cache, ignore_errors=True)
                else:
                    wall, cpu, code = _run(src, args.cache, args.home, args.offline, argv)
                walls[arm].append(wall)
                cpus[arm].append(cpu)
                codes.add(code)
        won = sum(1 for c, d in zip(walls["control"], walls["candidate"], strict=True) if d < c)
        control_median = statistics.median(walls["control"])
        candidate_median = statistics.median(walls["candidate"])
        report[label] = {
            "control": walls["control"],
            "candidate": walls["candidate"],
            "control_median": control_median,
            "candidate_median": candidate_median,
            "control_p90": _p90(walls["control"]),
            "candidate_p90": _p90(walls["candidate"]),
            "pairs_won": won,
            "control_cpu": cpus["control"],
            "candidate_cpu": cpus["candidate"],
            "control_cpu_median": statistics.median(cpus["control"]),
            "candidate_cpu_median": statistics.median(cpus["candidate"]),
            "pairs": args.pairs,
            "exit_codes": sorted(codes),
        }
        print(
            f"{label:42s} {control_median:9.1f} {candidate_median:9.1f} "
            f"{candidate_median / control_median:7.3f} {won:>3d}/{args.pairs:<2d}  "
            f"{_p90(walls['control']):.0f}/{_p90(walls['candidate']):.0f}  cpu "
            f"{statistics.median(cpus['control']):.0f}/{statistics.median(cpus['candidate']):.0f}"
            + ("" if codes == {0} else f"  exit codes {sorted(codes)}"),
            flush=True,
        )
    if args.out:
        Path(args.out).write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
