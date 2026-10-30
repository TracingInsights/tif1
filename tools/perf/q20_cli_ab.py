"""Q20: fresh-process wall time of ``tif1`` CLI commands, control vs candidate.

Every run is ``python -m tif1.cli <args>`` in a fresh process with
``COLUMNS=120 NO_COLOR=1 TERM=dumb`` and ``TIF1_CACHE_DIR`` pointing at a warm
cache copy. Control and candidate are interleaved pair by pair (order
alternates), stdout/stderr bytes are compared, medians and win counts printed.

Usage:
    uv run python tools/perf/q20_cli_ab.py --control-src /tmp/tif1-pergamon-control/src \
        --candidate-src src --cache /tmp/tif1-q-Q20/cache --pairs 8
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

COMMANDS: list[list[str]] = [
    ["--help"],
    ["version"],
    ["cache-info"],
    ["cache-clear", "--yes"],
    ["events", "2026"],
    ["sessions", "2026", "Monaco"],
    ["drivers", "2026", "Monaco", "Race"],
    ["fastest", "2026", "Monaco", "Race"],
    ["fastest", "2026", "Monaco", "Race", "-d", "VER"],
    ["fastest", "2026", "Monaco", "Race", "-d", "ANT"],
    ["debug", "2026", "Monaco", "Race"],
]


_LOG_STAMP = re.compile(rb"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d - ", re.MULTILINE)


def _run(src: str, cache: str, args: list[str]) -> tuple[float, bytes, bytes, int]:
    if args[0] == "cache-clear":
        # cache-clear wipes its cache: give every run a fresh copy (not timed).
        scratch = Path(cache).parent / "clear-scratch"
        shutil.rmtree(scratch, ignore_errors=True)
        shutil.copytree(cache, scratch)
        cache = str(scratch)
    env = dict(os.environ)
    env.update(
        {
            "PYTHONPATH": src,
            "TIF1_CACHE_DIR": cache,
            "COLUMNS": "120",
            "LINES": "40",
            "NO_COLOR": "1",
            "TERM": "dumb",
        }
    )
    t0 = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, "-m", "tif1.cli", *args], env=env, capture_output=True, check=False
    )
    elapsed = time.perf_counter() - t0
    # ``debug`` logs carry wall-clock stamps; compare the messages.
    return elapsed, proc.stdout, _LOG_STAMP.sub(b"", proc.stderr), proc.returncode


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--cache", required=True, help="shared warm cache dir (read-only use)")
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--only", nargs="*", help="command names to run (first arg)")
    parser.add_argument("--dump-dir", help="write stdout of the first pair per arm here")
    opts = parser.parse_args()

    srcs = {
        "control": str(Path(opts.control_src).resolve()),
        "cand": str(Path(opts.candidate_src).resolve()),
    }
    print(f"{'command':40s} {'ctl med':>8s} {'cand med':>8s} {'delta':>7s} wins  out")
    for args in COMMANDS:
        if opts.only and args[0] not in opts.only:
            continue
        times: dict[str, list[float]] = {"control": [], "cand": []}
        outs: dict[str, set[tuple[bytes, bytes, int]]] = {"control": set(), "cand": set()}
        for i in range(opts.pairs):
            order = ["control", "cand"] if i % 2 == 0 else ["cand", "control"]
            for arm in order:
                dt, out, err, rc = _run(srcs[arm], opts.cache, args)
                times[arm].append(dt)
                outs[arm].add((out, err, rc))
                if opts.dump_dir and i == 0:
                    name = "_".join(args).replace("-", "")
                    Path(opts.dump_dir, f"{arm}_{name}.out").write_bytes(out + b"\n--err--\n" + err)
        wins = sum(c < k for c, k in zip(times["cand"], times["control"], strict=True))
        same = outs["control"] == outs["cand"] and len(outs["control"]) == 1
        cm, km = statistics.median(times["control"]), statistics.median(times["cand"])
        print(
            f"{' '.join(args):40s} {cm * 1000:8.1f} {km * 1000:8.1f} "
            f"{(km - cm) / cm * 100:+6.1f}% {wins}/{opts.pairs}  "
            f"{'identical' if same else 'DIFFERENT'} (variants c={len(outs['control'])} "
            f"k={len(outs['cand'])})",
            flush=True,
        )


if __name__ == "__main__":
    main()
