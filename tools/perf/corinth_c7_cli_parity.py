"""Corinth C7: CLI output parity, control tree vs candidate tree.

Runs every command line of a list in a fresh process with each tree's
``src`` on ``PYTHONPATH`` (same interpreter, same cache, ``COLUMNS=120``) and
compares stdout, stderr and the exit code byte for byte. The control run goes
first, so on an online cache it fetches what the candidate then reads.

    python tools/perf/corinth_c7_cli_parity.py --control-src /tmp/corinth/control/src \
        --candidate-src src --cache /path/to/cache [--offline] --commands commands.txt
"""

from __future__ import annotations

import argparse
import difflib
import os
import shlex
import subprocess
import sys
from pathlib import Path


def run(src: str, cache: str, home: str, offline: bool, argv: list[str]) -> tuple[int, str, str]:
    env = dict(os.environ)
    env.update(
        PYTHONPATH=src,
        TIF1_CACHE_DIR=cache,
        HOME=home,
        COLUMNS="120",
        LINES="40",
        TERM="dumb",
    )
    env.pop("FORCE_COLOR", None)
    if offline:
        env["TIF1_OFFLINE_MODE"] = "true"
    proc = subprocess.run(
        [sys.executable, "-m", "tif1.cli", *argv],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _exception_line(stderr: str) -> str:
    """The last line of a traceback (exception type and message)."""
    lines = [line for line in stderr.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--home", default=os.environ.get("HOME", ""))
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--commands", required=True, help="file: one command line per line")
    args = parser.parse_args()

    lines = Path(args.commands).read_text(encoding="utf-8").splitlines()
    commands = [shlex.split(line) for line in lines if line.strip() and not line.startswith("#")]
    differing = 0
    for argv in commands:
        control = run(args.control_src, args.cache, args.home, args.offline, argv)
        candidate = run(args.candidate_src, args.cache, args.home, args.offline, argv)
        label = " ".join(argv)
        if (
            control[0] == candidate[0] != 0
            and control[1] == candidate[1]
            and "Traceback" in control[2]
            and "Traceback" in candidate[2]
            and _exception_line(control[2]) == _exception_line(candidate[2])
        ):
            # Both raised the same exception; the traceback frames show each
            # tree's own source lines and paths.
            print(f"SAME-ERROR rc={control[0]} {_exception_line(control[2])[:90]}  {label}")
            continue
        if control == candidate:
            lines_out = len(control[1].splitlines())
            print(f"SAME rc={control[0]} stdout={lines_out} lines  {label}")
            continue
        differing += 1
        print(f"DIFF {label}: rc {control[0]} vs {candidate[0]}")
        for name, a, b in (
            ("stdout", control[1], candidate[1]),
            ("stderr", control[2], candidate[2]),
        ):
            if a != b:
                diff = difflib.unified_diff(
                    a.splitlines(), b.splitlines(), "control", "candidate", lineterm="", n=1
                )
                print(f"  -- {name}")
                for line in list(diff)[:40]:
                    print("  " + line)
    print(f"{differing} differing of {len(commands)} commands")
    return 1 if differing else 0


if __name__ == "__main__":
    raise SystemExit(main())
