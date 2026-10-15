"""Q20: byte parity of ``tif1 fastest`` (table and ``-d``) control vs candidate.

For every session that has a pandas laps frame in the cache copy, run
``python -m tif1.cli fastest Y GP S`` in both trees (fresh process,
``COLUMNS=120 NO_COLOR=1 TERM=dumb``), then ``-d`` for the first and last
driver of the table plus one unknown code, and compare stdout/stderr/exit code
byte for byte.

Usage:
    uv run python tools/q20_cli_parity.py --control-src /tmp/tif1-pergamon-control/src \
        --candidate-src src --cache /tmp/tif1-q-Q20/usercache --workers 8
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import unquote

_ROW_RE = re.compile(r"^│ \d+\s+│ (\w+)\s+│")


def _run(src: str, cache: str, args: list[str]) -> tuple[bytes, bytes, int]:
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
    proc = subprocess.run(
        [sys.executable, "-m", "tif1.cli", *args], env=env, capture_output=True, check=False
    )
    stderr = proc.stderr
    if proc.returncode != 0 and stderr:
        # typer's pretty traceback quotes tree paths and source lines; the
        # exception line (last line) is what both trees must agree on.
        stderr = stderr.rstrip(b"\n").rsplit(b"\n", 1)[-1]
    return proc.stdout, stderr, proc.returncode


def _check(srcs: dict[str, str], cache: str, session: tuple[int, str, str]) -> list[str]:
    year, gp, name = session
    base = ["fastest", str(year), gp, name]
    problems: list[str] = []
    ctl = _run(srcs["control"], cache, base)
    cand = _run(srcs["cand"], cache, base)
    if ctl != cand:
        problems.append(f"{base}: table differs (rc {ctl[2]} vs {cand[2]})")
    drivers = [m.group(1) for line in ctl[0].decode().splitlines() if (m := _ROW_RE.match(line))]
    picks = [*dict.fromkeys([*drivers[:1], *drivers[-1:]]), "ZZZ"]
    for drv in picks:
        args = [*base, "-d", drv]
        if _run(srcs["control"], cache, args) != _run(srcs["cand"], cache, args):
            problems.append(f"{args}: differs")
    return problems if problems else [f"ok {base} ({len(drivers)} rows)"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--limit", type=int, default=0)
    opts = parser.parse_args()
    srcs = {
        "control": str(Path(opts.control_src).resolve()),
        "cand": str(Path(opts.candidate_src).resolve()),
    }
    with sqlite3.connect(f"file:{opts.cache}/cache.sqlite?mode=ro", uri=True) as conn:
        rows = conn.execute("SELECT year, gp, session FROM laps_frames ORDER BY 1, 2, 3").fetchall()
    sessions = [(int(y), unquote(g), unquote(s)) for y, g, s in rows]
    if opts.limit:
        sessions = sessions[: opts.limit]
    bad = 0
    with ThreadPoolExecutor(max_workers=opts.workers) as pool:
        for lines in pool.map(lambda s: _check(srcs, opts.cache, s), sessions):
            for line in lines:
                bad += not line.startswith("ok ")
                print(line, flush=True)
    print(f"{len(sessions)} sessions, {bad} differences")


if __name__ == "__main__":
    main()
