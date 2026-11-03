"""Gortyn G4: run ``real_parity.py`` on control, candidate and candidate with numpy blocked.

Usage (from the repo root):
    python tools/perf/gortyn_g4/real_parity_ab.py --control-src /tmp/tif1-gortyn-control/src \
        --candidate-src src --cache /tmp/tif1-gortyn-warm-G4 --out-dir /tmp
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "real_parity.py"


def _run(src: str, cache: str, out: Path, *extra: str) -> None:
    env = dict(
        os.environ,
        TIF1_CACHE_DIR=cache,
        TIF1_OFFLINE_MODE="true",
        PYTHONPATH=str(Path(src).resolve()),
    )
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), *extra],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    out.write_text(proc.stdout, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--out-dir", default="/tmp")
    args = parser.parse_args()
    out_dir = Path(args.out_dir)
    control = out_dir / "G4_real_control.json"
    candidate = out_dir / "G4_real_candidate.json"
    blocked = out_dir / "G4_real_blocked.json"
    _run(args.control_src, args.cache, control)
    _run(args.candidate_src, args.cache, candidate)
    _run(args.candidate_src, args.cache, blocked, "--block-numpy")
    for other in (candidate, blocked):
        print(f"--- control vs {other.name}", flush=True)
        subprocess.run(
            [sys.executable, str(SCRIPT), "--compare", str(control), str(other)], check=True
        )


if __name__ == "__main__":
    main()
