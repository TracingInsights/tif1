"""Gortyn G6: E4's serial decode-speed probe, interleaved over tif1-native overlays.

Runs ``elis_e4_decode_speed.py`` (every narrowed driver row of a warm cache,
pyarrow's codec vs ``tif1_native.lz4_frame_decompress_into``, best of 7) in a
fresh process per arm and round, alternating the arms, each arm an unpacked
wheel put first on ``PYTHONPATH``. Usage (repo root)::

    python tools/perf/gortyn_g6_decode_speed.py --cache /tmp/tif1-gortyn-warm-G6 \
        --arm G1=/tmp/tif1-gortyn/G1/native/target/site \
        --arm G6=/tmp/tif1-gortyn/G6/native/target/site --rounds 3
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", required=True)
    parser.add_argument("--arm", action="append", required=True, help="NAME=SITE_DIR")
    parser.add_argument("--src", default=str(HERE.parents[1] / "src"))
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    arms = [item.partition("=")[::2] for item in args.arm]
    for _ in range(args.rounds):
        for name, site in arms:
            env = dict(os.environ, PYTHONPATH=os.pathsep.join([site, args.src]))
            proc = subprocess.run(
                [sys.executable, str(HERE / "elis_e4_decode_speed.py"), "--cache", args.cache],
                capture_output=True,
                text=True,
                env=env,
                check=True,
            )
            print(f"{name}: {proc.stdout.strip()}", flush=True)


if __name__ == "__main__":
    main()
