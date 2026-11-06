"""Naxos N8: cold (empty cache, live CDN) parity of the frames a one-driver wave builds.

Each tree runs one fresh process with an empty cache: ``laps``, VER's and
LEC's ``pick_driver(...).telemetry`` (two bridged waves) and
``get_fastest_lap_tel()``. Every frame is digested (class, columns, dtypes,
index head, a hash of all values) and the digests of the trees are compared.

Usage:
    python tools/perf/naxos_n8_cold_parity.py --lib polars \
        --src /tmp/naxos/control/src --src src
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def _digest(frame) -> dict:
    cls = type(frame).__name__
    if hasattr(frame, "hash_rows"):  # polars
        values = str(frame.hash_rows(seed=0).to_list())
        flags = {c: str(frame[c].flags) for c in frame.columns}
        return {
            "class": cls,
            "columns": frame.columns,
            "dtypes": [str(d) for d in frame.dtypes],
            "height": frame.height,
            "values": hashlib.sha256(values.encode()).hexdigest(),
            "flags": flags,
        }
    import pandas as pd

    values = pd.util.hash_pandas_object(frame, index=True).to_numpy().tobytes()
    return {
        "class": cls,
        "columns": [str(c) for c in frame.columns],
        "dtypes": [str(d) for d in frame.dtypes],
        "index": [str(i) for i in frame.index[:5]],
        "len": len(frame),
        "values": hashlib.sha256(values).hexdigest(),
    }


def _child(lib: str) -> None:
    import tif1

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=lib)
    laps = session.laps
    out = {
        "laps": _digest(laps),
        "VER": _digest(laps.pick_driver("VER").telemetry),
        "LEC": _digest(laps.pick_driver("LEC").telemetry),
        "fastest": _digest(session.get_fastest_lap_tel()),
        "niquests_imported": "niquests" in sys.modules,
    }
    print(json.dumps(out))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--src", action="append", default=[])
    args = parser.parse_args()
    if args.child:
        _child(args.lib)
        return
    digests = []
    for src in args.src:
        cache_dir = tempfile.mkdtemp(prefix="tif1-n8-parity-")
        env = dict(os.environ, TIF1_CACHE_DIR=cache_dir, PYTHONPATH=str(Path(src).resolve()))
        env.pop("TIF1_OFFLINE_MODE", None)
        proc = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--child", "--lib", args.lib],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        shutil.rmtree(cache_dir, ignore_errors=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr[-2000:])
        digests.append(json.loads(proc.stdout.strip().splitlines()[-1]))
    base = digests[0]
    for src, other in zip(args.src[1:], digests[1:], strict=True):
        frames = [k for k in base if k != "niquests_imported"]
        differing = [k for k in frames if base[k] != other[k]]
        print(
            f"[{args.lib}] {src}: {len(differing)} differing frames of {len(frames)} {differing}"
            f" | niquests imported: control {base['niquests_imported']},"
            f" this {other['niquests_imported']}"
        )


if __name__ == "__main__":
    main()
