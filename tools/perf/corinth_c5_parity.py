"""Corinth C5: parity digests on Python 3.13 and 3.14, control and candidate trees.

Writes ``/tmp/corinth/C5_parity_<arm>_<py>_<lib>.json`` with
``delphi_parity.py`` (offline, the C5 warm cache) and compares: 3.13
candidate vs the round's control digests, 3.14 candidate vs a 3.14 control
digest, 3.14 control vs the 3.13 control digest (the interpreter's own effect).

    python tools/perf/corinth_c5_parity.py
"""

from __future__ import annotations

import os
import subprocess

PARITY = "/home/harish/Documents/tif1/tools/perf/delphi_parity.py"
PYTHONS = {
    "313": "/home/harish/Documents/tif1/.venv/bin/python",
    "314": "/tmp/corinth/venv314/bin/python",
}
SRCS = {"control": "/tmp/corinth/control/src", "candidate": "/tmp/corinth/C5/src"}


def _digest(arm: str, py: str, lib: str) -> str:
    out = f"/tmp/corinth/C5_parity_{arm}_{py}_{lib}.json"
    env = dict(
        os.environ,
        PYTHONPATH=SRCS[arm],
        TIF1_CACHE_DIR="/tmp/corinth/warm-C5",
        TIF1_OFFLINE_MODE="true",
        HOME="/tmp/corinth/home-C5",
    )
    subprocess.run([PYTHONS[py], PARITY, "--lib", lib, "--out", out], env=env, check=True)
    return out


def _compare(a: str, b: str) -> None:
    print(f"--- {a} vs {b}", flush=True)
    subprocess.run([PYTHONS["313"], PARITY, "--compare", a, b], check=False)


def main() -> None:
    for lib in ("pandas", "polars"):
        cand313 = _digest("candidate", "313", lib)
        _compare(f"/tmp/corinth/parity_{lib}.json", cand313)
        ctl314 = _digest("control", "314", lib)
        cand314 = _digest("candidate", "314", lib)
        _compare(ctl314, cand314)
        _compare(f"/tmp/corinth/parity_{lib}.json", ctl314)


if __name__ == "__main__":
    main()
