"""Q16 parity: control tree vs candidate tree frames (warm notebook + cold-in-loop path).

Usage:
    uv run python tools/q16_parity.py --control-src /tmp/tif1-pergamon-control/src \
        --cache-master /tmp/tif1-q-Q16/ctl_cache_master.sqlite [--lib polars] \
        [--candidate-in-loop] [--offline]

Child mode (``--dump OUT``) loads the session and pickles every frame.
"""

from __future__ import annotations

import argparse
import os
import pickle
import shutil
import subprocess
import sys
from pathlib import Path


def dump(args: argparse.Namespace) -> None:
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    os.environ["TIF1_CACHE_DIR"] = args.cache_dir
    if args.offline:
        os.environ["TIF1_OFFLINE_MODE"] = "true"

    def cell() -> dict:
        import tif1

        s = tif1.get_session(2026, "Monaco Grand Prix", "Race", lib=args.lib)
        s.load()
        out = {"laps": s.laps, "weather": s.weather, "rcm": s.race_control_messages}
        for key, frame in s.fetch_all_laps_telemetry().items():
            out[("tel", *key)] = frame
        if args.lib == "pandas":
            out["results"] = s.results
            for drv in ("LEC", "VER", "SAI"):
                out[("drv", drv)] = s.laps.pick_drivers(drv).telemetry
            out["fastest"] = s.laps.pick_fastest().get_telemetry()
        return out

    if args.in_loop:
        import asyncio

        async def kernel() -> dict:
            return cell()

        out = asyncio.run(kernel())
    else:
        out = cell()
    plain = {}
    for k, v in out.items():
        if hasattr(v, "_mgr"):
            c = v.copy(deep=False)
            for name in getattr(c, "_metadata", ()):
                object.__setattr__(c, name, None)
            plain[k] = (v.__class__.__name__, c, v._mgr.nblocks)
        else:
            plain[k] = (v.__class__.__name__, v, None)
    Path(args.dump).write_bytes(pickle.dumps(plain))


def same(a, b) -> bool:
    if hasattr(a, "_mgr"):
        return (
            list(a.columns) == list(b.columns)
            and list(map(str, a.dtypes)) == list(map(str, b.dtypes))
            and a.equals(b)
        )
    return a.schema == b.schema and a.equals(b)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dump")
    p.add_argument("--src-dir")
    p.add_argument("--cache-dir")
    p.add_argument("--control-src")
    p.add_argument("--cache-master")
    p.add_argument("--lib", default="pandas")
    p.add_argument("--in-loop", action="store_true")
    p.add_argument("--candidate-in-loop", action="store_true")
    p.add_argument("--offline", action="store_true")
    args = p.parse_args()
    if args.dump:
        dump(args)
        return
    work = Path("/tmp/tif1-q-Q16/parity")
    results = {}
    for arm in ("control", "candidate"):
        cache_dir = work / arm
        shutil.rmtree(cache_dir, ignore_errors=True)
        cache_dir.mkdir(parents=True)
        shutil.copy(args.cache_master, cache_dir / "cache.sqlite")
        out = work / f"{arm}.pkl"
        cmd = [sys.executable, __file__, "--dump", str(out), "--cache-dir", str(cache_dir)]
        cmd += ["--lib", args.lib]
        if arm == "control":
            cmd += ["--src-dir", args.control_src]
        elif args.candidate_in_loop:
            cmd += ["--in-loop"]
        if args.offline:
            cmd += ["--offline"]
        subprocess.run(cmd, check=True, capture_output=True)
        results[arm] = pickle.loads(out.read_bytes())
    ctl, cand = results["control"], results["candidate"]
    assert ctl.keys() == cand.keys(), (len(ctl), len(cand))
    bad = []
    for k in ctl:
        (cn, a, ab), (dn, b, bb) = ctl[k], cand[k]
        if cn != dn or ab != bb or not same(a, b):
            bad.append(k)
    n_tel = sum(1 for k in ctl if isinstance(k, tuple) and k[0] == "tel")
    print(
        f"lib={args.lib} keys={len(ctl)} telemetry_frames={n_tel} mismatches={len(bad)} {bad[:5]}"
    )


if __name__ == "__main__":
    main()
