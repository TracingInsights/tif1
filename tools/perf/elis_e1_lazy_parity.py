"""Elis E1: frame-by-frame parity of a lazy ``load()`` against the eager one.

Phase 1 (cold, live CDN): two fresh processes, each on its own empty cache,
run the same flow after ``load(telemetry=True)`` and ``load(telemetry="lazy")``:
four drivers' merged telemetry, their fastest laps' car data, one lap of an
untouched driver through ``lap.telemetry``, ``get_fastest_lap_tel()`` and
then ``fetch_all_laps_telemetry()``. Every frame is dumped and compared
(class, columns, dtypes, index, values; and the order of the
``fetch_all_laps_telemetry`` keys).

Phase 2 (warm, offline): each of the two caches the cold runs wrote is read
back by a fresh process (``load()`` then ``fetch_all_laps_telemetry()`` and the
merged telemetry), so the cache and frame-tier writes of the lazy load are
checked against the eager load's too.

Usage:
    python tools/perf/elis_e1_lazy_parity.py --lib pandas [--keep]
"""

from __future__ import annotations

import argparse
import os
import pickle
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
FOUR_DRIVERS = ("VER", "LEC", "NOR", "PIA")
UNTOUCHED = "HAM"


def _plain(frame, lib: str):
    """``(class name, plain frame)``: the frame without its session binding."""
    name = type(frame).__name__
    if lib == "polars":
        import polars as pl

        return name, pl.DataFrame(frame)
    import pandas as pd

    return name, pd.DataFrame(frame, copy=True)


def _child(mode: str, lib: str, out: str) -> None:
    import tif1

    session = tif1.get_session(YEAR, GP, SESSION, lib=lib)
    dump: dict = {}
    if mode == "warm":
        session.load()
        laps = session.laps
    elif mode in ("warm-eager", "warm-lazy"):
        # A primed cache: a polars lazy load reads the frame tier per wave.
        session.load(telemetry=True if mode == "warm-eager" else "lazy")
        laps = session.laps
        dump["lap_telemetry:VER:3"] = _plain(
            list(laps.pick_driver("VER").iterlaps())[2][1].telemetry, lib
        )
        for _, lap in laps.iterlaps():
            dump[f"lap:{lap['Driver']}:{int(lap['LapNumber'])}"] = _plain(lap.telemetry, lib)
    else:
        session.load(telemetry=True if mode == "eager" else "lazy", weather=True, messages=True)
        laps = session.laps
        fastest = [laps.pick_driver(d).pick_fastest() for d in FOUR_DRIVERS]
        for d, lap in zip(FOUR_DRIVERS, fastest, strict=True):
            dump[f"fastest_car_data:{d}"] = _plain(lap.get_car_data(), lib)
            dump[f"fastest_get_telemetry:{d}"] = _plain(lap.get_telemetry(), lib)
        lap = list(laps.pick_driver(UNTOUCHED).iterlaps())[9][1]
        dump[f"lap_telemetry:{UNTOUCHED}"] = _plain(lap.telemetry, lib)
        dump["get_fastest_lap_tel"] = _plain(session.get_fastest_lap_tel(), lib)
    for d in FOUR_DRIVERS:
        dump[f"merged:{d}"] = _plain(laps.pick_driver(d).telemetry, lib)
    all_tel = session.fetch_all_laps_telemetry()
    dump["fetch_all_keys"] = list(all_tel)
    for ref, frame in all_tel.items():
        dump[f"all:{ref[0]}:{ref[1]}"] = _plain(frame, lib)
    with open(out, "wb") as fh:
        pickle.dump(dump, fh)


def _run(mode: str, lib: str, cache_dir: str, out: str, *, offline: bool) -> None:
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir)
    if offline:
        env["TIF1_OFFLINE_MODE"] = "true"
    else:
        env.pop("TIF1_OFFLINE_MODE", None)
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", mode, "--lib", lib]
    proc = subprocess.run(
        [*cmd, "--out", out], capture_output=True, text=True, env=env, check=False
    )
    if proc.returncode != 0:
        raise SystemExit(f"{mode} failed:\n{proc.stderr[-3000:]}")


def _compare(a: dict, b: dict, lib: str) -> list[str]:
    diffs: list[str] = []
    if a.keys() != b.keys():
        diffs.append(f"surface keys differ: {sorted(set(a) ^ set(b))[:10]}")
    for key in a.keys() & b.keys():
        left, right = a[key], b[key]
        if key == "fetch_all_keys":
            if left != right:
                diffs.append("fetch_all_laps_telemetry key order differs")
            continue
        if left[0] != right[0]:
            diffs.append(f"{key}: class {left[0]} != {right[0]}")
        if lib == "polars":
            if left[1].schema != right[1].schema or not left[1].equals(right[1]):
                diffs.append(f"{key}: frame differs")
        else:
            import pandas as pd

            try:
                pd.testing.assert_frame_equal(left[1], right[1], check_exact=True)
            except AssertionError as error:
                diffs.append(f"{key}: {str(error).splitlines()[0]}")
    return diffs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--out", default=None)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument(
        "--warm-cache",
        default=None,
        help="compare a warm eager and a warm lazy load on this primed cache (offline) instead",
    )
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib, args.out)
        return
    if args.warm_cache:
        work = Path(tempfile.mkdtemp(prefix="tif1-elis-e1-parity-"))
        try:
            dumps = {}
            for mode in ("warm-eager", "warm-lazy"):
                out = work / f"{mode}.pkl"
                _run(mode, args.lib, args.warm_cache, str(out), offline=True)
                with open(out, "rb") as fh:
                    dumps[mode] = pickle.load(fh)
            diffs = _compare(dumps["warm-eager"], dumps["warm-lazy"], args.lib)
            print(f"[{args.lib}] warm: {len(dumps['warm-eager'])} surfaces, {len(diffs)} differ")
            for line in diffs:
                print("  ", line)
        finally:
            shutil.rmtree(work, ignore_errors=True)
        return
    work = Path(tempfile.mkdtemp(prefix="tif1-elis-e1-parity-"))
    try:
        dumps: dict[str, dict] = {}
        for mode in ("eager", "lazy"):
            cache = work / f"cache-{mode}"
            cache.mkdir()
            out = work / f"{mode}.pkl"
            _run(mode, args.lib, str(cache), str(out), offline=False)
            warm_out = work / f"{mode}-warm.pkl"
            _run("warm", args.lib, str(cache), str(warm_out), offline=True)
            with open(out, "rb") as fh:
                dumps[mode] = pickle.load(fh)
            with open(warm_out, "rb") as fh:
                dumps[f"{mode}-warm"] = pickle.load(fh)
        cold = _compare(dumps["eager"], dumps["lazy"], args.lib)
        warm = _compare(dumps["eager-warm"], dumps["lazy-warm"], args.lib)
        print(
            f"[{args.lib}] cold: {len(dumps['eager'])} surfaces, {len(cold)} differ; "
            f"warm read-back: {len(dumps['eager-warm'])} surfaces, {len(warm)} differ"
        )
        for line in cold + warm:
            print("  ", line)
    finally:
        if args.keep:
            print("kept", work)
        else:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
