"""Naxos N2: parity of the frames a warm ``load()`` leaves behind, per access shape.

``delphi_parity.py`` never calls ``load()``, so it cannot see whether the
lazy polars load serves the frames the eager one did. This probe runs
``load()`` (defaults) and then one access shape per fresh interpreter, and
digests every frame it touches (class, shape, per-column dtype, chunk count
and values, the bound driver). Run it once per tree and compare.

Usage (from the repo root):
    python tools/perf/naxos_n2_load_parity.py --lib polars --src /tmp/naxos/control/src \
        --cache /tmp/naxos/warm-N2 --out /tmp/ctl.json
    python tools/perf/naxos_n2_load_parity.py --lib polars --src /tmp/naxos/N2/src \
        --cache /tmp/naxos/warm-N2 --out /tmp/cand.json
    python tools/perf/naxos_n2_load_parity.py --compare /tmp/ctl.json /tmp/cand.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
SHAPES = (
    "one_driver",
    "two_drivers_then_reverse_walk",
    "in_order_walk",
    "merged_x4_then_fetch_all",
    "fastest",
    "second_load",
)


def _digest(frame: Any, lib: str) -> dict[str, Any]:
    if frame is None:
        return {"none": True}
    out: dict[str, Any] = {"class": type(frame).__name__, "shape": list(frame.shape)}
    cols: dict[str, Any] = {}
    if lib == "polars":
        for name in frame.columns:
            s = frame.get_column(name)
            cols[name] = [
                str(s.dtype),
                s.n_chunks(),
                hashlib.blake2b(repr(s.to_list()).encode(), digest_size=12).hexdigest(),
            ]
    else:
        for i, name in enumerate(frame.columns):
            s = frame.iloc[:, i]
            cols[str(name)] = [
                str(s.dtype),
                hashlib.blake2b(repr(s.tolist()).encode(), digest_size=12).hexdigest(),
            ]
        out["index"] = hashlib.blake2b(repr(list(frame.index)).encode()).hexdigest()
    out["columns"] = cols
    out["driver"] = repr(getattr(frame, "driver", None))
    return out


def _child(shape: str, lib: str) -> None:
    import tif1

    session = tif1.get_session(YEAR, GP, SESSION, lib=lib)
    session.load()
    laps = session.laps
    out: dict[str, Any] = {}

    def lap_tel(lap: Any) -> Any:
        tel = lap.telemetry
        assert tel.session is session
        return tel

    def walk(rows: list[Any], tag: str) -> None:
        for _, lap in rows:
            out[f"{tag}:{lap['Driver']}:{int(lap['LapNumber'])}"] = _digest(lap_tel(lap), lib)

    if shape == "one_driver":
        lap = laps.pick_drivers("LEC").pick_fastest()
        out["LEC_fastest_car"] = _digest(lap.get_car_data(), lib)
        out["LEC_fastest_pos"] = _digest(lap.get_pos_data(), lib)
        out["LEC_fastest_tel"] = _digest(lap.get_telemetry(), lib)
        out["LEC_merged"] = _digest(laps.pick_drivers("LEC").telemetry, lib)
    elif shape == "two_drivers_then_reverse_walk":
        out["NOR_fastest_car"] = _digest(
            laps.pick_drivers("NOR").pick_fastest().get_car_data(), lib
        )
        out["HAM_fastest_car"] = _digest(
            laps.pick_drivers("HAM").pick_fastest().get_car_data(), lib
        )
        walk(list(laps.iterlaps())[::-1], "rev")
    elif shape == "in_order_walk":
        walk(list(laps.iterlaps()), "walk")
    elif shape == "merged_x4_then_fetch_all":
        for d in ("VER", "LEC", "NOR", "PIA"):
            out[f"merged:{d}"] = _digest(laps.pick_drivers(d).telemetry, lib)
        all_frames = session.fetch_all_laps_telemetry()
        out["all:order"] = [f"{driver}/{lap_number}" for driver, lap_number in all_frames]
        for (driver, lap_number), frame in all_frames.items():
            out[f"all:{driver}:{lap_number}"] = _digest(frame, lib)
    elif shape == "fastest":
        out["fastest_lap_tel"] = _digest(session.get_fastest_lap_tel(), lib)
        out["VER_fastest_lap_tel"] = _digest(session.get_driver("VER").get_fastest_lap_tel(), lib)
        out["fastest_laps_tels"] = _digest(session.get_fastest_laps_tels(by_driver=True), lib)
        out["pick_fastest_tel"] = _digest(laps.pick_fastest().telemetry, lib)
    elif shape == "second_load":
        out["PIA_merged"] = _digest(laps.pick_drivers("PIA").telemetry, lib)
        session.load()
        walk(list(laps.iterlaps())[::7], "after_reload")
    print(json.dumps(out))


def _run(src: str, cache: str, lib: str) -> dict[str, Any]:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", TIF1_CACHE_DIR=cache, PYTHONPATH=src)
    result: dict[str, Any] = {}
    for shape in SHAPES:
        cmd = [sys.executable, str(Path(__file__).resolve()), "--child", shape, "--lib", lib]
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
        if proc.returncode != 0:
            raise RuntimeError(f"{shape} failed: {proc.stderr[-3000:]}")
        result[shape] = json.loads(proc.stdout.strip().splitlines()[-1])
        print(f"{shape}: {len(result[shape])} surfaces", flush=True)
    return result


def _compare(a_path: str, b_path: str) -> None:
    a = json.loads(Path(a_path).read_text(encoding="utf-8"))
    b = json.loads(Path(b_path).read_text(encoding="utf-8"))
    total = differ = 0
    for shape in sorted(set(a) | set(b)):
        sa, sb = a.get(shape, {}), b.get(shape, {})
        for key in sorted(set(sa) | set(sb)):
            total += 1
            if sa.get(key) != sb.get(key):
                differ += 1
                if differ <= 20:
                    print(f"DIFF {shape} {key}")
    print(f"{differ} differing surfaces out of {total}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--lib", default="polars", choices=["pandas", "polars"])
    parser.add_argument("--src", default=None)
    parser.add_argument("--cache", default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--compare", nargs=2, default=None)
    args = parser.parse_args()
    if args.compare:
        _compare(*args.compare)
        return
    if args.child:
        _child(args.child, args.lib)
        return
    result = _run(args.src, args.cache, args.lib)
    Path(args.out).write_text(json.dumps(result, indent=0) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
