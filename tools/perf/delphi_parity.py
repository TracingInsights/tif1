"""Delphi round: parity digest of the user-visible surfaces of a session (both backends).

Writes one JSON file of per-surface digests (shape, column dtypes and a hash
of every column's values) for the 2025 Bahrain race and qualifying from a
warm cache, offline. Two digests (control tree, candidate tree) are then
compared surface by surface.

Usage:
    PYTHONPATH=/tmp/tif1-delphi-control/src TIF1_CACHE_DIR=/tmp/tif1-delphi-warm \
        TIF1_OFFLINE_MODE=true python tools/perf/delphi_parity.py --lib pandas --out /tmp/ctl.json
    PYTHONPATH=/tmp/tif1-delphi/D4/src TIF1_CACHE_DIR=/tmp/tif1-delphi-warm-D4 \
        TIF1_OFFLINE_MODE=true python tools/perf/delphi_parity.py --lib pandas --out /tmp/cand.json
    python tools/perf/delphi_parity.py --compare /tmp/ctl.json /tmp/cand.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from typing import Any

YEAR, GP = 2025, "Bahrain Grand Prix"


def _hash_values(values: Any) -> str:
    h = hashlib.blake2b(digest_size=12)
    try:
        import numpy as np

        arr = np.asarray(values)
        if arr.dtype == object:
            h.update(repr(list(arr)).encode())
        else:
            h.update(arr.tobytes())
    except Exception:
        h.update(repr(values).encode())
    return h.hexdigest()


def _frame_digest(frame: Any, lib: str) -> dict[str, Any]:
    if frame is None:
        return {"none": True}
    if lib == "polars":
        import polars as pl

        if isinstance(frame, pl.Series):
            frame = frame.to_frame()
        if not hasattr(frame, "columns"):  # PolarsLap / PolarsDriverResult rows
            if hasattr(frame, "to_frame"):
                frame = frame.to_frame()
            elif hasattr(frame, "to_dict"):
                return {"row": _hash_values(repr(frame.to_dict()))}
            else:
                return {"row": _hash_values(repr(frame))}
        cols: dict[str, Any] = {}
        for name in frame.columns:
            s = frame.get_column(name)
            try:
                vals = s.to_list()
            except Exception:
                vals = s.to_numpy()
            cols[name] = [
                str(s.dtype),
                hashlib.blake2b(repr(vals).encode(), digest_size=12).hexdigest(),
            ]
        return {"shape": list(frame.shape), "columns": cols, "class": type(frame).__name__}
    import pandas as pd

    if isinstance(frame, pd.Series):
        frame = frame.to_frame().T if frame.index.dtype == object else frame.to_frame()
    cols = {}
    for i, name in enumerate(frame.columns):
        s = frame.iloc[:, i]
        try:
            raw = s.to_numpy()
        except Exception:
            raw = list(s)
        if getattr(raw, "dtype", None) is not None and raw.dtype == object:
            raw = [
                None
                if (
                    x is None
                    or (isinstance(x, float) and math.isnan(x))
                    or x is pd.NaT
                    or x is pd.NA
                )
                else x
                for x in raw
            ]
            digest = hashlib.blake2b(repr(raw).encode(), digest_size=12).hexdigest()
        else:
            digest = _hash_values(raw)
        cols[str(name)] = [str(s.dtype), digest]
    return {
        "shape": list(frame.shape),
        "index": _hash_values(list(frame.index)),
        "columns": cols,
        "class": type(frame).__name__,
    }


def _scalar(value: Any) -> str:
    return f"{type(value).__name__}:{value!r}"


def build(lib: str) -> dict[str, Any]:
    import tif1

    out: dict[str, Any] = {}
    for sess_name in ("Race", "Qualifying"):
        s = tif1.get_session(YEAR, GP, sess_name, lib=lib)
        key = sess_name
        laps = s.laps
        out[f"{key}/laps"] = _frame_digest(laps, lib)
        out[f"{key}/results"] = _frame_digest(s.results, lib)
        out[f"{key}/drivers"] = list(s.drivers)
        out[f"{key}/weather"] = _frame_digest(s.weather_data, lib)
        out[f"{key}/rcm"] = _frame_digest(s.race_control_messages, lib)
        out[f"{key}/track_status"] = _frame_digest(s.track_status, lib)
        out[f"{key}/session_status"] = _frame_digest(s.session_status, lib)
        out[f"{key}/t0_date"] = _scalar(s.t0_date)
        out[f"{key}/session_start_time"] = _scalar(s.session_start_time)
        out[f"{key}/total_laps"] = _scalar(s.total_laps)
        out[f"{key}/fastest_lap_tel"] = _frame_digest(s.get_fastest_lap_tel(), lib)
        for d in ("VER", "LEC", "NOR"):
            out[f"{key}/{d}/driver_tel"] = _frame_digest(laps.pick_driver(d).telemetry, lib)
            drv = s.get_driver(d)
            out[f"{key}/{d}/driver_fastest_lap"] = _frame_digest(drv.get_fastest_lap(), lib)
            out[f"{key}/{d}/driver_fastest_tel"] = _frame_digest(drv.get_fastest_lap_tel(), lib)
            out[f"{key}/{d}/driver_laps"] = _frame_digest(drv.laps, lib)
            out[f"{key}/{d}/driver_row"] = _frame_digest(
                drv if lib == "pandas" else drv.to_frame() if hasattr(drv, "to_frame") else None,
                lib,
            )
        out[f"{key}/pick_quicklaps"] = _frame_digest(laps.pick_quicklaps(), lib)
        out[f"{key}/pick_fastest"] = _frame_digest(laps.pick_fastest(), lib)
        out[f"{key}/pick_compounds"] = _frame_digest(laps.pick_compounds(["SOFT"]), lib)
        out[f"{key}/pick_wo_box"] = _frame_digest(laps.pick_wo_box(), lib)
        out[f"{key}/pick_track_status"] = _frame_digest(laps.pick_track_status("1"), lib)
        if sess_name == "Qualifying":
            q1, q2, q3 = laps.split_qualifying_sessions()
            out[f"{key}/split"] = [_frame_digest(q, lib) for q in (q1, q2, q3)]
        fastest = []
        # Sorted: pandas unique() keeps appearance order, polars unique() a per-process
        # hash order, so the analysis laps were not reproducible on polars.
        drivers = laps["Driver"].unique()
        for d in sorted(drivers.tolist() if lib == "pandas" else drivers.to_list()):
            lap = laps.pick_driver(d).pick_fastest()
            if lap is not None:
                fastest.append(lap)
            if len(fastest) == 4:
                break
        for i, lap in enumerate(fastest):
            tel = lap.get_telemetry()
            out[f"{key}/analysis{i}/tel"] = _frame_digest(tel, lib)
            tel = tel.add_distance().add_differential_distance()
            out[f"{key}/analysis{i}/dist"] = _frame_digest(tel, lib)
            out[f"{key}/analysis{i}/resample"] = _frame_digest(
                tel.resample_channels(rule="0.1s"), lib
            )
            out[f"{key}/analysis{i}/resample1s"] = _frame_digest(tel.resample_channels(), lib)
            out[f"{key}/analysis{i}/fill"] = _frame_digest(tel.fill_missing(), lib)
            out[f"{key}/analysis{i}/car"] = _frame_digest(lap.get_car_data(), lib)
            out[f"{key}/analysis{i}/pos"] = _frame_digest(lap.get_pos_data(), lib)
            out[f"{key}/analysis{i}/car_pad"] = _frame_digest(
                lap.get_car_data(pad=1, pad_side="both"), lib
            )
        if lib == "pandas":
            loop = [lap.get_car_data() for _, lap in laps.pick_driver("VER").iterlaps()]
            out[f"{key}/VER/car_loop"] = [_frame_digest(f, lib) for f in loop]
        allt = s.fetch_all_laps_telemetry()
        out[f"{key}/all_tel/count"] = len(allt)
        out[f"{key}/all_tel/frames"] = {
            f"{k[0]}/{k[1]}": _frame_digest(v, lib)
            for k, v in sorted(allt.items(), key=lambda kv: (str(kv[0][0]), int(kv[0][1])))
        }
    return out


def compare(a: dict[str, Any], b: dict[str, Any]) -> int:
    diffs = 0
    keys = sorted(set(a) | set(b))
    for k in keys:
        if k not in a or k not in b:
            print(
                f"MISSING {k}: control={'yes' if k in a else 'no'} candidate={'yes' if k in b else 'no'}"
            )
            diffs += 1
            continue
        if a[k] != b[k]:
            diffs += 1
            va, vb = a[k], b[k]
            if (
                isinstance(va, dict)
                and isinstance(vb, dict)
                and "columns" in va
                and "columns" in vb
            ):
                if va.get("shape") != vb.get("shape"):
                    print(f"DIFF {k}: shape {va.get('shape')} -> {vb.get('shape')}")
                if va.get("index") != vb.get("index"):
                    print(f"DIFF {k}: index differs")
                if va.get("class") != vb.get("class"):
                    print(f"DIFF {k}: class {va.get('class')} -> {vb.get('class')}")
                for c in sorted(set(va["columns"]) | set(vb["columns"])):
                    if va["columns"].get(c) != vb["columns"].get(c):
                        print(
                            f"DIFF {k}: column {c}: {va['columns'].get(c)} -> {vb['columns'].get(c)}"
                        )
            elif isinstance(va, dict) and isinstance(vb, dict):
                sub = compare(va, vb)
                diffs += sub - 1
            else:
                print(f"DIFF {k}: {str(va)[:120]} -> {str(vb)[:120]}")
    print(f"{diffs} differing surfaces out of {len(keys)}")
    return diffs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--out", default=None)
    parser.add_argument("--compare", nargs=2, default=None)
    args = parser.parse_args()
    if args.compare:
        with (
            open(args.compare[0], encoding="utf-8") as fa,
            open(args.compare[1], encoding="utf-8") as fb,
        ):
            a, b = json.load(fa), json.load(fb)
        sys.exit(1 if compare(a, b) else 0)
    digest = build(args.lib)
    text = json.dumps(digest, indent=0, sort_keys=True)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    print(f"{len(digest)} surfaces written" + (f" to {args.out}" if args.out else ""))


if __name__ == "__main__":
    main()
