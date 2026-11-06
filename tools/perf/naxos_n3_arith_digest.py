"""Naxos N3: digest of the polars telemetry arithmetic over many laps (control vs candidate).

Runs every ``PolarsTelemetry`` method N3 rewrote (``add_distance``,
``add_differential_distance``, ``add_relative_distance``,
``resample_channels`` at several rules, ``fill_missing``, ``slice_by_time``
with and without padding and edge interpolation, ``slice_by_lap``,
``merge_channels``, ``get_first_non_zero_time_index``) on the laps of several
drivers of the 2025 Bahrain race and qualifying, on copies with null/NaN gaps
and duplicated timestamps injected, and on one driver's whole-race telemetry,
then writes one hash per result (dtype + ``repr`` of every value). Run it with
each tree on ``PYTHONPATH`` against the same warm cache and compare the files:

    TIF1_CACHE_DIR=... TIF1_OFFLINE_MODE=true PYTHONPATH=<tree>/src \
        python tools/perf/naxos_n3_arith_digest.py --out a.json
    python tools/perf/naxos_n3_arith_digest.py --compare a.json b.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
from pathlib import Path
from typing import Any

DRIVERS = ("VER", "LEC", "NOR", "PIA", "HAM", "RUS")
RULES = ("0.1s", "1s", "50ms", "250ms", "2s")


def _digest(value: Any) -> Any:
    import polars as pl

    if isinstance(value, pl.Series):
        value = value.to_frame()
    if isinstance(value, pl.DataFrame):
        cols = {}
        for name in value.columns:
            series = value.get_column(name)
            text = repr(series.to_list()).encode()
            cols[name] = [str(series.dtype), hashlib.blake2b(text, digest_size=10).hexdigest()]
        return {"shape": list(value.shape), "class": type(value).__name__, "columns": cols}
    return repr(value)


def _gapped(tel: Any, seed: int) -> Any:
    """``tel`` with nulls and NaN in some channels and a duplicated timestamp."""
    import polars as pl

    rng = random.Random(seed)
    n = tel.height
    columns = []
    for column in tel.get_columns():
        name = column.name
        if name in ("Speed", "RPM", "X", "Throttle") or name in ("nGear", "DriverAhead"):
            values = column.to_list()
            for _ in range(max(1, n // 25)):
                i = rng.randrange(n)
                values[i] = None
            if column.dtype == pl.Float64:
                for _ in range(max(1, n // 40)):
                    values[rng.randrange(n)] = float("nan")
                values[0] = None if seed % 2 else float("nan")
            column = pl.Series(name, values, dtype=column.dtype)  # noqa: PLW2901
        columns.append(column)
    frame = pl.DataFrame(columns)
    if n > 10:  # a duplicated sample (equal clocks)
        frame = pl.concat([frame.slice(0, 6), frame.slice(5, 1), frame.slice(6)])
    return tel._rewrap(frame)


def _analyse(out: dict[str, Any], key: str, tel: Any, other: Any, lap: Any) -> None:
    def put(name: str, fn: Any) -> None:
        try:
            out[f"{key}/{name}"] = _digest(fn())
        except Exception as exc:  # errors are part of the contract
            out[f"{key}/{name}"] = f"error {type(exc).__name__}: {exc}"

    put("dist", lambda: tel.add_distance())
    put("diff", lambda: tel.add_differential_distance())
    put("rel", lambda: tel.add_relative_distance())
    put("rel_nodist", lambda: tel.drop("Distance").add_relative_distance())
    put("integrate", lambda: tel.integrate_distance())
    put("first_nz", lambda: tel.get_first_non_zero_time_index())
    put("fill", lambda: tel.fill_missing())
    for rule in RULES:
        put(f"resample_{rule}", lambda rule=rule: tel.resample_channels(rule=rule))
    if tel.height:
        times = tel.get_column("Time")
        a, b = times[len(times) // 5], times[(4 * len(times)) // 5]
        put("slice", lambda: tel.slice_by_time(a, b))
        put("slice_pad", lambda: tel.slice_by_time(a, b, pad=3, pad_side="both"))
        put("slice_pad_after", lambda: tel.slice_by_time(a, b, pad=2, pad_side="after"))
        put("slice_edges", lambda: tel.slice_by_time(a, b, interpolate_edges=True))
        put("slice_edges_pad", lambda: tel.slice_by_time(a, b, pad=1, interpolate_edges=True))
    if lap is not None:
        put("slice_lap", lambda: tel.slice_by_lap(lap))
        put("slice_lap_edges", lambda: tel.slice_by_lap(lap, interpolate_edges=True))
    if other is not None:
        put("merge", lambda: tel.merge_channels(other))
        put("merge_10hz", lambda: tel.merge_channels(other, frequency=10))


def build() -> dict[str, Any]:
    import tif1

    out: dict[str, Any] = {}
    for session_name in ("Race", "Qualifying"):
        session = tif1.get_session(2025, "Bahrain Grand Prix", session_name, lib="polars")
        laps = session.laps
        for driver in DRIVERS:
            driver_laps = laps.pick_driver(driver)
            previous = None
            for i, (_, lap) in enumerate(driver_laps.iterlaps()):
                if session_name == "Race" and i % 2:
                    continue
                number = lap.lap_number or 0
                tel = lap.get_telemetry()
                key = f"{session_name}/{driver}/{number}"
                _analyse(out, key, tel, previous, lap)
                if number % 7 == 0:
                    _analyse(out, f"{key}/gapped", _gapped(tel, number), previous, lap)
                car = lap.get_car_data()
                if number % 5 == 0:
                    _analyse(out, f"{key}/car", car, tel, None)
                previous = tel
        if session_name == "Race":
            whole = laps.pick_driver("VER").telemetry
            _analyse(out, "Race/VER/whole", whole, None, None)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out")
    parser.add_argument("--compare", nargs=2)
    parser.add_argument(
        "--preload-numpy",
        action="store_true",
        help="import numpy.ma first (the candidate's numpy kernels then run on large frames)",
    )
    args = parser.parse_args()
    if args.compare:
        a, b = (json.loads(Path(p).read_text(encoding="utf-8")) for p in args.compare)
        keys = sorted((set(a) | set(b)) - {"numpy_loaded"})
        diff = [k for k in keys if a.get(k) != b.get(k)]
        for k in diff[:40]:
            print("DIFF", k, "\n  a:", a.get(k), "\n  b:", b.get(k))
        print(f"{len(diff)} differing of {len(keys)}")
        sys.exit(1 if diff else 0)
    if args.preload_numpy:
        import numpy.ma  # noqa: F401

    result = build()
    result["numpy_loaded"] = "numpy" in sys.modules
    Path(args.out).write_text(json.dumps(result, indent=0, sort_keys=True), encoding="utf-8")
    print(f"{len(result)} entries; numpy loaded: {result['numpy_loaded']}")


if __name__ == "__main__":
    main()
