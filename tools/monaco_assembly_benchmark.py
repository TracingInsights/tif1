"""Offline micro-benchmark for telemetry DataFrame assembly (experiment H4).

Two modes:

- ``--dump N``: fetch N real telemetry payloads (network, once) and pickle
  them to /tmp/monaco_tel_payloads.pkl for offline iteration.
- default: load the pickle and benchmark assembly variants:

  A. current path: per-payload ``_create_telemetry_df`` loop (what
     ``fetch_all_laps_telemetry_async`` does today)
  B. merged-dict assembly + dtype pass once + iloc view splits per lap
     (pandas 3 Copy-on-Write keeps the splits cheap and safe)

Also verifies parity between A and B on the real payloads: same keys, same
columns, same dtypes, same values.
"""

from __future__ import annotations

import argparse
import os
import pickle
import tempfile
import time

import numpy as np

PICKLE = "/tmp/monaco_tel_payloads.pkl"


def dump(args: argparse.Namespace) -> None:
    import asyncio

    os.environ.setdefault("TIF1_CACHE_DIR", tempfile.mkdtemp(prefix="tif1-dump-"))
    import tif1

    session = tif1.get_session(args.year, args.gp, args.session)
    laps = session.laps
    refs: list[tuple[str, int]] = []
    for _, row in laps.iterrows():
        driver = row.get("Driver")
        lap_num = row.get("LapNumber")
        if driver is not None and lap_num is not None:
            refs.append((str(driver), int(lap_num)))
    refs = refs[: args.dump]

    from tif1.async_fetch import fetch_multiple_async

    requests = [
        (args.year, args.gp, args.session, f"{driver}/{lap}_tel.json") for driver, lap in refs
    ]

    async def run():
        return await fetch_multiple_async(
            requests, use_cache=False, write_cache=False, validate_payload=False
        )

    results = asyncio.run(run())
    entries = [
        (driver, lap, result.get("tel", {}))
        for (driver, lap), result in zip(refs, results)
        if isinstance(result, dict) and result.get("tel")
    ]
    with open(PICKLE, "wb") as f:
        pickle.dump(entries, f)
    print(f"dumped {len(entries)} payloads to {PICKLE}")


def bench() -> None:
    import pandas as pd

    from tif1.core_utils.constants import TELEMETRY_RENAME_MAP
    from tif1.core_utils.helpers import (
        _create_telemetry_df,
        _merge_telemetry_payloads,
        _telemetry_frame_from_merged,
    )

    with open(PICKLE, "rb") as f:
        entries: list[tuple[str, int, dict]] = pickle.load(f)
    print(f"loaded {len(entries)} payloads, rows={sum(len(e[2].get('Time', [])) for e in entries)}")

    def variant_a() -> dict[tuple[str, int], pd.DataFrame]:
        out: dict[tuple[str, int], pd.DataFrame] = {}
        for driver, lap, tel in entries:
            df = _create_telemetry_df(tel, driver, lap, "pandas")
            if df is not None and not df.empty:
                out[(driver, lap)] = df
        return out

    def variant_b() -> dict[tuple[str, int], pd.DataFrame]:
        merged = _merge_telemetry_payloads(entries)
        frame = _telemetry_frame_from_merged(merged)
        # Per-entry row counts mirroring _merge_telemetry_payloads semantics:
        # accepted entries contribute max(len(channel)) rows each.
        lengths = []
        accepted = []
        for driver, lap, tel in entries:
            if not isinstance(tel, dict) or not tel:
                continue
            lists = [v for v in tel.values() if isinstance(v, list)]
            if not lists:
                continue
            n_rows = max(len(v) for v in lists)
            if n_rows == 0:
                continue
            lengths.append(n_rows)
            accepted.append((driver, lap))
        bounds = np.cumsum([0, *lengths])
        out: dict[tuple[str, int], pd.DataFrame] = {}
        for (driver, lap), start, end in zip(accepted, bounds[:-1], bounds[1:]):
            out[(str(driver), int(lap))] = frame.iloc[start:end].reset_index(drop=True)
        return out

    def variant_c() -> dict[tuple[str, int], pd.DataFrame]:
        """Pre-typed constructor: canonical columns typed in the constructor."""
        out: dict[tuple[str, int], pd.DataFrame] = {}
        for driver, lap, tel in entries:
            if not isinstance(tel, dict) or not tel:
                continue
            col_data = {
                TELEMETRY_RENAME_MAP.get(k, k): v for k, v in tel.items() if isinstance(v, list)
            }
            if not col_data:
                continue
            max_len = max(len(v) for v in col_data.values())
            if max_len == 0:
                continue
            normalized = {
                k: (v + [None] * (max_len - len(v))) if len(v) < max_len else v
                for k, v in col_data.items()
            }
            frame_data: dict = {}
            ok = True
            for k, v in normalized.items():
                if k == "Time":
                    frame_data[k] = pd.to_timedelta(v, unit="s")
                elif k == "Brake" and None not in v:
                    frame_data[k] = np.asarray(v, dtype=bool)
                elif k in ("nGear", "DRS"):
                    frame_data[k] = pd.array(v, dtype="Int64")
                else:
                    frame_data[k] = v
            frame_data["Driver"] = np.full(max_len, driver, dtype=object)
            frame_data["LapNumber"] = pd.array([lap] * max_len, dtype="Int64")
            try:
                df = pd.DataFrame(frame_data, copy=False)
            except Exception:
                ok = False
            if ok:
                for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
                    if col not in df.columns:
                        df[col] = pd.NA
                if not df.empty:
                    out[(driver, lap)] = df
        return out

    # Warmup + timing (best of 5).
    for name, fn in (
        ("A current", variant_a),
        ("B merged+split", variant_b),
        ("C typed-constructor", variant_c),
    ):
        times = []
        for _ in range(5):
            t0 = time.perf_counter()
            result = fn()
            times.append(time.perf_counter() - t0)
        print(f"{name}: best={min(times):.4f}s median={sorted(times)[2]:.4f}s frames={len(result)}")

    # Parity check: B and C must both match A (current production path).
    a = variant_a()
    for other_name, other in (("B", variant_b()), ("C", variant_c())):
        assert set(a) == set(other), f"key mismatch {other_name}"
        issues = 0
        for key in a:
            fa, fb = a[key], other[key]
            if list(fa.columns) != list(fb.columns):
                issues += 1
                if issues <= 3:
                    print(
                        f"column mismatch {other_name} {key}: "
                        f"A={list(fa.columns)} {other_name}={list(fb.columns)}"
                    )
                continue
            if list(fa.dtypes) != list(fb.dtypes):
                issues += 1
                if issues <= 3:
                    print(f"dtype mismatch {other_name} {key}")
                continue
            if not fa.equals(fb):
                issues += 1
                if issues <= 3:
                    print(f"value mismatch {other_name} {key}")
        print(f"parity {other_name}: {len(a) - issues}/{len(a)} frames identical, issues={issues}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump", type=int, default=0)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    args = parser.parse_args()
    if args.dump:
        dump(args)
    else:
        bench()


if __name__ == "__main__":
    main()
