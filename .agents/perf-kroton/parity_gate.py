"""Kroton parity gate: build all 1452 telemetry frames via the CURRENT
`_create_telemetry_df` and pickle them; `--check` mode compares a later run
against the snapshot. Usage:
  snapshot: uv run python .agents/perf-kroton/parity_gate.py snapshot
  check:    uv run python .agents/perf-kroton/parity_gate.py check
"""

from __future__ import annotations

import os
import pickle
import sys

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

SNAP = "/tmp/kroton-parity-baseline.pkl"

import orjson  # noqa: E402
import pandas as pd  # noqa: E402

from tif1.cache import _decode_sqlite_value, get_cache  # noqa: E402
from tif1.core_utils.helpers import _create_telemetry_df  # noqa: E402


def get_payloads():
    c = get_cache()
    rows = c.conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=2026"
        " AND gp='Monaco%20Grand%20Prix' AND session='Race'"
    ).fetchall()
    out = []
    for driver, lap, blob in rows:
        data = orjson.loads(_decode_sqlite_value(blob))
        tel = data["tel"] if isinstance(data, dict) and "tel" in data else data
        out.append((driver, lap, tel))
    return out


def frames_identical(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    if list(a.columns) != list(b.columns):
        return False
    if not a.dtypes.equals(b.dtypes):
        return False
    if len(a) != len(b):
        return False
    return all(a[c].equals(b[c]) for c in a.columns)


def main():
    payloads = get_payloads()
    mode = sys.argv[1] if len(sys.argv) > 1 else "check"
    if mode == "snapshot":
        frames = {}
        for driver, lap, tel in payloads:
            frame = _create_telemetry_df(tel, driver, lap, "pandas")
            if frame is not None:
                frames[(driver, lap)] = (
                    frame.columns.tolist(),
                    frame.dtypes.astype(str).tolist(),
                    frame,
                )
        with open(SNAP, "wb") as f:
            pickle.dump([(k, c, dt, fr) for k, (c, dt, fr) in frames.items()], f)
        print(f"snapshot: {len(frames)} frames pickled to {SNAP}")
        return

    with open(SNAP, "rb") as f:
        snap = pickle.load(f)
    mismatch = 0
    checked = 0
    for (driver, lap), _cols, _dtypes, ref in snap:
        src = next((t for d, lap_n, t in payloads if d == driver and lap_n == lap), None)
        if src is None:
            continue
        frame = _create_telemetry_df(src, driver, lap, "pandas")
        checked += 1
        if frame is None or not frames_identical(frame, ref):
            mismatch += 1
            if mismatch <= 3:
                print(f"MISMATCH {driver} lap {lap}")
    print(f"parity check: {checked - mismatch}/{checked} frames identical ({mismatch} mismatches)")
    sys.exit(1 if mismatch else 0)


if __name__ == "__main__":
    main()
