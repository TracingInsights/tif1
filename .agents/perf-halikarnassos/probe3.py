"""Probe DataFrame construction variants, to_timedelta alternatives, laps profile."""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

import numpy as np  # noqa: E402
import orjson  # noqa: E402
import pandas as pd  # noqa: E402
import zstandard  # noqa: E402


def timed(fn, repeat: int = 5) -> float:
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best


def build_frame_dicts() -> tuple[list, list]:
    conn = sqlite3.connect("/tmp/perf-v1/cache.sqlite")
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=2026 AND gp='Monaco%20Grand%20Prix' AND session='Race' LIMIT 200"
    ).fetchall()
    from tif1.core_utils.constants import TELEMETRY_RENAME_MAP

    list_dicts = []
    nd_dicts = []
    for _d, _lap, blob in rows:
        if isinstance(blob, bytes) and blob[:4] == b"\x28\xb5\x2f\xfd":
            blob = zstandard.ZstdDecompressor().decompress(blob)  # noqa: PLW2901
        tel = orjson.loads(blob)
        col_data = {}
        for k, v in tel.items():
            if not isinstance(v, list):
                continue
            col_data[TELEMETRY_RENAME_MAP.get(k, k)] = v
        n = max(len(v) for v in col_data.values())
        normalized = {
            k: (v + [None] * (n - len(v))) if len(v) < n else v for k, v in col_data.items()
        }

        fd_lists: dict[str, object] = {}
        fd_nd: dict[str, object] = {}
        for k, v in normalized.items():
            if k == "Time":
                td = pd.to_timedelta(np.asarray(v, dtype="float64"), unit="s")
                fd_lists[k] = td
                fd_nd[k] = td
            elif k == "Brake" and None not in v:
                arr = np.asarray(v, dtype=bool)
                fd_lists[k] = arr
                fd_nd[k] = arr
            elif k in ("nGear", "DRS"):
                arr = np.asarray(v)
                if arr.dtype.kind in "iu":
                    ia = pd.arrays.IntegerArray(arr, np.zeros(len(arr), dtype=bool))
                else:
                    ia = pd.array(v, dtype="Int64")
                fd_lists[k] = ia
                fd_nd[k] = ia
            else:
                fd_lists[k] = v
                try:
                    conv = np.asarray(v, dtype="float64")
                except (TypeError, ValueError):
                    conv = v
                fd_nd[k] = conv
        driver_arr = np.full(n, "VER", dtype=object)
        fd_lists["Driver"] = pd.Series(driver_arr, dtype=object)
        fd_lists["LapNumber"] = pd.array([1] * n, dtype="Int64")
        fd_nd["Driver"] = pd.Series(driver_arr, dtype=object)
        fd_nd["LapNumber"] = pd.arrays.IntegerArray(
            np.full(n, 1, dtype="int64"), np.zeros(n, dtype=bool)
        )
        list_dicts.append(fd_lists)
        nd_dicts.append(fd_nd)
    return list_dicts, nd_dicts


def main() -> None:
    list_dicts, nd_dicts = build_frame_dicts()
    n = len(list_dicts)
    print(f"frames prepared: {n}")

    def construct_lists():
        for fd in list_dicts:
            pd.DataFrame(fd, copy=False)

    def construct_nd():
        for fd in nd_dicts:
            pd.DataFrame(fd, copy=False)

    t_lists = timed(construct_lists)
    t_nd = timed(construct_nd)
    print(
        f"construct from lists={t_lists * 1e6 / n:.0f}us vs preconverted ndarrays={t_nd * 1e6 / n:.0f}us"
    )

    a = pd.DataFrame(list_dicts[0], copy=False)
    b = pd.DataFrame(nd_dicts[0], copy=False)
    parity = list(a.columns) == list(b.columns) and a.dtypes.equals(b.dtypes)
    if parity:
        for c in a.columns:
            if not (a[c].equals(b[c])):
                parity = False
                break
    print(f"construct parity: {parity}")
    if not parity:
        print("  dtypes a:", dict(a.dtypes.astype(str)))
        print("  dtypes b:", dict(b.dtypes.astype(str)))

    # to_timedelta alternatives on real Time lists
    conn = sqlite3.connect("/tmp/perf-v1/cache.sqlite")
    rows = conn.execute(
        "SELECT data FROM telemetry_cache WHERE year=2026 AND gp='Monaco%20Grand%20Prix' AND session='Race' LIMIT 200"
    ).fetchall()
    time_lists = []
    for (blob,) in rows:
        if isinstance(blob, bytes) and blob[:4] == b"\x28\xb5\x2f\xfd":
            blob = zstandard.ZstdDecompressor().decompress(blob)  # noqa: PLW2901
        tel = orjson.loads(blob)
        time_lists.append(tel["time"])

    def via_to_timedelta():
        for v in time_lists:
            pd.to_timedelta(np.asarray(v, dtype="float64"), unit="s")

    def via_tdindex():
        for v in time_lists:
            pd.TimedeltaIndex(np.asarray(v, dtype="float64") * 1e9)

    t1 = timed(via_to_timedelta)
    t2 = timed(via_tdindex)
    print(
        f"to_timedelta={t1 * 1e6 / len(time_lists):.0f}us vs TimedeltaIndex={t2 * 1e6 / len(time_lists):.0f}us"
    )
    vals_ok = True
    for v in time_lists:
        x = pd.to_timedelta(np.asarray(v, dtype="float64"), unit="s")
        y = pd.TimedeltaIndex(np.asarray(v, dtype="float64") * 1e9)
        if not (np.asarray(x._values) == np.asarray(y._values)).all():
            vals_ok = False
            break
    print(f"timedelta value+dtype parity: {vals_ok}")

    # Laps warm profile
    os.environ["TIF1_CACHE_DIR"] = "/tmp/perf-v1"
    import cProfile
    import io
    import pstats

    import tif1

    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    pr = cProfile.Profile()
    pr.enable()
    laps = session.laps
    pr.disable()
    _ = len(laps)
    out = io.StringIO()
    stats = pstats.Stats(pr, stream=out).sort_stats("cumulative")
    stats.print_stats(22)
    print(out.getvalue())


if __name__ == "__main__":
    main()
