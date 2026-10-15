"""Q17 probe: count CDN walks for known-missing telemetry refs on a warm cache.

Usage:
    TIF1_CACHE_DIR=<copy> uv run python tools/q17_known_missing_probe.py \
        --src-dir SRC --mode MODE [--lib pandas|polars] [--live]

Modes: single:<DRV>:<LAP> (Lap.telemetry), getlap:<DRV>:<LAP> (get_telemetry),
internal:<DRV>:<LAP> (_LapInternal), getdrv:<DRV> (Laps.telemetry), iterlaps,
iterrows (every laps row incl. rows iterlaps skips), fastest.
Without --live the sync (CDNManager.try_sources) and async
(CDNManager.try_sources_async) CDN walks are stubbed to count the call and raise
the all-404 DataNotFoundError, so no network is touched.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path


def digest(obj) -> str:
    import pandas as pd

    if isinstance(obj, BaseException):
        return f"EXC:{type(obj).__name__}"
    if isinstance(obj, pd.DataFrame):
        h = hashlib.sha1()
        h.update(repr(obj.shape).encode())
        h.update(repr(list(obj.columns)).encode())
        h.update(repr([str(d) for d in obj.dtypes]).encode())
        h.update(type(obj).__name__.encode())
        if len(obj):
            h.update(pd.util.hash_pandas_object(obj, index=True).values.tobytes())
        return h.hexdigest()[:16]
    return f"OTHER:{type(obj).__name__}"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--src-dir", required=True)
    p.add_argument("--mode", required=True)
    p.add_argument("--live", action="store_true")
    p.add_argument("--lib", default="pandas")
    a = p.parse_args()
    sys.path.insert(0, str(Path(a.src_dir).resolve()))
    import tif1
    from tif1 import cdn
    from tif1.exceptions import DataNotFoundError

    calls: list[str] = []
    if not a.live:

        def fake_sync(self, year, gp, session, path, fetch):
            calls.append(f"sync:{path}")
            raise DataNotFoundError(year=year, event=gp, session=session)

        async def fake_async(self, year, gp, session, path, fetch):
            calls.append(f"async:{path}")
            raise DataNotFoundError(year=year, event=gp, session=session)

        cdn.CDNManager.try_sources = fake_sync
        cdn.CDNManager.try_sources_async = fake_async

    session = tif1.get_session(2026, "Monaco Grand Prix", "Race", lib=a.lib)
    laps = session.laps
    out: dict = {}
    t0 = time.perf_counter()
    if a.mode.startswith("single:"):
        _, drv, lap_n = a.mode.split(":")
        lap = laps.pick_drivers(drv).pick_laps(int(lap_n)).iloc[0]
        t0 = time.perf_counter()
        try:
            res = lap.telemetry
        except Exception as e:
            res = e
        out["digest"] = digest(res)
    elif a.mode.split(":")[0] in ("getlap", "internal"):
        kind, drv, lap_n = a.mode.split(":")
        if kind == "getlap":
            lap = session.get_driver(drv).get_lap(int(lap_n))
        else:
            from tif1.core import _LapInternal

            lap = _LapInternal(session, drv, int(lap_n))
        t0 = time.perf_counter()
        try:
            res = lap.get_telemetry() if kind == "getlap" else lap.telemetry
        except Exception as e:
            res = e
        out["digest"] = digest(res)
    elif a.mode.startswith("getdrv:"):
        drv = a.mode.split(":")[1]
        dl = laps.pick_drivers(drv)
        t0 = time.perf_counter()
        try:
            res = dl.telemetry
        except Exception as e:
            res = e
        out["digest"] = digest(res)
        out["rows"] = len(res) if not isinstance(res, BaseException) else None
    elif a.mode in ("iterlaps", "iterrows"):
        t0 = time.perf_counter()
        digs = []
        it = laps.iterlaps() if a.mode == "iterlaps" else laps.iterrows()
        for _, lap in it:
            try:
                res = lap.telemetry
            except Exception as e:
                res = e
            digs.append(digest(res))
        out["n"] = len(digs)
        out["exc"] = sum(1 for d in digs if d.startswith("EXC"))
        out["empty"] = digs.count("41d57d9183fa512b")
        out["digest"] = hashlib.sha1("|".join(digs).encode()).hexdigest()[:16]
    elif a.mode == "fastest":
        t0 = time.perf_counter()
        try:
            res = session.get_fastest_lap_tel()
        except Exception as e:
            res = e
        out["digest"] = digest(res)
    wall = time.perf_counter() - t0
    out.update({"mode": a.mode, "wall_s": round(wall, 4), "walks": len(calls), "calls": calls})
    print(json.dumps(out))


if __name__ == "__main__":
    main()
