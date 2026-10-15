"""Fresh-process probe of the fastest-lap convenience paths on a warm cache (Q10).

Runs ONE entry point as the first telemetry call after ``get_session`` (no
``load()``) and prints one JSON line: wall time of the call, the ``Cache``
methods it touched (call counts), whether ``asyncio`` was imported, and a
digest of the result frame. ``--dump`` pickles the result for parity checks.

Usage:
    TIF1_CACHE_DIR=/tmp/tif1-q-Q10/cache uv run python tools/q10_fastest_probe.py \
        --entry session_fastest_lap_tel [--src-dir SRC] [--dump out.pkl]
"""

from __future__ import annotations

import argparse
import collections
import functools
import hashlib
import json
import pickle
import sys
import time
from pathlib import Path
from typing import Any

ENTRIES = (
    "session_fastest_lap_tel",
    "session_fastest_laps_tels",
    "session_fastest_laps_tels_3drv",
    "session_fastest_laps_tels_overall",
    "driver_fastest_lap_tel",
    "driver_fastest_lap",
    "laps_pick_fastest_tel",
    "session_fastest_laps",
)


def _instrument(cache_cls: Any, counts: collections.Counter) -> None:
    for name, attr in list(vars(cache_cls).items()):
        if not callable(attr) or name.startswith("__"):
            continue

        def _wrap(fn: Any, label: str = name) -> Any:
            @functools.wraps(fn)
            def inner(*args: Any, **kwargs: Any) -> Any:
                counts[label] += 1
                return fn(*args, **kwargs)

            return inner

        setattr(cache_cls, name, _wrap(attr))


def _digest(frame: Any) -> str:
    try:
        import pandas as pd

        if isinstance(frame, pd.DataFrame):
            h = pd.util.hash_pandas_object(frame, index=True).values.tobytes()
            dtypes = ",".join(f"{c}:{t}" for c, t in frame.dtypes.items())
            return hashlib.sha1(h + dtypes.encode()).hexdigest()[:12]
    except (TypeError, ValueError):
        pass
    return hashlib.sha1(pickle.dumps(frame)).hexdigest()[:12]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--entry", choices=ENTRIES, required=True)
    parser.add_argument("--driver", default="LEC")
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--dump", default=None)
    parser.add_argument("--no-instrument", action="store_true")
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    import tif1
    from tif1 import cache as cache_mod

    counts: collections.Counter = collections.Counter()
    if not args.no_instrument:
        _instrument(cache_mod.Cache, counts)

    session = tif1.get_session(2026, "Monaco Grand Prix", "Race", lib=args.lib)
    asyncio_before = "asyncio" in sys.modules
    t0 = time.perf_counter()
    entry = args.entry
    if entry == "session_fastest_lap_tel":
        result = session.get_fastest_lap_tel()
    elif entry == "session_fastest_laps_tels":
        result = session.get_fastest_laps_tels()
    elif entry == "session_fastest_laps_tels_3drv":
        result = session.get_fastest_laps_tels(by_driver=True, drivers=["LEC", "NOR", "VER"])
    elif entry == "session_fastest_laps_tels_overall":
        result = session.get_fastest_laps_tels(by_driver=False)
    elif entry == "driver_fastest_lap_tel":
        result = session.get_driver(args.driver).get_fastest_lap_tel()
    elif entry == "driver_fastest_lap":
        result = session.get_driver(args.driver).get_fastest_lap()
    elif entry == "laps_pick_fastest_tel":
        result = session.laps.pick_fastest().get_telemetry()
    else:
        result = session.get_fastest_laps()
    wall = time.perf_counter() - t0
    if args.dump:
        import pandas as pd

        dumped = result
        if isinstance(result, pd.DataFrame) and type(result) is not pd.DataFrame:
            # Session-bound subclasses do not pickle; keep the manager (blocks)
            # and record the subclass + driver separately.
            dumped = {
                "class": type(result).__name__,
                "driver": getattr(result, "driver", None),
                "frame": pd.DataFrame._from_mgr(result._mgr, axes=result._mgr.axes),
            }
        with open(args.dump, "wb") as fh:
            pickle.dump(dumped, fh, protocol=5)
    print(
        json.dumps(
            {
                "entry": entry,
                "wall_ms": round(wall * 1000, 3),
                "asyncio_before": asyncio_before,
                "asyncio_after": "asyncio" in sys.modules,
                "cache_calls": dict(sorted(counts.items())),
                "shape": list(getattr(result, "shape", ())),
                "type": type(result).__name__,
                "digest": _digest(result),
            }
        )
    )


if __name__ == "__main__":
    main()
