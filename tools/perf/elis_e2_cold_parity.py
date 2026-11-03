"""Elis E2: cold-session parity (empty cache, live CDN) between two trees.

``--dump OUT`` (child, run with the tree on PYTHONPATH): load a cold session
and pickle its laps, fastest-lap telemetry, a driver's telemetry and results,
plus whether niquests was imported. ``--compare A B``: frame-by-frame
comparison (values, dtypes, index, columns).
"""

from __future__ import annotations

import argparse
import os
import pickle
import shutil
import sys
import tempfile


def dump(out: str, lib: str) -> None:
    cache_dir = tempfile.mkdtemp(prefix="tif1-e2-parity-")
    os.environ["TIF1_CACHE_DIR"] = cache_dir
    os.environ.pop("TIF1_OFFLINE_MODE", None)
    try:
        import tif1

        session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=lib)
        frames = {"laps": session.laps, "fastest_lap_tel": session.get_fastest_lap_tel()}
        frames["niquests_after_fastest"] = "niquests" in sys.modules
        frames["results"] = session.results
        frames["ver_tel"] = session.laps.pick_drivers("VER").telemetry
        frames["drivers"] = list(session.drivers)
        plain = {}
        for key, value in frames.items():
            if hasattr(value, "shape"):
                import pandas as pd

                plain[f"{key}.class"] = type(value).__name__
                plain[key] = pd.DataFrame(value.to_pandas() if lib == "polars" else value)
            else:
                plain[key] = value
        frames = plain
        with open(out, "wb") as fh:
            pickle.dump(frames, fh)
    finally:
        shutil.rmtree(cache_dir, ignore_errors=True)


def compare(a: str, b: str) -> int:
    import pandas as pd

    with open(a, "rb") as fh:
        left = pickle.load(fh)
    with open(b, "rb") as fh:
        right = pickle.load(fh)
    bad = 0
    for key in left:
        x, y = left[key], right[key]
        if isinstance(x, pd.DataFrame):
            same = (
                x.equals(y)
                and list(x.columns) == list(y.columns)
                and list(x.dtypes) == list(y.dtypes)
                and x.index.equals(y.index)
                and type(x).__name__ == type(y).__name__
            )
        else:
            same = x == y
        detail = "" if same or hasattr(x, "shape") else f"  {x!r} vs {y!r}"
        print(f"{key:24s} {'same' if same else 'DIFFERENT'}{detail}")
        bad += 0 if same or key == "niquests_after_fastest" else 1
    print(f"{bad} differing frames")
    return bad


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump")
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--compare", nargs=2)
    args = parser.parse_args()
    if args.dump:
        dump(args.dump, args.lib)
    else:
        sys.exit(1 if compare(*args.compare) else 0)


if __name__ == "__main__":
    main()
