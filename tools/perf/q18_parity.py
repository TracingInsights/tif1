"""Parity of the Q18 per-lap decode against the generic loop and the control tree.

Usage:
    uv run python tools/perf/q18_parity.py --db DB --year Y [--control-pickle OUT.pkl]
    uv run python tools/perf/q18_parity.py --db DB --year Y --src-dir CONTROL_SRC --dump OUT.pkl

With ``--dump`` the frames decoded by ``--src-dir`` are pickled to OUT.pkl;
without it the current tree's fast route is compared to its generic loop and,
when given, to the control dump.
"""

from __future__ import annotations

import argparse
import pickle
import sqlite3
import sys
from itertools import pairwise
from pathlib import Path


def _decode_all(db: str, year: int, gp: str, session: str) -> dict:
    from tif1.cache import _decompress_frame_blob
    from tif1.core_utils.frame_codec import decode_driver_frames
    from tif1.models import Telemetry

    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT driver, frame FROM telemetry_frames_drv WHERE year=? AND gp=? AND session=?",
        (year, gp, session),
    ).fetchall()
    conn.close()
    out = {}
    for driver, blob in rows:
        for lap, frame in decode_driver_frames(
            bytearray(_decompress_frame_blob(blob)), None, Telemetry
        ).items():
            out[(driver, lap)] = frame
    return out


def _block_sig(frame) -> list:
    sig = []
    for block in frame._mgr.blocks:
        values = block.values
        extra = sorted(vars(values)) if hasattr(values, "__dict__") else None
        sig.append(
            (
                type(block).__name__,
                tuple(block.mgr_locs.as_array.tolist()),
                type(values).__name__,
                str(values.dtype),
                extra,
            )
        )
    return sig


def _same(a, b) -> str | None:
    if type(a) is not type(b):
        return "type"
    if list(a.columns) != list(b.columns):
        return "columns"
    if not a.dtypes.equals(b.dtypes):
        return "dtypes"
    if not a.equals(b):
        return "values"
    if not a.index.equals(b.index) or type(a.index) is not type(b.index):
        return "index"
    if _block_sig(a) != _block_sig(b):
        return "blocks"
    for name in getattr(type(a), "_metadata", ()):
        if getattr(a, name, None) != getattr(b, name, None):
            return "metadata"
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--db", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--gp", default="Monaco%20Grand%20Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--dump", default=None)
    parser.add_argument("--control-pickle", default=None)
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    if args.dump:
        frames = _decode_all(args.db, args.year, args.gp, args.session)
        Path(args.dump).write_bytes(pickle.dumps(frames, protocol=5))
        print(f"dumped {len(frames)}")
        return
    from tif1.core_utils import frame_codec

    assert frame_codec._FAST_LAP_FRAMES
    fast = _decode_all(args.db, args.year, args.gp, args.session)
    frame_codec._FAST_LAP_FRAMES = False
    generic = _decode_all(args.db, args.year, args.gp, args.session)
    frame_codec._FAST_LAP_FRAMES = True
    bad = {k: _same(fast[k], generic[k]) for k in generic if _same(fast[k], generic.get(k))}
    print(f"fast vs generic: {len(fast)} vs {len(generic)} frames, mismatches {len(bad)}", bad)
    if args.control_pickle:
        control = pickle.loads(Path(args.control_pickle).read_bytes())
        # pickling round-trips the candidate too, so both sides compare as unpickled frames
        fast_rt = pickle.loads(pickle.dumps(fast, protocol=5))
        bad = {k: _same(fast_rt.get(k), control[k]) for k in control}
        bad = {k: v for k, v in bad.items() if v}
        live = {k: _same(fast[k], control[k]) for k in control}
        live = {k: v for k, v in live.items() if v}
        print(
            f"fast vs control: {len(fast)} vs {len(control)} frames, "
            f"pickled mismatches {len(bad)}, live mismatches {len(live)}",
            dict(list(live.items())[:5]),
        )
    # writable + mutation isolation on every frame with a str/Int64 block
    checked = 0
    for (driver, lap), frame in fast.items():
        for block in frame._mgr.blocks:
            values = block.values
            arr = getattr(values, "_data", values)
            if hasattr(arr, "flags") and not arr.flags.writeable:
                raise SystemExit(f"read-only block in {driver} {lap}")
        checked += 1
    print(f"writable: {checked} frames")
    keys = sorted(fast)
    for col in ("LapNumber", "DRS", "nGear", "DriverAhead"):
        for k1, k2 in pairwise(keys):
            if k1[0] != k2[0]:
                continue
            a, b = fast[k1], fast[k2]
            if col not in a.columns or col not in b.columns or not len(a) or not len(b):
                continue
            before_b = b[col].copy()
            before_a_last = a[col].iloc[-1]
            a.loc[len(a) - 1, col] = None
            a.loc[0, col] = None
            if not b[col].equals(before_b):
                raise SystemExit(f"mutation leaked {col} {k1}->{k2}")
            del before_a_last
            break
    print("mutation isolation ok")


if __name__ == "__main__":
    main()
