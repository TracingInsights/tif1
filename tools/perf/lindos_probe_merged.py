"""N-series probe: merged-assembly + slice variants vs per-frame construction.

Measures on real cached Monaco telemetry payloads:
- column-set uniformity across the 1452 payloads
- per-frame construction (baseline, what cold/warm#1 pay)
- merged single-pass construction + slicing per ref (parity-checked)
- one merged-frame unpickle + slices vs 1452 per-frame unpickles (frame tier)
- msgspec / simdjson parse vs orjson
- zstd level -3 vs level 1 compress
"""

from __future__ import annotations

import os
import pickle
import sqlite3
import time

WARM = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM

import msgspec  # noqa: E402
import numpy as np  # noqa: E402
import orjson  # noqa: E402
import pandas as pd  # noqa: E402
import zstandard  # noqa: E402

import tif1  # noqa: E402
from tif1.core_utils.helpers import (  # noqa: E402
    _create_telemetry_df,
    _merge_telemetry_payloads,
    _telemetry_frame_from_merged,
)


def t() -> float:
    return time.perf_counter()


def frames_equal(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    if list(a.columns) != list(b.columns):
        return False
    if list(a.dtypes) != list(b.dtypes):
        return False
    if len(a) != len(b):
        return False
    for col in a.columns:
        av, bv = a[col], b[col]
        if av.dtype == object:
            if av.tolist() != bv.tolist():
                return False
        else:
            an, bn = av.to_numpy(), bv.to_numpy()
            if av.dtype.kind in "mM":
                if not (an == bn).all():
                    return False
            elif not np.array_equal(an, bn, equal_nan=True):
                return False
    return True


def main() -> None:
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    _ = session.laps
    conn = sqlite3.connect(f"{WARM}/cache.sqlite")
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=? AND gp=? AND session=?",
        (session.year, session.gp, session.session),
    ).fetchall()
    dec = zstandard.ZstdDecompressor()
    raws = [(r["driver"], int(r["lap"]), dec.decompress(r["data"])) for r in rows]
    print(f"payloads: {len(raws)}, {sum(len(b) for _, _, b in raws) / 1e6:.1f} MB")

    # ---- parse probes
    for name, fn in [
        ("orjson", lambda: [orjson.loads(b) for _, _, b in raws]),
        ("msgspec", lambda: [msgspec.json.decode(b) for _, _, b in raws]),
    ]:
        best = 1e9
        for _ in range(3):
            t0 = t()
            fn()
            best = min(best, t() - t0)
        print(f"parse {name}: {best:.4f}s")

    parsed = [(drv, lp, orjson.loads(b)) for drv, lp, b in raws]

    # ---- column-set uniformity
    col_sets: dict[tuple, int] = {}
    for _, _, pl in parsed:
        cols = tuple(sorted(k for k, v in pl.items() if isinstance(v, list)))
        col_sets[cols] = col_sets.get(cols, 0) + 1
    print(f"distinct list-column sets across payloads: {len(col_sets)}")
    for cols, n in sorted(col_sets.items(), key=lambda kv: -kv[1])[:5]:
        print(f"  {n} payloads x {len(cols)} cols")

    # ---- baseline per-frame assembly
    t0 = t()
    base_frames: dict[tuple, pd.DataFrame] = {}
    for drv, lp, pl in parsed:
        fr = _create_telemetry_df(pl, drv, lp, "pandas")
        if fr is not None and not fr.empty:
            base_frames[(drv, lp)] = fr
    per_frame_s = t() - t0
    print(f"per-frame assembly: {per_frame_s:.4f}s ({len(base_frames)} frames)")

    # ---- merged + slice
    t0 = t()
    tels = [(drv, lp, pl) for drv, lp, pl in parsed if isinstance(pl, dict) and pl]
    merged = _telemetry_frame_from_merged(_merge_telemetry_payloads(tels))
    merge_s = t() - t0
    print(f"merged single-pass assembly: {merge_s:.4f}s rows={len(merged)}")

    # slice per ref
    t0 = t()
    bounds: dict[tuple, tuple[int, int]] = {}
    start = 0
    for drv, lp, pl in tels:
        cols = [v for v in pl.values() if isinstance(v, list)]
        n = max((len(v) for v in cols), default=0)
        if n:
            bounds[(drv, lp)] = (start, start + n)
            start += n
    slices: dict[tuple, pd.DataFrame] = {
        ref: merged.iloc[a:b].reset_index(drop=True) for ref, (a, b) in bounds.items()
    }
    slice_s = t() - t0
    print(f"slice per ref: {slice_s:.4f}s ({len(slices)} refs)")

    # parity
    mismatches = 0
    for ref, fr in base_frames.items():
        sl = slices.get(ref)
        if sl is None or not frames_equal(fr, sl):
            mismatches += 1
            if mismatches <= 3:
                print(
                    "  MISMATCH",
                    ref,
                    list(fr.columns) == (list(sl.columns) if sl is not None else None),
                )
    print(f"parity mismatches (per-frame vs merged+slice): {mismatches}/{len(base_frames)}")

    # ---- frame tier: one merged blob vs per-frame blobs
    comp = zstandard.ZstdCompressor(level=1)
    merged_tbl_bytes = comp.compress(pickle.dumps(merged, protocol=5))
    t0 = t()
    m2 = pickle.loads(dec.decompress(merged_tbl_bytes))
    unmerge_s = t() - t0
    print(f"unpickle merged frame: {unmerge_s:.4f}s ({len(merged_tbl_bytes) / 1e6:.1f} MB)")

    t0 = t()
    _back2 = {ref: m2.iloc[a:b].reset_index(drop=True) for ref, (a, b) in bounds.items()}
    print(f"slice merged-after-unpickle: {t() - t0:.4f}s")

    t0 = t()
    per_blobs = [comp.compress(pickle.dumps(fr, protocol=5)) for fr in base_frames.values()]
    _unp = [pickle.loads(dec.decompress(b)) for b in per_blobs]
    print(
        f"per-frame pickle roundtrip: {t() - t0:.4f}s ({sum(len(b) for b in per_blobs) / 1e6:.1f} MB)"
    )

    # ---- zstd level -3 vs 1 on telemetry payload bytes
    raw_bytes = [b for _, _, b in raws]
    for lvl in (1, -3):
        c = zstandard.ZstdCompressor(level=lvl)
        t0 = t()
        out = [c.compress(b) for b in raw_bytes]
        print(f"zstd level {lvl}: {t() - t0:.4f}s total={sum(len(o) for o in out) / 1e6:.1f} MB")

    # ---- parse-dict->construction split for merged path (per driver grouping)
    t0 = t()
    by_driver: dict[str, list] = {}
    for drv, lp, pl in tels:
        by_driver.setdefault(drv, []).append((drv, lp, pl))
    print(f"group by driver: {t() - t0:.4f}s ({len(by_driver)} drivers)")
    t0 = t()
    driver_frames = sum(
        1
        for _ in (
            _telemetry_frame_from_merged(_merge_telemetry_payloads(entries))
            for entries in by_driver.values()
        )
    )
    print(f"per-driver merged assembly ({driver_frames} frames): {t() - t0:.4f}s")


if __name__ == "__main__":
    main()
