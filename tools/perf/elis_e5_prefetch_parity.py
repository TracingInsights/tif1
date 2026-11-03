"""Elis E5 parity: every lap frame built through the reads ahead equals the eager build.

Session A: ``load()``, VER then LEC (the second driver starts the reads
ahead), then every lap's telemetry in reverse laps order and the merged
telemetry of four drivers. Session B (fresh ``Session``, no ``load()``):
``fetch_all_laps_telemetry()`` and the same merged frames. Every frame must
match exactly (values, dtypes, index, class, block layout).

Usage (from the repo root, warm cache):
    TIF1_CACHE_DIR=/tmp/tif1-elis-warm-E5 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/elis_e5_prefetch_parity.py
"""

from __future__ import annotations

import pandas as pd

import tif1
from tif1 import cache as cache_mod

ARGS = (2025, "Bahrain Grand Prix", "Race")
MERGED = ("VER", "LEC", "NOR", "PIA")


def _check(left: pd.DataFrame, right: pd.DataFrame, what: str) -> None:
    assert type(left) is type(right), (what, type(left), type(right))
    assert list(left.columns) == list(right.columns), what
    assert left.dtypes.equals(right.dtypes), what
    pd.testing.assert_frame_equal(left, right, check_exact=True, obj=what)
    blocks = sorted(type(b).__name__ for b in left._mgr.blocks)
    assert blocks == sorted(type(b).__name__ for b in right._mgr.blocks), what


def main() -> None:
    starts: list[list[str]] = []
    real = cache_mod.Cache.prefetch_telemetry_frame_rows

    def spy(self, year, gp, session, drivers):
        starts.append(list(drivers))
        return real(self, year, gp, session, drivers)

    cache_mod.Cache.prefetch_telemetry_frame_rows = spy
    a = tif1.get_session(*ARGS)
    a.load()
    laps_a = a.laps
    first = {
        d: laps_a.pick_driver(d).iloc[0].telemetry for d in ("VER", "LEC")
    }  # LEC is not the driver after VER in the laps' order
    assert len(starts) == 1, starts
    built = {}
    for idx in reversed(range(len(laps_a))):
        lap = laps_a.iloc[idx]
        built[(lap["Driver"], int(lap["LapNumber"]))] = lap.telemetry
    merged_a = {d: laps_a.pick_driver(d).telemetry for d in MERGED}

    b = tif1.get_session(*ARGS)
    eager = b.fetch_all_laps_telemetry()
    merged_b = {d: b.laps.pick_driver(d).telemetry for d in MERGED}
    assert set(eager) == set(built), set(eager) ^ set(built)
    for ref, frame in eager.items():
        _check(built[ref], frame, f"lap {ref}")
    for d, frame in first.items():
        _check(frame, eager[(d, 1)], f"first {d}")
    for d in MERGED:
        _check(merged_a[d], merged_b[d], f"merged {d}")
    print(f"reads ahead started once for {len(starts[0])} drivers")
    print(f"{len(eager)} lap frames and {len(MERGED)} merged frames identical")


if __name__ == "__main__":
    main()
