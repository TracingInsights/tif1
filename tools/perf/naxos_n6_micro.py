"""Naxos N6 probe: micro-timings of the per-lap pieces (steady state, best of 7).

Usage (from the tree root, offline, warm cache):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_micro.py
"""

from __future__ import annotations

import time


def _t(label, fn, n=1, reps=7):
    best = float("inf")
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    print(f"  {label:52s} {best * 1e6 / n:9.2f} us")


def main() -> None:
    import numpy as np

    import tif1
    from tif1 import models
    from tif1.cache import get_cache
    from tif1.core_utils import frame_codec

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    laps = session.laps
    td_blk = next(b for b in laps._mgr.blocks if b.dtype.kind == "m")
    nd = td_blk.values._ndarray
    tdnew = models._ints_to_pytimedelta
    _t("td 2-D ints_to_pytimedelta (block)", lambda: tdnew(nd, box=True))
    _t(
        "td per-column 1-D ints_to_pytimedelta",
        lambda: [tdnew(nd[j], box=True) for j in range(nd.shape[0])],
    )
    ndc = np.ascontiguousarray(nd)
    _t("td 2-D contiguous", lambda: tdnew(ndc, box=True))
    _t("to_numpy(object)", lambda: laps.to_numpy(dtype=object))
    rows = cache_rows = get_cache().get_telemetry_frame_rows(
        session.year, session.gp, session.session, ["VER"]
    )
    raw = bytearray(cache_rows["VER"])
    _t(
        "DriverFrameSource(VER)",
        lambda: frame_codec.DriverFrameSource(bytearray(raw), models.Telemetry),
    )
    src = frame_codec.DriverFrameSource(bytearray(raw), models.Telemetry)
    columns, glaps, offsets, parts = src._groups[0]
    lengths = [offsets[i + 1] - offsets[i] for i in range(len(offsets) - 1)]
    _t(
        "_LapBuilder init",
        lambda: frame_codec._LapBuilder(parts, columns, lengths, offsets[-1], models.Telemetry),
    )
    b = src._builder(0)
    nlaps = len(glaps)
    _t(
        "build per lap",
        lambda: [b.build(offsets[i], offsets[i + 1]) for i in range(nlaps)],
        n=nlaps,
    )
    _t("src.frame per lap", lambda: [src.frame(lap) for lap in glaps], n=nlaps)
    frame = src.frame(glaps[3])
    _t("adopt", lambda: session._adopt_telemetry_frame(frame, "VER"), n=1)
    _t("_session_telemetry", lambda: session._session_telemetry(frame, "VER"), n=1)
    _t("frame.columns + 'SessionTime' in", lambda: "SessionTime" in frame.columns)
    _t("_ergast_only", lambda: session._ergast_only)
    for code in ("SOFT",):
        _ = code
    print("plan:", [p[0] for p in b._plan])
    _ = rows


if __name__ == "__main__":
    main()
