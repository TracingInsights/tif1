"""Naxos N6 probe: the cost of decoding one pandas driver row (``DriverFrameSource``).

Reads every driver's decompressed frame-tier row once, then times the decode
and its pieces (the ``str`` and object column decodes) per row.

Usage (from the tree root, offline, warm cache):
    TIF1_CACHE_DIR=/tmp/naxos/warm-N6 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/naxos_n6_decode.py
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict


def main() -> None:
    import numpy as np

    import tif1
    from tif1.cache import get_cache
    from tif1.core_utils import frame_codec

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    laps = session.laps
    drivers = [str(d) for d in laps["Driver"].unique()]
    cache = get_cache()
    rows = cache.get_telemetry_frame_rows(session.year, session.gp, session.session, drivers)
    kinds: Counter[str] = Counter()
    piece_times: dict[str, float] = defaultdict(float)
    orig_str = frame_codec._decode_str
    orig_obj = frame_codec._decode_object

    def timed_str(cats, codes):
        t0 = time.perf_counter()
        try:
            return orig_str(cats, codes)
        finally:
            piece_times["_decode_str"] += time.perf_counter() - t0

    def timed_obj(cats, codes):
        t0 = time.perf_counter()
        try:
            return orig_obj(cats, codes)
        finally:
            piece_times["_decode_object"] += time.perf_counter() - t0

    frame_codec._decode_str = timed_str
    frame_codec._decode_object = timed_obj
    total = 0.0
    for driver, raw in rows.items():
        header, _ = frame_codec._driver_header(memoryview(raw))
        for columns, _group_laps, _lengths, specs in header:
            for spec in specs:
                if spec[0] == "obj":
                    for col_spec in spec[2]:
                        kinds[f"obj:{col_spec[0]}:{columns[spec[1][spec[2].index(col_spec)]]}"] += 1
                elif spec[0] == "str":
                    kinds[f"str:{columns[spec[1][0]]}:ncats={len(spec[2])}"] += 1
                else:
                    kinds[spec[0]] += 1
        t0 = time.perf_counter()
        src = frame_codec.DriverFrameSource(raw, tif1.models.Telemetry)
        dt = time.perf_counter() - t0
        total += dt
        rows_n = sum(o[-1] for _c, _l, o, _p in src._groups)
        print(f"{driver}: groups={len(src._groups)} rows={rows_n} decode={dt * 1000:.2f} ms")
    print(f"total decode {total * 1000:.1f} ms")
    for key, val in piece_times.items():
        print(f"  {key}: {val * 1000:.1f} ms")
    for key, n in sorted(kinds.items()):
        print(f"  spec {key}: {n}")
    _ = np


if __name__ == "__main__":
    main()
