"""Naxos N10: print the layout of the pandas driver rows (header specs) of the warm race."""

from __future__ import annotations

import time

import tif1
from tif1 import cache as cache_mod
from tif1.core_utils import frame_codec

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
laps = session.laps
c = cache_mod.get_cache()
rows = c.get_telemetry_frame_rows(2025, session.gp, session.session, ["VER", "HAM"])
for driver, raw in rows.items():
    view = memoryview(raw)
    header, body_start = frame_codec._driver_header(view)
    print(driver, "bytes", len(view), "groups", len(header))
    for columns, glaps, lengths, specs in header:
        print("  columns", columns)
        print("  laps", len(glaps), "rows", sum(lengths))
        for spec in specs:
            if spec[0] == "obj":
                print(
                    "   obj",
                    spec[1],
                    [(s[0], s[1] if s[0] == "const" else len(s[1])) for s in spec[2]],
                )
            elif spec[0] == "str":
                print("   str", spec[1], "cats", len(spec[2]))
            elif spec[0] == "Int64c":
                print("   Int64c", spec[1], len(spec[2]))
            else:
                print("  ", spec[:4] if spec[0] == "np" else spec[:2])
    import pyarrow

    t0 = time.perf_counter()
    frame_codec._writable_body(raw)
    print("writable_body ms", (time.perf_counter() - t0) * 1000)
