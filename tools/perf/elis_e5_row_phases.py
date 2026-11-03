"""Elis E5 probe: per-row cost of the worker-side halves (blob read, lz4, widening), serial.

Reads every pandas driver row of the 2025 Bahrain race on the calling
thread and times the three halves of ``_read_frame_row`` separately, plus
the number of widening ops per row (each a few numpy calls under the GIL).

Usage (from the repo root, warm cache):
    TIF1_CACHE_DIR=/tmp/tif1-elis-warm-E5 PYTHONPATH=src python tools/perf/elis_e5_row_phases.py
"""

from __future__ import annotations

import statistics
import time

from tif1 import cache as cache_mod
from tif1.core_utils import frame_narrow

KEY = (2025, "Bahrain Grand Prix", "Race")


def main() -> None:
    import tif1

    session = tif1.get_session(*KEY)
    cache = cache_mod.get_cache()
    codec = cache_mod._pa_lz4_codec()
    conn = cache.conn
    table = cache_mod._PANDAS_DRIVER_TABLE
    rows = conn.execute(
        f"SELECT driver, frame FROM {table} WHERE year = ? AND gp = ? AND session = ?",
        (session.year, session.gp, session.session),
    ).fetchall()
    lz4_ms, expand_ms, ops = [], [], []
    for _driver, blob in rows:
        plan, inner = frame_narrow.split_row(blob)
        t0 = time.perf_counter()
        buffer = codec.decompress(
            inner, decompressed_size=plan[0], memory_pool=cache_mod._PA_LZ4_POOL[0]
        )
        t1 = time.perf_counter()
        frame_narrow.expand(plan, buffer)
        t2 = time.perf_counter()
        lz4_ms.append((t1 - t0) * 1000)
        expand_ms.append((t2 - t1) * 1000)
        ops.append(len(plan[3]))
    print(f"rows {len(rows)}, compressed {sum(len(b) for _, b in rows) / 1e6:.1f} MB")
    print(f"lz4    median {statistics.median(lz4_ms):.2f} ms  sum {sum(lz4_ms):.1f}")
    print(f"expand median {statistics.median(expand_ms):.2f} ms  sum {sum(expand_ms):.1f}")
    print(f"ops per row median {statistics.median(ops)}  max {max(ops)}")


if __name__ == "__main__":
    main()
