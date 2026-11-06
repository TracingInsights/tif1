"""Elis E4: serial lz4 decode speed, pyarrow's codec vs ``tif1_native.lz4_frame_decompress_into``.

Decodes every narrowed driver row of a warm cache (both 2025 Bahrain
sessions) into pre-faulted destinations, so only the decoder is timed; the
best of ``--reps`` rounds is printed. Run it with ``PYTHONPATH`` pointing at
the ``tif1_native`` build to measure::

    PYTHONPATH=/path/to/extracted/wheel:src python tools/perf/elis_e4_decode_speed.py \
        --cache /tmp/tif1-elis-warm-E4
"""

from __future__ import annotations

import argparse
import sqlite3
import time

import pyarrow as pa
import tif1_native

from tif1.core_utils import frame_narrow


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", required=True)
    parser.add_argument("--reps", type=int, default=7)
    args = parser.parse_args()
    conn = sqlite3.connect(f"file:{args.cache}/cache.sqlite?mode=ro", uri=True)
    rows = [
        frame_narrow.split_row(b) for (b,) in conn.execute("SELECT frame FROM telemetry_frames_drv")
    ]
    codec = pa.Codec("lz4")
    pool = pa.system_memory_pool()
    dsts = [bytearray(b"\x01" * plan[0]) for plan, _ in rows]  # touched: no faults timed
    shared = bytearray(b"\x01" * max(plan[0] for plan, _ in rows))
    best = dict.fromkeys(("pyarrow reused", "pyarrow kept", "native reused", "native kept"), 1e9)
    for _ in range(args.reps):
        # "reused": each output's memory is recycled by the next row (cache-warm);
        # "kept": every row lands in its own memory (what a session does).
        t0 = time.perf_counter()
        for plan, inner in rows:
            codec.decompress(inner, decompressed_size=plan[0], memory_pool=pool)
        best["pyarrow reused"] = min(best["pyarrow reused"], time.perf_counter() - t0)
        t0 = time.perf_counter()
        kept = [codec.decompress(i, decompressed_size=p[0], memory_pool=pool) for p, i in rows]
        best["pyarrow kept"] = min(best["pyarrow kept"], time.perf_counter() - t0)
        del kept
        t0 = time.perf_counter()
        for _plan, inner in rows:
            tif1_native.lz4_frame_decompress_into(inner, shared)
        best["native reused"] = min(best["native reused"], time.perf_counter() - t0)
        t0 = time.perf_counter()
        for (_plan, inner), dst in zip(rows, dsts, strict=True):
            tif1_native.lz4_frame_decompress_into(inner, dst)
        best["native kept"] = min(best["native kept"], time.perf_counter() - t0)
    total = sum(plan[0] for plan, _ in rows)
    print(
        f"{len(rows)} rows, {total / 1e6:.0f} MB: "
        + ", ".join(f"{k} {v * 1e3:.1f} ms" for k, v in best.items())
        + f" ({tif1_native.__file__})"
    )


if __name__ == "__main__":
    main()
