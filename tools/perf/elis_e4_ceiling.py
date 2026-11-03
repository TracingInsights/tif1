"""Elis E4 ceiling probe: page faults of the frame-tier decompression.

Reads the 20 pandas driver rows of the 2025 Bahrain race from a warm cache
and decompresses + widens them on 8 threads, in a fresh process per run:

- ``pyarrow``: today's path (``cache._decompress_row``: pyarrow's lz4 codec,
  system pool, fresh memory per row).
- ``mmap``: liblz4's frame decoder (ctypes, GIL released) writing into an
  anonymous ``mmap`` sized to the row (rounded up to 2 MB), no madvise.
- ``huge``: the same with ``madvise(MADV_HUGEPAGE)`` before the first write.
- ``huge_prefault``: ``huge`` with ``MADV_POPULATE_WRITE`` (pre-fault) first.

Usage (from the repo root)::

    python tools/perf/elis_e4_ceiling.py --cache /tmp/tif1-elis-warm-E4 --runs 8

Prints, per mode, the median wall of the threaded decompression, the minor
faults and the system/user CPU time of the process during it.
"""

from __future__ import annotations

import argparse
import ctypes
import mmap
import os
import resource
import sqlite3
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

HUGE = 2 * 1024 * 1024
MODES = ("pyarrow", "mmap", "huge", "huge_prefault", "native_mmap", "native_huge")


def _rows(cache_dir: str) -> list[tuple[str, bytes]]:
    conn = sqlite3.connect(f"file:{cache_dir}/cache.sqlite?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT driver, frame FROM telemetry_frames_drv WHERE year = 2025 "
        "AND gp = 'Bahrain%20Grand%20Prix' AND session = 'Race'"
    ).fetchall()
    conn.close()
    return rows


class _Lz4F:
    def __init__(self) -> None:
        lib = ctypes.CDLL("liblz4.so.1")
        lib.LZ4F_createDecompressionContext.argtypes = [
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.c_uint,
        ]
        lib.LZ4F_decompress.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.c_void_p,
        ]
        lib.LZ4F_decompress.restype = ctypes.c_size_t
        lib.LZ4F_isError.argtypes = [ctypes.c_size_t]
        lib.LZ4F_isError.restype = ctypes.c_uint
        lib.LZ4F_freeDecompressionContext.argtypes = [ctypes.c_void_p]
        self.lib = lib

    def decompress_into(self, src: bytes, offset: int, dst_addr: int, dst_len: int) -> int:
        lib = self.lib
        dctx = ctypes.c_void_p()
        lib.LZ4F_createDecompressionContext(ctypes.byref(dctx), 100)
        src_addr = ctypes.cast(ctypes.c_char_p(src), ctypes.c_void_p).value + offset
        src_left = len(src) - offset
        written = 0
        try:
            while True:
                dsize = ctypes.c_size_t(dst_len - written)
                ssize = ctypes.c_size_t(src_left)
                ret = lib.LZ4F_decompress(
                    dctx,
                    dst_addr + written,
                    ctypes.byref(dsize),
                    src_addr,
                    ctypes.byref(ssize),
                    None,
                )
                if lib.LZ4F_isError(ret):
                    raise ValueError("lz4 error")
                written += dsize.value
                src_addr += ssize.value
                src_left -= ssize.value
                if ret == 0:
                    return written
                if ssize.value == 0 and dsize.value == 0:
                    raise ValueError("lz4 stalled")
        finally:
            lib.LZ4F_freeDecompressionContext(dctx)


def _child(mode: str, cache_dir: str) -> None:
    import pandas as pd  # noqa: F401  - same process state as a pandas session

    from tif1 import cache as tcache
    from tif1.core_utils import frame_narrow

    rows = _rows(cache_dir)
    codec = tcache._pa_lz4_codec()
    lz4f = _Lz4F()

    def via_mmap(row: tuple[str, bytes]) -> object:
        _driver, blob = row
        plan, inner = frame_narrow.split_row(blob)
        size = plan[0]
        # MAP_PRIVATE: the default MAP_SHARED anonymous map is shmem, whose
        # huge pages follow shmem_enabled (never), not the madvise setting.
        mm = mmap.mmap(-1, -(-size // HUGE) * HUGE, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
        if "huge" in mode:
            mm.madvise(mmap.MADV_HUGEPAGE)
        if mode == "huge_prefault":
            mm.madvise(23)  # MADV_POPULATE_WRITE (Linux 5.14+)
        view = memoryview(mm)[:size]
        if mode.startswith("native"):
            import tif1_native

            tif1_native.lz4_frame_decompress_into(inner, view)
        else:
            anchor = ctypes.c_char.from_buffer(mm)
            lz4f.decompress_into(blob, len(blob) - len(inner), ctypes.addressof(anchor), size)
            del anchor
        frame_narrow.expand(plan, view)
        return view

    work = (lambda row: tcache._decompress_row(row, codec)) if mode == "pyarrow" else via_mmap
    pool = ThreadPoolExecutor(8)
    list(pool.map(lambda _: None, range(8)))  # threads up
    r0 = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.perf_counter()
    out = list(pool.map(work, rows))
    wall = (time.perf_counter() - t0) * 1e3
    r1 = resource.getrusage(resource.RUSAGE_SELF)
    total = sum(len(memoryview(o[-1] if isinstance(o, tuple) else o)) for o in out)
    print(
        f"{wall:.2f} {r1.ru_minflt - r0.ru_minflt} {(r1.ru_stime - r0.ru_stime) * 1e3:.1f} "
        f"{(r1.ru_utime - r0.ru_utime) * 1e3:.1f} {total}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", required=True)
    parser.add_argument("--runs", type=int, default=8)
    parser.add_argument("--child")
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.cache)
        return
    results: dict[str, list[list[float]]] = {m: [] for m in MODES}
    for _ in range(args.runs):
        for mode in MODES:
            out = subprocess.run(
                [sys.executable, __file__, "--cache", args.cache, "--child", mode],
                capture_output=True,
                text=True,
                check=True,
                env=os.environ,
            ).stdout.split()
            results[mode].append([float(x) for x in out])
    print(f"{'mode':15} {'wall ms':>8} {'faults':>8} {'sys ms':>8} {'user ms':>8} bytes")
    for mode, runs in results.items():
        med = [statistics.median(col) for col in zip(*runs, strict=True)]
        print(f"{mode:15} {med[0]:8.1f} {med[1]:8.0f} {med[2]:8.1f} {med[3]:8.1f} {med[4]:.0f}")


if __name__ == "__main__":
    main()
