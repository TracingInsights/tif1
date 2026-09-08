"""F10 prototype: thread-parallel per-lap frame assembly on real payloads.

The post-fetch assembly loop is serial today. Threads can't beat the GIL for
pure-Python work, but pandas construction spends time in C — measure whether
2/4/8 worker threads beat the serial loop on this 2-vCPU host. Parity of the
assembled maps (keys, columns, dtypes) is checked too.
"""

from __future__ import annotations

import pickle
import time
from concurrent.futures import ThreadPoolExecutor

from tif1.core_utils.helpers import _create_telemetry_df

N_THREADS = (2, 4, 8)


def serial(entries):
    out = {}
    for driver, lap, tel in entries:
        df = _create_telemetry_df(tel, driver, lap, "pandas")
        if df is not None and not df.empty:
            out[(driver, lap)] = df
    return out


def parallel(entries, workers: int):
    with ThreadPoolExecutor(max_workers=workers) as pool:
        frames = pool.map(
            lambda e: _create_telemetry_df(e[2], e[0], e[1], "pandas"), entries, chunksize=8
        )
        out = {}
        for (driver, lap, _tel), df in zip(entries, frames):
            if df is not None and not df.empty:
                out[(driver, lap)] = df
        return out


def bench(fn, entries, repeat=3):
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn(entries)
        best = min(best, time.perf_counter() - t0)
    return best


def main() -> None:
    with open("/tmp/monaco_tel_payloads.pkl", "rb") as f:
        entries = pickle.load(f)
    print(f"{len(entries)} payloads")

    base = serial(entries)
    t_serial = bench(serial, entries)
    print(f"serial: {t_serial:.3f}s")

    for w in N_THREADS:
        out = parallel(entries, w)
        assert set(out) == set(base)
        for k in base:
            assert list(base[k].columns) == list(out[k].columns)
            for col in base[k].columns:
                assert str(base[k][col].dtype) == str(out[k][col].dtype), (k, col)
        t = bench(lambda e, w=w: parallel(e, w), entries)
        print(f"parallel({w} threads): {t:.3f}s ({t_serial / t:.2f}x)")


if __name__ == "__main__":
    main()
