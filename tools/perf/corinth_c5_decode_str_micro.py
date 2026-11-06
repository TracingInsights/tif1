"""Corinth C5: ``frame_codec._decode_str`` alone, no worker threads (run on 3.13 and 3.14).

PYTHONPATH=src python tools/perf/corinth_c5_decode_str_micro.py
"""

from __future__ import annotations

import sys
import time

import numpy as np

from tif1.core_utils import frame_codec


def main() -> None:
    rng = np.random.default_rng(0)
    cats = [f"{n}" for n in range(1, 21)]
    codes = rng.integers(-1, 20, size=43_000).astype("int32")
    pa = frame_codec._pa()
    stages = {"indices": [], "take": [], "wrap": [], "total": []}
    import pandas as pd

    dtype = pd.StringDtype("pyarrow", na_value=np.nan)
    for _ in range(60):
        t0 = time.perf_counter()
        indices = pa.array(codes, type=pa.int32(), mask=codes < 0)
        t1 = time.perf_counter()
        strings = pa.array(cats, type=pa.large_string()).take(indices)
        t2 = time.perf_counter()
        pd.arrays.ArrowStringArray(strings, dtype=dtype)
        t3 = time.perf_counter()
        frame_codec._decode_str(cats, codes)
        t4 = time.perf_counter()
        stages["indices"].append(t1 - t0)
        stages["take"].append(t2 - t1)
        stages["wrap"].append(t3 - t2)
        stages["total"].append(t4 - t3)
    print(sys.version.split()[0], end=" ")
    for name, vals in stages.items():
        kept = sorted(vals[5:])
        print(f"{name} med {kept[len(kept) // 2] * 1e6:7.1f} us", end="  ")
    print()


if __name__ == "__main__":
    main()
