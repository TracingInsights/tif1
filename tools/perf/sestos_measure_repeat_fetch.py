"""L3 measurement: repeat fetch_all_laps_telemetry() calls in one process.

Warm persistent cache, fresh process. Times call 1 (cold-in-process) and
call 2 (repeat) of session.fetch_all_laps_telemetry(), plus parity of the
returned maps (same keys; frame contents equal via pandas .equals sampling).
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

WARM_CACHE_DIR = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM_CACHE_DIR

if len(sys.argv) > 1:
    sys.path.insert(0, str(Path(sys.argv[1]).resolve() / "src"))

import tif1  # noqa: E402


def main() -> None:
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    _ = session.laps

    t0 = time.perf_counter()
    tel1 = session.fetch_all_laps_telemetry()
    t1 = time.perf_counter() - t0

    t0 = time.perf_counter()
    tel2 = session.fetch_all_laps_telemetry()
    t2 = time.perf_counter() - t0

    same_keys = set(tel1) == set(tel2)
    sampled = list(tel2.items())[:40]
    parity = all(tel1[k].equals(v) for k, v in sampled)

    import pandas as pd

    print(
        pd.Series(
            {
                "call1_s": round(t1, 3),
                "call2_s": round(t2, 3),
                "frames1": len(tel1),
                "frames2": len(tel2),
                "same_keys": same_keys,
                "parity_sampled": parity,
            }
        ).to_json()
    )


if __name__ == "__main__":
    main()
