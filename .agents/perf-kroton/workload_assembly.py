"""KR-2 workload: payload tier -> all 1452 telemetry frames (pure assembly)."""

import os
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

import orjson

from tif1.cache import _decode_sqlite_value, get_cache
from tif1.core_utils.helpers import _create_telemetry_df


def main():
    c = get_cache()
    rows = c.conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=2026"
        " AND gp='Monaco%20Grand%20Prix' AND session='Race'"
    ).fetchall()
    payloads = []
    for driver, lap, blob in rows:
        data = orjson.loads(_decode_sqlite_value(blob))
        payloads.append((driver, lap, data.get("tel", data)))

    t0 = time.perf_counter()
    n = 0
    for driver, lap, tel in payloads:
        if _create_telemetry_df(tel, driver, lap, "pandas") is not None:
            n += 1
    dt = time.perf_counter() - t0
    print(f"ASSEMBLY_MS {dt * 1000:.1f} frames={n}")


if __name__ == "__main__":
    main()
