"""KR-4 workload: merged-dict telemetry assembly for the full corpus."""

import os
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

import orjson  # noqa: E402

from tif1.cache import _decode_sqlite_value, get_cache  # noqa: E402
from tif1.core import _merge_telemetry_payloads  # noqa: E402
from tif1.core_utils.helpers import _telemetry_frame_from_merged  # noqa: E402


def main():
    c = get_cache()
    rows = c.conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=2026"
        " AND gp='Monaco%20Grand%20Prix' AND session='Race'"
    ).fetchall()
    tels = []
    for driver, lap, blob in rows:
        data = orjson.loads(_decode_sqlite_value(blob))
        tels.append((driver, lap, data["tel"] if "tel" in data else data))

    t0 = time.perf_counter()
    merged = _merge_telemetry_payloads(tels)
    frame = _telemetry_frame_from_merged(merged)
    dt = time.perf_counter() - t0
    print(f"MERGED_MS {dt * 1000:.1f} rows={len(frame)}")


if __name__ == "__main__":
    main()
