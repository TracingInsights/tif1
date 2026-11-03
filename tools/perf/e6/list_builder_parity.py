"""E6 parity probe: numpy-free list telemetry builder vs the numpy builders.

For every cached telemetry payload of a session (SQLite payload tier), build
the polars frame three ways and compare them: the list twin on orjson lists,
the numpy typed builder on the same lists, and the full ``_create_telemetry_df``
on the native-decoded payload. Usage:
    python tools/perf/e6/list_builder_parity.py CACHE_DIR [Race|Qualifying]
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import orjson
from polars.testing import assert_frame_equal

from tif1.cache import _decode_sqlite_value
from tif1.core_utils import helpers
from tif1.core_utils.json_utils import decode_telemetry_payload

helpers._ensure_polars_available()
cache_dir = Path(sys.argv[1])
session = sys.argv[2] if len(sys.argv) > 2 else "Race"
conn = sqlite3.connect((cache_dir / "cache.sqlite").as_posix())
rows = conn.execute(
    "SELECT driver, lap, data FROM telemetry_cache WHERE year = 2025 AND session = ?", (session,)
).fetchall()
print(f"{len(rows)} payloads")
clock = (3_600_000_000_000, 1_744_000_000_000_000_000)
checked = unhandled = 0
for driver, lap, data in rows:
    raw = _decode_sqlite_value(data)
    if isinstance(raw, str):
        raw = raw.encode()
    lists = orjson.loads(raw)
    native = decode_telemetry_payload(raw)
    col_lists: dict = {}
    col_native: dict = {}
    for src, dst in ((lists, col_lists), (native, col_native)):
        for k, v in src.items():
            if isinstance(v, helpers._telemetry_channel_types()):
                dst[helpers.TELEMETRY_RENAME_MAP.get(k, k)] = v
    ahead = col_lists.get("DriverAhead")
    if ahead is not None:
        col_lists["DriverAhead"] = helpers._without_null_tokens(ahead)
    for clk in (None, clock):
        twin = helpers._polars_list_telemetry_frame(dict(col_lists), driver, lap, clk)
        if twin is helpers._UNHANDLED:
            unhandled += 1
            continue
        ref = helpers._polars_typed_telemetry_frame(dict(col_lists), driver, lap, clk)
        assert ref is not None, (driver, lap)
        assert_frame_equal(twin, ref, check_exact=True)
        assert twin.schema == ref.schema
        assert twin.columns == ref.columns
        full = helpers._create_telemetry_df(dict(native), driver, lap, "polars", clock=clk)
        assert_frame_equal(twin, full, check_exact=True)
        checked += 1
print(f"checked {checked} frames equal, {unhandled} unhandled (numpy builder answers)")
