"""Build the payload bundle of the CI performance gate (``tests/fixtures/perf/``).

The gate (``tools/perf/perf_gate.py``, ``tests/perf_gate/``) runs fresh-process
warm workloads against a cache seeded from this bundle. The bundle holds the
raw payloads only (the JSON tier rows and the per-lap telemetry rows), not a
SQLite file: the gate seeds a new cache through the ``Cache`` API of the tree
under test, so a change of the cache schema or of the frame tiers never makes
the fixture stale. The frame tiers are built by the gate's first (priming)
pass.

Contents: the 2025 Bahrain Grand Prix race and qualifying, every JSON payload
of the two sessions (laps of all drivers, drivers, weather, race control,
session info, the Ergast Stats classification) and the telemetry of a few
drivers (``TELEMETRY_DRIVERS``), ~7 MB xz-compressed.

File layout (xz): an 8-byte little-endian manifest length, the orjson
manifest (``{"version", "source", "missing": [key], "json": [[key, offset,
length]], "telemetry": [[year, gp, session, driver, lap, offset, length]]}``),
then the concatenated raw payload bytes the offsets point into. ``missing``
lists the telemetry keys of the other drivers, seeded as everywhere-missing
verdicts (the CDN 404 path) so whole-session telemetry reads skip them.

Usage (from the repository root):
    # From a cache that already holds the two sessions (any tif1 cache dir):
    python tools/perf/perf_gate_fixture.py --source-cache ~/.cache/tif1
    # Or fetch the payloads first (network) into a temporary cache:
    python tools/perf/perf_gate_fixture.py --fetch
"""

from __future__ import annotations

import argparse
import lzma
import os
import sqlite3
import struct
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import quote

import orjson

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "tests" / "fixtures" / "perf" / "payloads.bin.xz"
BUNDLE_VERSION = 1
YEAR = 2025
GP = "Bahrain Grand Prix"
#: Telemetry kept per session: the race's VER (driver_tel / car_data_loop), the
#: qualifying's pole lap (PIA, the session's fastest lap) and two more drivers
#: for the multi-driver steps. The other drivers keep laps, no telemetry.
TELEMETRY_DRIVERS: dict[str, tuple[str, ...]] = {
    "Race": ("VER",),
    "Qualifying": ("PIA", "VER", "LEC"),
}
_GP_KEY = quote(GP, safe="")
_STATS_KEYS = (
    f"stats/{YEAR}/events.json",
    f"stats/{YEAR}/bahrain-grand-prix/results.json",
    f"stats/{YEAR}/bahrain-grand-prix/quali_results.json",
)


def _decode(stored: object) -> bytes:
    from tif1.cache import _decode_sqlite_value

    value = _decode_sqlite_value(stored)
    return value if isinstance(value, bytes) else str(value).encode("utf-8")


def _collect(
    source_db: Path,
) -> tuple[list[tuple[str, bytes]], list[tuple[tuple, bytes]], list[str]]:
    conn = sqlite3.connect(f"file:{source_db.as_posix()}?mode=ro", uri=True)
    try:
        # Every other driver's telemetry key: the gate records them as
        # everywhere-missing so fetch_all_laps_telemetry() skips them offline
        # (the CDN 404 path) instead of failing one network attempt per lap.
        missing = [
            f"{year}/{gp}/{sess}/{driver}/{lap}_tel.json"
            for year, gp, sess, driver, lap in conn.execute(
                "SELECT year, gp, session, driver, lap FROM telemetry_cache "
                "WHERE year = ? AND gp = ? ORDER BY session, driver, lap",
                (YEAR, _GP_KEY),
            )
            if sess in TELEMETRY_DRIVERS and driver not in TELEMETRY_DRIVERS[sess]
        ]
        json_rows: list[tuple[str, bytes]] = []
        prefixes = tuple(f"{YEAR}/{_GP_KEY}/{s}/" for s in TELEMETRY_DRIVERS)
        for key, data in conn.execute("SELECT key, data FROM cache ORDER BY key"):
            if key.startswith(prefixes) or key in _STATS_KEYS:
                json_rows.append((key, _decode(data)))
        tel_rows: list[tuple[tuple, bytes]] = []
        for session, drivers in TELEMETRY_DRIVERS.items():
            marks = ",".join("?" * len(drivers))
            query = (
                "SELECT year, gp, session, driver, lap, data FROM telemetry_cache "
                f"WHERE year = ? AND gp = ? AND session = ? AND driver IN ({marks}) "
                "ORDER BY driver, lap"
            )
            for year, gp, sess, driver, lap, data in conn.execute(
                query, (YEAR, _GP_KEY, session, *drivers)
            ):
                tel_rows.append(((year, gp, sess, driver, lap), _decode(data)))
    finally:
        conn.close()
    found = {k for k, _ in json_rows}
    lacking = [k for k in _STATS_KEYS if k not in found]
    tel_found = {(k[2], k[3]) for k, _ in tel_rows}
    for session, drivers in TELEMETRY_DRIVERS.items():
        names = ("drivers.json", "session_laptimes.json", "weather.json", "rcm.json")
        lacking.extend(
            key for name in names if (key := f"{YEAR}/{_GP_KEY}/{session}/{name}") not in found
        )
        lacking.extend(f"telemetry {session}/{d}" for d in drivers if (session, d) not in tel_found)
    if lacking:
        raise SystemExit(f"source cache lacks: {', '.join(lacking)}")
    return json_rows, tel_rows, missing


def write_bundle(
    path: Path,
    json_rows: list[tuple[str, bytes]],
    tel_rows: list[tuple[tuple, bytes]],
    missing: list[str],
    source: dict[str, object],
) -> int:
    """Write the xz bundle; return its size in bytes."""
    blobs: list[bytes] = []
    offset = 0
    manifest: dict[str, object] = {
        "version": BUNDLE_VERSION,
        "source": source,
        "missing": missing,
    }
    json_index = []
    for key, blob in json_rows:
        json_index.append([key, offset, len(blob)])
        blobs.append(blob)
        offset += len(blob)
    tel_index = []
    for key, blob in tel_rows:
        tel_index.append([*key, offset, len(blob)])
        blobs.append(blob)
        offset += len(blob)
    manifest["json"] = json_index
    manifest["telemetry"] = tel_index
    head = orjson.dumps(manifest)
    raw = struct.pack("<Q", len(head)) + head + b"".join(blobs)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(lzma.compress(raw, preset=9 | lzma.PRESET_EXTREME))
    return path.stat().st_size


def _fetch_into(cache_dir: Path) -> None:
    """Load the two sessions online into ``cache_dir`` (a child process, clean config)."""
    code = (
        "import tif1\n"
        f"for name, drivers in {dict(TELEMETRY_DRIVERS)!r}.items():\n"
        f"    s = tif1.get_session({YEAR}, {GP!r}, name)\n"
        "    s.load(laps=True, telemetry=False, weather=True, messages=True)\n"
        "    s.results; s.t0_date; s.track_status; s.session_status\n"
        "    for d in drivers:\n"
        "        s.laps.pick_drivers(d).telemetry\n"
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith("TIF1_")}
    env["TIF1_CACHE_DIR"] = str(cache_dir)
    subprocess.run([sys.executable, "-c", code], env=env, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--source-cache", type=Path, help="a tif1 cache dir holding both sessions")
    group.add_argument("--fetch", action="store_true", help="fetch the payloads (network)")
    parser.add_argument("--out", type=Path, default=BUNDLE)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="tif1-perf-fixture-") as tmp:
        cache_dir = Path(tmp) if args.fetch else args.source_cache.expanduser()
        if args.fetch:
            _fetch_into(cache_dir)
        json_rows, tel_rows, missing = _collect(cache_dir / "cache.sqlite")
        raw_bytes = sum(len(b) for _, b in json_rows) + sum(len(b) for _, b in tel_rows)
        source = {"year": YEAR, "gp": GP, "telemetry_drivers": TELEMETRY_DRIVERS}
        size = write_bundle(args.out, json_rows, tel_rows, missing, source)
    print(
        f"{args.out}: {len(json_rows)} JSON payloads, {len(tel_rows)} telemetry laps, "
        f"{len(missing)} missing-telemetry verdicts, {raw_bytes / 1e6:.1f} MB raw -> "
        f"{size / 1e6:.2f} MB"
    )


if __name__ == "__main__":
    main()
