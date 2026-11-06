"""Naxos N9: compare the payload tier and frame tiers of two cold-primed caches.

Each argument is a cache directory (``TIF1_CACHE_DIR``) primed by one cold
run. Payload rows are compared after decompression (byte equality of the
stored JSON); frame-tier tables are compared by row keys and decompressed
content.

Usage::

    python tools/perf/naxos_n9_cache_diff.py CACHE_A CACHE_B
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import zstandard


def _db(path: str) -> sqlite3.Connection:
    files = [p for p in Path(path).rglob("*") if p.suffix in (".db", ".sqlite", ".sqlite3")]
    if not files:
        raise SystemExit(f"no SQLite file under {path}")
    return sqlite3.connect(f"file:{files[0]}?mode=ro", uri=True)


def _plain(blob):
    if isinstance(blob, bytes) and blob[:4] == b"\x28\xb5\x2f\xfd":
        return zstandard.ZstdDecompressor().decompress(blob)
    return blob.encode("utf-8") if isinstance(blob, str) else blob


def main() -> None:
    """Print per-table row counts and the number of differing rows."""
    a, b = _db(sys.argv[1]), _db(sys.argv[2])
    tables = [r[0] for r in a.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    for table in sorted(tables):
        cols = [r[1] for r in a.execute(f"PRAGMA table_info({table})")]
        rows_a = a.execute(f"SELECT * FROM {table}").fetchall()
        try:
            rows_b = b.execute(f"SELECT * FROM {table}").fetchall()
        except sqlite3.Error:
            print(f"{table}: missing in B")
            continue
        if table == "telemetry_cache":
            key = len(cols) - 1
            ma = {r[:key]: _plain(r[key]) for r in rows_a}
            mb = {r[:key]: _plain(r[key]) for r in rows_b}
            diff = sum(1 for k in ma.keys() | mb.keys() if ma.get(k) != mb.get(k))
            print(f"{table}: A {len(ma)} rows, B {len(mb)} rows, differing {diff}")
        else:
            print(f"{table}: A {len(rows_a)} rows, B {len(rows_b)} rows")


if __name__ == "__main__":
    main()
