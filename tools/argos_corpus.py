"""Extract the real Monaco telemetry corpus from a primed tif1 cache (Argos series).

Reads every telemetry payload row of one session straight out of the SQLite
payload tier (no network), decodes the zstd/zlib blob, and writes
``/tmp/argos_corpus.pkl`` with ``{"refs": [(driver, lap)], "raw": {ref: bytes},
"parsed": {ref: dict}}`` for the offline harnesses.

Usage:
    uv run python tools/argos_corpus.py --cache-dir /tmp/tif1-warm-cache
"""

from __future__ import annotations

import argparse
import pickle
import sqlite3
import sys
from pathlib import Path
from urllib.parse import quote

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from tif1.cache import _decode_sqlite_value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-dir", default="/tmp/tif1-warm-cache")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--out", default="/tmp/argos_corpus.pkl")
    args = parser.parse_args()

    conn = sqlite3.connect(str(Path(args.cache_dir) / "cache.sqlite"))
    rows = []
    # The cache stores ``gp`` URL-encoded (CDN path form); accept either spelling.
    for gp in (quote(args.gp), args.gp):
        rows = conn.execute(
            "SELECT driver, lap, data FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ?",
            (args.year, gp, args.session),
        ).fetchall()
        if rows:
            break
    raw: dict[tuple[str, int], bytes] = {}
    parsed: dict[tuple[str, int], dict] = {}
    for driver, lap, data in rows:
        blob = _decode_sqlite_value(data)
        if blob is None:
            continue
        if isinstance(blob, str):
            blob = blob.encode()
        ref = (str(driver), int(lap))
        raw[ref] = bytes(blob)
        parsed[ref] = orjson.loads(blob)
    refs = sorted(raw)
    with open(args.out, "wb") as f:
        pickle.dump({"refs": refs, "raw": raw, "parsed": parsed, "gp": args.gp}, f, protocol=5)
    total = sum(len(b) for b in raw.values())
    print(f"corpus: {len(refs)} payloads, {total / 1e6:.1f} MB raw JSON -> {args.out}")


if __name__ == "__main__":
    main()
