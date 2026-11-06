"""Encode a session's lap telemetry JSON into per-driver columnar files (``tel.v2``).

The data repositories (``TracingInsights/{year}``) publish one
``{driver}/{lap}_tel.json`` per lap. This tool writes, next to them, the
files ``{driver}/tel.v2.{first}.bin`` (ten laps each, ``first`` = 1, 11,
21...) that tif1 reads first on a cold telemetry wave (format:
``design/TELEMETRY_COLUMNAR_FORMAT.md``, codec:
``tif1.core_utils.columnar_telemetry``).

Sources:

- ``--json-dir DIR``: a session folder of the data repository
  (``DIR/{driver}/{lap}_tel.json``; ``.json.gz`` too): what the pipeline runs on;
- ``--cache DIR --year Y --gp GP --session S``: the ``telemetry_cache`` tier of a
  tif1 cache (the payloads tif1 fetched, re-serialized by orjson).

``--verify`` decodes every file written with each decoder the installed
``tif1-native`` allows: its whole-file decoder (``tel_decode_file``), the
column-by-column decoder around its prediction kernels, and the codec's
numpy twins, and checks, lap by lap, that every decoded mapping
equal the JSON decode bit for bit and that the pandas and polars frames built
from them equal the frames built from the JSON
(``assert_frame_equal(check_exact=True)`` / ``DataFrame.equals``). The tool
needs ``tif1-native``: the files hold what the native decoder returns
(int64/float64 arrays, dictionary ``DriverAhead``), and the round trip is
checked against it.

Usage (from the repo root)::

    PYTHONPATH=src python tools/data/encode_driver_telemetry.py \\
        --json-dir "2025/Bahrain Grand Prix/Race" --out-dir out/ --verify
"""

from __future__ import annotations

import argparse
import gzip
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

import numpy as np
import pandas as pd
import polars as pl

from tif1.cache import _decode_sqlite_value
from tif1.core_utils import json_utils
from tif1.core_utils.columnar_telemetry import decode_driver_file, encode_driver_files
from tif1.core_utils.helpers import _create_telemetry_df
from tif1.core_utils.json_utils import DictionaryChannel, decode_telemetry_payload


def _tel_of(raw: bytes) -> dict[str, Any]:
    """The ``tel`` mapping of a lap payload (``{"tel": ...}`` or the bare mapping)."""
    data = decode_telemetry_payload(raw)
    if not isinstance(data, dict):
        raise ValueError("payload is not a JSON object")
    tel = data.get("tel", data)
    if not isinstance(tel, dict):
        raise ValueError("payload has no 'tel' object")
    return tel


def _from_json_dir(root: Path) -> dict[str, dict[int, bytes]]:
    laps: dict[str, dict[int, bytes]] = defaultdict(dict)
    for path in sorted(root.glob("*/*_tel.json*")):
        stem = path.name.removesuffix(".gz").removesuffix("_tel.json")
        if not stem.isdigit():
            continue
        raw = path.read_bytes()
        laps[path.parent.name][int(stem)] = gzip.decompress(raw) if path.suffix == ".gz" else raw
    return laps


def _from_cache(cache_dir: Path, year: int, gp: str, session: str) -> dict[str, dict[int, bytes]]:
    conn = sqlite3.connect(f"file:{(cache_dir / 'cache.sqlite').as_posix()}?mode=ro", uri=True)
    laps: dict[str, dict[int, bytes]] = defaultdict(dict)
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ?",
        (year, quote(unquote(gp)), session),
    )
    for driver, lap, data in rows:
        raw = _decode_sqlite_value(data)
        laps[driver][int(lap)] = raw if isinstance(raw, bytes) else raw.encode("utf-8")
    conn.close()
    return laps


def _same(expected: dict[str, Any], actual: dict[str, Any]) -> str | None:
    """None when ``actual`` equals ``expected`` bit for bit, else the first difference."""
    if list(expected) != list(actual):
        return f"keys {list(expected)} != {list(actual)}"
    for key, value in expected.items():
        got = actual[key]
        if isinstance(value, np.ndarray):
            if not isinstance(got, np.ndarray) or got.dtype != value.dtype:
                return f"{key}: dtype"
            if got.view(np.uint8).tobytes() != value.view(np.uint8).tobytes():
                return f"{key}: values"
        elif isinstance(value, DictionaryChannel):
            if not isinstance(got, DictionaryChannel) or got.categories != value.categories:
                return f"{key}: categories"
            if not np.array_equal(got.codes, value.codes):
                return f"{key}: codes"
        elif got != value or type(got) is not type(value):
            return f"{key}: value"
    return None


def _verify_lap(driver: str, lap: int, expected: dict[str, Any], actual: dict[str, Any]) -> None:
    problem = _same(expected, actual)
    if problem is not None:
        raise AssertionError(f"{driver} lap {lap}: {problem}")
    for lib in ("pandas", "polars"):
        want = _create_telemetry_df(expected, driver, lap, lib)
        got = _create_telemetry_df(actual, driver, lap, lib)
        if lib == "pandas":
            pd.testing.assert_frame_equal(got, want, check_exact=True)
        elif not (
            isinstance(got, pl.DataFrame)
            and isinstance(want, pl.DataFrame)
            and got.schema == want.schema
            and got.equals(want)
        ):
            raise AssertionError(f"{driver} lap {lap}: polars frames differ")


_NATIVE_KERNELS = tuple(name for name in dir(json_utils) if name.startswith("NATIVE_TEL_"))


def _decode_without(names: tuple[str, ...]) -> Any:
    """A decoder that runs with the native handles ``names`` hidden."""

    def decode(blob: bytes) -> dict[int, dict[str, Any]]:
        saved = {name: getattr(json_utils, name) for name in names}
        try:
            for name in names:
                setattr(json_utils, name, None)
            return decode_driver_file(blob)
        finally:
            for name, kernel in saved.items():
                setattr(json_utils, name, kernel)

    return decode


# The whole-file decoder (when installed), the column-by-column decoder around
# the native kernels, and the numpy twins (the client without the kernels).
_DECODERS = (
    decode_driver_file,
    _decode_without(("NATIVE_TEL_DECODE_FILE",)),
    _decode_without(_NATIVE_KERNELS),
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--json-dir", type=Path, help="session folder of lap JSON files")
    source.add_argument("--cache", type=Path, help="tif1 cache directory (cache.sqlite)")
    parser.add_argument("--year", type=int)
    parser.add_argument("--gp", help="event name (as in the CDN path)")
    parser.add_argument("--session", help="session name (as in the CDN path)")
    parser.add_argument("--out-dir", type=Path, help="write {driver}/tel.v2.{first}.bin here")
    parser.add_argument("--level", type=int, default=19, help="zstd level (default 19)")
    parser.add_argument("--verify", action="store_true", help="decode and compare every lap")
    args = parser.parse_args()

    if not json_utils.NATIVE_TELEMETRY_DECODER_AVAILABLE:
        print("tif1-native is required (pip install tif1-native)", file=sys.stderr)
        return 2
    if args.cache is not None:
        if not (args.year and args.gp and args.session):
            parser.error("--cache needs --year, --gp and --session")
        raw_laps = _from_cache(args.cache, args.year, args.gp, args.session)
    else:
        raw_laps = _from_json_dir(args.json_dir)
    if not raw_laps:
        print("no lap telemetry found", file=sys.stderr)
        return 1

    t0 = time.perf_counter()
    json_bytes = file_bytes = n_laps = n_files = 0
    for driver, laps in sorted(raw_laps.items()):
        tels = {lap: _tel_of(raw) for lap, raw in laps.items()}
        files = encode_driver_files(tels, level=args.level)
        json_bytes += sum(len(raw) for raw in laps.values())
        file_bytes += sum(len(blob) for blob in files.values())
        n_laps += len(tels)
        n_files += len(files)
        if args.out_dir is not None:
            target = args.out_dir / driver
            target.mkdir(parents=True, exist_ok=True)
            for name, blob in files.items():
                (target / name).write_bytes(blob)
        if args.verify:
            for blob in files.values():
                for decode in _DECODERS:
                    for lap, decoded in decode(blob).items():
                        _verify_lap(driver, lap, tels[lap], decoded)
    print(
        f"{len(raw_laps)} drivers, {n_laps} laps -> {n_files} files, "
        f"{file_bytes / 1e6:.2f} MB (JSON {json_bytes / 1e6:.2f} MB uncompressed)"
        + (", verified exact" if args.verify else "")
        + f" in {time.perf_counter() - t0:.1f} s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
