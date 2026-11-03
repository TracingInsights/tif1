# ruff: noqa: B023, PLR0402  (timing lambdas run inside their loop iteration)
"""Gortyn G3: compare candidate columnar telemetry layouts on a session's laps.

Reads every lap payload of the given sessions from a tif1 SQLite cache
(``telemetry_cache`` tier, the bytes the CDN JSON re-serialized by orjson),
and reports per session:

- JSON: bytes of ``{"tel": ...}`` raw and gzip -6 (the CDN serves gzip);
- (a) one Arrow IPC file per driver, zstd (pyarrow writer; int32 ms clocks,
  narrowed integers, float64, dictionary ``DriverAhead``);
- (b) the ``tel.v1.bin`` codec, one file per lap;
- (c) the ``tel.v1.bin`` codec, one file per driver (the chosen layout);

and the decode time per driver of (a) and (c) to the decoder's mapping, of
the JSON laps through the native decoder, and the pandas/polars frame build
from each. Every (c) lap is checked against the JSON decode (exact).

Usage (from the repo root)::

    PYTHONPATH=src python tools/perf/gortyn_g3_format_compare.py \\
        --cache /tmp/tif1-gortyn-warm-G3/cache.sqlite --out g3_formats.json
"""

from __future__ import annotations

import argparse
import gzip
import io
import sqlite3
import statistics
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import orjson

from tif1.cache import _decode_sqlite_value
from tif1.core_utils.columnar_telemetry import decode_driver_file, encode_driver_file
from tif1.core_utils.helpers import _create_telemetry_df
from tif1.core_utils.json_utils import DictionaryChannel, decode_telemetry_payload


def _arrow_driver_file(laps: dict[int, dict]) -> bytes:
    import pyarrow as pa
    import pyarrow.ipc as ipc

    keys = list(next(iter(laps.values())))
    cols: dict[str, list] = defaultdict(list)
    lap_col, n_col = [], []
    for lap, tel in sorted(laps.items()):
        n = len(tel["time"])
        lap_col.append(lap)
        n_col.append(n)
        for key in keys:
            value = tel.get(key)
            if key == "time":
                cols[key].append(pa.array(np.round(value * 1000).astype(np.int32)))
            elif isinstance(value, DictionaryChannel):
                codes = pa.array(np.asarray(value.codes), mask=np.asarray(value.codes) < 0)
                cols[key].append(
                    pa.DictionaryArray.from_arrays(
                        codes.cast(pa.int8()), pa.array(value.categories, pa.string())
                    ).cast(pa.string())
                )
            elif isinstance(value, np.ndarray):
                if value.dtype.kind == "i":
                    cols[key].append(pa.array(value.astype(np.int16)))
                else:
                    cols[key].append(pa.array(value))
    table = pa.table({key: pa.concat_arrays(parts) for key, parts in cols.items()})
    table = table.set_column(
        table.schema.get_field_index("DriverAhead"),
        "DriverAhead",
        table.column("DriverAhead").dictionary_encode(),
    )
    sink = io.BytesIO()
    meta = {b"laps": orjson.dumps({"lap": lap_col, "n": n_col})}
    with ipc.new_file(
        sink,
        table.schema.with_metadata(meta),
        options=ipc.IpcWriteOptions(compression=pa.Codec("zstd", 19)),
    ) as writer:
        writer.write_table(table)
    return sink.getvalue()


def _arrow_decode(blob: bytes) -> dict[int, dict]:
    import pyarrow as pa
    import pyarrow.ipc as ipc

    table = ipc.open_file(pa.BufferReader(blob)).read_all()
    meta = orjson.loads(table.schema.metadata[b"laps"])
    arrays = {}
    for name in table.column_names:
        col = table.column(name).combine_chunks()
        if name == "DriverAhead":
            arrays[name] = (col.indices.to_numpy(zero_copy_only=False), col.dictionary.to_pylist())
        else:
            arrays[name] = col.to_numpy(zero_copy_only=False)
    out = {}
    pos = 0
    for lap, n in zip(meta["lap"], meta["n"], strict=True):
        tel = {}
        for name, arr in arrays.items():
            if name == "DriverAhead":
                codes, cats = arr
                tel[name] = DictionaryChannel(codes[pos : pos + n].astype(np.int32), cats)
            elif name == "time":
                tel[name] = arr[pos : pos + n] / 1000.0
            else:
                tel[name] = arr[pos : pos + n]
        out[lap] = tel
        pos += n
    return out


def _same(a: dict, b: dict) -> bool:
    if list(a) != list(b):
        return False
    for key, va in a.items():
        vb = b[key]
        if isinstance(va, np.ndarray):
            if not isinstance(vb, np.ndarray) or va.dtype != vb.dtype:
                return False
            if not np.array_equal(va.view(np.int64), vb.view(np.int64)):
                return False
        elif isinstance(va, DictionaryChannel):
            if not isinstance(vb, DictionaryChannel) or va.categories != vb.categories:
                return False
            if vb.codes.dtype != va.codes.dtype or not np.array_equal(va.codes, vb.codes):
                return False
        elif va != vb:
            return False
    return True


def _best(fn, reps: int = 5) -> float:
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return min(times) * 1000


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--level", type=int, default=19)
    args = parser.parse_args()

    conn = sqlite3.connect(f"file:{args.cache}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT year, gp, session, driver, lap, data FROM telemetry_cache"
    ).fetchall()
    sessions: dict[tuple, dict[str, dict[int, bytes]]] = defaultdict(lambda: defaultdict(dict))
    for year, gp, session, driver, lap, data in rows:
        raw = _decode_sqlite_value(data)
        sessions[(year, gp, session)][driver][lap] = raw if isinstance(raw, bytes) else raw.encode()

    report: dict[str, dict] = {}
    for key, drivers in sessions.items():
        s = {
            "laps": 0,
            "json_raw": 0,
            "json_gzip": 0,
            "a_arrow_driver": 0,
            "b_bin_lap": 0,
            "c_bin_driver": 0,
            "c_mismatch_laps": 0,
            "a_mismatch_laps": 0,
        }
        t_json, t_c, t_a, t_pd_json, t_pd_c, t_pl_json, t_pl_c = ([] for _ in range(7))
        for driver, laps in sorted(drivers.items()):
            payloads = {lap: b'{"tel":' + raw + b"}" for lap, raw in laps.items()}
            s["laps"] += len(laps)
            s["json_raw"] += sum(len(p) for p in payloads.values())
            s["json_gzip"] += sum(len(gzip.compress(p, 6)) for p in payloads.values())
            decoded = {lap: decode_telemetry_payload(p)["tel"] for lap, p in payloads.items()}
            c_blob = encode_driver_file(decoded, level=args.level)
            s["c_bin_driver"] += len(c_blob)
            s["b_bin_lap"] += sum(
                len(encode_driver_file({lap: tel}, level=args.level))
                for lap, tel in decoded.items()
            )
            a_blob = _arrow_driver_file(decoded)
            s["a_arrow_driver"] += len(a_blob)
            c_back = decode_driver_file(c_blob)
            s["c_mismatch_laps"] += sum(not _same(decoded[lap], c_back[lap]) for lap in decoded)
            a_back = _arrow_decode(a_blob)
            s["a_mismatch_laps"] += sum(not _same(decoded[lap], a_back[lap]) for lap in decoded)

            t_json.append(_best(lambda: [decode_telemetry_payload(p) for p in payloads.values()]))
            t_c.append(_best(lambda: decode_driver_file(c_blob)))
            t_a.append(_best(lambda: _arrow_decode(a_blob)))

            def build(source, lib):
                for lap, tel in source.items():
                    _create_telemetry_df(tel, driver, lap, lib)

            t_pd_json.append(_best(lambda: build(decoded, "pandas"), 3))
            t_pd_c.append(_best(lambda: build(decode_driver_file(c_blob), "pandas"), 3))
            t_pl_json.append(_best(lambda: build(decoded, "polars"), 3))
            t_pl_c.append(_best(lambda: build(decode_driver_file(c_blob), "polars"), 3))
        s["drivers"] = len(drivers)
        s["decode_ms_per_driver_median"] = {
            "json_native": statistics.median(t_json),
            "c_bin_driver": statistics.median(t_c),
            "a_arrow_driver": statistics.median(t_a),
        }
        s["frames_ms_per_driver_median"] = {
            "pandas_from_json_decode": statistics.median(t_pd_json),
            "pandas_from_c_incl_decode": statistics.median(t_pd_c),
            "polars_from_json_decode": statistics.median(t_pl_json),
            "polars_from_c_incl_decode": statistics.median(t_pl_c),
        }
        report["/".join(map(str, key))] = s
        print(orjson.dumps(s, option=orjson.OPT_INDENT_2).decode(), flush=True)
    Path(args.out).write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))


if __name__ == "__main__":
    main()
