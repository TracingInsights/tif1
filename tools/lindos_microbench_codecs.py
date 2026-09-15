"""N-series microbenchmarks on real cached Monaco telemetry payloads.

Measures, on the same 1452 payload set:
- orjson parse vs msgspec parse (if installed)
- current typed pandas assembly (orjson -> _typed_telemetry_frame)
- pyarrow-native parse+assembly (pyarrow.json.read_json -> cast -> to_pandas)
- pickle+zstd vs Arrow IPC frame serialize/deserialize roundtrip
"""

from __future__ import annotations

import json
import os
import pickle
import time

WARM = os.environ.get("TIF1_WARM_CACHE_DIR", "/tmp/tif1-warm-cache")
os.environ["TIF1_CACHE_DIR"] = WARM

import orjson  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.json as pajson  # noqa: E402
import zstandard  # noqa: E402

import tif1  # noqa: E402
from tif1.cache import get_cache  # noqa: E402
from tif1.core_utils.helpers import _create_telemetry_df  # noqa: E402


def t() -> float:
    return time.perf_counter()


def load_raw_payloads() -> list[tuple[str, int, bytes]]:
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    laps = session.laps
    refs_frame = laps[["Driver", "LapNumber"]].dropna()
    _refs = list(zip(refs_frame["Driver"].astype(str), refs_frame["LapNumber"].astype(int)))
    _cache = get_cache()
    # payload tier, decompressed
    import sqlite3

    conn = sqlite3.connect(f"{WARM}/cache.sqlite")
    conn.row_factory = sqlite3.Row
    gp_key = session.gp
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=? AND gp=? AND session=?",
        (session.year, gp_key, session.session),
    ).fetchall()
    dec = zstandard.ZstdDecompressor()
    out = []
    for r in rows:
        try:
            out.append((r["driver"], int(r["lap"]), dec.decompress(r["data"])))
        except Exception:
            pass
    print(f"loaded {len(out)} raw payload blobs, {sum(len(b) for _, _, b in out) / 1e6:.1f} MB")
    return out


def bench(name: str, fn, repeat: int = 3) -> float:
    best = 1e9
    for _ in range(repeat):
        t0 = t()
        fn()
        best = min(best, t() - t0)
    print(f"{name}: {best:.4f}s")
    return best


def main() -> None:
    raws = load_raw_payloads()
    out: dict[str, float] = {}

    # ---- parse-only benchmarks
    out["orjson_parse_s"] = bench("orjson parse", lambda: [orjson.loads(b) for _, _, b in raws])

    try:
        import msgspec

        out["msgspec_parse_s"] = bench(
            "msgspec parse", lambda: [msgspec.json.decode(b) for _, _, b in raws]
        )
    except ImportError:
        print("msgspec not installed")

    # ---- current assembly (orjson -> typed frame), what cold/warm1 pays
    parsed = [(drv, lp, orjson.loads(b)) for drv, lp, b in raws]

    def assemble_current():
        frames = []
        for drv, lp, pl in parsed:
            fr = _create_telemetry_df(
                pl.get("tel", pl) if isinstance(pl, dict) else pl, drv, lp, "pandas"
            )
            if fr is not None and not fr.empty:
                frames.append(fr)
        return frames

    out["current_assembly_s"] = bench("current typed assembly", assemble_current)
    frames = assemble_current()
    print(f"assembled {len(frames)} frames")

    # ---- pyarrow-native: read_json -> renamed/cast -> to_pandas
    def arrow_path_one(drv: str, lp: int, blob: bytes) -> pd.DataFrame | None:
        try:
            tbl = pajson.read_json(pa.BufferReader(blob))
        except Exception:
            return None
        df = tbl.to_pandas()
        return df

    # telemetry payloads nest channels under "tel"
    nested = 0
    sample = orjson.loads(raws[0][2])
    if isinstance(sample, dict) and "tel" in sample:
        nested = 1
    print("payload nested under 'tel':", bool(nested))

    def arrow_path():
        out_frames = []
        for _drv, _lp, blob in raws:
            try:
                tbl = pajson.read_json(pa.BufferReader(blob))
            except Exception:
                out_frames.append(None)
                continue
            out_frames.append(tbl)
        return out_frames

    out["arrow_read_json_s"] = bench("pyarrow.json read_json (raw)", arrow_path)

    # ---- frame storage roundtrips
    comp = zstandard.ZstdCompressor(level=1)
    dec = zstandard.ZstdDecompressor()

    def pickle_zstd_roundtrip():
        blobs = [comp.compress(pickle.dumps(fr, protocol=5)) for fr in frames]
        back = [pickle.loads(dec.decompress(b)) for b in blobs]
        return blobs, back

    out["pickle_zstd_roundtrip_s"] = bench("pickle+zstd roundtrip", pickle_zstd_roundtrip)
    p_blobs = [comp.compress(pickle.dumps(fr, protocol=5)) for fr in frames]
    t0 = t()
    _ = [pickle.loads(dec.decompress(b)) for b in p_blobs]
    out["pickle_zstd_deser_s"] = t() - t0
    print(
        f"pickle+zstd deser: {out['pickle_zstd_deser_s']:.4f}s blob={sum(len(b) for b in p_blobs) / 1e6:.1f}MB"
    )

    # Arrow IPC per-frame
    def arrow_ipc_roundtrip():
        buffers = []
        for fr in frames:
            tbl = pa.Table.from_pandas(fr, preserve_index=False)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, tbl.schema) as w:
                w.write_table(tbl)
            buffers.append(sink.getvalue())
        back = []
        for buf in buffers:
            reader = pa.ipc.open_stream(buf)
            back.append(reader.read_all().to_pandas())
        return buffers, back

    out["arrow_ipc_roundtrip_s"] = bench("arrow IPC roundtrip", arrow_ipc_roundtrip, repeat=1)

    # Arrow IPC compressed with zstd
    def arrow_ipc_zstd():
        buffers = []
        for fr in frames:
            tbl = pa.Table.from_pandas(fr, preserve_index=False)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, tbl.schema) as w:
                w.write_table(tbl)
            buffers.append(comp.compress(sink.getvalue()))
        back = []
        for buf in buffers:
            reader = pa.ipc.open_stream(dec.decompress(buf))
            back.append(reader.read_all().to_pandas())
        return buffers, back

    out["arrow_ipc_zstd_roundtrip_s"] = bench(
        "arrow IPC + zstd roundtrip", arrow_ipc_zstd, repeat=1
    )

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
