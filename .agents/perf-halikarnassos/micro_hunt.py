"""Offline micro-benchmarks for candidate hypotheses on real Monaco data."""

from __future__ import annotations

import os
import pickle
import sqlite3
import sys
import time
from pathlib import Path

os.environ["TIF1_CACHE_DIR"] = "/tmp/perf-v1"

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))


def timeit(fn, *, repeat: int = 3) -> float:
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best


def main() -> None:
    conn = sqlite3.connect("/tmp/perf-v1/cache.sqlite")
    rows = conn.execute(
        "SELECT driver, lap, frame FROM telemetry_frames WHERE year=2026 AND gp='Monaco%20Grand%20Prix' AND session='Race'"
    ).fetchall()
    print(f"frames: {len(rows)}")

    from tif1.cache import _decompress_frame_blob

    blobs = [r[2] for r in rows]
    raw = [_decompress_frame_blob(b) for b in blobs]
    frames = [pickle.loads(b) for b in raw]
    f0 = frames[0]
    print("frame columns:", list(f0.columns))
    print("frame dtypes:", {c: str(t) for c, t in f0.dtypes.items()})
    print("frame shape:", f0.shape)

    # --- A: lz4.frame vs lz4.block
    from lz4 import block as lz4_block
    from lz4 import frame as lz4_frame

    t_frame_dec = timeit(lambda: [lz4_frame.decompress(b) for b in blobs])
    t_frame_enc = timeit(lambda: [lz4_frame.compress(b, compression_level=0) for b in raw])
    block_blobs = [lz4_block.compress(b, store_size=True) for b in raw]
    t_block_dec = timeit(lambda: [lz4_block.decompress(b) for b in block_blobs])
    t_block_enc = timeit(lambda: [lz4_block.compress(b, store_size=True) for b in raw])
    ok = all(lz4_block.decompress(bb) == rr for bb, rr in zip(block_blobs, raw))
    print(
        f"A lz4: frame dec={t_frame_dec * 1000:.0f}ms enc={t_frame_enc * 1000:.0f}ms | "
        f"block dec={t_block_dec * 1000:.0f}ms enc={t_block_enc * 1000:.0f}ms parity={ok} "
        f"stored frame={sum(len(b) for b in blobs) / 1e6:.1f}MB block={sum(len(b) for b in block_blobs) / 1e6:.1f}MB"
    )

    # --- B: IN-list vs range-scan SQL read
    refs = [(r[0], r[1]) for r in rows]
    params_flat: list = [2026, "Monaco%20Grand%20Prix", "Race"]
    for d, lap in refs:
        params_flat.extend([d, lap])
    ph = ", ".join(["(?, ?)"] * len(refs))

    def in_list_read():
        return conn.execute(
            f"SELECT driver, lap, frame FROM telemetry_frames WHERE year=? AND gp=? AND session=? AND (driver, lap) IN ({ph})",
            params_flat,
        ).fetchall()

    def range_read():
        return [
            r
            for r in conn.execute(
                "SELECT driver, lap, frame FROM telemetry_frames WHERE year=? AND gp=? AND session=?",
                (2026, "Monaco%20Grand%20Prix", "Race"),
            )
            if (r[0], r[1]) in ref_set
        ]

    ref_set = set(refs)
    t_in = timeit(in_list_read)
    t_range = timeit(range_read)
    print(f"B sql: IN-list={t_in * 1000:.0f}ms range-scan={t_range * 1000:.0f}ms")

    # --- C: constant-column elision roundtrip
    def trim_frame(df, driver, lap):
        cols = {}
        const_meta = {}
        for i, col in enumerate(df.columns):
            s = df[col]
            if col in {"Driver", "LapNumber"}:
                const_meta[col] = (i, s.iloc[0] if len(s) else None, str(s.dtype))
            else:
                cols[col] = s
        return cols, const_meta

    def trim_encode(df, driver, lap):
        cols, const_meta = trim_frame(df, driver, lap)
        payload = {
            "cols": {c: cols[c] for c in df.columns if c in cols},
            "const": const_meta,
            "order": list(df.columns),
            "n": len(df),
        }
        return pickle.dumps(payload, protocol=5)

    def trim_decode(blob):
        payload = pickle.loads(blob)
        n = payload["n"]
        frame_data = {}
        for col in payload["order"]:
            meta = payload["const"].get(col)
            if meta is not None:
                _, val, _dtype = meta
                if col == "LapNumber":
                    frame_data[col] = pd.array(np.full(n, val, dtype="int64"), dtype="Int64")
                else:
                    frame_data[col] = pd.Series(np.full(n, val, dtype=object), dtype=object)
            else:
                frame_data[col] = payload["cols"][col]
        return pd.DataFrame(frame_data)

    trimmed_blobs = [trim_encode(f, r[0], r[1]) for f, r in zip(frames, rows)]
    t_trim_dec = timeit(lambda: [trim_decode(b) for b in trimmed_blobs])
    parity = True
    for f, b in zip(frames, trimmed_blobs):
        g = trim_decode(b)
        if list(f.columns) != list(g.columns) or not f.dtypes.equals(g.dtypes) or len(f) != len(g):
            parity = False
            break
        for c in f.columns:
            if not (f[c].equals(g[c])):
                parity = False
                break
    print(
        f"C const-elide: unpickle={t_trim_dec * 1000:.0f}ms (vs full ~470-550ms) "
        f"stored={sum(len(b) for b in trimmed_blobs) / 1e6:.1f}MB (vs {sum(len(b) for b in raw) / 1e6:.1f}MB) values_parity={parity}"
    )

    # --- D: typed-frame internals on real payloads
    tel_rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=2026 AND gp='Monaco%20Grand%20Prix' AND session='Race' LIMIT 60"
    ).fetchall()
    print("telemetry payload rows found:", len(tel_rows))
    if tel_rows:
        import orjson

        def get_blob_data(blob):
            if isinstance(blob, bytes) and blob[:4] == b"\x28\xb5\x2f\xfd":
                import zstandard

                return orjson.loads(zstandard.ZstdDecompressor().decompress(blob))
            return orjson.loads(blob)

        payloads = [get_blob_data(r[2]) for r in tel_rows]
        print("sample payload keys:", list(payloads[0].keys()))

        def typed_all():
            from tif1.core_utils.helpers import _create_telemetry_df

            for (d, _lap, _), p in zip(tel_rows, payloads):
                _create_telemetry_df(p, d, _lap, "pandas")

        t_all = timeit(typed_all, repeat=3)
        print(f"D cold assembly: {t_all * 1000 / len(payloads):.0f}us/frame x{len(payloads)}")

    # --- F: str-dtype column (DriverAhead) pickle roundtrip cost
    obj_frames = []
    for f in frames:
        g = f.copy()
        g["DriverAhead"] = g["DriverAhead"].astype(object)
        obj_frames.append(g)

    obj_blobs = [pickle.dumps(f, protocol=5) for f in obj_frames]
    t_str_unpickle = timeit(lambda: [pickle.loads(b) for b in raw])
    t_obj_unpickle = timeit(lambda: [pickle.loads(b) for b in obj_blobs])
    print(
        f"F str-col: unpickle str={t_str_unpickle * 1000:.0f}ms vs object={t_obj_unpickle * 1000:.0f}ms "
        f"stored str={sum(len(b) for b in raw) / 1e6:.1f}MB vs object={sum(len(b) for b in obj_blobs) / 1e6:.1f}MB"
    )

    # --- E: schedule load cost
    from tif1 import events as ev

    t_sched = timeit(lambda: ev._load_vendored_f1schedule_years(), repeat=3)
    files = sorted((REPO / "src/tif1/data/schedules/f1schedule").glob("*.json"))
    total_bytes = sum(f.stat().st_size for f in files)
    print(
        f"E schedule: stdlib-json load all years={t_sched * 1000:.0f}ms over {len(files)} files {total_bytes / 1e6:.1f}MB"
    )

    def orjson_schedule():
        import orjson

        out = {}
        for f in files:
            raw = orjson.loads(f.read_bytes())
            out[f.stem] = raw
        return out

    t_orjson = timeit(orjson_schedule, repeat=3)
    print(f"E schedule: orjson load={t_orjson * 1000:.0f}ms")

    ev._load_schedule_payload.cache_clear()
    t_validate = timeit(lambda: ev._load_schedule_payload(), repeat=3)
    print(f"E schedule: full payload+validate={t_validate * 1000:.0f}ms")


if __name__ == "__main__":
    main()
