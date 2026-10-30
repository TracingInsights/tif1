"""A9: offline process-pool parse+assembly benchmark on the raw Monaco corpus.

Compares the post-gather work of the cold bulk path on raw JSON bytes:

- serial: orjson parse + ``_create_telemetry_df`` per payload (in-process)
- pool(N): workers parse + build + pickle (protocol 5); the parent unpickles

Both variants produce the same frames (parity checked with ``.equals`` +
dtypes). Pool start-up is reported separately because on the real cold path
it overlaps the network wave.

Usage:
    uv run python tools/perf/argos_procpool_bench.py --workers 4 8 16
"""

from __future__ import annotations

import argparse
import gc
import os
import pickle
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))


def _build_many(chunk: list[tuple[str, int, bytes]]) -> bytes:
    import orjson

    from tif1.core_utils.helpers import _create_telemetry_df

    out = []
    for driver, lap, raw in chunk:
        parsed = orjson.loads(raw)
        tel = parsed.get("tel", parsed)
        frame = _create_telemetry_df(tel, driver, lap, "pandas")
        out.append((driver, lap, frame))
    return pickle.dumps(out, protocol=5)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", default="/tmp/argos_corpus.pkl")
    parser.add_argument("--workers", type=int, nargs="+", default=[4, 8])
    parser.add_argument("--reps", type=int, default=3)
    args = parser.parse_args()
    import orjson

    from tif1.core_utils.helpers import _create_telemetry_df

    with open(args.corpus, "rb") as f:
        corpus = pickle.load(f)
    items = [(d, lap, corpus["raw"][(d, lap)]) for d, lap in corpus["refs"]]
    print(f"corpus: {len(items)} payloads, cpu_count={os.cpu_count()}")

    def serial():
        out = []
        for driver, lap, raw in items:
            parsed = orjson.loads(raw)
            tel = parsed.get("tel", parsed)
            out.append((driver, lap, _create_telemetry_df(tel, driver, lap, "pandas")))
        return out

    best = 1e9
    ref = None
    for _ in range(args.reps):
        gc.disable()
        t0 = time.perf_counter()
        ref = serial()
        best = min(best, time.perf_counter() - t0)
        gc.enable()
    print(f"serial parse+build: {best * 1000:.0f} ms")

    for n in args.workers:
        t0 = time.perf_counter()
        pool = ProcessPoolExecutor(max_workers=n)
        # warm the workers (import pandas) so start-up is measured separately
        list(pool.map(_build_many, [[]] * n))
        startup = time.perf_counter() - t0
        chunk = max(8, len(items) // (n * 4))
        chunks = [items[i : i + chunk] for i in range(0, len(items), chunk)]
        best_pool = 1e9
        got = None
        for _ in range(args.reps):
            gc.disable()
            t0 = time.perf_counter()
            blobs = list(pool.map(_build_many, chunks))
            t_workers = time.perf_counter() - t0
            got = [item for blob in blobs for item in pickle.loads(blob)]
            total = time.perf_counter() - t0
            gc.enable()
            best_pool = min(best_pool, total)
        pool.shutdown()
        mism = sum(
            1
            for (d1, l1, a), (d2, l2, b) in zip(ref, got)
            if (a is None) != (b is None)
            or (a is not None and not (a.equals(b) and list(a.dtypes) == list(b.dtypes)))
        )
        print(
            f"pool({n}): startup {startup * 1000:.0f} ms, parse+build+unpickle best {best_pool * 1000:.0f} ms "
            f"(workers {t_workers * 1000:.0f} ms), parity mismatches {mism}/{len(ref)}"
        )


if __name__ == "__main__":
    main()
