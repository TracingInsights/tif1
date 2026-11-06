"""Naxos N7: decode time of a session's columnar files with the tree on PYTHONPATH.

``--encode OUT`` encodes every driver's files of one session from a tif1 cache
(the tree's own format: ``tel.v1`` for the control, ``tel.v2`` for N7) and
pickles them; ``--decode IN`` decodes them, one thread, best of ``--reps``,
and prints the total and the median per file. ``--numpy`` hides the native
prediction kernels (the decoder without a tif1-native that has them).

Usage (from the repo root)::

    PYTHONPATH=src python tools/perf/naxos_n7_decode_bench.py --cache /tmp/naxos/warm-N7 \\
        --encode /tmp/files.pkl
    PYTHONPATH=src python tools/perf/naxos_n7_decode_bench.py --decode /tmp/files.pkl
"""

from __future__ import annotations

import argparse
import pickle
import sqlite3
import statistics
import time
from collections import defaultdict
from pathlib import Path


def _encode(cache_dir: str, session: str, out: str) -> None:
    from tif1.cache import _decode_sqlite_value
    from tif1.core_utils.columnar_telemetry import encode_driver_files
    from tif1.core_utils.json_utils import decode_telemetry_payload

    conn = sqlite3.connect(f"file:{Path(cache_dir) / 'cache.sqlite'}?mode=ro", uri=True)
    laps: dict[str, dict[int, bytes]] = defaultdict(dict)
    for driver, lap, data in conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE session = ?", (session,)
    ):
        raw = _decode_sqlite_value(data)
        raw = raw if isinstance(raw, bytes) else raw.encode()
        laps[driver][lap] = raw
    files = []
    for driver in sorted(laps):
        tels = {
            lap: decode_telemetry_payload(b'{"tel":' + raw + b"}")["tel"]
            for lap, raw in laps[driver].items()
        }
        files.extend(encode_driver_files(tels).values())
    Path(out).write_bytes(pickle.dumps(files))
    print(f"{session}: {len(files)} files, {sum(map(len, files)) / 1e6:.2f} MB")


def _decode(path: str, reps: int, numpy_only: bool) -> None:
    from tif1.core_utils import json_utils
    from tif1.core_utils.columnar_telemetry import decode_driver_file

    if numpy_only:
        for name in dir(json_utils):
            if name.startswith("NATIVE_TEL_"):
                setattr(json_utils, name, None)
    files = pickle.loads(Path(path).read_bytes())
    per_file: list[float] = []
    totals: list[float] = []
    for _ in range(reps):
        times = []
        for blob in files:
            t0 = time.perf_counter()
            decode_driver_file(blob)
            times.append(time.perf_counter() - t0)
        totals.append(sum(times))
        per_file = times if not per_file else [min(a, b) for a, b in zip(per_file, times)]
    print(
        f"{len(files)} files: total best {1000 * min(totals):.0f} ms, "
        f"median per file {1000 * statistics.median(per_file):.2f} ms"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/naxos/warm-N7")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--encode", default=None)
    parser.add_argument("--decode", default=None)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--numpy", action="store_true")
    args = parser.parse_args()
    if args.encode:
        _encode(args.cache, args.session, args.encode)
    if args.decode:
        _decode(args.decode, args.reps, args.numpy)


if __name__ == "__main__":
    main()
