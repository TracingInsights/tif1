"""Corinth C8: decode time and exactness of the ``tel.v2`` whole-file native decoder.

Input: pickled lists of a session's files (``naxos_n7_decode_bench.py
--encode``). Modes (``--mode``): ``file`` (``tif1_native.tel_decode_file``,
the reader's path), ``columns`` (the column-by-column decoder around the
native kernels: the control's path), ``numpy`` (the numpy twins). Prints the
one-thread total (best of ``--reps``), the median per file, and with
``--threads N`` the wall time of decoding every file in an N-thread pool (the
reader decodes in the fetch pool). ``--verify`` compares every lap of every
file decoded by ``file`` with ``columns`` (and ``numpy`` with ``--numpy``):
keys and their order, types, dtypes, bits, DriverAhead codes and categories.

Usage (from the repo root, tif1-native built from this tree)::

    PYTHONPATH=src python tools/perf/corinth_c8_decode_bench.py race.pkl quali.pkl --verify
    PYTHONPATH=src python tools/perf/corinth_c8_decode_bench.py race.pkl --mode file --threads 8
"""

from __future__ import annotations

import argparse
import pickle
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np

from tif1.core_utils import json_utils
from tif1.core_utils.columnar_telemetry import decode_driver_file
from tif1.core_utils.json_utils import DictionaryChannel

_SAVED = {
    name: getattr(json_utils, name) for name in dir(json_utils) if name.startswith("NATIVE_TEL_")
}


def _set_mode(mode: str) -> None:
    for name, value in _SAVED.items():
        setattr(json_utils, name, value)
    if mode == "columns":
        json_utils.NATIVE_TEL_DECODE_FILE = None
    elif mode == "numpy":
        for name in _SAVED:
            setattr(json_utils, name, None)
    elif mode != "file":
        raise SystemExit(f"unknown mode {mode}")


def _same(a: Any, b: Any) -> str | None:
    if type(a) is not type(b):
        return f"type {type(a).__name__} != {type(b).__name__}"
    if isinstance(a, np.ndarray):
        if a.dtype != b.dtype or a.shape != b.shape:
            return f"dtype/shape {a.dtype}{a.shape} != {b.dtype}{b.shape}"
        width = a.dtype.itemsize
        view = {1: np.uint8, 2: np.uint16, 4: np.uint32, 8: np.uint64}[width]
        if not np.array_equal(a.view(view), b.view(view)):
            return "bits differ"
        return None
    if isinstance(a, DictionaryChannel):
        if type(a.categories) is not type(b.categories) or a.categories != b.categories:
            return "categories differ"
        return _same(a.codes, b.codes)
    if a != b or repr(a) != repr(b):
        return "value differs"
    return None


def _compare(left: dict[int, dict[str, Any]], right: dict[int, dict[str, Any]]) -> list[str]:
    problems = []
    if list(left) != list(right):
        return [f"laps {list(left)} != {list(right)}"]
    for lap, a in left.items():
        b = right[lap]
        if list(a) != list(b):
            problems.append(f"lap {lap}: keys {list(a)} != {list(b)}")
            continue
        for key in a:
            issue = _same(a[key], b[key])
            if issue:
                problems.append(f"lap {lap} {key}: {issue}")
    return problems


def _verify(files: list[bytes], numpy_too: bool) -> None:
    laps = mismatched = 0
    for index, blob in enumerate(files):
        _set_mode("file")
        native = decode_driver_file(blob)
        _set_mode("columns")
        reference = decode_driver_file(blob)
        problems = _compare(native, reference)
        if numpy_too:
            _set_mode("numpy")
            problems += [f"numpy: {p}" for p in _compare(native, decode_driver_file(blob))]
        laps += len(native)
        if problems:
            mismatched += 1
            print(f"file {index}: {problems[:5]}")
    _set_mode("file")
    print(f"verify: {len(files)} files, {laps} laps, {mismatched} files differ")


def _bench(files: list[bytes], mode: str, reps: int, threads: int) -> dict[str, float]:
    _set_mode(mode)
    for blob in files:  # warm-up
        decode_driver_file(blob)
    totals, per_file = [], []
    for _ in range(reps):
        times = []
        for blob in files:
            t0 = time.perf_counter()
            decode_driver_file(blob)
            times.append(time.perf_counter() - t0)
        totals.append(sum(times))
        per_file = times if not per_file else [min(a, b) for a, b in zip(per_file, times)]
    out = {
        "one_thread_ms": round(1000 * min(totals), 1),
        "median_file_ms": round(1000 * statistics.median(per_file), 3),
    }
    if threads > 1:
        walls = []
        with ThreadPoolExecutor(threads) as pool:
            list(pool.map(decode_driver_file, files))
            for _ in range(reps):
                t0 = time.perf_counter()
                list(pool.map(decode_driver_file, files))
                walls.append(time.perf_counter() - t0)
        out[f"pool{threads}_wall_ms"] = round(1000 * min(walls), 1)
    _set_mode("file")
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+")
    parser.add_argument("--mode", default="file,columns")
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--numpy", action="store_true", help="--verify the numpy twins too")
    args = parser.parse_args()
    if json_utils.NATIVE_TEL_DECODE_FILE is None:
        raise SystemExit("the installed tif1_native has no tel_decode_file")
    for path in args.files:
        files = pickle.loads(Path(path).read_bytes())
        print(f"{path}: {len(files)} files, {sum(map(len, files)) / 1e6:.2f} MB")
        if args.verify:
            _verify(files, args.numpy)
        for mode in args.mode.split(","):
            print(f"  {mode:8s} {_bench(files, mode, args.reps, args.threads)}")


if __name__ == "__main__":
    main()
