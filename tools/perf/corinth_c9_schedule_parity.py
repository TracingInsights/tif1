"""Corinth C9: schedule parity digest (pandas ``EventSchedule`` and the polars schedule).

``--out FILE`` digests every surface of the pandas schedules the tree on
``PYTHONPATH`` builds: ``get_event_schedule`` for every season 1950-2026 and
backend (``None``, ``"fastf1"``, ``"ergast"``), with and without testing
events, ``get_events_remaining`` at several reference times, ``get_event`` /
``get_event_by_round`` / ``get_testing_event`` answers: class, ``year`` and
``backend`` attributes, columns, dtypes, index and the ``repr`` and type of
every value. ``--compare A B`` diffs two digests.

``--polars`` (candidate tree only) checks that ``lib="polars"`` returns the
same rows and columns as the pandas schedule value for value, under the
documented mapping (local session dates as naive wall-clock times, missing
values as nulls), and prints the number of mismatches.

Run offline against a cache (``TIF1_CACHE_DIR``, ``TIF1_OFFLINE_MODE=true``).
"""

# ruff: noqa: DTZ001 - the schedule's reference times are naive UTC, as the API takes them

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import warnings
from datetime import datetime
from pathlib import Path

YEARS = range(1950, 2027)
BACKENDS = (None, "fastf1", "ergast")
REMAINING_DATES = (
    datetime(2024, 1, 1),
    datetime(2024, 7, 1),
    datetime(2025, 2, 27, 12),
    datetime(2025, 7, 1),
    datetime(2025, 12, 31),
    datetime(2026, 3, 1),
    datetime(1998, 5, 20),
)


def _frame_digest(frame) -> dict:
    out = {
        "class": f"{type(frame).__module__}.{type(frame).__qualname__}",
        "year": repr(getattr(frame, "year", "<none>")),
        "backend": repr(getattr(frame, "backend", "<none>")),
        "columns": [str(c) for c in frame.columns],
        "dtypes": [str(d) for d in frame.dtypes],
        "index": repr(frame.index),
    }
    h = hashlib.sha256()
    for column in frame.columns:
        for value in frame[column].tolist():
            h.update(f"{column}|{type(value).__name__}|{value!r}\n".encode())
    out["values"] = h.hexdigest()
    out["repr"] = hashlib.sha256(repr(frame).encode()).hexdigest()
    return out


def _series_digest(event) -> dict:
    return {
        "class": f"{type(event).__module__}.{type(event).__qualname__}",
        "name": repr(event.name),
        "dtype": str(event.dtype),
        "index": [str(i) for i in event.index],
        "values": [f"{type(v).__name__}|{v!r}" for v in event.tolist()],
        "year": repr(getattr(event, "year", None)),
    }


def _call(fn):
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return fn(), None
    except Exception as exc:  # the digest records the failure
        return None, f"{type(exc).__name__}: {exc}"


def digest() -> dict:
    import tif1

    out: dict[str, dict] = {}
    for year in YEARS:
        for backend in BACKENDS:
            for include_testing in (True, False):
                key = f"schedule/{year}/{backend}/{include_testing}"
                frame, err = _call(
                    lambda y=year, b=backend, t=include_testing: tif1.get_event_schedule(
                        y, include_testing=t, backend=b
                    )
                )
                out[key] = {"error": err} if err else _frame_digest(frame)
    for dt in REMAINING_DATES:
        for backend in BACKENDS:
            for include_testing in (True, False):
                key = f"remaining/{dt.isoformat()}/{backend}/{include_testing}"
                frame, err = _call(
                    lambda d=dt, b=backend, t=include_testing: tif1.get_events_remaining(
                        d, include_testing=t, backend=b
                    )
                )
                out[key] = {"error": err} if err else _frame_digest(frame)
    queries = [
        (2025, 1),
        (2025, 8),
        (2025, 0),
        (2025, 99),
        (2025, "Bahrain"),
        (2025, "Monza"),
        (2025, "Monacco"),
        (2025, "Pre-Season Testing"),
        (2025, "Moon GP"),
        (2025, "3"),
        (2024, "Spa"),
        (2026, "Pre-Season Testing"),
        (1998, "Monaco"),
        (1998, 3),
        (2021, "Sao Paulo"),
        (2025, None),
        (2025, 8.0),
        (2025, 8.5),
        (2025, True),
        (2025, float("nan")),
        (2030, "Monza"),
        (2030, 1),
        (1950, 1),
        (2025, "Spielberg"),
        (2025, "british"),
        (2025, "Pre-Season Testing 1"),
    ]
    import numpy as np

    queries.append((2025, np.int64(5)))
    for year, gp in queries:
        for exact in (False, True):
            event, err = _call(lambda y=year, g=gp, e=exact: tif1.get_event(y, g, exact_match=e))
            out[f"event/{year}/{gp!r}/{exact}"] = (
                {"error": err} if err else (_series_digest(event) if event is not None else {})
            )
    for year, number in ((2025, 1), (2025, 24), (1998, 16), (2025, 30), (2025, 0), (2025, None)):
        event, err = _call(lambda y=year, n=number: tif1.get_event_by_round(y, n))
        out[f"by_round/{year}/{number}"] = {"error": err} if err else _series_digest(event)
    for year, number in ((2025, 1), (2026, 2), (2024, 1), (2025, 3)):
        event, err = _call(lambda y=year, n=number: tif1.get_testing_event(y, n))
        out[f"testing/{year}/{number}"] = {"error": err} if err else _series_digest(event)
    return out


def _expected_polars(value, column: str):
    import pandas as pd

    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().replace(tzinfo=None)
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if column.endswith("Date") and isinstance(value, str):
        return datetime.fromisoformat(value).replace(tzinfo=None)
    if hasattr(value, "item"):
        return value.item()
    return value


def polars_check() -> int:
    import tif1

    mismatches = 0
    checked = 0

    def compare(label, pd_frame, pl_frame):
        nonlocal mismatches, checked
        checked += 1
        if list(pd_frame.columns) != pl_frame.columns or len(pd_frame) != pl_frame.height:
            mismatches += 1
            print("SHAPE", label, list(pd_frame.columns), pl_frame.columns)
            return
        for column in pd_frame.columns:
            expected = [_expected_polars(v, column) for v in pd_frame[column].tolist()]
            got = pl_frame[column].to_list()
            if expected != got:
                mismatches += 1
                print("VALUES", label, column, expected[:3], got[:3])

    for year in YEARS:
        for backend in BACKENDS:
            for include_testing in (True, False):
                pd_frame, err = _call(
                    lambda y=year, b=backend, t=include_testing: tif1.get_event_schedule(
                        y, include_testing=t, backend=b
                    )
                )
                pl_frame, err2 = _call(
                    lambda y=year, b=backend, t=include_testing: tif1.get_event_schedule(
                        y, include_testing=t, backend=b, lib="polars"
                    )
                )
                if err or err2:
                    if bool(err) != bool(err2):
                        mismatches += 1
                        print("ERROR", year, backend, err, err2)
                    continue
                compare(f"schedule/{year}/{backend}/{include_testing}", pd_frame, pl_frame)
    for dt in REMAINING_DATES:
        for backend in BACKENDS:
            for include_testing in (True, False):
                pd_frame, err = _call(
                    lambda d=dt, b=backend, t=include_testing: tif1.get_events_remaining(
                        d, include_testing=t, backend=b
                    )
                )
                pl_frame, err2 = _call(
                    lambda d=dt, b=backend, t=include_testing: tif1.get_events_remaining(
                        d, include_testing=t, backend=b, lib="polars"
                    )
                )
                if err or err2:
                    if bool(err) != bool(err2):
                        mismatches += 1
                        print("ERROR", dt, backend, err, err2)
                    continue
                compare(f"remaining/{dt}/{backend}/{include_testing}", pd_frame, pl_frame)
    print(f"polars schedules checked {checked}, mismatches {mismatches}")
    return mismatches


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out")
    parser.add_argument("--compare", nargs=2)
    parser.add_argument("--polars", action="store_true")
    args = parser.parse_args()
    if args.compare:
        a, b = (json.loads(Path(p).read_text(encoding="utf-8")) for p in args.compare)
        differing = sorted(k for k in a.keys() | b.keys() if a.get(k) != b.get(k))
        for key in differing:
            print("DIFF", key, a.get(key), b.get(key))
        print(f"{len(a)} surfaces, {len(differing)} differing")
        sys.exit(1 if differing else 0)
    if args.polars:
        sys.exit(1 if polars_check() else 0)
    Path(args.out).write_text(json.dumps(digest(), indent=0) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
