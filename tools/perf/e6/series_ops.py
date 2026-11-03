"""E6 probe: eager pl.Series kernel costs (first call of a process and steady state).

Run in a fresh interpreter: ``python tools/perf/e6/series_ops.py [--numpy]``. Without
``--numpy`` numpy is blocked, so every op listed must be numpy-free; with it the
numpy-mask equivalents are timed too.
"""

from __future__ import annotations

import sys
import time

USE_NUMPY = "--numpy" in sys.argv
if not USE_NUMPY:
    sys.modules["numpy"] = None  # type: ignore[assignment]

import polars as pl  # noqa: E402

N_LAPS = 1128
N_TEL = 40_000
drivers = [f"D{i % 20:02d}" for i in range(N_LAPS)]
laps_driver = pl.Series("Driver", sorted(drivers), dtype=pl.String)
laps_num = pl.Series("LapNumber", [i % 57 + 1 for i in range(N_LAPS)], dtype=pl.Int64)
laps_frame = pl.DataFrame(
    [laps_driver, laps_num]
    + [pl.Series(f"c{j}", [float(i) for i in range(N_LAPS)]) for j in range(30)]
)
tel_time = pl.Series("SessionTime", list(range(0, N_TEL * 250_000_000, 250_000_000)))
tel_frame = pl.DataFrame(
    [tel_time] + [pl.Series(f"c{j}", [float(i) for i in range(N_TEL)]) for j in range(20)]
)


def timed(label, fn, reps=50):
    t0 = time.perf_counter()
    fn()
    first = (time.perf_counter() - t0) * 1e3
    t0 = time.perf_counter()
    for _ in range(reps):
        fn()
    steady = (time.perf_counter() - t0) * 1e3 / reps
    print(f"{label:55s} first {first:7.3f} ms  steady {steady * 1000:8.1f} us")


mask_series = laps_driver == "D05"
timed("str eq (laps)", lambda: laps_driver == "D05")
timed("int eq (laps)", lambda: laps_num == 5)
timed("int ge & le (laps)", lambda: (laps_num >= 5) & (laps_num <= 9))
timed("mask.all()", lambda: mask_series.all())
timed("mask.any()", lambda: mask_series.any())
timed("mask.sum()", lambda: mask_series.sum())
timed("mask.null_count()", lambda: mask_series.null_count())
timed("~mask", lambda: ~mask_series)
timed("mask.not_()", lambda: mask_series.not_())
timed("Series.filter(mask)", lambda: laps_driver.filter(mask_series))
timed(
    "frame cols filter (32 cols)",
    lambda: pl.DataFrame([c.filter(mask_series) for c in laps_frame.get_columns()]),
)
timed(
    "frame._df.gather_with_series(idx) (32)",
    lambda: pl.DataFrame._from_pydf(laps_frame._df.gather_with_series(mask_series.arg_true()._s)),
)
timed("DataFrame.filter(Series)", lambda: laps_frame.filter(mask_series))
timed("arg_true", lambda: mask_series.arg_true())
idx = mask_series.arg_true()
timed("Series[idx Series]", lambda: laps_driver[idx])
timed("Series.gather(idx)", lambda: laps_driver.gather(idx))
timed(
    "Series._s.gather_with_series",
    lambda: pl.Series._from_pyseries(laps_driver._s.gather_with_series(idx._s)),
)
timed("Series[list]", lambda: laps_driver[idx.to_list()])
timed("fill_null(False) bool", lambda: mask_series.fill_null(False))
timed("Series.search_sorted", lambda: tel_time.search_sorted(5_000_000_000))
timed("tel ge & le", lambda: (tel_time >= 1_000_000_000) & (tel_time <= 9_000_000_000))
tmask = (tel_time >= 1_000_000_000) & (tel_time <= 9_000_000_000)
timed(
    "tel frame filter cols (21)",
    lambda: pl.DataFrame([c.filter(tmask) for c in tel_frame.get_columns()]),
)
timed("tel slice(a,b) cols", lambda: tel_frame.slice(4, 32))
timed("tel_time - scalar", lambda: tel_time - 1_000_000_000)
timed("tel_time cast Duration", lambda: tel_time.cast(pl.Duration("ns")))
timed("is_null (laps)", lambda: laps_driver.is_null())
timed("is_not_null (laps)", lambda: laps_driver.is_not_null())
timed("laps_driver.cast(String)", lambda: laps_driver.cast(pl.String))
timed("to_list (laps str)", lambda: laps_driver.to_list())
timed("min/max", lambda: (tel_time.min(), tel_time.max()))
timed("Series arange int_range? pl.int_range eager", lambda: pl.int_range(0, N_LAPS, eager=True))
timed("Series(range)", lambda: pl.Series(range(N_LAPS)))
timed("Series(list bool)", lambda: pl.Series([True] * N_LAPS, dtype=pl.Boolean))

if USE_NUMPY:
    import numpy as np

    keep = mask_series.to_numpy()
    timed("NP: str eq -> to_numpy", lambda: (laps_driver == "D05").to_numpy())
    timed("NP: keep.all()", lambda: keep.all())
    timed("NP: pl.Series(keep)", lambda: pl.Series(keep, dtype=pl.Boolean))
    timed(
        "NP: _filter_rows path (32 cols)",
        lambda: pl.DataFrame(
            [c.filter(pl.Series(keep, dtype=pl.Boolean)) for c in laps_frame.get_columns()]
        ),
    )
    pos = np.flatnonzero(keep)
    timed("NP: Series[np idx]", lambda: laps_driver[pos])
    tn = tel_time.to_numpy()
    timed("NP: tel to_numpy", lambda: tel_time.to_numpy())
    timed("NP: tel ge&le numpy", lambda: (tn >= 1_000_000_000) & (tn <= 9_000_000_000))
    timed("NP: searchsorted", lambda: np.searchsorted(tn, 5_000_000_000))
