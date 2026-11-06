"""Corinth C6 probe: phase split of one native chart call (fresh process, warm cache)."""

from __future__ import annotations

import sys
import time

t0 = time.perf_counter()
marks: list[tuple[str, float]] = []


def mark(name: str) -> None:
    marks.append((name, time.perf_counter()))


import matplotlib as mpl  # noqa: E402

mpl.use("Agg")
mark("import_matplotlib")
import matplotlib.pyplot as plt  # noqa: E402, F401

mark("import_pyplot")
import pandas as pd  # noqa: E402, F401

mark("import_pandas")
import tif1  # noqa: E402

mark("import_tif1")
import tif1.charts  # noqa: E402, F401

mark("import_charts")
if sys.argv[1] in ("driver_laptimes", "distribution", "heatmap"):
    import seaborn as sns  # noqa: F401

    mark("import_seaborn")
from tif1.charts import _common  # noqa: E402

YEAR, GP = 2025, "Bahrain Grand Prix"
sess_name = "Qualifying" if sys.argv[1] in ("speed_traces",) else "Race"
sess = _common.load_session(YEAR, GP, sess_name, enable_cache=None, lib="pandas")
mark("load_session")
_ = sess.laps
mark("laps")
_ = sess.results
mark("results")
fn = {
    "driver_laptimes": tif1.plot_driver_laptimes,
    "speed_traces": lambda *a: tif1.plot_speed_traces(*a, drivers=["VER", "LEC"]),
    "position_changes": tif1.plot_position_changes,
}[sys.argv[1]]
if len(sys.argv) > 2:
    import cProfile
    import pstats

    prof = cProfile.Profile()
    fig, ax = prof.runcall(fn, YEAR, GP, sess_name)
    mark("chart")
    restrict = sys.argv[3] if len(sys.argv) > 3 else ""
    pstats.Stats(prof).sort_stats(sys.argv[2]).print_stats(restrict, 45)
else:
    fig, ax = fn(YEAR, GP, sess_name)
    mark("chart")
fig.canvas.draw()
mark("draw")
prev = t0
for name, t in marks:
    print(f"{name:20s} {1000 * (t - prev):8.1f}")
    prev = t
print(f"{'total':20s} {1000 * (prev - t0):8.1f}")
