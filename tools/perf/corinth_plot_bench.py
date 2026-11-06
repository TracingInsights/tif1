"""Corinth C6: fresh-process warm plotting workloads (pandas, 2025 Bahrain).

Each workload runs in a fresh interpreter (``--child <workload>``) against a
primed cache (``TIF1_CACHE_DIR``), offline, with the Agg backend, and prints
its in-process phase times (ms) as one JSON line. With ``C6_DIGEST=<file>``
the child also writes a digest of the figures' data (every line/collection/
patch x/y array, colors, labels, ticks, title) to ``<file>`` for the parity
comparison between two trees (``--compare a.json b.json``).

The scripts follow the fastf1 gallery and tif1's docs: a speed trace, a
lap-time scatter coloured by compound, a tyre-strategy bar chart, a team
color lookup, and the native ``tif1.plot_*`` charts.

Usage (the A/B driver is ``corinth_plot_ab.py``):
    TIF1_CACHE_DIR=/tmp/corinth/warm-C6 python tools/perf/corinth_plot_bench.py \
        --child speed_trace
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

YEAR, GP = 2025, "Bahrain Grand Prix"
WORKLOADS = (
    "import_plotting",
    "team_color",
    "speed_trace",
    "laptime_scatter",
    "strategy",
    "chart_speed_traces",
    "chart_driver_laptimes",
    "chart_position_changes",
)
PNG = "/tmp/corinth/C6_plot.png"


def _digest_figure(fig) -> dict:
    """Return a JSON-able description of a figure's data (not its pixels)."""
    import numpy as np

    def h(arr) -> str:
        a = np.asarray(arr)
        if a.dtype == object:
            return hashlib.sha1(repr(a.tolist()).encode()).hexdigest()[:16]
        return (
            f"{a.dtype}:{a.shape}:"
            + hashlib.sha1(np.ascontiguousarray(a).tobytes()).hexdigest()[:16]
        )

    out = {"axes": [], "texts": [t.get_text() for t in fig.texts]}
    out["size"] = [float(v) for v in fig.get_size_inches()]
    out["facecolor"] = str(fig.get_facecolor())
    for ax in fig.axes:
        rec = {
            "title": ax.get_title(),
            "xlabel": ax.get_xlabel(),
            "ylabel": ax.get_ylabel(),
            "xlim": [float(v) for v in ax.get_xlim()],
            "ylim": [float(v) for v in ax.get_ylim()],
            "facecolor": str(ax.get_facecolor()),
            "xticklabels": [t.get_text() for t in ax.get_xticklabels()],
            "yticklabels": [t.get_text() for t in ax.get_yticklabels()],
            "lines": [
                {
                    "x": h(ln.get_xdata()),
                    "y": h(ln.get_ydata()),
                    "color": str(ln.get_color()),
                    "label": ln.get_label(),
                    "ls": str(ln.get_linestyle()),
                    "marker": str(ln.get_marker()),
                    "ms": float(ln.get_markersize()),
                    "mfc": str(ln.get_markerfacecolor()),
                    "mec": str(ln.get_markeredgecolor()),
                    "mew": float(ln.get_markeredgewidth()),
                    "zorder": ln.get_zorder(),
                }
                for ln in ax.get_lines()
            ],
            "collections": [
                {
                    "type": type(c).__name__,
                    "offsets": h(c.get_offsets()),
                    "facecolors": h(c.get_facecolors()),
                    "edgecolors": h(c.get_edgecolors()),
                    "sizes": h(c.get_sizes()),
                    "linewidths": h(c.get_linewidths()),
                    "array": None if c.get_array() is None else h(c.get_array()),
                    "label": c.get_label(),
                    "zorder": c.get_zorder(),
                }
                for c in ax.collections
            ],
            "children": [type(a).__name__ for a in ax.get_children()],
            "spines": {k: s.get_visible() for k, s in ax.spines.items()},
            "yinverted": bool(ax.yaxis_inverted()),
            "units": [
                type(axis.get_converter()).__name__ if hasattr(axis, "get_converter") else None
                for axis in (ax.xaxis, ax.yaxis)
            ],
            "tickers": [
                type(t).__name__
                for axis in (ax.xaxis, ax.yaxis)
                for t in (axis.get_major_locator(), axis.get_major_formatter())
            ],
            "datalim": [float(v) for v in ax.dataLim.bounds],
            "patches": h(
                [
                    [*p.get_xy(), p.get_width(), p.get_height(), *p.get_facecolor()]
                    for p in ax.patches
                    if hasattr(p, "get_width")
                ]
            )
            if ax.patches
            else None,
            "texts": [t.get_text() for t in ax.texts],
            "legend": None
            if ax.get_legend() is None
            else [ax.get_legend().get_title().get_text()]
            + [t.get_text() for t in ax.get_legend().get_texts()],
        }
        out["axes"].append(rec)
    fig.canvas.draw()
    out["pixels"] = hashlib.sha1(bytes(fig.canvas.buffer_rgba())).hexdigest()
    return out


def _child(workload: str) -> None:
    out: dict[str, float] = {}
    t_proc = time.perf_counter()
    figs: list = []
    extra: dict = {}

    def step(name, fn):
        t0 = time.perf_counter()
        r = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        return r

    import tif1

    out["import_tif1"] = round((time.perf_counter() - t_proc) * 1000, 2)

    if workload == "import_plotting":
        step("import_plotting", lambda: __import__("tif1.plotting"))
    elif workload == "team_color":
        # A script that only wants colors (plotly, a table, a web page).
        from tif1 import plotting

        out["import_plotting"] = round(
            (time.perf_counter() - t_proc) * 1000 - out["import_tif1"], 2
        )
        session = step("get_session", lambda: tif1.get_session(YEAR, GP, "Race"))

        def colors():
            return {
                "team": plotting.get_team_color("Red Bull Racing", session=session),
                "driver": plotting.get_driver_color("VER", session=session),
                "mapping": plotting.get_driver_color_mapping(session),
                "compound": plotting.get_compound_color("SOFT", session=session),
                "style": plotting.get_driver_style("LEC", ["color", "linestyle"], session),
            }

        extra["colors"] = step("colors", colors)
    elif workload == "speed_trace":
        # fastf1 gallery plot_speed_traces.py / survey.py plot.
        import matplotlib.pyplot as plt

        from tif1 import plotting

        out["import_plotting"] = round(
            (time.perf_counter() - t_proc) * 1000 - out["import_tif1"], 2
        )
        step(
            "setup_mpl",
            lambda: plotting.setup_mpl(mpl_timedelta_support=False, color_scheme="fastf1"),
        )
        session = step("get_session", lambda: tif1.get_session(YEAR, GP, "Qualifying"))

        def prep():
            laps = session.laps
            res = {}
            for drv in ("VER", "LEC"):
                lap = laps.pick_drivers(drv).pick_fastest()
                res[drv] = lap.get_car_data().add_distance()
            return res

        tels = step("data", prep)

        def draw():
            fig, ax = plt.subplots(figsize=(8, 5))
            for drv, tel in tels.items():
                style = plotting.get_driver_style(drv, ["color", "linestyle"], session)
                ax.plot(tel["Distance"], tel["Speed"], label=drv, **style)
            ax.set_xlabel("Distance in m")
            ax.set_ylabel("Speed in km/h")
            ax.legend()
            return fig

        figs.append(step("draw", draw))
    elif workload == "laptime_scatter":
        # fastf1 gallery plot_driver_laptimes.py (matplotlib scatter instead of seaborn).
        import matplotlib.pyplot as plt

        from tif1 import plotting

        out["import_plotting"] = round(
            (time.perf_counter() - t_proc) * 1000 - out["import_tif1"], 2
        )
        step(
            "setup_mpl",
            lambda: plotting.setup_mpl(mpl_timedelta_support=False, color_scheme="fastf1"),
        )
        session = step("get_session", lambda: tif1.get_session(YEAR, GP, "Race"))

        def prep():
            laps = session.laps.pick_drivers(["VER", "LEC", "NOR"]).pick_quicklaps().reset_index()
            mapping = plotting.get_compound_mapping(session=session)
            colors = [mapping.get(c, mapping["UNKNOWN"]) for c in laps["Compound"]]
            return laps, colors

        laps, colors = step("data", prep)

        def draw():
            fig, ax = plt.subplots(figsize=(8, 8))
            ax.scatter(laps["LapNumber"], laps["LapTime"].dt.total_seconds(), c=colors, s=30)
            ax.set_xlabel("Lap Number")
            ax.set_ylabel("Lap Time")
            ax.invert_yaxis()
            return fig

        figs.append(step("draw", draw))
    elif workload == "strategy":
        # fastf1 gallery plot_strategy.py.
        import matplotlib.pyplot as plt

        from tif1 import plotting

        out["import_plotting"] = round(
            (time.perf_counter() - t_proc) * 1000 - out["import_tif1"], 2
        )
        session = step("get_session", lambda: tif1.get_session(YEAR, GP, "Race"))

        def prep():
            laps = session.laps
            drivers = [session.get_driver(d)["Abbreviation"] for d in session.drivers]
            stints = laps[["Driver", "Stint", "Compound", "LapNumber"]]
            stints = stints.groupby(["Driver", "Stint", "Compound"]).count().reset_index()
            stints = stints.rename(columns={"LapNumber": "StintLength"})
            return drivers, stints

        drivers, stints = step("data", prep)

        def draw():
            fig, ax = plt.subplots(figsize=(5, 10))
            for driver in drivers:
                driver_stints = stints.loc[stints["Driver"] == driver]
                previous_stint_end = 0
                for _, row in driver_stints.iterrows():
                    compound_color = plotting.get_compound_color(row["Compound"], session=session)
                    ax.barh(
                        y=driver,
                        width=row["StintLength"],
                        left=previous_stint_end,
                        color=compound_color,
                        edgecolor="black",
                        fill=True,
                    )
                    previous_stint_end += row["StintLength"]
            ax.set_xlabel("Lap Number")
            ax.invert_yaxis()
            return fig

        figs.append(step("draw", draw))
    elif workload == "chart_speed_traces":
        fig, _ax = step(
            "chart",
            lambda: tif1.plot_speed_traces(YEAR, GP, "Qualifying", drivers=["VER", "LEC"]),
        )
        figs.append(fig)
    elif workload == "chart_driver_laptimes":
        fig, _ax = step("chart", lambda: tif1.plot_driver_laptimes(YEAR, GP, "Race"))
        figs.append(fig)
    elif workload == "chart_driver_laptimes_variants":
        # Parity coverage: one driver, every driver with deleted laps, qualifying,
        # no theme (no timple converter), and an explicit light theme.
        def charts():
            return [
                tif1.plot_driver_laptimes(YEAR, GP, "Race", drivers=["VER"])[0],
                tif1.plot_driver_laptimes(
                    YEAR, GP, "Race", drivers=None, include_deleted=True, laptime_cutoff=None
                )[0],
                tif1.plot_driver_laptimes(YEAR, GP, "Qualifying", drivers=["NOR", "PIA"])[0],
                tif1.plot_driver_laptimes(YEAR, GP, "Race", drivers=["LEC"], color_scheme=None)[0],
                tif1.plot_driver_laptimes(YEAR, GP, "Race", drivers=["HAM"], color_scheme="light")[
                    0
                ],
            ]

        figs.extend(step("chart", charts))
    elif workload == "chart_position_changes":
        fig, _ax = step("chart", lambda: tif1.plot_position_changes(YEAR, GP, "Race"))
        figs.append(fig)
    else:
        raise SystemExit(f"unknown workload {workload}")

    if figs:
        step("savefig", lambda: figs[0].savefig(PNG, dpi=60))
    digest_path = os.environ.get("C6_DIGEST")
    if digest_path:
        digest = {"workload": workload, "extra": extra, "figs": [_digest_figure(f) for f in figs]}
        digest["modules"] = sorted(
            m for m in ("matplotlib", "matplotlib.pyplot", "pandas", "numpy") if m in sys.modules
        )
        Path(digest_path).write_text(
            json.dumps(digest, indent=1, default=str) + "\n", encoding="utf-8"
        )
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    print(json.dumps(out))


def _compare(a: str, b: str) -> None:
    da = json.loads(Path(a).read_text(encoding="utf-8"))
    db = json.loads(Path(b).read_text(encoding="utf-8"))
    da.pop("modules", None)
    db.pop("modules", None)
    print("EQUAL" if da == db else "DIFFERENT")
    if da != db:
        print(json.dumps(da, indent=1)[:3000])
        print(json.dumps(db, indent=1)[:3000])
        raise SystemExit(1)


def main() -> None:
    if sys.argv[1] == "--child":
        os.environ.setdefault("MPLBACKEND", "Agg")
        _child(sys.argv[2])
    elif sys.argv[1] == "--compare":
        _compare(sys.argv[2], sys.argv[3])


if __name__ == "__main__":
    main()
