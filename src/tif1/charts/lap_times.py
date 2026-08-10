"""Lap-time based charts.

Contains the seaborn-backed lap time charts plus the qualifying grid, lap
delta, position changes and track temperature charts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.patches import Patch

from . import _common

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

__all__ = [
    "plot_driver_laptimes",
    "plot_laptimes_distribution",
    "plot_laptime_heatmap",
    "plot_qualifying_grid",
    "plot_lap_delta",
    "plot_position_changes",
    "plot_track_temperature",
]

_DRIVER_LAPTIMES_FIGSIZE = (10, 8)
_DISTRIBUTION_FIGSIZE = (10, 5)
_HEATMAP_FIGSIZE = (16, 10)
_QUALIFYING_FIGSIZE = (10, 8)
_LAP_DELTA_FIGSIZE = (14, 8)
_POSITION_FIGSIZE = (8.0, 4.9)
_TEMPERATURE_FIGSIZE = (12, 6)

_COMPOUND_ORDER = ["SOFT", "MEDIUM", "HARD"]


def _team_color_for_driver(sess, driver: str) -> str:
    """Resolve a driver's team color (fallback white)."""
    team = sess.laps[sess.laps["Driver"] == driver]["Team"].dropna()
    if team.empty:
        return "#ffffff"
    from tif1.plotting import get_team_color

    return get_team_color(team.iloc[0], sess)


def plot_driver_laptimes(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    auto_save: bool | None = None,
    dpi: int = 150,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = "#1a1a1a",
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    drivers: list[str] | None = None,
    laptime_cutoff: float | None = 1.07,
    include_deleted: bool = False,
) -> tuple[Figure, Axes]:
    """Plot lap times per lap number, colored by tire compound.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Explicit output path; ``None`` defers to automatic saving
            when enabled via :func:`configure_chart_saving`.
        auto_save: Override the global auto-save setting for this call
            (``True``/``False`` force it on/off; ``None`` follows the config).
        dpi: Resolution used when saving.
        figsize: Figure size; defaults to ``(10, 8)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        drivers: Driver abbreviations; defaults to the top-3 finishers.
        laptime_cutoff: Per-driver outlier cutoff (fastest * multiplier).
        include_deleted: Keep deleted laps when ``True``.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    drivers = _common.resolve_drivers(sess, drivers, 3)

    laps = _common.apply_common_filters(
        sess.laps,
        drivers=drivers,
        include_deleted=include_deleted,
        laptime_cutoff=laptime_cutoff,
        laptime_cutoff_scope="per_driver",
        session=sess,
    )
    laps = laps.reset_index(drop=True)

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _DRIVER_LAPTIMES_FIGSIZE)
    sns.scatterplot(
        data=laps,
        x="LapNumber",
        y="LapTime",
        ax=ax,
        hue="Compound",
        palette=_compound_mapping(sess),
        s=80,
        linewidth=0,
        legend="auto",
    )
    ax.set_xlabel("Lap Number", fontsize=12)
    ax.set_ylabel("Lap Time", fontsize=12)
    ax.invert_yaxis()

    if len(drivers) == 1:
        title = f"{drivers[0]} Lap Times in the {sess.event.year} {sess.event['EventName']}"
    else:
        title = f"Top {len(drivers)} Finishers - {sess.event['EventName']} {sess.event.year}"
    fig.suptitle(title, fontsize=14, fontweight="bold")
    ax.grid(color="w", which="major", axis="both", alpha=0.3)
    sns.despine(left=True, bottom=True)

    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="driver_laptimes",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def plot_laptimes_distribution(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    auto_save: bool | None = None,
    dpi: int = 150,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = "#1a1a1a",
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    n_drivers: int = 10,
    laptime_cutoff: float | None = 1.10,
) -> tuple[Figure, Axes]:
    """Plot lap time distributions (violin + swarm) for the point finishers.

    Deleted and pit laps are always excluded, and the per-driver outlier
    cutoff removes slow (safety car / yellow flag) laps.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Explicit output path; ``None`` defers to automatic saving
            when enabled via :func:`configure_chart_saving`.
        auto_save: Override the global auto-save setting for this call
            (``True``/``False`` force it on/off; ``None`` follows the config).
        dpi: Resolution used when saving.
        figsize: Figure size; defaults to ``(10, 5)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        n_drivers: Number of top finishers to include (default 10).
        laptime_cutoff: Per-driver outlier cutoff multiplier (default 1.10).

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    finish_order = _common.finishing_order(sess, n_drivers)

    laps = _common.apply_common_filters(
        sess.laps, n_drivers=n_drivers, finish_order=finish_order, session=sess
    )
    laps = _common.apply_laptime_cutoff(laps, laptime_cutoff, "per_driver")
    laps = laps.reset_index(drop=True)
    laps["LapTime(s)"] = laps["LapTime"].dt.total_seconds()

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _DISTRIBUTION_FIGSIZE)
    sns.violinplot(
        data=laps,
        x="Driver",
        y="LapTime(s)",
        hue="Driver",
        inner=None,
        density_norm="area",
        order=finish_order,
        palette=_driver_colors(sess),
        legend=False,
        ax=ax,
    )
    sns.swarmplot(
        data=laps,
        x="Driver",
        y="LapTime(s)",
        order=finish_order,
        hue="Compound",
        palette=_compound_mapping(sess),
        hue_order=_COMPOUND_ORDER,
        linewidth=0,
        size=4,
        ax=ax,
    )

    ax.set_xlabel("Driver")
    ax.set_ylabel("Lap Time (s)")
    fig.suptitle(f"{sess.event.year} {sess.event['EventName']} Lap Time Distributions")
    sns.despine(left=True, bottom=True)

    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="laptimes_distribution",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def plot_laptime_heatmap(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    auto_save: bool | None = None,
    dpi: int = 300,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = None,
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    include_deleted: bool = False,
    include_pit_laps: bool = False,
    laptime_cutoff: float | None = 1.07,
    cmap: str = "RdYlGn_r",
    xticklabels: int | list[int] = 5,
) -> tuple[Figure, Axes]:
    """Plot a driver x lap heatmap of lap times.

    The color scale is fixed to the fastest lap up to ``fastest * 1.07``
    (or the configured ``laptime_cutoff``) so slow laps stand out.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Explicit output path; ``None`` defers to automatic saving
            when enabled via :func:`configure_chart_saving`.
        auto_save: Override the global auto-save setting for this call
            (``True``/``False`` force it on/off; ``None`` follows the config).
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(16, 10)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        include_deleted: Keep deleted laps when ``True``.
        include_pit_laps: Keep pit laps when ``True``.
        laptime_cutoff: Global outlier cutoff multiplier (default 1.07).
        cmap: Matplotlib colormap for the heatmap.
        xticklabels: Tick spacing for the lap number axis (default 5).

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)

    laps = _common.apply_common_filters(
        sess.laps,
        include_deleted=include_deleted,
        include_pit_laps=include_pit_laps,
        laptime_cutoff=laptime_cutoff,
        laptime_cutoff_scope="global",
    )
    laps = laps.copy()
    laps["LapTimeSeconds"] = laps["LapTime"].dt.total_seconds()

    if laps.empty:
        raise ValueError("No lap data available for the lap time heatmap.")
    fastest_lap = laps["LapTimeSeconds"].min()
    heatmap_data = laps.pivot_table(
        index="Driver", columns="LapNumber", values="LapTimeSeconds", aggfunc="first"
    )
    driver_avg = heatmap_data.mean(axis=1).sort_values()
    heatmap_data = heatmap_data.loc[driver_avg.index]

    vmax_multiplier = laptime_cutoff if laptime_cutoff is not None else 1.07
    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _HEATMAP_FIGSIZE)
    sns.heatmap(
        heatmap_data,
        cmap=cmap,
        vmin=fastest_lap,
        vmax=fastest_lap * vmax_multiplier,
        cbar_kws={"label": "Lap Time (seconds)", "aspect": 40},
        linewidths=0.5,
        linecolor="#1a1a1a",
        xticklabels=xticklabels,
        ax=ax,
    )

    ax.set_xlabel("Lap Number", fontsize=12, fontweight="bold")
    ax.set_ylabel("Driver", fontsize=12, fontweight="bold")
    ax.set_title(
        f"{sess.event.year} {sess.event['EventName']} - Lap Time Heatmap",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.tick_params(axis="y", rotation=0, labelsize=10)
    ax.tick_params(axis="x", labelsize=10)

    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="laptime_heatmap",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def plot_qualifying_grid(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    auto_save: bool | None = None,
    dpi: int = 150,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = "#1a1a1a",
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    n_drivers: int | None = None,
    include_deleted: bool = False,
) -> tuple[Figure, Axes]:
    """Plot the qualifying gaps to pole as a horizontal bar chart.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Explicit output path; ``None`` defers to automatic saving
            when enabled via :func:`configure_chart_saving`.
        auto_save: Override the global auto-save setting for this call
            (``True``/``False`` force it on/off; ``None`` follows the config).
        dpi: Resolution used when saving.
        figsize: Figure size; defaults to ``(10, 8)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        n_drivers: Keep only the top-N finishers (all when ``None``).
        include_deleted: Keep deleted laps when ``True``. Defaults to
            ``False`` (a deleted lap can never win the pole).

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    finish_order = _common.finishing_order(sess, len(sess.drivers))
    laps = _common.apply_common_filters(
        sess.laps,
        n_drivers=n_drivers,
        include_deleted=include_deleted,
        finish_order=finish_order,
        session=sess,
    )

    rows = []
    for drv in pd.unique(laps["Driver"]):
        drv_laps = laps[laps["Driver"] == drv]
        if len(drv_laps) > 0:
            rows.append(drv_laps.loc[drv_laps["LapTime"].idxmin()])

    fastest_laps = pd.DataFrame(rows)
    if fastest_laps.empty:
        raise ValueError("No driver could be processed for the qualifying grid chart.")
    fastest_laps = fastest_laps.sort_values(by="LapTime").reset_index(drop=True)
    pole_lap_time = fastest_laps["LapTime"].iloc[0]
    fastest_laps["LapTimeDelta"] = fastest_laps["LapTime"] - pole_lap_time

    team_colors = [_team_color(sess, lap["Team"]) for _, lap in fastest_laps.iterrows()]

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _QUALIFYING_FIGSIZE)
    ax.barh(
        fastest_laps.index,
        fastest_laps["LapTimeDelta"],
        color=team_colors,
        edgecolor="grey",
        linewidth=0.5,
    )
    ax.set_yticks(fastest_laps.index)
    ax.set_yticklabels(fastest_laps["Driver"])
    # Tight y-limits around the rows: keeps the bars filling the axes no
    # matter how many drivers are selected (15, 22, ...).
    _common.set_tight_barh_ylim(ax, len(fastest_laps))
    ax.invert_yaxis()
    ax.set_axisbelow(True)
    ax.xaxis.grid(True, which="major", linestyle="--", color="white", alpha=0.3, zorder=-1000)
    ax.set_xlabel("Gap to Pole (seconds)", fontsize=11)
    ax.set_ylabel("Driver", fontsize=11)

    pole_driver = fastest_laps["Driver"].iloc[0]
    pole_time_str = (
        str(pole_lap_time).split()[-1]
        if isinstance(pole_lap_time, pd.Timedelta)
        else f"{pole_lap_time:.3f}"
    )
    fig.suptitle(
        f"{sess.event.year} {sess.event['EventName']} Qualifying Results\n"
        f"Pole Position: {pole_time_str} ({pole_driver})",
        fontsize=13,
        fontweight="bold",
    )
    # Branded footer + watermarks from the plot style (footer_y spacing key).
    _common.add_style_branding(fig, _common.resolve_plot_style(color_scheme), ax=ax)
    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="qualifying_grid",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def plot_lap_delta(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    auto_save: bool | None = None,
    dpi: int = 300,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = None,
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    drivers: list[str] | None = None,
    laps: list[int] | None = None,
    include_pit_laps: bool = False,
    laptime_cutoff: float | None = None,
    ylim: tuple[float, float] | None = None,
) -> tuple[Figure, Axes]:
    """Plot the lap-by-lap time delta between two drivers.

    Bars below zero mean the first driver was faster on that lap; bars above
    zero mean the second driver was faster. Bars are colored with the faster
    driver's team color.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Explicit output path; ``None`` defers to automatic saving
            when enabled via :func:`configure_chart_saving`.
        auto_save: Override the global auto-save setting for this call
            (``True``/``False`` force it on/off; ``None`` follows the config).
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(14, 8)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        drivers: Exactly two driver abbreviations; defaults to the top-2
            finishers.
        laps: Restrict the comparison to these lap numbers.
        include_pit_laps: Keep pit laps when ``True``.
        laptime_cutoff: Per-driver outlier cutoff (fastest * multiplier).
        ylim: Explicit y-axis limits; defaults to an auto-scaled range with a
            small pad.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    drivers = _common.resolve_drivers(sess, drivers, 2)[:2]
    if len(drivers) < 2:
        raise ValueError("plot_lap_delta requires exactly two drivers")
    driver_1, driver_2 = drivers

    filtered = _common.apply_common_filters(
        sess.laps,
        drivers=drivers,
        laps=laps,
        include_pit_laps=include_pit_laps,
        laptime_cutoff=laptime_cutoff,
        laptime_cutoff_scope="per_driver",
        session=sess,
    )

    d1_laps = filtered[filtered["Driver"] == driver_1][["LapNumber", "LapTime"]].copy()
    d2_laps = filtered[filtered["Driver"] == driver_2][["LapNumber", "LapTime"]].copy()
    d1_laps["LapTime"] = d1_laps["LapTime"].dt.total_seconds()
    d2_laps["LapTime"] = d2_laps["LapTime"].dt.total_seconds()

    merged = d1_laps.merge(d2_laps, on="LapNumber", suffixes=("_d1", "_d2"))
    if merged.empty:
        raise ValueError("No common laps found for the two drivers.")
    merged["Delta"] = merged["LapTime_d1"] - merged["LapTime_d2"]

    color_1 = _team_color_for_driver(sess, driver_1)
    color_2 = _team_color_for_driver(sess, driver_2)
    merged["Color"] = merged["Delta"].apply(lambda x: color_1 if x < 0 else color_2)

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _LAP_DELTA_FIGSIZE)
    ax.bar(
        merged["LapNumber"],
        merged["Delta"],
        color=merged["Color"],
        width=0.8,
        edgecolor="white",
        linewidth=0.5,
    )
    ax.axhline(0, color="white", linestyle="--", linewidth=1, alpha=0.7)
    ax.set_xlabel("Lap Number", fontsize=14)
    ax.set_ylabel("Time Delta (seconds)", fontsize=14)
    ax.set_title(
        f"{sess.event.year} {sess.event['EventName']} - Lap Time Delta\n{driver_1} vs {driver_2}",
        fontsize=16,
        fontweight="bold",
        pad=20,
    )

    legend_elements = [
        Patch(facecolor=color_1, label=f"{driver_1} faster"),
        Patch(facecolor=color_2, label=f"{driver_2} faster"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=12)
    ax.grid(True, alpha=0.3, axis="y")

    if ylim is None:
        delta_min, delta_max = merged["Delta"].min(), merged["Delta"].max()
        pad = 0.2
        ax.set_ylim(delta_min - pad, delta_max + pad)
    else:
        ax.set_ylim(ylim[0], ylim[1])

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="lap_delta",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def plot_position_changes(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    auto_save: bool | None = None,
    dpi: int = 150,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = None,
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    drivers: list[str] | None = None,
    n_drivers: int | None = None,
) -> tuple[Figure, Axes]:
    """Plot each driver's position over the race laps.

    Axis limits are derived from the data (no hardcoded 20-driver grid), so
    the chart works for any grid size.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Explicit output path; ``None`` defers to automatic saving
            when enabled via :func:`configure_chart_saving`.
        auto_save: Override the global auto-save setting for this call
            (``True``/``False`` force it on/off; ``None`` follows the config).
        dpi: Resolution used when saving.
        figsize: Figure size; defaults to ``(8.0, 4.9)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        drivers: Driver abbreviations; all drivers when ``None``.
        n_drivers: Keep only the top-N finishers (all when ``None``).

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    finish_order = _common.finishing_order(sess, len(sess.drivers))
    laps = _common.apply_common_filters(
        sess.laps,
        drivers=drivers,
        n_drivers=n_drivers,
        finish_order=finish_order,
        session=sess,
    )

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _POSITION_FIGSIZE)
    for drv in laps["Driver"].unique():
        drv_laps = laps[laps["Driver"] == drv].sort_values("LapNumber")
        color = _driver_color(sess, drv)
        ax.plot(drv_laps["LapNumber"], drv_laps["Position"], label=drv, color=color)

    max_pos = int(laps["Position"].max())
    ax.set_ylim(max_pos + 0.5, 0.5)
    ax.set_yticks([1, *range(5, max_pos + 1, 5)])
    ax.set_xlabel("Lap")
    ax.set_ylabel("Position")
    # Vertically centred legend next to the axes: a top-anchored legend grows
    # downward with more entries and can span the whole figure on a 22-driver
    # grid, and one column of 22 labels is taller than the default figure.
    # Centring the anchor keeps it balanced; more than 10 entries wrap into
    # two columns so the legend stays inside the canvas at any grid size. The
    # explicit fontsize also shields it from themes with a large legend font
    # rcParam (e.g. the default-* styles).
    n_entries = len(laps["Driver"].unique())
    ax.legend(
        bbox_to_anchor=(1.02, 0.5),
        loc="center left",
        borderaxespad=0,
        fontsize=11,
        ncol=2 if n_entries > 10 else 1,
    )

    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="position_changes",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def plot_track_temperature(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    auto_save: bool | None = None,
    dpi: int = 150,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = "#1a1a1a",
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    drivers: list[str] | None = None,
) -> tuple[Figure, Axes]:
    """Plot the track temperature across the race.

    Requires the session's weather data. When ``drivers`` is ``None``, the
    driver with the most recorded laps is used; otherwise one line is drawn
    per driver. The legend is anchored outside the axes (vertically centred,
    wrapping to two columns past 10 entries) so it never obscures the plot
    lines at any grid size.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Explicit output path; ``None`` defers to automatic saving
            when enabled via :func:`configure_chart_saving`.
        auto_save: Override the global auto-save setting for this call
            (``True``/``False`` force it on/off; ``None`` follows the config).
        dpi: Resolution used when saving.
        figsize: Figure size; defaults to ``(12, 6)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        drivers: Driver abbreviations; defaults to the driver with the most
            recorded laps.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)

    if drivers is None:
        driver_counts = sess.laps.groupby("Driver", observed=False)["LapNumber"].count()
        if driver_counts.empty:
            raise ValueError("No lap data available for the track temperature chart.")
        drivers = [driver_counts.idxmax()]

    laps = _common.apply_common_filters(sess.laps, drivers=drivers, session=sess)

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _TEMPERATURE_FIGSIZE)
    for driver in drivers:
        driver_laps = laps[laps["Driver"] == driver]
        label = "Track Temperature" if len(drivers) == 1 else driver
        ax.plot(
            driver_laps["LapNumber"],
            driver_laps["TrackTemp"],
            color="#ff4444",
            linewidth=2.5,
            label=label,
        )

    ax.set_xlabel("Lap Number", fontsize=12)
    ax.set_ylabel("Track Temperature (°C)", fontsize=12)
    # Grid-size-adaptive legend: matplotlib's default "best" placement drops a
    # 22-entry legend into the middle of the plot, obscuring the lines, and a
    # top-anchored legend would grow downward past the canvas. Centring it
    # beside the axes keeps it balanced at any grid size; more than 10 entries
    # wrap into two columns so it always fits the figure height. The explicit
    # fontsize also shields it from themes with a large legend font rcParam.
    n_entries = len(drivers)
    ax.legend(
        bbox_to_anchor=(1.02, 0.5),
        loc="center left",
        borderaxespad=0,
        fontsize=11,
        ncol=2 if n_entries > 10 else 1,
    )
    ax.grid(color="w", which="major", axis="both", alpha=0.3)
    fig.suptitle(
        f"Track Temperature - {sess.event['EventName']} {sess.event.year}",
        fontsize=14,
        fontweight="bold",
    )
    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="track_temperature",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def _compound_mapping(sess) -> dict[str, str]:
    """Return the season-aware compound color mapping."""
    from tif1.plotting import get_compound_mapping

    return get_compound_mapping(sess)


def _driver_colors(sess) -> dict[str, str]:
    """Return the driver -> color mapping."""
    from tif1.plotting import get_driver_color_mapping

    return get_driver_color_mapping(sess)


def _driver_color(sess, driver: str) -> str:
    """Return a single driver's color."""
    from tif1.plotting import get_driver_color

    return get_driver_color(driver, sess)


def _team_color(sess, team: str) -> str:
    """Return a single team's color."""
    from tif1.plotting import get_team_color

    return get_team_color(team, sess)
