"""Track-layout maps colored by telemetry channels.

Contains the five ``plot_track_*`` charts, the gear shift visualization and
the multi-driver speed comparison, all of which draw a ``LineCollection``
along the track geometry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colormaps
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D

from . import _common

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

__all__ = [
    "plot_track_speed_map",
    "plot_track_throttle_map",
    "plot_track_brake_zones",
    "plot_track_acceleration_map",
    "plot_gear_shifts",
    "plot_multi_driver_speed_comparison",
]

_DEFAULT_FIGSIZE = (12, 10)
_GEAR_FIGSIZE = (10, 8)
_MULTI_FIGSIZE = (12, 6.75)


def _draw_segment_map(
    sess,
    *,
    values: Any,
    cmap: str,
    norm: Any,
    cbar_label: str,
    title: str,
    figsize: tuple[float, float],
    cbar_ticks: list[float] | None = None,
    cbar_ticklabels: list[str] | None = None,
) -> tuple[Figure, Axes]:
    """Draw a color-coded track map from a fastest lap's telemetry."""
    fastest_lap = _common.fastest_lap(sess.laps)
    telemetry = fastest_lap.get_car_data()
    x = telemetry["X"]
    y = telemetry["Y"]

    _, segments = _common.track_segments(x, y)
    lc = LineCollection(segments, cmap=cmap, norm=norm, linewidth=4)
    lc.set_array(values)

    fig, ax = plt.subplots(figsize=figsize)
    line = ax.add_collection(lc)
    cbar = plt.colorbar(line, ax=ax, label=cbar_label)
    if cbar_ticks is not None:
        cbar.set_ticks(cbar_ticks)
    if cbar_ticklabels is not None:
        cbar.ax.set_yticklabels(cbar_ticklabels)

    ax.set_aspect("equal")
    ax.axis("off")
    fig.suptitle(title)
    return fig, ax


def _team_color_for_driver(sess, driver: str) -> str:
    """Resolve a driver's team color for the legend (fallback white)."""
    team = sess.laps[sess.laps["Driver"] == driver]["Team"].dropna()
    if team.empty:
        return "#ffffff"
    from tif1.plotting import get_team_color

    return get_team_color(team.iloc[0], sess)


def plot_track_speed_map(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    dpi: int = 300,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = None,
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    cmap: str = "plasma",
) -> tuple[Figure, Axes]:
    """Plot the speed of the fastest lap along the track layout.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Optional output path; nothing is written when ``None``.
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(12, 10)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        cmap: Matplotlib colormap used for the speed values.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    telemetry = _common.fastest_lap(sess.laps).get_car_data()
    speed = telemetry["Speed"]

    norm = plt.Normalize(speed.min(), speed.max())
    fig, ax = _draw_segment_map(
        sess,
        values=speed,
        cmap=cmap,
        norm=norm,
        cbar_label="Speed (km/h)",
        title=f"{sess.event['EventName']} {sess.event.year} - Speed Map",
        figsize=figsize if figsize is not None else _DEFAULT_FIGSIZE,
    )
    return _common.finalize_figure(fig, ax, save_path=save_path, dpi=dpi, facecolor=facecolor)


def plot_track_throttle_map(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    dpi: int = 300,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = None,
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    cmap: str = "RdYlGn",
) -> tuple[Figure, Axes]:
    """Plot the throttle application of the fastest lap along the track layout.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Optional output path; nothing is written when ``None``.
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(12, 10)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        cmap: Matplotlib colormap used for the throttle values.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    telemetry = _common.fastest_lap(sess.laps).get_car_data()
    throttle = telemetry["Throttle"]

    norm = plt.Normalize(0, 100)
    fig, ax = _draw_segment_map(
        sess,
        values=throttle,
        cmap=cmap,
        norm=norm,
        cbar_label="Throttle (%)",
        title=f"{sess.event['EventName']} {sess.event.year} - Throttle Map",
        figsize=figsize if figsize is not None else _DEFAULT_FIGSIZE,
    )
    return _common.finalize_figure(fig, ax, save_path=save_path, dpi=dpi, facecolor=facecolor)


def plot_track_brake_zones(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    dpi: int = 300,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = None,
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    cmap: str = "RdYlGn_r",
) -> tuple[Figure, Axes]:
    """Plot the brake usage of the fastest lap along the track layout.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Optional output path; nothing is written when ``None``.
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(12, 10)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        cmap: Matplotlib colormap used for the brake values.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    telemetry = _common.fastest_lap(sess.laps).get_car_data()
    brake = telemetry["Brake"]

    norm = plt.Normalize(0, 1)
    fig, ax = _draw_segment_map(
        sess,
        values=brake,
        cmap=cmap,
        norm=norm,
        cbar_label="Brake",
        title=f"{sess.event['EventName']} {sess.event.year} - Brake Zones",
        figsize=figsize if figsize is not None else _DEFAULT_FIGSIZE,
        cbar_ticks=[0, 1],
        cbar_ticklabels=["Off", "On"],
    )
    return _common.finalize_figure(fig, ax, save_path=save_path, dpi=dpi, facecolor=facecolor)


def plot_track_acceleration_map(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    dpi: int = 300,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = None,
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    cmap: str = "RdBu_r",
) -> tuple[Figure, Axes]:
    """Plot the longitudinal acceleration of the fastest lap along the track.

    Acceleration is derived from the speed trace with ``numpy.gradient``.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Optional output path; nothing is written when ``None``.
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(12, 10)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        cmap: Matplotlib colormap used for the acceleration values.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    telemetry = _common.fastest_lap(sess.laps).get_car_data()

    speed_ms = telemetry["Speed"] / 3.6
    time_s = telemetry["Time"].dt.total_seconds()
    lon_acc = np.gradient(speed_ms, time_s) / 9.81

    norm = plt.Normalize(lon_acc.min(), lon_acc.max())
    fig, ax = _draw_segment_map(
        sess,
        values=lon_acc,
        cmap=cmap,
        norm=norm,
        cbar_label="Longitudinal Acceleration (g)",
        title=f"{sess.event['EventName']} {sess.event.year} - Acceleration Map",
        figsize=figsize if figsize is not None else _DEFAULT_FIGSIZE,
    )
    return _common.finalize_figure(fig, ax, save_path=save_path, dpi=dpi, facecolor=facecolor)


def plot_gear_shifts(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    dpi: int = 150,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = "#1a1a1a",
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    cmap: str = "Paired",
) -> tuple[Figure, Axes]:
    """Visualize the gear usage of the fastest lap along the track layout.

    Uses ``get_telemetry()`` for the ``nGear`` channel. The colorbar is
    generalized from the top gear actually used rather than a hardcoded
    8-gear range.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Optional output path; nothing is written when ``None``.
        dpi: Resolution used when saving.
        figsize: Figure size; defaults to ``(10, 8)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        cmap: Matplotlib colormap used for the gear values.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    lap = _common.fastest_lap(sess.laps)
    tel = lap.get_telemetry()

    x = np.array(tel["X"].values)
    y = np.array(tel["Y"].values)
    _, segments = _common.track_segments(x, y)
    gear = tel["nGear"].to_numpy().astype(float)

    max_gear = int(np.nanmax(gear))
    cmap_obj = colormaps[cmap]
    lc = LineCollection(segments, norm=plt.Normalize(1, cmap_obj.N + 1), cmap=cmap_obj)
    lc.set_array(gear)
    lc.set_linewidth(4)

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _GEAR_FIGSIZE)
    ax.add_collection(lc)
    ax.axis("equal")
    ax.tick_params(labelleft=False, left=False, labelbottom=False, bottom=False)

    fig.suptitle(
        f"Fastest Lap Gear Shift Visualization\n"
        f"{lap['Driver']} - {sess.event['EventName']} {sess.event.year}",
        fontsize=14,
        fontweight="bold",
    )

    cbar = plt.colorbar(mappable=lc, label="Gear", boundaries=np.arange(1, max_gear + 2))
    cbar.set_ticks(np.arange(1.5, max_gear + 1.5).tolist())
    cbar.set_ticklabels(np.arange(1, max_gear + 1).tolist())

    return _common.finalize_figure(fig, ax, save_path=save_path, dpi=dpi, facecolor=facecolor)


def plot_multi_driver_speed_comparison(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    dpi: int = 300,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = None,
    color_scheme: str | None = "fastf1",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    drivers: list[str] | None = None,
    cmap: str = "plasma",
) -> tuple[Figure, Axes]:
    """Overlay the fastest-lap speed traces of several drivers on the track.

    All drivers share a single axes and a common color normalization across
    all their speeds, so colors are directly comparable. A black background
    track line, a driver legend and a horizontal speed colorbar are added.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Optional output path; nothing is written when ``None``.
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(12, 6.75)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        drivers: Driver abbreviations to overlay; defaults to the top-3
            finishers.
        cmap: Matplotlib colormap used for the speed values.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    drivers = _common.resolve_drivers(sess, drivers, 3)

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _MULTI_FIGSIZE)
    fig.suptitle(f"{sess.event['EventName']} {sess.event.year} - Speed", size=24, y=0.97)
    ax.axis("off")
    ax.set_aspect("equal", adjustable="datalim")

    tel_by_driver: list[tuple[str, Any]] = []
    for driver in drivers:
        try:
            lap = _common.fastest_lap(sess.laps, driver)
            telemetry = lap.get_car_data()
            tel_by_driver.append((driver, telemetry))
        except Exception:
            continue
    if not tel_by_driver:
        raise ValueError("No driver could be processed for the multi-driver speed comparison.")

    all_speeds = np.concatenate([np.asarray(tel["Speed"]) for _, tel in tel_by_driver])
    norm = plt.Normalize(all_speeds.min(), all_speeds.max())
    cmap_obj = colormaps[cmap]

    proxies: list[Line2D] = []
    last_lc: LineCollection | None = None
    for driver, telemetry in tel_by_driver:
        x, y, speed = telemetry["X"], telemetry["Y"], telemetry["Speed"]
        ax.plot(x, y, color="black", linestyle="-", linewidth=16, zorder=0)
        _, segments = _common.track_segments(x, y)
        lc = LineCollection(segments, cmap=cmap_obj, norm=norm, linestyle="-", linewidth=5)
        lc.set_array(speed)
        ax.add_collection(lc)
        last_lc = lc
        proxies.append(
            Line2D([], [], color=_team_color_for_driver(sess, driver), linewidth=2, label=driver)
        )
    if proxies:
        ax.legend(handles=proxies, loc="upper right", fontsize=10)

    assert last_lc is not None  # tel_by_driver is guaranteed non-empty above
    legend = fig.colorbar(last_lc, ax=ax, orientation="horizontal", label="Speed (km/h)", pad=0.05)
    legend.ax.xaxis.label.set_fontsize(12)
    legend.ax.xaxis.set_tick_params(color="white")
    plt.setp(plt.getp(legend.ax.axes, "xticklabels"), color="white")

    return _common.finalize_figure(fig, ax, save_path=save_path, dpi=dpi, facecolor=facecolor)
