"""Telemetry-based charts.

Contains the speed-trace charts, the four-panel telemetry comparison and the
G-G (acceleration envelope) diagram. The G-G diagram is the only chart that
requires ``scipy`` (``ConvexHull``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import matplotlib.pyplot as plt
import numpy as np
from scipy.spatial import ConvexHull

from . import _acceleration, _common

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

__all__ = [
    "plot_speed_traces",
    "plot_annotated_speed_trace",
    "plot_telemetry_comparison",
    "plot_gg_diagram",
]

_SPEED_FIGSIZE = (12, 6)
_COMPARISON_FIGSIZE = (16, 12)
_GG_FIGSIZE = (12, 12)

_ACTION_COLORS = {"Full Throttle": "lime", "Lift": "grey", "Brake": "red"}


def _driver_team_color(sess, driver: str, fallback: str) -> str:
    """Resolve a driver's team color, falling back for unknown drivers."""
    team = sess.laps[sess.laps["Driver"] == driver]["Team"].dropna()
    if team.empty:
        return fallback
    from tif1.plotting import get_team_color

    return get_team_color(team.iloc[0], sess)


def _fastest_lap_telemetry(sess, driver: str) -> Any:
    """Return the fastest lap's telemetry for one driver (adds distance)."""
    lap = _common.fastest_lap(sess.laps, driver)
    return lap.get_car_data().add_distance()


def plot_speed_traces(
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
    """Plot the fastest-lap speed traces of multiple drivers over distance.

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
        drivers: Driver abbreviations to compare; defaults to the top-2
            finishers.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    drivers = _common.resolve_drivers(sess, drivers, 2)

    traces: list[tuple[str, Any, str]] = []
    for driver in drivers:
        try:
            lap = _common.fastest_lap(sess.laps, driver)
            telemetry = lap.get_car_data().add_distance()
            traces.append((driver, telemetry, _driver_team_color(sess, driver, "#ffffff")))
        except Exception:
            continue
    if not traces:
        raise ValueError("No driver could be processed for the speed traces.")

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _SPEED_FIGSIZE)
    for driver, telemetry, color in traces:
        ax.plot(
            telemetry["Distance"],
            telemetry["Speed"],
            color=color,
            label=driver,
            linewidth=2,
        )

    ax.set_xlabel("Distance (m)", fontsize=12)
    ax.set_ylabel("Speed (km/h)", fontsize=12)
    ax.legend(fontsize=11)
    ax.grid(color="w", which="major", axis="both", alpha=0.3)
    fig.suptitle(
        f"Fastest Lap Speed Comparison\n{sess.event['EventName']} {sess.event.year} {sess.name}",
        fontsize=14,
        fontweight="bold",
    )
    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="speed_traces",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def plot_annotated_speed_trace(
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
) -> tuple[Figure, Axes]:
    """Plot the session fastest lap's speed trace annotated with corner markers.

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

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    fastest_lap = _common.fastest_lap(sess.laps)
    car_data = fastest_lap.get_car_data().add_distance()
    circuit_info = sess.get_circuit_info()

    team_color = _driver_team_color(sess, fastest_lap["Driver"], "#ffffff")

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _SPEED_FIGSIZE)
    ax.plot(
        car_data["Distance"],
        car_data["Speed"],
        color=team_color,
        label=fastest_lap["Driver"],
        linewidth=2,
    )

    v_min = car_data["Speed"].min()
    v_max = car_data["Speed"].max()
    ax.vlines(
        x=circuit_info.corners["Distance"],
        ymin=v_min - 20,
        ymax=v_max + 20,
        linestyles="dotted",
        colors="grey",
    )
    for _, corner in circuit_info.corners.iterrows():
        txt = f"{corner['Number']}{corner['Letter']}"
        ax.text(
            corner["Distance"], v_min - 30, txt, va="center_baseline", ha="center", size="small"
        )

    ax.set_xlabel("Distance (m)", fontsize=12)
    ax.set_ylabel("Speed (km/h)", fontsize=12)
    ax.legend(fontsize=11)
    ax.grid(color="w", which="major", axis="both", alpha=0.3)
    ax.set_ylim(v_min - 40, v_max + 20)
    fig.suptitle(
        f"Speed Trace with Corner Annotations\n{sess.event['EventName']} {sess.event.year} {sess.name}",
        fontsize=14,
        fontweight="bold",
    )
    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="annotated_speed_trace",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def _label_actions(tel: Any) -> Any:
    """Label each telemetry sample as Brake / Full Throttle / Lift and return
    the action segments (max distance per contiguous action)."""
    tel = tel.copy()
    tel.loc[tel["Brake"] > 0, "Action"] = "Brake"
    tel.loc[tel["Throttle"] == 100, "Action"] = "Full Throttle"
    tel.loc[(tel["Brake"] == 0) & (tel["Throttle"] < 100), "Action"] = "Lift"

    tel["ActionID"] = (tel["Action"] != tel["Action"].shift(1)).cumsum()
    actions = (
        tel[["ActionID", "Action", "Distance"]]
        .groupby(["ActionID", "Action"])["Distance"]
        .max()
        .reset_index()
    )
    actions["DistanceDelta"] = actions["Distance"] - actions["Distance"].shift(1)
    actions.loc[0, "DistanceDelta"] = actions.loc[0, "Distance"]
    return actions


def plot_telemetry_comparison(
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
    distance_min: float = 1000,
    distance_max: float = 2500,
) -> tuple[Figure, np.ndarray]:
    """Compare two drivers across speed, longitudinal/lateral acceleration and
    driver actions in a four-panel chart.

    The returned ``ax`` is a numpy array of the four panel axes.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Explicit output path; ``None`` defers to automatic saving
            when enabled via :func:`configure_chart_saving`.
        auto_save: Override the global auto-save setting for this call
            (``True``/``False`` force it on/off; ``None`` follows the config).
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(16, 12)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        drivers: Exactly two driver abbreviations; defaults to the top-2
            finishers.
        distance_min: Lower bound of the distance window analysed.
        distance_max: Upper bound of the distance window analysed.

    Returns:
        A ``(fig, ax)`` tuple where ``ax`` holds the four panel axes.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    drivers = _common.resolve_drivers(sess, drivers, 2)[:2]
    if len(drivers) < 2:
        raise ValueError("plot_telemetry_comparison requires exactly two drivers")

    driver_1, driver_2 = drivers
    tel_d1 = _fastest_lap_telemetry(sess, driver_1)
    tel_d2 = _fastest_lap_telemetry(sess, driver_2)

    lon_acc_d1, lat_acc_d1 = _acceleration.compute_accelerations(tel_d1)
    lon_acc_d2, lat_acc_d2 = _acceleration.compute_accelerations(tel_d2)
    tel_d1["LongAcc"] = lon_acc_d1
    tel_d1["LatAcc"] = lat_acc_d1
    tel_d2["LongAcc"] = lon_acc_d2
    tel_d2["LatAcc"] = lat_acc_d2

    color_d1 = _driver_team_color(sess, driver_1, "#0600ef")
    color_d2 = _driver_team_color(sess, driver_2, "#dc0000")

    actions_d1 = _label_actions(tel_d1)
    actions_d2 = _label_actions(tel_d2)

    avg_speed_d1 = tel_d1.loc[
        (tel_d1["Distance"] >= distance_min) & (tel_d1["Distance"] <= distance_max), "Speed"
    ].mean()
    avg_speed_d2 = tel_d2.loc[
        (tel_d2["Distance"] >= distance_min) & (tel_d2["Distance"] <= distance_max), "Speed"
    ].mean()

    if avg_speed_d1 > avg_speed_d2:
        speed_text = f"{driver_1} {round(avg_speed_d1 - avg_speed_d2, 2)} km/h faster"
    else:
        speed_text = f"{driver_2} {round(avg_speed_d2 - avg_speed_d1, 2)} km/h faster"

    fig, ax = plt.subplots(
        4, figsize=figsize if figsize is not None else _COMPARISON_FIGSIZE, sharex=True
    )

    ax[0].plot(tel_d1["Distance"], tel_d1["Speed"], label=driver_1, color=color_d1, linewidth=2)
    ax[0].plot(tel_d2["Distance"], tel_d2["Speed"], label=driver_2, color=color_d2, linewidth=2)
    ax[0].set_ylabel("Speed (km/h)", fontsize=14)
    ax[0].legend(loc="lower right", fontsize=12)
    ax[0].text(distance_min + 50, 280, speed_text, fontsize=12, color="lime")
    ax[0].grid(True, alpha=0.3)

    ax[1].plot(tel_d1["Distance"], tel_d1["LongAcc"], label=driver_1, color=color_d1, linewidth=2)
    ax[1].plot(tel_d2["Distance"], tel_d2["LongAcc"], label=driver_2, color=color_d2, linewidth=2)
    ax[1].set_ylabel("Long. Acc (g)", fontsize=14)
    ax[1].axhline(0, color="white", linestyle="--", alpha=0.5)
    ax[1].legend(loc="lower right", fontsize=12)
    ax[1].grid(True, alpha=0.3)

    ax[2].plot(tel_d1["Distance"], tel_d1["LatAcc"], label=driver_1, color=color_d1, linewidth=2)
    ax[2].plot(tel_d2["Distance"], tel_d2["LatAcc"], label=driver_2, color=color_d2, linewidth=2)
    ax[2].set_ylabel("Lat. Acc (g)", fontsize=14)
    ax[2].axhline(0, color="white", linestyle="--", alpha=0.5)
    ax[2].legend(loc="lower right", fontsize=12)
    ax[2].grid(True, alpha=0.3)

    for _driver, actions, y_pos in [(driver_1, actions_d1, 0), (driver_2, actions_d2, 1)]:
        previous_end = 0
        for _, action in actions.iterrows():
            ax[3].barh(
                [y_pos],
                action["DistanceDelta"],
                left=previous_end,
                color=_ACTION_COLORS[action["Action"]],
                height=0.8,
            )
            previous_end += action["DistanceDelta"]

    ax[3].set_yticks([0, 1])
    ax[3].set_yticklabels([driver_1, driver_2])
    ax[3].set_xlabel("Distance (m)", fontsize=14)
    ax[3].set_ylabel("Driver Actions", fontsize=14)

    handles = [plt.Rectangle((0, 0), 1, 1, color=_ACTION_COLORS[label]) for label in _ACTION_COLORS]
    ax[3].legend(handles, _ACTION_COLORS.keys(), loc="upper right", fontsize=10)

    for panel in ax:
        panel.set_xlim(distance_min, distance_max)

    fig.suptitle(
        f"{sess.event.year} {sess.event['EventName']} - {sess.name}",
        fontsize=18,
        y=0.995,
    )
    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="telemetry_comparison",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def plot_gg_diagram(
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
) -> tuple[Figure, Axes]:
    """Plot a G-G diagram (lateral vs longitudinal acceleration envelope).

    Scatter points are colored per driver and the performance envelope is
    drawn with a convex hull. Requires ``scipy``.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Explicit output path; ``None`` defers to automatic saving
            when enabled via :func:`configure_chart_saving`.
        auto_save: Override the global auto-save setting for this call
            (``True``/``False`` force it on/off; ``None`` follows the config).
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(12, 12)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        drivers: Driver abbreviations to compare; defaults to the top-3
            finishers.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    drivers = _common.resolve_drivers(sess, drivers, 3)

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _GG_FIGSIZE)
    processed = 0
    for driver in drivers:
        try:
            driver_lap = _common.fastest_lap(sess.laps, driver)
            telemetry = driver_lap.get_telemetry().add_distance()
            lon_acc, lat_acc = _acceleration.compute_accelerations(telemetry)
            color = _driver_team_color(sess, driver, "#ffffff")

            ax.scatter(lat_acc, lon_acc, s=20, alpha=0.4, color=color, label=driver)

            points = np.column_stack([lat_acc, lon_acc])
            points = points[~np.isnan(points).any(axis=1)]
            if len(points) > 3:
                hull = ConvexHull(points)
                for simplex in hull.simplices:
                    ax.plot(
                        points[simplex, 0],
                        points[simplex, 1],
                        color=color,
                        linewidth=2.5,
                        alpha=0.9,
                    )
            processed += 1
        except Exception:
            continue
    if processed == 0:
        raise ValueError("No driver could be processed for the G-G diagram.")

    ax.set_xlabel("Lateral Acceleration (g)", fontsize=14)
    ax.set_ylabel("Longitudinal Acceleration (g)", fontsize=14)
    ax.set_title(
        f"G-G Diagram - {sess.event.year} {sess.event['EventName']} {sess.name}",
        fontsize=16,
        fontweight="bold",
        pad=20,
    )
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.axhline(0, color="white", linestyle="-", alpha=0.5, linewidth=0.8)
    ax.axvline(0, color="white", linestyle="-", alpha=0.5, linewidth=0.8)
    ax.set_aspect("equal")
    ax.legend(loc="lower right", fontsize=12)

    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        chart_name="gg_diagram",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )
