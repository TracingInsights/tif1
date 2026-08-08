"""Performance-oriented charts.

Contains the per-driver fastest-lap metrics charts (downforce levels, full
throttle distance) and the tire degradation chart with fuel correction.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import matplotlib.pyplot as plt
import numpy as np

from . import _common

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

__all__ = ["plot_downforce_levels", "plot_throttle_distance", "plot_tire_degradation"]

_BAR_FIGSIZE = (12, 8)
_DEGRADATION_FIGSIZE = (14, 8)


def _per_driver_fastest_telemetry(sess, driver: str) -> Any:
    """Return the fastest lap telemetry (with distance) for one driver."""
    lap = _common.fastest_lap(sess.laps, driver)
    return lap.get_car_data().add_distance()


def _team_color_for_driver(sess, driver: str) -> str:
    """Resolve a driver's team color (fallback white)."""
    team = sess.laps[sess.laps["Driver"] == driver]["Team"].dropna()
    if team.empty:
        return "#ffffff"
    from tif1.plotting import get_team_color

    return get_team_color(team.iloc[0], sess)


def _driver_team(sess, driver: str) -> str:
    """Return a driver's team name (fallback unknown)."""
    team = sess.laps[sess.laps["Driver"] == driver]["Team"].dropna()
    return team.iloc[0] if not team.empty else "Unknown"


def plot_downforce_levels(
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
) -> tuple[Figure, Axes]:
    """Plot a relative downforce metric (avg speed / max speed x 100) per driver.

    Drivers are sorted by the metric; bars show the difference from the
    lowest value with the actual metric annotated.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Optional output path; nothing is written when ``None``.
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(12, 8)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        drivers: Driver abbreviations; all drivers when ``None``.

    Returns:
        The ``(fig, ax)`` pair.

    Raises:
        ValueError: If no driver could be processed.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    if drivers is None:
        drivers = list(sess.laps["Driver"].unique())

    downforce_data: list[dict[str, Any]] = []
    for driver in drivers:
        try:
            telemetry = _per_driver_fastest_telemetry(sess, driver)
            max_speed = telemetry["Speed"].max()
            lap_time = _common.fastest_lap(sess.laps, driver)["LapTime"].total_seconds()
            circuit_length = telemetry["Distance"].max()
            avg_speed = (circuit_length / lap_time) * 3.6
            downforce_data.append(
                {
                    "Driver": driver,
                    "Team": _driver_team(sess, driver),
                    "Metric": (avg_speed / max_speed) * 100,
                    "MaxSpeed": max_speed,
                    "AvgSpeed": avg_speed,
                }
            )
        except Exception:
            continue
    if not downforce_data:
        raise ValueError("No driver could be processed for the downforce levels chart.")

    downforce_data.sort(key=lambda d: d["Metric"], reverse=True)
    min_metric = min(d["Metric"] for d in downforce_data)
    for d in downforce_data:
        d["MetricDiff"] = d["Metric"] - min_metric

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _BAR_FIGSIZE)
    drivers_list = [d["Driver"] for d in downforce_data]
    metrics_diff = [d["MetricDiff"] for d in downforce_data]
    metrics_actual = [d["Metric"] for d in downforce_data]
    colors = [_team_color_for_driver(sess, d["Driver"]) for d in downforce_data]

    ax.barh(drivers_list, metrics_diff, color=colors, alpha=0.8, edgecolor="white", linewidth=1)
    for i, metric in enumerate(metrics_actual):
        ax.text(
            metrics_diff[i] + 0.02, i, f"{metric:.2f}", va="center", fontsize=10, fontweight="bold"
        )

    ax.set_xlabel("Downforce Level (relative difference)", fontsize=12, fontweight="bold")
    ax.set_ylabel("Driver", fontsize=12, fontweight="bold")
    ax.set_title(
        f"{sess.event.year} {sess.event['EventName']} Qualifying - Downforce Levels\n"
        "(Avg Speed / Max Speed x 100)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3, linestyle="--")
    ax.set_xlim(left=0)
    fig.text(
        0.5,
        0.02,
        "Higher values indicate more downforce setup (better cornering, lower top speeds)",
        ha="center",
        fontsize=9,
        style="italic",
        alpha=0.7,
    )
    return _common.finalize_figure(fig, ax, save_path=save_path, dpi=dpi, facecolor=facecolor)


def plot_throttle_distance(
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
    throttle_threshold: float = 98,
) -> tuple[Figure, Axes]:
    """Plot the percentage of the lap spent at (near) full throttle per driver.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Optional output path; nothing is written when ``None``.
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(12, 8)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        drivers: Driver abbreviations; all drivers when ``None``.
        throttle_threshold: Throttle percentage treated as full throttle.

    Returns:
        The ``(fig, ax)`` pair.

    Raises:
        ValueError: If no driver could be processed.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    if drivers is None:
        drivers = list(sess.laps["Driver"].unique())

    throttle_data: list[dict[str, Any]] = []
    for driver in drivers:
        try:
            telemetry = _per_driver_fastest_telemetry(sess, driver)
            telemetry["Distance_delta"] = telemetry["Distance"].diff()
            circuit_length = telemetry["Distance"].max()
            full_throttle = telemetry[telemetry["Throttle"] >= throttle_threshold]
            throttle_distance = full_throttle["Distance_delta"].sum()
            throttle_percentage = (throttle_distance / circuit_length) * 100
            throttle_data.append(
                {
                    "Driver": driver,
                    "Team": _driver_team(sess, driver),
                    "ThrottlePercentage": round(throttle_percentage, 2),
                }
            )
        except Exception:
            continue
    if not throttle_data:
        raise ValueError("No driver could be processed for the throttle distance chart.")

    throttle_data.sort(key=lambda d: d["ThrottlePercentage"], reverse=True)
    min_percentage = min(d["ThrottlePercentage"] for d in throttle_data)
    for d in throttle_data:
        d["PercentageDiff"] = d["ThrottlePercentage"] - min_percentage

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _BAR_FIGSIZE)
    drivers_list = [d["Driver"] for d in throttle_data]
    percentages_diff = [d["PercentageDiff"] for d in throttle_data]
    percentages_actual = [d["ThrottlePercentage"] for d in throttle_data]
    colors = [_team_color_for_driver(sess, d["Driver"]) for d in throttle_data]

    ax.barh(drivers_list, percentages_diff, color=colors, alpha=0.8, edgecolor="white", linewidth=1)
    for i, percentage in enumerate(percentages_actual):
        ax.text(
            percentages_diff[i] + 0.1,
            i,
            f"{percentage:.1f}%",
            va="center",
            fontsize=10,
            fontweight="bold",
        )

    ax.set_xlabel("Distance at Full Throttle (relative difference)", fontsize=12, fontweight="bold")
    ax.set_ylabel("Driver", fontsize=12, fontweight="bold")
    ax.set_title(
        f"{sess.event.year} {sess.event['EventName']} Qualifying - Full Throttle Distance\n"
        f"(% of lap distance at >= {throttle_threshold:.0f}% throttle)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3, linestyle="--")
    ax.set_xlim(left=0)
    fig.text(
        0.5,
        0.02,
        "Higher values indicate more time at full throttle (power-limited tracks)",
        ha="center",
        fontsize=9,
        style="italic",
        alpha=0.7,
    )
    return _common.finalize_figure(fig, ax, save_path=save_path, dpi=dpi, facecolor=facecolor)


def rolling_median(data: np.ndarray, window: int = 5) -> np.ndarray:
    """Simple rolling median for smoothing a 1-D array.

    Args:
        data: Input array.
        window: Window size (odd numbers recommended).

    Returns:
        The smoothed array.
    """
    result = []
    for i in range(len(data)):
        start = max(0, i - window // 2)
        end = min(len(data), i + window // 2 + 1)
        result.append(np.median(data[start:end]))
    return np.array(result)


def plot_tire_degradation(
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
    compounds: list[str] | None = None,
    fuel_correction: float = 0.03,
    min_laps: int = 10,
    smoothing_window: int = 5,
    laptime_cutoff: float | None = 1.07,
) -> tuple[Figure, Axes]:
    """Plot fuel-corrected lap times against tire life per compound.

    Lap 1 and slow laps (slower than the global ``fastest * laptime_cutoff``)
    are excluded, lap times are corrected for fuel burn, and a rolling median
    smooths the degradation trend per compound.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        save_path: Optional output path; nothing is written when ``None``.
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to ``(14, 8)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; ``None`` leaves rcParams untouched.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        compounds: Compounds to plot; defaults to SOFT/MEDIUM/HARD.
        fuel_correction: Estimated lap-time gain per lap from fuel burn (s).
        min_laps: Minimum laps per compound before it is plotted.
        smoothing_window: Window size of the rolling-median smoother.
        laptime_cutoff: Global outlier cutoff multiplier (default 1.07).

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)
    if compounds is None:
        compounds = ["SOFT", "MEDIUM", "HARD"]

    laps = sess.laps.copy()
    laps["LapTimeSeconds"] = laps["LapTime"].dt.total_seconds()
    clean_laps = laps[
        (laps["LapNumber"] > 1)
        & (laps["LapTimeSeconds"] < laps["LapTimeSeconds"].min() * (laptime_cutoff or 1.07))
    ].copy()

    max_lap = clean_laps["LapNumber"].max()
    clean_laps["FuelCorrectedTime"] = clean_laps["LapTimeSeconds"] - (
        fuel_correction * (max_lap - clean_laps["LapNumber"])
    )

    compound_colors = _compound_colors(sess)

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _DEGRADATION_FIGSIZE)
    for compound in compounds:
        color = compound_colors.get(compound, "#888888")
        compound_laps = clean_laps[clean_laps["Compound"] == compound]
        if len(compound_laps) <= min_laps:
            continue

        ax.scatter(
            compound_laps["TyreLife"],
            compound_laps["FuelCorrectedTime"],
            color=color,
            alpha=0.3,
            s=30,
            zorder=1,
        )

        grouped = compound_laps.groupby("TyreLife")["FuelCorrectedTime"].median()
        tire_life = grouped.index.values
        lap_times = grouped.values
        smoothed_times = (
            rolling_median(lap_times, window=smoothing_window) if len(lap_times) > 5 else lap_times
        )

        ax.plot(tire_life, smoothed_times, color=color, linewidth=4, label=compound, zorder=2)
        ax.annotate(
            compound,
            xy=(tire_life[-1], smoothed_times[-1]),
            xytext=(tire_life[-1] + 2, smoothed_times[-1]),
            fontsize=14,
            fontweight="bold",
            color=color,
            va="center",
        )

    ax.set_xlabel("Tire Life (Laps)", fontsize=16, fontweight="bold")
    ax.set_ylabel("Fuel-Corrected Lap Time (s)", fontsize=16, fontweight="bold")
    ax.set_title(
        f"Tire Degradation Analysis - {sess.event['EventName']} {sess.event['EventDate'].year}",
        fontsize=18,
        fontweight="bold",
        pad=20,
    )
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.legend(fontsize=14, loc="upper left", framealpha=0.9)

    if not clean_laps.empty:
        y_min = clean_laps["FuelCorrectedTime"].quantile(0.01)
        y_max = clean_laps["FuelCorrectedTime"].quantile(0.99)
        ax.set_ylim(y_min - 0.5, y_max + 0.5)

    return _common.finalize_figure(fig, ax, save_path=save_path, dpi=dpi, facecolor=facecolor)


def _compound_colors(sess) -> dict[str, str]:
    """Return the season-aware compound color mapping."""
    from tif1.plotting import get_compound_mapping

    return get_compound_mapping(sess)
