"""Performance-oriented charts.

Contains the per-driver fastest-lap metrics charts (downforce levels, full
throttle distance), the tire degradation chart with fuel correction, and the
race-launch performance ratings chart (v2 ``Race_Launch_Performance_Ratings``
style, rendered with the ``default-dark`` plot style).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from . import _common

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

__all__ = [
    "plot_downforce_levels",
    "plot_race_launch_ratings",
    "plot_throttle_distance",
    "plot_tire_degradation",
]

#: Speed thresholds (km/h) for which lap-1 crossing times are computed.
_SPEED_THRESHOLDS = (50, 100, 150, 200)

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
    value_labels = []
    for i, metric in enumerate(metrics_actual):
        value_labels.append(
            ax.text(
                metrics_diff[i] + 0.02,
                i,
                f"{metric:.2f}",
                va="center",
                fontsize=10,
                fontweight="bold",
            )
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
    # Tight y-limits around the rows: keeps the bars filling the axes no
    # matter how many drivers are selected (15, 22, ...).
    _common.set_tight_barh_ylim(ax, len(drivers_list))
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3, linestyle="--")
    ax.set_xlim(left=0)
    fig.text(
        0.5,
        -0.115,
        "Higher values indicate more downforce setup (better cornering, lower top speeds)",
        ha="center",
        fontsize=9,
        style="italic",
        alpha=0.7,
        transform=ax.transAxes,
    )
    # Branded footer + watermarks from the plot style (footer_y spacing key).
    _common.add_style_branding(fig, _common.resolve_plot_style(color_scheme), ax=ax)
    # Value labels past the longest bar would float outside the axes box when
    # the value spread is small; expand the x-limit after layout so every label
    # fits at any grid size (the launch chart's universal label-fit guarantee).
    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        label_fit=(ax, value_labels),
    )


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
    value_labels = []
    for i, percentage in enumerate(percentages_actual):
        value_labels.append(
            ax.text(
                percentages_diff[i] + 0.1,
                i,
                f"{percentage:.1f}%",
                va="center",
                fontsize=10,
                fontweight="bold",
            )
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
    # Tight y-limits around the rows: keeps the bars filling the axes no
    # matter how many drivers are selected (15, 22, ...).
    _common.set_tight_barh_ylim(ax, len(drivers_list))
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3, linestyle="--")
    ax.set_xlim(left=0)
    fig.text(
        0.5,
        -0.115,
        "Higher values indicate more time at full throttle (power-limited tracks)",
        ha="center",
        fontsize=9,
        style="italic",
        alpha=0.7,
        transform=ax.transAxes,
    )
    # Branded footer + watermarks from the plot style (footer_y spacing key).
    _common.add_style_branding(fig, _common.resolve_plot_style(color_scheme), ax=ax)
    # Value labels past the longest bar would float outside the axes box when
    # the value spread is small; expand the x-limit after layout so every label
    # fits at any grid size (the launch chart's universal label-fit guarantee).
    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        label_fit=(ax, value_labels),
    )


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


# ---------------------------------------------------------------------------
# Race launch performance ratings (v2 Race_Launch_Performance_Ratings style)
# ---------------------------------------------------------------------------


def _speed_crossing_time(time_s: pd.Series, speed: pd.Series, threshold: float) -> float | None:
    """Interpolate the lap-relative time (s) when speed first crosses ``threshold``.

    Returns ``None`` when the speed never rises from at-or-below the threshold
    to above it (mirrors the v2 ``get_speed_time`` helper).
    """
    frame = pd.DataFrame({"t": time_s, "s": speed}).dropna()
    if len(frame) < 2:
        return None
    below = (frame["s"].to_numpy(dtype=float) <= float(threshold)).astype(bool)
    crossing = below[:-1] & ~below[1:]
    if not crossing.any():
        return None
    index = int(np.argmax(crossing))
    t0, t1 = float(frame["t"].iloc[index]), float(frame["t"].iloc[index + 1])
    s0, s1 = float(frame["s"].iloc[index]), float(frame["s"].iloc[index + 1])
    fraction = (float(threshold) - s0) / (s1 - s0) if s1 != s0 else 0.0
    return t0 + fraction * (t1 - t0)


def _lap1_threshold_times(sess, driver: str) -> dict[int, float] | None:
    """Return lap-relative crossing times (s) for the 50/100/150/200 km/h marks.

    Uses each driver's first lap of the session. Returns ``None`` when the
    driver has no lap 1 or no telemetry for it.
    """
    laps = sess.laps.pick_drivers([driver]).pick_lap(1)
    if laps.empty:
        return None
    telemetry = laps.get_car_data()
    if telemetry is None or telemetry.empty or "Speed" not in telemetry.columns:
        return None

    time_series = telemetry["Time"]
    if pd.api.types.is_timedelta64_dtype(time_series):
        time_s = pd.Series(time_series).dt.total_seconds()
    else:
        time_s = pd.to_numeric(time_series, errors="coerce")
    if time_s.empty or pd.isna(time_s.iloc[0]):
        return None
    # Lap-relative seconds: telemetry Time can be session-relative, while the
    # ratings compare times within the lap (offset-invariant either way).
    time_s = time_s - float(time_s.iloc[0])
    speed = pd.to_numeric(telemetry["Speed"], errors="coerce")

    times: dict[int, float] = {}
    for threshold in _SPEED_THRESHOLDS:
        crossing = _speed_crossing_time(time_s, speed, threshold)
        if crossing is not None:
            times[threshold] = crossing
    return times


def plot_race_launch_ratings(
    year: int,
    event: str | int,
    session: str | int,
    *,
    save_path: str | None = None,
    dpi: int = 300,
    figsize: tuple[float, float] | None = None,
    facecolor: str | None = None,
    color_scheme: str | None = "default-dark",
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
    drivers: list[str] | None = None,
    speed_threshold: int = 50,
    speed_range: tuple[int, int] | None = None,
) -> tuple[Figure, Axes]:
    """Plot race-launch performance ratings computed from lap-1 telemetry.

    For every driver the lap-1 telemetry is used to interpolate the time taken
    to reach the 50/100/150/200 km/h marks. A 0-10 rating is derived for the
    chosen speed window (``speed_threshold`` gives a "lights out to X km/h"
    rating, ``speed_range`` a "X to Y km/h" rating) by normalizing the slowest
    driver to 0 and the fastest to 10.

    The chart reproduces the TracingInsights v2 ``Race_Launch_Performance_Ratings.py``
    look: the ``default-dark`` plot style (``#011627`` background, lime text,
    white bar/ytick labels, Coolvetica/Azonix/GreatVibes fonts), bundled car
    images drawn only when the rating exceeds the style's 2.5 ``car_threshold``,
    tyre compound images, footer and watermarks.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"R"``, ``"Race"``, ``"SQ"``).
        save_path: Optional output path; nothing is written when ``None``.
        dpi: Resolution used when saving (default 300).
        figsize: Figure size; defaults to the style's ``(20, 20)``.
        facecolor: Figure background; ``None`` keeps the theme background.
        color_scheme: Matplotlib theme; defaults to ``"default-dark"``. When a
            ``default-*`` style is selected, the chart's visuals (colors, fonts,
            margins, car threshold) follow that style's configuration.
        enable_cache: Caching passthrough to :func:`tif1.get_session`.
        lib: Data backend passthrough (defaults to ``"pandas"``).
        drivers: Driver abbreviations; all drivers when ``None``.
        speed_threshold: One of 50, 100, 150 or 200; the "0-X km/h" rating.
        speed_range: Optional ``(start, end)`` speed window for a range rating;
            takes precedence over ``speed_threshold`` when provided.

    Returns:
        The ``(fig, ax)`` pair.

    Raises:
        ValueError: If ``speed_threshold``/``speed_range`` are invalid, or no
            driver could be processed.
    """
    if speed_range is not None:
        if len(speed_range) != 2:
            raise ValueError("speed_range must be a (start, end) pair")
        start_speed, end_speed = speed_range
        if (
            start_speed not in _SPEED_THRESHOLDS
            or end_speed not in _SPEED_THRESHOLDS
            or start_speed >= end_speed
        ):
            raise ValueError(
                f"speed_range endpoints must be in {_SPEED_THRESHOLDS} with start < end, "
                f"got {speed_range}"
            )
        time_col = f"Time_{start_speed}_{end_speed}"
        title_template = f"Start rating for {{}} \n({start_speed} to {end_speed} kmph)"
    else:
        if speed_threshold not in _SPEED_THRESHOLDS:
            raise ValueError(
                f"speed_threshold must be one of {_SPEED_THRESHOLDS}, got {speed_threshold}"
            )
        time_col = f"Time_{speed_threshold}"
        title_template = f"Start rating for {{}} \n(Lights out to {speed_threshold} kmph)"

    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)

    if drivers is None:
        drivers = list(sess.laps["Driver"].unique())

    rows: list[dict[str, Any]] = []
    for driver in drivers:
        times = _lap1_threshold_times(sess, driver)
        if times is None or times.get(50) is None:
            continue
        lap_row = sess.laps.pick_drivers([driver]).pick_lap(1).iloc[0]
        team = lap_row.get("Team")
        compound = lap_row.get("Compound")
        if not team:
            continue
        # Match the v2 pipeline: crossing times are rounded to 3 decimals
        # BEFORE the rating normalization (the original rounds each Time_XX
        # column via ``.round(3)``, then derives range times from those).
        times_rounded = {k: round(v, 3) for k, v in times.items()}
        entry: dict[str, Any] = {
            "Driver": driver,
            "Team": team,
            "Compound": compound,
            "Time_50": times_rounded.get(50),
            "Time_100": times_rounded.get(100),
            "Time_150": times_rounded.get(150),
            "Time_200": times_rounded.get(200),
        }
        if speed_range is not None:
            start_t = entry[f"Time_{start_speed}"]
            end_t = entry[f"Time_{end_speed}"]
            entry[time_col] = (
                round(end_t - start_t, 3) if end_t is not None and start_t is not None else None
            )
        rows.append(entry)

    rated = [row for row in rows if row[time_col] is not None]
    if not rated:
        raise ValueError("No driver could be processed for the race launch ratings chart.")

    slowest = max(row[time_col] for row in rated)
    fastest = min(row[time_col] for row in rated)
    for row in rated:
        if fastest == slowest:
            row["Rating"] = 10.0
        else:
            row["Rating"] = round(10 - (row[time_col] - fastest) / (slowest - fastest) * 10, 2)
    # Reproduce the original v2 sort exactly: ``groupby("Driver").first()``
    # (which reorders to driver-alphabetical) followed by pandas' default
    # ``sort_values`` (unstable quicksort). This keeps the original's
    # tie-breaking for identical ratings - required for pixel parity.
    rated_df = pd.DataFrame(rated)
    rated_df = rated_df.groupby("Driver").first().reset_index(drop=False)
    rated_df = rated_df.sort_values(by="Rating", ascending=False)
    rated = rated_df.to_dict("records")

    season_year = int(sess.event.year)
    from tif1.plotting import team_code_mapping, team_color_mapping

    color_map = team_color_mapping(season_year)
    code_map = team_code_mapping(season_year)
    for row in rated:
        row["Team_color"] = color_map.get(row["Team"], "#888888")
        row["Team_code"] = code_map.get(row["Team"], "")

    df = pd.DataFrame(rated)

    style = _common.resolve_plot_style(color_scheme)
    text_color = style["colors"]["text"]
    bar_label_color = style["colors"]["bar_label"]
    ytick_color = style["colors"]["ytick"]
    grid_color = style["colors"]["grid"]
    car_threshold = style["images"]["car_threshold"]
    tyre_zoom = style["images"]["tyre_zoom"]
    tyre_x_offset = style["images"]["tyre_x_offset"]
    car_zoom = style["images"]["car_zoom"]
    car_x_offset = style["images"]["car_x_offset"]
    label_padding = style["spacing"]["label_padding"]
    title_size = style["fonts"]["title_size"]
    label_size = style["fonts"]["label_size"]

    from matplotlib import font_manager

    from tif1 import assets

    heading_font = font_manager.FontProperties(
        fname=str(assets.font_path(style["fonts"]["heading"]))
    )
    heading2_font = font_manager.FontProperties(
        fname=str(assets.font_path(style["fonts"]["heading2"]))
    )

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else style["figure"]["size"])
    ax.patch.set_alpha(0)

    x = df["Rating"].tolist()
    y = df["Driver"].tolist()
    time_values = df[time_col].tolist()

    hbars = ax.barh(
        y,
        x,
        color=df["Team_color"].tolist(),
        height=style["bar"]["height"],
        alpha=style["bar"]["alpha"],
        zorder=1000,
    )
    bar_labels = ax.bar_label(
        hbars,
        labels=[f"{value} ({round(time_val, 3)}s)" for value, time_val in zip(x, time_values)],
        padding=label_padding,
        color=bar_label_color,
        fontsize=label_size,
        fontproperties=heading_font,
    )

    # Tight y-limits: without them the default 5% auto-margins leave a wide
    # empty band below the last bar (matching the v2 script's ylim behaviour
    # is not needed - it only wastes canvas space on a full-canvas export).
    _common.set_tight_barh_ylim(ax, len(df))
    ax.invert_yaxis()

    fig.suptitle(
        title_template.format(sess.event["EventName"]),
        fontsize=title_size,
        color=text_color,
        x=0.5,
        y=0.95,
        fontproperties=heading2_font,
        ha="center",
    )

    # Tyre images anchored at the y-axis (v2 layout: (0, row) with a point offset).
    for i, compound in enumerate(df["Compound"]):
        assets.add_tyre_image_at_position(
            ax, 0, i, compound, zoom=tyre_zoom, x_offset=tyre_x_offset
        )

    # Car images only where the bar is wide enough to hold them (style threshold).
    assets.add_car_images(
        ax,
        df,
        season_year,
        x_col="Rating",
        y_col="Driver",
        team_code_col="Team_code",
        zoom=car_zoom,
        x_offset=car_x_offset,
        threshold=car_threshold,
    )

    labels = [f"{index + 1}. {driver}  " for index, driver in enumerate(df["Driver"])]
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(
        labels,
        horizontalalignment="right",
        fontproperties=heading_font,
        color=ytick_color,
        fontsize=label_size,
    )

    ax.tick_params(axis="both", which="both", length=0)
    for spine in ["top", "bottom", "left", "right"]:
        ax.spines[spine].set_visible(False)
    ax.set_xticklabels([])

    # Branded footer + watermarks from the plot style (footer_y spacing key).
    _common.add_style_branding(fig, style)

    ax.axvline(x=0, color=grid_color, linewidth=1, linestyle="--")
    fig.subplots_adjust(
        left=style["spacing"]["subplot_left"],
        right=style["spacing"]["subplot_right"],
        top=style["spacing"]["subplot_top"],
        bottom=style["spacing"]["subplot_bottom"],
    )

    # The right margin is intentionally slim (matches the v2 script's
    # subplots_adjust), so bar labels rendered past the longest bar would
    # otherwise be clipped at the figure edge. Expand the x-limit to a fixed
    # point so every label sits inside the full-canvas export (styles whose
    # labels already fit, e.g. default-light with negative label padding,
    # are left untouched).
    _common.fit_labels_inside_xlim(ax, bar_labels)

    # The v2 script exports the full canvas with its own subplot margins (no
    # tight_layout, no bbox cropping) - preserve that exact output.
    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        tight_layout=False,
        bbox_inches=None,
    )
