"""Top speeds by team chart."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import matplotlib.pyplot as plt

from . import _common

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

__all__ = ["plot_top_speeds"]

_DEFAULT_FIGSIZE = (10, 8)


def plot_top_speeds(
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
    teams: list[str] | None = None,
    speed_trap: str | None = None,
) -> tuple[Figure, Axes]:
    """Plot the maximum speeds recorded by each team at the fastest speed trap.

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
        teams: Restrict the comparison to these teams (fuzzy-resolved).
        speed_trap: One of ``"SpeedI1"``, ``"SpeedI2"``, ``"SpeedST"``,
            ``"SpeedFL"``; defaults to the trap with the highest readings.

    Returns:
        The ``(fig, ax)`` pair.
    """
    _common.setup_theme(color_scheme)
    sess = _common.load_session(year, event, session, enable_cache=enable_cache, lib=lib)

    laps = _common.apply_common_filters(sess.laps, teams=teams, session=sess)

    if speed_trap is None:
        speed_data = laps[list(_common.SPEED_TRAP_COLUMNS)]
        fastest_trap = speed_data.idxmax(axis=1).value_counts().index[0]
    else:
        if speed_trap not in _common.SPEED_TRAP_COLUMNS:
            raise ValueError(
                f"speed_trap must be one of {list(_common.SPEED_TRAP_COLUMNS)}, got {speed_trap!r}"
            )
        fastest_trap = speed_trap

    team_speeds = laps[[fastest_trap, "Team"]].copy()
    max_speeds = team_speeds.groupby("Team")[fastest_trap].max().reset_index()
    max_speeds.columns = ["Team", "MaxSpeed"]
    max_speeds = max_speeds.sort_values("MaxSpeed", ascending=False)
    max_speeds["Diff"] = max_speeds["MaxSpeed"] - max_speeds["MaxSpeed"].min()

    team_colors = {team: _team_color(team, sess) for team in max_speeds["Team"]}
    max_speeds["Color"] = max_speeds["Team"].map(team_colors)

    fig, ax = plt.subplots(figsize=figsize if figsize is not None else _DEFAULT_FIGSIZE)
    ax.barh(
        y=max_speeds["Team"],
        width=max_speeds["Diff"],
        color=max_speeds["Color"],
        height=0.7,
    )

    value_labels = []
    for i, (speed, diff) in enumerate(zip(max_speeds["MaxSpeed"], max_speeds["Diff"])):
        value_labels.append(
            ax.text(
                diff + 0.2, i, f"{int(speed)} km/h", va="center", fontsize=10, fontweight="bold"
            )
        )

    ax.set_xlabel("Speed Difference (km/h)", fontsize=11)
    ax.set_title(
        f"{sess.event.year} {sess.event['EventName']} - Top Speeds by Team",
        fontsize=13,
        fontweight="bold",
        pad=20,
    )
    # Tight y-limits around the rows: keeps the bars filling the axes no
    # matter how many teams/drivers are selected.
    _common.set_tight_barh_ylim(ax, len(max_speeds))
    ax.invert_yaxis()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="x", alpha=0.3, linestyle="--")

    # Branded footer + watermarks from the plot style (footer_y spacing key).
    _common.add_style_branding(fig, _common.resolve_plot_style(color_scheme), ax=ax)
    # Value labels past the longest bar would float outside the axes box;
    # expand the x-limit after layout so every label fits at any grid size.
    return _common.finalize_figure(
        fig,
        ax,
        save_path=save_path,
        dpi=dpi,
        facecolor=facecolor,
        label_fit=(ax, value_labels),
        chart_name="top_speeds",
        year=year,
        event=event,
        session=session,
        auto_save=auto_save,
    )


def _team_color(team: str, sess) -> str:
    """Resolve a team color lazily through :mod:`tif1.plotting`."""
    from tif1.plotting import get_team_color

    return get_team_color(team, sess)
