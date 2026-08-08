"""Shared helpers for the :mod:`tif1.charts` module.

All public chart functions funnel through this module so that the session
loading seam (``_load_session``) and the deterministic lap-data filter
pipeline (``_apply_common_filters``) are defined exactly once.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from matplotlib.figure import Figure

__all__ = [
    "SPEED_TRAP_COLUMNS",
    "apply_laptime_cutoff",
    "driver_colors",
    "fastest_lap",
    "finalize_figure",
    "finishing_order",
    "load_session",
    "resolve_drivers",
    "setup_theme",
    "team_colors",
    "track_segments",
]

#: The four speed trap columns available in lap data.
SPEED_TRAP_COLUMNS = ("SpeedI1", "SpeedI2", "SpeedST", "SpeedFL")


def load_session(
    year: int,
    event: str | int,
    session: str | int,
    *,
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] = "pandas",
) -> Any:
    """Load a session through :func:`tif1.get_session`.

    ``tif1.get_session`` is resolved lazily on first use to avoid import
    cycles. This function is the single seam that unit tests monkeypatch.

    Args:
        year: Season year (2018-current).
        event: Grand Prix name or round number.
        session: Session name (e.g. ``"Q"``, ``"R"``, ``"Qualifying"``).
        enable_cache: Enable caching; ``None`` uses the configured default.
        lib: Data backend; defaults to ``"pandas"`` because the chart logic
            relies on pandas idioms.

    Returns:
        A loaded :class:`tif1.Session`.
    """
    from tif1.core import get_session

    return get_session(year, event, session, enable_cache=enable_cache, lib=lib)


def setup_theme(color_scheme: str | None) -> None:
    """Apply the FastF1 plotting theme, unless ``color_scheme`` is ``None``."""
    if color_scheme is not None:
        from tif1.plotting import setup_mpl

        setup_mpl(color_scheme=color_scheme, mpl_timedelta_support=True)


def finalize_figure(
    fig: Figure,
    ax: Any,
    *,
    save_path: str | None,
    dpi: int,
    facecolor: str | None,
) -> tuple[Figure, Any]:
    """Single source of truth for layout + saving of a chart figure.

    Applies ``fig.tight_layout()``, optionally sets the figure background and
    saves to ``save_path`` with ``bbox_inches="tight"``. ``facecolor`` is only
    forwarded to :meth:`matplotlib.figure.Figure.savefig` when not ``None``,
    preserving the original scripts' transparent/unset background behaviour.

    Args:
        fig: The figure to finalize.
        ax: The chart axes (may be an ndarray of axes for multi-panel charts).
        save_path: Output path, or ``None`` to only return the figure.
        dpi: Dots per inch used when saving.
        facecolor: Background color passed to ``savefig``; ``None`` leaves the
            background unchanged.

    Returns:
        The ``(fig, ax)`` pair.
    """
    fig.tight_layout()
    if facecolor is not None:
        fig.set_facecolor(facecolor)
    if save_path is not None:
        save_kwargs: dict[str, Any] = {"dpi": dpi, "bbox_inches": "tight"}
        if facecolor is not None:
            save_kwargs["facecolor"] = facecolor
        fig.savefig(save_path, **save_kwargs)
    return fig, ax


def finishing_order(session: Any, n: int) -> list[str]:
    """Return the first ``n`` driver abbreviations in finishing order.

    Args:
        session: A loaded session.
        n: Number of top drivers to return.

    Returns:
        Driver abbreviations ordered by finishing position.
    """
    return [session.get_driver(i)["Abbreviation"] for i in session.drivers[:n]]


def resolve_drivers(session: Any, drivers: list[str] | None, n: int) -> list[str]:
    """Resolve the driver list, defaulting to the top ``n`` finishers.

    Args:
        session: A loaded session.
        drivers: Explicit driver abbreviations, or ``None`` to auto-select.
        n: Number of top finishers to auto-select when ``drivers`` is ``None``.

    Returns:
        The resolved driver abbreviation list.
    """
    if drivers is not None:
        return list(drivers)
    return finishing_order(session, n)


def track_segments(x: Any, y: Any) -> tuple[Any, Any]:
    """Build ``(points, segments)`` arrays for a ``LineCollection``.

    Args:
        x: X coordinates of the track telemetry.
        y: Y coordinates of the track telemetry.

    Returns:
        Tuple of ``(points, segments)`` numpy arrays suitable for
        :class:`matplotlib.collections.LineCollection`.
    """
    points = np.array([x, y]).T.reshape(-1, 1, 2)
    segments = np.concatenate([points[:-1], points[1:]], axis=1)
    return points, segments


def fastest_lap(session_laps: Any, driver: str | None = None) -> Any:
    """Return the fastest lap, optionally restricted to one driver.

    Args:
        session_laps: A ``Laps``-like frame.
        driver: Optional driver abbreviation.

    Returns:
        The fastest lap row.
    """
    if driver is not None:
        return session_laps.pick_drivers(driver).pick_fastest()
    return session_laps.pick_fastest()


def driver_colors(session: Any) -> dict[str, str]:
    """Return a mapping of driver abbreviation to team color."""
    from tif1.plotting import get_driver_color_mapping

    return get_driver_color_mapping(session)


def team_colors(session: Any) -> dict[str, str]:
    """Return a mapping of team name to team color."""
    from tif1.plotting import get_team_color, list_team_names

    return {team: get_team_color(team, session) for team in list_team_names(session)}


def apply_laptime_cutoff(
    df: pd.DataFrame, cutoff: float | None, scope: Literal["global", "per_driver"]
) -> pd.DataFrame:
    """Keep laps faster than ``fastest * cutoff``.

    Args:
        df: Lap data frame with a ``LapTime`` column.
        cutoff: Multiplier of the fastest lap used as the upper bound; the
            frame is returned unchanged when ``None``.
        scope: ``"per_driver"`` compares each driver against their own fastest
            lap; ``"global"`` compares all laps against the session fastest.

    Returns:
        The filtered lap data frame.
    """
    if cutoff is None:
        return df
    if scope == "per_driver":
        kept = [
            group[group["LapTime"] < group["LapTime"].min() * cutoff]
            for _, group in df.groupby("Driver")
        ]
        return pd.concat(kept) if kept else df.iloc[0:0]
    return df[df["LapTime"] < df["LapTime"].min() * cutoff]


def _resolve_driver_abbreviations(session: Any, drivers: list[str]) -> list[str]:
    """Fuzzy-resolve driver identifiers against the session (warns on correction)."""
    if session is None:
        return [str(driver) for driver in drivers]
    from tif1.plotting import get_driver_abbreviation

    return [get_driver_abbreviation(driver, session) for driver in drivers]


def apply_common_filters(
    laps_df: pd.DataFrame,
    *,
    drivers: list[str] | None = None,
    teams: list[str] | None = None,
    n_drivers: int | None = None,
    laps: list[int] | None = None,
    include_deleted: bool = False,
    include_pit_laps: bool = False,
    laptime_cutoff: float | None = None,
    laptime_cutoff_scope: Literal["global", "per_driver"] = "global",
    finish_order: list[str] | None = None,
    session: Any = None,
) -> pd.DataFrame:
    """Apply the shared, deterministic lap-data filter pipeline.

    The order of operations is fixed:

    1. ``laps`` — restrict to the given lap numbers.
    2. ``drivers`` — restrict to the given driver abbreviations (fuzzy-resolved
       against the session when available).
    3. ``teams`` — restrict to the given team names (fuzzy-resolved).
    4. ``n_drivers`` — keep only the top-N drivers by finishing position.
    5. ``include_deleted`` — when ``False``, drop deleted laps.
    6. ``include_pit_laps`` — when ``False``, drop in/out pit laps.
    7. ``laptime_cutoff`` — when not ``None``, keep laps faster than
       ``fastest * cutoff`` (global or per-driver scope).

    Args:
        laps_df: Raw lap data frame.
        drivers: Driver abbreviations to keep.
        teams: Team names to keep (expanded to their drivers).
        n_drivers: Keep only the top-N finishers (uses ``finish_order``).
        laps: Lap numbers to keep.
        include_deleted: Keep deleted laps when ``True``.
        include_pit_laps: Keep pit laps when ``True``.
        laptime_cutoff: Optional outlier cutoff multiplier.
        laptime_cutoff_scope: Cutoff scope (``"global"`` or ``"per_driver"``).
        finish_order: Precomputed finishing order used by ``n_drivers``.
        session: Session used for fuzzy driver/team resolution.

    Returns:
        The filtered lap data frame.
    """
    df = laps_df.copy()

    if laps is not None:
        df = df[df["LapNumber"].isin(laps)]

    if drivers is not None:
        df = df[df["Driver"].isin(_resolve_driver_abbreviations(session, list(drivers)))]

    if teams is not None:
        team_drivers: list[str] = []
        for team in teams:
            if session is None:
                team_drivers.append(str(team))
            else:
                from tif1.plotting import get_driver_abbreviations_by_team

                team_drivers.extend(get_driver_abbreviations_by_team(team, session))
        df = df[df["Driver"].isin(team_drivers)]

    if n_drivers is not None and finish_order:
        df = df[df["Driver"].isin(finish_order[:n_drivers])]

    if not include_deleted and "Deleted" in df.columns:
        df = df[~df["Deleted"].fillna(False).astype(bool)]

    if not include_pit_laps:
        keep = pd.Series(True, index=df.index)
        if "PitInTime" in df.columns:
            keep &= df["PitInTime"].isna()
        if "PitOutTime" in df.columns:
            keep &= df["PitOutTime"].isna()
        df = df[keep]

    if laptime_cutoff is not None and "LapTime" in df.columns:
        df = apply_laptime_cutoff(df, laptime_cutoff, laptime_cutoff_scope)

    return df


def event_name(session: Any) -> str:
    """Return the event name of a session (``session.event['EventName']``)."""
    return session.event["EventName"]
