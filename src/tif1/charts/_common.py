"""Shared helpers for the :mod:`tif1.charts` module.

All public chart functions funnel through this module so that the session
loading seam (``_load_session``) and the deterministic lap-data filter
pipeline (``_apply_common_filters``) are defined exactly once.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from matplotlib.figure import Figure

__all__ = [
    "SPEED_TRAP_COLUMNS",
    "add_style_branding",
    "apply_laptime_cutoff",
    "build_save_path",
    "configure_chart_saving",
    "driver_colors",
    "fastest_lap",
    "finalize_figure",
    "finishing_order",
    "fit_labels_inside_xlim",
    "get_chart_save_config",
    "load_session",
    "resolve_drivers",
    "setup_theme",
    "set_tight_barh_ylim",
    "team_colors",
    "track_segments",
]

logger = logging.getLogger(__name__)

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


# ---------------------------------------------------------------------------
# Automatic chart saving
# ---------------------------------------------------------------------------

#: Session identifiers -> filesystem folder slug (sanitized, lowercase).
_SESSION_SLUGS: dict[str, str] = {
    "fp1": "practice-1",
    "practice 1": "practice-1",
    "free practice 1": "practice-1",
    "fp2": "practice-2",
    "practice 2": "practice-2",
    "free practice 2": "practice-2",
    "fp3": "practice-3",
    "practice 3": "practice-3",
    "free practice 3": "practice-3",
    "q": "qualifying",
    "qualifying": "qualifying",
    "qualification": "qualifying",
    "sq": "sprint-qualifying",
    "sprint qualifying": "sprint-qualifying",
    "sprint shootout": "sprint-qualifying",
    "s": "sprint",
    "sprint": "sprint",
    "sprint race": "sprint",
    "r": "race",
    "race": "race",
}


@dataclass
class ChartSaveConfig:
    """Configuration for automatic chart file naming and placement.

    Attributes:
        enabled: When ``True``, charts called without an explicit
            ``save_path`` are written automatically.
        output_dir: Root directory of the auto-save tree (relative or
            absolute; ``~`` is expanded). Defaults to ``tracinginsights``.
        format: File extension, e.g. ``"png"``, ``"svg"``, ``"pdf"``.
        folder_template: Format string for the per-session sub-directory.
            ``{year}``, ``{event}``, ``{session}`` and ``{chart}`` are
            available; every value is sanitized (lowercase, hyphenated).
        filename_template: Format string for the file name (without
            extension), sharing the same placeholders as ``folder_template``.
        overwrite: When ``False`` an unused ``_1``, ``_2``, ... suffix is
            appended instead of overwriting an existing file.
        dpi: Optional dpi override applied only to auto-saved files.
    """

    enabled: bool = False
    output_dir: str | Path = "tracinginsights"
    format: str = "png"
    folder_template: str = "{year}/{event}/{session}"
    filename_template: str = "{chart}"
    overwrite: bool = True
    dpi: int | None = None


#: Module-level auto-save settings (see :func:`configure_chart_saving`).
_CHART_SAVE_CONFIG = ChartSaveConfig()


def get_chart_save_config() -> ChartSaveConfig:
    """Return the current automatic chart-saving configuration.

    Returns:
        The live :class:`ChartSaveConfig` instance; update it via
        :func:`configure_chart_saving` rather than mutating it in place.
    """
    return _CHART_SAVE_CONFIG


def configure_chart_saving(**kwargs: Any) -> None:
    """Configure automatic chart saving.

    When enabled, every chart function called without an explicit
    ``save_path`` writes its figure to
    ``<output_dir>/<year>/<event>/<session>/<chart>.<format>`` using the
    configured templates. Example::

        from tif1 import configure_chart_saving, plot_top_speeds

        configure_chart_saving(
            enabled=True,
            output_dir="tracinginsights",
            folder_template="{year}/{event}/{session}",
            filename_template="{chart}",
            overwrite=True,
            dpi=300,
        )
        plot_top_speeds(2024, "Italian Grand Prix", "Q")
        # writes tracinginsights/2024/italian-grand-prix/qualifying/top_speeds.png

    Every chart also accepts an ``auto_save`` keyword to override the global
    setting for a single call (``True``/``False`` force it on/off, ``None``
    follows the config).

    Args:
        **kwargs: Any :class:`ChartSaveConfig` attribute to update.

    Raises:
        ValueError: If an unknown option is passed.
    """
    config = get_chart_save_config()
    valid = {field.name for field in fields(config)}
    for key, value in kwargs.items():
        if key not in valid:
            raise ValueError(
                f"Unknown chart saving option '{key}'. Valid options: {', '.join(sorted(valid))}"
            )
        if key in ("enabled", "overwrite") and not isinstance(value, bool):
            raise ValueError(
                f"Chart saving option '{key}' must be a bool, got {type(value).__name__}"
            )
        if key == "format" and (not isinstance(value, str) or not value.strip()):
            raise ValueError("Chart saving option 'format' must be a non-empty string")
        if key in ("folder_template", "filename_template") and (
            not isinstance(value, str) or not value.strip()
        ):
            raise ValueError(f"Chart saving option '{key}' must be a non-empty string")
        setattr(config, key, value)


def _slugify(value: Any) -> str:
    """Filesystem-safe lowercase slug for a folder segment."""
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text or "unknown"


def _session_slug(session: Any) -> str:
    """Slug for a session identifier, mapping known codes to full names."""
    lookup = str(session).strip().casefold()
    if lookup in _SESSION_SLUGS:
        return _SESSION_SLUGS[lookup]
    return _slugify(session)


def _safe_stem(value: str) -> str:
    """Sanitize a file stem (keeps underscores, drops path separators)."""
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value).strip(" .")
    return value or "chart"


def _chart_slug(chart_name: Any) -> str:
    """Filesystem-safe lowercase identifier for a chart (keeps underscores)."""
    text = re.sub(r"[^a-zA-Z0-9]+", "_", str(chart_name)).strip("_").lower()
    return _safe_stem(text)


def _uniquify(directory: Path, filename: str) -> str:
    """Return ``filename`` or a ``name_N`` variant that does not exist yet."""
    candidate = directory / filename
    if not candidate.exists():
        return filename
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    index = 1
    while (directory / f"{stem}_{index}{suffix}").exists():
        index += 1
    return f"{stem}_{index}{suffix}"


def build_save_path(
    chart_name: str | None,
    year: int | None,
    event: str | int | None,
    session: str | int | None,
    *,
    config: ChartSaveConfig | None = None,
    enabled: bool | None = None,
) -> str | None:
    """Resolve the automatic save path for a chart.

    Returns ``None`` when auto-saving is disabled (or forced off via
    ``enabled``) or the context is incomplete (no ``chart_name``/``year``).
    The parent directory is **not** created here;
    :func:`finalize_figure` creates it on save.

    Args:
        chart_name: Chart identifier, e.g. ``"top_speeds"``.
        year: Season year used for the ``{year}`` placeholder.
        event: Event (name or round) used for the ``{event}`` placeholder.
        session: Session (name or code) used for the ``{session}``
            placeholder; known codes (``Q``, ``R``, ``FP1``, ...) are mapped
            to their full lowercase names.
        config: Config to use; defaults to the global
            :func:`get_chart_save_config`.
        enabled: Override the config's ``enabled`` flag for this call
            (``None`` follows the config).

    Returns:
        The output path, or ``None``.
    """
    cfg = config if config is not None else get_chart_save_config()
    active = cfg.enabled if enabled is None else enabled
    if not active or not chart_name or year is None:
        return None

    values = {
        "year": _slugify(year),
        "event": _slugify(event),
        "session": _session_slug(session),
        "chart": _chart_slug(chart_name),
    }
    try:
        folder = cfg.folder_template.format(**values).strip("/")
        stem = _safe_stem(cfg.filename_template.format(**values))
    except (KeyError, IndexError, ValueError) as exc:
        raise ValueError(
            f"Invalid chart saving template (folder_template={cfg.folder_template!r}, "
            f"filename_template={cfg.filename_template!r}): {exc}. "
            f"Available placeholders: {{year}}, {{event}}, {{session}}, {{chart}}"
        ) from exc
    filename = f"{stem}.{str(cfg.format).lstrip('.')}"

    base = Path(cfg.output_dir).expanduser()
    directory = base / folder if folder else base
    if not cfg.overwrite:
        filename = _uniquify(directory, filename)
    return str(directory / filename)


def finalize_figure(
    fig: Figure,
    ax: Any,
    *,
    save_path: str | None,
    dpi: int,
    facecolor: str | None,
    tight_layout: bool = True,
    bbox_inches: str | bool | None = "tight",
    label_fit: tuple[Any, list[Any]] | None = None,
    chart_name: str | None = None,
    year: int | None = None,
    event: str | int | None = None,
    session: str | int | None = None,
    auto_save: bool | None = None,
) -> tuple[Figure, Any]:
    """Single source of truth for layout + saving of a chart figure.

    By default applies ``fig.tight_layout()`` and saves to ``save_path`` with
    ``bbox_inches="tight"``. ``facecolor`` is only forwarded to
    :meth:`matplotlib.figure.Figure.savefig` when not ``None``, preserving the
    original scripts' transparent/unset background behaviour. Charts that
    manage their own layout (e.g. the v2 ``Race_Launch_Performance_Ratings``
    look with explicit ``subplots_adjust`` margins and a full-canvas export)
    can opt out with ``tight_layout=False, bbox_inches=None``.

    When ``save_path`` is ``None`` and auto-saving is active (see
    :func:`configure_chart_saving`), the figure is written automatically to
    ``<output_dir>/<year>/<event>/<session>/<chart>.<format>`` — the parent
    directory is created as needed, for auto-saved and explicit paths alike.

    ``label_fit`` runs :func:`fit_labels_inside_xlim` **after** the axes
    geometry is final (post ``tight_layout``), so bar value labels measured
    against the laid-out axes stay inside the x-limit regardless of the
    active theme's font sizes. Callers that fit labels earlier (e.g. the
    race-launch ratings chart, which skips ``tight_layout``) should not pass
    it.

    Args:
        fig: The figure to finalize.
        ax: The chart axes (may be an ndarray of axes for multi-panel charts).
        save_path: Output path, or ``None`` to auto-save (when enabled) or
            only return the figure.
        dpi: Dots per inch used when saving.
        facecolor: Background color passed to ``savefig``; ``None`` leaves the
            background unchanged.
        tight_layout: Run ``fig.tight_layout()`` before saving (default True).
        bbox_inches: Value passed to ``savefig`` as ``bbox_inches``; ``None``
            saves the full canvas (default ``"tight"``).
        label_fit: Optional ``(axes, labels)`` pair passed to
            :func:`fit_labels_inside_xlim` after layout completes.
        chart_name: Chart identifier used for the ``{chart}`` placeholder
            when auto-saving (e.g. ``"top_speeds"``).
        year: Season year used for the ``{year}`` placeholder.
        event: Event used for the ``{event}`` placeholder.
        session: Session used for the ``{session}`` placeholder.
        auto_save: Per-call override of the global auto-save setting; ``None``
            follows :func:`configure_chart_saving`.

    Returns:
        The ``(fig, ax)`` pair. When the figure is saved (explicitly or
        automatically), ``fig._tif1_save_path`` holds the path that was
        written.
    """
    if save_path is None:
        cfg = get_chart_save_config()
        should_auto_save = cfg.enabled if auto_save is None else auto_save
        if should_auto_save:
            resolved = build_save_path(
                chart_name, year, event, session, config=cfg, enabled=should_auto_save
            )
            if resolved is not None:
                save_path = resolved
                if cfg.dpi is not None:
                    dpi = cfg.dpi
    if tight_layout:
        fig.tight_layout()
    if label_fit is not None:
        fit_ax, labels = label_fit
        fit_labels_inside_xlim(fit_ax, labels)
    if facecolor is not None:
        fig.set_facecolor(facecolor)
    if save_path is not None:
        save_path = str(save_path)
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        save_kwargs: dict[str, Any] = {"dpi": dpi}
        if bbox_inches is not None:
            save_kwargs["bbox_inches"] = bbox_inches
        if facecolor is not None:
            save_kwargs["facecolor"] = facecolor
        fig.savefig(save_path, **save_kwargs)
        # Where the figure was written; inspect ``fig._tif1_save_path`` after
        # a chart call to discover auto-saved file locations.
        setattr(fig, "_tif1_save_path", save_path)  # noqa: B010
        logger.info("Saved chart to %s", save_path)
    return fig, ax


def resolve_plot_style(color_scheme: str | None) -> dict[str, Any]:
    """Resolve a plot style dict from a color scheme name.

    ``"default-light"``/``"default-dark"`` map to themselves; any other
    scheme (e.g. the ``"fastf1"`` charts' default) falls back to
    ``"default-dark"``, matching the race-launch ratings chart's resolution
    so every chart shares the same branded look.

    Args:
        color_scheme: A matplotlib theme name, or ``None``.

    Returns:
        A plot style dict from :func:`tif1.plotting.get_plot_style`.
    """
    from tif1.plotting import get_plot_style

    style_name = (
        color_scheme if color_scheme in ("default-light", "default-dark") else "default-dark"
    )
    return get_plot_style(style_name)


def add_style_branding(fig: Any, style: dict[str, Any], *, ax: Any = None) -> None:
    """Add the TracingInsights footer and watermarks from a plot style.

    Reproduces the branded bottom band of the race-launch ratings chart
    (:func:`tif1.charts.performance.plot_race_launch_ratings`): the
    ``TRACINGINSIGHTS.COM`` footer is drawn centred with the style's heading2
    font, and the ``@TracingInsights`` watermarks are placed at the same
    baseline (bottom-left, bottom-right) plus the top-left corner - drawn
    with ``alpha=0`` exactly like the v2 scripts (the original's watermark is
    present but transparent). Font sizes are scaled by the figure height
    relative to the 20-inch reference figure the styles were tuned for, so
    the branding stays proportionally consistent on smaller charts.

    Placement:
        * ``ax`` is ``None`` (full-canvas charts like the race-launch
          ratings): the footer sits at the style's ``spacing.footer_y``
          figure fraction, inside the dedicated bottom margin.
        * ``ax`` is given (tight-bbox charts): the footer is anchored below
          the axes via ``ax.transAxes`` so it never collides with the
          x-axis label, and the ``bbox_inches="tight"`` export crops it in.

    Args:
        fig: The figure to annotate.
        style: A plot style dict (see :func:`resolve_plot_style`).
        ax: Optional axes used to anchor the footer below the plot area.
    """
    from matplotlib import font_manager

    from tif1 import assets

    text_color = style["colors"]["text"]
    footer_y = style["spacing"]["footer_y"]
    watermark = style["watermark"]
    footer = style["footer"]
    logo_font = font_manager.FontProperties(fname=str(assets.font_path(style["fonts"]["logo"])))
    heading2_font = font_manager.FontProperties(
        fname=str(assets.font_path(style["fonts"]["heading2"]))
    )

    # Scale relative to the 20-inch reference figure the styles were tuned
    # for, so branding stays proportional on smaller charts.
    height_in = float(fig.get_size_inches()[1])
    scale = height_in / 20.0
    watermark_size = style["fonts"]["watermark_size"] * scale
    footer_size = style["fonts"]["footer_size"] * scale

    if ax is not None:
        # Below-axes anchor: the footer sits under the x-axis label, in the
        # crop area that bbox_inches="tight" includes. (x labels sit at
        # roughly -0.06 of the axes height; the footer goes further down.)
        footer_transform = ax.transAxes
        footer_y = -0.18
    else:
        footer_transform = None

    # ``fig.text`` only falls back to the figure-fraction transform when the
    # ``transform`` kwarg is absent; passing ``None`` leaves the text in raw
    # pixel coordinates, clipping it at the bottom-left corner on full-canvas
    # exports. Omit the kwarg entirely on the full-canvas path.
    transform_kwargs: dict[str, Any] = (
        {"transform": footer_transform} if footer_transform is not None else {}
    )

    for x in (0.0, 0.9):
        fig.text(
            x=x,
            y=footer_y,
            s=watermark,
            fontdict={"size": watermark_size},
            alpha=0,
            color=text_color,
            zorder=10,
            fontproperties=logo_font,
            fontweight="bold",
            **transform_kwargs,
        )
    fig.text(
        x=0.0,
        y=0.96,
        s=watermark,
        fontdict={"size": watermark_size},
        alpha=0,
        color=text_color,
        zorder=10,
        fontproperties=logo_font,
        fontweight="bold",
    )
    fig.text(
        x=0.5,
        y=footer_y,
        s=footer,
        fontdict={"size": footer_size},
        color=text_color,
        zorder=10,
        fontproperties=heading2_font,
        fontweight="bold",
        ha="center",
        **transform_kwargs,
    )


def set_tight_barh_ylim(ax: Any, n_bars: int) -> None:
    """Tighten the y-limits of a horizontal bar chart around its rows.

    Horizontal bar rows live at integer positions ``0..n_bars-1`` (default
    bars of height 0.8 span ``-0.4..n_bars-0.6``). Without explicit limits,
    matplotlib's default 5% auto-margins leave empty bands above the first
    and below the last row - wasted space on full-canvas exports and
    inconsistent proportions as the number of bars changes. Setting the
    limits to ``(-0.5, n_bars - 0.5)`` hugs the rows with a small uniform
    padding regardless of how many bars (15, 22, ...) are drawn.

    Call **before** ``ax.invert_yaxis()`` so the limits are interpreted in
    pre-inversion data coordinates (the inverted axis then spans the same
    numeric range top-down).

    Note:
        Assumes the default 0.8 bar height. The launch chart's
        ``default-light`` style (height 0.1, negative label padding) calls
        this too; its wider rows are intentional, reproducing the v2
        ``Fastest_Lap.py`` look.

    Args:
        ax: The axes to adjust.
        n_bars: Number of horizontal bars (rows) on the axis.
    """
    ax.set_ylim(-0.5, n_bars - 0.5)


def fit_labels_inside_xlim(ax: Any, labels: Any) -> None:
    """Expand the x-limit until every bar label fits inside the canvas.

    Bar labels are placed past the longest bar with a fixed point offset, so
    on full-canvas exports (``bbox_inches=None``, no cropping) the trailing
    labels can be clipped at the right edge. Widening the x-limit shrinks the
    points-per-data-unit scale, which moves the measured label edge further
    right in data units - so the limit is iterated to a fixed point (up to 8
    passes, converging within ``1e-6``) until every label's window extent
    sits inside the x-limit. A small 0.05 data-unit buffer is added only when
    labels actually overflowed, leaving charts whose labels already fit
    untouched. Works for any number of bars.

    Args:
        ax: The axes whose x-limit is expanded.
        labels: The label text artists to measure (e.g. from ``bar_label``).
    """
    expanded = False
    for _ in range(8):
        ax.figure.canvas.draw()
        rightmost = float(ax.get_xlim()[1])
        for label in labels:
            # renderer=None resolves the canvas renderer internally.
            extent = label.get_window_extent()
            data_x = float(ax.transData.inverted().transform((extent.x1, extent.y0))[0])
            rightmost = max(rightmost, data_x)
        if rightmost <= float(ax.get_xlim()[1]) + 1e-6:
            break
        ax.set_xlim(right=rightmost)
        expanded = True
    if expanded:
        # Breathing room so renderer round-off can never push the widest
        # label past the canvas edge.
        ax.set_xlim(right=float(ax.get_xlim()[1]) + 0.05)


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
