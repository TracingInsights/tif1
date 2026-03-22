"""Plotting utilities for tif1, compatible with fastf1.plotting."""

from __future__ import annotations

import importlib
import unicodedata
import warnings
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import cycler  # type: ignore[attr-defined]

from tif1.fuzzy import fuzzy_matcher
from tif1.plotting_constants import DEFAULT_COMPOUND_COLORS, YEAR_CONSTANTS

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.legend import Legend

# Default colormap
_DEFAULT_COLORMAP: Literal["fastf1", "official"] = "fastf1"

_SESSION_TEAM_OVERRIDES: dict[Any, dict[str, dict[str, str]]] = {}
_PLOTTING_MAPPINGS: dict[Any, _DriverTeamMapping] = {}
_TIMPLE_IMPORT_WARNING_SHOWN = False
_COLOR_PALETTE = ["#FF79C6", "#50FA7B", "#8BE9FD", "#BD93F9", "#FFB86C", "#FF5555", "#F1FA8C"]
_UNKNOWN_PLOTTING_COLOR = "#ffffff"

# Default plotting configuration
DEFAULT_PLOT_CONFIG = {
    "figure": {
        "size": (20, 20),
        "dpi": 300,
        "constrained_layout": True,
    },
    "fonts": {
        "title_size": 48,
        "label_size": 32,
        "annotation_size": 25,
    },
    "colors": {
        "background": "lightblue",
        "text": "black",
        "grid": "black",
    },
    "bar": {
        "height": 0.1,
        "alpha": 1.0,
        "linewidth": 0.1,
    },
    "images": {
        "tyre_zoom": 0.07,
        "car_zoom": 0.5,
    },
    "spacing": {
        "label_padding": -330,
        "x_margin": 0.4,
    },
}


@dataclass(slots=True)
class _DriverInfo:
    abbreviation: str
    full_name: str
    first_name: str
    last_name: str
    team_key: str
    order: int


@dataclass(slots=True)
class _TeamInfo:
    key: str
    name: str
    short_name: str
    official_color: str
    fastf1_color: str
    order: int
    aliases: set[str] = field(default_factory=set)
    driver_abbreviations: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _DriverTeamMapping:
    year: int | None
    drivers: list[_DriverInfo]
    teams: list[_TeamInfo]
    drivers_by_abbreviation: dict[str, _DriverInfo]
    teams_by_key: dict[str, _TeamInfo]


def setup_mpl(
    mpl_timedelta_support: bool | str = True,
    color_scheme: str | None = None,
    *,
    misc_mpl_mods: bool = True,
    **kwargs: Any,
) -> None:
    """Setup matplotlib for F1 plotting.

    Args:
        mpl_timedelta_support: Enable timedelta support. A positional string is
            also accepted for backwards compatibility and interpreted as
            ``color_scheme``.
        color_scheme: Color scheme to use ('fastf1', 'light', or None)
        misc_mpl_mods: Apply style-related matplotlib changes
        **kwargs: Additional configuration overrides

    Supported kwargs for 'light' scheme:
        - background: Background color (default: 'lightblue')
        - text_color: Text color (default: 'black')
    """
    if isinstance(mpl_timedelta_support, str):
        color_scheme = mpl_timedelta_support
        mpl_timedelta_support = False

    if mpl_timedelta_support:
        _enable_timple()

    if not misc_mpl_mods:
        return

    if color_scheme == "fastf1":
        _enable_fastf1_color_scheme()
    elif color_scheme == "light":
        _enable_light_color_scheme(
            background=kwargs.get("background", "lightblue"),
            text_color=kwargs.get("text_color", "black"),
        )


def apply_plot_style(
    background: str | None = None,
    text_color: str | None = None,
    transparent: bool = False,
):
    """Apply custom plot styling.

    Args:
        background: Background color (uses default if None)
        text_color: Text color (uses default if None)
        transparent: Make background transparent
    """
    if background is None:
        background = str(DEFAULT_PLOT_CONFIG["colors"]["background"])
    if text_color is None:
        text_color = str(DEFAULT_PLOT_CONFIG["colors"]["text"])

    if transparent:
        plt.rcParams["figure.facecolor"] = "none"
        plt.rcParams["axes.facecolor"] = "none"
    else:
        plt.rcParams["figure.facecolor"] = background
        plt.rcParams["axes.facecolor"] = background

    plt.rcParams["text.color"] = text_color
    plt.rcParams["axes.labelcolor"] = text_color
    plt.rcParams["xtick.color"] = text_color
    plt.rcParams["ytick.color"] = text_color
    plt.rcParams["axes.edgecolor"] = text_color


def load_custom_font(font_path_or_url: str):
    """Load a custom font from a file path or URL.

    Args:
        font_path_or_url: Path to font file or URL

    Returns:
        FontProperties object if successful, None otherwise
    """
    from matplotlib import font_manager

    try:
        if font_path_or_url.startswith(("http://", "https://")):
            import tempfile
            from io import BytesIO
            from urllib.request import urlopen

            with urlopen(font_path_or_url) as response:
                font_data = BytesIO(response.read())
                with tempfile.NamedTemporaryFile(delete=False, suffix=".ttf") as tmp:
                    tmp.write(font_data.getvalue())
                    tmp_path = tmp.name

            font_manager.fontManager.addfont(tmp_path)
            return font_manager.FontProperties(fname=tmp_path)

        font_manager.fontManager.addfont(font_path_or_url)
        return font_manager.FontProperties(fname=font_path_or_url)
    except Exception:
        return None


def get_plot_config(key: str | None = None):
    """Get plotting configuration.

    Args:
        key: Specific config key to retrieve (e.g., 'figure', 'fonts', 'colors')
             If None, returns entire config dict

    Returns:
        Configuration value or entire config dict
    """
    if key is None:
        return DEFAULT_PLOT_CONFIG.copy()
    return DEFAULT_PLOT_CONFIG.get(key, {}).copy()


def set_default_colormap(colormap: Literal["fastf1", "official"]) -> None:
    """Set the default colormap for color lookups.

    Args:
        colormap: One of 'fastf1' or 'official'

    Raises:
        ValueError: If colormap is not 'fastf1' or 'official'
    """
    global _DEFAULT_COLORMAP
    if colormap not in ("fastf1", "official"):
        raise ValueError(f"Invalid colormap '{colormap}'")
    _DEFAULT_COLORMAP = colormap


def _enable_fastf1_color_scheme() -> None:
    """Apply FastF1's plotting color scheme."""
    plt.rcParams["figure.facecolor"] = "#292625"
    plt.rcParams["axes.edgecolor"] = "#2d2928"
    plt.rcParams["xtick.color"] = "#f1f2f3"
    plt.rcParams["ytick.color"] = "#f1f2f3"
    plt.rcParams["axes.labelcolor"] = "#f1f2f3"
    plt.rcParams["axes.facecolor"] = "#1e1c1b"
    plt.rcParams["axes.titlesize"] = "19"
    plt.rcParams["font.weight"] = "medium"
    plt.rcParams["text.color"] = "#f1f1f3"
    plt.rcParams["axes.titlepad"] = 12
    plt.rcParams["axes.titleweight"] = "light"
    plt.rcParams["axes.prop_cycle"] = cycler("color", _COLOR_PALETTE)
    plt.rcParams["legend.fancybox"] = False
    plt.rcParams["legend.facecolor"] = (0.1, 0.1, 0.1, 0.7)
    plt.rcParams["legend.edgecolor"] = (0.1, 0.1, 0.1, 0.9)
    plt.rcParams["savefig.transparent"] = False
    plt.rcParams["axes.axisbelow"] = True


def _enable_light_color_scheme(*, background: str, text_color: str) -> None:
    """Apply tif1's light plotting color scheme."""
    plt.rcParams["axes.facecolor"] = background
    plt.rcParams["figure.facecolor"] = background
    plt.rcParams["axes.edgecolor"] = text_color
    plt.rcParams["axes.labelcolor"] = text_color
    plt.rcParams["xtick.color"] = text_color
    plt.rcParams["ytick.color"] = text_color
    plt.rcParams["text.color"] = text_color


def _enable_timple() -> None:
    """Enable timedelta plotting support via the optional timple dependency."""
    global _TIMPLE_IMPORT_WARNING_SHOWN

    try:
        timple = importlib.import_module("timple")
    except ImportError:
        if not _TIMPLE_IMPORT_WARNING_SHOWN:
            warnings.warn(
                "Failed to import optional dependency 'timple'. "
                "Plotting of timedelta values will be restricted. "
                "Install tif1 with the 'plotting' extra for full setup_mpl support.",
                RuntimeWarning,
                stacklevel=2,
            )
            _TIMPLE_IMPORT_WARNING_SHOWN = True
        return

    tick_formats = ["%d %d ay", "%H:00", "%H:%m", "%M:%s.0", "%M:%s.%ms"]
    tmpl = timple.Timple(
        converter="concise",
        formatter_args={"show_offset_zero": False, "formats": tick_formats},
    )
    tmpl.enable()


def _normalize_identifier(identifier: str) -> str:
    """Normalize identifier for matching (lowercase, ASCII, compact spacing)."""
    normalized = (
        unicodedata.normalize("NFKD", str(identifier)).encode("ascii", "ignore").decode("ascii")
    )
    return " ".join(normalized.casefold().split())


def _normalize_team_identifier(identifier: str) -> str:
    """Normalize team identifier and remove common non-unique words."""
    normalized = _normalize_identifier(identifier)
    for word in ("racing", "team", "f1", "scuderia"):
        normalized = normalized.replace(word, " ")
    return " ".join(normalized.split())


def _validate_colormap(colormap: str) -> str:
    """Validate colormap value and resolve the default alias."""
    if colormap == "default":
        return _DEFAULT_COLORMAP
    if colormap not in ("fastf1", "official"):
        raise ValueError(f"Invalid colormap '{colormap}'")
    return colormap


def _get_session_year(session: Any) -> int | None:
    """Extract year from session object."""
    if session is None:
        return None

    if hasattr(session, "year"):
        try:
            return int(session.year)
        except (ValueError, TypeError):
            pass

    if hasattr(session, "event") and hasattr(session.event, "year"):
        try:
            return int(session.event.year)
        except (ValueError, TypeError):
            pass

    if hasattr(session, "event"):
        event = session.event
        try:
            event_date = event["EventDate"]
            if hasattr(event_date, "year"):
                return int(event_date.year)
        except Exception:
            pass
        if isinstance(event, dict):
            for key in ("year", "Year"):
                year = event.get(key)
                if year is not None:
                    try:
                        return int(year)
                    except (ValueError, TypeError):
                        pass

    if hasattr(session, "session_info") and isinstance(session.session_info, dict):
        year = session.session_info.get("year")
        if year is not None:
            try:
                return int(year)
            except (ValueError, TypeError):
                pass

    return None


def _team_aliases(team_key: str, short_name: str) -> set[str]:
    """Build common aliases for a team."""
    aliases = {team_key, short_name}

    normalized = short_name.casefold()
    if normalized == "red bull":
        aliases.update({"Red Bull Racing", "red bull racing"})
    elif normalized == "rb":
        aliases.update({"Racing Bulls", "racing bulls", "RB", "rb"})
    elif normalized == "sauber":
        aliases.update({"Kick Sauber", "kick sauber", "Sauber", "sauber"})
    elif normalized == "alphatauri":
        aliases.update({"AlphaTauri", "alphatauri"})
    elif normalized == "toro rosso":
        aliases.update({"Toro Rosso", "toro rosso"})
    elif normalized == "force india":
        aliases.update({"Force India", "force india"})
    elif normalized == "racing point":
        aliases.update({"Racing Point", "racing point"})

    return aliases


def _get_session_overrides(session: Any) -> dict[str, dict[str, str]]:
    """Return session-scoped team overrides."""
    if session is None:
        return {}
    return _SESSION_TEAM_OVERRIDES.get(session, {})


def _get_override_short_name(team_key: str, session: Any) -> str | None:
    """Return an overridden short name for a team if available."""
    return _get_session_overrides(session).get(team_key, {}).get("short_name")


def _team_profiles_for_year(year: int | None) -> dict[str, dict[str, Any]]:
    """Return team profiles for a season."""
    if year is None or year not in YEAR_CONSTANTS:
        return {}

    profiles: dict[str, dict[str, Any]] = {}
    for team_key, team_data in YEAR_CONSTANTS[year].get("teams", {}).items():
        key = _normalize_team_identifier(team_key)
        profiles[key] = {
            "name": team_data["short_name"],
            "short_name": team_data["short_name"],
            "official_color": team_data["colors"]["official"],
            "fastf1_color": team_data["colors"]["fastf1"],
            "aliases": _team_aliases(team_key, team_data["short_name"]),
        }
    return profiles


def _resolve_team_profile(
    team_name: str, year: int | None
) -> tuple[str, str, str, str, str, set[str]]:
    """Resolve a team's canonical plotting profile."""
    normalized_name = _normalize_team_identifier(team_name)
    profiles = _team_profiles_for_year(year)

    profile = profiles.get(normalized_name)
    if profile is not None:
        aliases = {str(alias) for alias in profile["aliases"]}
        aliases.add(team_name)
        return (
            normalized_name,
            team_name,
            str(profile["short_name"]),
            str(profile["official_color"]),
            str(profile["fastf1_color"]),
            aliases,
        )

    for key, candidate in profiles.items():
        aliases = {str(alias) for alias in candidate["aliases"]}
        aliases.add(str(candidate["name"]))
        if any(_normalize_team_identifier(alias) == normalized_name for alias in aliases):
            aliases.add(team_name)
            return (
                key,
                team_name,
                str(candidate["short_name"]),
                str(candidate["official_color"]),
                str(candidate["fastf1_color"]),
                aliases,
            )

    return (
        normalized_name,
        team_name,
        _shorten_team_name(team_name),
        _UNKNOWN_PLOTTING_COLOR,
        _UNKNOWN_PLOTTING_COLOR,
        {team_name},
    )


def _iter_session_rows(session: Any) -> list[dict[str, str]]:
    """Return canonical plotting rows for a session."""
    rows: list[dict[str, str]] = []

    results = getattr(session, "results", None)
    try:
        if results is not None and hasattr(results, "iterrows") and not results.empty:
            for _, row in results.iterrows():
                abbreviation = str(row.get("Abbreviation", row.get("Driver", ""))).strip()
                team_name = str(row.get("TeamName", row.get("Team", ""))).strip()
                if not abbreviation or not team_name:
                    continue
                first_name = str(row.get("FirstName", "")).strip()
                last_name = str(row.get("LastName", "")).strip()
                full_name = (
                    str(row.get("FullName", "")).strip() or f"{first_name} {last_name}".strip()
                )
                rows.append(
                    {
                        "abbreviation": abbreviation,
                        "team_name": team_name,
                        "first_name": first_name,
                        "last_name": last_name,
                        "full_name": full_name,
                    }
                )
    except Exception:
        rows = []

    if rows:
        return rows

    drivers_df = getattr(session, "drivers_df", None)
    try:
        if drivers_df is None or drivers_df.empty:
            return rows
        for _, row in drivers_df.iterrows():
            abbreviation = str(row.get("Driver", "")).strip()
            team_name = str(row.get("Team", "")).strip()
            if not abbreviation or not team_name:
                continue
            first_name = str(row.get("FirstName", "")).strip()
            last_name = str(row.get("LastName", "")).strip()
            rows.append(
                {
                    "abbreviation": abbreviation,
                    "team_name": team_name,
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": f"{first_name} {last_name}".strip(),
                }
            )
    except Exception:
        return []

    return rows


def _build_driver_team_mapping(session: Any) -> _DriverTeamMapping:
    """Build a cached session-specific plotting mapping."""
    year = _get_session_year(session)
    rows = _iter_session_rows(session)

    teams_by_key: dict[str, _TeamInfo] = {}
    drivers: list[_DriverInfo] = []

    for order, row in enumerate(rows):
        team_key, team_name, short_name, official_color, fastf1_color, aliases = (
            _resolve_team_profile(row["team_name"], year)
        )
        override = _get_session_overrides(session).get(team_key, {})
        team_name = str(override.get("name", team_name))
        short_name = str(override.get("short_name", short_name))
        official_color = str(override.get("official_color", official_color))
        fastf1_color = str(override.get("fastf1_color", fastf1_color))

        team = teams_by_key.get(team_key)
        if team is None:
            team = _TeamInfo(
                key=team_key,
                name=team_name,
                short_name=short_name,
                official_color=official_color,
                fastf1_color=fastf1_color,
                order=len(teams_by_key),
                aliases={str(alias) for alias in aliases} | {team_name, short_name},
            )
            teams_by_key[team_key] = team
        else:
            team.aliases.update({str(alias) for alias in aliases} | {team_name, short_name})
            team.name = team_name
            team.short_name = short_name
            team.official_color = official_color
            team.fastf1_color = fastf1_color

        driver = _DriverInfo(
            abbreviation=row["abbreviation"],
            full_name=row["full_name"] or row["abbreviation"],
            first_name=row["first_name"],
            last_name=row["last_name"],
            team_key=team_key,
            order=order,
        )
        drivers.append(driver)
        team.driver_abbreviations.append(driver.abbreviation)

    drivers_by_abbreviation = {driver.abbreviation: driver for driver in drivers}
    return _DriverTeamMapping(
        year=year,
        drivers=drivers,
        teams=list(teams_by_key.values()),
        drivers_by_abbreviation=drivers_by_abbreviation,
        teams_by_key=teams_by_key,
    )


def _get_driver_team_mapping(session: Any) -> _DriverTeamMapping:
    """Get a cached driver/team mapping for a session."""
    mapping = _PLOTTING_MAPPINGS.get(session)
    if mapping is None:
        mapping = _build_driver_team_mapping(session)
        _PLOTTING_MAPPINGS[session] = mapping
    return mapping


def _driver_features(driver: _DriverInfo) -> list[str]:
    """Return fuzzy-matchable features for a driver."""
    features = [driver.abbreviation, driver.full_name]
    if driver.first_name:
        features.append(driver.first_name)
    if driver.last_name:
        features.append(driver.last_name)
    return [_normalize_identifier(feature) for feature in features if feature]


def _team_features(team: _TeamInfo) -> list[str]:
    """Return fuzzy-matchable features for a team."""
    features = {team.name, team.short_name, *team.aliases}
    normalized = {_normalize_team_identifier(feature) for feature in features if feature}
    return [feature for feature in normalized if feature]


def _warn_on_correction(query: str, resolved: str) -> None:
    """Warn when fuzzy matching corrects user input."""
    if _normalize_identifier(query) == _normalize_identifier(resolved):
        return
    warnings.warn(
        f"Correcting user input '{query}' to '{resolved}'",
        UserWarning,
        stacklevel=3,
    )


def _match_driver(identifier: str, session: Any, *, exact_match: bool = False) -> _DriverInfo:
    """Resolve a driver from a session plotting mapping."""
    mapping = _get_driver_team_mapping(session)
    if not mapping.drivers:
        raise KeyError("No driver data available for this session")

    normalized_identifier = _normalize_identifier(identifier)
    for driver in mapping.drivers:
        if normalized_identifier in _driver_features(driver):
            return driver

    if exact_match:
        raise KeyError(f"No driver found for '{identifier}' (exact match only)")

    reference = [_driver_features(driver) for driver in mapping.drivers]
    index, exact = fuzzy_matcher(normalized_identifier, reference)
    driver = mapping.drivers[index]
    if not exact:
        _warn_on_correction(identifier, driver.full_name)
    return driver


def _match_team(identifier: str, session: Any, *, exact_match: bool = False) -> _TeamInfo:
    """Resolve a team from a session plotting mapping."""
    mapping = _get_driver_team_mapping(session)
    if not mapping.teams:
        raise KeyError("No team data available for this session")

    normalized_identifier = _normalize_team_identifier(identifier)
    for team in mapping.teams:
        if normalized_identifier in _team_features(team):
            return team

    if exact_match:
        raise KeyError(f"No team found for '{identifier}' (exact match only)")

    reference = [_team_features(team) for team in mapping.teams]
    index, exact = fuzzy_matcher(normalized_identifier, reference)
    team = mapping.teams[index]
    if not exact:
        _warn_on_correction(identifier, team.name)
    return team


def _get_year_specific_team_colors(
    year: int | None, colormap: str, session: Any = None
) -> dict[str, str]:
    """Get year-specific team colors."""
    colormap = _validate_colormap(colormap)

    if session is not None:
        try:
            mapping = _get_driver_team_mapping(session)
        except KeyError:
            mapping = None
        if mapping is not None and mapping.teams:
            result: dict[str, str] = {}
            for team in mapping.teams:
                team_color = team.official_color if colormap == "official" else team.fastf1_color
                for alias in {team.name, team.short_name, *team.aliases}:
                    result[alias] = team_color
            return result

    if year is None or year not in YEAR_CONSTANTS:
        return {}

    result = {}
    for team_key, team_data in YEAR_CONSTANTS[year].get("teams", {}).items():
        color = team_data["colors"]["official" if colormap == "official" else "fastf1"]
        for alias in _team_aliases(team_key, team_data["short_name"]):
            result[str(alias)] = color
    return result


def _get_year_specific_compound_colors(year: int | None) -> dict[str, str]:
    """Get year-specific compound colors."""
    if year is None or year not in YEAR_CONSTANTS:
        return DEFAULT_COMPOUND_COLORS.copy()
    return YEAR_CONSTANTS[year].get("compound_colors", DEFAULT_COMPOUND_COLORS).copy()


def _require_session_for_color_lookup(session: Any, helper_name: str) -> None:
    """Require a session for FastF1-compatible team and driver color lookups."""
    if session is None:
        raise ValueError(f"{helper_name} requires a session")


def get_team_color(
    identifier: str,
    session: Any = None,
    *,
    colormap: str = "default",
    exact_match: bool = False,
) -> str:
    """Get team color based on team name identifier.

    Raises:
        ValueError: If ``session`` is not provided or ``colormap`` is invalid.
        KeyError: If the team cannot be resolved from the session.
    """
    resolved_colormap = _validate_colormap(colormap)
    _require_session_for_color_lookup(session, "get_team_color")
    team = _match_team(identifier, session, exact_match=exact_match)
    return team.official_color if resolved_colormap == "official" else team.fastf1_color


def get_driver_color(
    identifier: str,
    session: Any = None,
    *,
    colormap: str = "default",
    exact_match: bool = False,
) -> str:
    """Get driver color (returns team color for the driver).

    Raises:
        ValueError: If ``session`` is not provided or ``colormap`` is invalid.
        KeyError: If the driver cannot be resolved from the session.
    """
    resolved_colormap = _validate_colormap(colormap)
    _require_session_for_color_lookup(session, "get_driver_color")
    driver = _match_driver(identifier, session, exact_match=exact_match)
    team = _get_driver_team_mapping(session).teams_by_key[driver.team_key]
    return team.official_color if resolved_colormap == "official" else team.fastf1_color


def get_driver_color_mapping(session: Any, *, colormap: str = "default") -> dict[str, str]:
    """Get mapping from driver abbreviation to color."""
    _validate_colormap(colormap)
    if session is None:
        return {}

    mapping = _get_driver_team_mapping(session)
    result: dict[str, str] = {}
    for driver in mapping.drivers:
        result[driver.abbreviation] = get_driver_color(
            driver.abbreviation,
            session,
            colormap=colormap,
            exact_match=True,
        )
    return result


def get_compound_color(compound: str, session: Any = None) -> str:
    """Get compound color as hexadecimal RGB color code."""
    year = _get_session_year(session)
    colors = _get_year_specific_compound_colors(year)
    if not isinstance(compound, str):
        return colors["UNKNOWN"]
    return colors.get(compound.upper(), colors["UNKNOWN"])


def get_compound_mapping(session: Any = None) -> dict[str, str]:
    """Get mapping from compound names to colors."""
    year = _get_session_year(session)
    return _get_year_specific_compound_colors(year)


def get_driver_abbreviation(identifier: str, session: Any, *, exact_match: bool = False) -> str:
    """Get driver abbreviation based on identifier."""
    if session is None:
        return ""
    return _match_driver(identifier, session, exact_match=exact_match).abbreviation


def get_driver_name(identifier: str, session: Any, *, exact_match: bool = False) -> str:
    """Get full driver name based on identifier."""
    if session is None:
        return ""
    return _match_driver(identifier, session, exact_match=exact_match).full_name


def get_team_name(
    identifier: str, session: Any, *, short: bool = False, exact_match: bool = False
) -> str:
    """Get team name based on identifier."""
    if session is None:
        return ""

    team = _match_team(identifier, session, exact_match=exact_match)
    if short:
        return _get_override_short_name(team.key, session) or team.short_name
    return team.name


def get_team_name_by_driver(
    identifier: str, session: Any, *, short: bool = False, exact_match: bool = False
) -> str:
    """Get team name for a driver."""
    if session is None:
        return ""

    driver = _match_driver(identifier, session, exact_match=exact_match)
    team = _get_driver_team_mapping(session).teams_by_key[driver.team_key]
    if short:
        return _get_override_short_name(team.key, session) or team.short_name
    return team.name


def _shorten_team_name(team: str) -> str:
    """Shorten team name for display."""
    return team.replace(" F1 Team", "").replace(" Racing", "").strip()


def list_driver_abbreviations(session: Any) -> list[str]:
    """Get list of all driver abbreviations in session."""
    if session is None:
        return []
    return [driver.abbreviation for driver in _get_driver_team_mapping(session).drivers]


def list_driver_names(session: Any) -> list[str]:
    """Get list of all driver full names in session."""
    if session is None:
        return []
    return [driver.full_name for driver in _get_driver_team_mapping(session).drivers]


def list_team_names(session: Any, *, short: bool = False) -> list[str]:
    """Get list of all team names in session."""
    if session is None:
        return []

    mapping = _get_driver_team_mapping(session)
    if short:
        return [
            _get_override_short_name(team.key, session) or team.short_name for team in mapping.teams
        ]
    return [team.name for team in mapping.teams]


def list_compounds(session: Any) -> list[str]:
    """Get list of all compound names for the season."""
    year = _get_session_year(session)
    return list(_get_year_specific_compound_colors(year).keys())


def get_driver_abbreviations_by_team(
    identifier: str, session: Any, *, exact_match: bool = False
) -> list[str]:
    """Get list of driver abbreviations for a team."""
    if session is None:
        return []
    team = _match_team(identifier, session, exact_match=exact_match)
    return list(_get_driver_team_mapping(session).teams_by_key[team.key].driver_abbreviations)


def get_driver_names_by_team(
    identifier: str, session: Any, *, exact_match: bool = False
) -> list[str]:
    """Get list of driver full names for a team."""
    if session is None:
        return []

    mapping = _get_driver_team_mapping(session)
    team = _match_team(identifier, session, exact_match=exact_match)
    return [
        mapping.drivers_by_abbreviation[abbr].full_name
        for abbr in mapping.teams_by_key[team.key].driver_abbreviations
    ]


def get_driver_style(
    identifier: str,
    style: str | Sequence[str] | Sequence[dict[str, Any]],
    session: Any,
    *,
    colormap: str = "default",
    additional_color_kws: Sequence[str] = (),
    exact_match: bool = False,
) -> dict[str, Any]:
    """Get unique plotting style for a driver."""
    if not style:
        raise ValueError("The provided style info is empty!")

    driver_abbr = get_driver_abbreviation(identifier, session, exact_match=exact_match)
    driver_color = get_driver_color(driver_abbr, session, colormap=colormap, exact_match=True)
    color_keys = {
        "color",
        "colors",
        "c",
        "gapcolor",
        "markeredgecolor",
        "mec",
        "markerfacecolor",
        "mfc",
        "markerfacecoloralt",
        "mfcalt",
        "facecolor",
        "facecolors",
        "fc",
        "edgecolor",
        "edgecolors",
        "ec",
        "ecolor",
        *additional_color_kws,
    }
    stylers = {
        "linestyle": ["solid", "dashed", "dashdot", "dotted"],
        "marker": ["x", "o", "^", "D"],
    }

    if isinstance(style, str):
        style = [style]

    if isinstance(style, (list, tuple)) and all(isinstance(item, str) for item in style):
        result = {}
        driver_index = _get_driver_index_in_team(driver_abbr, session)
        for key in style:
            if key in color_keys:
                result[key] = driver_color
            elif key in stylers:
                result[key] = stylers[key][driver_index % len(stylers[key])]
            else:
                raise ValueError(f"'{key}' is not a supported styling option")
        return result if result else {"color": driver_color}

    if isinstance(style, (list, tuple)) and style and all(isinstance(item, dict) for item in style):
        driver_index = _get_driver_index_in_team(driver_abbr, session)
        if driver_index < len(style):
            custom_style = dict(style[driver_index])  # type: ignore[arg-type]
            _replace_auto_colors(custom_style, driver_color, additional_color_kws)
            return custom_style
        raise ValueError(
            "The provided custom style info does not contain enough variants! "
            f"(Has: {len(style)}, Required: {driver_index + 1})"
        )

    raise ValueError("The provided style info has an invalid format!")


def _get_driver_index_in_team(driver_abbr: str, session: Any) -> int:
    """Get driver's index within their team."""
    if session is None:
        return 0

    mapping = _get_driver_team_mapping(session)
    driver = mapping.drivers_by_abbreviation.get(driver_abbr)
    if driver is None:
        return 0

    team = mapping.teams_by_key.get(driver.team_key)
    if team is None:
        return 0
    try:
        return team.driver_abbreviations.index(driver_abbr)
    except ValueError:
        return 0


def _get_driver_linestyle(driver_abbr: str, session: Any) -> str:
    """Get linestyle for driver based on team position."""
    index = _get_driver_index_in_team(driver_abbr, session)
    linestyles = ["-", "--", "-.", ":"]
    return linestyles[index % len(linestyles)]


def _get_driver_marker(driver_abbr: str, session: Any) -> str:
    """Get marker for driver based on team position."""
    index = _get_driver_index_in_team(driver_abbr, session)
    markers = ["x", "o", "^", "s"]
    return markers[index % len(markers)]


def _replace_auto_colors(
    style_dict: dict[str, Any],
    color: str,
    additional_color_kws: Sequence[str] = (),
) -> None:
    """Replace 'auto' color values recursively in style dictionary."""
    color_keys = {
        "color",
        "facecolor",
        "edgecolor",
        "markerfacecolor",
        "markeredgecolor",
        *additional_color_kws,
    }

    for key, value in style_dict.items():
        if isinstance(value, dict):
            _replace_auto_colors(value, color, additional_color_kws)
        elif key in color_keys and value == "auto":
            style_dict[key] = color


def add_sorted_driver_legend(ax: Axes, session: Any, *args: Any, **kwargs: Any) -> Legend:
    """Add legend with drivers grouped by team and sorted."""
    try:
        ret = mpl.legend._parse_legend_args([ax], *args, **kwargs)  # type: ignore[attr-defined]
        if len(ret) == 3:
            handles, labels, kwargs = ret
            extra_args: list[Any] = []
        else:
            handles, labels, extra_args, kwargs = ret
    except AttributeError:
        warnings.warn(
            "Failed to parse optional legend arguments correctly.",
            UserWarning,
            stacklevel=2,
        )
        extra_args = []
        kwargs.pop("handles", None)
        kwargs.pop("labels", None)
        handles, labels = ax.get_legend_handles_labels()

    if not handles or not labels:
        return ax.legend(*extra_args, **kwargs)

    resolved: list[tuple[int, int, Any, str]] = []
    unresolved: list[tuple[Any, str]] = []
    mapping = _get_driver_team_mapping(session)
    team_order = {team.key: team.order for team in mapping.teams}

    for handle, label in zip(handles, labels):
        try:
            driver = _match_driver(label, session, exact_match=True)
        except KeyError:
            unresolved.append((handle, label))
            continue
        resolved.append(
            (team_order.get(driver.team_key, len(team_order)), driver.order, handle, label)
        )

    resolved.sort(key=lambda item: item[:2])
    sorted_handles = [item[2] for item in resolved] + [item[0] for item in unresolved]
    sorted_labels = [item[3] for item in resolved] + [item[1] for item in unresolved]

    return ax.legend(sorted_handles, sorted_labels, *extra_args, **kwargs)


def override_team_constants(
    identifier: str,
    session: Any,
    *,
    short_name: str | None = None,
    official_color: str | None = None,
    fastf1_color: str | None = None,
) -> None:
    """Override default team constants for a specific team.

    Raises:
        ValueError: If ``session`` is not provided.
        KeyError: If the team cannot be resolved from the session.
    """
    _require_session_for_color_lookup(session, "override_team_constants")
    team = _match_team(identifier, session, exact_match=True)
    session_overrides = _SESSION_TEAM_OVERRIDES.setdefault(session, {})
    override_entry = session_overrides.setdefault(team.key, {})
    if short_name is not None:
        override_entry["short_name"] = short_name
    if official_color is not None:
        override_entry["official_color"] = official_color
    if fastf1_color is not None:
        override_entry["fastf1_color"] = fastf1_color
    _PLOTTING_MAPPINGS.pop(session, None)
