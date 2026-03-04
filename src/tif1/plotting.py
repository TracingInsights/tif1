"""Plotting utilities for tif1, compatible with fastf1.plotting."""

import matplotlib.pyplot as plt

# F1 Team Colors (approximate)
TEAM_COLORS = {
    "Mercedes": "#00d2be",
    "Red Bull Racing": "#0600ef",
    "Red Bull": "#0600ef",
    "Ferrari": "#dc0000",
    "McLaren": "#ff8700",
    "Alpine": "#0090ff",
    "Aston Martin": "#006f62",
    "RB": "#6692ff",
    "Haas": "#ffffff",
    "Sauber": "#52e252",
    "Williams": "#005aff",
}

DRIVER_COLORS = {
    "VER": "#0600ef",
    "HAM": "#00d2be",
    "LEC": "#dc0000",
    "NOR": "#ff8700",
    "ALO": "#006f62",
}

COMPOUND_COLORS = {
    "SOFT": "#da291c",
    "MEDIUM": "#ffd12e",
    "HARD": "#f0f0ec",
    "INTERMEDIATE": "#43b02a",
    "WET": "#0067ad",
    "UNKNOWN": "#8a8a8a",
}


def setup_mpl(
    color_scheme="fastf1",
    misc_mpl_mods=True,
    mpl_timedelta_support=False,
    **kwargs,
):
    """Setup matplotlib for F1 plotting."""
    _ = (misc_mpl_mods, mpl_timedelta_support, kwargs)
    if color_scheme == "fastf1":
        plt.rcParams["axes.facecolor"] = "#151515"
        plt.rcParams["figure.facecolor"] = "#151515"
        plt.rcParams["axes.edgecolor"] = "#ffffff"
        plt.rcParams["axes.labelcolor"] = "#ffffff"
        plt.rcParams["xtick.color"] = "#ffffff"
        plt.rcParams["ytick.color"] = "#ffffff"
        plt.rcParams["text.color"] = "#ffffff"


def get_team_color(team, session=None):
    """Get color for a team."""
    return TEAM_COLORS.get(team, "#ffffff")


def get_driver_color(driver, session=None):
    """Get color for a driver."""
    _ = session
    return DRIVER_COLORS.get(driver, "#ffffff")


def get_driver_color_mapping(session=None):
    """Get mapping from driver code to color."""
    mapping = dict(DRIVER_COLORS)
    if session is None:
        return mapping

    for item in getattr(session, "drivers", []):
        if not isinstance(item, dict):
            continue
        code = item.get("driver") or item.get("Abbreviation")
        team = item.get("team") or item.get("TeamName")
        if isinstance(code, str):
            mapping[code] = (
                get_team_color(str(team), session=session) if team else get_driver_color(code)
            )
    return mapping


def get_compound_color(compound, session=None):
    """Get color for tyre compound."""
    _ = session
    if not isinstance(compound, str):
        return COMPOUND_COLORS["UNKNOWN"]
    return COMPOUND_COLORS.get(compound.upper(), COMPOUND_COLORS["UNKNOWN"])


def get_compound_mapping(session=None):
    """Get mapping from compound to color."""
    _ = session
    return dict(COMPOUND_COLORS)


def get_driver_style(identifier, style=None, session=None):
    """Build a driver style dictionary compatible with fastf1.plotting."""
    code = str(identifier)
    default_style = {"color": get_driver_color(code, session=session), "linestyle": "-"}

    if style is None:
        return default_style

    if isinstance(style, list) and style and all(isinstance(item, str) for item in style):
        out = {}
        for key in style:
            if key == "color":
                out["color"] = default_style["color"]
            elif key == "linestyle":
                out["linestyle"] = default_style["linestyle"]
        return out if out else default_style

    if isinstance(style, list) and style and all(isinstance(item, dict) for item in style):
        candidate = dict(style[0])
        if candidate.get("color") == "auto":
            candidate["color"] = default_style["color"]
        return candidate

    return default_style


def add_sorted_driver_legend(ax, session=None):
    """Add a legend with labels sorted alphabetically by driver code."""
    _ = session
    handles, labels = ax.get_legend_handles_labels()
    sorted_pairs = sorted(zip(labels, handles), key=lambda item: item[0])
    if not sorted_pairs:
        return None
    sorted_labels, sorted_handles = zip(*sorted_pairs)
    return ax.legend(sorted_handles, sorted_labels)
