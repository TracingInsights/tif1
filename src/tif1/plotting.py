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


def setup_mpl(
    color_scheme="fastf1",
    misc_mpl_mods=True,
    mpl_timedelta_support=False,
    **kwargs,
):
    """Setup matplotlib for F1 plotting.

    Args:
        color_scheme: Color scheme to use ('fastf1', 'light', or 'custom')
        misc_mpl_mods: Apply miscellaneous matplotlib modifications
        mpl_timedelta_support: Enable timedelta support
        **kwargs: Additional configuration overrides

    Supported kwargs for 'custom' scheme:
        - background: Background color (default: 'lightblue')
        - text_color: Text color (default: 'black')
        - grid_color: Grid color (default: 'black')
    """
    _ = (misc_mpl_mods, mpl_timedelta_support)

    if color_scheme == "fastf1":
        plt.rcParams["axes.facecolor"] = "#151515"
        plt.rcParams["figure.facecolor"] = "#151515"
        plt.rcParams["axes.edgecolor"] = "#ffffff"
        plt.rcParams["axes.labelcolor"] = "#ffffff"
        plt.rcParams["xtick.color"] = "#ffffff"
        plt.rcParams["ytick.color"] = "#ffffff"
        plt.rcParams["text.color"] = "#ffffff"
    elif color_scheme == "light":
        background = kwargs.get("background", "lightblue")
        text_color = kwargs.get("text_color", "black")
        plt.rcParams["axes.facecolor"] = background
        plt.rcParams["figure.facecolor"] = background
        plt.rcParams["axes.edgecolor"] = text_color
        plt.rcParams["axes.labelcolor"] = text_color
        plt.rcParams["xtick.color"] = text_color
        plt.rcParams["ytick.color"] = text_color
        plt.rcParams["text.color"] = text_color
    # For 'custom' or any other scheme, do nothing (user manages their own settings)


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


def get_team_color(team, session=None):
    """Get color for a team."""
    _ = session  # Reserved for future use
    return TEAM_COLORS.get(team, "#ffffff")


def get_driver_color(driver, session=None):
    """Get color for a driver."""
    _ = session  # Reserved for future use
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
    _ = session  # Reserved for future use
    if not isinstance(compound, str):
        return COMPOUND_COLORS["UNKNOWN"]
    return COMPOUND_COLORS.get(compound.upper(), COMPOUND_COLORS["UNKNOWN"])


def get_compound_mapping(session=None):
    """Get mapping from compound to color."""
    _ = session  # Reserved for future use
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
    _ = session  # Reserved for future use
    handles, labels = ax.get_legend_handles_labels()
    sorted_pairs = sorted(zip(labels, handles), key=lambda item: item[0])
    if not sorted_pairs:
        return None
    sorted_labels, sorted_handles = zip(*sorted_pairs)
    return ax.legend(sorted_handles, sorted_labels)
