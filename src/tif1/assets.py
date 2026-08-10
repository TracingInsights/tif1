"""Bundled plot assets for tif1: car images, tyre images and fonts.

All assets are shipped inside the package under ``tif1/assets/`` so that the
Fastest_Lap-style charts (and any other plot) can render car and tyre artwork
fully offline, without fetching from the network at plot time.

Typical usage::

    import matplotlib.pyplot as plt
    from tif1 import assets

    fig, ax = plt.subplots(figsize=(20, 20))
    ax.barh(...)
    assets.add_car_images(ax, df, year=2024)

The team codes used by :func:`car_image_path` are the same short codes the
TracingInsights v2 scripts use (``RBR``, ``FER``, ...). Resolve a timing-data
team name to a code with :func:`tif1.plotting.get_team_code`.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import numpy as np

__all__ = [
    "ASSET_DIR",
    "CARS_DIR",
    "FONTS_DIR",
    "TYRES_DIR",
    "add_car_image_at_position",
    "add_car_images",
    "add_tyre_image_at_position",
    "add_tyre_images",
    "available_car_years",
    "available_team_codes",
    "car_image_path",
    "font_path",
    "load_car_image",
    "load_tyre_image",
    "tyre_image_path",
]

#: Root directory of the bundled assets.
ASSET_DIR = Path(__file__).resolve().parent / "assets"

#: Directory containing ``cars/<year>/<CODE>.png`` artwork.
CARS_DIR = ASSET_DIR / "cars"

#: Directory containing ``<COMPOUND>.png`` tyre artwork.
TYRES_DIR = ASSET_DIR / "tyres"

#: Directory containing bundled fonts.
FONTS_DIR = ASSET_DIR / "fonts"


def available_car_years() -> list[int]:
    """Return the sorted list of years that have bundled car images."""
    return sorted(
        int(path.name) for path in CARS_DIR.iterdir() if path.is_dir() and path.name.isdigit()
    )


def available_team_codes(year: int) -> list[str]:
    """Return the sorted list of team codes available for a given year.

    Raises:
        FileNotFoundError: If no images are bundled for the year.
    """
    directory = CARS_DIR / str(year)
    if not directory.is_dir():
        raise FileNotFoundError(f"No car images bundled for year {year}")
    return sorted(path.stem for path in directory.glob("*.png"))


def _available_team_codes_safe(year: int) -> list[str]:
    """Return team codes for ``year`` without raising on a missing directory."""
    directory = CARS_DIR / str(year)
    if not directory.is_dir():
        return []
    return sorted(path.stem for path in directory.glob("*.png"))


def car_image_path(year: int, team_code: str) -> Path:
    """Return the bundled path of a car image for ``year`` and ``team_code``.

    Raises:
        FileNotFoundError: If no image is bundled for the given year/code.
    """
    path = CARS_DIR / str(year) / f"{team_code}.png"
    if not path.is_file():
        available = _available_team_codes_safe(year)
        raise FileNotFoundError(
            f"No car image bundled for year {year} and team code '{team_code}'. "
            f"Available codes for {year}: {available}"
        )
    return path


def tyre_image_path(compound: str) -> Path:
    """Return the bundled path of a tyre image for ``compound``.

    Raises:
        FileNotFoundError: If no image is bundled for the given compound.
    """
    path = TYRES_DIR / f"{compound}.png"
    if not path.is_file():
        raise FileNotFoundError(f"No tyre image bundled for compound '{compound}'")
    return path


def font_path(name: str) -> Path:
    """Return the bundled path of a font file.

    Raises:
        FileNotFoundError: If the font is not bundled.
    """
    path = FONTS_DIR / name
    if not path.is_file():
        raise FileNotFoundError(f"No bundled font named '{name}'. Available fonts: {list_fonts()}")
    return path


def list_fonts() -> list[str]:
    """Return the names of the bundled fonts."""
    return sorted(path.name for path in FONTS_DIR.iterdir() if path.is_file())


@cache
def load_car_image(year: int, team_code: str) -> np.ndarray:
    """Load a bundled car image as an RGBA numpy array (cached)."""
    import matplotlib.image as mpimg

    return mpimg.imread(car_image_path(year, team_code))


@cache
def load_tyre_image(compound: str) -> np.ndarray:
    """Load a bundled tyre image as an RGBA numpy array (cached)."""
    import matplotlib.image as mpimg

    return mpimg.imread(tyre_image_path(compound))


def add_car_images(
    ax: Any,
    df: Any,
    year: int,
    *,
    x_col: str = "LapTimeDelta",
    y_col: str = "Driver",
    team_code_col: str = "Team_code",
    zoom: float = 0.5,
    x_offset: int = -110,
    threshold: float | None = None,
    y_offset: float = 0.0,
) -> None:
    """Annotate an axes with the bundled car image for every row of ``df``.

    Mirrors the ``add_car_images`` helper of the TracingInsights v2
    ``Fastest_Lap.py`` script, except the artwork is read from the bundled
    package assets instead of the network.

    Args:
        ax: Matplotlib axes to annotate.
        df: DataFrame with one row per car to draw.
        year: Season year used to pick the correct car artwork.
        x_col: Column with the x-position of each car.
        y_col: Column with the y-position of each car (label value). Note that,
            matching ``Fastest_Lap.py``, the drawn y-position is the barh row
            index (``i``) rather than this column.
        team_code_col: Column with the team code (e.g. ``"RBR"``).
        zoom: Zoom factor of the car image.
        x_offset: Horizontal offset (in points) of the image from the point.
        threshold: If set, rows where ``x <= threshold`` are skipped.
        y_offset: Vertical offset applied to the y position.
    """
    from matplotlib.offsetbox import AnnotationBbox, OffsetImage

    for i, (x_val, _y_val, team_code) in enumerate(zip(df[x_col], df[y_col], df[team_code_col])):
        if threshold is not None and x_val <= threshold:
            continue
        try:
            img = load_car_image(year, team_code)
        except FileNotFoundError:
            continue
        im = OffsetImage(img, zoom=zoom)
        im.image.axes = ax
        ab = AnnotationBbox(
            im,
            (x_val, i + y_offset),
            xybox=(x_offset, 0),
            frameon=False,
            xycoords="data",
            boxcoords="offset points",
            pad=0,
            zorder=1000,
        )
        ax.add_artist(ab)


def add_tyre_images(
    ax: Any,
    df: Any,
    *,
    x_col: str = "LapTimeDelta",
    y_col: str = "Driver",
    compound_col: str = "Compound",
    zoom: float = 0.07,
    x_offset: int = -190,
    y_offset: float = 0.1,
) -> None:
    """Annotate an axes with the bundled tyre image for every row of ``df``.

    Mirrors the ``add_tyre_images`` helper of the TracingInsights v2
    ``Fastest_Lap.py`` script, reading artwork from the bundled assets.

    Args:
        ax: Matplotlib axes to annotate.
        df: DataFrame with one row per tyre to draw.
        x_col: Column with the x-position of each tyre.
        y_col: Column with the y-position of each tyre (label value). Note that,
            matching ``Fastest_Lap.py``, the drawn y-position is the barh row
            index (``i``) rather than this column.
        compound_col: Column with the compound name (e.g. ``"SOFT"``).
        zoom: Zoom factor of the tyre image.
        x_offset: Horizontal offset (in points) of the image from the point.
        y_offset: Vertical offset applied to the y position.
    """
    from matplotlib.offsetbox import AnnotationBbox, OffsetImage

    for i, (x_val, _y_val, compound) in enumerate(zip(df[x_col], df[y_col], df[compound_col])):
        try:
            img = _load_tyre_with_fallback(compound)
        except FileNotFoundError:
            continue
        im = OffsetImage(img, zoom=zoom)
        im.image.axes = ax
        ab = AnnotationBbox(
            im,
            (x_val, i + y_offset),
            xybox=(x_offset, 0),
            frameon=False,
            xycoords="data",
            boxcoords="offset points",
            pad=0,
            zorder=1001,
        )
        ax.add_artist(ab)


def _load_tyre_with_fallback(compound: str) -> np.ndarray:
    """Load a tyre image, falling back to the generic ``None`` artwork."""
    try:
        return load_tyre_image(str(compound).upper())
    except FileNotFoundError:
        return load_tyre_image("None")


def add_car_image_at_position(
    ax: Any,
    x: float,
    y: float,
    year: int,
    team_code: str,
    *,
    zoom: float = 0.5,
    x_offset: int = -110,
) -> None:
    """Add a single bundled car image at an explicit position on ``ax``."""
    from matplotlib.offsetbox import AnnotationBbox, OffsetImage

    try:
        img = load_car_image(year, team_code)
    except FileNotFoundError:
        return
    im = OffsetImage(img, zoom=zoom)
    im.image.axes = ax
    ab = AnnotationBbox(
        im,
        (x, y),
        xybox=(x_offset, 0),
        frameon=False,
        xycoords="data",
        boxcoords="offset points",
        pad=0,
        zorder=1000,
    )
    ax.add_artist(ab)


def add_tyre_image_at_position(
    ax: Any,
    x: float,
    y: float,
    compound: str,
    *,
    zoom: float = 0.07,
    x_offset: int = -190,
) -> None:
    """Add a single bundled tyre image at an explicit position on ``ax``."""
    from matplotlib.offsetbox import AnnotationBbox, OffsetImage

    try:
        img = _load_tyre_with_fallback(compound)
    except FileNotFoundError:
        return
    im = OffsetImage(img, zoom=zoom)
    im.image.axes = ax
    ab = AnnotationBbox(
        im,
        (x, y),
        xybox=(x_offset, 0),
        frameon=False,
        xycoords="data",
        boxcoords="offset points",
        pad=0,
        zorder=1001,
    )
    ax.add_artist(ab)
