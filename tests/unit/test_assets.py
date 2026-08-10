"""Tests for bundled plot assets (cars, tyres, fonts)."""

from __future__ import annotations

import matplotlib as mpl

mpl.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from tif1 import assets


@pytest.fixture(autouse=True)
def close_figures():
    """Close any figures created by image-annotation tests."""
    yield
    plt.close("all")


def test_available_car_years_covers_2018_to_2026():
    years = assets.available_car_years()
    assert years[0] == 2018
    assert years[-1] == 2026
    assert len(years) == 9


def test_available_team_codes_per_year():
    codes = assets.available_team_codes(2024)
    assert "RBR" in codes
    assert "KS" in codes
    assert "RB" in codes

    with pytest.raises(FileNotFoundError, match="1999"):
        assets.available_team_codes(1999)


def test_car_image_path_exists():
    path = assets.car_image_path(2024, "RBR")
    assert path.is_file()
    assert path.name == "RBR.png"
    assert "assets" in str(path)


def test_car_image_path_unknown_raises():
    with pytest.raises(FileNotFoundError, match="RBRXX"):
        assets.car_image_path(2024, "RBRXX")


def test_car_image_path_missing_year_has_clear_message():
    """Missing-year errors keep their own message (no nested exception)."""
    with pytest.raises(FileNotFoundError, match="year 1999 and team code"):
        assets.car_image_path(1999, "RBR")


def test_tyre_image_path_exists():
    assert assets.tyre_image_path("SOFT").is_file()
    assert assets.tyre_image_path("INTERMEDIATE").is_file()


def test_font_path_and_list():
    assert "Tenada.ttf" in assets.list_fonts()
    assert assets.font_path("Tenada.ttf").is_file()
    with pytest.raises(FileNotFoundError, match=r"nope\.ttf"):
        assets.font_path("nope.ttf")


def test_load_car_image_returns_rgba_array():
    img = assets.load_car_image(2024, "MCL")
    assert isinstance(img, np.ndarray)
    assert img.ndim == 3
    assert img.shape[2] == 4


def test_load_car_image_is_cached():
    first = assets.load_car_image(2024, "MCL")
    second = assets.load_car_image(2024, "MCL")
    assert first is second


def test_load_tyre_image_returns_array():
    img = assets.load_tyre_image("SOFT")
    assert isinstance(img, np.ndarray)
    assert img.ndim == 3


def test_unknown_tyre_compound_has_none_fallback():
    img = assets._load_tyre_with_fallback("HYPERSOFT")
    assert isinstance(img, np.ndarray)


def test_add_car_images_annotates_axes():
    _, ax = plt.subplots()
    df = pd.DataFrame(
        {
            "LapTimeDelta": [0.1, 0.3],
            "Driver": ["VER", "HAM"],
            "Team_code": ["RBR", "MER"],
        }
    )
    before = len(ax.artists)
    assets.add_car_images(ax, df, year=2024)
    assert len(ax.artists) == before + 2


def test_add_car_images_threshold_skips_rows():
    _, ax = plt.subplots()
    df = pd.DataFrame(
        {
            "LapTimeDelta": [0.1, 0.3],
            "Driver": ["VER", "HAM"],
            "Team_code": ["RBR", "MER"],
        }
    )
    assets.add_car_images(ax, df, year=2024, threshold=0.2)
    assert len(ax.artists) == 1


def test_add_car_images_skips_unknown_code():
    _, ax = plt.subplots()
    df = pd.DataFrame(
        {
            "LapTimeDelta": [0.1],
            "Driver": ["VER"],
            "Team_code": ["NOPE"],
        }
    )
    assets.add_car_images(ax, df, year=2024)
    assert len(ax.artists) == 0


def test_add_tyre_images_annotates_axes():
    _, ax = plt.subplots()
    df = pd.DataFrame(
        {
            "LapTimeDelta": [0.1, 0.3],
            "Driver": ["VER", "HAM"],
            "Compound": ["SOFT", "MEDIUM"],
        }
    )
    before = len(ax.artists)
    assets.add_tyre_images(ax, df)
    assert len(ax.artists) == before + 2


def test_add_single_image_at_position():
    _, ax = plt.subplots()
    before = len(ax.artists)
    assets.add_car_image_at_position(ax, 0.5, 1, 2024, "FER")
    assets.add_tyre_image_at_position(ax, 0.5, 1, "HARD")
    assert len(ax.artists) == before + 2
