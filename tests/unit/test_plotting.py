"""Unit tests for plotting utilities."""

from __future__ import annotations

from types import SimpleNamespace

import matplotlib.pyplot as plt

from tif1 import plotting


def test_setup_mpl_fastf1_scheme_sets_dark_theme():
    plotting.setup_mpl(color_scheme="fastf1")
    assert plt.rcParams["axes.facecolor"] == "#151515"
    assert plt.rcParams["text.color"] == "#ffffff"


def test_setup_mpl_other_scheme_is_noop():
    before = plt.rcParams["axes.facecolor"]
    plotting.setup_mpl(color_scheme="custom")
    assert plt.rcParams["axes.facecolor"] == before


def test_get_driver_color_mapping_uses_session_team_colors():
    session = SimpleNamespace(
        drivers=[
            {"driver": "VER", "team": "Red Bull Racing"},
            {"Abbreviation": "NEW", "TeamName": "Mercedes"},
            "not-a-dict",
        ]
    )
    mapping = plotting.get_driver_color_mapping(session=session)
    assert mapping["VER"] == plotting.TEAM_COLORS["Red Bull Racing"]
    assert mapping["NEW"] == plotting.TEAM_COLORS["Mercedes"]


def test_get_compound_color_unknown_and_non_string():
    assert plotting.get_compound_color("mystery") == plotting.COMPOUND_COLORS["UNKNOWN"]
    assert plotting.get_compound_color(123) == plotting.COMPOUND_COLORS["UNKNOWN"]


def test_get_driver_style_variants():
    default_style = plotting.get_driver_style("VER")
    assert default_style["linestyle"] == "-"

    keys_style = plotting.get_driver_style("VER", style=["color", "linestyle", "ignored"])
    assert set(keys_style) == {"color", "linestyle"}

    dict_style = plotting.get_driver_style("VER", style=[{"color": "auto", "linewidth": 2}])
    assert dict_style["color"] == plotting.get_driver_color("VER")
    assert dict_style["linewidth"] == 2

    fallback_style = plotting.get_driver_style("VER", style=[123])  # type: ignore[list-item]
    assert fallback_style == default_style


def test_add_sorted_driver_legend_handles_empty_and_sorted():
    fig, ax = plt.subplots()
    try:
        assert plotting.add_sorted_driver_legend(ax) is None

        ax.plot([0, 1], [0, 1], label="VER")
        ax.plot([0, 1], [1, 0], label="ALO")
        legend = plotting.add_sorted_driver_legend(ax)
        assert legend is not None
        assert [text.get_text() for text in legend.get_texts()] == ["ALO", "VER"]
    finally:
        plt.close(fig)
