"""Tests for plotting module."""

from __future__ import annotations

import importlib
from typing import cast

import matplotlib.pyplot as plt
import pandas as pd
import pytest

from tif1 import plotting


class MockSession:
    """Mock session for testing plotting helpers."""

    def __init__(self, year: int | None = None, driver_rows: list[dict[str, str]] | None = None):
        self.year = year
        rows = driver_rows or [
            {
                "Abbreviation": "VER",
                "TeamName": "Red Bull Racing",
                "FirstName": "Max",
                "LastName": "Verstappen",
                "FullName": "Max Verstappen",
            },
            {
                "Abbreviation": "PER",
                "TeamName": "Red Bull Racing",
                "FirstName": "Sergio",
                "LastName": "Perez",
                "FullName": "Sergio Perez",
            },
            {
                "Abbreviation": "HAM",
                "TeamName": "Mercedes",
                "FirstName": "Lewis",
                "LastName": "Hamilton",
                "FullName": "Lewis Hamilton",
            },
            {
                "Abbreviation": "RUS",
                "TeamName": "Mercedes",
                "FirstName": "George",
                "LastName": "Russell",
                "FullName": "George Russell",
            },
            {
                "Abbreviation": "LEC",
                "TeamName": "Ferrari",
                "FirstName": "Charles",
                "LastName": "Leclerc",
                "FullName": "Charles Leclerc",
            },
            {
                "Abbreviation": "SAI",
                "TeamName": "Ferrari",
                "FirstName": "Carlos",
                "LastName": "Sainz",
                "FullName": "Carlos Sainz",
            },
        ]
        self._results = pd.DataFrame(rows)
        self._results["TeamColor"] = ""
        self.drivers_df = pd.DataFrame(
            {
                "Driver": self._results["Abbreviation"],
                "Team": self._results["TeamName"],
                "FirstName": self._results["FirstName"],
                "LastName": self._results["LastName"],
                "DriverNumber": [str(index + 1) for index in range(len(self._results))],
                "TeamColor": [""] * len(self._results),
                "HeadshotUrl": [""] * len(self._results),
            }
        )

    @property
    def results(self) -> pd.DataFrame:
        """Return synthetic session results."""
        return self._results


@pytest.fixture(autouse=True)
def reset_plotting_state():
    """Reset global plotting state between tests."""
    plotting.set_default_colormap("fastf1")
    plotting._SESSION_TEAM_OVERRIDES.clear()
    plotting._PLOTTING_MAPPINGS.clear()
    plotting._TIMPLE_IMPORT_WARNING_SHOWN = False
    yield
    plotting.set_default_colormap("fastf1")
    plotting._SESSION_TEAM_OVERRIDES.clear()
    plotting._PLOTTING_MAPPINGS.clear()
    plotting._TIMPLE_IMPORT_WARNING_SHOWN = False
    plt.close("all")


def test_set_default_colormap():
    """Test setting default colormap."""
    plotting.set_default_colormap("fastf1")
    assert plotting._DEFAULT_COLORMAP == "fastf1"

    plotting.set_default_colormap("official")
    assert plotting._DEFAULT_COLORMAP == "official"

    with pytest.raises(ValueError, match="Invalid colormap"):
        plotting.set_default_colormap("invalid")


def test_get_team_color_with_session():
    """Test getting session-backed team colors."""
    session = MockSession(year=2024)

    assert plotting.get_team_color("Red Bull Racing", session) == "#0600ef"
    assert plotting.get_team_color("Red Bull", session) == "#0600ef"
    assert plotting.get_team_color("Ferrari", session) == "#e8002d"
    assert plotting.get_team_color("Ferrari", session, colormap="official") == "#e8002d"


def test_get_team_color_requires_session():
    """Test team-color lookups require a session."""
    with pytest.raises(ValueError, match="get_team_color requires a session"):
        plotting.get_team_color("Ferrari")


def test_get_driver_color_with_session():
    """Test getting session-backed driver colors."""
    session = MockSession(year=2024)

    assert plotting.get_driver_color("VER", session) == "#0600ef"
    assert plotting.get_driver_color("Hamilton", session) == "#27f4d2"
    assert plotting.get_driver_color("LEC", session) == "#e8002d"


def test_get_driver_color_requires_session():
    """Test driver-color lookups require a session."""
    with pytest.raises(ValueError, match="get_driver_color requires a session"):
        plotting.get_driver_color("VER")


def test_get_compound_color():
    """Test getting compound colors."""
    assert plotting.get_compound_color("SOFT") == "#da291c"
    assert plotting.get_compound_color("MEDIUM") == "#ffd12e"
    assert plotting.get_compound_color("soft") == "#da291c"
    assert plotting.get_compound_color("invalid") == "#00ffff"

    session_2018 = MockSession(year=2018)
    assert plotting.get_compound_color("SOFT", session_2018) == "#ffd318"
    assert plotting.get_compound_color("HYPERSOFT", session_2018) == "#feb1c1"


def test_get_driver_abbreviation_and_name():
    """Test getting driver abbreviations and names."""
    session = MockSession(year=2024)

    assert plotting.get_driver_abbreviation("VER", session) == "VER"
    assert plotting.get_driver_abbreviation("Verstappen", session) == "VER"
    assert plotting.get_driver_name("VER", session) == "Max Verstappen"
    assert plotting.get_driver_name("Leclerc", session) == "Charles Leclerc"


def test_get_team_name_variants():
    """Test getting full and short team names."""
    session = MockSession(year=2024)

    assert plotting.get_team_name("Red Bull", session) == "Red Bull Racing"
    assert plotting.get_team_name("Red Bull", session, short=True) == "Red Bull"
    assert plotting.get_team_name_by_driver("VER", session) == "Red Bull Racing"
    assert plotting.get_team_name_by_driver("VER", session, short=True) == "Red Bull"


def test_list_helpers():
    """Test listing helpers."""
    session = MockSession(year=2024)

    assert plotting.list_driver_abbreviations(session) == ["VER", "PER", "HAM", "RUS", "LEC", "SAI"]
    assert "Max Verstappen" in plotting.list_driver_names(session)
    assert plotting.list_team_names(session) == ["Red Bull Racing", "Mercedes", "Ferrari"]
    assert plotting.list_team_names(session, short=True) == ["Red Bull", "Mercedes", "Ferrari"]
    assert "SOFT" in plotting.list_compounds(session)


def test_get_driver_abbreviations_and_names_by_team():
    """Test team-to-driver lookup helpers."""
    session = MockSession(year=2024)

    assert plotting.get_driver_abbreviations_by_team("Red Bull", session) == ["VER", "PER"]
    assert plotting.get_driver_names_by_team("Ferrari", session) == [
        "Charles Leclerc",
        "Carlos Sainz",
    ]


def test_get_driver_color_mapping():
    """Test getting driver color mapping."""
    session = MockSession(year=2024)

    mapping = plotting.get_driver_color_mapping(session)
    assert mapping["VER"] == "#0600ef"
    assert mapping["HAM"] == "#27f4d2"
    assert mapping["LEC"] == "#e8002d"


def test_get_compound_mapping():
    """Test getting compound mapping."""
    mapping = plotting.get_compound_mapping()
    assert mapping["SOFT"] == "#da291c"
    assert mapping["MEDIUM"] == "#ffd12e"

    session_2018 = MockSession(year=2018)
    mapping_2018 = plotting.get_compound_mapping(session_2018)
    assert mapping_2018["SOFT"] == "#ffd318"
    assert "HYPERSOFT" in mapping_2018


def test_get_driver_style_builtin():
    """Test getting driver style with built-in options."""
    session = MockSession(year=2024)

    style = plotting.get_driver_style("VER", ["color", "marker"], session)
    assert style == {"color": "#0600ef", "marker": "x"}

    style = plotting.get_driver_style("PER", ["color", "linestyle"], session)
    assert style == {"color": "#0600ef", "linestyle": "dashed"}


def test_get_driver_style_custom():
    """Test getting driver style with custom styles."""
    session = MockSession(year=2024)

    custom_styles = [
        {"linestyle": "solid", "color": "auto", "custom_arg": True},
        {"linestyle": "dotted", "color": "#FF0060", "other_arg": 10},
    ]

    style = plotting.get_driver_style("VER", custom_styles, session)
    assert style["linestyle"] == "solid"
    assert style["color"] == "#0600ef"
    assert style["custom_arg"] is True

    style = plotting.get_driver_style("PER", custom_styles, session)
    assert style["linestyle"] == "dotted"
    assert style["color"] == "#FF0060"


def test_get_driver_style_validation():
    """Test FastF1-compatible driver style validation."""
    session = MockSession(year=2024)

    with pytest.raises(ValueError, match="empty"):
        plotting.get_driver_style("VER", [], session)

    with pytest.raises(ValueError, match="supported styling option"):
        plotting.get_driver_style("VER", ["linewidth"], session)

    with pytest.raises(ValueError, match="invalid format"):
        plotting.get_driver_style("VER", [1, 2], session)  # type: ignore[ty:invalid-argument-type]

    with pytest.raises(ValueError, match="contain enough variants"):
        plotting.get_driver_style("PER", [{"color": "auto"}], session)


def test_exact_match_failures_raise_key_error():
    """Test exact-match errors for session-backed helpers."""
    session = MockSession(year=2024)

    with pytest.raises(KeyError, match="No team found"):
        plotting.get_team_color("Ferrarii", session, exact_match=True)

    with pytest.raises(KeyError, match="No driver found"):
        plotting.get_driver_name("Verstapen", session, exact_match=True)


def test_fuzzy_corrections_warn():
    """Test fuzzy-matching correction warnings."""
    session = MockSession(year=2024)

    with pytest.warns(UserWarning, match="Correcting user input"):
        assert plotting.get_driver_name("Verstapen", session) == "Max Verstappen"

    with pytest.warns(UserWarning, match="Correcting user input"):
        assert plotting.get_team_color("Ferari", session) == "#e8002d"


def test_invalid_colormap_raises_consistently():
    """Test invalid colormap handling."""
    session = MockSession(year=2024)

    with pytest.raises(ValueError, match="Invalid colormap"):
        plotting.get_team_color("Ferrari", session, colormap="invalid")

    with pytest.raises(ValueError, match="Invalid colormap"):
        plotting.get_team_color("Ferrari", colormap="invalid")

    with pytest.raises(ValueError, match="Invalid colormap"):
        plotting.get_driver_color("VER", colormap="invalid")


def test_year_specific_team_colors():
    """Test year-specific team colors."""
    assert (
        plotting.get_team_color("Ferrari", MockSession(year=2024), colormap="fastf1") == "#e8002d"
    )
    assert (
        plotting.get_team_color("Ferrari", MockSession(year=2021), colormap="fastf1") == "#dc0004"
    )
    assert (
        plotting.get_team_color("Ferrari", MockSession(year=2018), colormap="fastf1") == "#dc0000"
    )


def test_year_specific_compound_colors():
    """Test year-specific compound colors."""
    session_2018 = MockSession(year=2018)
    assert plotting.get_compound_color("HYPERSOFT", session_2018) == "#feb1c1"
    assert plotting.get_compound_color("ULTRASOFT", session_2018) == "#b24ba7"
    assert plotting.get_compound_color("SUPERSOFT", session_2018) == "#fc2b2a"

    session_2024 = MockSession(year=2024)
    assert plotting.get_compound_color("SOFT", session_2024) == "#da291c"
    assert plotting.get_compound_color("MEDIUM", session_2024) == "#ffd12e"
    assert plotting.get_compound_color("HARD", session_2024) == "#f0f0ec"


def test_session_year_extraction():
    """Test extracting year from session."""
    assert plotting._get_session_year(MockSession(year=2024)) == 2024
    assert plotting._get_session_year(MockSession()) is None
    assert plotting._get_session_year(None) is None


def test_new_teams_in_2026():
    """Test new teams appearing in 2026."""
    session_2026 = MockSession(
        year=2026,
        driver_rows=[
            {
                "Abbreviation": "HUL",
                "TeamName": "Audi",
                "FirstName": "Nico",
                "LastName": "Hulkenberg",
                "FullName": "Nico Hulkenberg",
            },
            {
                "Abbreviation": "BOT",
                "TeamName": "Audi",
                "FirstName": "Valtteri",
                "LastName": "Bottas",
                "FullName": "Valtteri Bottas",
            },
            {
                "Abbreviation": "COL",
                "TeamName": "Cadillac",
                "FirstName": "Franco",
                "LastName": "Colapinto",
                "FullName": "Franco Colapinto",
            },
            {
                "Abbreviation": "MAL",
                "TeamName": "Cadillac",
                "FirstName": "Jack",
                "LastName": "Doohan",
                "FullName": "Jack Doohan",
            },
        ],
    )
    assert plotting.get_team_color("Audi", session_2026, colormap="fastf1") == "#ff2d00"
    assert plotting.get_team_color("Cadillac", session_2026, colormap="fastf1") == "#444444"


def test_setup_mpl_fastf1_compatible_call_patterns():
    """Test setup_mpl with FastF1-style and tif1 call patterns."""
    plotting.setup_mpl(color_scheme="fastf1", mpl_timedelta_support=False)
    plotting.setup_mpl(False, "fastf1")
    plotting.setup_mpl("fastf1")


def test_setup_mpl_default_light_theme():
    """Test the default-light (Fastest_Lap.py) matplotlib theme."""
    plotting.setup_mpl(color_scheme="default-light", mpl_timedelta_support=False)
    assert plt.rcParams["figure.facecolor"] == "lightblue"
    assert plt.rcParams["axes.facecolor"] == "lightblue"
    assert plt.rcParams["text.color"] == "black"
    assert plt.rcParams["xtick.color"] == "black"
    assert plt.rcParams["ytick.color"] == "black"
    assert plt.rcParams["axes.spines.top"] is False
    assert plt.rcParams["axes.spines.right"] is False
    assert plt.rcParams["legend.frameon"] is True
    assert plt.rcParams["ytick.major.size"] == 0


def test_setup_mpl_default_dark_theme():
    """Test the default-dark (TracingInsights brand) matplotlib theme."""
    plotting.setup_mpl(color_scheme="default-dark", mpl_timedelta_support=False)
    assert plt.rcParams["figure.facecolor"] == "#011627"
    assert plt.rcParams["axes.facecolor"] == "#011627"
    assert plt.rcParams["text.color"] == "lime"
    assert plt.rcParams["xtick.color"] == "lime"


def test_get_plot_style_light_matches_fastest_lap_constants():
    """Test the default-light style config mirrors Fastest_Lap.py constants."""
    style = plotting.get_plot_style("default-light")
    assert style["colors"]["background"] == "lightblue"
    assert style["colors"]["text"] == "black"
    assert style["figure"] == {"size": (20, 20), "dpi": 300, "constrained_layout": True}
    assert style["fonts"]["title_size"] == 48
    assert style["fonts"]["label_size"] == 32
    assert style["fonts"]["annotation_size"] == 25
    assert style["fonts"]["heading"] == "Tenada.ttf"
    assert style["fonts"]["footer_size"] == 50
    assert style["bar"]["height"] == 0.1
    assert style["images"]["car_zoom"] == 0.5
    assert style["images"]["tyre_zoom"] == 0.07
    assert style["images"]["tyre_x_offset"] == -190
    assert style["images"]["car_threshold"] is None
    assert style["spacing"]["label_padding"] == -330
    assert style["spacing"]["x_margin"] == 0.4


def test_get_plot_style_dark_matches_race_launch_script():
    """Test the default-dark style mirrors Race_Launch_Performance_Ratings.py."""
    style = plotting.get_plot_style("default-dark")
    assert style["colors"]["background"] == "#011627"
    assert style["colors"]["text"] == "lime"
    assert style["colors"]["grid"] == "black"  # black axvline in the script
    assert style["colors"]["bar_label"] == "white"
    assert style["colors"]["ytick"] == "white"
    assert style["fonts"]["heading"] == "coolvetica rg.otf"
    assert style["fonts"]["heading2"] == "Azonix.otf"
    assert style["fonts"]["logo"] == "GreatVibes-Regular.ttf"
    assert style["fonts"]["footer_size"] == 50
    assert style["fonts"]["watermark_size"] == 48
    assert style["figure"] == {"size": (20, 20), "dpi": 300, "constrained_layout": False}
    assert style["bar"] == {"height": 0.8, "alpha": 1.0, "linewidth": None}
    assert style["images"]["tyre_x_offset"] == -150
    assert style["images"]["car_threshold"] == 2.5
    assert style["spacing"]["label_padding"] == 20
    assert style["spacing"]["subplot_left"] == 0.15
    assert style["spacing"]["subplot_right"] == 0.97
    assert style["footer"] == "TRACINGINSIGHTS.COM"
    assert style["watermark"] == "@TracingInsights"


def test_get_plot_style_defaults_and_errors():
    """Test get_plot_style defaults and unknown-style errors."""
    assert plotting.get_plot_style()["name"] == "default-light"
    with pytest.raises(ValueError, match="Unknown plot style"):
        plotting.get_plot_style("nope")


def test_get_plot_style_returns_copy():
    """Test get_plot_style returns a mutable copy."""
    style = plotting.get_plot_style("default-light")
    style["colors"]["background"] = "red"
    assert plotting.get_plot_style("default-light")["colors"]["background"] == "lightblue"


def test_team_code_mapping():
    """Test team code mapping for a year."""
    mapping = plotting.team_code_mapping(2024)
    assert mapping["Red Bull Racing"] == "RBR"
    assert mapping["Ferrari"] == "FER"
    assert mapping["Kick Sauber"] == "KS"
    assert plotting.team_code_mapping(1999) == {}


def test_team_color_mapping_v2_palette():
    """Test the TracingInsights v2 palette used by Fastest_Lap.py."""
    mapping = plotting.team_color_mapping(2024)
    assert mapping["Red Bull Racing"] == "#ffe119"
    assert mapping["Ferrari"] == "#e6194b"
    assert mapping["Kick Sauber"] == "#00ff00"
    assert plotting.team_color_mapping(1999) == {}


def test_team_color_mapping_covers_code_variants():
    """Every name with a car code also gets a colour (consistent mappings)."""
    mapping_2024 = plotting.team_color_mapping(2024)
    for name in plotting.team_code_mapping(2024):
        assert name in mapping_2024, name
    assert mapping_2024["Alfa Romeo Ferrari"] == "#00ff00"  # Kick Sauber (KS) variant
    assert mapping_2024["Racing Bulls Honda RBPT"] == "#dcbeff"  # RB variant

    mapping_2018 = plotting.team_color_mapping(2018)
    assert mapping_2018["Scuderia Toro Rosso Honda"] == "#dcbeff"  # Toro Rosso (TR)


def test_get_team_code_with_session():
    """Test resolving car image codes from a session."""
    session = MockSession(year=2024)
    assert plotting.get_team_code("Red Bull Racing", session) == "RBR"
    assert plotting.get_team_code("Red Bull", session) == "RBR"
    assert plotting.get_team_code("Ferrari", session) == "FER"
    assert plotting.get_team_code("Mercedes", session) == "MER"


def test_get_team_code_with_year():
    """Test resolving car image codes with an explicit year."""
    assert plotting.get_team_code("Ferrari", year=2018) == "FER"
    assert plotting.get_team_code("Force India", year=2018) == "FI"
    assert plotting.get_team_code("AlphaTauri", year=2020) == "APT"
    assert plotting.get_team_code("Kick Sauber", year=2024) == "KS"
    assert plotting.get_team_code("Audi", year=2026) == "AUD"


def test_get_team_code_errors():
    """Test get_team_code validation errors."""
    with pytest.raises(ValueError, match="requires a session or a year"):
        plotting.get_team_code("Ferrari")

    with pytest.raises(ValueError, match="could not resolve a season year"):
        plotting.get_team_code("Ferrari", MockSession(year=None))

    with pytest.raises(KeyError, match="No team code"):
        plotting.get_team_code("Williams", year=1999)


def test_setup_mpl_misc_mods_switch():
    """Test that misc_mpl_mods controls style changes."""
    original = plt.rcParams["axes.facecolor"]
    plt.rcParams["axes.facecolor"] = "#abcdef"

    plotting.setup_mpl(False, "fastf1", misc_mpl_mods=False)
    assert plt.rcParams["axes.facecolor"] == "#abcdef"

    plt.rcParams["axes.facecolor"] = original


def test_setup_mpl_warns_without_timple(monkeypatch):
    """Test graceful fallback when timple is unavailable."""
    plotting._TIMPLE_IMPORT_WARNING_SHOWN = False

    def fake_import_module(name: str):
        if name == "timple":
            raise ImportError("forced missing timple for test")
        return importlib.import_module(name)

    monkeypatch.setattr(plotting.importlib, "import_module", fake_import_module)
    with pytest.warns(RuntimeWarning, match="optional dependency 'timple'"):
        plotting.setup_mpl(True, None, misc_mpl_mods=False)


def test_setup_mpl_enables_timple_when_available(monkeypatch):
    """Test setup_mpl with an available timple module."""
    calls: dict[str, object] = {}

    class FakeTimpleInstance:
        def __init__(self, *, converter, formatter_args):
            calls["converter"] = converter
            calls["formatter_args"] = formatter_args

        def enable(self):
            calls["enabled"] = True

    class FakeTimpleModule:
        @staticmethod
        def Timple(*, converter, formatter_args):  # noqa: N802
            return FakeTimpleInstance(
                converter=converter,
                formatter_args=formatter_args,
            )

    def fake_import_module(name: str):
        if name == "timple":
            return FakeTimpleModule
        return importlib.import_module(name)

    monkeypatch.setattr(plotting.importlib, "import_module", fake_import_module)

    plotting.setup_mpl(True, None, misc_mpl_mods=False)

    assert calls["converter"] == "concise"
    assert calls["enabled"] is True
    assert "formats" in cast(dict, calls["formatter_args"])


def test_add_sorted_driver_legend_groups_by_team():
    """Test driver legend sorting by team order."""
    session = MockSession(year=2024)
    _, ax = plt.subplots()
    ax.plot([0, 1], [0, 1], label="HAM")
    ax.plot([0, 1], [1, 2], label="LEC")
    ax.plot([0, 1], [2, 3], label="VER")
    ax.plot([0, 1], [3, 4], label="PER")

    legend = plotting.add_sorted_driver_legend(ax, session)
    labels = [text.get_text() for text in legend.get_texts()]
    assert labels == ["VER", "PER", "HAM", "LEC"]


def test_add_sorted_driver_legend_keeps_unresolved_labels_last():
    """Test unresolved legend labels are appended after resolved drivers."""
    plt.switch_backend("Agg")  # Use non-interactive backend
    session = MockSession(year=2024)
    _, ax = plt.subplots()
    ax.plot([0, 1], [0, 1], label="HAM")
    ax.plot([0, 1], [1, 2], label="Random label")
    ax.plot([0, 1], [2, 3], label="VER")

    legend = plotting.add_sorted_driver_legend(ax, session)
    labels = [text.get_text() for text in legend.get_texts()]
    assert labels == ["VER", "HAM", "Random label"]


def test_override_team_constants_requires_session():
    """Test that overrides require session context."""
    with pytest.raises(ValueError, match="override_team_constants requires a session"):
        plotting.override_team_constants("Ferrari", None, fastf1_color="#ff0000")


def test_override_team_constants_session_scope():
    """Test that year-aware overrides apply only to the provided session."""
    session = MockSession(year=2024)
    other_session = MockSession(year=2024)

    plotting.override_team_constants(
        "Ferrari",
        session,
        short_name="Scuderia",
        fastf1_color="#ff0000",
    )

    assert plotting.get_team_color("Ferrari", session, colormap="fastf1") == "#ff0000"
    assert plotting.get_team_name("Ferrari", session, short=True) == "Scuderia"
    assert plotting.get_team_color("Ferrari", other_session, colormap="fastf1") == "#e8002d"


def test_override_team_constants_session_requires_exact_match():
    """Test session-scoped overrides keep exact-match semantics."""
    session = MockSession(year=2024)

    with pytest.raises(KeyError, match="No team found"):
        plotting.override_team_constants("Ferari", session, fastf1_color="#ff0000")
