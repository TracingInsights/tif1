"""Tests for plotting module."""

from __future__ import annotations

import importlib

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
        plotting.get_driver_style("VER", [1, 2], session)

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
    assert plotting.get_team_color("Ferrari", MockSession(year=2024), colormap="fastf1") == "#e8002d"
    assert plotting.get_team_color("Ferrari", MockSession(year=2021), colormap="fastf1") == "#dc0004"
    assert plotting.get_team_color("Ferrari", MockSession(year=2018), colormap="fastf1") == "#dc0000"


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
    assert "formats" in calls["formatter_args"]


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
