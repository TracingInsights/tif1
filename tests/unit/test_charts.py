"""Tests for the native tif1.charts functions.

All tests run fully offline: ``tif1.charts._common.load_session`` (the single
loading seam) is monkeypatched to return a synthetic :class:`FakeSession`, so
the real ``get_session`` is never called.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import ClassVar
from unittest import mock

import matplotlib as mpl

mpl.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from tif1.charts import (
    _common,
    lap_times,
    performance,
    telemetry,
    top_speeds,
    track_maps,
)
from tif1.exceptions import DataNotFoundError

# fmt: off
DRIVERS = [
    ("VER", "Red Bull Racing", "1"),
    ("PER", "Red Bull Racing", "11"),
    ("LEC", "Ferrari", "16"),
    ("SAI", "Ferrari", "55"),
    ("HAM", "Mercedes", "44"),
]
# fmt: on

N_LAPS = 12
_TEAM_COLORS = {
    "Red Bull Racing": "#0600ef",
    "Ferrari": "#dc0000",
    "Mercedes": "#00f5d0",
    "McLaren": "#ff8700",
    "Aston Martin": "#006f62",
    "Alpine": "#0093cc",
    "Williams": "#005aff",
    "RB": "#6692ff",
    "Kick Sauber": "#52e252",
    "Haas": "#b6babd",
}


class FakeEvent:
    """Minimal dict-like event with attribute and item access."""

    def __init__(self, name: str, year: int) -> None:
        self.name = name
        self.year = year
        self._event_name = name
        self._event_date = pd.Timestamp(f"{year}-07-21")

    def __getitem__(self, key: str):
        if key == "EventName":
            return self._event_name
        if key == "EventDate":
            return self._event_date
        raise KeyError(key)


class FakeTelemetry(pd.DataFrame):
    """Telemetry frame with the ``add_distance`` method already satisfied."""

    _metadata: ClassVar[list[str]] = []

    def add_distance(self, drop_existing: bool = True):  # noqa: ARG002
        return self


class FakeLap:
    """A single lap row exposing the fastf1-style surface the charts use."""

    def __init__(self, session, row: pd.Series) -> None:
        self.session = session
        self._row = row

    def __getitem__(self, key):
        return self._row[key]

    def get_car_data(self):
        return self.session.telemetry_for(self._row["Driver"], self._row["LapNumber"])

    def get_telemetry(self):
        return self.get_car_data()


class FakeLaps(pd.DataFrame):
    """DataFrame subclass with fastf1-style ``pick_*`` methods."""

    _metadata: ClassVar[list[str]] = ["session"]

    def pick_driver(self, identifier):
        return self.pick_drivers([identifier])

    def pick_drivers(self, identifiers):
        if isinstance(identifiers, (str, int)) or not isinstance(identifiers, (list, tuple, set)):
            identifiers = [identifiers]
        normalized = [str(i) for i in identifiers]
        result = FakeLaps(self[self["Driver"].isin(normalized)])
        result.session = self.session
        return result

    def pick_lap(self, lap_number):
        result = FakeLaps(self[self["LapNumber"] == lap_number])
        result.session = self.session
        return result

    def get_car_data(self, **kwargs):  # noqa: ARG002
        if self.empty:
            return FakeTelemetry()
        row = self.iloc[0]
        return self.session.telemetry_for(row["Driver"], int(row["LapNumber"]))

    def pick_fastest(self, only_by_time: bool = False):  # noqa: ARG002
        if self.empty:
            return None
        row = self.loc[self["LapTime"].idxmin()]
        return FakeLap(self.session, row)


class FakeSession:
    """Synthetic session covering exactly the surface the charts touch."""

    def __init__(
        self, *, year: int = 2023, name: str = "Qualifying", with_laps: bool = True
    ) -> None:
        self.year = year
        self.name = name
        self.event = FakeEvent("Italian Grand Prix", year)
        self.drivers = [dn for _, _, dn in DRIVERS]
        self._laps = _make_laps() if with_laps else _make_laps().iloc[0:0]
        self._laps.session = self

        rows = [
            {
                "Abbreviation": abbr,
                "TeamName": team,
                "FirstName": abbr,
                "LastName": abbr,
                "FullName": abbr,
            }
            for abbr, team, _dn in DRIVERS
        ]
        self._results = pd.DataFrame(rows)
        self.drivers_df = pd.DataFrame(
            {
                "Driver": [abbr for abbr, _, _ in DRIVERS],
                "Team": [team for _, team, _ in DRIVERS],
                "DriverNumber": [dn for _, _, dn in DRIVERS],
                "TeamColor": [_TEAM_COLORS[team] for _, team, _ in DRIVERS],
                "FirstName": [abbr for abbr, _, _ in DRIVERS],
                "LastName": [abbr for abbr, _, _ in DRIVERS],
                "HeadshotUrl": [""] * len(DRIVERS),
            }
        )

    @property
    def laps(self) -> FakeLaps:
        return self._laps

    @laps.setter
    def laps(self, value: FakeLaps) -> None:
        self._laps = value

    @property
    def results(self) -> pd.DataFrame:
        return self._results

    def get_driver(self, identifier):
        for abbr, team, dn in DRIVERS:
            if str(identifier) == dn:
                return {
                    "Abbreviation": abbr,
                    "Team": team,
                    "DriverNumber": dn,
                    "TeamColor": _TEAM_COLORS[team],
                }
        raise KeyError(f"Driver {identifier} not found")

    def get_circuit_info(self):
        corners = pd.DataFrame(
            {
                "Number": [1, 2, 3, 4],
                "Letter": ["", "", "", ""],
                "Distance": [500.0, 1500.0, 2500.0, 3500.0],
            }
        )
        return SimpleNamespace(corners=corners)

    def telemetry_for(self, driver: str, lap_number: int) -> FakeTelemetry:
        idx = [d[0] for d in DRIVERS].index(driver)
        return _make_telemetry(idx, lap_number)


def _make_laps() -> FakeLaps:
    """Synthetic lap data: 5 drivers x 12 laps with a wide pace spread."""
    rows = []
    for pos, (abbr, team, _dn) in enumerate(DRIVERS, start=1):
        for lap_num in range(1, N_LAPS + 1):
            # Lap time spread: ~1.5 s/lap delta per position -> HAM is ~10 s off VER
            base_seconds = 90.0 + pos * 2.5 + lap_num * 0.1 + 0.05 * (lap_num % 3)
            compound = "SOFT" if lap_num <= 6 else "MEDIUM"
            tyre_life = lap_num if lap_num <= 6 else lap_num - 6
            rows.append(
                {
                    "Driver": abbr,
                    "Team": team,
                    "LapNumber": lap_num,
                    "LapTime": pd.Timedelta(seconds=base_seconds),
                    "Position": pos,
                    "Compound": compound,
                    "Stint": 1 if lap_num <= 6 else 2,
                    "TyreLife": tyre_life,
                    "Deleted": abbr == "VER" and lap_num == 2,
                    "PitInTime": pd.Timestamp("2023-07-21 14:30:00")
                    if abbr == "HAM" and lap_num == 5
                    else pd.NaT,
                    "PitOutTime": pd.Timestamp("2023-07-21 14:31:00")
                    if abbr == "HAM" and lap_num == 5
                    else pd.NaT,
                    "SpeedI1": 300.0 + pos * 2,
                    "SpeedI2": 310.0 + pos * 2,
                    "SpeedST": 320.0 + pos * 2,
                    "SpeedFL": 290.0 + pos * 2,
                    "TrackTemp": 35.0 + 0.5 * lap_num,
                }
            )
    return FakeLaps(pd.DataFrame(rows))


def _make_telemetry(driver_idx: int, lap_number: int) -> FakeTelemetry:
    """Synthetic fastest-lap telemetry (~64 samples of a lap)."""
    n = 64
    lap_seconds = 85.0 + driver_idx * 2.0
    t_sec = np.linspace(0, lap_seconds, n)
    speed = 180.0 * (1 + driver_idx * 0.02) + 120.0 * np.sin(np.linspace(0, 2 * np.pi, n))
    dt = np.diff(t_sec, prepend=t_sec[0])
    distance = np.cumsum(speed / 3.6 * dt)

    u = np.linspace(0, 2 * np.pi, n)
    x = np.cos(u) * 400.0 + np.sin(u * 3) * 20.0
    y = np.sin(u) * 250.0 + np.cos(u * 2) * 15.0

    brake = (np.sin(u * 6) > 0.7).astype(int)
    throttle = np.where(brake > 0, 0.0, 100.0)
    gear = np.clip(np.round(speed / 40.0), 1, 8)

    return FakeTelemetry(
        {
            "Time": pd.to_timedelta(t_sec, unit="s"),
            "SessionTime": pd.to_timedelta(t_sec, unit="s"),
            "Distance": distance,
            "Speed": speed,
            "X": x,
            "Y": y,
            "Throttle": throttle,
            "Brake": brake,
            "nGear": gear,
        }
    )


@pytest.fixture
def fake_session(monkeypatch):
    """Patch the session loading seam and yield a synthetic session."""
    session = FakeSession()
    monkeypatch.setattr(_common, "load_session", mock.Mock(return_value=session))
    yield session
    plt.close("all")


@pytest.fixture
def no_laps_session(monkeypatch):
    """Patch the seam with a session that has no lap data."""
    session = FakeSession(with_laps=False)
    monkeypatch.setattr(_common, "load_session", mock.Mock(return_value=session))
    yield session
    plt.close("all")


def _make_launch_telemetry(driver_idx: int) -> FakeTelemetry:
    """Telemetry with a monotonic launch ramp crossing 50/100/150/200 km/h.

    Higher ``driver_idx`` means a slightly faster ramp, giving distinct
    crossing times per driver so the 0-10 ratings differ.
    """
    n = 128
    t_sec = np.linspace(0.0, 8.0, n)
    speed = np.linspace(0.0, 300.0, n) * (1 + driver_idx * 0.02)
    return FakeTelemetry({"Time": pd.to_timedelta(t_sec, unit="s"), "Speed": speed})


#: Extra drivers used to exercise charts with bigger grids (15/22 drivers).
_MORE_DRIVERS = [
    ("RUS", "Mercedes", "63"),
    ("NOR", "McLaren", "4"),
    ("PIA", "McLaren", "81"),
    ("ALO", "Aston Martin", "14"),
    ("STR", "Aston Martin", "18"),
    ("OCO", "Alpine", "31"),
    ("GAS", "Alpine", "10"),
    ("ALB", "Williams", "23"),
    ("SAR", "Williams", "2"),
    ("TSU", "RB", "22"),
    ("RIC", "RB", "3"),
    ("BOT", "Kick Sauber", "77"),
    ("ZHO", "Kick Sauber", "24"),
    ("HUL", "Haas", "27"),
    ("MAG", "Haas", "20"),
    ("LAW", "Red Bull Racing", "6"),
    ("BEA", "Ferrari", "7"),
]


def _make_big_session(n_drivers: int, *, launch: bool = False):
    """Build a FakeSession-like session with ``n_drivers`` (15 or 22).

    Reuses the real :class:`FakeSession` machinery but with an extended
    driver list, so charts that resolve drivers/teams/colors see the full
    grid size. ``launch`` switches telemetry to the launch ramp so the
    race-launch ratings chart can process every driver.
    """

    driver_list = DRIVERS + _MORE_DRIVERS[: n_drivers - len(DRIVERS)]

    class BigSession(FakeSession):
        pass

    session = BigSession(year=2024)
    laps_rows = []
    for pos, (abbr, team, _dn) in enumerate(driver_list, start=1):
        for lap_num in range(1, N_LAPS + 1):
            base_seconds = 90.0 + pos * 2.5 + lap_num * 0.1 + 0.05 * (lap_num % 3)
            compound = "SOFT" if lap_num <= 6 else "MEDIUM"
            laps_rows.append(
                {
                    "Driver": abbr,
                    "Team": team,
                    "LapNumber": lap_num,
                    "LapTime": pd.Timedelta(seconds=base_seconds),
                    "Position": pos,
                    "Compound": compound,
                    "Stint": 1 if lap_num <= 6 else 2,
                    "TyreLife": lap_num if lap_num <= 6 else lap_num - 6,
                    "Deleted": False,
                    "PitInTime": pd.NaT,
                    "PitOutTime": pd.NaT,
                    "SpeedI1": 300.0 + pos * 2,
                    "SpeedI2": 310.0 + pos * 2,
                    "SpeedST": 320.0 + pos * 2,
                    "SpeedFL": 290.0 + pos * 2,
                    "TrackTemp": 35.0 + 0.5 * lap_num,
                }
            )
    laps = FakeLaps(pd.DataFrame(laps_rows))
    laps.session = session
    session._laps = laps

    session._results = pd.DataFrame(
        [
            {
                "Abbreviation": abbr,
                "TeamName": team,
                "FirstName": abbr,
                "LastName": abbr,
                "FullName": abbr,
            }
            for abbr, team, _dn in driver_list
        ]
    )
    session.drivers_df = pd.DataFrame(
        {
            "Driver": [abbr for abbr, _, _ in driver_list],
            "Team": [team for _, team, _ in driver_list],
            "DriverNumber": [dn for _, _, dn in driver_list],
            "TeamColor": [_TEAM_COLORS[team] for _, team, _ in driver_list],
            "FirstName": [abbr for abbr, _, _ in driver_list],
            "LastName": [abbr for abbr, _, _ in driver_list],
            "HeadshotUrl": [""] * len(driver_list),
        }
    )
    session.drivers = [dn for _, _, dn in driver_list]

    def get_driver(identifier):
        for abbr, team, dn in driver_list:
            if str(identifier) == dn:
                return {
                    "Abbreviation": abbr,
                    "Team": team,
                    "DriverNumber": dn,
                    "TeamColor": _TEAM_COLORS[team],
                }
        raise KeyError(f"Driver {identifier} not found")

    session.get_driver = get_driver

    order = [abbr for abbr, _, _ in driver_list]
    if launch:
        session.telemetry_for = lambda driver, lap: _make_launch_telemetry(  # noqa: ARG005
            order.index(driver)
        )
    else:
        session.telemetry_for = lambda driver, lap: _make_telemetry(
            order.index(driver), lap
        )
    return session


@pytest.fixture
def launch_session(monkeypatch):
    """Patch the seam with a session whose telemetry ramps from 0 km/h."""
    session = FakeSession(year=2024)
    driver_order = [abbr for abbr, _team, _dn in DRIVERS]
    session.telemetry_for = lambda driver, lap: _make_launch_telemetry(  # noqa: ARG005
        driver_order.index(driver)
    )
    monkeypatch.setattr(_common, "load_session", mock.Mock(return_value=session))
    yield session
    plt.close("all")


# ---------------------------------------------------------------------------
# Happy paths: every function executes with defaults and returns (fig, ax)
# ---------------------------------------------------------------------------

CHART_CASES = [
    (top_speeds.plot_top_speeds, "patches"),
    (track_maps.plot_track_speed_map, "collections"),
    (track_maps.plot_track_throttle_map, "collections"),
    (track_maps.plot_track_brake_zones, "collections"),
    (track_maps.plot_track_acceleration_map, "collections"),
    (track_maps.plot_gear_shifts, "collections"),
    (telemetry.plot_annotated_speed_trace, "lines"),
    (telemetry.plot_speed_traces, "lines"),
    (track_maps.plot_multi_driver_speed_comparison, "collections"),
    (lap_times.plot_lap_delta, "patches"),
    (telemetry.plot_telemetry_comparison, "lines"),
    (telemetry.plot_gg_diagram, "collections"),
    (lap_times.plot_driver_laptimes, "collections"),
    (lap_times.plot_laptimes_distribution, "collections"),
    (lap_times.plot_laptime_heatmap, "collections"),
    (lap_times.plot_qualifying_grid, "patches"),
    (lap_times.plot_position_changes, "lines"),
    (lap_times.plot_track_temperature, "lines"),
    (performance.plot_downforce_levels, "patches"),
    (performance.plot_throttle_distance, "patches"),
    (performance.plot_tire_degradation, "collections"),
]


@pytest.mark.parametrize(("func", "attr"), CHART_CASES, ids=[c[0].__name__ for c in CHART_CASES])
def test_chart_happy_path(fake_session, func, attr):
    """Every chart runs with defaults and returns real artists."""
    fig, ax = func(2023, "Italian Grand Prix", "Q")
    assert isinstance(fig, plt.Figure)
    if isinstance(ax, np.ndarray):
        assert ax.size == 4
        assert sum(len(a.lines) + len(a.patches) + len(a.collections) for a in ax) > 0
    else:
        assert len(getattr(ax, attr)) > 0


# ---------------------------------------------------------------------------
# Shared filter pipeline
# ---------------------------------------------------------------------------


def test_apply_common_filters_lap_numbers(fake_session):
    out = _common.apply_common_filters(fake_session.laps, laps=[1, 2, 3])
    assert set(out["LapNumber"].unique()) <= {1, 2, 3}


def test_apply_common_filters_drivers(fake_session):
    out = _common.apply_common_filters(fake_session.laps, drivers=["VER"], session=fake_session)
    assert set(out["Driver"].unique()) == {"VER"}


def test_apply_common_filters_teams(fake_session):
    out = _common.apply_common_filters(fake_session.laps, teams=["Ferrari"], session=fake_session)
    assert set(out["Driver"].unique()) == {"LEC", "SAI"}


def test_apply_common_filters_n_drivers_top_n(fake_session):
    finish_order = _common.finishing_order(fake_session, len(fake_session.drivers))
    assert finish_order == ["VER", "PER", "LEC", "SAI", "HAM"]
    out = _common.apply_common_filters(fake_session.laps, n_drivers=2, finish_order=finish_order)
    assert set(out["Driver"].unique()) == {"VER", "PER"}


def test_apply_common_filters_deleted_and_pit_laps(fake_session):
    default = _common.apply_common_filters(fake_session.laps)
    assert not default["Deleted"].any()
    assert default["PitInTime"].notna().sum() == 0

    with_deleted = _common.apply_common_filters(fake_session.laps, include_deleted=True)
    assert with_deleted["Deleted"].any()

    with_pit = _common.apply_common_filters(fake_session.laps, include_pit_laps=True)
    assert with_pit["PitInTime"].notna().any()


def test_apply_common_filters_cutoff_scope(fake_session):
    """Global cutoff drops slow drivers; per-driver cutoff keeps them."""
    laps = fake_session.laps
    per_driver = _common.apply_laptime_cutoff(laps, 1.10, "per_driver")
    global_scope = _common.apply_laptime_cutoff(laps, 1.10, "global")

    assert "HAM" in set(per_driver["Driver"])  # HAM is slow overall but fine vs own fastest
    assert "HAM" not in set(global_scope["Driver"])
    assert len(per_driver) > len(global_scope)


def test_apply_common_filters_cutoff_disabled(fake_session):
    out = _common.apply_common_filters(
        fake_session.laps, laptime_cutoff=None, include_pit_laps=True, include_deleted=True
    )
    assert len(out) == len(fake_session.laps)


def test_filters_through_chart(fake_session):
    """A chart accepts the shared filters and applies them."""
    fig, ax = lap_times.plot_driver_laptimes(2023, "Italian Grand Prix", "Q", drivers=["VER"])
    assert "VER" in ax.get_title() or fig.texts
    title_texts = [t.get_text() for t in fig.texts]
    assert any("VER Lap Times" in t for t in title_texts)


# ---------------------------------------------------------------------------
# Output behaviour
# ---------------------------------------------------------------------------


def test_save_path_writes_file(fake_session, tmp_path):
    out = tmp_path / "top_speeds.png"
    top_speeds.plot_top_speeds(
        2023, "Italian Grand Prix", "Q", save_path=str(out), dpi=150, facecolor="#1a1a1a"
    )
    assert out.exists()
    assert out.stat().st_size > 0


def test_no_save_without_save_path(fake_session, monkeypatch):
    def _fail_save(self, *args, **kwargs):
        raise AssertionError("savefig must not be called without save_path")

    monkeypatch.setattr(plt.Figure, "savefig", _fail_save)
    fig, _ = top_speeds.plot_top_speeds(2023, "Italian Grand Prix", "Q")
    assert fig is not None


def test_savefig_kwargs_facecolor_and_dpi(fake_session, tmp_path, monkeypatch):
    captured: dict[str, object] = {}
    original = plt.Figure.savefig

    def _capture_save(self, fname, **kwargs):
        captured["kwargs"] = kwargs
        return original(self, fname, **kwargs)

    monkeypatch.setattr(plt.Figure, "savefig", _capture_save)

    top_speeds.plot_top_speeds(
        2023, "Italian Grand Prix", "Q", save_path=str(tmp_path / "a.png"), facecolor=None, dpi=150
    )
    assert "facecolor" not in captured["kwargs"]
    assert captured["kwargs"]["dpi"] == 150
    assert captured["kwargs"]["bbox_inches"] == "tight"

    top_speeds.plot_top_speeds(
        2023,
        "Italian Grand Prix",
        "Q",
        save_path=str(tmp_path / "b.png"),
        facecolor="#1a1a1a",
        dpi=300,
    )
    assert captured["kwargs"]["facecolor"] == "#1a1a1a"
    assert captured["kwargs"]["dpi"] == 300


def test_facecolor_sets_figure_background(fake_session):
    fig, _ = top_speeds.plot_top_speeds(2023, "Italian Grand Prix", "Q", facecolor="#1a1a1a")
    assert fig.get_facecolor() == (
        0.10196078431372549,
        0.10196078431372549,
        0.10196078431372549,
        1.0,
    )

    fig2, _ = lap_times.plot_driver_laptimes(2023, "Italian Grand Prix", "Q", facecolor=None)
    assert fig2.get_facecolor() != (
        0.10196078431372549,
        0.10196078431372549,
        0.10196078431372549,
        1.0,
    )


# ---------------------------------------------------------------------------
# Theme behaviour
# ---------------------------------------------------------------------------

_FASTF1_KEYS = [
    "figure.facecolor",
    "axes.facecolor",
    "text.color",
    "axes.labelcolor",
    "xtick.color",
    "ytick.color",
]


def test_default_theme_applies_fastf1(fake_session):
    top_speeds.plot_top_speeds(2023, "Italian Grand Prix", "Q")
    assert plt.rcParams["figure.facecolor"] == "#292625"


def test_color_scheme_none_leaves_rcparams(fake_session):
    before = {key: plt.rcParams[key] for key in _FASTF1_KEYS}
    top_speeds.plot_top_speeds(2023, "Italian Grand Prix", "Q", color_scheme=None)
    after = {key: plt.rcParams[key] for key in _FASTF1_KEYS}
    assert before == after


# ---------------------------------------------------------------------------
# Chart-specific behaviour
# ---------------------------------------------------------------------------


def test_telemetry_comparison_requires_two_drivers(fake_session):
    with pytest.raises(ValueError, match="exactly two drivers"):
        telemetry.plot_telemetry_comparison(2023, "Italian Grand Prix", "Q", drivers=["VER"])


def test_lap_delta_requires_two_drivers(fake_session):
    with pytest.raises(ValueError, match="exactly two drivers"):
        lap_times.plot_lap_delta(2023, "Italian Grand Prix", "Q", drivers=["VER"])


def test_lap_delta_auto_ylim_not_hardcoded(fake_session):
    _, ax = lap_times.plot_lap_delta(2023, "Italian Grand Prix", "Q")
    ymin, ymax = ax.get_ylim()
    assert ymax - ymin < 5  # no longer the hardcoded (-2, 2) range


def test_position_changes_ylim_derived_from_data(fake_session):
    _, ax = lap_times.plot_position_changes(2023, "Italian Grand Prix", "Q")
    bottom, top = ax.get_ylim()  # axis is inverted: (max_pos + 0.5, 0.5)
    assert bottom == 5.5  # max_pos (5) + 0.5, not the hardcoded 20.5
    assert top == 0.5


def test_top_speeds_speed_trap_validation(fake_session):
    with pytest.raises(ValueError, match="speed_trap"):
        top_speeds.plot_top_speeds(2023, "Italian Grand Prix", "Q", speed_trap="SpeedXX")


def test_top_speeds_teams_filter(fake_session):
    _, ax = top_speeds.plot_top_speeds(2023, "Italian Grand Prix", "Q", teams=["Ferrari"])
    y_labels = [t.get_text() for t in ax.get_yticklabels()]
    assert set(y_labels) == {"Ferrari"}


def test_qualifying_grid_include_deleted_false_by_default(fake_session):
    """The deleted VER lap must never win the pole."""
    _, ax = lap_times.plot_qualifying_grid(2023, "Italian Grand Prix", "Q")
    y_labels = [t.get_text() for t in ax.get_yticklabels()]
    assert y_labels[0] == "VER"  # VER still on pole (deleted lap excluded)


# ---------------------------------------------------------------------------
# Per-driver telemetry loops: skip failures, raise when nothing processed
# ---------------------------------------------------------------------------


def test_per_driver_loop_skips_failing_driver(fake_session):
    session = FakeSession()
    session.laps = session.laps[~session.laps["Driver"].isin(["LEC"])]
    _, ax = telemetry.plot_gg_diagram(2023, "Italian Grand Prix", "Q", drivers=["VER", "LEC"])
    assert len(ax.collections) >= 1


def test_per_driver_loop_raises_when_none_processed(no_laps_session):
    with pytest.raises(ValueError, match="No driver could be processed"):
        telemetry.plot_gg_diagram(2023, "Italian Grand Prix", "Q")
    with pytest.raises(ValueError, match="No driver could be processed"):
        performance.plot_downforce_levels(2023, "Italian Grand Prix", "Q")
    with pytest.raises(ValueError, match="No driver could be processed"):
        performance.plot_throttle_distance(2023, "Italian Grand Prix", "Q")
    with pytest.raises(ValueError, match="No driver could be processed"):
        track_maps.plot_multi_driver_speed_comparison(2023, "Italian Grand Prix", "Q")


# ---------------------------------------------------------------------------
# Error propagation from the loading seam
# ---------------------------------------------------------------------------


def test_empty_laps_raise_clean_value_error(no_laps_session):
    """Charts without lap data raise a clear ValueError, not a raw IndexError."""
    with pytest.raises(ValueError, match="qualifying grid"):
        lap_times.plot_qualifying_grid(2023, "Italian Grand Prix", "Q")
    with pytest.raises(ValueError, match="track temperature"):
        lap_times.plot_track_temperature(2023, "Italian Grand Prix", "R")
    with pytest.raises(ValueError, match="heatmap"):
        lap_times.plot_laptime_heatmap(2023, "Italian Grand Prix", "R")


def test_load_session_error_propagates(monkeypatch):
    def _boom(*args, **kwargs):
        raise DataNotFoundError(2023, "Italian Grand Prix")

    monkeypatch.setattr(_common, "load_session", _boom)
    with pytest.raises(DataNotFoundError):
        top_speeds.plot_top_speeds(2023, "Italian Grand Prix", "Q")


def test_unknown_driver_fuzzy_resolves_with_warning(fake_session):
    """Unknown driver identifiers are fuzzy-resolved with a correction warning."""
    with pytest.warns(UserWarning, match="Correcting user input"):
        _, ax = lap_times.plot_driver_laptimes(2023, "Italian Grand Prix", "Q", drivers=["ZZZ"])
    assert len(ax.collections) > 0


# ---------------------------------------------------------------------------
# Race launch performance ratings
# ---------------------------------------------------------------------------


def test_race_launch_ratings_happy_path(launch_session):
    """Default-dark chart renders one bar per driver with tyre and car images."""
    fig, ax = performance.plot_race_launch_ratings(2024, "Italian Grand Prix", "Q")
    assert isinstance(fig, plt.Figure)
    assert len(ax.patches) == len(DRIVERS)
    # one tyre image per driver, plus cars hidden by the 2.5 threshold
    assert len(ax.artists) >= len(DRIVERS)
    assert len(ax.artists) < 2 * len(DRIVERS)
    # default-dark theme applied
    assert plt.rcParams["figure.facecolor"] == "#011627"
    # title mentions the event
    title_texts = " ".join(text.get_text() for text in fig.texts)
    assert "Italian Grand Prix" in title_texts
    assert "Lights out to 50 kmph" in title_texts


def test_race_launch_ratings_rating_order(launch_session):
    """Drivers are sorted by rating descending (fastest to 50 km/h first)."""
    _, ax = performance.plot_race_launch_ratings(2024, "Italian Grand Prix", "Q")
    labels = [text.get_text() for text in ax.get_yticklabels()]
    assert labels[0].startswith("1. HAM")
    assert labels[-1].startswith("5. VER")


def test_race_launch_ratings_speed_range(launch_session):
    """Range ratings use the X-Y km/h window and title."""
    fig, ax = performance.plot_race_launch_ratings(
        2024, "Italian Grand Prix", "Q", speed_range=(100, 200)
    )
    assert len(ax.patches) == len(DRIVERS)
    title_texts = " ".join(text.get_text() for text in fig.texts)
    assert "100 to 200 kmph" in title_texts


def test_race_launch_ratings_drivers_filter(launch_session):
    """The drivers filter restricts which bars are drawn."""
    _, ax = performance.plot_race_launch_ratings(
        2024, "Italian Grand Prix", "Q", drivers=["HAM", "VER"]
    )
    assert len(ax.patches) == 2


def test_race_launch_ratings_light_scheme(launch_session):
    """A default-light color_scheme switches the theme and style config."""
    _, ax = performance.plot_race_launch_ratings(
        2024, "Italian Grand Prix", "Q", color_scheme="default-light"
    )
    assert plt.rcParams["figure.facecolor"] == "lightblue"
    assert len(ax.patches) == len(DRIVERS)
    # light style has no car threshold: every driver gets a car image
    assert len(ax.artists) == 2 * len(DRIVERS)


def test_race_launch_ratings_validation(launch_session):
    """Invalid speed windows are rejected before loading any data."""
    with pytest.raises(ValueError, match="speed_threshold"):
        performance.plot_race_launch_ratings(2024, "Italian Grand Prix", "Q", speed_threshold=75)
    with pytest.raises(ValueError, match="speed_range"):
        performance.plot_race_launch_ratings(
            2024, "Italian Grand Prix", "Q", speed_range=(200, 100)
        )


def test_race_launch_ratings_no_data_raises(fake_session):
    """A session whose telemetry never crosses 50 km/h yields no ratings."""
    with pytest.raises(ValueError, match="No driver could be processed"):
        performance.plot_race_launch_ratings(2023, "Italian Grand Prix", "Q")


def test_race_launch_ratings_save_path(launch_session, tmp_path):
    """save_path writes a file at the requested dpi."""
    out = tmp_path / "launch.png"
    performance.plot_race_launch_ratings(
        2024, "Italian Grand Prix", "Q", save_path=str(out), dpi=150
    )
    assert out.exists()
    assert out.stat().st_size > 0


def test_race_launch_ratings_bottom_space_is_compact(launch_session):
    """The launch chart must not waste canvas space at the bottom.

    The v2 script's 15% bottom subplot margin left a large empty band below
    the footer on the full-canvas export. The default-* styles now use a slim
    bottom margin with a matching footer position, and the y-limits are
    tightened to the bar range so the bars span the axes.
    """
    fig, ax = performance.plot_race_launch_ratings(2024, "Italian Grand Prix", "Q")
    fig.canvas.draw()

    # Axes bottom edge must sit low in the figure (small margin), not at 15%.
    bounds = ax.get_position().bounds
    assert bounds[1] < 0.08, f"axes bottom margin too large: {bounds[1]:.3f}"

    # The suptitle must clear the axes top edge (no title/bar collision).
    title_bottom = fig._suptitle.get_window_extent().y0
    axes_top = ax.transAxes.transform((0, 1))[1]
    assert title_bottom >= axes_top - 1e-6, "suptitle overlaps the axes"

    # Tight y-limits: bars (0..n-1, height 0.8) should nearly fill the axes.
    n = len(launch_session.drivers)
    bottom, top = sorted(ax.get_ylim())
    assert bottom <= -0.4 - 1e-9  # first bar's lower edge is at -0.4
    assert top >= n - 1 + 0.4 - 1e-9  # last bar's upper edge is at n - 0.6
    assert top - bottom < n + 1.5  # no big auto-margin beyond the bars


def test_race_launch_ratings_labels_fit_inside_xlim(launch_session):
    """Bar labels at the bar ends must stay inside the figure canvas.

    The launch chart exports the full canvas (no bbox crop), so the x-limit
    must be expanded to the widest bar-label extent - otherwise the trailing
    rating labels get clipped at the right edge of the figure.
    """
    fig, ax = performance.plot_race_launch_ratings(2024, "Italian Grand Prix", "Q")
    fig.canvas.draw()
    xlim_right = ax.get_xlim()[1]
    for label in ax.texts:
        extent = label.get_window_extent()
        data_x = ax.transData.inverted().transform((extent.x1, extent.y0))[0]
        assert data_x <= xlim_right, f"label {label.get_text()!r} overflows x-limit"


@pytest.mark.parametrize("n_drivers", [15, 22])
def test_bar_charts_ylim_scale_with_driver_count(n_drivers, monkeypatch):
    """Every bar chart tightens its y-limits for any grid size.

    The shared ``set_tight_barh_ylim`` helper must keep the bars filling the
    axes whether 15 or 22 drivers are selected - no fixed 20-row assumption.
    """
    session = _make_big_session(n_drivers)
    monkeypatch.setattr(_common, "load_session", mock.Mock(return_value=session))

    for func in (
        performance.plot_downforce_levels,
        performance.plot_throttle_distance,
        lap_times.plot_qualifying_grid,
    ):
        _, ax = func(2024, "Italian Grand Prix", "Q")
        assert len(ax.patches) == n_drivers, f"{func.__name__} bars"
        bottom, top = sorted(ax.get_ylim())
        # Rows at 0..n-1 with height 0.8 span -0.4..n-0.6; the helper adds
        # exactly 0.1 padding each side -> total span n (no auto-margins).
        assert bottom <= -0.4 + 1e-9
        assert top >= n_drivers - 1 + 0.4 - 1e-9
        assert top - bottom < n_drivers + 0.5, "y-limits must hug the bars"
        plt.close("all")


@pytest.mark.parametrize("n_drivers", [15, 22])
def test_bar_charts_value_labels_fit_inside_xlim(n_drivers, monkeypatch):
    """Value labels on the bar charts must sit inside the x-limit at any grid size.

    When the value spread is small (or identical, as with the synthetic
    throttle data) the fixed label offset can push labels past the auto-scaled
    x-limit, so they float outside the axes box on inline display. The shared
    ``fit_labels_inside_xlim`` helper must expand the limit so every value
    label stays inside the axes for 15 and 22 drivers alike.
    """
    session = _make_big_session(n_drivers)
    monkeypatch.setattr(_common, "load_session", mock.Mock(return_value=session))

    for func in (
        performance.plot_downforce_levels,
        performance.plot_throttle_distance,
        top_speeds.plot_top_speeds,
    ):
        # default-dark is the harshest layout: its large fonts widen the value
        # labels, and labels fitted before tight_layout overflow once the axes
        # are resized for those fonts. Exercising it here makes the test
        # deterministic regardless of what earlier tests left in rcParams.
        fig, ax = func(2024, "Italian Grand Prix", "Q", color_scheme="default-dark")
        fig.canvas.draw()
        xlim_right = ax.get_xlim()[1]
        for label in ax.texts:
            if not (label.get_text() or "").strip():
                continue
            extent = label.get_window_extent()
            data_x = ax.transData.inverted().transform((extent.x1, extent.y0))[0]
            assert data_x <= xlim_right + 1e-6, (
                f"{func.__name__} label {label.get_text()!r} overflows x-limit "
                f"({data_x:.3f} > {xlim_right:.3f})"
            )
        plt.close("all")


@pytest.mark.parametrize("n_drivers", [15, 22])
def test_position_changes_legend_stays_in_canvas(n_drivers, monkeypatch):
    """The position-changes legend must stay inside the figure at any grid size.

    The legend is anchored beside the axes (outside the plot area); with a
    top-anchored legend a 22-driver grid grew the legend down to the figure's
    bottom edge. It must be vertically centred next to the axes and fully
    inside the canvas for 15 and 22 drivers alike.
    """
    session = _make_big_session(n_drivers)
    monkeypatch.setattr(_common, "load_session", mock.Mock(return_value=session))

    fig, ax = lap_times.plot_position_changes(2024, "Italian Grand Prix", "R")
    fig.canvas.draw()

    assert len(ax.lines) == n_drivers
    legend = ax.get_legend()
    assert legend is not None
    assert len(legend.get_texts()) == n_drivers

    renderer = fig.canvas.get_renderer()
    fb = fig.bbox
    leg = legend.get_window_extent(renderer)
    ax_ext = ax.get_window_extent(renderer)

    # Legend fully inside the canvas (nothing clipped), with a margin from
    # the top/bottom edges - the old top-anchored legend sat flush against
    # the bottom edge (y0 = 2px) on a 22-driver grid.
    assert leg.x0 >= fb.x0
    assert leg.x1 <= fb.x1
    assert leg.y0 >= fb.y0 + 5, f"legend flush against bottom edge (y0={leg.y0:.0f}px)"
    assert leg.y1 <= fb.y1 - 5, f"legend flush against top edge (y1={leg.y1:.0f}px)"

    # Legend clear of the plot area (no overlap with the lines).
    overlap_w = max(0, min(leg.x1, ax_ext.x1) - max(leg.x0, ax_ext.x0))
    assert overlap_w <= 1, f"legend overlaps the axes ({overlap_w:.1f}px)"

    # Vertically centred next to the axes: the legend's centre aligns with
    # the axes' centre regardless of the theme's figure margins.
    leg_center = (leg.y0 + leg.y1) / 2
    ax_center = (ax_ext.y0 + ax_ext.y1) / 2
    assert abs(leg_center - ax_center) <= 30, (
        f"legend not centred on the axes (offset {abs(leg_center - ax_center):.1f}px)"
    )
    plt.close("all")


@pytest.mark.parametrize("n_drivers", [5, 15, 22])
def test_track_temperature_legend_stays_in_canvas(n_drivers, monkeypatch):
    """The track-temperature legend must not obscure the plot at any grid size.

    With the default legend placement matplotlib drops a 22-entry legend into
    the middle of the plot, covering the lines, and a top-anchored legend
    would grow downward past the canvas. The legend must sit outside the axes
    (beside the plot), be vertically centred on the axes, and stay fully
    inside the figure for 5, 15 and 22 drivers alike.
    """
    session = _make_big_session(n_drivers)
    monkeypatch.setattr(_common, "load_session", mock.Mock(return_value=session))
    drivers = list(session.laps["Driver"].unique())

    fig, ax = lap_times.plot_track_temperature(
        2024, "Italian Grand Prix", "R", drivers=drivers
    )
    fig.canvas.draw()

    assert len(ax.lines) == n_drivers
    legend = ax.get_legend()
    assert legend is not None
    assert len(legend.get_texts()) == n_drivers

    renderer = fig.canvas.get_renderer()
    fb = fig.bbox
    leg = legend.get_window_extent(renderer)
    ax_ext = ax.get_window_extent(renderer)

    # Legend fully inside the canvas (nothing clipped).
    assert leg.x0 >= fb.x0
    assert leg.x1 <= fb.x1
    assert leg.y0 >= fb.y0 - 1
    assert leg.y1 <= fb.y1 + 1

    # Legend must not overlap the plot area (sits beside the axes).
    overlap_w = max(0, min(leg.x1, ax_ext.x1) - max(leg.x0, ax_ext.x0))
    assert overlap_w <= 1, f"legend overlaps the axes ({overlap_w:.1f}px)"

    # Vertically centred next to the axes.
    leg_center = (leg.y0 + leg.y1) / 2
    ax_center = (ax_ext.y0 + ax_ext.y1) / 2
    assert abs(leg_center - ax_center) <= 30, (
        f"legend not centred on the axes (offset {abs(leg_center - ax_center):.1f}px)"
    )
    plt.close("all")


@pytest.mark.parametrize("n_drivers", [15, 22])
def test_line_scatter_charts_render_at_any_grid_size(n_drivers, monkeypatch):
    """Line/scatter charts render with full data at 15 and 22 drivers."""
    session = _make_big_session(n_drivers)
    monkeypatch.setattr(_common, "load_session", mock.Mock(return_value=session))

    # lap delta: exactly two drivers even on a big grid.
    _, ax = lap_times.plot_lap_delta(2024, "Italian Grand Prix", "Q")
    assert len(ax.patches) >= 1
    assert len(ax.get_legend().get_texts()) == 2
    plt.close("all")

    # tire degradation: aggregates all drivers' laps per compound.
    _, ax = performance.plot_tire_degradation(
        2024, "Italian Grand Prix", "R", min_laps=3, smoothing_window=3
    )
    assert len(ax.collections) >= 1
    assert len(ax.lines) >= 1
    plt.close("all")


@pytest.mark.parametrize("n_drivers", [15, 22])
def test_race_launch_ratings_any_driver_count(n_drivers, monkeypatch):
    """The launch chart keeps its compact layout for 15 and 22 drivers."""
    session = _make_big_session(n_drivers, launch=True)
    monkeypatch.setattr(_common, "load_session", mock.Mock(return_value=session))

    fig, ax = performance.plot_race_launch_ratings(2024, "Italian Grand Prix", "Q")
    fig.canvas.draw()

    assert len(ax.patches) == n_drivers
    # Labels must still fit inside the x-limit at any grid size.
    xlim_right = ax.get_xlim()[1]
    for label in ax.texts:
        extent = label.get_window_extent()
        data_x = ax.transData.inverted().transform((extent.x1, extent.y0))[0]
        assert data_x <= xlim_right, f"label {label.get_text()!r} overflows x-limit"
    # Bottom margin stays compact regardless of grid size.
    bounds = ax.get_position().bounds
    assert bounds[1] < 0.08
    # Title still clears the axes top.
    title_bottom = fig._suptitle.get_window_extent().y0
    axes_top = ax.transAxes.transform((0, 1))[1]
    assert title_bottom >= axes_top - 1e-6
    plt.close("all")


def test_race_launch_ratings_exports_full_canvas(launch_session, tmp_path, monkeypatch):
    """The launch chart mirrors the v2 script: no tight_layout, no bbox crop.

    This is required for pixel parity with Race_Launch_Performance_Ratings.py,
    which saves the full 20x20in canvas with its own subplots_adjust margins.
    """
    captured: dict[str, object] = {}
    original = plt.Figure.savefig

    def _capture_save(self, fname, **kwargs):
        captured["kwargs"] = kwargs
        return original(self, fname, **kwargs)

    def _fail_tight_layout(self):
        raise AssertionError("tight_layout must not run for the launch chart")

    monkeypatch.setattr(plt.Figure, "savefig", _capture_save)
    monkeypatch.setattr(plt.Figure, "tight_layout", _fail_tight_layout)

    performance.plot_race_launch_ratings(
        2024, "Italian Grand Prix", "Q", save_path=str(tmp_path / "launch.png"), dpi=150
    )
    assert captured["kwargs"]["dpi"] == 150
    assert "bbox_inches" not in captured["kwargs"]
    assert "facecolor" not in captured["kwargs"]


def test_add_style_branding_footer_not_clipped_full_canvas():
    """Full-canvas branding must use figure-fraction coordinates.

    ``fig.text`` does not fall back to ``fig.transFigure`` when passed
    ``transform=None`` explicitly - the text is left in raw pixel
    coordinates and lands clipped at the bottom-left corner of full-canvas
    exports (regression: the launch chart's footer rendered off-canvas).
    The helper must omit the transform kwarg on the full-canvas path so the
    footer sits centred at the style's ``footer_y`` figure fraction.
    """
    from tif1.plotting import get_plot_style

    style = get_plot_style("default-dark")
    fig = plt.figure(figsize=(20, 20))

    _common.add_style_branding(fig, style)  # no ax -> full-canvas path

    footers = [t for t in fig.texts if t.get_text() == style["footer"]]
    assert footers, "footer text missing"
    footer = footers[0]
    # Must resolve through the figure transform, not raw pixel coordinates.
    assert footer.get_transform() == fig.transFigure
    assert footer.get_position() == (0.5, style["spacing"]["footer_y"])

    # The footer must sit fully inside the canvas (x/y >= 0), not clipped.
    fig.canvas.draw()
    extent = footer.get_window_extent()
    assert extent.x0 >= 0, f"footer clipped at left edge (x0={extent.x0:.1f})"
    assert extent.x1 <= fig.bbox.width, "footer clipped at right edge"
    assert extent.y0 >= 0, f"footer clipped at bottom edge (y0={extent.y0:.1f})"
    assert extent.y1 <= fig.bbox.height, "footer clipped at top edge"
    plt.close("all")


def test_race_launch_ratings_rounding_matches_v2_pipeline(launch_session):
    """Ratings are computed from crossing times rounded to 3 decimals first.

    The v2 script rounds each Time_XX column (``.round(3)``) before the 0-10
    normalization; the native chart must do the same for identical ratings.
    """
    raw = [400.0 / (300.0 * (1 + i * 0.02)) for i in range(len(DRIVERS))]
    rounded = [round(t, 3) for t in raw]
    fastest, slowest = min(rounded), max(rounded)
    expected = {
        abbr: round(10 - (t - fastest) / (slowest - fastest) * 10, 2)
        for abbr, t in zip([d[0] for d in DRIVERS], rounded)
    }

    _, ax = performance.plot_race_launch_ratings(2024, "Italian Grand Prix", "Q")
    bars = dict(
        zip(
            [t.get_text().split(". ")[1].strip() for t in ax.get_yticklabels()],
            [round(p.get_width(), 2) for p in ax.patches],
        )
    )
    for driver, rating in expected.items():
        assert bars[driver] == rating


def test_race_launch_ratings_tie_order_matches_v2_pipeline(launch_session):
    """Identical ratings are ordered like the original (groupby + quicksort).

    The v2 script sorts via ``groupby("Driver").first()`` (driver-alphabetical)
    followed by pandas' default unstable ``sort_values`` (quicksort). Replicating
    that keeps exact-tie ordering pixel-identical with the original output.
    Note: this pins pandas' default quicksort tie behavior; if pandas changes
    its default sort kind, the tie order will change together with the script.
    """
    session = launch_session
    driver_order = [abbr for abbr, _team, _dn in DRIVERS]
    # Give SAI and HAM identical ramps -> identical rating -> a tie.
    session.telemetry_for = lambda driver, lap: _make_launch_telemetry(  # noqa: ARG005
        4 if driver in ("HAM", "SAI") else driver_order.index(driver)
    )

    _, ax = performance.plot_race_launch_ratings(2024, "Italian Grand Prix", "Q")
    rendered_order = [label.get_text().split(". ")[1].strip() for label in ax.get_yticklabels()]

    # Expected order from the original pipeline applied to the same ratings.
    crossing = {
        abbr: round(400.0 / (300.0 * (1 + i * 0.02)), 3)
        for abbr, i in zip(driver_order, range(len(driver_order)))
    }
    crossing["SAI"] = crossing["HAM"]  # identical ramp -> identical rating
    times = list(crossing.values())
    fastest, slowest = min(times), max(times)
    ratings = {
        abbr: round(10 - (t - fastest) / (slowest - fastest) * 10, 2)
        for abbr, t in crossing.items()
    }
    frame = pd.DataFrame({"Driver": list(ratings), "Rating": list(ratings.values())})
    expected_order = list(
        frame.groupby("Driver")
        .first()
        .reset_index()
        .sort_values(by="Rating", ascending=False)["Driver"]
    )
    assert rendered_order == expected_order


# ---------------------------------------------------------------------------
# Top-level lazy exports
# ---------------------------------------------------------------------------


def test_top_level_exports_resolve():
    import tif1
    from tif1 import charts as charts_module

    assert tif1.charts is charts_module
    for name in charts_module.__all__:
        assert getattr(tif1, name) is getattr(charts_module, name)


def test_all_function_names_are_exposed():
    import tif1

    for name in tif1.charts.__all__:
        assert name in tif1.__all__
        assert name in tif1._LAZY_EXPORTS
