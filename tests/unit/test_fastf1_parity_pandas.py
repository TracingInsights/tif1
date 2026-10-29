"""fastf1 parity of the pandas models and ``Session`` surfaces, on in-memory payloads.

Covers the keyword and property additions of the fastf1-parity work: per-lap
weather, qualifying splits, ``pick_*`` keywords, telemetry ``frequency``,
``drop_unknown_channels``, ``merge_channels``/``resample_channels``,
``drop_existing``, ``add_track_status`` on the session's table,
``LazyTelemetryDict`` laziness, ``get_driver`` as fastf1's ``DriverResult``,
``livedata``, ``f1_api_support``/``api_path`` and the telemetry session clock.
Every payload is served from memory: nothing reaches the network.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, ClassVar

import numpy as np
import pandas as pd
import pytest

import tif1
from tif1 import core
from tif1.config import get_config
from tif1.core_utils.constants import FASTF1_RESULTS_COLUMNS, TIF1_RESULTS_EXTRA_COLUMNS
from tif1.exceptions import DriverNotFoundError
from tif1.models import (
    Driver,
    DriverResult,
    Lap,
    Laps,
    LazyTelemetryDict,
    Telemetry,
    _weather_rows_for_laps,
)
from tif1.payload_loader import InMemoryTransport

# 2025 Bahrain Grand Prix: the race is scheduled at 15:00 UTC (vendored schedule).
T0 = pd.Timestamp("2025-04-13 14:55:00")
LAP_TIME = 95.0
LAP_STARTS = {"VER": [300.0, 395.0, 490.0], "HAM": [300.5, 396.0, 491.5]}
DRIVERS = [
    {"driver": "VER", "dn": "1", "team": "Red Bull Racing", "fn": "Max", "ln": "Verstappen"},
    {"driver": "HAM", "dn": "44", "team": "Ferrari", "fn": "Lewis", "ln": "Hamilton"},
]
#: Telemetry samples per lap (0.25 s apart).
TEL_SAMPLES = 9
#: Session time of the safety car in the race-control payload.
SC_DEPLOYED_S = 396.0

#: fastf1's telemetry column order (``fastf1.core.Telemetry`` after a merge).
FASTF1_TELEMETRY_ORDER = [
    "Date",
    "SessionTime",
    "DriverAhead",
    "DistanceToDriverAhead",
    "Time",
    "RPM",
    "Speed",
    "nGear",
    "Throttle",
    "Brake",
    "DRS",
    "Source",
    "Distance",
    "RelativeDistance",
    "Status",
    "X",
    "Y",
    "Z",
]
WEATHER_COLUMNS = [
    "Time",
    "AirTemp",
    "Humidity",
    "Pressure",
    "Rainfall",
    "TrackTemp",
    "WindDirection",
    "WindSpeed",
]


def _td(seconds: list[float]) -> pd.Series:
    """Seconds as a ``timedelta64[ns]`` series (the resolution session frames carry).

    pandas 3 infers ``timedelta64[s]`` for ``pd.to_timedelta(floats, unit="s")``;
    the tests of the selection logic use the session's resolution, and the
    resolution tests below use pandas' inferred one on purpose.
    """
    return pd.Series(pd.to_timedelta(seconds, unit="s")).astype("timedelta64[ns]")


# -- in-memory CDN ------------------------------------------------------------------


def _iso(seconds: float) -> str:
    return (T0 + pd.Timedelta(seconds=seconds)).isoformat()


def _laptimes_payload(code: str, number: str, team: str, codes: list[str]) -> dict[str, Any]:
    """A ``{DRIVER}/laptimes.json`` payload of three laps starting at ``LAP_STARTS[code]``.

    Args:
        code: Driver abbreviation.
        number: Car number.
        team: Team name.
        codes: The laps' ``TrackStatus`` strings.

    Returns:
        The raw payload (TracingInsights keys).
    """
    starts = LAP_STARTS[code]
    n = len(starts)
    ends = [start + LAP_TIME for start in starts]
    return {
        "time": [LAP_TIME] * n,
        "lap": list(range(1, n + 1)),
        "compound": ["SOFT"] * n,
        "stint": [1] * n,
        "s1": [30.0] * n,
        "s2": [30.0] * n,
        "s3": [LAP_TIME - 60.0] * n,
        "life": list(range(1, n + 1)),
        "pos": [1.0 if code == "VER" else 2.0] * n,
        "status": codes,
        "pb": [False] * n,
        "sesT": ends,
        "drv": [code] * n,
        "dNum": [number] * n,
        "pout": [None] * n,
        "pin": [None] * n,
        "s1T": [start + 30.0 for start in starts],
        "s2T": [start + 60.0 for start in starts],
        "s3T": ends,
        "vi1": [300.0] * n,
        "vi2": [300.0] * n,
        "vfl": [300.0] * n,
        "vst": [300.0] * n,
        "fresh": [True] * n,
        "team": [team] * n,
        "lST": starts,
        "lSD": [_iso(start) for start in starts],
        "del": [False] * n,
        "delR": [None] * n,
        "ff1G": [False] * n,
        "iacc": [True] * n,
        "wT": [start + 10.0 for start in starts],
        "wAT": [27.0 + i for i in range(n)],
        "wH": [45.0] * n,
        "wP": [1017.0] * n,
        "wR": [False] * n,
        "wTT": [35.0] * n,
        "wWD": [200] * n,
        "wWS": [1.0] * n,
    }


def _tel_payload(ahead: str) -> dict[str, Any]:
    """A ``{DRIVER}/{lap}_tel.json`` payload: ``TEL_SAMPLES`` samples 0.25 s apart."""
    n = TEL_SAMPLES
    return {
        "tel": {
            "time": [i * 0.25 for i in range(n)],
            "rpm": [10000.0 + 100 * i for i in range(n)],
            "speed": [200.0 + i for i in range(n)],
            "gear": [7] * n,
            "throttle": [100.0] * n,
            "brake": [False] * n,
            "drs": [0] * n,
            "distance": [i * 14.0 for i in range(n)],
            "rel_distance": [i / (n - 1) for i in range(n)],
            "driver_ahead": [ahead] * n,
            "distance_to_driver_ahead": [12.0] * n,
            "x": [float(i) for i in range(n)],
            "y": [0.0] * n,
            "z": [0.0] * n,
        }
    }


def _rcm_payload() -> dict[str, Any]:
    """Race control: pit exit, a safety car (``SC_DEPLOYED_S``), track clear, chequered flag."""
    rows = [
        (60.0, "Flag", "GREEN LIGHT - PIT EXIT OPEN", None, "GREEN", "Track"),
        (SC_DEPLOYED_S, "SafetyCar", "SAFETY CAR DEPLOYED", "DEPLOYED", None, None),
        (470.0, "SafetyCar", "SAFETY CAR IN THIS LAP", "IN THIS LAP", None, None),
        (480.0, "Flag", "TRACK CLEAR", None, "CLEAR", "Track"),
        (600.0, "Flag", "CHEQUERED FLAG", None, "CHEQUERED", "Track"),
    ]
    return {
        "time": [_iso(row[0]) for row in rows],
        "cat": [row[1] for row in rows],
        "msg": [row[2] for row in rows],
        "status": [row[3] or "None" for row in rows],
        "flag": [row[4] or "None" for row in rows],
        "scope": [row[5] or "None" for row in rows],
        "sector": ["None"] * len(rows),
        "dNum": ["None"] * len(rows),
        "lap": [1, 2, 2, 3, 3],
    }


def _weather_payload() -> dict[str, Any]:
    times = [250.0, 310.0, 405.0, 500.0]
    return {
        "wT": times,
        "wAT": [26.0, 27.0, 28.0, 29.0],
        "wH": [40.0] * 4,
        "wP": [1010.0] * 4,
        "wR": [False] * 4,
        "wTT": [30.0] * 4,
        "wWD": [100] * 4,
        "wWS": [1.0] * 4,
    }


def _session_payloads() -> dict[str, Any]:
    payloads: dict[str, Any] = {
        "drivers.json": {"drivers": [dict(d) for d in DRIVERS]},
        "rcm.json": _rcm_payload(),
        "weather.json": _weather_payload(),
        "VER/laptimes.json": _laptimes_payload("VER", "1", "Red Bull Racing", ["1", "14", "1"]),
        "HAM/laptimes.json": _laptimes_payload("HAM", "44", "Ferrari", ["1", "14", "1"]),
    }
    for code, ahead in (("VER", "44"), ("HAM", "1")):
        for lap in (1, 2, 3):
            payloads[f"{code}/{lap}_tel.json"] = _tel_payload(ahead)
    return payloads


@dataclass
class FakeCdn:
    """In-memory CDN for both fetch seams (sync transport and async batch fetch)."""

    payloads: dict[str, Any]
    transport: InMemoryTransport
    batch_paths: list[str] = field(default_factory=list)

    def requested(self) -> list[str]:
        """Every path requested so far through either seam."""
        return [*self.batch_paths, *self.transport.calls]

    def requested_matching(self, fragment: str) -> list[str]:
        """Requested paths containing ``fragment``."""
        return [path for path in self.requested() if fragment in path]

    def session(self, session_name: str = "Race") -> core.Session:
        """A 2025 Bahrain session of ``session_name`` served by this CDN."""
        session = core.Session(
            2025, "Bahrain Grand Prix", session_name, enable_cache=False, lib="pandas"
        )
        session._payload_loader.transport = self.transport
        return session


@pytest.fixture
def cdn(monkeypatch) -> FakeCdn:
    """Serve every session payload from memory and record what is requested."""
    payloads = _session_payloads()
    fake = FakeCdn(payloads=payloads, transport=InMemoryTransport(payloads))

    async def fetch_multiple(requests, **_kwargs):
        fake.batch_paths.extend(request[3] for request in requests)
        return [payloads.get(request[3]) for request in requests]

    monkeypatch.setattr(core, "fetch_multiple_async", fetch_multiple)
    # No background all-laps telemetry thread: it would race the request
    # assertions and outlive the patched fetch.
    monkeypatch.setitem(get_config()._config, "prefetch_all_telemetry_after_laps_load", False)
    return fake


@pytest.fixture
def race(cdn) -> core.Session:
    return cdn.session("Race")


# -- telemetry: session clock and column order ---------------------------------------


class TestTelemetrySessionClock:
    def test_lap_frame_has_fastf1s_column_order(self, race):
        tel = race.laps.pick_drivers("VER").pick_laps(2).iloc[0].telemetry
        assert list(tel.columns[:2]) == ["Date", "SessionTime"]
        fastf1_part = [col for col in tel.columns if col in FASTF1_TELEMETRY_ORDER]
        assert fastf1_part == FASTF1_TELEMETRY_ORDER
        extras = [col for col in tel.columns if col not in FASTF1_TELEMETRY_ORDER]
        assert extras == ["Driver", "LapNumber"]

    def test_source_and_status_are_constant_channels(self, race):
        tel = race.laps.pick_drivers("VER").pick_laps(1).iloc[0].telemetry
        assert tel["Source"].eq("merged").all()
        assert tel["Status"].eq("OnTrack").all()

    def test_session_time_and_date_are_lap_start_plus_time(self, race):
        lap = race.laps.pick_drivers("HAM").pick_laps(3).iloc[0]
        tel = lap.telemetry
        assert tel["SessionTime"].dtype == "timedelta64[ns]"
        assert tel["Date"].dtype == "datetime64[ns]"
        assert (tel["SessionTime"] - tel["Time"]).eq(lap["LapStartTime"]).all()
        assert (tel["Date"] - tel["Time"]).eq(lap["LapStartDate"]).all()

    def test_multi_lap_frames_use_each_laps_start(self, race):
        laps = race.laps.pick_drivers("VER")
        tel = laps.telemetry
        starts = laps.set_index("LapNumber")["LapStartTime"]
        lap_start = tel["LapNumber"].map(lambda n: starts[float(n)]).astype("timedelta64[ns]")
        # Each sample sits on its own lap's clock ...
        assert (tel["SessionTime"] >= lap_start).all()
        # ... and Time runs on from the first lap's start (fastf1's slice_by_lap).
        first_start = laps["LapStartTime"].min()
        assert (tel["SessionTime"] - tel["Time"]).eq(first_start).all()
        assert tel["Time"].is_monotonic_increasing


# -- car_data / pos_data ------------------------------------------------------------------


class TestCarData:
    def test_is_a_lazy_dict_shared_with_pos_data(self, race):
        assert isinstance(race.car_data, LazyTelemetryDict)
        assert race.pos_data is race.car_data

    def test_len_iteration_and_membership_fetch_no_telemetry(self, race, cdn):
        car_data = race.car_data
        assert len(car_data) == 2
        assert list(car_data) == ["1", "44"]
        assert list(car_data.keys()) == ["1", "44"]
        assert "1" in car_data
        assert "VER" in car_data
        assert "XYZ" not in car_data
        assert "LazyTelemetryDict" in repr(car_data)
        assert cdn.requested_matching("_tel.json") == []
        assert cdn.requested_matching("laptimes") == []

    def test_reading_a_key_fetches_that_drivers_laps(self, race, cdn):
        tel = race.car_data["1"]
        assert isinstance(tel, Telemetry)
        assert len(tel) == 3 * TEL_SAMPLES
        fetched = {
            path.rsplit("/", 2)[-2] + "/" + path.rsplit("/", 1)[-1]
            for path in cdn.requested_matching("_tel.json")
        }
        assert {f"VER/{lap}_tel.json" for lap in (1, 2, 3)} <= fetched

    def test_numbers_and_abbreviations_share_the_memoised_frame(self, race, cdn):
        by_number = race.car_data["1"]
        before = len(cdn.requested())
        assert race.car_data["VER"] is by_number
        assert race.car_data[1] is by_number
        assert len(cdn.requested()) == before

    def test_session_time_is_monotonic_over_the_session(self, race):
        tel = race.car_data["44"]
        assert tel["SessionTime"].is_monotonic_increasing
        assert tel["SessionTime"].iloc[0] == pd.Timedelta(seconds=LAP_STARTS["HAM"][0])

    def test_unknown_driver_raises_key_error_and_get_defaults(self, race):
        with pytest.raises(KeyError):
            race.car_data["XYZ"]
        assert race.car_data.get("XYZ") is None
        assert race.car_data.get("XYZ", "missing") == "missing"


class _StubDriverLaps:
    def __init__(self, provider: _StubProvider, code: str):
        self._provider = provider
        self._code = code

    @property
    def telemetry(self) -> Telemetry:
        self._provider.telemetry_builds.append(self._code)
        return Telemetry({"Time": pd.to_timedelta([0.0, 0.5], unit="s"), "Speed": [1.0, 2.0]})


class _StubProvider:
    """The two members of the session seam ``LazyTelemetryDict`` reads."""

    def __init__(self) -> None:
        self.laps_reads = 0
        self.telemetry_builds: list[str] = []
        self._drivers_data = [
            {"driver": "VER", "dn": "1"},
            {"driver": "HAM", "dn": "44"},
            {"driver": "BOR", "dn": "5"},
        ]

    @property
    def laps(self) -> SimpleNamespace:
        self.laps_reads += 1
        return SimpleNamespace(pick_driver=lambda code: _StubDriverLaps(self, code))


class TestLazyTelemetryDict:
    def test_len_iteration_and_membership_never_read_laps(self):
        provider = _StubProvider()
        lazy = LazyTelemetryDict(provider)  # type: ignore[ty:invalid-argument-type]
        assert len(lazy) == 3
        assert list(lazy) == ["1", "44", "5"]
        assert "HAM" in lazy
        assert "44" in lazy
        assert "ALO" not in lazy
        _ = repr(lazy)
        assert provider.laps_reads == 0
        assert provider.telemetry_builds == []

    def test_value_is_built_once_per_driver(self):
        provider = _StubProvider()
        lazy = LazyTelemetryDict(provider)  # type: ignore[ty:invalid-argument-type]
        first = lazy["HAM"]
        assert lazy["44"] is first
        assert lazy[44] is first
        assert provider.telemetry_builds == ["HAM"]

    def test_shared_car_label_resolves_to_the_driver(self):
        provider = _StubProvider()
        lazy = LazyTelemetryDict(provider)  # type: ignore[ty:invalid-argument-type]
        assert "5/BOR" in lazy
        assert lazy["5/BOR"] is lazy["5"]
        assert provider.telemetry_builds == ["BOR"]

    def test_items_and_values_build_every_driver(self):
        provider = _StubProvider()
        lazy = LazyTelemetryDict(provider)  # type: ignore[ty:invalid-argument-type]
        pairs = lazy.items()
        assert [pair[0] for pair in pairs] == ["1", "44", "5"]
        assert len(lazy.values()) == 3
        assert sorted(provider.telemetry_builds) == ["BOR", "HAM", "VER"]


# -- get_telemetry(frequency=) ------------------------------------------------------------


class TestTelemetryFrequency:
    def test_default_is_telemetry_frequency_original(self, race):
        lap = race.laps.pick_drivers("VER").pick_laps(1).iloc[0]
        assert Telemetry.TELEMETRY_FREQUENCY == "original"
        assert len(lap.get_telemetry()) == TEL_SAMPLES
        assert len(lap.get_telemetry(frequency="original")) == TEL_SAMPLES

    def test_get_telemetry_adds_driver_ahead(self, race):
        tel = race.laps.pick_drivers("VER").pick_laps(1).iloc[0].get_telemetry()
        assert {"DriverAhead", "DistanceToDriverAhead"} <= set(tel.columns)

    def test_lap_frequency_in_hz_resamples(self, race):
        lap = race.laps.pick_drivers("VER").pick_laps(2).iloc[0]
        tel = lap.get_telemetry(frequency=10)
        steps = tel["Time"].diff().dropna().unique()
        assert list(steps) == [pd.Timedelta(milliseconds=100)]
        assert (tel["SessionTime"] - tel["Time"]).eq(lap["LapStartTime"]).all()

    def test_none_follows_the_class_frequency(self, race, monkeypatch):
        monkeypatch.setattr(Telemetry, "TELEMETRY_FREQUENCY", 2)
        lap = race.laps.pick_drivers("VER").pick_laps(1).iloc[0]
        tel = lap.get_telemetry()
        assert tel["Time"].diff().dropna().eq(pd.Timedelta(milliseconds=500)).all()

    def test_laps_frequency_keeps_every_lap(self, race):
        """A multi-lap resample covers every lap (fastf1 resamples the session timeline)."""
        laps = race.laps.pick_drivers("VER")
        tel = laps.get_telemetry(frequency=10)
        assert set(tel["LapNumber"].dropna().astype(int)) == {1, 2, 3}
        assert tel["SessionTime"].min() == pd.Timedelta(seconds=LAP_STARTS["VER"][0])
        assert tel["SessionTime"].max() >= pd.Timedelta(seconds=LAP_STARTS["VER"][2])
        assert tel["SessionTime"].is_monotonic_increasing

    @pytest.mark.parametrize("frequency", [0, -5, "fast", True])
    def test_invalid_frequency_raises(self, race, frequency):
        lap = race.laps.pick_drivers("VER").pick_laps(1).iloc[0]
        with pytest.raises(ValueError, match="frequency"):
            lap.get_telemetry(frequency=frequency)


# -- Telemetry keywords ---------------------------------------------------------------------


def _clocked_telemetry(session: Any = None) -> Telemetry:
    """Six samples 0.25 s apart, lap starting 300 s into the session."""
    offsets = pd.to_timedelta([0.0, 0.25, 0.5, 0.75, 1.0, 1.25], unit="s")
    lap_start = pd.Timedelta(seconds=300)
    return Telemetry(
        {
            "Date": T0 + lap_start + offsets,
            "SessionTime": lap_start + offsets,
            "Time": offsets,
            "Speed": [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            "nGear": pd.array([3, 4, 5, 6, 7, 8], dtype="Int64"),
            "DRS": pd.array([0, 0, 1, 1, 1, 0], dtype="Int64"),
            "Brake": [False, True, False, False, True, True],
            "Source": ["merged"] * 6,
            "Status": ["OnTrack"] * 6,
            "Driver": ["VER"] * 6,
            "LapNumber": pd.array([1] * 6, dtype="Int64"),
        },
        session=session,
    )


class TestDropUnknownChannels:
    FRAME: ClassVar[dict[str, Any]] = {
        "Time": pd.to_timedelta([0.0, 0.5], unit="s"),
        "Speed": [1.0, 2.0],
        "Mystery": [9, 9],
        "Driver": ["VER", "VER"],
        "LapNumber": [1, 1],
    }

    def test_default_keeps_every_column(self):
        assert "Mystery" in Telemetry(self.FRAME).columns

    def test_drops_unregistered_columns_but_keeps_metadata(self):
        tel = Telemetry(self.FRAME, drop_unknown_channels=True)
        assert list(tel.columns) == ["Time", "Speed", "Driver", "LapNumber"]

    def test_registered_channel_is_kept(self, monkeypatch):
        monkeypatch.setattr(Telemetry, "_CHANNELS", dict(Telemetry._CHANNELS))
        Telemetry.register_new_channel("Mystery", "discrete")
        assert "Mystery" in Telemetry(self.FRAME, drop_unknown_channels=True).columns


class TestMergeChannels:
    @staticmethod
    def _frames() -> tuple[Telemetry, pd.DataFrame]:
        left = Telemetry(
            {
                "Time": pd.to_timedelta([0.0, 0.5, 1.0, 1.5, 2.0], unit="s"),
                "Speed": [100.0, 110.0, 120.0, 130.0, 140.0],
            }
        )
        right = pd.DataFrame(
            {"Time": pd.to_timedelta([0.1, 0.6, 1.1, 1.6], unit="s"), "RPM": [1.0, 2.0, 3.0, 4.0]}
        )
        return left, right

    def test_default_keeps_the_merged_samples(self):
        left, right = self._frames()
        merged = left.merge_channels(right)
        assert len(merged) == 5
        assert merged["RPM"].tolist() == [1.0, 2.0, 3.0, 4.0, 4.0]
        assert merged.equals(left.merge_channels(right, frequency="original"))

    def test_frequency_resamples_after_the_merge(self):
        left, right = self._frames()
        merged = left.merge_channels(right, frequency=4)
        assert merged["Time"].diff().dropna().eq(pd.Timedelta(milliseconds=250)).all()
        assert {"Speed", "RPM"} <= set(merged.columns)


class TestResampleChannels:
    def test_rule_and_new_date_ref_are_exclusive(self):
        tel = _clocked_telemetry()
        with pytest.raises(ValueError, match="only specify one"):
            tel.resample_channels(rule="100ms", new_date_ref=tel["Date"])

    def test_continuous_channels_are_averaged_per_bin(self):
        out = _clocked_telemetry().resample_channels(rule="500ms")
        assert out["Time"].tolist() == list(pd.to_timedelta([0.0, 0.5, 1.0], unit="s"))
        assert out["Speed"].tolist() == [150.0, 350.0, 550.0]

    def test_discrete_channels_hold_the_value_in_force(self):
        out = _clocked_telemetry().resample_channels(rule="500ms")
        assert out["nGear"].tolist() == [3, 5, 7]
        assert out["DRS"].tolist() == [0, 1, 1]
        assert out["Brake"].tolist() == [False, False, True]
        assert out["nGear"].dtype == "Int64"

    def test_clock_columns_follow_the_new_axis(self):
        tel = _clocked_telemetry()
        out = tel.resample_channels(rule="500ms")
        assert (out["SessionTime"] - out["Time"]).eq(pd.Timedelta(seconds=300)).all()
        assert (out["Date"] - out["Time"]).eq(tel["Date"].iloc[0]).all()

    def test_constant_text_columns_and_order_survive(self):
        tel = _clocked_telemetry()
        out = tel.resample_channels(rule="500ms")
        assert list(out.columns) == list(tel.columns)
        assert out["Driver"].eq("VER").all()
        assert out["Source"].eq("merged").all()

    def test_no_rule_means_one_second(self):
        out = _clocked_telemetry().resample_channels()
        assert out["Time"].tolist() == [pd.Timedelta(0), pd.Timedelta(seconds=1)]
        assert out["Speed"].tolist() == [250.0, 550.0]

    def test_new_date_ref_resamples_onto_the_given_dates(self):
        tel = _clocked_telemetry()
        targets = pd.Series(tel["Date"].iloc[0] + pd.to_timedelta([0.1, 0.6, 1.1], unit="s"))
        out = tel.resample_channels(new_date_ref=targets)
        assert out["Date"].tolist() == targets.tolist()
        assert out["Speed"].round(6).tolist() == [140.0, 340.0, 540.0]
        assert out["nGear"].tolist() == [3, 5, 7]
        assert out["Time"].tolist() == list(pd.to_timedelta([0.1, 0.6, 1.1], unit="s"))
        assert (out["SessionTime"] - out["Time"]).eq(pd.Timedelta(seconds=300)).all()

    def test_new_date_ref_needs_a_date_column(self):
        tel = Telemetry({"Time": pd.to_timedelta([0.0, 1.0], unit="s"), "Speed": [1.0, 2.0]})
        with pytest.raises(ValueError, match="Date"):
            tel.resample_channels(new_date_ref=pd.Series([T0]))


class TestDropExisting:
    def test_differential_distance_kept_when_not_dropping(self):
        tel = _clocked_telemetry()
        tel["DifferentialDistance"] = -1.0
        assert tel.add_differential_distance(drop_existing=False) is tel

    def test_differential_distance_recomputed_by_default(self):
        tel = _clocked_telemetry()
        tel["DifferentialDistance"] = -1.0
        out = tel.add_differential_distance()
        expected = (tel["Speed"] / 3.6 * tel["Time"].dt.total_seconds().diff().fillna(0.0)).round(9)
        assert out["DifferentialDistance"].round(9).tolist() == expected.tolist()

    def test_track_status_kept_when_not_dropping(self):
        tel = _clocked_telemetry()
        tel["TrackStatus"] = "9"
        assert tel.add_track_status(drop_existing=False) is tel

    def test_track_status_recomputed_by_default(self):
        tel = _clocked_telemetry()
        tel["TrackStatus"] = "9"
        assert tel.add_track_status()["TrackStatus"].eq("1").all()


class TestAddTrackStatus:
    TABLE = pd.DataFrame(
        {
            "Time": _td([0.0, 300.5, 301.0]),
            "Status": ["1", "4", "1"],
            "Message": ["AllClear", "SCDeployed", "AllClear"],
        }
    )

    def test_maps_session_time_onto_the_session_table(self):
        tel = _clocked_telemetry(SimpleNamespace(track_status=self.TABLE))
        # A sample exactly on a change takes the new status (fastf1's searchsorted).
        assert tel.add_track_status()["TrackStatus"].tolist() == ["1", "1", "4", "4", "1", "1"]

    def test_falls_back_to_time_without_a_session_clock(self):
        table = self.TABLE.assign(Time=_td([0.0, 0.5, 1.0]))
        tel = _clocked_telemetry(SimpleNamespace(track_status=table)).drop(
            columns=["SessionTime", "Date"]
        )
        tel.session = SimpleNamespace(track_status=table)
        assert tel.add_track_status()["TrackStatus"].tolist() == ["1", "1", "4", "4", "1", "1"]

    def test_all_clear_without_a_session(self):
        assert _clocked_telemetry().add_track_status()["TrackStatus"].eq("1").all()

    def test_second_resolution_session_time_maps_like_nanoseconds(self):
        """A hand-built frame in ``timedelta64[s]`` maps onto the (ns) session table alike."""
        table = self.TABLE.assign(Time=_td([0.0, 300.0, 302.0]))
        tel = Telemetry(
            {
                "SessionTime": pd.to_timedelta([299.0, 300.0, 301.0, 302.0], unit="s"),
                "Time": pd.to_timedelta([0.0, 1.0, 2.0, 3.0], unit="s"),
            },
            session=SimpleNamespace(track_status=table),
        )
        assert tel["SessionTime"].dtype == "timedelta64[s]"
        assert tel.add_track_status()["TrackStatus"].tolist() == ["1", "4", "4", "1"]

    def test_on_a_session_uses_its_derived_track_status(self, race):
        lap = race.laps.pick_drivers("VER").pick_laps(2).iloc[0]  # starts at 395 s
        tel = lap.telemetry.add_track_status()
        expected = np.where(tel["SessionTime"] >= pd.Timedelta(seconds=SC_DEPLOYED_S), "4", "1")
        assert tel["TrackStatus"].tolist() == expected.tolist()
        assert "4" in set(tel["TrackStatus"])


# -- Laps: weather, qualifying split, pick_* -----------------------------------------------


class TestLapWeather:
    def test_laps_weather_follows_the_session_table_aligned_with_the_index(self, race):
        # HAM's laps start at 300.5/396/491.5 s; samples at 250/310/405/500 s:
        # the first sample within each lap (fastf1's rule).
        laps = race.laps.pick_drivers("HAM")
        weather = laps.get_weather_data()
        assert list(weather.columns) == WEATHER_COLUMNS
        assert weather.index.equals(laps.index)
        assert weather["Time"].tolist() == _td([310.0, 405.0, 500.0]).tolist()
        assert weather["AirTemp"].tolist() == [27.0, 28.0, 29.0]

    def test_lap_weather_is_a_series_named_after_the_lap(self, race):
        laps = race.laps
        lap = laps.iloc[4]
        row = lap.get_weather_data()
        assert isinstance(row, pd.Series)
        assert list(row.index) == WEATHER_COLUMNS
        assert row.name == lap.name
        assert row["Time"] == laps.get_weather_data().loc[lap.name, "Time"]

    def test_session_weather_table_stays_whole(self, race):
        assert len(race.weather_data) == 4


def _weather_table() -> pd.DataFrame:
    times = [90.0, 120.0, 150.0, 400.0]
    return pd.DataFrame(
        {
            "Time": _td(times),
            "AirTemp": [20.0, 21.0, 22.0, 23.0],
            "Humidity": [50.0] * 4,
            "Pressure": [1000.0] * 4,
            "Rainfall": [False] * 4,
            "TrackTemp": [30.0] * 4,
            "WindDirection": [90] * 4,
            "WindSpeed": [2.0] * 4,
        }
    )


def _bare_laps(*, resolution: str = "ns") -> Laps:
    """Laps without the weather columns, bound to a session with a weather table.

    Lap 10: a sample within the lap (the first one wins); lap 20: none within,
    the last before its end; lap 30: no sample before its end; lap 40: no start
    (the last sample before its end, fastf1); lap 50: no ``Time`` (no sample,
    fastf1).

    Args:
        resolution: Unit of the laps' timedelta columns (the weather table is ns).
    """
    dtype = f"timedelta64[{resolution}]"
    index = [10, 20, 30, 40, 50]
    return Laps(
        pd.DataFrame(
            {
                "Driver": ["VER"] * 5,
                "LapNumber": [1.0, 2.0, 3.0, 4.0, 5.0],
                "LapStartTime": _td([100.0, 195.0, 0.0, np.nan, 380.0]).astype(dtype).values,
                "Time": _td([195.0, 290.0, 50.0, 410.0, np.nan]).astype(dtype).values,
                "LapTime": _td([95.0, 95.0, 50.0, np.nan, 30.0]).astype(dtype).values,
            },
            index=index,
        ),
        session=SimpleNamespace(weather_data=_weather_table()),
    )


class TestWeatherRule:
    def test_first_sample_within_else_last_before_end(self):
        weather = _bare_laps().get_weather_data()
        assert weather.index.tolist() == [10, 20, 30, 40, 50]
        assert list(weather.columns) == WEATHER_COLUMNS
        air = weather["AirTemp"].tolist()
        assert air[0] == 21.0  # 120 s is the first sample in [100, 195]
        assert air[1] == 22.0  # nothing in [195, 290]: last before 290 is 150 s
        assert np.isnan(air[2])  # nothing before 50 s
        assert air[3] == 23.0  # no start: the last sample before its end (410 s)
        assert np.isnan(air[4])  # no Time: fastf1 finds no sample

    def test_second_resolution_laps_pick_the_same_samples(self):
        """pandas 3 infers ``timedelta64[s]`` for hand-built laps; the rule must not change."""
        weather = _bare_laps(resolution="s").get_weather_data()
        expected = _bare_laps().get_weather_data()
        assert weather["AirTemp"].equals(expected["AirTemp"])
        assert weather["Time"].equals(expected["Time"])

    def test_lap_without_a_sample_is_a_nan_series(self):
        laps = _bare_laps()
        row = laps.loc[30]
        row.session = laps.session
        weather = row.get_weather_data()
        assert list(weather.index) == WEATHER_COLUMNS  # fastf1: NaN over the keys
        assert weather.isna().all()
        assert weather.name == 30

    def test_lap_with_a_sample_falls_back_to_the_session_table(self):
        laps = _bare_laps()
        row = laps.loc[10]
        row.session = laps.session
        weather = row.get_weather_data()
        assert weather["AirTemp"] == 21.0
        assert weather.name == 10

    def test_without_a_session_the_frame_is_empty_and_aligned(self):
        laps = Laps(_bare_laps())
        weather = laps.get_weather_data()
        assert weather.index.tolist() == [10, 20, 30, 40, 50]
        assert weather.isna().all().all()

    def test_positions_helper_with_nat(self):
        nat = np.iinfo("int64").min
        rows = _weather_rows_for_laps(
            np.array([30, 10, 20], dtype="int64"),
            np.array([15, nat, 0, 40], dtype="int64"),
            np.array([25, 12, 5, nat], dtype="int64"),
        )
        # Positions index the unsorted input: 20 is position 2, 10 is position 1.
        assert rows.tolist() == [2, 1, -1, -1]

    def test_positions_helper_without_samples(self):
        empty = np.array([], dtype="int64")
        starts = np.array([0, 1], dtype="int64")
        assert _weather_rows_for_laps(empty, starts, starts).tolist() == [-1, -1]


def _quali_laps(
    lap_ends_s: list[float], *, session: Any = None, resolution: str = "ns", **columns: Any
) -> Laps:
    """90-second qualifying laps ending at ``lap_ends_s`` (timedelta columns in ``resolution``)."""
    dtype = f"timedelta64[{resolution}]"
    n = len(lap_ends_s)
    frame = pd.DataFrame(
        {
            "Driver": ["VER"] * n,
            "LapNumber": [float(i + 1) for i in range(n)],
            "LapStartTime": _td([end - 90.0 for end in lap_ends_s]).astype(dtype),
            "Time": _td(lap_ends_s).astype(dtype),
            "LapTime": _td([90.0] * n).astype(dtype),
            **columns,
        }
    )
    return Laps(frame, session=session)


def _status_table(rows: list[tuple[float, str]]) -> pd.DataFrame:
    return pd.DataFrame({"Time": _td([row[0] for row in rows]), "Status": [row[1] for row in rows]})


class TestSplitQualifyingSessions:
    def test_marker_with_a_missing_segment_returns_none(self):
        laps = _quali_laps([500.0, 900.0, 1600.0], QualifyingSession=["Q1", "Q1", "Q2"])
        q1, q2, q3 = laps.split_qualifying_sessions()
        assert q1 is not None
        assert q1["LapNumber"].tolist() == [1.0, 2.0]
        assert q2 is not None
        assert q2["LapNumber"].tolist() == [3.0]
        assert q3 is None

    def test_segments_stay_bound_to_the_session(self):
        session = SimpleNamespace(session_status=pd.DataFrame())
        laps = _quali_laps([500.0], session=session, QualifyingSession=["Q1"])
        q1, _, _ = laps.split_qualifying_sessions()
        assert isinstance(q1, Laps)
        assert q1.session is session

    def test_started_rows_split_and_an_aborted_restart_continues_the_segment(self):
        status = _status_table(
            [
                (0.0, "Started"),
                (1000.0, "Finished"),
                (1500.0, "Started"),
                (1700.0, "Aborted"),
                (1900.0, "Started"),  # resume after a red flag: still Q2
                (2500.0, "Finished"),
                (3000.0, "Started"),
                (3600.0, "Finished"),
            ]
        )
        laps = _quali_laps(
            [500.0, 900.0, 1600.0, 2000.0, 2400.0, 3200.0],
            session=SimpleNamespace(session_status=status),
        )
        q1, q2, q3 = laps.split_qualifying_sessions()
        assert q1["LapNumber"].tolist() == [1.0, 2.0]
        assert q2["LapNumber"].tolist() == [3.0, 4.0, 5.0]
        assert q3["LapNumber"].tolist() == [6.0]

    def test_session_status_with_one_segment_gives_none_for_the_rest(self):
        status = _status_table([(0.0, "Started"), (3600.0, "Finished")])
        laps = _quali_laps([500.0, 900.0], session=SimpleNamespace(session_status=status))
        q1, q2, q3 = laps.split_qualifying_sessions()
        assert len(q1) == 2
        assert q2 is None
        assert q3 is None

    def test_lap_start_gaps_split_without_marker_or_status(self):
        laps = _quali_laps([200.0, 300.0, 1000.0, 1100.0, 1800.0])
        q1, q2, q3 = laps.split_qualifying_sessions()
        assert q1["LapNumber"].tolist() == [1.0, 2.0]
        assert q2["LapNumber"].tolist() == [3.0, 4.0]
        assert q3["LapNumber"].tolist() == [5.0]

    def test_one_gap_gives_two_segments_and_none(self):
        laps = _quali_laps([200.0, 300.0, 1000.0])
        q1, q2, q3 = laps.split_qualifying_sessions()
        assert len(q1) == 2
        assert len(q2) == 1
        assert q3 is None

    def test_second_resolution_laps_split_like_nanosecond_laps(self):
        """pandas 3 infers ``timedelta64[s]`` for hand-built laps; the split must not change."""
        laps = _quali_laps([200.0, 300.0, 1000.0, 1100.0, 1800.0], resolution="s")
        q1, q2, q3 = laps.split_qualifying_sessions()
        assert q1["LapNumber"].tolist() == [1.0, 2.0]
        assert q2["LapNumber"].tolist() == [3.0, 4.0]
        assert q3["LapNumber"].tolist() == [5.0]

    def test_short_gaps_are_one_segment(self):
        q1, q2, q3 = _quali_laps([200.0, 300.0, 400.0]).split_qualifying_sessions()
        assert len(q1) == 3
        assert q2 is None
        assert q3 is None


def _timed_laps(lap_times_s: list[float], **columns: Any) -> Laps:
    return Laps(
        pd.DataFrame(
            {
                "Driver": ["VER"] * len(lap_times_s),
                "LapNumber": [float(i + 1) for i in range(len(lap_times_s))],
                "LapTime": pd.to_timedelta(lap_times_s, unit="s"),
                **columns,
            }
        )
    )


class TestPickKeywords:
    def test_quicklaps_none_uses_the_class_threshold(self):
        laps = _timed_laps([100.0, 106.0, 108.0, np.nan])
        assert Laps.QUICKLAP_THRESHOLD == 1.07
        assert laps.pick_quicklaps(None)["LapNumber"].tolist() == [1.0, 2.0]
        assert laps.pick_quicklaps()["LapNumber"].tolist() == [1.0, 2.0]

    def test_quicklaps_threshold_follows_the_class_attribute(self, monkeypatch):
        monkeypatch.setattr(Laps, "QUICKLAP_THRESHOLD", 1.10)
        laps = _timed_laps([100.0, 106.0, 108.0])
        assert len(laps.pick_quicklaps()) == 3
        assert laps.pick_quicklaps(1.01)["LapNumber"].tolist() == [1.0]

    @pytest.mark.parametrize(
        ("call", "expected"),
        [
            (lambda laps: laps.pick_laps(2), [2.0]),
            (lambda laps: laps.pick_laps([1, 3]), [1.0, 3.0]),
            (lambda laps: laps.pick_laps(lap_numbers=[1, 3]), [1.0, 3.0]),
            (lambda laps: laps.pick_laps(laps=[1, 3]), [1.0, 3.0]),
            (lambda laps: laps.pick_laps(slice(2, None)), [2.0, 3.0]),
            (lambda laps: laps.pick_laps(slice(None, 2)), [1.0, 2.0]),
        ],
        ids=["int", "list", "lap_numbers", "laps-alias", "open-slice", "slice-to"],
    )
    def test_pick_laps_spellings(self, call, expected):
        laps = _timed_laps([90.0, 91.0, 92.0])
        assert call(laps)["LapNumber"].tolist() == expected

    def test_pick_laps_needs_lap_numbers(self):
        with pytest.raises(TypeError, match="lap_numbers"):
            _timed_laps([90.0]).pick_laps()

    @pytest.mark.parametrize(
        ("status", "how", "expected"),
        [
            ("1", "equals", ["1"]),
            ("4", "contains", ["14", "4"]),
            ("4", "excludes", ["1", "26"]),
            ("26", "any", ["26"]),
            ("26", "none", ["1", "14", "4"]),
        ],
    )
    def test_pick_track_status_reads_the_lap_codes_not_the_session_table(
        self, status, how, expected
    ):
        laps = _timed_laps([90.0] * 4, TrackStatus=["1", "14", "26", "4"])
        # A session whose time-based table says SC everywhere must not matter.
        laps.session = SimpleNamespace(
            track_status=pd.DataFrame(
                {"Time": [pd.Timedelta(0)], "Status": ["4"], "Message": ["SCDeployed"]}
            )
        )
        picked = laps.pick_track_status(status, how=how)
        assert picked["TrackStatus"].tolist() == expected

    def test_pick_track_status_default_is_equals(self):
        laps = _timed_laps([90.0, 91.0], TrackStatus=["1", "12"])
        assert laps.pick_track_status(1)["TrackStatus"].tolist() == ["1"]

    def test_base_class_view_is_a_plain_frame(self):
        view = _timed_laps([90.0]).base_class_view
        assert type(view) is pd.DataFrame


# -- get_driver ----------------------------------------------------------------------------


class TestGetDriverWithoutClassification:
    @pytest.mark.parametrize("session_name", ["Race", "Practice 1"])
    def test_driver_is_a_driver_result_with_fastf1_columns(self, cdn, session_name):
        driver = cdn.session(session_name).get_driver("VER")
        assert isinstance(driver, Driver)
        assert isinstance(driver, DriverResult)
        assert list(driver.index[: len(FASTF1_RESULTS_COLUMNS)]) == list(FASTF1_RESULTS_COLUMNS)
        assert list(driver.index[len(FASTF1_RESULTS_COLUMNS) :]) == list(TIF1_RESULTS_EXTRA_COLUMNS)

    def test_practice_driver_is_its_results_row(self, cdn):
        """The same row whether or not ``results`` was read first (fastf1: always the row)."""
        session = cdn.session("Practice 1")
        driver = session.get_driver("VER")
        assert driver["Abbreviation"] == "VER"
        assert driver["DriverNumber"] == "1"
        assert driver["FullName"] == "Max Verstappen"
        assert driver["TeamName"] == "Red Bull Racing"
        assert driver.name == "1"
        row = session.results.loc["1"]
        for column in ("Position", "ClassifiedPosition", "Status", "Laps", "Time"):
            assert driver[column] == row[column], column
        assert np.isnan(driver["Points"])
        assert np.isnan(driver["GridPosition"])
        laps = session.laps
        ver = laps[(laps["Driver"] == "VER") & ~laps["Deleted"].fillna(False).astype(bool)]
        assert driver["Time"] == ver["LapTime"].min()  # fastf1: a practice Time is the best lap
        assert driver.dnf is False

    def test_practice_get_driver_requests_no_telemetry(self, cdn):
        session = cdn.session("Practice 1")
        session.get_driver("VER")
        # The results row of a practice session is classified from the laps.
        assert session._results is not None
        assert cdn.requested_matching("_tel.json") == []

    @pytest.mark.parametrize("identifier", ["VER", "1", 1, "1/VER"])
    def test_identifiers(self, race, identifier):
        assert race.get_driver(identifier)["Abbreviation"] == "VER"

    def test_full_name_identifier(self, race):
        """The plan's identifier list includes the full name (section 4)."""
        assert race.get_driver("Max Verstappen")["Abbreviation"] == "VER"

    def test_unknown_driver_raises(self, race):
        with pytest.raises(DriverNotFoundError):
            race.get_driver("XYZ")

    def test_tif1_helpers_are_kept(self, race):
        driver = race.get_driver("HAM")
        assert driver.driver == "HAM"
        assert driver.laps["LapNumber"].tolist() == [1.0, 2.0, 3.0]
        assert driver.get_lap(2)["LapNumber"] == 2.0
        assert driver.get_fastest_lap() is not None


def _chinese_gp_drivers() -> dict[str, Any]:
    """``drivers.json`` of the 2025 Chinese GP from the Ergast fixture (number order)."""
    import json
    from pathlib import Path

    path = (
        Path(__file__).parent.parent
        / "fixtures"
        / "ergast"
        / "stats"
        / "2025"
        / "chinese-grand-prix"
        / "results.json"
    )
    race = json.loads(path.read_text(encoding="utf-8"))["MRData"]["RaceTable"]["Races"][0]
    rows = [
        {
            "driver": row["Driver"]["code"],
            "dn": row["number"],
            "team": row["Constructor"]["name"],
            "fn": row["Driver"]["givenName"],
            "ln": row["Driver"]["familyName"],
        }
        for row in race["Results"]
    ]
    return {"drivers": sorted(rows, key=lambda d: int(d["dn"]))}


@pytest.fixture
def chinese_gp_race(ergast_offline, monkeypatch) -> core.Session:
    """2025 Chinese GP race with the Ergast classification merged (fixture payloads)."""
    payloads = {"drivers.json": _chinese_gp_drivers()}

    async def fetch_multiple(requests, **_kwargs):
        return [payloads.get(request[3]) for request in requests]

    monkeypatch.setattr(core, "fetch_multiple_async", fetch_multiple)
    session = tif1.get_session(2025, "Chinese Grand Prix", "Race", enable_cache=False)
    session._payload_loader.transport = InMemoryTransport(payloads)
    return session


class TestGetDriverWithClassification:
    def test_driver_carries_the_results_row(self, chinese_gp_race):
        driver = chinese_gp_race.get_driver("VER")
        assert isinstance(driver, DriverResult)
        assert driver["Position"] == 4.0
        assert driver["Points"] == 12.0
        assert driver["Status"] == "Finished"
        assert driver["DriverId"] == "max_verstappen"
        assert driver.dnf is False

    def test_row_matches_session_results(self, chinese_gp_race):
        driver = chinese_gp_race.get_driver("PIA")
        row = chinese_gp_race.results.loc["81"]
        for column in FASTF1_RESULTS_COLUMNS:
            left, right = driver[column], row[column]
            assert (pd.isna(left) and pd.isna(right)) or left == right, column

    @pytest.mark.parametrize(("code", "dnf"), [("ALO", True), ("LEC", True), ("BOR", False)])
    def test_dnf(self, chinese_gp_race, code, dnf):
        assert chinese_gp_race.get_driver(code).dnf is dnf


class TestGetDriverErgastOnly:
    @pytest.mark.usefixtures("ergast_offline")
    def test_requests_no_timing_payload(self, monkeypatch):
        """Ergast-only seasons have no timing files: get_driver must not ask the CDN for any."""
        batch: list[str] = []

        async def fetch_multiple(requests, **_kwargs):
            batch.extend(request[3] for request in requests)
            return [None] * len(requests)

        monkeypatch.setattr(core, "fetch_multiple_async", fetch_multiple)
        winner = tif1.get_session(1950, "British Grand Prix", "Race", enable_cache=False)
        abbreviation = winner.results.iloc[0]["Abbreviation"]

        session = tif1.get_session(1950, "British Grand Prix", "Race", enable_cache=False)
        transport = InMemoryTransport()
        session._payload_loader.transport = transport
        driver = session.get_driver(abbreviation)
        assert driver["Position"] == 1.0
        assert batch == []
        assert transport.calls == []


# -- load(livedata=), f1_api_support, api_path ------------------------------------------------


class TestLoadKeywords:
    def test_livedata_raises_before_any_request(self, race, cdn):
        with pytest.raises(NotImplementedError, match="historical data only"):
            race.load(livedata="recording.txt")
        assert cdn.requested() == []

    def test_load_async_livedata_raises(self, race, cdn):
        with pytest.raises(NotImplementedError, match="historical data only"):
            asyncio.run(race.load_async(livedata=object()))
        assert cdn.requested() == []

    def test_livedata_none_and_positional_flags_still_work(self, race, cdn):
        assert race.load(True, False, False, False, livedata=None) is race
        assert not race.laps.empty
        assert cdn.requested_matching("_tel.json") == []
        assert cdn.requested_matching("rcm.json") == []
        assert cdn.requested_matching("weather.json") == []

    def test_livedata_is_keyword_only(self, race):
        with pytest.raises(TypeError):
            race.load(True, False, False, False, "recording.txt")  # type: ignore[ty:too-many-positional-arguments]


class TestApiSurface:
    @pytest.mark.parametrize(
        ("year", "event", "supported"),
        [
            (2017, "Australian Grand Prix", False),
            (2018, "Australian Grand Prix", True),
            (2025, "Bahrain Grand Prix", True),
        ],
    )
    def test_f1_api_support(self, year, event, supported):
        session = core.Session(year, event, "Race", enable_cache=False)
        assert session.f1_api_support is supported

    def test_api_path_is_the_cdn_prefix(self):
        session = core.Session(2025, "Bahrain Grand Prix", "Practice 1", enable_cache=False)
        assert session.api_path == "2025/Bahrain%20Grand%20Prix/Practice%201/"


# -- Lap-level helpers -----------------------------------------------------------------------


class TestLapHelpers:
    def test_lap_is_bound_and_telemetry_is_a_telemetry(self, race):
        # The payload flags no personal best: pick_fastest() is None (fastf1).
        assert race.laps.pick_drivers("VER").pick_fastest() is None
        lap = race.laps.pick_drivers("VER").pick_fastest(only_by_time=True)
        assert isinstance(lap, Lap)
        assert isinstance(lap.get_car_data(), Telemetry)
        assert lap.get_pos_data().equals(lap.get_car_data())

    @pytest.mark.parametrize(
        ("pad_side", "first_s", "last_s"),
        [
            ("both", LAP_STARTS["VER"][0] + 2.0, LAP_STARTS["VER"][2] + 0.25),
            ("before", LAP_STARTS["VER"][0] + 2.0, LAP_STARTS["VER"][2]),
            ("after", LAP_STARTS["VER"][1], LAP_STARTS["VER"][2] + 0.25),
        ],
    )
    def test_car_data_pad_reaches_into_the_neighbouring_laps(self, race, pad_side, first_s, last_s):
        lap = race.laps.pick_drivers("VER").pick_laps(2).iloc[0]
        assert len(lap.get_car_data()) == TEL_SAMPLES  # no pad: the lap's own frame
        padded = lap.get_car_data(pad=1, pad_side=pad_side)
        # The lap window ends inclusively at the lap end (= next lap's first sample).
        assert padded["SessionTime"].min() == pd.Timedelta(seconds=first_s)
        assert padded["SessionTime"].max() == pd.Timedelta(seconds=last_s)
        assert lap.get_pos_data(pad=1, pad_side=pad_side).equals(padded)

    def test_session_info_requests_nothing(self, race, cdn):
        info = race.session_info
        assert info["Meeting"]["Circuit"]["Key"] is not None
        assert cdn.requested() == []

    def test_session_track_status_is_the_derived_table(self, race):
        track = race.track_status
        assert track["Status"].tolist() == ["1", "4", "1"]
        assert track["Time"].tolist() == [
            pd.Timedelta(0),
            pd.Timedelta(seconds=SC_DEPLOYED_S),
            pd.Timedelta(seconds=480),
        ]

    def test_session_status_and_start(self, race):
        assert race.t0_date == T0
        assert race.session_start_time == pd.Timedelta(minutes=5)
        status = race.session_status
        assert status["Status"].tolist() == ["Started", "Finished"]
