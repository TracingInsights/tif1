"""Derived session state (``tif1.core_utils.session_state``) and its ``Session`` surfaces.

The track-status state machine runs on hand-built race-control frames shaped
like ``Session.race_control_messages`` (pandas and polars), with fastf1's
status codes and message strings; ``session_status``, ``session_start_time``,
``t0_date`` and ``session_info`` are checked on the functions and on a
``Session`` whose laps and messages are set in memory (no network).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, ClassVar

import numpy as np
import pandas as pd
import pytest

from tif1 import core
from tif1.core_utils import session_state as ss
from tif1.models import Laps

T0 = pd.Timestamp("2024-06-09 17:59:06")
T0_NS = int(T0.value)
SEC = 1_000_000_000

BACKENDS = ["pandas", "polars"]

#: fastf1's track-status vocabulary (``fastf1.api.track_status_data``).
FASTF1_TRACK_MESSAGES = {
    "1": "AllClear",
    "2": "Yellow",
    "4": "SCDeployed",
    "5": "Red",
    "6": "VSCDeployed",
    "7": "VSCEnding",
}

_RCM_COLUMNS = ("Time", "Category", "Message", "Status", "Flag", "Scope", "Sector")


# -- builders ---------------------------------------------------------------------


def flag(
    seconds: float,
    flag_name: str,
    scope: str = "Track",
    *,
    sector: int | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    """A race-control ``Flag`` row ``seconds`` after ``T0``.

    Args:
        seconds: Session time of the message.
        flag_name: The ``Flag`` value (``"YELLOW"``, ``"CLEAR"``, ...).
        scope: ``"Track"``, ``"Sector"`` or ``"Driver"``.
        sector: Marshal sector of a sector-scoped flag.
        message: The ``Message`` text (derived from the flag when omitted).

    Returns:
        The row as a column-name mapping.
    """
    if message is None:
        if scope == "Sector":
            message = f"{flag_name} IN TRACK SECTOR {sector}"
        elif flag_name == "CLEAR":
            message = "TRACK CLEAR"
        elif flag_name == "GREEN":
            message = "GREEN LIGHT - PIT EXIT OPEN"
        else:
            message = f"{flag_name} FLAG"
    return {
        "Time": T0 + pd.Timedelta(seconds=seconds),
        "Category": "Flag",
        "Message": message,
        "Status": None,
        "Flag": flag_name,
        "Scope": scope,
        "Sector": None if sector is None else float(sector),
    }


def safety_car(seconds: float, status: str, *, virtual: bool = False) -> dict[str, Any]:
    """A race-control ``SafetyCar`` row (``DEPLOYED``, ``IN THIS LAP``, ``ENDING``).

    Args:
        seconds: Session time of the message.
        status: The ``Status`` value.
        virtual: Virtual safety car instead of the safety car.

    Returns:
        The row as a column-name mapping.
    """
    mode = "VIRTUAL SAFETY CAR" if virtual else "SAFETY CAR"
    return {
        "Time": T0 + pd.Timedelta(seconds=seconds),
        "Category": "SafetyCar",
        "Message": f"{mode} {status}",
        "Status": status,
        "Flag": None,
        "Scope": None,
        "Sector": None,
    }


def other(seconds: float, message: str, category: str = "Other") -> dict[str, Any]:
    """A race-control row of another category (``Other``, ``Drs``, ``CarEvent``).

    Args:
        seconds: Session time of the message.
        message: The ``Message`` text.
        category: The ``Category`` value.

    Returns:
        The row as a column-name mapping.
    """
    return {
        "Time": T0 + pd.Timedelta(seconds=seconds),
        "Category": category,
        "Message": message,
        "Status": None,
        "Flag": None,
        "Scope": None,
        "Sector": None,
    }


def rcm_frame(rows: list[dict[str, Any]], lib: str = "pandas") -> Any:
    """A race-control frame of ``rows`` with ``Session.race_control_messages`` dtypes.

    Args:
        rows: Rows built by :func:`flag`, :func:`safety_car` or :func:`other`.
        lib: ``"pandas"`` or ``"polars"``.

    Returns:
        ``Time`` datetime64[ns], text columns, ``Sector`` float64.
    """
    columns = {name: [row[name] for row in rows] for name in _RCM_COLUMNS}
    if lib == "polars":
        pl = pytest.importorskip("polars")
        return pl.DataFrame(
            {
                "Time": pl.Series("Time", columns["Time"], dtype=pl.Datetime("ns")),
                **{
                    name: pl.Series(name, columns[name], dtype=pl.String)
                    for name in ("Category", "Message", "Status", "Flag", "Scope")
                },
                "Sector": pl.Series("Sector", columns["Sector"], dtype=pl.Float64),
            }
        )
    frame = pd.DataFrame(
        {name: pd.Series(columns[name], dtype=object) for name in _RCM_COLUMNS[1:6]}
    )
    frame.insert(0, "Time", pd.Series(columns["Time"], dtype="datetime64[ns]"))
    frame["Sector"] = pd.Series(columns["Sector"], dtype="float64")
    return frame


def transitions(rows: list[dict[str, Any]], lib: str = "pandas") -> list[tuple[float, str]]:
    """``(seconds, status)`` transitions of the track-status machine over ``rows``.

    Args:
        rows: Race-control rows.
        lib: Backend of the input frame.

    Returns:
        The machine's rows with times in seconds.
    """
    return [(ns / SEC, code) for ns, code in ss.track_status_rows(rcm_frame(rows, lib), T0_NS)]


def laps_frame(starts: list[float | None], codes: list[str], lap_numbers: list[int]) -> Any:
    """A laps frame with ``LapStartTime``, ``TrackStatus`` and ``LapNumber``.

    Args:
        starts: Lap start session times in seconds (None for NaT).
        codes: The laps' ``TrackStatus`` strings.
        lap_numbers: The laps' numbers.

    Returns:
        A pandas frame.
    """
    return pd.DataFrame(
        {
            "LapStartTime": pd.to_timedelta([np.nan if s is None else s for s in starts], unit="s"),
            "TrackStatus": codes,
            "LapNumber": [float(n) for n in lap_numbers],
        }
    )


# -- track status: race-control state machine -----------------------------------------


class TestTrackStatusMachine:
    @pytest.mark.parametrize("lib", BACKENDS)
    def test_first_row_is_all_clear_at_time_zero(self, lib):
        assert transitions([other(5, "DRS ENABLED", "Drs")], lib) == [(0.0, "1")]

    def test_no_messages_still_starts_all_clear(self):
        assert ss.track_status_rows(None, T0_NS) == [(0, "1")]
        assert ss.track_status_rows(rcm_frame([]), T0_NS) == [(0, "1")]

    @pytest.mark.parametrize("lib", BACKENDS)
    def test_sector_yellows_clear_only_when_every_sector_is_clear(self, lib):
        rows = [
            flag(10, "YELLOW", "Sector", sector=3),
            flag(20, "YELLOW", "Sector", sector=5),
            flag(30, "CLEAR", "Sector", sector=3),
            flag(40, "CLEAR", "Sector", sector=5),
        ]
        assert transitions(rows, lib) == [(0.0, "1"), (10.0, "2"), (40.0, "1")]

    def test_clearing_a_sector_that_was_never_yellow_keeps_the_yellow(self):
        rows = [
            flag(10, "YELLOW", "Sector", sector=3),
            flag(20, "CLEAR", "Sector", sector=9),
            flag(30, "CLEAR", "Sector", sector=3),
        ]
        assert transitions(rows) == [(0.0, "1"), (10.0, "2"), (30.0, "1")]

    @pytest.mark.parametrize("lib", BACKENDS)
    def test_double_yellow_in_a_sector_is_yellow(self, lib):
        rows = [
            flag(10, "DOUBLE YELLOW", "Sector", sector=7),
            flag(25, "CLEAR", "Sector", sector=7),
        ]
        assert transitions(rows, lib) == [(0.0, "1"), (10.0, "2"), (25.0, "1")]

    def test_double_yellow_on_the_track_is_yellow(self):
        rows = [flag(10, "DOUBLE YELLOW", "Track"), flag(20, "CLEAR", "Track")]
        assert transitions(rows) == [(0.0, "1"), (10.0, "2"), (20.0, "1")]

    @pytest.mark.parametrize("lib", BACKENDS)
    def test_safety_car_deployed_in_this_lap_then_track_clear(self, lib):
        rows = [
            flag(5, "YELLOW", "Sector", sector=12),
            safety_car(10, "DEPLOYED"),
            flag(50, "YELLOW", "Sector", sector=4),  # a yellow under SC keeps SC
            safety_car(100, "IN THIS LAP"),
            flag(105, "CLEAR", "Sector", sector=12),  # still behind the SC
            flag(106, "CLEAR", "Sector", sector=4),
            flag(120, "CLEAR", "Track"),
        ]
        assert transitions(rows, lib) == [(0.0, "1"), (5.0, "2"), (10.0, "4"), (120.0, "1")]

    @pytest.mark.parametrize("lib", BACKENDS)
    def test_virtual_safety_car_deployed_ending_then_clear(self, lib):
        rows = [
            safety_car(10, "DEPLOYED", virtual=True),
            safety_car(50, "ENDING", virtual=True),
            flag(60, "CLEAR", "Track"),
        ]
        assert transitions(rows, lib) == [(0.0, "1"), (10.0, "6"), (50.0, "7"), (60.0, "1")]

    def test_safety_car_ending_is_not_a_vsc_ending(self):
        rows = [safety_car(10, "DEPLOYED"), safety_car(50, "ENDING"), flag(60, "CLEAR")]
        assert transitions(rows) == [(0.0, "1"), (10.0, "4"), (60.0, "1")]

    def test_vsc_then_full_safety_car(self):
        rows = [
            safety_car(10, "DEPLOYED", virtual=True),
            safety_car(30, "DEPLOYED"),
            safety_car(90, "IN THIS LAP"),
            flag(120, "CLEAR"),
        ]
        assert transitions(rows) == [(0.0, "1"), (10.0, "6"), (30.0, "4"), (120.0, "1")]

    @pytest.mark.parametrize("lib", BACKENDS)
    def test_red_flag_then_resume_on_green_light(self, lib):
        rows = [
            flag(5, "YELLOW", "Sector", sector=2),
            flag(10, "RED", "Track"),
            flag(15, "YELLOW", "Sector", sector=6),  # ignored while red
            flag(16, "CLEAR", "Sector", sector=6),
            flag(600, "GREEN", "Track"),
        ]
        assert transitions(rows, lib) == [(0.0, "1"), (5.0, "2"), (10.0, "5"), (600.0, "1")]

    @pytest.mark.parametrize("lib", BACKENDS)
    def test_chequered_blue_and_driver_flags_do_not_change_the_status(self, lib):
        rows = [
            flag(10, "BLUE", "Driver", message="WAVED BLUE FLAG FOR CAR 2 (SAR)"),
            flag(20, "BLACK AND WHITE", "Driver", message="BLACK AND WHITE FLAG FOR CAR 1"),
            flag(25, "BLACK AND ORANGE", "Driver", message="BLACK AND ORANGE FLAG FOR CAR 4"),
            flag(30, "CHEQUERED", "Track", message="CHEQUERED FLAG"),
        ]
        assert transitions(rows, lib) == [(0.0, "1")]

    def test_chequered_flag_does_not_end_a_yellow(self):
        rows = [flag(10, "YELLOW", "Sector", sector=1), flag(20, "CHEQUERED")]
        assert transitions(rows) == [(0.0, "1"), (10.0, "2")]

    def test_consecutive_identical_statuses_collapse(self):
        rows = [
            flag(1, "GREEN", "Track"),  # already AllClear: no row
            flag(10, "YELLOW", "Track"),
            flag(12, "YELLOW", "Sector", sector=4),
            flag(14, "DOUBLE YELLOW", "Sector", sector=5),
            flag(20, "CLEAR", "Track"),
            flag(21, "CLEAR", "Track"),
        ]
        assert transitions(rows) == [(0.0, "1"), (10.0, "2"), (20.0, "1")]

    def test_time_is_session_time_relative_to_t0(self):
        rows = [flag(61.25, "YELLOW", "Sector", sector=3)]
        result = ss.track_status_rows(rcm_frame(rows), T0_NS)
        assert result[1] == (int(61.25 * SEC), "2")

    def test_messages_are_processed_in_time_order(self):
        rows = [
            flag(40, "CLEAR", "Sector", sector=5),
            flag(10, "YELLOW", "Sector", sector=5),
        ]
        assert transitions(rows) == [(0.0, "1"), (10.0, "2"), (40.0, "1")]

    def test_rows_without_a_timestamp_are_ignored(self):
        frame = rcm_frame([flag(10, "YELLOW", "Sector", sector=3), flag(20, "RED")])
        frame.loc[1, "Time"] = pd.NaT
        assert ss.track_status_rows(frame, T0_NS) == [(0, "1"), (10 * SEC, "2")]

    def test_messages_before_t0_never_produce_a_negative_time(self):
        rows = [flag(-30, "YELLOW", "Sector", sector=3), flag(10, "CLEAR", "Sector", sector=3)]
        result = ss.track_status_rows(rcm_frame(rows), T0_NS)
        assert all(time_ns >= 0 for time_ns, _ in result)
        assert [code for _, code in result] == ["1", "2", "1"]

    def test_real_canada_2024_safety_car_sequence(self):
        """The 2024 Canadian GP race-control order around the first safety car."""
        rows = [
            flag(0, "GREEN", "Track"),
            flag(2430, "YELLOW", "Sector", sector=3),
            safety_car(2480, "DEPLOYED"),
            safety_car(2908, "IN THIS LAP"),
            flag(2916, "CLEAR", "Sector", sector=3),
            flag(2999, "CLEAR", "Track"),
        ]
        assert transitions(rows) == [(0.0, "1"), (2430.0, "2"), (2480.0, "4"), (2999.0, "1")]


# -- track status: laps fallback ------------------------------------------------------


class TestTrackStatusFromLaps:
    def test_multi_code_lap_emits_its_codes_in_order_at_the_lap_start(self):
        laps = laps_frame([100.0, 190.0, 280.0, 370.0], ["1", "124", "41", "1"], [1, 2, 3, 4])
        rows = ss.track_status_rows_from_laps(laps)
        # "41" starts with the SC already in force: only its "1" is a change.
        assert rows == [(0, "1"), (190 * SEC, "2"), (190 * SEC, "4"), (280 * SEC, "1")]

    def test_earliest_lap_start_of_each_lap_number_is_used(self):
        laps = laps_frame([195.0, 190.0, 100.0], ["2", "2", "1"], [2, 2, 1])
        assert ss.track_status_rows_from_laps(laps) == [(0, "1"), (190 * SEC, "2")]

    def test_unknown_codes_and_missing_starts_are_skipped(self):
        laps = laps_frame([None, 100.0, 200.0], ["5", "3", "6"], [1, 2, 3])
        assert ss.track_status_rows_from_laps(laps) == [(0, "1"), (200 * SEC, "6")]

    def test_no_laps_is_the_single_all_clear_row(self):
        assert ss.track_status_rows_from_laps(None) == [(0, "1")]
        assert ss.track_status_rows_from_laps(laps_frame([], [], [])) == [(0, "1")]

    def test_polars_laps(self):
        pl = pytest.importorskip("polars")
        laps = pl.DataFrame(
            {
                "LapStartTime": pl.Series(
                    [timedelta(seconds=100), timedelta(seconds=190), timedelta(seconds=280)],
                    dtype=pl.Duration("ns"),
                ),
                "TrackStatus": ["1", "67", "1"],
                "LapNumber": [1.0, 2.0, 3.0],
            }
        )
        assert ss.track_status_rows_from_laps(laps) == [
            (0, "1"),
            (190 * SEC, "6"),
            (190 * SEC, "7"),
            (280 * SEC, "1"),
        ]


# -- status frames ------------------------------------------------------------------


class TestStatusFrame:
    def test_vocabulary_is_fastf1s(self):
        assert ss.TRACK_STATUS_MESSAGES == FASTF1_TRACK_MESSAGES

    def test_pandas_track_status_frame(self):
        rows = [(0, code) for code in FASTF1_TRACK_MESSAGES]
        frame = ss.status_frame(rows, "pandas", with_message=True)
        assert list(frame.columns) == ["Time", "Status", "Message"]
        assert frame["Time"].dtype == "timedelta64[ns]"
        assert frame["Status"].dtype == object
        assert frame["Message"].dtype == object
        assert dict(zip(frame["Status"], frame["Message"], strict=True)) == FASTF1_TRACK_MESSAGES

    def test_polars_track_status_frame(self):
        pl = pytest.importorskip("polars")
        frame = ss.status_frame([(0, "1"), (5 * SEC, "4")], "polars", with_message=True)
        assert frame.columns == ["Time", "Status", "Message"]
        assert frame.schema["Time"] == pl.Duration("ns")
        assert frame.schema["Status"] == pl.String
        assert frame["Message"].to_list() == ["AllClear", "SCDeployed"]
        assert frame["Time"].to_list() == [timedelta(0), timedelta(seconds=5)]

    @pytest.mark.parametrize("lib", BACKENDS)
    def test_empty_session_status_frame_is_typed(self, lib):
        frame = ss.status_frame([], lib, with_message=False)
        assert list(frame.columns) == ["Time", "Status"]
        assert len(frame) == 0
        if lib == "pandas":
            assert frame["Time"].dtype == "timedelta64[ns]"
        else:
            pl = pytest.importorskip("polars")
            assert frame.schema["Time"] == pl.Duration("ns")


# -- session status ------------------------------------------------------------------


class TestSessionStatus:
    @pytest.mark.parametrize("lib", BACKENDS)
    def test_race_started_aborted_restarted_finished(self, lib):
        rows = [
            flag(60, "GREEN", "Track"),  # pit exit opens before the race start
            flag(600, "RED", "Track"),
            flag(1200, "GREEN", "Track"),
            flag(5000, "CHEQUERED", "Track", message="CHEQUERED FLAG"),
        ]
        result = ss.session_status_rows(rcm_frame(rows, lib), T0_NS, 300 * SEC)
        assert result == [
            (300 * SEC, "Started"),
            (600 * SEC, "Aborted"),
            (1200 * SEC, "Started"),
            (5000 * SEC, "Finished"),
        ]

    def test_qualifying_segments_each_start_on_a_green_light(self):
        rows = [
            flag(0, "GREEN"),
            flag(1080, "CHEQUERED", message="CHEQUERED FLAG"),
            flag(1500, "GREEN"),
            flag(2400, "CHEQUERED", message="CHEQUERED FLAG"),
            flag(2880, "GREEN"),
            flag(3600, "CHEQUERED", message="CHEQUERED FLAG"),
        ]
        result = ss.session_status_rows(rcm_frame(rows), T0_NS, 0)
        assert [status for _, status in result] == [
            "Started",
            "Finished",
            "Started",
            "Finished",
            "Started",
            "Finished",
        ]
        assert [time_ns // SEC for time_ns, _ in result] == [0, 1080, 1500, 2400, 2880, 3600]

    def test_without_a_start_time_the_first_green_light_starts(self):
        rows = [flag(42, "GREEN"), flag(900, "CHEQUERED", message="CHEQUERED FLAG")]
        assert ss.session_status_rows(rcm_frame(rows), T0_NS, None) == [
            (42 * SEC, "Started"),
            (900 * SEC, "Finished"),
        ]

    def test_track_clear_is_not_a_restart(self):
        rows = [flag(600, "RED"), flag(700, "CLEAR"), flag(1200, "GREEN")]
        result = ss.session_status_rows(rcm_frame(rows), T0_NS, 0)
        assert [status for _, status in result] == ["Started", "Aborted", "Started"]

    def test_no_messages_and_no_start_is_empty(self):
        assert ss.session_status_rows(rcm_frame([]), T0_NS, None) == []

    def test_finalised_and_ends_are_never_emitted(self):
        rows = [flag(0, "GREEN"), flag(10, "CHEQUERED", message="CHEQUERED FLAG")]
        statuses = {status for _, status in ss.session_status_rows(rcm_frame(rows), T0_NS, 0)}
        assert statuses <= {"Started", "Aborted", "Finished"}


# -- t0_date / session_start_time ----------------------------------------------------------


class TestT0Date:
    @staticmethod
    def _laps(offsets_ns: list[int]) -> pd.DataFrame:
        starts = [300 * SEC + 95 * SEC * i for i in range(len(offsets_ns))]
        return pd.DataFrame(
            {
                "LapStartTime": pd.to_timedelta(starts, unit="ns"),
                "LapStartDate": pd.to_datetime(
                    [
                        T0_NS + start + noise
                        for start, noise in zip(starts, offsets_ns, strict=True)
                    ],
                    unit="ns",
                ),
            }
        )

    def test_median_offset_rounded_to_the_millisecond(self):
        laps = self._laps([400, -300, 200, 499_000])  # sub-ms noise and one outlier
        t0 = ss.t0_date_ns(laps)
        assert t0 == T0_NS
        assert t0 % 1_000_000 == 0

    def test_rounds_half_a_millisecond_up(self):
        laps = self._laps([600_000, 600_000, 600_000])
        assert ss.t0_date_ns(laps) == T0_NS + 1_000_000

    def test_polars_laps(self):
        pl = pytest.importorskip("polars")
        laps = pl.from_pandas(self._laps([0, 100, -100]))
        assert ss.t0_date_ns(laps) == T0_NS

    def test_float_second_lap_starts_are_accepted(self):
        laps = pd.DataFrame(
            {
                "LapStartTime": [300.0000004, 395.0],
                "LapStartDate": [T0 + pd.Timedelta(seconds=300), T0 + pd.Timedelta(seconds=395)],
            }
        )
        assert ss.t0_date_ns(laps) == T0_NS

    @pytest.mark.parametrize(
        "laps",
        [
            None,
            pd.DataFrame(),
            pd.DataFrame({"LapStartTime": [pd.Timedelta(seconds=1)]}),
            pd.DataFrame(
                {
                    "LapStartTime": pd.to_timedelta([np.nan]),
                    "LapStartDate": pd.to_datetime([pd.NaT]),
                }
            ),
        ],
        ids=["none", "empty", "no-dates", "all-nat"],
    )
    def test_none_without_usable_laps(self, laps):
        assert ss.t0_date_ns(laps) is None


class TestSessionStartTime:
    RCM: ClassVar[list[dict[str, Any]]] = [
        flag(120, "GREEN"),
        flag(4000, "CHEQUERED", message="CHEQUERED FLAG"),
    ]
    SCHEDULED = T0_NS + 300 * SEC

    def test_race_like_sessions_start_on_the_scheduled_time(self):
        start = ss.session_start_time_ns(rcm_frame(self.RCM), T0_NS, self.SCHEDULED, race_like=True)
        assert start == 300 * SEC

    @pytest.mark.parametrize("lib", BACKENDS)
    def test_other_sessions_start_on_the_first_green_light(self, lib):
        start = ss.session_start_time_ns(
            rcm_frame(self.RCM, lib), T0_NS, self.SCHEDULED, race_like=False
        )
        assert start == 120 * SEC

    def test_green_flag_without_green_light_message_is_not_a_start(self):
        rows = [flag(50, "GREEN", message="TRACK GREEN"), flag(120, "GREEN")]
        assert ss.session_start_time_ns(rcm_frame(rows), T0_NS, None, race_like=False) == 120 * SEC

    def test_each_falls_back_to_the_other(self):
        rcm = rcm_frame(self.RCM)
        assert ss.session_start_time_ns(rcm, T0_NS, None, race_like=True) == 120 * SEC
        no_green = rcm_frame([flag(4000, "CHEQUERED", message="CHEQUERED FLAG")])
        assert (
            ss.session_start_time_ns(no_green, T0_NS, self.SCHEDULED, race_like=False) == 300 * SEC
        )
        assert ss.session_start_time_ns(no_green, T0_NS, None, race_like=False) is None

    def test_none_without_t0(self):
        assert (
            ss.session_start_time_ns(rcm_frame(self.RCM), None, self.SCHEDULED, race_like=True)
            is None
        )


# -- session_info -----------------------------------------------------------------------


class TestSessionInfoDict:
    EVENT = pd.Series(
        {
            "EventName": "Bahrain Grand Prix",
            "OfficialEventName": "FORMULA 1 GULF AIR BAHRAIN GRAND PRIX 2025",
            "Location": "Sakhir",
            "Country": "Bahrain",
            "CircuitName": "Bahrain International Circuit",
        }
    )

    def _info(self, session_name: str = "Race", **kwargs: Any) -> dict[str, Any]:
        defaults: dict[str, Any] = {
            "start_date": pd.Timestamp("2025-04-13 18:00"),
            "gmt_offset": pd.Timedelta(hours=3),
        }
        defaults.update(kwargs)
        return ss.session_info_dict(self.EVENT, session_name, **defaults)

    def test_nested_keys_are_fastf1s(self):
        info = self._info()
        assert set(info) == {
            "Meeting",
            "ArchiveStatus",
            "Key",
            "Type",
            "Name",
            "StartDate",
            "EndDate",
            "GmtOffset",
            "Path",
        }
        assert set(info["Meeting"]) == {
            "Key",
            "Name",
            "OfficialName",
            "Location",
            "Country",
            "Circuit",
        }
        assert set(info["Meeting"]["Country"]) == {"Key", "Code", "Name"}
        assert set(info["Meeting"]["Circuit"]) == {"Key", "ShortName"}

    def test_values_from_the_schedule_row(self):
        info = self._info(circuit_key=63, country_key=36, country_code="BRN")
        meeting = info["Meeting"]
        assert meeting["Name"] == "Bahrain Grand Prix"
        assert meeting["OfficialName"] == "FORMULA 1 GULF AIR BAHRAIN GRAND PRIX 2025"
        assert meeting["Location"] == "Sakhir"
        assert meeting["Country"] == {"Key": 36, "Code": "BRN", "Name": "Bahrain"}
        assert meeting["Circuit"] == {"Key": 63, "ShortName": "Bahrain International Circuit"}
        assert info["Name"] == "Race"

    def test_underivable_keys_are_none(self):
        info = self._info()
        assert info["Key"] is None
        assert info["Meeting"]["Key"] is None
        assert info["Path"] is None
        assert info["ArchiveStatus"] is None
        assert info["Meeting"]["Country"]["Key"] is None
        assert info["Meeting"]["Circuit"]["Key"] is None

    def test_dates_and_offset_types(self):
        info = self._info()
        assert isinstance(info["StartDate"], pd.Timestamp)
        assert isinstance(info["EndDate"], pd.Timestamp)
        assert isinstance(info["GmtOffset"], pd.Timedelta)
        assert info["EndDate"] - info["StartDate"] == pd.Timedelta(hours=2)

    def test_short_name_override(self):
        info = self._info(circuit_short_name="Sakhir")
        assert info["Meeting"]["Circuit"]["ShortName"] == "Sakhir"

    @pytest.mark.parametrize(
        ("session_name", "session_type", "duration"),
        [
            ("Practice 1", "Practice", pd.Timedelta(hours=1, minutes=30)),
            ("Practice 3", "Practice", pd.Timedelta(hours=1)),
            ("Qualifying", "Qualifying", pd.Timedelta(hours=1)),
            ("Sprint Qualifying", "Qualifying", pd.Timedelta(minutes=44)),
            ("Sprint Shootout", "Qualifying", pd.Timedelta(minutes=44)),
            ("Sprint", "Race", pd.Timedelta(hours=1)),
            ("Race", "Race", pd.Timedelta(hours=2)),
        ],
    )
    def test_type_and_nominal_duration(self, session_name, session_type, duration):
        info = self._info(session_name)
        assert info["Type"] == session_type
        assert info["Name"] == session_name
        assert info["EndDate"] - info["StartDate"] == duration

    def test_unknown_start_has_no_end(self):
        info = self._info(start_date=None, gmt_offset=None)
        assert info["StartDate"] is None
        assert info["EndDate"] is None
        assert info["GmtOffset"] is None


# -- Session surfaces (laps and messages set in memory) ---------------------------------


def _session(session_name: str, *, with_laps: bool = True) -> core.Session:
    """A 2025 Bahrain session (vendored schedule) with laps and messages set in memory.

    ``t0_date`` is 2025-04-13 14:55 for the Race (scheduled 15:00 UTC) and
    2025-04-12 15:55 for Qualifying (16:00 UTC).

    Args:
        session_name: ``"Race"`` or ``"Qualifying"``.
        with_laps: Attach a two-lap laps table (otherwise an empty one).

    Returns:
        The session; nothing it reads reaches the network.
    """
    session = core.Session(2025, "Bahrain Grand Prix", session_name, enable_cache=False)
    t0 = (
        pd.Timestamp("2025-04-13 14:55")
        if session_name == "Race"
        else pd.Timestamp("2025-04-12 15:55")
    )
    if with_laps:
        starts = pd.to_timedelta([240.0004, 335.0], unit="s")
        frame = pd.DataFrame(
            {
                "Driver": ["VER", "VER"],
                "LapNumber": [1.0, 2.0],
                "LapStartTime": starts,
                "LapStartDate": [t0 + pd.Timedelta(seconds=240), t0 + pd.Timedelta(seconds=335)],
                "TrackStatus": ["1", "1"],
            }
        )
    else:
        frame = pd.DataFrame()
    session._laps = Laps(frame, session=session)
    base = T0
    shift = t0 - base
    rows = [
        flag(120, "GREEN"),
        flag(900, "RED"),
        flag(1500, "GREEN"),
        flag(4000, "CHEQUERED", message="CHEQUERED FLAG"),
    ]
    for row in rows:
        row["Time"] = row["Time"] + shift
    session._race_control_messages = rcm_frame(rows)
    return session


class TestSessionSurfaces:
    def test_t0_date_is_a_millisecond_timestamp(self):
        session = _session("Race")
        assert session.t0_date == pd.Timestamp("2025-04-13 14:55")
        assert session.t0_date.value % 1_000_000 == 0

    def test_t0_date_is_nat_without_laps(self):
        session = _session("Race", with_laps=False)
        assert session.t0_date is pd.NaT
        assert session.session_start_time is pd.NaT

    def test_race_starts_on_the_scheduled_time(self):
        assert _session("Race").session_start_time == pd.Timedelta(minutes=5)

    def test_qualifying_starts_on_the_first_green_light(self):
        assert _session("Qualifying").session_start_time == pd.Timedelta(seconds=120)

    def test_session_status_rows_and_dtypes(self):
        status = _session("Race").session_status
        assert list(status.columns) == ["Time", "Status"]
        assert status["Time"].dtype == "timedelta64[ns]"
        assert status["Status"].tolist() == ["Started", "Aborted", "Started", "Finished"]
        assert status["Time"].tolist() == [
            pd.Timedelta(minutes=5),
            pd.Timedelta(seconds=900),
            pd.Timedelta(seconds=1500),
            pd.Timedelta(seconds=4000),
        ]

    def test_session_status_is_empty_and_typed_without_laps(self):
        status = _session("Race", with_laps=False).session_status
        assert list(status.columns) == ["Time", "Status"]
        assert status.empty
        assert status["Time"].dtype == "timedelta64[ns]"

    def test_track_status_from_race_control(self):
        track = _session("Race").track_status
        assert list(track.columns) == ["Time", "Status", "Message"]
        assert track["Status"].tolist() == ["1", "5", "1"]
        assert track["Message"].tolist() == ["AllClear", "Red", "AllClear"]
        assert track["Time"].tolist() == [
            pd.Timedelta(0),
            pd.Timedelta(seconds=900),
            pd.Timedelta(seconds=1500),
        ]

    def test_derived_state_is_memoised_until_the_laps_change(self):
        session = _session("Race")
        first = session.track_status
        assert session.track_status is first
        session._laps = Laps(session._laps.copy(), session=session)
        assert session.track_status is not first

    def test_session_info_from_the_vendored_schedule(self):
        info = _session("Race").session_info
        assert info["Name"] == "Race"
        assert info["Type"] == "Race"
        assert info["Meeting"]["Name"] == "Bahrain Grand Prix"
        assert info["Meeting"]["Country"]["Name"] == "Bahrain"
        assert info["StartDate"] == pd.Timestamp("2025-04-13 18:00")  # local time
        assert info["EndDate"] == pd.Timestamp("2025-04-13 20:00")
        assert info["GmtOffset"] == pd.Timedelta(hours=3)
        assert isinstance(info["Meeting"]["Circuit"]["Key"], int)
        assert isinstance(info["Meeting"]["Country"]["Key"], int)
        assert info["Meeting"]["Country"]["Code"] == "BRN"
        assert info["Key"] is None
        assert info["Path"] is None
