"""Unit tests for ``tif1.core_utils.circuit_info``.

``corners.json`` old and new schema parsing, the MultiViewer backup (injected
``mv_fetch``), polars frames, circuit-key resolution (vendored table and an
injected index) and parsing parity with fastf1's ``get_circuit_info``.
"""

from __future__ import annotations

import copy
import json
import math
from importlib.resources import files
from typing import Any

import pandas as pd
import pytest

from tif1.core_utils import circuit_info as ci_mod
from tif1.core_utils.circuit_info import (
    MARKER_COLUMNS,
    SECTIONS_COLUMNS,
    TRACK_COLUMNS,
    build_circuit_info,
    circuit_info_from_multiviewer,
    is_new_schema,
    load_vendored_circuits,
    match_circuit_index,
    multiviewer_request_key,
    resolve_circuit_key,
)
from tif1.models import CircuitInfo

# -- fixtures (shaped like the CDN files and the MultiViewer API) -------------

OLD_PAYLOAD: dict[str, Any] = {
    "CornerNumber": [1, 2, 3],
    "X": [42.4, 820.8, "None"],
    "Y": [8329.2, None, 8065.4],
    "Angle": [-246.6, -101.2, "None"],
    "Distance": [712.3, "None", 921.3],
    "Rotation": 92.0,
}

NEW_PAYLOAD: dict[str, Any] = {
    "CornerNumber": [1, 2, 3],
    "X": [42.4, 820.8, 1912.9],
    "Y": [8329.2, 7879.0, 8065.4],
    "Angle": [-246.6, -101.2, -261.4],
    "Distance": [710.5, "None", 921.3],
    "Rotation": 92.0,
    "Section": ["Low", None, "Straight"],
    "SectionType": ["Straight", "Low", "Straight"],
    "SectionStartPercent": [0.0, 10.31, 17.02],
    "SectionEndPercent": [10.31, 17.02, 25.08],
    "SectionStartDistance": [0.0, 554.3, 915.1],
    "SectionEndDistance": [554.3, 915.1, "None"],
    "CornerLetter": ["", "A", ""],
    "CornerLabel": ["1", "2A", "3"],
    "MVDistance": [704.6, 798.8, 910.6],
    "MarshalLightNumber": [1, 2],
    "MarshalLightX": [-403.0, -171.0],
    "MarshalLightY": [819.3, 6888.1],
    "MarshalLightAngle": [-182.6, -181.6],
    "MarshalLightDistance": [5363.6, "None"],
    "MarshalLightMVDistance": [5287.0, 554.8],
    "MarshalSectorNumber": [1, 2],
    "MarshalSectorX": [-393.2, -175.1],
    "MarshalSectorY": [1019.2, 6721.3],
    "MarshalSectorAngle": [-183.0, -181.7],
    "MarshalSectorDistance": [5363.6, 547.1],
    "MarshalSectorMVDistance": [5307.0, 538.1],
    "CircuitKey": 63,
    "CircuitName": "Sakhir",
    "Location": "Sakhir",
    "CountryName": "Bahrain",
    "CountryKey": 36,
    "CountryIocCode": "BRN",
    "PitLossNormal": 23.96,
    "PitLossSC": "None",
    "PitLossVSC": 17.33,
    "LayoutYear": 2022,
    "LayoutRound": 1,
    "LayoutRaceDate": "2022-03-20",
    "LayoutMeetingKey": 1124,
    "LayoutMeetingName": "Bahrain Grand Prix",
    "LayoutMeetingOfficialName": "FORMULA 1 GULF AIR BAHRAIN GRAND PRIX 2022",
    "LayoutLapDriverNumber": "1",
    "LayoutLapNumber": 3,
    "LayoutLapSession": "FP1",
    "LayoutLapTime": 97.766,
    "LayoutLapStartDate": "2022-03-18T12:05:19.921000",
    "LayoutLapStartSessionTime": 1219.908,
    "LayoutLapSessionStartTime": 900.206,
    "TrackX": [-375, -372, -372, -370],
    "TrackY": [1344, 1443, 1462, 1500],
    "TrackTime": [],
    "MiniSectorIndex": [20, 38, "None"],
}


def _mv_marker(number: int, x: float, y: float, angle: float, letter: str | None = None) -> dict:
    entry = {
        "angle": angle,
        "length": 1000.0 * number,
        "number": number,
        "trackPosition": {"x": x, "y": y},
    }
    if letter is not None:
        entry["letter"] = letter
    return entry


MV_PAYLOAD: dict[str, Any] = {
    "candidateLap": {
        "driverNumber": "1",
        "lapNumber": 3,
        "lapStartDate": "2022-03-18T12:05:19.921000",
        "lapStartSessionTime": 1219.908,
        "lapTime": 97.766,
        "session": "FP1",
        "sessionStartTime": 900.206,
    },
    "circuitKey": 63,
    "circuitName": "Sakhir",
    "corners": [
        _mv_marker(1, 1.5, 2.5, -246.6),
        _mv_marker(2, 3.5, 4.5, -101.2, letter="A"),
        _mv_marker(3, 5.5, 6.5, -261.4),
    ],
    "countryIocCode": "BRN",
    "countryKey": 36,
    "countryName": "Bahrain",
    "location": "Sakhir",
    "marshalLights": [_mv_marker(1, -403.0, 819.3, -182.6), _mv_marker(2, -171.0, 6888.1, -181.6)],
    "marshalSectors": [
        _mv_marker(1, -393.2, 1019.2, -183.0),
        _mv_marker(2, -175.1, 6721.3, -181.7),
    ],
    "meetingKey": "1124",
    "meetingName": "Bahrain Grand Prix",
    "meetingOfficialName": "FORMULA 1 GULF AIR BAHRAIN GRAND PRIX 2022",
    "miniSectorsIndexes": [20, 38],
    "pitLoss": {"normal": "23.84", "sc": "15.46", "vsc": "16.43"},
    "raceDate": "2022-03-20",
    "rotation": 45,
    "round": 1,
    "x": [-375, -372],
    "y": [1344, 1443],
    "year": 2022,
}

#: ``/api/v1/circuits`` entries (a subset shaped like the live index).
MV_INDEX: dict[str, Any] = {
    "10": {"name": "Melbourne", "country": "Australia", "years": [2019, 2022], "circuitKey": 10},
    "9": {"name": "Austin", "country": "United States", "years": [2018], "circuitKey": 9},
    "15": {"name": "Catalunya", "country": "Spain", "years": [2019, 2023], "circuitKey": 15},
    "63": {"name": "Sakhir", "country": "Bahrain", "years": [2019, 2022], "circuitKey": 63},
    "148": {"name": "Sakhir Outer Track", "country": "Bahrain", "years": [2020], "circuitKey": 148},
    "61": {"name": "Singapore", "country": "Singapore", "years": [2018], "circuitKey": 61},
    "147": {
        "name": "Algarve International Circuit",
        "country": "Portugal",
        "years": [2020, 2021],
        "circuitKey": 147,
    },
    "6": {"name": "Imola", "country": "Italy", "years": [2020], "circuitKey": 6},
    "39": {"name": "Monza", "country": "Italy", "years": [2018], "circuitKey": 39},
    "2": {"name": "Silverstone", "country": "Great Britain", "years": [2019], "circuitKey": 2},
}


class _Recorder:
    """An ``mv_fetch`` that records its calls and answers a fixed payload."""

    def __init__(self, payload: Any = None, error: BaseException | None = None):
        self.payload = payload
        self.error = error
        self.calls: list[tuple[int, int]] = []

    def __call__(self, year: int, circuit_key: int) -> Any:
        self.calls.append((year, circuit_key))
        if self.error is not None:
            raise self.error
        return self.payload


def _assert_marker_frame(frame: pd.DataFrame, rows: int) -> None:
    assert isinstance(frame, pd.DataFrame)
    assert list(frame.columns) == MARKER_COLUMNS
    assert len(frame) == rows
    if rows:
        assert frame["X"].dtype == "float64"
        assert frame["Y"].dtype == "float64"
        assert frame["Number"].dtype == "int64"
        assert frame["Letter"].dtype == object
        assert frame["Angle"].dtype == "float64"
        assert frame["Distance"].dtype == "float64"


# -- old schema ---------------------------------------------------------------


class TestOldSchema:
    def test_corners_dtypes_values_and_sentinels(self):
        info = build_circuit_info(
            OLD_PAYLOAD, year=2024, circuit_id="bahrain", allow_multiviewer=False
        )

        assert isinstance(info, CircuitInfo)
        _assert_marker_frame(info.corners, 3)
        assert info.corners["Number"].tolist() == [1, 2, 3]
        assert info.corners["X"].iloc[0] == 42.4
        assert math.isnan(info.corners["X"].iloc[2])
        assert math.isnan(info.corners["Y"].iloc[1])
        assert math.isnan(info.corners["Angle"].iloc[2])
        assert math.isnan(info.corners["Distance"].iloc[1])
        assert info.corners["Distance"].iloc[2] == 921.3
        assert (info.corners["Letter"] == "").all()
        assert info.rotation == 92.0
        _assert_marker_frame(info.marshal_lights, 0)
        _assert_marker_frame(info.marshal_sectors, 0)
        assert info.source == "corners.json"
        assert info.circuit_key is None
        assert info.pit_loss == {}
        assert list(info.track.columns) == TRACK_COLUMNS
        assert info.track.empty
        assert list(info.sections.columns) == SECTIONS_COLUMNS
        assert info.sections.empty
        assert info.mini_sector_index == []

    def test_rotation_none_sentinel_is_zero(self):
        payload = {**OLD_PAYLOAD, "Rotation": "None"}
        info = build_circuit_info(payload, year=2024, circuit_id=None, allow_multiviewer=False)
        assert info.rotation == 0.0

    @pytest.mark.parametrize("payload", [None, {}])
    def test_missing_file_without_backup_is_empty(self, payload):
        info = build_circuit_info(payload, year=2024, circuit_id="bahrain", allow_multiviewer=False)
        _assert_marker_frame(info.corners, 0)
        _assert_marker_frame(info.marshal_lights, 0)
        assert info.rotation == 0.0
        assert info.source == ""

    def test_is_new_schema(self):
        assert not is_new_schema(OLD_PAYLOAD)
        assert is_new_schema(NEW_PAYLOAD)
        assert not is_new_schema(None)
        assert not is_new_schema(["MarshalLightX"])


# -- new schema ---------------------------------------------------------------


class TestNewSchema:
    def test_letters_marshals_and_extras(self):
        fetch = _Recorder(MV_PAYLOAD)
        info = build_circuit_info(NEW_PAYLOAD, year=2025, circuit_id="bahrain", mv_fetch=fetch)

        assert fetch.calls == []
        assert info.source == "corners.json"
        _assert_marker_frame(info.corners, 3)
        assert info.corners["Letter"].tolist() == ["", "A", ""]
        assert info.corners["Distance"].iloc[0] == 710.5
        assert math.isnan(info.corners["Distance"].iloc[1])
        _assert_marker_frame(info.marshal_lights, 2)
        assert info.marshal_lights["Number"].tolist() == [1, 2]
        assert info.marshal_lights["X"].tolist() == [-403.0, -171.0]
        assert info.marshal_lights["Distance"].iloc[0] == 5363.6
        assert math.isnan(info.marshal_lights["Distance"].iloc[1])
        _assert_marker_frame(info.marshal_sectors, 2)
        assert info.marshal_sectors["Y"].tolist() == [1019.2, 6721.3]
        assert info.rotation == 92.0
        assert info.circuit_key == 63
        assert info.circuit_name == "Sakhir"
        assert info.location == "Sakhir"
        assert info.country == "Bahrain"
        assert info.country_code == "BRN"
        assert info.country_key == 36
        assert info.pit_loss == {"normal": 23.96, "vsc": 17.33}
        assert list(info.track.columns) == TRACK_COLUMNS
        assert info.track["X"].tolist() == [-375.0, -372.0, -372.0, -370.0]
        assert info.track["Time"].isna().all()
        assert list(info.sections.columns) == SECTIONS_COLUMNS
        assert info.sections["Number"].tolist() == [1, 2, 3]
        assert info.sections["Section"].tolist() == ["Low", "", "Straight"]
        assert info.sections["SectionType"].tolist() == ["Straight", "Low", "Straight"]
        assert info.sections["StartPercent"].tolist() == [0.0, 10.31, 17.02]
        assert math.isnan(info.sections["EndDistance"].iloc[2])
        assert info.mini_sector_index == [20, 38]

    def test_sections_empty_when_section_lists_are_empty(self):
        payload = {
            **NEW_PAYLOAD,
            "Section": [None, None, None],
            "SectionType": [],
            "SectionStartPercent": [],
            "SectionEndPercent": [],
            "SectionStartDistance": [],
            "SectionEndDistance": [],
            "MiniSectorIndex": [],
        }
        info = build_circuit_info(payload, year=2025, circuit_id="vegas", allow_multiviewer=False)
        assert list(info.sections.columns) == SECTIONS_COLUMNS
        assert info.sections.empty
        assert info.mini_sector_index == []

    def test_new_schema_without_rows_uses_backup_and_file_key(self):
        """A new-schema file with empty lists (2020 Tuscan GP) is filled by MultiViewer."""
        payload = {
            key: ([] if isinstance(value, list) else None) for key, value in NEW_PAYLOAD.items()
        }
        payload["Rotation"] = 273.0
        fetch = _Recorder(MV_PAYLOAD)

        info = build_circuit_info(payload, year=2020, circuit_id="mugello", mv_fetch=fetch)

        assert fetch.calls == [(2020, 146)]
        assert info.source == "corners.json+multiviewer"
        assert info.corners["Number"].tolist() == [1, 2, 3]
        assert info.rotation == 45.0  # no corners in the file: MultiViewer's layout wins
        assert len(info.marshal_lights) == 2
        assert info.circuit_key == 63

    def test_file_circuit_key_wins_over_vendored_table(self):
        """The 2020 Sakhir GP file names the outer track (148) while Ergast says 'bahrain'."""
        payload = {
            **NEW_PAYLOAD,
            "CircuitKey": 148,
            "CircuitName": "Sakhir Outer Track",
            "MarshalLightNumber": [],
            "MarshalLightX": [],
            "MarshalLightY": [],
            "MarshalLightAngle": [],
            "MarshalLightDistance": [],
            "MarshalSectorNumber": [],
            "MarshalSectorX": [],
            "MarshalSectorY": [],
            "MarshalSectorAngle": [],
            "MarshalSectorDistance": [],
        }
        fetch = _Recorder(MV_PAYLOAD)

        info = build_circuit_info(payload, year=2020, circuit_id="bahrain", mv_fetch=fetch)

        assert fetch.calls == [(2020, 148)]
        assert info.circuit_key == 148  # the file's metadata is kept
        assert info.circuit_name == "Sakhir Outer Track"
        assert info.corners["X"].tolist() == [42.4, 820.8, 1912.9]  # file corners win
        assert info.rotation == 92.0
        assert info.marshal_lights["Number"].tolist() == [1, 2]  # filled by MultiViewer
        assert info.source == "corners.json+multiviewer"


# -- MultiViewer backup -------------------------------------------------------


class TestMultiViewerBackup:
    def test_old_schema_calls_multiviewer_once_and_merges(self):
        fetch = _Recorder(MV_PAYLOAD)

        info = build_circuit_info(OLD_PAYLOAD, year=2024, circuit_id="bahrain", mv_fetch=fetch)

        assert fetch.calls == [(2024, 63)]
        assert info.source == "corners.json+multiviewer"
        # corners.json corners and rotation win.
        _assert_marker_frame(info.corners, 3)
        assert info.corners["X"].iloc[0] == 42.4
        assert math.isnan(info.corners["X"].iloc[2])
        assert info.corners["Distance"].iloc[0] == 712.3
        assert info.rotation == 92.0
        # MultiViewer fills the letters (by corner number), the marshal frames and the extras.
        assert info.corners["Letter"].tolist() == ["", "A", ""]
        _assert_marker_frame(info.marshal_lights, 2)
        assert info.marshal_lights["X"].tolist() == [-403.0, -171.0]
        assert info.marshal_lights["Distance"].isna().all()
        _assert_marker_frame(info.marshal_sectors, 2)
        assert info.circuit_key == 63
        assert info.circuit_name == "Sakhir"
        assert info.country_code == "BRN"
        assert info.country_key == 36
        assert info.pit_loss == {"normal": 23.84, "sc": 15.46, "vsc": 16.43}
        assert info.track["X"].tolist() == [-375.0, -372.0]
        assert info.track["Time"].isna().all()
        assert info.sections.empty
        assert info.mini_sector_index == [20, 38]

    def test_missing_file_uses_multiviewer_entirely(self):
        fetch = _Recorder(MV_PAYLOAD)

        info = build_circuit_info(None, year=2024, circuit_id="bahrain", mv_fetch=fetch)

        assert fetch.calls == [(2024, 63)]
        assert info.source == "multiviewer"
        _assert_marker_frame(info.corners, 3)
        assert info.corners["X"].tolist() == [1.5, 3.5, 5.5]
        assert info.corners["Letter"].tolist() == ["", "A", ""]
        assert info.corners["Distance"].isna().all()
        assert info.rotation == 45.0
        assert len(info.marshal_sectors) == 2

    def test_rotation_sentinel_in_file_falls_back_to_multiviewer(self):
        payload = {**OLD_PAYLOAD, "Rotation": "None"}
        info = build_circuit_info(
            payload, year=2024, circuit_id="bahrain", mv_fetch=_Recorder(MV_PAYLOAD)
        )
        assert info.rotation == 45.0

    @pytest.mark.parametrize(
        "fetch",
        [
            _Recorder(error=RuntimeError("boom")),
            _Recorder(payload=None),
            _Recorder(payload={}),
            _Recorder(payload="not a dict"),
        ],
    )
    def test_failure_leaves_frames_empty_and_never_raises(self, fetch):
        info = build_circuit_info(OLD_PAYLOAD, year=2024, circuit_id="bahrain", mv_fetch=fetch)
        assert fetch.calls == [(2024, 63)]
        _assert_marker_frame(info.corners, 3)
        _assert_marker_frame(info.marshal_lights, 0)
        _assert_marker_frame(info.marshal_sectors, 0)
        assert info.source == "corners.json"
        assert info.circuit_key is None

        empty = build_circuit_info(None, year=2024, circuit_id="bahrain", mv_fetch=fetch)
        _assert_marker_frame(empty.corners, 0)
        assert empty.source == ""
        assert empty.rotation == 0.0

    def test_allow_multiviewer_false_never_calls(self):
        fetch = _Recorder(MV_PAYLOAD)
        info = build_circuit_info(
            None, year=2024, circuit_id="bahrain", allow_multiviewer=False, mv_fetch=fetch
        )
        assert fetch.calls == []
        assert info.source == ""

    def test_unresolvable_circuit_skips_multiviewer(self, monkeypatch):
        from tif1.mvapi import api as api_mod

        monkeypatch.setattr(api_mod, "get_circuits_index", lambda: None)
        fetch = _Recorder(MV_PAYLOAD)

        info = build_circuit_info(OLD_PAYLOAD, year=2026, circuit_id="madring", mv_fetch=fetch)

        assert fetch.calls == []
        assert info.source == "corners.json"

    def test_letters_matched_by_corner_number(self):
        payload = {**OLD_PAYLOAD, "CornerNumber": [3, 2, 1, 2]}
        payload["X"] = [1.0, 2.0, 3.0, 4.0]
        payload["Y"] = [1.0, 2.0, 3.0, 4.0]
        payload["Angle"] = [0.0, 0.0, 0.0, 0.0]
        payload["Distance"] = [0.0, 0.0, 0.0, 0.0]
        info = build_circuit_info(
            payload, year=2024, circuit_id="bahrain", mv_fetch=_Recorder(MV_PAYLOAD)
        )
        assert info.corners["Letter"].tolist() == ["", "A", "", "A"]

    def test_ambiguous_donor_numbers_leave_letters_empty(self):
        mv = copy.deepcopy(MV_PAYLOAD)
        mv["corners"].append(_mv_marker(2, 9.0, 9.0, 0.0, letter="B"))
        info = build_circuit_info(
            OLD_PAYLOAD, year=2024, circuit_id="bahrain", mv_fetch=_Recorder(mv)
        )
        assert info.corners["Letter"].tolist() == ["", "", ""]

    def test_mugello_request_key_override(self):
        assert multiviewer_request_key(149, "Mugello") == 146
        assert multiviewer_request_key(149, " mugello ") == 146
        assert multiviewer_request_key(149, "Jeddah") == 149
        assert multiviewer_request_key(63, "Mugello") == 63

        payload = {
            key: ([] if isinstance(value, list) else None) for key, value in NEW_PAYLOAD.items()
        }
        payload["CircuitKey"] = 149
        payload["CircuitName"] = "Mugello"
        fetch = _Recorder(MV_PAYLOAD)
        build_circuit_info(payload, year=2020, circuit_id="mugello", mv_fetch=fetch)
        assert fetch.calls == [(2020, 146)]

    def test_default_fetch_is_the_mvapi_client(self, monkeypatch):
        from tif1.mvapi import api as api_mod

        calls: list[tuple[int, int]] = []

        def fake_get_circuit(*, year: int, circuit_key: int):
            calls.append((year, circuit_key))
            return MV_PAYLOAD

        monkeypatch.setattr(api_mod, "get_circuit", fake_get_circuit)
        info = build_circuit_info(OLD_PAYLOAD, year=2024, circuit_id="bahrain")
        assert calls == [(2024, 63)]
        assert info.source == "corners.json+multiviewer"


# -- polars -------------------------------------------------------------------


class TestPolars:
    @pytest.fixture(autouse=True)
    def _polars(self):
        pytest.importorskip("polars")

    def test_new_schema_polars_frames(self):
        import polars as pl

        info = build_circuit_info(
            NEW_PAYLOAD, year=2025, circuit_id="bahrain", lib="polars", allow_multiviewer=False
        )

        for frame in (info.corners, info.marshal_lights, info.marshal_sectors):
            assert isinstance(frame, pl.DataFrame)
            assert frame.columns == MARKER_COLUMNS
            assert frame.schema["X"] == pl.Float64
            assert frame.schema["Y"] == pl.Float64
            assert frame.schema["Number"] == pl.Int64
            assert frame.schema["Letter"] == pl.String
            assert frame.schema["Angle"] == pl.Float64
            assert frame.schema["Distance"] == pl.Float64
        assert info.corners["Letter"].to_list() == ["", "A", ""]
        assert info.corners["Distance"].to_list()[1] is None
        assert info.marshal_lights.height == 2
        assert isinstance(info.track, pl.DataFrame)
        assert info.track.columns == TRACK_COLUMNS
        assert info.track.schema["Time"] == pl.Float64
        assert info.track["Time"].null_count() == 4
        assert isinstance(info.sections, pl.DataFrame)
        assert info.sections.columns == SECTIONS_COLUMNS
        assert info.sections.schema["Section"] == pl.String
        assert info.sections.schema["StartPercent"] == pl.Float64
        assert info.sections["Section"].to_list() == ["Low", "", "Straight"]

    def test_backup_and_empty_polars_frames(self):
        import polars as pl

        info = build_circuit_info(
            OLD_PAYLOAD,
            year=2024,
            circuit_id="bahrain",
            lib="polars",
            mv_fetch=_Recorder(MV_PAYLOAD),
        )
        assert info.source == "corners.json+multiviewer"
        assert info.corners["X"].to_list()[2] is None
        assert info.marshal_lights["Distance"].null_count() == 2
        assert info.sections.height == 0
        assert info.sections.schema["Number"] == pl.Int64

        empty = build_circuit_info(
            None, year=2024, circuit_id=None, lib="polars", allow_multiviewer=False
        )
        assert isinstance(empty.corners, pl.DataFrame)
        assert empty.corners.columns == MARKER_COLUMNS
        assert empty.corners.schema["Letter"] == pl.String
        assert empty.corners.height == 0

    def test_multiviewer_polars(self):
        import polars as pl

        info = circuit_info_from_multiviewer(MV_PAYLOAD, lib="polars")
        assert isinstance(info.corners, pl.DataFrame)
        assert info.corners["Number"].to_list() == [1, 2, 3]
        assert info.source == "multiviewer"


# -- circuit-key resolution ---------------------------------------------------


class TestResolveCircuitKey:
    def test_vendored_table(self):
        table = load_vendored_circuits()
        assert table["bahrain"]["circuitKey"] == 63
        assert table["bahrain"]["iocCountryCode"] == "BRN"
        assert resolve_circuit_key("bahrain", allow_live=False) == 63
        assert resolve_circuit_key("mugello", allow_live=False) == 146
        assert resolve_circuit_key("red_bull_ring", allow_live=False) == 19
        assert resolve_circuit_key("jeddah", allow_live=False) == 149
        assert resolve_circuit_key("madring", allow_live=False) is None
        assert resolve_circuit_key(None, allow_live=False) is None
        assert resolve_circuit_key("", allow_live=False) is None

    def test_vendored_table_covers_the_packaged_schedules(self):
        table = load_vendored_circuits()
        unlisted = {"madring", "sepang"}  # not in MultiViewer's index
        for year in range(2018, 2027):
            path = files("tif1").joinpath(f"data/schedules/ergast/schedule_{year}.json")
            with path.open("r", encoding="utf-8") as handle:
                races = json.load(handle)["races"]
            for race in races:
                circuit_id = race["Circuit"]["circuitId"]
                if circuit_id in unlisted:
                    continue
                assert circuit_id in table, f"{circuit_id} ({year}) missing from mv_circuits.json"
        for circuit_id, entry in table.items():
            assert isinstance(entry["circuitKey"], int), circuit_id
            assert isinstance(entry["countryKey"], int), circuit_id
            assert isinstance(entry["iocCountryCode"], str)
            assert entry["iocCountryCode"]
            assert entry["name"]
            assert entry["country"]
            assert entry["years"] == sorted(entry["years"])
        assert list(table) == sorted(table)

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({"circuit_id": "albert_park", "location": "Melbourne", "country": "Australia"}, 10),
            (
                {"circuit_name": "Circuit of the Americas", "location": "Austin", "country": "USA"},
                9,
            ),
            ({"circuit_name": "Circuit de Barcelona-Catalunya", "country": "Spain"}, 15),
            ({"location": "Sakhir", "country": "Bahrain"}, 63),
            ({"circuit_name": "Marina Bay Street Circuit", "country": "Singapore"}, 61),
            (
                {
                    "circuit_name": "Autódromo Internacional do Algarve",
                    "location": "Portimão",
                    "country": "Portugal",
                },
                147,
            ),
            ({"circuit_id": "silverstone", "country": "UK"}, 2),
            ({"circuit_name": "Autodromo Nazionale di Monza", "location": "Monza"}, 39),
            ({"location": "Melbourne", "country": "Austria"}, None),  # wrong country
            ({"circuit_name": "Sakhir", "country": "Spain"}, None),
            ({"location": "Nowhere"}, None),
            ({}, None),
        ],
    )
    def test_index_matching(self, kwargs, expected):
        assert match_circuit_index(MV_INDEX, **kwargs) == expected

    def test_ambiguous_match_is_none_unless_year_breaks_the_tie(self):
        index = {
            "1": {"name": "Layout", "country": "X", "years": [2019], "circuitKey": 1},
            "2": {"name": "Layout", "country": "X", "years": [2023], "circuitKey": 2},
        }
        assert match_circuit_index(index, location="Layout", country="X") is None
        assert match_circuit_index(index, location="Layout", country="X", year=2023) == 2
        assert match_circuit_index(index, location="Layout", country="X", year=2021) == 1

    def test_injected_index_and_live_fallback(self, monkeypatch):
        from tif1.mvapi import api as api_mod

        assert (
            resolve_circuit_key(
                "unknown_id", location="Melbourne", country="Australia", index=MV_INDEX
            )
            == 10
        )
        calls: list[int] = []

        def fake_index():
            calls.append(1)
            return MV_INDEX

        monkeypatch.setattr(api_mod, "get_circuits_index", fake_index)
        assert resolve_circuit_key("unknown_id", location="Austin", country="United States") == 9
        assert calls == [1]
        # The vendored table answers first: no index request.
        assert resolve_circuit_key("bahrain", location="Austin") == 63
        assert calls == [1]
        # allow_live=False never asks for the index.
        assert resolve_circuit_key("unknown_id", location="Austin", allow_live=False) is None
        assert calls == [1]

    def test_missing_vendored_file_is_an_empty_table(self, monkeypatch):
        monkeypatch.setattr(ci_mod, "_vendored_circuits", None)
        monkeypatch.setattr(ci_mod, "VENDORED_CIRCUITS_PATH", "data/circuits/does_not_exist.json")
        assert load_vendored_circuits() == {}
        monkeypatch.setattr(ci_mod, "_vendored_circuits", None)


# -- fastf1 parity ------------------------------------------------------------


class TestFastf1Parity:
    def test_multiviewer_parsing_matches_fastf1(self, monkeypatch):
        fastf1_data = pytest.importorskip("fastf1.mvapi.data")
        monkeypatch.setattr(fastf1_data, "get_circuit", lambda **_: copy.deepcopy(MV_PAYLOAD))

        theirs = fastf1_data.get_circuit_info(year=2024, circuit_key=63)
        ours = circuit_info_from_multiviewer(MV_PAYLOAD)

        assert ours.rotation == theirs.rotation
        for name in ("corners", "marshal_lights", "marshal_sectors"):
            mine, ref = getattr(ours, name), getattr(theirs, name)
            assert list(mine.columns) == list(ref.columns)
            assert len(mine) == len(ref)
            for column in ("X", "Y", "Angle", "Distance"):
                assert mine[column].dtype == ref[column].dtype == "float64"
            assert mine["Number"].dtype == ref["Number"].dtype == "int64"
            # pandas 3 infers ``str`` for fastf1's tuple rows; tif1 keeps ``object``.
            assert mine["Letter"].tolist() == ref["Letter"].tolist()
            pd.testing.assert_frame_equal(mine, ref, check_dtype=False)
