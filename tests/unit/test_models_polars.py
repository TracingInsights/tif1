"""Unit tests for the polars model family (``tif1.models_polars``).

The fixtures build frames with the exact column/dtype contract the session
wiring produces (laps: fastf1's 31 columns plus the tif1 extras, every
``*Time`` column ``Duration("ns")``; telemetry: the 23-channel contract) and
a small in-memory session that serves per-lap telemetry from a dict.
"""

import datetime as dt
import subprocess
import sys

import polars as pl
import pytest

from tif1.exceptions import DataNotFoundError, DriverNotFoundError, LapNotFoundError
from tif1.models_polars import (
    TELEMETRY_SCHEMA,
    WEATHER_COLUMNS,
    F1Namespace,
    IterLapResult,
    PolarsDriver,
    PolarsDriverResult,
    PolarsLap,
    PolarsLaps,
    PolarsSessionResults,
    PolarsTelemetry,
    _rule_to_ns,
    _to_ns,
)

NS = pl.Duration("ns")
DT_NS = pl.Datetime("ns")
T0 = dt.datetime(2024, 3, 2, 15, 0, 0)  # noqa: DTZ001 - naive like LapStartDate


def seconds(value: float) -> dt.timedelta:
    return dt.timedelta(seconds=value)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def build_laps_frame() -> pl.DataFrame:
    """Six laps (VER 1-3, HAM 1-3) with the session-wiring column contract."""
    n = 6
    data: dict[str, list] = {
        "Time": [seconds(3700 + 95 * i) for i in range(n)],
        "Driver": ["VER", "VER", "VER", "HAM", "HAM", "HAM"],
        "DriverNumber": ["1", "1", "1", "44", "44", "44"],
        "LapTime": [
            seconds(95.0),
            seconds(93.5),
            None,
            seconds(96.0),
            seconds(92.0),
            seconds(94.0),
        ],
        "LapNumber": [1, 2, 3, 1, 2, 3],
        "Stint": [1, 1, 2, 1, 1, 1],
        "PitOutTime": [seconds(3600), None, seconds(3800), seconds(3600), None, None],
        "PitInTime": [None, seconds(3790), None, None, None, seconds(3990)],
        "Sector1Time": [seconds(30)] * n,
        "Sector2Time": [seconds(30)] * n,
        "Sector3Time": [seconds(35)] * n,
        "Sector1SessionTime": [seconds(3630)] * n,
        "Sector2SessionTime": [seconds(3660)] * n,
        "Sector3SessionTime": [seconds(3695)] * n,
        "SpeedI1": [300.0] * n,
        "SpeedI2": [280.0] * n,
        "SpeedFL": [310.0] * n,
        "SpeedST": [320.0] * n,
        "IsPersonalBest": [False, True, False, False, True, False],
        "Compound": ["SOFT", "SOFT", "MEDIUM", "HARD", "HARD", "HARD"],
        "TyreLife": [1, 2, 1, 1, 2, 3],
        "FreshTyre": [True] * n,
        "Team": ["Red Bull Racing"] * 3 + ["Mercedes"] * 3,
        "LapStartTime": [seconds(3605 + 95 * i) for i in range(n)],
        "LapStartDate": [T0 + seconds(95 * i) for i in range(n)],
        "TrackStatus": ["1", "12", "1", "1", "267", "1"],
        "Position": [1, 1, 1, 2, 2, 2],
        "Deleted": [False, False, False, False, True, False],
        "DeletedReason": ["", "", "", "", "TRACK LIMITS", ""],
        "FastF1Generated": [False] * n,
        "IsAccurate": [True, True, False, True, True, True],
        "WeatherTime": [seconds(3600 + 60 * i) for i in range(n)],
        "AirTemp": [25.0] * n,
        "Humidity": [40.0] * n,
        "Pressure": [1010.0] * n,
        "Rainfall": [False] * n,
        "TrackTemp": [40.0] * n,
        "WindDirection": [180] * n,
        "WindSpeed": [1.5] * n,
        "LapTimeSeconds": [95.0, 93.5, None, 96.0, 92.0, 94.0],
        "ms1": [30000] * n,
        "ms2": [30000] * n,
        "ms3": [35000] * n,
    }
    return pl.DataFrame(data).with_columns(
        *[pl.col(c).cast(NS) for c in data if c.endswith("Time")],
        pl.col("LapStartDate").cast(DT_NS),
        pl.col("LapNumber", "Stint", "TyreLife", "Position").cast(pl.Int64),
    )


def build_telemetry_frame(
    driver: str, lap: int, n: int = 10, start: float = 3605.0
) -> pl.DataFrame:
    """``n`` samples at 10 Hz from session time ``start`` with the telemetry contract."""
    times = [0.1 * i for i in range(n)]
    frame = pl.DataFrame(
        {
            "Date": [T0 + seconds(start - 3600 + t) for t in times],
            "SessionTime": [seconds(start + t) for t in times],
            "DriverAhead": [None] * n,
            "DistanceToDriverAhead": [float("nan")] * n,
            "Time": [seconds(t) for t in times],
            "RPM": [10000.0 + 100 * i for i in range(n)],
            "Speed": [200.0 + 5 * i for i in range(n)],
            "nGear": [7] * n,
            "Throttle": [100.0] * n,
            "Brake": [False] * n,
            "DRS": [0] * n,
            "Source": pl.Series(["merged"] * n, dtype=pl.Categorical),
            "Distance": [10.0 * i for i in range(n)],
            "RelativeDistance": [i / (n - 1) for i in range(n)],
            "Status": pl.Series(["OnTrack"] * n, dtype=pl.Categorical),
            "X": [1.0 * i for i in range(n)],
            "Y": [2.0 * i for i in range(n)],
            "Z": [0.0] * n,
            "AccelerationX": [0.0] * n,
            "AccelerationY": [0.0] * n,
            "AccelerationZ": [0.0] * n,
            "Driver": [driver] * n,
            "LapNumber": [lap] * n,
        },
        schema_overrides={"DriverAhead": pl.String},
    )
    return frame.with_columns(
        pl.col("Date").cast(DT_NS),
        pl.col("SessionTime", "Time").cast(NS),
        pl.col("nGear", "DRS", "LapNumber").cast(pl.Int64),
    )


def build_results_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "DriverNumber": ["1", "44", "2/MOS"],
            "BroadcastName": ["M VERSTAPPEN", "L HAMILTON", "S MOSS"],
            "Abbreviation": ["VER", "HAM", "MOS"],
            "DriverId": ["max_verstappen", "hamilton", "moss"],
            "TeamName": ["Red Bull Racing", "Mercedes", "Mercedes"],
            "TeamColor": ["3671C6", "27F4D2", "27F4D2"],
            "TeamId": ["red_bull", "mercedes", "mercedes"],
            "FirstName": ["Max", "Lewis", "Stirling"],
            "LastName": ["Verstappen", "Hamilton", "Moss"],
            "FullName": ["Max Verstappen", "Lewis Hamilton", "Stirling Moss"],
            "HeadshotUrl": ["", "", ""],
            "CountryCode": ["NED", "GBR", "GBR"],
            "Position": [1.0, 2.0, 3.0],
            "ClassifiedPosition": ["1", "2", "R"],
            "GridPosition": [1.0, 2.0, 3.0],
            "Q1": [None, None, None],
            "Q2": [None, None, None],
            "Q3": [None, None, None],
            "Time": [seconds(5400.0), seconds(5420.0), None],
            "Status": ["Finished", "+1 Lap", "Accident"],
            "Points": [25.0, 18.0, 0.0],
            "Laps": [57.0, 56.0, 20.0],
        },
        schema_overrides={"Q1": NS, "Q2": NS, "Q3": NS},
    ).with_columns(pl.col("Time").cast(NS))


class FakeSession:
    """The narrow session surface the polars models consume."""

    lib = "polars"
    year = 2024
    gp = "Bahrain"
    session = "Race"

    def __init__(self) -> None:
        self.laps = PolarsLaps._from_frame(build_laps_frame(), self)
        self.results = PolarsSessionResults._from_frame(build_results_frame(), self)
        self.weather_data = self.laps.get_weather_data()
        self.track_status = pl.DataFrame(
            {
                "Time": [seconds(0), seconds(3605.35), seconds(3605.75)],
                "Status": ["1", "2", "1"],
                "Message": ["AllClear", "Yellow", "AllClear"],
            }
        ).with_columns(pl.col("Time").cast(NS))
        self.failures: list[tuple[str, int, Exception]] = []
        self.fetches: list[tuple[str, int]] = []
        self.telemetry = {
            ("VER", 1): build_telemetry_frame("VER", 1),
            ("VER", 2): build_telemetry_frame("VER", 2, start=3700.0),
            ("HAM", 2): build_telemetry_frame("HAM", 2, start=3700.0),
        }

    def _resolve_telemetry_ultra_cold_mode(self, ultra_cold):  # noqa: ARG002
        return False

    def _get_telemetry_df_for_ref(self, driver, lap, *, ultra_cold, allow_prefetch=True):
        assert ultra_cold is False
        assert allow_prefetch is False
        self.fetches.append((driver, lap))
        frame = self.telemetry.get((driver, lap))
        if frame is None:
            raise DataNotFoundError(driver=driver, lap=lap)
        return PolarsTelemetry._from_frame(frame, self, driver)

    def _record_telemetry_failure(self, driver, lap, error):
        self.failures.append((driver, lap, error))

    def _get_driver_info(self, code):
        assert code == "NOR"
        return {"fn": "Lando", "ln": "Norris", "dn": "4", "team": "McLaren", "tc": "FF8000"}


@pytest.fixture
def session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def laps(session: FakeSession) -> PolarsLaps:
    return session.laps


@pytest.fixture
def tel(session: FakeSession) -> PolarsTelemetry:
    return PolarsTelemetry._from_frame(build_telemetry_frame("VER", 1), session, "VER")


# --------------------------------------------------------------------------- #
# Contracts and class survival
# --------------------------------------------------------------------------- #


class TestFrameContracts:
    def test_laps_fixture_dtypes(self, laps):
        schema = laps.schema
        for column in ("Time", "LapTime", "PitOutTime", "PitInTime", "LapStartTime", "WeatherTime"):
            assert schema[column] == NS
        assert schema["LapStartDate"] == DT_NS
        for column in ("LapNumber", "Stint", "TyreLife", "Position"):
            assert schema[column] == pl.Int64
        for column in ("Driver", "DriverNumber", "Team", "Compound", "TrackStatus"):
            assert schema[column] == pl.String
        assert schema["Deleted"] == pl.Boolean

    def test_telemetry_fixture_matches_schema(self, tel):
        assert list(tel.columns) == list(TELEMETRY_SCHEMA)
        for column, dtype in TELEMETRY_SCHEMA.items():
            assert tel.schema[column] == dtype, column

    def test_empty_telemetry_has_contract_columns(self):
        empty = PolarsTelemetry._empty()
        assert empty.height == 0
        assert list(empty.columns) == list(TELEMETRY_SCHEMA)
        assert empty.schema["Time"] == NS

    def test_from_frame_shares_data_and_sets_session(self, session):
        source = build_laps_frame()
        laps = PolarsLaps._from_frame(source, session)
        assert isinstance(laps, PolarsLaps)
        assert laps.session is session
        assert laps._tif1_session is session
        assert laps.height == source.height

    def test_constructor_accepts_session_keyword(self, session):
        laps = PolarsLaps(build_laps_frame(), session=session)
        assert laps.session is session
        tel = PolarsTelemetry(build_telemetry_frame("VER", 1), session=session, driver="VER")
        assert (tel.session, tel.driver) == (session, "VER")

    def test_drop_unknown_channels(self):
        frame = build_telemetry_frame("VER", 1).with_columns(pl.lit(1.0).alias("Custom"))
        tel = PolarsTelemetry(frame, drop_unknown_channels=True)
        assert "Custom" not in tel.columns
        # Registered channels and the metadata columns stay (pandas parity).
        assert {"Speed", "AccelerationX", "Driver", "LapNumber"} <= set(tel.columns)

    @pytest.mark.parametrize(
        "operation",
        [
            lambda laps: laps.head(2),
            lambda laps: laps.tail(2),
            lambda laps: laps.limit(3),
            lambda laps: laps.slice(1, 2),
            lambda laps: laps.clone(),
            lambda laps: laps.reverse(),
            lambda laps: laps[1:3],
            lambda laps: laps[["Driver", "LapNumber"]],
        ],
    )
    def test_polars_row_operations_keep_class_and_session(self, laps, session, operation):
        result = operation(laps)
        assert type(result) is PolarsLaps
        assert result.session is session

    def test_column_access_returns_series(self, laps):
        assert isinstance(laps["Driver"], pl.Series)

    def test_native_filter_returns_plain_frame(self, laps):
        plain = laps.filter(pl.col("Driver") == "VER")
        assert type(plain) is pl.DataFrame

    def test_base_class_view_is_plain_and_independent(self, laps, tel, session):
        view = laps.base_class_view
        assert type(view) is pl.DataFrame
        assert view.height == laps.height
        assert type(tel.base_class_view) is pl.DataFrame
        results = session.results.base_class_view
        assert type(results) is pl.DataFrame

    def test_chain_survival(self, laps, session):
        tel = laps.pick_driver("VER").pick_fastest().get_telemetry()
        assert isinstance(tel, PolarsTelemetry)
        assert tel.session is session
        assert tel.driver == "VER"
        assert tel.height == 10
        assert "DriverAhead" in tel.columns

    def test_telemetry_methods_carry_session_and_driver(self, tel, session):
        chained = tel.add_differential_distance().fill_missing().slice_by_mask([True] * 10)
        assert isinstance(chained, PolarsTelemetry)
        assert chained.session is session
        assert chained.driver == "VER"


# --------------------------------------------------------------------------- #
# PolarsLaps
# --------------------------------------------------------------------------- #


class TestPolarsLapsSelection:
    def test_pick_driver_by_code_and_number(self, laps):
        assert laps.pick_driver("VER").height == 3
        assert laps.pick_driver(1).height == 3
        assert laps.pick_driver("44").height == 3
        assert laps.pick_driver("XXX").height == 0
        assert isinstance(laps.pick_driver("VER"), PolarsLaps)

    def test_pick_drivers_mixed_identifiers(self, laps):
        picked = laps.pick_drivers([44, "VER"])
        assert picked.height == 6
        assert laps.pick_drivers("HAM").height == 3
        assert laps.pick_drivers(("1",)).height == 3

    def test_pick_driver_with_integer_driver_number_column(self, session):
        frame = build_laps_frame().with_columns(pl.col("DriverNumber").cast(pl.Int64))
        laps = PolarsLaps._from_frame(frame, session)
        assert laps.pick_driver(44).height == 3

    def test_pick_driver_accepts_driver_like_objects(self, laps, session):
        driver = session.results.row_for("HAM")
        assert laps.pick_driver(driver).height == 3
        assert laps.pick_driver({"Abbreviation": "VER"}).height == 3

    def test_pick_lap_and_pick_laps(self, laps):
        assert laps.pick_lap(2).height == 2
        assert laps.pick_laps(2).height == 2
        assert laps.pick_laps([1, 2]).height == 4
        assert laps.pick_laps(range(1, 3)).height == 4
        assert laps.pick_laps(2.0).height == 2
        assert laps.pick_laps(slice(2, None)).height == 4
        assert laps.pick_laps(slice(None, 1)).height == 2
        assert laps.pick_laps(slice(2, 3)).height == 4
        assert laps.pick_laps(laps=[3]).height == 2

    def test_pick_laps_rejects_bad_input(self, laps):
        with pytest.raises(ValueError, match=r"Invalid value 1\.5"):
            laps.pick_laps([1.5])
        with pytest.raises(TypeError):
            laps.pick_laps()

    def test_pick_team_and_teams(self, laps):
        assert laps.pick_team("Mercedes").height == 3
        assert laps.pick_teams(["Mercedes", "Red Bull Racing"]).height == 6
        assert laps.pick_teams("Red Bull Racing").height == 3

    def test_pick_fastest_takes_personal_bests_only(self, laps, session):
        # fastf1: only IsPersonalBest laps are candidates (VER 2 and HAM 2 here).
        fastest = laps.pick_fastest()
        assert isinstance(fastest, PolarsLap)
        assert fastest.driver == "HAM"
        assert fastest["LapTime"] == seconds(92.0)
        assert fastest.name == 4
        assert fastest.session is session
        assert laps.pick_driver("VER").pick_fastest()["LapTime"] == seconds(93.5)

    def test_pick_fastest_without_personal_best_column_skips_deleted(self, laps):
        fastest = laps.drop("IsPersonalBest").f1.pick_fastest()
        assert fastest.driver == "VER"  # HAM's 92.0 is deleted
        assert fastest["LapTime"] == seconds(93.5)

    def test_pick_fastest_only_by_time(self, laps):
        fastest = laps.pick_fastest(only_by_time=True)
        assert fastest.driver == "HAM"
        assert fastest.lap_number == 2
        assert fastest.name == 4

    def test_pick_fastest_none_cases(self, laps):
        assert laps.filter(pl.col("Driver") == "XXX").f1.pick_fastest() is None
        null_only = laps.pick_driver("VER").pick_lap(3)
        assert null_only.pick_fastest() is None
        assert laps.drop("LapTime").f1.pick_fastest() is None

    def test_pick_fastest_name_is_position_in_frame(self, laps):
        ham = laps.pick_driver("HAM")
        fastest = ham.pick_fastest(only_by_time=True)
        assert fastest.name == 1
        assert ham.slice(1, 1).row(0) == fastest.frame.row(0)

    def test_pick_quicklaps(self, laps):
        # The best time (HAM 92.0, deleted) still sets the bar: fastf1 parity.
        assert laps.pick_quicklaps().height == 5
        assert laps.pick_quicklaps(1.02).height == 2
        assert laps.pick_quicklaps(threshold=None).height == 5
        assert PolarsLaps.QUICKLAP_THRESHOLD == 1.07

    def test_pick_quicklaps_without_times_returns_self(self, laps):
        null_only = laps.pick_driver("VER").pick_lap(3)
        assert null_only.pick_quicklaps() is null_only

    def test_pick_tyre_and_compounds(self, laps):
        assert laps.pick_tyre("SOFT").height == 2
        assert laps.pick_compounds(["SOFT", "HARD"]).height == 5
        assert laps.pick_compounds("MEDIUM").height == 1

    @pytest.mark.parametrize(
        ("status", "how", "expected"),
        [
            ("1", "equals", 4),
            ("2", "contains", 2),
            ("2", "excludes", 4),
            ("26", "any", 2),
            ("26", "none", 4),
            (1, "equals", 4),
        ],
    )
    def test_pick_track_status(self, laps, status, how, expected):
        assert laps.pick_track_status(status, how=how).height == expected

    def test_pick_track_status_invalid_how(self, laps):
        with pytest.raises(ValueError, match="Invalid value 'sometimes'"):
            laps.pick_track_status("1", how="sometimes")

    def test_pick_track_status_null_is_not_a_match(self, session):
        frame = build_laps_frame().with_columns(
            pl.when(pl.col("LapNumber") == 1)
            .then(None)
            .otherwise(pl.col("TrackStatus"))
            .alias("TrackStatus")
        )
        laps = PolarsLaps._from_frame(frame, session)
        # Lap 1 of both drivers is null: "12", "1", "267", "1" remain.
        assert laps.pick_track_status("1", "contains").height == 3
        assert laps.pick_track_status("1", "excludes").height == 3  # nulls are kept

    def test_pick_box_laps(self, laps):
        assert laps.pick_wo_box().height == 1
        assert laps.pick_box_laps().height == 5
        assert laps.pick_box_laps("in").height == 2
        assert laps.pick_box_laps("out").height == 3
        with pytest.raises(ValueError, match="Invalid value 'sideways'"):
            laps.pick_box_laps("sideways")

    def test_box_methods_without_pit_columns_return_self(self, laps):
        plain = laps.drop("PitInTime").f1.as_laps()
        assert plain.pick_wo_box() is plain
        assert plain.pick_box_laps() is plain

    def test_pick_not_deleted_and_accurate(self, laps):
        assert laps.pick_not_deleted().height == 5
        assert laps.pick_accurate().height == 5
        plain = laps.drop("Deleted", "IsAccurate").f1.as_laps()
        assert plain.pick_not_deleted() is plain
        assert plain.pick_accurate() is plain

    def test_pick_not_deleted_keeps_unknown_flags(self, session):
        frame = build_laps_frame().with_columns(
            pl.when(pl.col("LapNumber") == 1)
            .then(None)
            .otherwise(pl.col("Deleted"))
            .alias("Deleted")
        )
        assert PolarsLaps._from_frame(frame, session).pick_not_deleted().height == 5


class TestPolarsLapsQualifying:
    def test_split_with_qualifying_session_column(self, laps, session):
        frame = laps.with_columns(
            pl.Series("QualifyingSession", ["Q1", "Q1", "q2", "Q2", "Q2", "Q2"])
        )
        q1, q2, q3 = PolarsLaps._from_frame(frame, session).split_qualifying_sessions()
        assert q1.height == 2
        assert q2.height == 4
        assert q3 is None
        assert isinstance(q1, PolarsLaps)
        assert q1.session is session

    def test_split_with_all_null_marks_falls_back(self, laps, session):
        frame = laps.with_columns(pl.lit(None, dtype=pl.String).alias("QualifyingSession"))
        q1, q2, q3 = PolarsLaps._from_frame(frame, session).split_qualifying_sessions()
        assert q1.height == 6
        assert (q2, q3) == (None, None)

    def test_split_without_breaks_is_all_q1(self, laps):
        q1, q2, q3 = laps.split_qualifying_sessions()
        assert q1.height == laps.height
        assert (q2, q3) == (None, None)

    def test_split_empty_frame(self, laps):
        empty = laps.filter(pl.col("Driver") == "XXX").f1.split_qualifying_sessions()
        assert empty == [None, None, None]

    def test_split_on_lap_start_gaps(self, session):
        # Lap starts: Q1 at 0..5 min, a 10 min break, Q2 at 15..18 min, an
        # 11 min break, Q3 at 29..31 min; one pit-out lap crosses the Q1/Q2
        # break (starts in Q1, ends in Q2) and belongs to Q2.
        starts = [0, 95, 190, 300, 15 * 60, 15 * 60 + 100, 18 * 60, 29 * 60, 31 * 60]
        ends = [s + 90 for s in starts]
        ends[3] = 16 * 60  # the early out-lap ends after the Q2 split
        pit_out = [None, None, None, seconds(290), None, None, None, None, None]
        frame = pl.DataFrame(
            {
                "Driver": ["VER"] * 9,
                "LapNumber": list(range(1, 10)),
                "LapStartTime": [seconds(s) for s in starts],
                "Time": [seconds(e) for e in ends],
                "PitOutTime": pit_out,
                "LapTime": [seconds(90.0)] * 9,
            }
        ).with_columns(pl.col("LapStartTime", "Time", "PitOutTime", "LapTime").cast(NS))
        q1, q2, q3 = PolarsLaps._from_frame(frame, session).split_qualifying_sessions()
        assert q1["LapNumber"].to_list() == [1, 2, 3]
        assert q2["LapNumber"].to_list() == [4, 5, 6, 7]
        assert q3["LapNumber"].to_list() == [8, 9]
        assert q3.session is session

    def test_split_uses_the_two_largest_gaps(self, session):
        starts = [0, 6 * 60, 20 * 60, 40 * 60]  # three gaps: 6, 14 and 20 minutes
        frame = pl.DataFrame(
            {
                "Driver": ["VER"] * 4,
                "LapNumber": [1, 2, 3, 4],
                "LapStartTime": [seconds(s) for s in starts],
                "Time": [seconds(s + 90) for s in starts],
            }
        ).with_columns(pl.col("LapStartTime", "Time").cast(NS))
        q1, q2, q3 = PolarsLaps._from_frame(frame, session).split_qualifying_sessions()
        assert q1["LapNumber"].to_list() == [1, 2]
        assert q2["LapNumber"].to_list() == [3]
        assert q3["LapNumber"].to_list() == [4]


class TestPolarsLapsIteration:
    def test_iterlaps_yields_every_lap_by_default(self, laps, session):
        results = list(laps.iterlaps())
        assert len(results) == 6  # fastf1: no requirement by default
        assert all(isinstance(item, IterLapResult) for item in results)
        index, lap = results[0]
        assert index == 0
        assert isinstance(lap, PolarsLap)
        assert lap.session is session
        assert results[0].index == 0
        assert results[0].lap.driver == "VER"
        assert [item.index for item in results] == [0, 1, 2, 3, 4, 5]
        assert results[2].lap["LapTime"] is None
        assert results[3].lap.name == 3
        assert results[3].lap["Driver"] == "HAM"
        assert [i for i, _ in laps.iterlaps(require=["LapTime", "Driver"])] == [0, 1, 3, 4, 5]

    def test_iterlaps_custom_require(self, laps):
        assert [i for i, _ in laps.iterlaps(require=["PitInTime"])] == [1, 5]
        assert len(list(laps.iterlaps(require=[]))) == 6

    def test_iterlaps_missing_column_yields_nothing(self, laps):
        assert list(laps.iterlaps(require=["Missing"])) == []  # fastf1

    def test_iterlaps_rows_match_frame(self, laps):
        for index, lap in laps.iterlaps():
            assert lap.to_dict() == laps.row(index, named=True)


class TestPolarsLapsTelemetry:
    def test_telemetry_concatenates_driver_laps(self, laps, session):
        tel = laps.pick_driver("VER").telemetry
        assert isinstance(tel, PolarsTelemetry)
        assert tel.height == 20
        assert tel.driver == "VER"
        assert tel.session is session
        assert tel["LapNumber"].unique().sort().to_list() == [1, 2]
        assert [failure[:2] for failure in session.failures] == [("VER", 3)]

    def test_telemetry_is_memoised_per_refs(self, laps, session):
        ver = laps.pick_driver("VER")
        first = ver.telemetry
        fetches = len(session.fetches)
        assert ver.telemetry is first
        assert len(session.fetches) == fetches

    def test_telemetry_multiple_drivers_raises(self, laps):
        with pytest.raises(ValueError, match="multiple drivers"):
            _ = laps.telemetry

    def test_telemetry_empty_and_detached(self, laps, session):
        empty = laps.filter(pl.col("Driver") == "XXX").f1.as_laps(session).telemetry
        assert isinstance(empty, PolarsTelemetry)
        assert empty.height == 0
        detached = PolarsLaps._from_frame(build_laps_frame(), None).pick_driver("VER").telemetry
        assert detached.height == 0
        assert detached.session is None

    def test_get_telemetry_adds_driver_ahead_and_resamples(self, laps):
        ver = laps.pick_driver("VER").pick_lap(1)
        tel = ver.get_telemetry()
        assert {"DriverAhead", "DistanceToDriverAhead"} <= set(tel.columns)
        assert tel.height == 10
        assert ver.get_telemetry(frequency="original").height == 10
        resampled = ver.get_telemetry(frequency=5)
        assert resampled.height == 5
        assert resampled["Time"].to_list()[1] == seconds(0.2)

    def test_get_telemetry_rejects_bad_frequency(self, laps):
        with pytest.raises(ValueError, match="Invalid telemetry frequency"):
            laps.pick_driver("VER").get_telemetry(frequency=0)

    def test_get_car_and_pos_data(self, laps):
        both = laps.get_car_data()
        assert both.height == 30  # VER 1, 2 and HAM 2
        assert both.driver is None
        assert laps.get_pos_data(pad=1).height == 30
        assert laps.pick_driver("HAM").get_car_data().height == 10
        empty = laps.filter(pl.col("Driver") == "XXX").f1.get_car_data()
        assert empty.height == 0

    def test_f1_telemetry_without_a_session_raises(self, laps, session):
        # A plain frame from filter() has no session: an empty telemetry frame
        # would look like a session without telemetry, so the namespace refuses.
        plain = laps.filter(pl.col("Driver") == "HAM")
        assert type(plain) is pl.DataFrame
        for call in (
            lambda: plain.f1.get_telemetry(),
            lambda: plain.f1.get_car_data(),
            lambda: plain.f1.get_pos_data(),
            lambda: plain.f1.telemetry,
        ):
            with pytest.raises(ValueError, match="needs the session"):
                call()
        assert plain.f1.get_telemetry(session=session).height == 10
        assert plain.f1.as_laps(session).telemetry.height == 10
        # Non-telemetry methods and detached class instances are unchanged.
        assert plain.f1.pick_fastest()["Driver"] == "HAM"
        assert PolarsLaps._from_frame(plain, None).telemetry.height == 0

    def test_get_weather_data_follows_the_session_table(self, laps):
        # Samples every 60 s from 3600 s; laps start at 3605 + 95 i and end
        # 95 s later: the first sample within the lap, else the last before.
        weather = laps.get_weather_data()
        assert type(weather) is pl.DataFrame
        assert weather.columns == ["Time", *WEATHER_COLUMNS]
        assert weather.schema["Time"] == NS
        assert weather["Time"].to_list() == [
            seconds(s) for s in (3660, 3720, 3840, 3900, 3900, 3900)
        ]
        assert weather.height == laps.height

    def test_get_weather_data_without_a_session_table_slices_the_laps(self, laps):
        weather = PolarsLaps._from_frame(laps, None).get_weather_data()
        assert weather["Time"].to_list() == laps["WeatherTime"].to_list()

    def test_get_weather_data_without_columns(self, laps):
        weather = laps.drop("WeatherTime", "AirTemp").f1.get_weather_data()
        assert weather.columns == ["Time", *WEATHER_COLUMNS]
        assert weather.height == laps.height
        assert weather["Time"].null_count() == weather.height
        assert weather["AirTemp"].null_count() == weather.height
        assert weather.schema["AirTemp"] == pl.Float64


# --------------------------------------------------------------------------- #
# PolarsLap
# --------------------------------------------------------------------------- #


class TestPolarsLap:
    def test_scalar_access(self, laps, session):
        lap = laps.pick_driver("VER").pick_fastest()
        assert lap["LapTime"] == seconds(93.5)
        assert lap.LapTime == seconds(93.5)
        assert lap.get("LapNumber") == 2
        assert lap.get("Nope", "default") == "default"
        assert lap.driver == "VER"
        assert lap.lap_number == 2
        assert "LapTime" in lap
        assert "Nope" not in lap
        assert len(lap) == laps.width
        assert list(lap) == list(lap.keys()) == laps.columns
        assert lap.to_dict()["Driver"] == "VER"
        assert lap[["Driver", "LapNumber"]] == {"Driver": "VER", "LapNumber": 2}
        assert dict(lap.items())["Team"] == "Red Bull Racing"
        assert list(lap.values())[1] == "VER"
        assert lap.session is session
        assert isinstance(lap.frame, PolarsLaps)
        assert lap.frame.height == 1
        assert "PolarsLap(name=1" in repr(lap)

    def test_missing_attribute_and_key(self, laps):
        lap = laps.pick_fastest()
        with pytest.raises(AttributeError):
            _ = lap.NotAColumn
        with pytest.raises(KeyError):
            _ = lap["NotAColumn"]
        with pytest.raises(KeyError):
            _ = lap[3]

    def test_one_row_only(self, laps):
        with pytest.raises(ValueError, match="exactly one row"):
            PolarsLap(laps.head(2))
        with pytest.raises(TypeError):
            PolarsLap({"Driver": ["VER"]})

    def test_wraps_plain_frame_with_session(self, laps, session):
        lap = PolarsLap(laps.head(1).base_class_view, session=session, name=0)
        assert isinstance(lap.frame, PolarsLaps)
        assert lap.frame.session is session
        assert lap.name == 0

    def test_telemetry_and_variants(self, laps, session):
        lap = laps.pick_driver("VER").pick_lap(1).pick_fastest(only_by_time=True)
        tel = lap.telemetry
        assert isinstance(tel, PolarsTelemetry)
        assert tel.height == 10
        assert tel.session is session
        assert tel.driver == "VER"
        assert lap.get_car_data(pad=1).height == 10
        assert lap.get_pos_data().height == 10
        assert "DriverAhead" in lap.get_telemetry().columns
        assert lap.get_telemetry(frequency=5).height == 5

    def test_telemetry_failure_is_recorded(self, laps, session):
        lap = laps.pick_driver("VER").pick_lap(3)
        lap = next(iter(lap.iterlaps(require=[]))).lap
        tel = lap.telemetry
        assert tel.height == 0
        assert tel.driver == "VER"
        assert [failure[:2] for failure in session.failures] == [("VER", 3)]

    def test_telemetry_without_session(self, laps):
        lap = PolarsLap(laps.head(1).base_class_view)
        assert lap.telemetry.height == 0

    def test_get_weather_data(self, laps):
        lap = laps.pick_driver("HAM").pick_lap(2).pick_fastest(only_by_time=True)
        weather = lap.get_weather_data()
        assert list(weather) == ["Time", *WEATHER_COLUMNS]
        assert weather["Time"] == seconds(3600 + 60 * 5)  # last sample before 4080 s
        assert weather["AirTemp"] == 25.0
        assert weather["Rainfall"] is False


# --------------------------------------------------------------------------- #
# PolarsTelemetry
# --------------------------------------------------------------------------- #


class TestPolarsTelemetryChannels:
    def test_register_new_channel(self):
        PolarsTelemetry.register_new_channel("Custom", "continuous", "linear")
        try:
            assert PolarsTelemetry._CHANNELS["Custom"] == {
                "type": "continuous",
                "method": "linear",
            }
        finally:
            del PolarsTelemetry._CHANNELS["Custom"]
        with pytest.raises(ValueError, match="Unknown signal type"):
            PolarsTelemetry.register_new_channel("Bad", "fuzzy")
        with pytest.raises(ValueError, match="requires interpolation_method"):
            PolarsTelemetry.register_new_channel("Bad", "continuous")
        assert "Bad" not in PolarsTelemetry._CHANNELS
        assert PolarsTelemetry.TELEMETRY_FREQUENCY == "original"
        assert set(PolarsTelemetry._CHANNELS) >= {"Time", "Speed", "nGear", "DriverAhead"}

    def test_get_first_non_zero_time_index(self, tel):
        assert tel.get_first_non_zero_time_index() == 1
        assert tel.head(1).get_first_non_zero_time_index() == 0
        assert tel.drop("Time").f1.get_first_non_zero_time_index() == 0
        shifted = tel.with_columns(pl.col("Time") + seconds(1)).f1.as_telemetry()
        assert shifted.get_first_non_zero_time_index() == 0

    def test_fill_missing(self, tel):
        gappy = tel.with_columns(
            pl.Series("Speed", [None, 1.0, None, 3.0, float("nan"), 5.0, None, 7.0, None, 9.0]),
            pl.Series("nGear", [None, 2, None, 4, None, None, 7, None, None, None], dtype=pl.Int64),
        )
        filled = PolarsTelemetry._from_frame(gappy, None, "VER").fill_missing()
        assert filled["Speed"].to_list() == [1.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
        assert filled["nGear"].to_list() == [2, 2, 2, 4, 4, 4, 7, 7, 7, 7]
        assert filled.schema["nGear"] == pl.Int64
        assert filled.driver == "VER"

    def test_fill_missing_without_gaps_is_a_copy(self, tel):
        filled = tel.fill_missing()
        assert filled is not tel
        assert filled.equals(tel)

    def test_differential_and_integrated_distance(self, tel):
        diff = tel.calculate_differential_distance()
        assert isinstance(diff, pl.Series)
        assert diff.dtype == pl.Float64
        assert diff[0] == 0.0
        assert diff[1] == pytest.approx(205.0 / 3.6 * 0.1)
        distance = tel.integrate_distance()
        assert distance[-1] == pytest.approx(diff.sum())
        assert tel.head(0).integrate_distance().len() == 0
        assert tel.drop("Speed").f1.calculate_differential_distance().len() == 0

    def test_differential_distance_without_time_is_zero(self, tel):
        no_time = PolarsTelemetry._from_frame(tel.drop("Time"))
        assert no_time.calculate_differential_distance().sum() == 0.0

    def test_add_differential_distance(self, tel):
        with_diff = tel.add_differential_distance()
        assert with_diff["DifferentialDistance"][2] == pytest.approx(210.0 / 3.6 * 0.1)
        assert with_diff.add_differential_distance(drop_existing=False) is with_diff
        again = with_diff.add_differential_distance()
        assert again is not with_diff
        empty = PolarsTelemetry._empty().add_differential_distance()
        assert empty.schema["DifferentialDistance"] == pl.Float64

    def test_add_distance_drop_existing(self, tel):
        # fastf1: drop_existing=False keeps the column, True re-integrates it in place.
        assert tel.add_distance(drop_existing=False) is tel
        recomputed = tel.add_distance()
        assert recomputed.columns == tel.columns
        assert recomputed["Distance"].to_list() == tel.integrate_distance().to_list()
        assert recomputed.driver == "VER"
        appended = tel.drop("Distance").f1.add_distance(driver="VER")
        assert appended.columns[-1] == "Distance"
        assert appended["Distance"][-1] == pytest.approx(tel.integrate_distance()[-1])
        assert appended.driver == "VER"

    def test_add_relative_distance(self, tel):
        assert tel.add_relative_distance(drop_existing=False) is tel
        rel = tel.with_columns(pl.col("Distance") * 2).f1.add_relative_distance()
        assert rel["RelativeDistance"].to_list() == pytest.approx(
            (tel["Distance"] / tel["Distance"][-1]).to_list()
        )
        appended = tel.drop("RelativeDistance").f1.add_relative_distance()
        assert appended["RelativeDistance"][-1] == 1.0
        assert appended["RelativeDistance"][0] == 0.0
        zero = tel.drop("RelativeDistance").with_columns(pl.lit(0.0).alias("Distance"))
        assert zero.f1.add_relative_distance()["RelativeDistance"].is_nan().all()  # 0 / 0
        fresh = tel.drop("Distance", "RelativeDistance").f1.add_relative_distance()
        assert "Distance" not in fresh.columns  # fastf1 integrates without adding it
        assert fresh["RelativeDistance"][-1] == 1.0

    def test_calculate_driver_ahead(self, tel):
        ahead, distance = tel.calculate_driver_ahead()
        assert isinstance(ahead, pl.Series)
        assert distance.dtype == pl.Float64
        assert distance.len() == 10
        ahead, distance, reference = tel.calculate_driver_ahead(return_reference=True)
        assert reference is tel
        bare = PolarsTelemetry._from_frame(tel.drop("DriverAhead", "DistanceToDriverAhead"))
        ahead, distance = bare.calculate_driver_ahead()
        assert ahead.null_count() == 10
        assert distance.is_nan().all()

    def test_add_driver_ahead(self, tel):
        assert tel.add_driver_ahead() is tel
        bare = tel.drop("DriverAhead", "DistanceToDriverAhead").f1.add_driver_ahead()
        assert bare.schema["DriverAhead"] == pl.String
        assert bare.schema["DistanceToDriverAhead"] == pl.Float64
        assert bare.add_driver_ahead(drop_existing=False) is bare

    def test_add_track_status_maps_session_table(self, tel):
        statuses = tel.add_track_status()["TrackStatus"].to_list()
        # Yellow between 3605.35 and 3605.75 session time: samples 4..7.
        assert statuses == ["1", "1", "1", "1", "2", "2", "2", "2", "1", "1"]
        assert tel.add_track_status().schema["TrackStatus"] == pl.String

    def test_add_track_status_falls_back_to_green(self, tel, session):
        detached = PolarsTelemetry._from_frame(tel, None, "VER")
        assert set(detached.add_track_status()["TrackStatus"].to_list()) == {"1"}
        by_time = tel.drop("SessionTime").f1.add_track_status(session=session)
        assert set(by_time["TrackStatus"].to_list()) == {"1"}  # lap-relative Time < 3605.35

    def test_add_track_status_drop_existing(self, tel):
        tagged = tel.add_track_status()
        assert tagged.add_track_status(drop_existing=False) is tagged
        assert tagged.add_track_status() is not tagged


class TestPolarsTelemetrySlicing:
    def test_slice_by_mask(self, tel):
        mask = [i % 2 == 0 for i in range(10)]
        assert tel.slice_by_mask(mask).height == 5
        assert tel.slice_by_mask(pl.Series(mask)).height == 5
        padded = tel.slice_by_mask(
            [False, False, False, True, False, False, False, False, False, False], pad=1
        )
        assert padded["Time"].to_list() == [seconds(0.2), seconds(0.3), seconds(0.4)]
        before = tel.slice_by_mask(
            [False, False, False, True] + [False] * 6, pad=2, pad_side="before"
        )
        assert before["Time"][0] == seconds(0.1)
        assert before.height == 3
        after = tel.slice_by_mask(
            [False, False, False, True] + [False] * 6, pad=2, pad_side="after"
        )
        assert after.height == 3
        assert after["Time"][-1] == seconds(0.5)

    def test_slice_by_mask_rejects_wrong_length(self, tel):
        with pytest.raises(ValueError, match="Mask length"):
            tel.slice_by_mask([True])

    def test_slice_by_time_uses_session_time_and_zero_bases_time(self, tel):
        part = tel.slice_by_time(seconds(3605.2), seconds(3605.5))
        assert part.height == 4
        assert part["Time"].to_list() == [seconds(0), seconds(0.1), seconds(0.2), seconds(0.3)]
        assert part["SessionTime"][0] == seconds(3605.2)
        assert part.schema["Time"] == NS
        assert part.driver == "VER"

    def test_slice_by_time_accepts_numbers_and_strings(self, tel):
        assert tel.slice_by_time(3605.2, 3605.5).height == 4
        assert tel.slice_by_time("1:00:05.2", "1:00:05.5").height == 4
        assert tel.slice_by_time(3605.2, 3605.5, pad=1).height == 6
        assert tel.slice_by_time(float("nan"), 3605.5).height == 0
        assert tel.slice_by_time(None, 3605.5).height == 0

    def test_slice_by_time_falls_back_to_time(self, tel):
        lap_relative = tel.drop("SessionTime").f1.as_telemetry()
        assert lap_relative.slice_by_time(0.2, 0.5).height == 4
        no_time = PolarsTelemetry._from_frame(tel.drop("SessionTime", "Time"))
        assert no_time.slice_by_time(0, 1).height == 10

    def test_slice_by_time_interpolate_edges(self, tel):
        part = tel.slice_by_time(3605.25, 3605.45, interpolate_edges=True)
        assert part["SessionTime"].to_list() == [
            seconds(3605.25),
            seconds(3605.3),
            seconds(3605.4),
            seconds(3605.45),
        ]
        assert part["Speed"].to_list() == [212.5, 215.0, 220.0, 222.5]
        assert part["Time"][0] == seconds(0)
        assert part["Date"][0] == T0 + seconds(5.25)
        assert part["nGear"].to_list() == [7, 7, 7, 7]
        assert part["Driver"].to_list() == ["VER"] * 4
        assert part.schema["Brake"] == pl.Boolean
        exact = tel.slice_by_time(3605.2, 3605.5, interpolate_edges=True)
        assert exact.height == 4  # existing edge samples are not duplicated

    def test_slice_by_lap_with_session_time(self, session, laps):
        both = pl.concat(
            [build_telemetry_frame("VER", 1), build_telemetry_frame("VER", 2, start=3700.0)]
        )
        tel = PolarsTelemetry._from_frame(both, session, "VER")
        ver = laps.pick_driver("VER")
        lap2 = tel.slice_by_lap(ver.pick_lap(2).pick_fastest())
        assert lap2.height == 10
        assert lap2["LapNumber"].unique().to_list() == [2]
        assert lap2["Time"][0] == seconds(0)
        assert tel.slice_by_lap(ver.pick_laps([1, 2])).height == 20
        assert tel.slice_by_lap({"LapStartTime": seconds(3700), "Time": seconds(3701)}).height == 10

    def test_slice_by_lap_without_session_time_matches_lap_number(self, session, laps):
        both = pl.concat(
            [build_telemetry_frame("VER", 1), build_telemetry_frame("HAM", 2, start=3700.0)]
        ).drop("SessionTime")
        tel = PolarsTelemetry._from_frame(both, session)
        assert tel.slice_by_lap(2).height == 10
        assert tel.slice_by_lap(laps.pick_driver("HAM").pick_lap(2)).height == 10
        assert tel.slice_by_lap(laps.pick_driver("VER").pick_lap(2)).height == 0
        assert tel.slice_by_lap({"LapNumber": 1, "Driver": "VER"}).height == 10
        assert tel.slice_by_lap(7).height == 0

    def test_slice_by_lap_rejects_multiple_drivers_and_bad_input(self, tel, laps):
        with pytest.raises(ValueError, match="multiple drivers"):
            tel.slice_by_lap(laps)
        with pytest.raises(TypeError, match="ref_laps"):
            tel.slice_by_lap("VER")

    def test_slice_by_lap_without_any_reference(self, tel):
        bare = PolarsTelemetry._from_frame(tel.drop("SessionTime", "LapNumber"))
        assert bare.slice_by_lap(3).height == 10
        only_number = PolarsTelemetry._from_frame(tel.drop("SessionTime"))
        assert only_number.slice_by_lap({"Driver": "VER"}).height == 0


class TestPolarsTelemetryMergeResample:
    def test_merge_channels_nearest_on_time(self, tel, session):
        other = pl.DataFrame(
            {"Time": [seconds(0.05), seconds(0.56)], "Extra": [1.0, 2.0], "Speed": [0.0, 0.0]}
        ).with_columns(pl.col("Time").cast(NS))
        merged = tel.merge_channels(other)
        assert isinstance(merged, PolarsTelemetry)
        assert merged.session is session
        assert merged.height == 10
        assert merged["Extra"].to_list() == [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0]
        assert merged["Speed"][0] == 200.0
        assert merged["Speed_other"][0] == 0.0

    def test_merge_channels_unsorted_and_other_units(self, tel):
        other = pl.DataFrame(
            {"Time": [seconds(0.56), seconds(0.05)], "Extra": [2.0, 1.0]}
        ).with_columns(pl.col("Time").cast(pl.Duration("ms")))
        merged = tel.reverse().merge_channels(other)
        assert merged["Extra"].to_list() == [1.0] * 4 + [2.0] * 6
        assert merged["Time"].is_sorted()

    def test_merge_channels_without_time_concatenates(self, tel):
        other = pl.DataFrame({"Extra": list(range(10)), "Speed": [0.0] * 10})
        merged = tel.merge_channels(other)
        assert merged.width == tel.width + 2
        assert "Speed_other" in merged.columns
        with pytest.raises(TypeError):
            tel.merge_channels({"Time": []})

    def test_merge_channels_with_frequency(self, tel):
        other = pl.DataFrame({"Time": [seconds(0.05)], "Extra": [1.0]}).with_columns(
            pl.col("Time").cast(NS)
        )
        assert tel.merge_channels(other, frequency=5).height == 5
        assert tel.merge_channels(other, frequency="original").height == 10

    @pytest.mark.parametrize(
        ("rule", "expected"),
        [("200ms", 5), ("1s", 1), ("1S", 1), ("500L", 2), ("0.5s", 2), ("1T", 1), ("1H", 1)],
    )
    def test_resample_channels_rules(self, tel, rule, expected):
        resampled = tel.resample_channels(rule)
        assert resampled.height == expected
        assert resampled.columns == tel.columns
        assert resampled.schema["Time"] == NS

    def test_resample_channels_values(self, tel):
        resampled = tel.resample_channels("200ms")
        assert resampled["Time"].to_list() == [seconds(0.2 * i) for i in range(5)]
        assert resampled["Speed"].to_list() == [202.5, 212.5, 222.5, 232.5, 242.5]
        assert resampled["SessionTime"].to_list() == [seconds(3605 + 0.2 * i) for i in range(5)]
        assert resampled["Date"][1] == T0 + seconds(5.2)
        assert resampled["Brake"].to_list() == [False] * 5
        assert resampled.schema["Brake"] == pl.Boolean
        assert resampled["Driver"].to_list() == ["VER"] * 5
        assert resampled.driver == "VER"

    def test_resample_channels_upsamples_across_empty_buckets(self, tel):
        sparse = PolarsTelemetry._from_frame(tel.gather_every(3), None, "VER")  # 0, .3, .6, .9
        resampled = sparse.resample_channels("100ms")
        assert resampled.height == 10
        assert resampled["Speed"].to_list() == pytest.approx([200.0 + 5 * i for i in range(10)])

    def test_resample_channels_argument_validation(self, tel):
        with pytest.raises(ValueError, match="only specify one"):
            tel.resample_channels("1s", new_date_ref=tel["Date"])
        # Neither argument: tif1's "1s" default, as on the pandas class.
        assert tel.resample_channels().equals(tel.resample_channels("1s"))
        with pytest.raises(ValueError, match="Invalid resampling rule"):
            tel.resample_channels("??")
        with pytest.raises(ValueError, match="Invalid resampling rule"):
            tel.resample_channels(rule=object())
        with pytest.raises(ValueError, match="Unknown unit"):
            tel.resample_channels("1fortnight")
        with pytest.raises(ValueError, match="Unknown unit"):
            tel.resample_channels("soon")
        with pytest.raises(ValueError, match="positive"):
            tel.resample_channels("0s")

    def test_resample_channels_edge_cases(self, tel):
        no_time = PolarsTelemetry._from_frame(tel.drop("Time"))
        by_session_time = no_time.resample_channels("1s")
        assert by_session_time.height == 1  # SessionTime is the axis
        assert "Time" not in by_session_time.columns
        no_axis = PolarsTelemetry._from_frame(tel.drop("Time", "SessionTime", "Date"))
        assert no_axis.resample_channels("1s").height == 10
        assert tel.head(0).resample_channels("1s").height == 0
        assert tel.resample_channels(seconds(0.2)).height == 5
        assert tel.resample_channels(0.5).height == 2

    def test_resample_channels_new_date_ref(self, tel):
        dates = pl.Series([T0 + seconds(5.15), T0 + seconds(5.2), T0 + seconds(5.55)]).cast(DT_NS)
        resampled = tel.resample_channels(new_date_ref=dates)
        assert resampled.height == 3
        assert resampled["Date"].to_list() == dates.to_list()
        assert resampled["Speed"].to_list() == [207.5, 210.0, 227.5]
        assert resampled["Time"].to_list() == [seconds(0.15), seconds(0.2), seconds(0.55)]
        assert resampled["nGear"].to_list() == [7, 7, 7]
        assert resampled.columns == tel.columns
        assert tel.resample_channels(new_date_ref=list(dates)).height == 3

    def test_resample_channels_new_date_ref_requires_date(self, tel):
        with pytest.raises(ValueError, match="'Date' is required"):
            tel.drop("Date").f1.resample_channels(new_date_ref=tel["Date"])


# --------------------------------------------------------------------------- #
# Results, driver results and drivers
# --------------------------------------------------------------------------- #


class TestPolarsSessionResults:
    def test_row_for_identifiers(self, session):
        results = session.results
        assert results.row_for(1).Abbreviation == "VER"
        assert results.row_for("44").Abbreviation == "HAM"
        assert results.row_for("ham").FullName == "Lewis Hamilton"
        assert results.row_for("Lewis Hamilton").DriverNumber == "44"
        assert results.row_for("M VERSTAPPEN").DriverNumber == "1"
        assert results.row_for("2/MOS").Abbreviation == "MOS"
        assert results.get_driver("MOS").name == "2/MOS"
        row = results.row_for("VER")
        assert isinstance(row, PolarsDriverResult)
        assert row.session is session
        assert row.name == "1"  # the pandas index label (fastf1's DriverResult.name)
        assert row["Points"] == 25.0
        assert row.Time == seconds(5400.0)

    def test_row_for_unknown_raises(self, session):
        with pytest.raises(DriverNotFoundError, match="'99'"):
            session.results.row_for(99)
        with pytest.raises(DriverNotFoundError):
            session.results.drop(
                "DriverNumber", "Abbreviation", "FullName", "BroadcastName"
            ).f1.row_for("VER")

    def test_results_keep_class_through_row_operations(self, session):
        head = session.results.head(2)
        assert isinstance(head, PolarsSessionResults)
        assert head.session is session
        assert head.row_for("HAM").Abbreviation == "HAM"

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            ("Finished", False),
            ("+1 Lap", False),
            ("+2 Laps", False),
            ("Lapped", False),
            ("finished", False),
            ("Accident", True),
            ("Not classified", True),
            ("", False),
            ("   ", False),
            (None, False),
        ],
    )
    def test_dnf(self, status, expected):
        frame = pl.DataFrame({"DriverNumber": ["1"], "Status": [status]})
        assert PolarsDriverResult(frame).dnf is expected

    def test_dnf_without_status_column(self):
        assert PolarsDriverResult(pl.DataFrame({"DriverNumber": ["1"]})).dnf is False


class TestPolarsDriver:
    def test_from_session_results_row(self, session):
        driver = PolarsDriver.from_session(session, "ver")
        assert isinstance(driver, PolarsDriverResult)
        assert driver.driver == "VER"
        assert driver.driver_number == "1"
        assert driver.FullName == "Max Verstappen"
        assert driver.name == "1"
        assert driver.session is session
        assert driver.dnf is False
        assert PolarsDriver.from_session(session, "2/MOS").dnf is True

    def test_from_session_driver_info_fallback(self, session):
        driver = PolarsDriver.from_session(session, "NOR")
        assert driver.driver == "NOR"
        assert driver.driver_number == "4"
        assert driver.FullName == "Lando Norris"
        assert driver.TeamName == "McLaren"
        assert driver.laps.height == 0

    def test_constructor_defaults_driver_to_abbreviation(self, session):
        driver = PolarsDriver(session.results.slice(1, 1), session=session)
        assert driver.driver == "HAM"
        explicit = PolarsDriver(session.results.slice(1, 1), session=session, driver="XXX")
        assert explicit.driver == "XXX"

    def test_laps_are_memoised_polars_laps(self, session):
        driver = PolarsDriver.from_session(session, "VER")
        laps = driver.laps
        assert isinstance(laps, PolarsLaps)
        assert laps.session is session
        assert laps["Driver"].unique().to_list() == ["VER"]
        assert driver.laps is laps

    def test_laps_from_plain_session_frame(self, session):
        session.laps = build_laps_frame()
        driver = PolarsDriver.from_session(session, "HAM")
        assert isinstance(driver.laps, PolarsLaps)
        assert driver.laps.height == 3
        session.laps = None
        assert PolarsDriver.from_session(session, "HAM").laps.height == 0

    def test_get_lap(self, session):
        driver = PolarsDriver.from_session(session, "VER")
        lap = driver.get_lap(2)
        assert isinstance(lap, PolarsLap)
        assert lap.lap_number == 2
        assert lap.name == 1
        assert lap.session is session
        with pytest.raises(LapNotFoundError) as info:
            driver.get_lap(9)
        assert info.value.context["driver"] == "VER"
        assert info.value.context["year"] == 2024
        with pytest.raises(ValueError, match="positive integer"):
            driver.get_lap(0)

    def test_fastest_lap_and_frame(self, session):
        driver = PolarsDriver.from_session(session, "HAM")
        assert driver.fastest_lap.lap_number == 3  # lap 2 is deleted
        frame = driver.get_fastest_lap()
        assert isinstance(frame, PolarsLaps)
        assert frame.height == 1
        assert frame["LapNumber"][0] == 3
        none = PolarsDriver.from_session(session, "NOR")
        assert none.fastest_lap is None
        empty = none.get_fastest_lap()
        assert isinstance(empty, PolarsLaps)
        assert empty.height == 0

    def test_telemetry_surfaces(self, session):
        driver = PolarsDriver.from_session(session, "VER")
        assert driver.telemetry.height == 20
        # Laps 1 and 2 sit 95 s apart on the SessionTime axis: one uniform
        # 5 Hz grid from 3605.0 to 3700.8 (interpolated across the gap).
        resampled = driver.get_telemetry(frequency=5)
        assert resampled.height == 480
        assert resampled["Time"][0] == seconds(0)
        assert resampled["Time"][-1] == seconds(95.8)
        assert resampled["SessionTime"][-1] == seconds(3700.8)
        assert "DriverAhead" in driver.get_telemetry().columns
        fastest_tel = driver.get_fastest_lap_tel()
        assert fastest_tel.height == 10
        assert fastest_tel["LapNumber"].unique().to_list() == [2]
        assert PolarsDriver.from_session(session, "NOR").get_fastest_lap_tel().height == 0


# --------------------------------------------------------------------------- #
# ``df.f1`` namespace
# --------------------------------------------------------------------------- #


class TestNamespace:
    def test_namespace_is_registered(self, laps):
        assert isinstance(laps.f1, F1Namespace)
        assert isinstance(pl.DataFrame().f1, F1Namespace)

    def test_session_resolution(self, laps, session):
        plain = laps.filter(pl.col("Driver") == "VER")
        assert plain.f1.session is None
        assert laps.f1.session is session
        assert plain.f1.as_laps(session).session is session
        plain._tif1_session = session
        assert plain.f1.session is session
        assert plain.f1.pick_fastest().session is session

    def test_methods_after_native_filter(self, laps, session):
        plain = laps.filter(pl.col("Driver") == "VER")
        fastest = plain.f1.pick_fastest(session=session)
        assert isinstance(fastest, PolarsLap)
        assert fastest.session is session
        tel = fastest.get_telemetry()
        assert isinstance(tel, PolarsTelemetry)
        assert tel.session is session
        picked = plain.f1.pick_laps([1, 2])
        assert isinstance(picked, PolarsLaps)
        assert picked.height == 2
        assert plain.f1.pick_quicklaps(session=session).session is session

    def test_as_wrappers_reuse_instances(self, laps, tel, session):
        assert laps.f1.as_laps() is laps
        assert laps.f1.as_laps(session) is laps
        assert tel.f1.as_telemetry() is tel
        assert tel.f1.as_telemetry(driver="HAM").driver == "HAM"
        assert session.results.f1.as_results() is session.results
        other = FakeSession()
        assert laps.f1.as_laps(other).session is other

    def test_telemetry_dispatch_with_driver(self, tel, session):
        plain = tel.filter(pl.col("Time") >= seconds(0.3))
        assert type(plain) is pl.DataFrame
        sliced = plain.f1.slice_by_time(3605.4, 3605.6, session=session, driver="VER")
        assert isinstance(sliced, PolarsTelemetry)
        assert sliced.height == 3
        assert sliced.session is session
        assert sliced.driver == "VER"
        assert plain.f1.add_distance().driver is None

    def test_property_and_results_dispatch(self, laps, session):
        plain = laps.filter(pl.col("Driver") == "HAM")
        plain._tif1_session = session
        assert plain.f1.telemetry.height == 10
        results = session.results.filter(pl.col("Position") <= 2)
        assert results.f1.row_for("HAM", session=session).session is session
        assert results.f1.get_driver(44).Abbreviation == "HAM"
        assert type(results.f1.base_class_view) is pl.DataFrame

    def test_class_level_names(self, laps):
        assert laps.f1.register_new_channel == PolarsTelemetry.register_new_channel

    def test_unknown_names_raise(self, laps):
        with pytest.raises(AttributeError):
            _ = laps.f1.not_a_method
        with pytest.raises(AttributeError):
            _ = laps.f1.TELEMETRY_FREQUENCY
        with pytest.raises(AttributeError):
            _ = laps.f1.filter


# --------------------------------------------------------------------------- #
# Helpers and import guard
# --------------------------------------------------------------------------- #


class TestHelpers:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, None),
            (seconds(1.5), 1_500_000_000),
            (2, 2_000_000_000),
            (0.25, 250_000_000),
            (float("nan"), None),
            ("1:23.456", 83_456_000_000),
            ("0 days 00:01:23.456000", 83_456_000_000),
            ("-0:00:01", -1_000_000_000),
            ("83.5", 83_500_000_000),
            ("soon", None),
            (True, None),
        ],
    )
    def test_to_ns(self, value, expected):
        assert _to_ns(value) == expected

    def test_to_ns_duck_types(self):
        class Timedelta:
            value = 42

            def total_seconds(self):
                return 0.0

        class Seconds:
            def total_seconds(self):
                return 1.5

        class NaT:
            def total_seconds(self):
                return float("nan")

        assert _to_ns(Timedelta()) == 42
        assert _to_ns(Seconds()) == 1_500_000_000
        assert _to_ns(NaT()) is None
        assert _to_ns(object()) is None

    @pytest.mark.parametrize(
        ("rule", "expected"),
        [
            ("1s", 1_000_000_000),
            ("1S", 1_000_000_000),
            ("100ms", 100_000_000),
            ("500L", 500_000_000),
            ("1min", 60_000_000_000),
            ("1T", 60_000_000_000),
            ("1H", 3_600_000_000_000),
            ("2h", 7_200_000_000_000),
            ("250us", 250_000),
            ("5ns", 5),
            ("ms", 1_000_000),
            (seconds(0.2), 200_000_000),
        ],
    )
    def test_rule_to_ns(self, rule, expected):
        assert _rule_to_ns(rule) == expected


def test_module_never_imports_pandas():
    code = (
        "import sys; import tif1.models_polars; "
        "print('pandas' in sys.modules, 'polars' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], check=True, capture_output=True, text=True
    ).stdout.strip()
    assert out == "False True"
