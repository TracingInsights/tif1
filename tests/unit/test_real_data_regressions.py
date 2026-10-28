"""Regressions found by running the documentation against real 2025 Bahrain data.

Each test pins a behaviour that was wrong on real payloads (both backends):
"None" sentinels in telemetry stringified numeric channels, ``pick_driver``
ignored driver numbers, ``slice_by_lap`` matched a session-time window against
tif1's lap-relative ``Time``, ``resample_channels`` defaulted to an offset
alias pandas 3 removed, ``delta_time`` was a stub, ``display_lap_info`` read a
slot the ``Lap`` series has no copy of, and session names were case-sensitive.
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pandas as pd
import pytest

from tif1 import core
from tif1.cache import _FRAME_TIER_SCHEMA_VERSION, Cache
from tif1.core_utils.helpers import _create_telemetry_df, _telemetry_frame_from_merged
from tif1.jupyter import display_lap_info
from tif1.models import Lap, Laps, Telemetry
from tif1.utils import delta_time

# A leader's lap: nobody ahead, so upstream writes the string "None" for the
# whole DistanceToDriverAhead channel; a lap start writes it for a few samples.
_LEADER_PAYLOAD: dict[str, list[Any]] = {
    "time": [0.0, 0.1, 0.2],
    "speed": [300.0, 301.0, 302.0],
    "gear": [8, 8, 8],
    "brake": [0, 0, 0],
    "drs": [1, 1, 1],
    "driver_ahead": ["None", "None", "None"],
    "distance_to_driver_ahead": ["None", "None", "None"],
    "x": [1.0, 2.0, 3.0],
}
_LAP_START_PAYLOAD: dict[str, list[Any]] = {
    "time": [0.0, 0.1, 0.2],
    "speed": [300.0, 301.0, 302.0],
    "driver_ahead": ["None", "12", "12"],
    "distance_to_driver_ahead": [101.165, 101.2, "None"],
}


class TestTelemetrySentinels:
    def test_pandas_leader_lap_channels_are_typed(self):
        frame = cast(pd.DataFrame, _create_telemetry_df(_LEADER_PAYLOAD, "PIA", 36, "pandas"))
        assert frame["DistanceToDriverAhead"].dtype == "float64"
        assert frame["DistanceToDriverAhead"].isna().all()
        assert frame["DriverAhead"].isna().all()
        assert not (frame["DriverAhead"].astype(str) == "None").any()
        # Untouched channels keep their contract.
        assert frame["Speed"].dtype == "float64"
        assert frame["Brake"].dtype == bool
        assert frame["nGear"].dtype.name == "Int64"

    def test_pandas_mixed_channel_keeps_values_and_nulls_sentinels(self):
        frame = cast(pd.DataFrame, _create_telemetry_df(_LAP_START_PAYLOAD, "VER", 10, "pandas"))
        dist = frame["DistanceToDriverAhead"]
        assert dist.dtype == "float64"
        assert dist.tolist()[:2] == [101.165, 101.2]
        assert np.isnan(dist.iloc[2])
        ahead = frame["DriverAhead"]
        assert pd.isna(ahead.iloc[0])
        assert ahead.tolist()[1:] == ["12", "12"]
        assert ahead.notna().sum() == 2

    def test_polars_leader_lap_channels_are_typed(self):
        pl = pytest.importorskip("polars")
        frame = cast(Any, _create_telemetry_df(_LEADER_PAYLOAD, "PIA", 36, "polars"))
        assert frame.schema["DistanceToDriverAhead"] == pl.Float64
        assert frame["DistanceToDriverAhead"].null_count() == 3
        assert frame.schema["DriverAhead"] == pl.String
        assert frame["DriverAhead"].null_count() == 3

    def test_polars_mixed_channel_keeps_values_and_nulls_sentinels(self):
        pl = pytest.importorskip("polars")
        frame = cast(Any, _create_telemetry_df(_LAP_START_PAYLOAD, "VER", 10, "polars"))
        assert frame.schema["DistanceToDriverAhead"] == pl.Float64
        assert frame["DistanceToDriverAhead"].to_list()[:2] == [101.165, 101.2]
        assert frame["DistanceToDriverAhead"][2] is None
        assert frame["DriverAhead"].to_list() == [None, "12", "12"]

    def test_merged_dict_build_applies_the_same_rules(self):
        merged = {
            "Time": [0.0, 0.1, 0.0, 0.1],
            "Speed": [300.0, 301.0, 290.0, 291.0],
            "DriverAhead": ["None", "12", "4", "4"],
            "DistanceToDriverAhead": ["None", "None", 50.5, 51.0],
            "Driver": ["PIA", "PIA", "VER", "VER"],
            "LapNumber": [36, 36, 10, 10],
        }
        frame = _telemetry_frame_from_merged(merged)
        assert frame["DistanceToDriverAhead"].dtype == "float64"
        assert frame["DistanceToDriverAhead"].isna().tolist() == [True, True, False, False]
        assert pd.isna(frame["DriverAhead"].iloc[0])
        assert frame["DriverAhead"].tolist()[1:] == ["12", "4", "4"]

    def test_clean_payload_dtypes_unchanged(self):
        payload = {
            "time": [0.0, 0.1],
            "speed": [300, 301],
            "rpm": [11000.5, 11001.5],
            "driver_ahead": ["12", "12"],
            "distance_to_driver_ahead": [10.0, 11.0],
        }
        frame = cast(pd.DataFrame, _create_telemetry_df(payload, "VER", 1, "pandas"))
        assert frame["Speed"].dtype == "int64"
        assert frame["RPM"].dtype == "float64"
        assert frame["DistanceToDriverAhead"].dtype == "float64"
        assert frame["DriverAhead"].tolist() == ["12", "12"]


class TestLapsPickDriverNumbers:
    @pytest.fixture
    def laps(self) -> Laps:
        return Laps(
            pd.DataFrame(
                {
                    "Driver": ["VER", "VER", "NOR"],
                    "DriverNumber": ["1", "1", "4"],
                    "LapNumber": [1, 2, 1],
                }
            )
        )

    def test_driver_number_int(self, laps):
        assert len(laps.pick_driver(1)) == 2

    def test_driver_number_str(self, laps):
        assert len(laps.pick_driver("4")) == 1

    def test_mixed_numbers_and_codes(self, laps):
        assert len(laps.pick_drivers([1, "NOR"])) == 3

    def test_numeric_driver_number_column(self):
        laps = Laps(pd.DataFrame({"Driver": ["VER", "NOR"], "DriverNumber": [1, 4]}))
        assert laps.pick_driver(4)["Driver"].tolist() == ["NOR"]

    def test_unknown_number_is_empty(self, laps):
        assert laps.pick_driver(99).empty

    def test_object_column_of_ints(self):
        numbers = pd.Series([1, 4], dtype=object)
        laps = Laps(pd.DataFrame({"Driver": ["VER", "NOR"], "DriverNumber": numbers}))
        assert laps.pick_driver(1)["Driver"].tolist() == ["VER"]


class TestSliceByLapLapRelativeTime:
    @pytest.fixture
    def tel(self) -> Telemetry:
        # tif1 telemetry: Time restarts at zero on every lap, no SessionTime.
        return Telemetry(
            pd.DataFrame(
                {
                    "Time": pd.to_timedelta([0.0, 0.5, 1.0, 0.0, 0.5, 0.0, 0.5], unit="s"),
                    "Speed": [100, 110, 120, 130, 140, 150, 160],
                    "Driver": ["VER", "VER", "VER", "VER", "VER", "NOR", "NOR"],
                    "LapNumber": [10, 10, 10, 11, 11, 10, 10],
                }
            )
        )

    def test_lap_series_with_session_time_window(self, tel):
        lap = Lap(
            pd.Series(
                {
                    "Driver": "VER",
                    "LapNumber": 10,
                    "LapStartTime": pd.Timedelta(hours=1, minutes=10),
                    "Time": pd.Timedelta(hours=1, minutes=12),
                }
            )
        )
        sliced = tel.slice_by_lap(lap)
        assert sliced["Speed"].tolist() == [100, 110, 120]

    def test_laps_frame_selects_every_lap_of_that_driver(self, tel):
        ref = Laps(
            pd.DataFrame(
                {
                    "Driver": ["VER", "VER"],
                    "LapNumber": [10, 11],
                    "LapStartTime": [pd.Timedelta(hours=1), pd.Timedelta(hours=1, minutes=2)],
                    "Time": [pd.Timedelta(hours=1, minutes=2), pd.Timedelta(hours=1, minutes=4)],
                }
            )
        )
        assert tel.slice_by_lap(ref)["Speed"].tolist() == [100, 110, 120, 130, 140]

    def test_driver_filter_on_multi_driver_frame(self, tel):
        nor_lap = Lap(pd.Series({"Driver": "NOR", "LapNumber": 10}))
        assert tel.slice_by_lap(nor_lap)["Speed"].tolist() == [150, 160]

    def test_session_time_frames_still_use_the_window(self):
        tel = Telemetry(
            pd.DataFrame(
                {
                    "SessionTime": [0.0, 1.0, 2.0, 3.0],
                    "Time": [0.0, 1.0, 2.0, 3.0],
                    "Speed": [1, 2, 3, 4],
                    "LapNumber": [1, 1, 2, 2],
                }
            )
        )
        ref = Laps(
            pd.DataFrame(
                {
                    "LapStartTime": [pd.Timedelta(seconds=2)],
                    "Time": [pd.Timedelta(seconds=3)],
                    "LapNumber": [9],  # window wins over the (wrong) lap number
                }
            )
        )
        assert tel.slice_by_lap(ref)["Speed"].tolist() == [3, 4]


class TestResampleChannelsAliases:
    @pytest.fixture
    def tel(self) -> Telemetry:
        return Telemetry(
            pd.DataFrame(
                {
                    "Time": pd.to_timedelta(np.arange(0, 3.0, 0.25), unit="s"),
                    "Speed": np.arange(12, dtype=float),
                }
            )
        )

    def test_default_rule_is_valid_on_pandas_3(self, tel):
        assert len(tel.resample_channels()) == 3

    @pytest.mark.parametrize(
        ("legacy", "modern"), [("1S", "1s"), ("500L", "500ms"), ("1T", "1min")]
    )
    def test_legacy_upper_case_aliases_are_translated(self, tel, legacy, modern):
        pd.testing.assert_frame_equal(
            pd.DataFrame(tel.resample_channels(legacy)), pd.DataFrame(tel.resample_channels(modern))
        )

    def test_modern_rule_untouched(self, tel):
        assert len(tel.resample_channels("500ms")) == 6


class TestDeltaTime:
    @staticmethod
    def _tel(seconds_per_metre: float, n: int = 11) -> Telemetry:
        distance = np.linspace(0.0, 1000.0, n)
        return Telemetry(
            pd.DataFrame(
                {
                    "Time": pd.to_timedelta(distance * seconds_per_metre, unit="s"),
                    "Distance": distance,
                    "Speed": np.full(n, 200.0),
                }
            )
        )

    def test_slower_comparison_lap_has_positive_growing_delta(self):
        ref = SimpleNamespace(get_car_data=lambda: self._tel(0.09))
        comp = SimpleNamespace(get_car_data=lambda: self._tel(0.10))
        delta, ref_tel, comp_tel = delta_time(ref, comp)
        assert isinstance(delta, pd.Series)
        assert len(delta) == len(ref_tel)
        assert delta.iloc[0] == pytest.approx(0.0)
        assert delta.iloc[-1] == pytest.approx(10.0)  # 1000 m * 0.01 s/m
        assert (np.diff(delta.to_numpy()) > 0).all()
        assert list(comp_tel.columns) == ["Time", "Distance", "Speed"]

    def test_frames_accepted_directly_and_distance_added(self):
        ref = self._tel(0.09)
        comp = self._tel(0.10).drop(columns=["Distance"])
        delta, _, comp_tel = delta_time(ref, comp)
        assert "Distance" in comp_tel.columns
        assert len(delta) == len(ref)

    def test_empty_telemetry_returns_empty_series(self):
        lap = cast(Any, pd.Series(dtype=object))
        lap.get_telemetry = lambda: Telemetry()
        delta, _, _ = delta_time(lap, lap)
        assert isinstance(delta, pd.Series)
        assert delta.empty


class TestDisplayLapInfoLapSeries:
    def test_lap_series_from_get_lap_renders(self):
        session = SimpleNamespace(
            year=2025,
            gp="Bahrain Grand Prix",
            session="Race",
            _get_telemetry_payload=lambda driver, _lap: {"speed": [1]} if driver == "VER" else None,
        )
        lap = Lap(pd.Series({"Driver": "VER", "LapNumber": 10}), session=session)
        html = display_lap_info(lap)
        assert "Lap 10" in html
        assert "VER" in html
        assert "Yes" in html

    def test_lap_series_without_memoized_telemetry_says_no(self):
        session = SimpleNamespace(
            year=2025,
            gp="Bahrain Grand Prix",
            session="Race",
            _get_telemetry_payload=lambda _driver, _lap: None,
        )
        lap = Lap(pd.Series({"Driver": "VER", "LapNumber": 10}), session=session)
        assert "No" in display_lap_info(lap)


class TestSessionNameCase:
    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            ("race", "Race"),
            ("RACE", "Race"),
            ("qualifying", "Qualifying"),
            ("practice 1", "Practice 1"),
            ("  Sprint   Qualifying ", "Sprint Qualifying"),
            ("fp1", "Practice 1"),
            ("r", "Race"),
            ("Unknown", "Unknown"),
        ],
    )
    def test_normalized(self, given, expected):
        assert core._normalize_session_name(given) == expected


class TestFrameTierSchemaVersion:
    def test_old_frame_tables_are_dropped_once(self, tmp_path):
        db = tmp_path / "cache.sqlite"
        conn = sqlite3.connect(db)
        conn.execute(
            "CREATE TABLE telemetry_frames (year INTEGER, gp TEXT, session TEXT, driver TEXT, "
            "lap INTEGER, frame BLOB, PRIMARY KEY (year, gp, session, driver, lap))"
        )
        conn.execute("INSERT INTO telemetry_frames VALUES (2025, 'x', 'Race', 'VER', 1, X'00')")
        conn.commit()
        conn.close()

        cache = Cache(tmp_path)
        assert cache.db_path == db
        version = cache.conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == _FRAME_TIER_SCHEMA_VERSION
        assert cache.conn.execute("SELECT COUNT(*) FROM telemetry_frames").fetchone()[0] == 0
        cache.close()

        # Re-opening a current database keeps its frames.
        cache = Cache(tmp_path)
        cache.conn.execute(
            "INSERT INTO telemetry_frames VALUES (2025, 'x', 'Race', 'VER', 1, X'00')"
        )
        cache.conn.commit()
        cache.close()
        cache = Cache(tmp_path)
        assert cache.conn.execute("SELECT COUNT(*) FROM telemetry_frames").fetchone()[0] == 1
        cache.close()


# ---------------------------------------------------------------------------
# Driver classification, results, decoded names, alias-not-in-season
# ---------------------------------------------------------------------------


def _session_with(drivers: list[dict], laps: pd.DataFrame | None, session: str = "Race"):
    sess = core.Session(2025, "Test GP", session, enable_cache=False, lib="pandas")
    sess._drivers = drivers
    sess._refresh_driver_indices()
    if laps is not None:
        sess._laps = Laps(laps, session=sess)
    return sess


def _race_laps() -> pd.DataFrame:
    # HAM wins a four-lap race, VER second, LEC two laps down, SAI retired on lap 1.
    return pd.DataFrame(
        {
            "Driver": ["VER", "HAM", "LEC", "SAI", "VER", "HAM", "LEC", "VER", "HAM", "VER", "HAM"],
            "DriverNumber": ["1", "44", "16", "55", "1", "44", "16", "1", "44", "1", "44"],
            "LapNumber": [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0],
            "Position": [1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 2.0, 1.0, 2.0, 1.0],
            "LapTimeSeconds": [90.0, 90.5, 91.0, 95.0, 89.0, 89.2, 90.0, 88.9, 88.5, 88.8, 88.4],
            "Deleted": [False] * 11,
        }
    )


_NUMBER_SORTED = [
    {"driver": "VER", "dn": "1", "team": "Red Bull Racing"},
    {"driver": "LEC", "dn": "16", "team": "Ferrari"},
    {"driver": "HAM", "dn": "44", "team": "Ferrari"},
    {"driver": "SAI", "dn": "55", "team": "Williams"},
]


class TestDriverClassification:
    def test_number_sorted_payload_is_reordered_from_laps(self):
        sess = _session_with([dict(d) for d in _NUMBER_SORTED], _race_laps())
        assert sess.drivers == ["44", "1", "16", "55"]
        assert sess.drivers_df["Driver"].tolist() == ["HAM", "VER", "LEC", "SAI"]

    def test_classified_payload_is_kept_without_touching_laps(self, monkeypatch):
        payload = [
            {"driver": "HAM", "dn": "44"},
            {"driver": "VER", "dn": "1"},
            {"driver": "LEC", "dn": "16"},
        ]
        sess = _session_with(payload, None)

        def _no_laps(_self):
            raise AssertionError("laps must not be loaded for a classified payload")

        monkeypatch.setattr(type(sess), "laps", property(_no_laps))
        assert sess.drivers == ["44", "1", "16"]

    def test_missing_numbers_are_backfilled_from_laps(self):
        payload = [
            {"driver": "VER", "team": "Red Bull Racing"},
            {"driver": "HAM", "team": "Ferrari"},
        ]
        sess = _session_with(payload, _race_laps())
        assert sess.drivers == ["44", "1"]
        assert sess.drivers_df["DriverNumber"].tolist() == ["44", "1"]
        # Number lookups see the backfilled numbers.
        assert sess.get_driver("44")["Abbreviation"] == "HAM"
        assert sess.get_driver("VER")["DriverNumber"] == "1"

    def test_non_race_sessions_order_by_best_clean_lap(self):
        laps = pd.DataFrame(
            {
                "Driver": ["VER", "HAM", "HAM", "LEC"],
                "DriverNumber": ["1", "44", "44", "16"],
                "LapNumber": [1.0, 1.0, 2.0, 1.0],
                "Position": [np.nan] * 4,
                "LapTimeSeconds": [90.0, 88.0, 89.5, 89.0],
                "Deleted": [False, True, False, False],  # HAM's 88.0 was deleted
            }
        )
        sess = _session_with([dict(d) for d in _NUMBER_SORTED[:3]], laps, session="Qualifying")
        assert sess.drivers_df["Driver"].tolist() == ["LEC", "HAM", "VER"]
        assert sess.results["Status"].tolist() == ["Finished"] * 3

    def test_results_carry_the_classification(self):
        sess = _session_with([dict(d) for d in _NUMBER_SORTED], _race_laps())
        results = sess.results
        assert results["Abbreviation"].tolist() == ["HAM", "VER", "LEC", "SAI"]
        assert results["Position"].tolist() == [1.0, 2.0, 3.0, 4.0]
        assert results["ClassifiedPosition"].tolist() == ["1", "2", "3", "4"]
        assert results["Laps"].tolist() == [4.0, 4.0, 2.0, 1.0]
        assert results["Status"].tolist() == ["Finished", "Finished", "+2 Laps", "+3 Laps"]
        assert results["GridPosition"].isna().all()
        assert results["Points"].isna().all()
        assert results["Position"].dtype == "float64"
        assert not results.iloc[0].dnf
        assert not results.iloc[2].dnf  # "+2 Laps" counts as classified
        assert not results.iloc[3].dnf  # so does "+3 Laps" (any lapped finisher)

    def test_driver_without_laps_is_last_and_did_not_start(self):
        payload = [dict(d) for d in _NUMBER_SORTED] + [{"driver": "BOT", "dn": "77"}]
        sess = _session_with(payload, _race_laps())
        results = sess.results
        assert results["Abbreviation"].tolist()[-1] == "BOT"
        assert results["Status"].tolist()[-1] == "Did not start"
        assert results["Laps"].tolist()[-1] == 0.0

    def test_results_without_laps_payload_keep_entrants(self):
        sess = _session_with([{"driver": "HAM", "dn": "44"}, {"driver": "VER", "dn": "1"}], None)
        sess._laps = Laps(pd.DataFrame(), session=sess)  # empty table: nothing to classify
        results = sess.results
        assert results["Abbreviation"].tolist() == ["HAM", "VER"]
        assert results["Position"].tolist() == [1.0, 2.0]  # the payload order is a classification
        assert results["Laps"].isna().all()
        assert results["Status"].isna().all()

    def test_polars_summary(self):
        pl = pytest.importorskip("polars")
        laps = pl.from_pandas(_race_laps())
        summary = core._summarize_laps_for_classification(laps, "polars")
        assert summary["HAM"] == {"number": "44", "laps": 4, "position": 1.0, "best": 88.4}
        assert summary["SAI"]["laps"] == 1

    @pytest.mark.parametrize(
        ("numbers", "expected"),
        [
            (["1", "10", "11", "14", "2", "20"], True),  # string sort
            (["1", "2", "10", "11", "14", "20"], True),  # integer sort
            (["81", "63", "4", "16"], False),
            (["1"], False),
            ([], False),
        ],
    )
    def test_is_number_sorted(self, numbers, expected):
        assert core._is_number_sorted(numbers) is expected

    def test_finishing_order_maps_numbers_through_drivers_df(self):
        from tif1.charts import _common

        sess = _session_with([dict(d) for d in _NUMBER_SORTED], _race_laps())
        assert _common.finishing_order(sess, 2) == ["HAM", "VER"]


class TestLapsResetIndexInPlace:
    def test_inplace_keeps_the_new_column(self):
        laps = Laps(pd.DataFrame({"index": [5, 6], "LapNumber": [1.0, 2.0]}))
        assert laps.reset_index(inplace=True) is None  # noqa: PD002
        assert "level_0" in laps.columns  # "index" already existed, pandas picks level_0

    def test_to_sql_with_index(self):
        laps = Laps(pd.DataFrame({"index": [5, 6], "LapNumber": [1.0, 2.0]}))
        conn = sqlite3.connect(":memory:")
        try:
            laps.to_sql("laps", conn, index=True)
            assert conn.execute("SELECT COUNT(*) FROM laps").fetchone()[0] == 2
        finally:
            conn.close()

    def test_copy_still_drops_level_0(self):
        laps = Laps(pd.DataFrame({"index": [5, 6], "LapNumber": [1.0, 2.0]}))
        assert "level_0" not in laps.reset_index().columns


class TestDecodedNames:
    def test_session_exposes_plain_names(self):
        sess = core.Session(2025, "São Paulo Grand Prix", "Practice 1", enable_cache=False)
        assert sess.gp == "S%C3%A3o%20Paulo%20Grand%20Prix"
        assert sess.event_name == "São Paulo Grand Prix"
        assert sess.session_name == "Practice 1"
        assert sess.name == "Practice 1"
        assert sess.session_info["EventName"] == "São Paulo Grand Prix"

    def test_driver_not_found_context_is_decoded(self):
        sess = _session_with([{"driver": "HAM", "dn": "44"}], None)
        sess.gp = "Bahrain%20Grand%20Prix"
        sess.session = "Practice%201"
        with pytest.raises(core.DriverNotFoundError) as info:
            sess.get_driver("VER")
        assert info.value.context["event"] == "Bahrain Grand Prix"
        assert info.value.context["session"] == "Practice 1"

    def test_data_not_found_decodes_fetch_path_names(self):
        from tif1.exceptions import DataNotFoundError

        err = DataNotFoundError(year=2025, event="Bahrain%20Grand%20Prix", session="Practice%201")
        assert err.context["event"] == "Bahrain Grand Prix"
        assert "event='Bahrain Grand Prix', session='Practice 1'" in str(err)


class TestAliasNotInSeason:
    def test_get_session_fails_fast(self):
        import tif1
        from tif1.exceptions import EventNotInSeasonError

        with pytest.raises(EventNotInSeasonError, match=r"Japanese Grand Prix.*2021") as info:
            tif1.get_session(2021, "Suzuka", "Race")
        assert isinstance(info.value, ValueError)
        assert tif1.EventNotInSeasonError is EventNotInSeasonError

    def test_alias_on_the_calendar_still_resolves(self):
        import tif1

        assert tif1.get_session(2021, "Spa", "Race").event_name == "Belgian Grand Prix"


# ---------------------------------------------------------------------------
# Infrastructure findings from running the API reference pages
# ---------------------------------------------------------------------------


class TestInfrastructureFindings:
    def test_default_cdns_ignore_global_minification(self, monkeypatch):
        from tif1 import cdn as cdn_module

        class _StubConfig:
            def get(self, key, default=None):
                if key == "cdn_use_minification":
                    return True
                if key == "cdns":
                    return [
                        "https://cdn.jsdelivr.net/gh/TracingInsights",
                        "https://huggingface.co/buckets/tracinginsights",
                        "https://cdn.staticdelivr.com/gh/TracingInsights",
                        "https://mirror.example.com/tif1",
                    ]
                return default

        monkeypatch.setattr("tif1.config.get_config", lambda: _StubConfig())
        manager = cdn_module.CDNManager()
        flags = {source.base_url: source.use_minification for source in manager.sources}
        # jsDelivr/StaticDelivr answer 404 for .min.json: the global option is a
        # guaranteed wasted round trip there, so only the custom mirror minifies.
        assert [flag for url, flag in flags.items() if "example.com" not in url] == [False] * 3
        assert flags["https://mirror.example.com/tif1"] is True
        custom = next(s for s in manager.sources if "example.com" in s.base_url)
        assert custom.format_url(2025, "X", "Race", "drivers.json").endswith("/drivers.min.json")

    def test_error_messages_without_context(self):
        from tif1.exceptions import DataNotFoundError, NetworkError

        assert str(DataNotFoundError()) == "Data not found"
        assert str(NetworkError()) == "Network request failed"
        assert str(NetworkError(url="x")) == "Network request failed: url='x'"

    def test_resolver_specifiers(self):
        from tif1.config import get_config
        from tif1.http_session import DEFAULT_RESOLVERS, _normalize_resolver

        assert DEFAULT_RESOLVERS == ("standard", "doh+cloudflare://", "doh+google://")
        assert _normalize_resolver("doh://cloudflare") == "doh+cloudflare://"
        assert _normalize_resolver("doh+quad9://") == "doh+quad9://"
        assert get_config().get("http_resolvers")[1:] == ["doh+cloudflare://", "doh+google://"]

    def test_connection_reuse_detection(self):
        from tif1.http_session import _response_reused_connection

        fresh = SimpleNamespace(
            conn_info=SimpleNamespace(established_latency=0.02, tls_handshake_latency=0.06)
        )
        reused = SimpleNamespace(
            conn_info=SimpleNamespace(established_latency=0, tls_handshake_latency=0)
        )
        assert _response_reused_connection(fresh) is False
        assert _response_reused_connection(reused) is True
        assert _response_reused_connection(SimpleNamespace()) is False

    def test_validators_accept_real_payload_shapes(self):
        from tif1.validation import validate_laps, validate_race_control

        rcm = validate_race_control(
            {"time": ["2025-04-13T15:00:00"], "cat": ["Flag"], "msg": ["GREEN LIGHT"]}
        )
        assert rcm.time == ["2025-04-13T15:00:00"]
        laps = validate_laps(
            {
                "time": [95.1, "None"],
                "lap": [1, 2],
                "compound": ["SOFT", "None"],
                "stint": [1, 1],
                "s1": [30.0, "None"],
                "s2": [30.0, 30.0],
                "s3": [35.1, 35.0],
                "life": [1, 2],
                "pos": [1, "None"],
                "status": ["1", "1"],
                "pb": [True, False],
            }
        )
        assert laps.time == [95.1, None]
        assert laps.pos == [1, None]

    def test_json_dumps_fallback_is_compact(self):
        from tif1.core_utils.json_utils import json_dumps

        assert json_dumps({"a": 1.5, "b": [1, 2]}) == '{"a":1.5,"b":[1,2]}'

    def test_has_session_data_accepts_plain_names(self, tmp_path):
        cache = Cache(tmp_path)
        try:
            cache.set("2025/Bahrain%20Grand%20Prix/Race/drivers.json", {"drivers": []})
            assert cache.has_session_data(2025, "Bahrain Grand Prix", "Race")
            assert cache.has_session_data(2025, "Bahrain%20Grand%20Prix", "Race")
            assert not cache.has_session_data(2025, "Monaco Grand Prix", "Race")
        finally:
            cache.close()

    def test_cli_seconds_handles_timedelta(self):
        from tif1.cli import _seconds

        assert _seconds(pd.Timedelta(seconds=95.14)) == pytest.approx(95.14)
        assert _seconds(95.14) == pytest.approx(95.14)

    def test_lap_card_title_is_an_integer(self):
        session = SimpleNamespace(
            year=2025,
            gp="Bahrain%20Grand%20Prix",
            session="Race",
            _get_telemetry_payload=lambda _driver, _lap: None,
        )
        lap = Lap(pd.Series({"Driver": "VER", "LapNumber": 10.0}), session=session)
        html_out = display_lap_info(lap)
        assert "Lap 10</h3>" in html_out
        assert "Lap 10.0" not in html_out
        assert "Bahrain Grand Prix" in html_out

    def test_unit_suite_cache_isolation(self):
        from pathlib import Path

        from tif1.cache import get_cache
        from tif1.config import get_config

        cache = get_cache()
        configured = Path(str(get_config().get("cache_dir")))
        assert cache.cache_dir == configured
        assert "tif1_cache" in str(cache.cache_dir)


class TestApiReferenceFindings:
    def test_get_sessions_resolves_aliases(self):
        from tif1.events import get_sessions

        assert get_sessions(2025, "Spielberg") == get_sessions(2025, "Austrian Grand Prix")
        assert (
            get_sessions(2025, "Austrian Grand Prix")
            != [
                "Practice 1",
                "Practice 2",
                "Practice 3",
                "Qualifying",
                "Race",
            ]
            or len(get_sessions(2025, "Austrian Grand Prix")) == 5
        )

    def test_setup_mpl_rejects_unknown_scheme(self):
        from tif1.plotting import setup_mpl

        with pytest.raises(ValueError, match="Unknown color_scheme 'official'"):
            setup_mpl(color_scheme="official")

    def test_nameless_drivers_are_not_fuzzy_matched(self):
        from tif1 import plotting

        session = core.Session(2024, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._drivers = [
            {"driver": "SAI", "dn": "55", "team": "Ferrari", "tc": "E80020"},
            {"driver": "HAM", "dn": "44", "team": "Mercedes", "tc": "27F4D2"},
        ]
        session._refresh_driver_indices()
        session._laps = Laps(
            pd.DataFrame({"Driver": ["SAI", "HAM"], "Team": ["Ferrari", "Mercedes"]}),
            session=session,
        )
        assert plotting.get_driver_color("HAM", session=session)
        with pytest.raises(KeyError, match="no driver names"):
            plotting.get_driver_color("Verstappen", session=session)

    def test_cdn_manager_reload_reads_config(self, monkeypatch):
        from tif1 import cdn as cdn_module

        urls = {"value": ["https://cdn.jsdelivr.net/gh/TracingInsights"]}

        class _StubConfig:
            def get(self, key, default=None):
                if key == "cdns":
                    return urls["value"]
                return default

        monkeypatch.setattr("tif1.config.get_config", lambda: _StubConfig())
        manager = cdn_module.CDNManager()
        assert [s.name for s in manager.sources] == ["jsDelivr"]
        urls["value"] = ["https://mirror.example.com/tif1"]
        manager.reload()
        assert [s.base_url for s in manager.sources] == ["https://mirror.example.com/tif1"]

    def test_laps_raise_when_every_cdn_failed(self, monkeypatch):
        import asyncio

        from tif1.cdn import get_cdn_manager
        from tif1.exceptions import NetworkError

        session = core.Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._drivers = [{"driver": "VER", "dn": "1", "team": "Red Bull Racing"}]
        session._refresh_driver_indices()

        async def failing_fetch(requests, **_kwargs):
            manager = get_cdn_manager()
            for source in manager.sources:
                manager.mark_failure(source.name)
            raise NetworkError(url="cdn down")

        monkeypatch.setattr(core, "fetch_multiple_async", failing_fetch)
        with pytest.raises(NetworkError):
            asyncio.run(session.laps_async())

    def test_laps_empty_when_payloads_are_missing_not_failing(self, monkeypatch):
        import asyncio

        session = core.Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        session._drivers = [{"driver": "VER", "dn": "1", "team": "Red Bull Racing"}]
        session._refresh_driver_indices()

        async def missing_fetch(requests, **_kwargs):
            return [None] * len(requests)  # every CDN answered 404

        monkeypatch.setattr(core, "fetch_multiple_async", missing_fetch)
        assert asyncio.run(session.laps_async()).empty


class TestAgentTwoFindings:
    def test_lap_not_found_context_is_decoded(self):
        from tif1.exceptions import LapNotFoundError

        err = LapNotFoundError(
            lap_number=999, driver="VER", event="Bahrain%20Grand%20Prix", session="Race"
        )
        assert err.context["event"] == "Bahrain Grand Prix"
        assert "Lap not found: lap=999, driver='VER'" in str(err)

    def test_open_circuit_breaker_is_a_network_error(self):
        from tif1.exceptions import CircuitBreakerOpenError, NetworkError
        from tif1.retry import CircuitBreaker

        breaker = CircuitBreaker(threshold=1, timeout=3600)

        def boom():
            raise RuntimeError("down")

        with pytest.raises(RuntimeError):
            breaker.call(boom)
        with pytest.raises(CircuitBreakerOpenError, match="Circuit breaker is open") as info:
            breaker.call(lambda: None)
        assert isinstance(info.value, NetworkError)
