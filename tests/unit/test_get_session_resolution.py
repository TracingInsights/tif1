"""``get_session`` event and identifier resolution: fastf1 parity and the pandas-free path.

Everything runs offline against the vendored schedules (past seasons only).
"""

from __future__ import annotations

import logging

import numpy as np
import pytest

import tif1
from tif1 import core
from tif1 import events as events_mod
from tif1.exceptions import (
    EventNotFoundError,
    EventNotInSeasonError,
    ExactEventNotFoundError,
    FuzzyMatchError,
)


@pytest.fixture(autouse=True)
def _clear_memo():
    events_mod._clear_schedule_memo()
    yield
    events_mod._clear_schedule_memo()


def _resolved(year, gp, identifier, **kwargs):
    session = tif1.get_session(year, gp, identifier, **kwargs)
    return session.event_name, session.session_name


class TestRoundNumbers:
    @pytest.mark.parametrize("gp", [8, np.int64(8), np.int32(8), 8.0, np.float64(8.0)])
    def test_any_real_number_is_a_round(self, gp):
        # fastf1 sends every non-string to get_event_by_round.
        assert _resolved(2025, gp, "R") == ("Monaco Grand Prix", "Race")

    def test_round_errors_match_get_event_by_round(self):
        with pytest.raises(ValueError, match="Cannot get testing event by round number!"):
            tif1.get_session(2025, 0, "R")
        with pytest.raises(ValueError, match=r"Invalid round: 99"):
            tif1.get_session(2025, 99, "R")
        with pytest.raises(ValueError, match=r"Invalid round: 8\.5"):
            tif1.get_session(2025, 8.5, "R")

    def test_ergast_backend_rounds(self):
        assert _resolved(1985, 1, "R") == ("Brazilian Grand Prix", "Race")
        assert _resolved(2021, 3, "Q", backend="ergast") == ("Portuguese Grand Prix", "Qualifying")

    def test_resolution_builds_no_event_series(self, monkeypatch):
        def _no_event(*_args, **_kwargs):
            raise AssertionError("get_session built an Event to read its name")

        monkeypatch.setattr(events_mod, "_create_event", _no_event)
        assert _resolved(2025, 8, "Q") == ("Monaco Grand Prix", "Qualifying")
        assert _resolved(2025, "Monaco", "Q") == ("Monaco Grand Prix", "Qualifying")

    def test_get_event_accepts_numpy_rounds(self):
        assert tif1.get_event(2025, np.int64(8))["EventName"] == "Monaco Grand Prix"


class TestResolvedOnce:
    def test_session_constructor_does_not_resolve_again(self, monkeypatch):
        calls = []
        real = core._resolve_gp_name

        def _counting(*args, **kwargs):
            calls.append(args)
            return real(*args, **kwargs)

        monkeypatch.setattr(core, "_resolve_gp_name", _counting)
        tif1.get_session(2025, "Monaco", "R")
        assert calls == [(2025, "Monaco")]
        # A direct construction still resolves its gp.
        session = core.Session(2025, "Monaco", "Race")
        assert session.event_name == "Monaco Grand Prix"
        assert calls[-1] == (2025, "Monaco")


class TestIdentifiers:
    @pytest.mark.parametrize(
        ("identifier", "expected"),
        [
            ("1", "Practice 1"),
            (" 5 ", "Race"),
            (1.0, "Practice 1"),
            (np.int64(4), "Qualifying"),
            (np.float64(5.0), "Race"),
            (True, "Practice 1"),
            (" R ", "Race"),
            ("Practice  1", "Practice 1"),
        ],
    )
    def test_numbers_and_whitespace(self, identifier, expected):
        assert _resolved(2025, "Bahrain Grand Prix", identifier)[1] == expected

    @pytest.mark.parametrize("identifier", [1.5, "2.5", 0, 6, "0"])
    def test_invalid_numbers_raise_value_error(self, identifier):
        with pytest.raises(ValueError, match=r"Invalid session type|out of range"):
            tif1.get_session(2025, "Bahrain Grand Prix", identifier)

    @pytest.mark.parametrize("year", [2021, 2022])
    @pytest.mark.parametrize("identifier", ["SQ", "sq", "Sprint Qualifying", "sprint qualifying"])
    def test_old_sprint_name_means_sprint(self, year, identifier):
        gp = "British Grand Prix" if year == 2021 else "Emilia Romagna Grand Prix"
        assert _resolved(year, gp, identifier)[1] == "Sprint"

    def test_sprint_names_by_format(self):
        assert _resolved(2023, "Azerbaijan Grand Prix", "SS")[1] == "Sprint Shootout"
        assert _resolved(2024, "Chinese Grand Prix", "SQ")[1] == "Sprint Qualifying"
        with pytest.raises(ValueError, match="does not exist"):
            tif1.get_session(2023, "Azerbaijan Grand Prix", "SQ")
        with pytest.raises(ValueError, match="does not exist"):
            tif1.get_session(2024, "Chinese Grand Prix", "SS")

    def test_normalize_session_name_year(self):
        assert core._normalize_session_name("SQ") == "Sprint Qualifying"
        assert core._normalize_session_name("SQ", 2021) == "Sprint"
        assert core._normalize_session_name("SQ", 2024) == "Sprint Qualifying"


class TestTestingEvents:
    def test_exact_name_reaches_testing(self):
        assert _resolved(2025, "Pre-Season Testing", 1) == ("Pre-Season Testing", "Practice 1")
        assert _resolved(2025, "pre-season testing", "FP2")[1] == "Practice 2"

    @pytest.mark.parametrize("query", ["testing", "Pre-Season Test", "Test GP"])
    def test_testing_is_not_a_fuzzy_candidate(self, query):
        with pytest.raises(FuzzyMatchError):
            tif1.get_session(2025, query, 1)

    def test_typo_no_longer_lands_on_testing(self):
        # Pre-Season Test 1 (Barcelona) used to win this one.
        assert events_mod.get_event_by_name(2020, "Barcelone")["EventName"] == (
            "Spanish Grand Prix"
        )
        assert events_mod.get_event_by_name(2025, "Bah")["EventName"] == "Bahrain Grand Prix"


class TestAliases:
    @pytest.mark.parametrize(
        ("year", "query"),
        [
            (2020, "Australian Grand Prix"),
            (2021, "Australian"),
            (2024, "Styrian Grand Prix"),
            (2019, "Styria"),
        ],
    )
    def test_event_names_off_the_calendar_raise(self, year, query):
        # Fuzzy matching used to load the Austrian GP for all of these.
        with pytest.raises(EventNotInSeasonError, match="not on the"):
            events_mod.get_event_by_name(year, query)

    def test_aliases_on_the_calendar(self):
        assert events_mod._resolve_event_name(2021, "Styria") == "Styrian Grand Prix"
        assert events_mod._resolve_event_name(2025, "Australian") == "Australian Grand Prix"
        assert events_mod._resolve_event_name(2025, "Austin Texas") == "United States Grand Prix"

    @pytest.mark.parametrize("query", ["Formula1 Monaco", "FORMULA1 Monaco GP", "F1 Monaco"])
    def test_formula1_is_a_common_word(self, query):
        assert events_mod._resolve_event_name(2025, query) == "Monaco Grand Prix"


class TestExceptions:
    def test_not_in_season_is_a_fuzzy_match_error(self):
        assert issubclass(EventNotInSeasonError, FuzzyMatchError)
        assert issubclass(EventNotInSeasonError, EventNotFoundError)
        with pytest.raises(FuzzyMatchError):
            tif1.get_session(2021, "Suzuka", "R")

    def test_exact_miss_is_a_key_error(self):
        with pytest.raises(KeyError) as excinfo:
            tif1.get_session(2025, "Monaco", "R", exact_match=True)
        assert isinstance(excinfo.value, ExactEventNotFoundError)
        assert isinstance(excinfo.value, EventNotFoundError)
        assert isinstance(excinfo.value, ValueError)
        assert str(excinfo.value) == "No exact match found for event 'Monaco' in 2025"


class TestErgastBackendMetadata:
    def test_fuzzy_features_come_from_the_requested_backend(self):
        # "Al Daayen" is Ergast's locality for Lusail; f1schedule has none.
        assert events_mod._resolve_event_name(2021, "Al Daayen", backend="ergast") == (
            "Qatar Grand Prix"
        )


class TestVendoredSchedule:
    def test_only_the_requested_season_is_loaded(self, monkeypatch):
        loaded: list[int] = []
        real = events_mod._load_vendored_f1schedule_year

        def _recording(year):
            loaded.append(year)
            return real(year)

        monkeypatch.setattr(events_mod, "_load_vendored_f1schedule_year", _recording)
        events_mod._resolve_event_name(2025, "Monaco")
        assert loaded == [2025]

    def test_per_season_loader_matches_the_full_payload(self):
        full = events_mod._load_schedule_payload()["years"]
        for year_str, payload in full.items():
            assert events_mod._load_vendored_f1schedule_year(int(year_str)) == payload
        assert events_mod._load_vendored_f1schedule_year(1999) is None


class TestResolutionMemo:
    def test_memo_is_per_record_and_replays_the_warning(self, caplog):
        record = events_mod._get_year_schedule(2025)
        with caplog.at_level(logging.WARNING, logger="tif1.events"):
            assert events_mod._resolve_event_name(2025, "Monacco") == "Monaco Grand Prix"
            assert events_mod._resolve_event_name(2025, "Monacco") == "Monaco Grand Prix"
        assert record.resolutions[("Monacco", False)][0] == "Monaco Grand Prix"
        corrections = [r for r in caplog.records if "Correcting user input" in r.getMessage()]
        assert len(corrections) == 2

    def test_failures_are_memoized_and_still_raise(self):
        for _ in range(2):
            with pytest.raises(FuzzyMatchError):
                events_mod._resolve_event_name(2025, "Moon GP")
        record = events_mod._get_year_schedule(2025)
        assert record.resolutions[("Moon GP", False)][0] is None

    def test_exact_match_is_part_of_the_key(self):
        assert events_mod._resolve_event_name(2025, "Monaco") == "Monaco Grand Prix"
        with pytest.raises(ExactEventNotFoundError):
            events_mod._resolve_event_name(2025, "Monaco", exact_match=True)

    def test_backends_keep_separate_memos(self):
        f1schedule = events_mod._get_year_schedule(2021)
        ergast = events_mod._get_year_schedule(2021, "ergast")
        assert f1schedule is not ergast
        events_mod._resolve_event_name(2021, "Monaco")
        events_mod._resolve_event_name(2021, "Monaco", backend="ergast")
        assert ("Monaco", False) in f1schedule.resolutions
        assert ("Monaco", False) in ergast.resolutions

    def test_a_refreshed_calendar_drops_the_memo(self, monkeypatch):
        # A live season whose calendar changes is rebuilt as a new record;
        # nothing resolved against the old calendar survives.
        old = events_mod._get_year_schedule(2025)
        assert events_mod._resolve_event_name(2025, "Monaco") == "Monaco Grand Prix"
        assert ("Monaco", False) in old.resolutions
        cancelled = dict(old.payload)
        cancelled["events"] = [n for n in old.payload["events"] if n != "Monaco Grand Prix"]
        fresh = events_mod._YearSchedule(cancelled, tuple(cancelled["events"]), float("inf"))
        monkeypatch.setitem(events_mod._year_schedules, 2025, fresh)
        with pytest.raises(EventNotInSeasonError):
            events_mod._resolve_event_name(2025, "Monaco")
        assert fresh.resolutions[("Monaco", False)][0] is None

    def test_memo_is_bounded(self, monkeypatch):
        monkeypatch.setattr(events_mod, "_RESOLUTION_MEMO_MAX", 3)
        record = events_mod._get_year_schedule(2025)
        for query in ("Monaco", "Spa", "Monza", "Suzuka", "Baku"):
            events_mod._resolve_event_name(2025, query)
        assert len(record.resolutions) <= 3
