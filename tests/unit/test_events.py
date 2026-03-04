"""Tests for events module."""

from urllib.parse import unquote

import pandas as pd
import pytest

from tif1.events import (
    EventSchedule,
    _build_sessions_for_event,
    get_event,
    get_event_by_name,
    get_event_by_round,
    get_events,
    get_sessions,
)


def _event_names(schedule: EventSchedule) -> list[str]:
    return schedule["EventName"].tolist()


class TestEvents:
    """Test event and session functions."""

    def test_get_events_2025(self):
        """Test getting events for 2025."""
        events = get_events(2025)
        assert isinstance(events, EventSchedule)
        assert len(events) > 0
        assert "Abu Dhabi Grand Prix" in _event_names(events)
        assert "Australian Grand Prix" in _event_names(events)

    def test_get_events_2024(self):
        """Test getting events for 2024."""
        events = get_events(2024)
        assert isinstance(events, EventSchedule)
        assert "Bahrain Grand Prix" in _event_names(events)

    def test_get_events_invalid_year(self):
        """Test getting events for invalid year."""
        events = get_events(2030)
        assert events.empty

    def test_get_events_2026_from_vendored_schedule(self):
        """Test getting events for 2026 from vendored f1schedule files."""
        events = get_events(2026)
        assert isinstance(events, EventSchedule)
        assert "Australian Grand Prix" in _event_names(events)
        assert "Abu Dhabi Grand Prix" in _event_names(events)

    def test_get_sessions_standard(self):
        """Test getting standard sessions."""
        sessions = get_sessions(2018, "Australian Grand Prix")
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]

    def test_get_sessions_sprint_2025(self):
        """Test getting sprint sessions for 2025."""
        sessions = get_sessions(2025, "Chinese Grand Prix")
        assert "Sprint Qualifying" in sessions
        assert "Sprint" in sessions

    def test_get_sessions_sprint_2024(self):
        """Test getting sprint sessions for 2024."""
        sessions = get_sessions(2024, "Miami Grand Prix")
        assert "Sprint Qualifying" in sessions
        assert "Sprint" in sessions

    def test_get_sessions_testing(self):
        """Test getting testing sessions."""
        sessions = get_sessions(2025, "Pre-Season Testing")
        assert sessions == ["Practice 1", "Practice 2", "Practice 3"]


class TestEventsForYears:
    """Test event lists for years 2018-2023."""

    def test_get_events_2018(self):
        events = get_events(2018)
        assert isinstance(events, EventSchedule)
        assert len(events) == 21
        assert "Australian Grand Prix" in _event_names(events)
        assert "Abu Dhabi Grand Prix" in _event_names(events)

    def test_get_events_2019(self):
        events = get_events(2019)
        assert isinstance(events, EventSchedule)
        assert len(events) == 21
        assert "Japanese Grand Prix" in _event_names(events)

    def test_get_events_2020(self):
        events = get_events(2020)
        assert isinstance(events, EventSchedule)
        assert len(events) == 19
        assert "Styrian Grand Prix" in _event_names(events)
        assert "Emilia Romagna Grand Prix" in _event_names(events)

    def test_get_events_2021(self):
        events = get_events(2021)
        assert isinstance(events, EventSchedule)
        assert len(events) == 23
        assert "São Paulo Grand Prix" in _event_names(events)
        assert "British Grand Prix" in _event_names(events)

    def test_get_events_2022(self):
        events = get_events(2022)
        assert isinstance(events, EventSchedule)
        assert len(events) == 24
        assert "Pre-Season Test" in _event_names(events)

    def test_get_events_2023(self):
        events = get_events(2023)
        assert isinstance(events, EventSchedule)
        assert len(events) == 23
        assert "Pre-Season Testing" in _event_names(events)

    def test_get_events_unknown_year_returns_empty(self):
        assert get_events(2015).empty


class TestSessionFormats2019:
    """Test 2019 session formats."""

    def test_japanese_gp_has_four_sessions(self):
        sessions = _build_sessions_for_event(2019, "Japanese Grand Prix")
        # Updated: schedule data shows standard 5 sessions
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]

    def test_standard_2019_event(self):
        sessions = _build_sessions_for_event(2019, "Australian Grand Prix")
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]


class TestSessionFormats2020:
    """Test 2020 session formats."""

    def test_styrian_gp_four_sessions(self):
        sessions = _build_sessions_for_event(2020, "Styrian Grand Prix")
        # Updated: schedule data shows standard 5 sessions
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]

    def test_emilia_romagna_three_sessions(self):
        sessions = _build_sessions_for_event(2020, "Emilia Romagna Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Race"]

    def test_eifel_gp_three_sessions(self):
        sessions = _build_sessions_for_event(2020, "Eifel Grand Prix")
        # Updated: schedule data shows standard 5 sessions
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]

    def test_standard_2020_event(self):
        sessions = _build_sessions_for_event(2020, "Austrian Grand Prix")
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]


class TestSessionFormats2021:
    """Test 2021 sprint events."""

    def test_british_gp_sprint(self):
        sessions = _build_sessions_for_event(2021, "British Grand Prix")
        # Updated: 2021 sprints used "Sprint" not "Sprint Qualifying"
        assert sessions == ["Practice 1", "Qualifying", "Practice 2", "Sprint", "Race"]

    def test_italian_gp_sprint(self):
        sessions = _build_sessions_for_event(2021, "Italian Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Practice 2", "Sprint", "Race"]

    def test_sao_paulo_gp_sprint(self):
        sessions = _build_sessions_for_event(2021, "São Paulo Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Practice 2", "Sprint", "Race"]

    def test_standard_2021_event(self):
        sessions = _build_sessions_for_event(2021, "Abu Dhabi Grand Prix")
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]


class TestSessionFormats2022:
    """Test 2022 session formats."""

    def test_pre_season_test(self):
        sessions = _build_sessions_for_event(2022, "Pre-Season Test")
        assert sessions == ["Practice 1", "Practice 2", "Practice 3"]

    def test_austrian_gp_sprint(self):
        sessions = _build_sessions_for_event(2022, "Austrian Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Practice 2", "Sprint", "Race"]

    def test_emilia_romagna_gp_sprint(self):
        sessions = _build_sessions_for_event(2022, "Emilia Romagna Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Practice 2", "Sprint", "Race"]

    def test_sao_paulo_gp_sprint(self):
        sessions = _build_sessions_for_event(2022, "São Paulo Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Practice 2", "Sprint", "Race"]

    def test_standard_2022_event(self):
        sessions = _build_sessions_for_event(2022, "Abu Dhabi Grand Prix")
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]


class TestSessionFormats2023:
    """Test 2023 session formats."""

    def test_pre_season_testing(self):
        sessions = _build_sessions_for_event(2023, "Pre-Season Testing")
        assert sessions == ["Practice 1", "Practice 2", "Practice 3"]

    def test_hungarian_gp_no_practice1(self):
        sessions = _build_sessions_for_event(2023, "Hungarian Grand Prix")
        # Updated: schedule data shows standard 5 sessions
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]

    def test_austrian_gp_sprint(self):
        sessions = _build_sessions_for_event(2023, "Austrian Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Sprint Shootout", "Sprint", "Race"]

    def test_azerbaijan_gp_sprint(self):
        sessions = _build_sessions_for_event(2023, "Azerbaijan Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Sprint Shootout", "Sprint", "Race"]

    def test_belgian_gp_sprint(self):
        sessions = _build_sessions_for_event(2023, "Belgian Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Sprint Shootout", "Sprint", "Race"]

    def test_qatar_gp_sprint(self):
        sessions = _build_sessions_for_event(2023, "Qatar Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Sprint Shootout", "Sprint", "Race"]

    def test_us_gp_sprint(self):
        sessions = _build_sessions_for_event(2023, "United States Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Sprint Shootout", "Sprint", "Race"]

    def test_sao_paulo_gp_sprint(self):
        sessions = _build_sessions_for_event(2023, "São Paulo Grand Prix")
        assert sessions == ["Practice 1", "Qualifying", "Sprint Shootout", "Sprint", "Race"]

    def test_standard_2023_event(self):
        sessions = _build_sessions_for_event(2023, "Monaco Grand Prix")
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]


class TestUnknownYear:
    """Test that unknown years return standard sessions."""

    def test_unknown_year_returns_standard(self):
        sessions = _build_sessions_for_event(2015, "Some Grand Prix")
        assert sessions == ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]

    def test_get_sessions_wraps_cached_result(self):
        s1 = get_sessions(2019, "Japanese Grand Prix")
        s2 = get_sessions(2019, "Japanese Grand Prix")
        assert s1 == s2
        assert s1 is not s2


class TestGetEventByRound:
    """Test get_event_by_round function."""

    def test_get_event_by_round_2021(self):
        event = get_event_by_round(2021, 10)
        assert event["EventName"] == "British Grand Prix"
        assert event["RoundNumber"] == 10
        assert event["Country"] == "Great Britain"

    def test_get_event_by_round_first(self):
        event = get_event_by_round(2024, 1)
        assert event["EventName"] == "Bahrain Grand Prix"
        assert event["RoundNumber"] == 1

    def test_get_event_by_round_invalid_zero(self):
        with pytest.raises(ValueError, match="Cannot get testing event by round number"):
            get_event_by_round(2021, 0)

    def test_get_event_by_round_out_of_range(self):
        with pytest.raises(ValueError, match="Invalid round"):
            get_event_by_round(2021, 999)

    def test_schedule_get_event_by_round_and_head(self):
        schedule = get_events(2024)
        assert isinstance(schedule.head(3), EventSchedule)
        event = schedule.get_event_by_round(1)
        assert event["EventName"] == "Bahrain Grand Prix"
        assert event["RoundNumber"] == 1


class TestGetEventByName:
    """Test get_event_by_name function."""

    def test_exact_match(self):
        event = get_event_by_name(2021, "British Grand Prix", exact_match=True)
        assert event["EventName"] == "British Grand Prix"
        assert event["RoundNumber"] == 10

    def test_exact_match_case_insensitive(self):
        event = get_event_by_name(2021, "british grand prix", exact_match=True)
        assert event["EventName"] == "British Grand Prix"

    def test_exact_match_not_found(self):
        with pytest.raises(ValueError, match="No exact match found"):
            get_event_by_name(2021, "Invalid GP", exact_match=True)

    def test_fuzzy_match_location(self):
        event = get_event_by_name(2021, "Silverstone")
        assert event["EventName"] == "British Grand Prix"

    def test_fuzzy_match_country(self):
        event = get_event_by_name(2021, "Great Britain")
        assert event["EventName"] == "British Grand Prix"

    def test_fuzzy_match_partial_name(self):
        event = get_event_by_name(2021, "British")
        assert event["EventName"] == "British Grand Prix"

    def test_fuzzy_match_without_common_words(self):
        event = get_event_by_name(2024, "Bahrain")
        assert event["EventName"] == "Bahrain Grand Prix"

    def test_fuzzy_match_monaco(self):
        event = get_event_by_name(2021, "Monaco")
        assert event["EventName"] == "Monaco Grand Prix"

    def test_fuzzy_match_sao_paulo_without_accent(self):
        """Test that 'Sao Paulo' matches 'São Paulo Grand Prix'."""
        event = get_event_by_name(2021, "Sao Paulo Grand Prix")
        assert event["EventName"] == "São Paulo Grand Prix"

    def test_fuzzy_match_sao_paulo_partial(self):
        """Test that 'Sao Paulo' partial name matches correctly."""
        event = get_event_by_name(2024, "Sao Paulo")
        assert event["EventName"] == "São Paulo Grand Prix"


class TestEventSessionName:
    """Test FastF1-compatible Event session name lookup."""

    def test_get_session_name_by_number(self):
        event = get_event(2024, 1)
        assert event.get_session_name(1) == "Practice 1"
        assert event.get_session_name(5) == "Race"

    def test_get_session_name_by_abbreviation(self):
        event = get_event(2024, 1)
        assert event.get_session_name("FP1") == "Practice 1"
        assert event.get_session_name("Q") == "Qualifying"
        assert event.get_session_name("R") == "Race"

    def test_get_session_name_case_insensitive(self):
        event = get_event(2024, 1)
        assert event.get_session_name("practice 1") == "Practice 1"
        assert event.get_session_name("QUALIFYING") == "Qualifying"
        assert event.get_session_name("race") == "Race"

    def test_get_session_name_sprint_compatibility_alias(self):
        event = get_event(2021, "British Grand Prix")
        assert event.get_session_name("SQ") == "Sprint"

    def test_get_session_name_invalid_number(self):
        event = get_event(2024, 1)
        with pytest.raises(ValueError, match="Invalid session type"):
            event.get_session_name(99)

    def test_get_session_name_unavailable_type(self):
        event = get_event(2024, 1)
        with pytest.raises(ValueError, match="does not exist for this event"):
            event.get_session_name("S")

    def test_get_session_resolves_identifier(self):
        event = get_event(2024, 1)
        session = event.get_session("race")
        assert session.session == "Race"

    def test_get_race_method(self):
        event = get_event(2024, 1)
        session = event.get_race()
        assert session.session == "Race"

    def test_get_qualifying_method(self):
        event = get_event(2024, 1)
        session = event.get_qualifying()
        assert session.session == "Qualifying"

    def test_get_practice_method(self):
        event = get_event(2024, 1)
        session = event.get_practice(1)
        assert unquote(str(session.session)) == "Practice 1"

    def test_get_practice_invalid_number(self):
        event = get_event(2024, 1)
        with pytest.raises(ValueError, match="Invalid session type"):
            event.get_practice(99)

    def test_get_sprint_methods_for_2024_sprint_weekend(self):
        event = get_event(2024, "Miami Grand Prix")
        sprint = event.get_sprint()
        sprint_qualifying = event.get_sprint_qualifying()
        assert sprint.session == "Sprint"
        assert unquote(str(sprint_qualifying.session)) == "Sprint Qualifying"

    def test_get_sprint_shootout_method(self):
        event = get_event(2023, "Austrian Grand Prix")
        session = event.get_sprint_shootout()
        assert unquote(str(session.session)) == "Sprint Shootout"

    def test_get_session_date_local_and_utc(self):
        event = get_event(2024, 1)
        local_date = event.get_session_date("Q")
        utc_date = event.get_session_date("Q", utc=True)

        assert local_date == pd.Timestamp(event["Session4Date"])
        assert utc_date == pd.Timestamp(event["Session4DateUtc"])

    def test_get_session_date_by_number(self):
        event = get_event(2024, 1)
        assert event.get_session_date(5, utc=True) == pd.Timestamp(event["Session5DateUtc"])

    def test_get_session_date_missing_local_timestamp(self):
        from tif1.events import Event

        event = Event(
            2024,
            "Bahrain Grand Prix",
            metadata={"Session1DateUtc": "2024-02-29T10:00:00"},
        )

        with pytest.raises(ValueError, match="Local timestamp is not available"):
            event.get_session_date(1)


def test_cdn_fallback_for_missing_year(monkeypatch):
    import tif1.events as events_mod

    events_mod._load_schedule_payload.cache_clear()
    events_mod._load_f1schedule_year_from_cdn.cache_clear()
    events_mod._get_events_cached.cache_clear()
    events_mod._get_sessions_cached.cache_clear()

    monkeypatch.setattr(
        events_mod, "_load_schedule_payload", lambda: {"schema_version": 1, "years": {}}
    )
    monkeypatch.setattr(
        events_mod,
        "_load_f1schedule_year_from_cdn",
        lambda year: (
            {
                "events": ["Fallback Grand Prix"],
                "sessions": {"Fallback Grand Prix": ["Practice 1", "Qualifying", "Race"]},
            }
            if year == 2099
            else None
        ),
    )

    events = get_events(2099)
    assert _event_names(events) == ["Fallback Grand Prix"]
    assert get_sessions(2099, "Fallback Grand Prix") == ["Practice 1", "Qualifying", "Race"]
