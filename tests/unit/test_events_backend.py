"""Schedule backends: the Ergast race table, ``backend=`` keywords, testing helpers."""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urlsplit

import pandas as pd
import pytest

import tif1
from tif1 import events as events_mod
from tif1.events import (
    _convert_ergast_year,
    _effective_backend,
    get_event,
    get_event_schedule,
    get_events_remaining,
    get_sessions,
    get_testing_event,
    get_testing_session,
)
from tif1.exceptions import DataNotFoundError


@pytest.fixture(autouse=True)
def _clear_memo():
    events_mod._clear_schedule_memo()
    yield
    events_mod._clear_schedule_memo()


class TestBackendDispatch:
    def test_effective_backend(self):
        assert _effective_backend(2025, None) == "f1schedule"
        assert _effective_backend(2017, None) == "ergast"
        assert _effective_backend(1995, "fastf1") == "f1schedule"
        assert _effective_backend(2025, "f1timing") == "f1schedule"
        assert _effective_backend(2025, "ergast") == "ergast"
        with pytest.raises(ValueError, match="Unknown schedule backend"):
            _effective_backend(2025, "livetiming")

    def test_force_ergast_is_deprecated_alias(self):
        with pytest.warns(DeprecationWarning, match="force_ergast"):
            schedule = get_event_schedule(2025, force_ergast=True)
        assert schedule.backend == "ergast"
        assert "Chinese Grand Prix" in schedule["EventName"].tolist()


class TestErgastSchedule:
    def test_pre_2018_schedule_from_packaged_file(self):
        schedule = get_event_schedule(1995)
        assert len(schedule) == 17
        assert schedule.backend is None
        row = schedule.iloc[0]
        assert row["EventName"] == "Brazilian Grand Prix"
        assert row["RoundNumber"] == 1
        assert row["EventFormat"] == "conventional"
        assert row["Location"] == "São Paulo"
        assert row["Country"] == "Brazil"
        assert row["Session5"] == "Race"
        assert row["Session5DateUtc"] == pd.Timestamp("1995-03-26")
        assert row["Session1DateUtc"] == pd.Timestamp("1995-03-24")  # estimated: two days before
        assert pd.isna(row["Session1Date"])
        assert row["F1ApiSupport"] is False or row["F1ApiSupport"] == False  # noqa: E712
        assert row["CircuitId"] == "interlagos"
        assert row["CircuitLatitude"] == pytest.approx(-23.7036)
        assert urlsplit(row["EventUrl"]).hostname == "en.wikipedia.org"
        assert not schedule.is_testing().any()

    def test_fastf1_columns_come_first(self):
        schedule = get_event_schedule(1995)
        fastf1_columns = [
            "RoundNumber",
            "Country",
            "Location",
            "OfficialEventName",
            "EventDate",
            "EventName",
            "EventFormat",
        ]
        assert list(schedule.columns[:7]) == fastf1_columns
        assert list(schedule.columns[-6:]) == [
            "CircuitId",
            "CircuitName",
            "CircuitLatitude",
            "CircuitLongitude",
            "EventUrl",
            "CircuitUrl",
        ]

    def test_sprint_formats(self):
        schedule_2021 = get_event_schedule(2021, backend="ergast")
        silverstone = schedule_2021[schedule_2021["EventName"] == "British Grand Prix"].iloc[0]
        assert silverstone["EventFormat"] == "sprint"
        assert silverstone["Session3"] == "Practice 2"
        schedule_2023 = get_event_schedule(2023, backend="ergast")
        baku = schedule_2023[schedule_2023["EventName"] == "Azerbaijan Grand Prix"].iloc[0]
        assert baku["EventFormat"] == "sprint_shootout"
        assert baku["Session3"] == "Sprint Shootout"
        assert baku["Session3DateUtc"] == pd.Timestamp("2023-04-29 09:30:00")  # real Jolpica key
        schedule_2025 = get_event_schedule(2025, backend="ergast")
        china = schedule_2025[schedule_2025["EventName"] == "Chinese Grand Prix"].iloc[0]
        assert china["EventFormat"] == "sprint_qualifying"
        assert china["Session2"] == "Sprint Qualifying"
        assert china["Session2DateUtc"] == pd.Timestamp("2025-03-21 07:30:00")
        assert china["Session5DateUtc"] == pd.Timestamp("2025-03-23 07:00:00")

    def test_sessions_and_events_before_2018(self):
        # fastf1 parity: all five sessions are listed and Session{i} is session i.
        assert get_sessions(1995, "Monaco") == [
            "Practice 1",
            "Practice 2",
            "Practice 3",
            "Qualifying",
            "Race",
        ]
        event = get_event(1995, "Monaco")
        assert event.EventName == "Monaco Grand Prix"
        assert event.RoundNumber == 5
        assert event.get_session_name("R") == "Race"
        assert event.get_session_name("FP1") == "Practice 1" == event["Session1"]
        assert event.get_session_name(5) == "Race" == event["Session5"]
        assert event.get_session_date("Race", utc=True) == pd.Timestamp("1995-05-28")
        assert event.get_session_date(1, utc=True) == pd.Timestamp("1995-05-26")  # estimated
        with pytest.raises(DataNotFoundError) as info:  # Ergast carries no practice data
            event.get_session("FP1")
        assert "no Practice 1 data" in info.value.context["reason"]
        assert tif1.get_event_by_round(1995, 5).EventName == "Monaco Grand Prix"
        assert tif1.get_event_by_name(1995, "monte carlo").EventName == "Monaco Grand Prix"

    def test_2018_plus_ergast_backend_lists_all_sessions(self):
        assert get_sessions(2025, "Chinese Grand Prix", backend="ergast") == [
            "Practice 1",
            "Sprint Qualifying",
            "Sprint",
            "Qualifying",
            "Race",
        ]
        event = get_event(2025, 2, backend="ergast")
        assert event.EventName == "Chinese Grand Prix"
        assert event._backend == "ergast"

    def test_circuit_columns_join_f1schedule_events(self):
        event = get_event(2025, "Monaco")
        assert event["CircuitId"] == "monaco"
        assert event["CircuitName"] == "Circuit de Monaco"
        assert event["CircuitLongitude"] == pytest.approx(7.42056)
        schedule = get_event_schedule(2025)
        assert "CircuitId" in schedule.columns
        assert schedule.loc[schedule["EventFormat"] == "testing", "CircuitId"].eq("").all()

    def test_convert_ergast_year_estimates_missing_sessions(self):
        races = [
            {
                "round": "1",
                "raceName": "Historic Grand Prix",
                "date": "1980-05-04",
                "Circuit": {
                    "circuitId": "x",
                    "circuitName": "X",
                    "Location": {"lat": "1.5", "long": "2.5", "locality": "L", "country": "C"},
                },
            }
        ]
        payload = _convert_ergast_year(races, 1980)
        row = payload["metadata"]["Historic Grand Prix"]
        assert payload["events"] == ["Historic Grand Prix"]
        assert payload["sessions"]["Historic Grand Prix"] == [
            "Practice 1",
            "Practice 2",
            "Practice 3",
            "Qualifying",
            "Race",
        ]
        assert row["Session1DateUtc"] == "1980-05-02T00:00:00"
        assert row["Session4DateUtc"] == "1980-05-03T00:00:00"
        assert row["Session5DateUtc"] == "1980-05-04T00:00:00"
        assert row["CircuitLatitude"] == 1.5

    def test_convert_ergast_year_skips_a_malformed_round(self):
        races = [
            {"round": "TBC", "raceName": "Phantom Grand Prix", "date": "2026-12-01"},
            {"round": "2", "raceName": "Second Grand Prix", "date": "2026-03-15"},
            {"round": "1", "raceName": "First Grand Prix", "date": "2026-03-08"},
            "not a race",
        ]
        payload = events_mod._convert_ergast_year(races, 2026)
        assert payload["events"] == ["First Grand Prix", "Second Grand Prix"]
        assert payload["metadata"]["Second Grand Prix"]["RoundNumber"] == 2

    def test_unknown_season_is_empty(self):
        assert get_event_schedule(1900).empty
        assert get_event_schedule(1900, backend="ergast").empty


class TestTestingHelpers:
    def test_is_testing(self):
        schedule = get_event_schedule(2026)
        flags = schedule.is_testing()
        assert flags.sum() == 2
        event = get_testing_event(2026, 1)
        assert event.is_testing()
        assert event.EventFormat == "testing"
        assert not get_event(2026, 1).is_testing()
        with pytest.raises(ValueError, match="does not exist"):
            get_testing_event(2026, 3)
        with pytest.raises(ValueError, match="ergast"):
            get_testing_event(2026, 1, backend="ergast")

    def test_get_testing_session(self, monkeypatch):
        captured: dict = {}

        def fake_get_session(year, gp, session, *, backend=None):
            captured.update(year=year, gp=gp, session=session, backend=backend)
            return "session"

        monkeypatch.setattr("tif1.core.get_session", fake_get_session)
        assert get_testing_session(2026, 1, 2) == "session"
        assert captured["year"] == 2026
        assert captured["session"] == "Practice 2"
        assert "Testing" in captured["gp"]

    def test_get_events_remaining(self):
        everything = get_events_remaining(datetime(2026, 1, 1))  # noqa: DTZ001
        assert len(everything) == len(get_event_schedule(2026))
        late = get_events_remaining(datetime(2026, 12, 31))  # noqa: DTZ001
        assert late.empty
        cutoff = datetime(2026, 10, 1)  # noqa: DTZ001
        mid = get_events_remaining(cutoff, include_testing=False)
        assert (pd.to_datetime(mid["Session5DateUtc"]) >= cutoff).all()
        assert len(mid) < len(everything)
        assert not mid.is_testing().any()
        assert tif1.get_events_remaining is get_events_remaining

    def test_get_events_remaining_keeps_undated_events(self, monkeypatch):
        schedule = get_event_schedule(2026, include_testing=False)
        last = schedule.index[-1]
        schedule.loc[last, "Session5DateUtc"] = pd.NaT  # no race time published
        monkeypatch.setattr(events_mod, "get_event_schedule", lambda *_args, **_kwargs: schedule)
        after_season = datetime(2026, 12, 31)  # noqa: DTZ001
        without_testing = get_events_remaining(after_season, include_testing=False)
        with_testing = get_events_remaining(after_season, include_testing=True)
        expected = [schedule.loc[last, "EventName"]]
        assert without_testing["EventName"].tolist() == expected
        assert with_testing["EventName"].tolist() == expected  # both branches agree


class TestSessionBackend:
    def test_backend_threads_through_session(self):
        session = tif1.get_session(
            2026, "Bahrain Grand Prix in Malaysia", "Race", enable_cache=False, backend="ergast"
        )
        assert session.event_name == "Bahrain Grand Prix in Malaysia"
        assert session._schedule_backend == "ergast"
        assert session.event.EventName == "Bahrain Grand Prix in Malaysia"
        assert session.event.RoundNumber == 16
        by_index = tif1.get_session(2026, 16, 2, enable_cache=False, backend="ergast")
        assert by_index.session_name == "Practice 2"
        assert by_index.event_name == "Bahrain Grand Prix in Malaysia"
        default = tif1.get_session(2026, 16, "Race", enable_cache=False)
        assert default.event_name == tif1.get_event(2026, 16).EventName  # f1schedule naming
        assert default.event_name != "Bahrain Grand Prix in Malaysia"
        assert default._schedule_backend is None


class TestEventFramesBackend:
    def test_event_reconstruct_keeps_backend(self):
        event = get_event(1995, 5)
        assert event._backend is None
        clone = event.copy()
        assert clone._event_name == "Monaco Grand Prix"
        assert clone._backend is None
        ergast_event = get_event(2025, 2, backend="ergast")
        assert ergast_event.copy()._backend == "ergast"

    def test_schedule_lookups_keep_backend(self):
        schedule = get_event_schedule(2025, backend="ergast")
        assert schedule.get_event_by_round(2)._backend == "ergast"
        assert schedule.get_event_by_name("China")._backend == "ergast"
        filtered = schedule[schedule["RoundNumber"] > 20]
        assert filtered.backend == "ergast"
