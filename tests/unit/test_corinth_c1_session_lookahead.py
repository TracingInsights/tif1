"""Next-round lookahead of a season loop (Corinth C1), offline.

The sequence logic runs against a recorder in place of
:func:`tif1.first_wave.prefetch`; the slot mechanics run against fake
``http.client`` connections (no socket is opened).
"""

from __future__ import annotations

import threading
from typing import Any

import orjson
import pytest

from tif1 import core, first_wave, payload_loader, session_lookahead
from tif1.cache import get_cache
from tif1.cdn import get_cdn_manager
from tif1.config import get_config

RACE = "Race"
AUS, CHN, JPN, BHR = (
    "Australian%20Grand%20Prix",
    "Chinese%20Grand%20Prix",
    "Japanese%20Grand%20Prix",
    "Bahrain%20Grand%20Prix",
)


def _url(gp: str, path: str) -> str:
    return get_cdn_manager().get_sources()[0].format_url(2025, gp, RACE, path)


@pytest.fixture
def lookahead(monkeypatch):
    """Lookahead on (the suite turns ``cdn_preconnect`` off) with prefetches recorded."""
    config = get_config()
    monkeypatch.setitem(config._config, "cdn_preconnect", True)
    monkeypatch.setitem(config._config, "session_lookahead", True)
    monkeypatch.setitem(config._config, "offline_mode", False)
    monkeypatch.setitem(config._config, "ci_mode", False)
    calls: list[list[str]] = []
    discarded: list[str] = []

    def _prefetch(urls: list[str], *, timeout: float) -> list[str]:
        assert timeout >= 1
        calls.append(list(urls))
        return list(urls)

    monkeypatch.setattr(first_wave, "prefetch", _prefetch)
    monkeypatch.setattr(first_wave, "discard", lambda urls: discarded.extend(urls))
    session_lookahead.reset()
    yield calls, discarded
    session_lookahead.reset()


def _visit(name: str, *paths: str, classification: str | None = None) -> bool | None:
    """``get_session``'s call for a 2025 race, then the fetches its loads make."""
    probe = session_lookahead.on_get_session(2025, name, RACE, None, True)
    gp = name.replace(" ", "%20")
    for path in paths:
        session_lookahead.note_fetch(2025, gp, RACE, path)
    if classification is not None:
        session_lookahead.note_classification(2025, gp, RACE, classification)
    return probe


def test_second_round_fetches_the_third_rounds_files(lookahead):
    calls, _discarded = lookahead
    assert _visit("Australian Grand Prix", "drivers.json", "session_laptimes.json") is None
    assert calls == []  # one session is no sequence
    assert _visit("Chinese Grand Prix") is False  # the probe ran on the current round
    assert calls == [[_url(JPN, "drivers.json"), _url(JPN, "session_laptimes.json")]]


def test_only_session_level_files_of_the_current_session_count(lookahead):
    calls, _discarded = lookahead
    _visit("Australian Grand Prix", "drivers.json", "VER/1_tel.json", "VER/tel.v2.1.bin")
    session_lookahead.note_fetch(2025, BHR, RACE, "weather.json")  # another session
    session_lookahead.note_fetch(2025, AUS, "Qualifying", "rcm.json")
    _visit("Chinese Grand Prix")
    assert calls == [[_url(JPN, "drivers.json")]]


def test_load_files_follow_what_the_previous_round_fetched(lookahead):
    calls, _discarded = lookahead
    paths = ("drivers.json", "rcm.json", "session_laptimes.json", "weather.json")
    _visit("Australian Grand Prix", *paths)
    _visit("Chinese Grand Prix")
    assert calls == [[_url(JPN, path) for path in sorted(paths)]]


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("Australian Grand Prix", "Japanese Grand Prix"),  # a round skipped
        ("Chinese Grand Prix", "Australian Grand Prix"),  # backwards
        ("Australian Grand Prix", "Australian Grand Prix"),  # the same round again
    ],
)
def test_no_lookahead_out_of_sequence(lookahead, first, second):
    calls, _discarded = lookahead
    _visit(first, "drivers.json")
    _visit(second, "drivers.json")
    assert calls == []


def test_other_session_type_or_season_breaks_the_sequence(lookahead):
    calls, _discarded = lookahead
    _visit("Australian Grand Prix", "drivers.json")
    session_lookahead.on_get_session(2025, "Chinese Grand Prix", "Qualifying", None, True)
    session_lookahead.on_get_session(2024, "Chinese Grand Prix", RACE, None, True)
    assert calls == []


def test_a_cached_previous_round_starts_nothing(lookahead):
    calls, _discarded = lookahead
    _visit("Australian Grand Prix")  # every file came from the cache
    _visit("Chinese Grand Prix")
    assert calls == []


def test_a_cached_current_round_starts_nothing(lookahead):
    calls, _discarded = lookahead
    get_cache().set(f"2025/{CHN}/{RACE}/drivers.json", {"drivers": []})
    _visit("Australian Grand Prix", "drivers.json")
    assert _visit("Chinese Grand Prix") is True
    assert calls == []


def test_a_cached_next_round_is_not_fetched(lookahead):
    calls, _discarded = lookahead
    get_cache().set(f"2025/{JPN}/{RACE}/drivers.json", {"drivers": []})
    _visit("Australian Grand Prix", "drivers.json")
    _visit("Chinese Grand Prix")
    assert calls == []


def test_the_last_round_has_no_next(lookahead):
    calls, _discarded = lookahead
    _visit("Qatar Grand Prix", "drivers.json")
    _visit("Abu Dhabi Grand Prix")
    assert calls == []


def test_a_session_that_has_not_started_is_not_fetched(lookahead, monkeypatch):
    calls, _discarded = lookahead
    monkeypatch.setattr(session_lookahead, "_session_started", lambda *_args: False)
    _visit("Australian Grand Prix", "drivers.json")
    _visit("Chinese Grand Prix")
    assert calls == []


@pytest.mark.parametrize(
    ("value", "started"),
    [
        ("2025-03-23T07:00:00", True),
        ("2999-03-23T07:00:00", False),
        ("2999-03-23T07:00:00+00:00", False),
        ("not a date", True),
        (None, True),
    ],
)
def test_session_started_reads_the_schedule_dates(monkeypatch, value, started):
    from tif1 import events

    metadata = {"Some Grand Prix": {"Session5DateUtc": value}}
    monkeypatch.setattr(events, "_record_metadata", lambda _record: metadata)
    assert session_lookahead._session_started(2025, "Some Grand Prix", 5, None) is started
    assert session_lookahead._session_started(2025, "Other Grand Prix", 5, None) is True


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("session_lookahead", False),
        ("cdn_preconnect", False),
        ("offline_mode", True),
        ("ci_mode", True),
    ],
)
def test_switches_turn_it_off(lookahead, monkeypatch, key, value):
    calls, _discarded = lookahead
    monkeypatch.setitem(get_config()._config, key, value)
    _visit("Australian Grand Prix", "drivers.json")
    _visit("Chinese Grand Prix")
    assert calls == []


def test_env_switch(monkeypatch):
    from tif1.config import Config

    monkeypatch.setenv("TIF1_SESSION_LOOKAHEAD", "false")
    config = get_config()
    snapshot = dict(config._config)
    try:
        Config._load_env(config)
        assert config.get("session_lookahead") is False
    finally:
        config._config.clear()
        config._config.update(snapshot)
    assert config.get("session_lookahead") is True


def test_ergast_only_seasons_are_left_alone(lookahead):
    calls, _discarded = lookahead
    session_lookahead.on_get_session(1998, "Australian Grand Prix", RACE, None, True)
    session_lookahead.note_fetch(1998, AUS, RACE, "drivers.json")
    session_lookahead.on_get_session(1998, "Brazilian Grand Prix", RACE, None, True)
    assert calls == []


class _FakeStats:
    """The Stats season index of 2025 (cached) for :func:`_stats_url`."""

    def __init__(self, cached: bool = True) -> None:
        self.cached = cached

    def season_index_cached(self, _season: int) -> bool:
        return self.cached

    def round_info(self, _season: int, round_number: int) -> Any:
        slugs = {1: "australian-grand-prix", 2: "chinese-grand-prix", 3: "japanese-grand-prix"}
        slug = slugs.get(round_number)
        return None if slug is None else type("Info", (), {"slug": slug})()


def _stats(slug: str) -> str:
    return (
        f"https://cdn.jsdelivr.net/gh/TracingInsights-Archive/Stats@main/2025/{slug}/results.json"
    )


def test_classification_of_this_round_and_the_next(lookahead, monkeypatch):
    calls, _discarded = lookahead
    from tif1.ergast import stats

    monkeypatch.setattr(stats, "get_stats_source", lambda: _FakeStats())
    _visit("Australian Grand Prix", "drivers.json", classification="results")
    _visit("Chinese Grand Prix")
    assert calls == [
        [_stats("chinese-grand-prix"), _url(JPN, "drivers.json"), _stats("japanese-grand-prix")]
    ]


def test_no_classification_without_a_cached_season_index(lookahead, monkeypatch):
    calls, _discarded = lookahead
    from tif1.ergast import stats

    monkeypatch.setattr(stats, "get_stats_source", lambda: _FakeStats(cached=False))
    _visit("Australian Grand Prix", "drivers.json", classification="results")
    _visit("Chinese Grand Prix")
    assert calls == [[_url(JPN, "drivers.json")]]


def test_fresh_classification_is_not_fetched(lookahead, monkeypatch):
    calls, _discarded = lookahead
    from tif1.ergast import payloads, stats

    monkeypatch.setattr(stats, "get_stats_source", lambda: _FakeStats())
    monkeypatch.setattr(payloads.ErgastPayloadLoader, "is_fresh", lambda _self, *_args: True)
    _visit("Australian Grand Prix", "drivers.json", classification="results")
    _visit("Chinese Grand Prix")
    assert calls == [[_url(JPN, "drivers.json")]]


def test_slots_of_a_passed_round_are_dropped(lookahead):
    calls, discarded = lookahead
    _visit("Australian Grand Prix", "drivers.json")
    _visit("Chinese Grand Prix", "drivers.json")
    ahead = calls[-1]
    _visit("Japanese Grand Prix", "drivers.json")  # takes the slots fetched for it
    assert discarded == []
    _visit("Australian Grand Prix")  # the sequence breaks: every slot goes
    assert set(ahead) <= set(discarded)
    assert _url("Bahrain%20Grand%20Prix", "drivers.json") in discarded


def test_get_session_hands_the_probe_to_the_session(lookahead, monkeypatch):
    calls, _discarded = lookahead
    monkeypatch.setattr(core, "_preconnect_started", True)  # the first wave ran already
    core.get_session(2025, 1, RACE, lib="pandas")
    session_lookahead.note_fetch(2025, AUS, RACE, "drivers.json")
    session = core.get_session(2025, 2, RACE, lib="pandas")
    assert session._memo.has_session_data is False
    assert calls == [[_url(JPN, "drivers.json")]]


def test_load_ergast_payload_notes_the_classification(lookahead, monkeypatch):
    session = core.Session(2025, "Australian Grand Prix", RACE, enable_cache=True)
    session_lookahead.on_get_session(2025, "Australian Grand Prix", RACE, None, True)
    monkeypatch.setattr(session, "_ergast_enabled", True, raising=False)
    monkeypatch.setattr(session, "_ergast_table_race", lambda _endpoint: {"round": "1"})
    session._load_ergast_payload()
    assert session_lookahead._current is not None
    assert session_lookahead._current.classification == "results"


# -- slots over the first wave's connections ----------------------------------


class _Response:
    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self._body = body
        self.will_close = False

    def read(self) -> bytes:
        return self._body

    def getheader(self, _name: str) -> str | None:
        return None


class _Connection:
    def __init__(self, server: _Server) -> None:
        self.server = server
        self.sock = None
        self.target = ""

    def request(self, method: str, target: str, headers: dict[str, str]) -> None:  # noqa: ARG002
        self.target = target
        with self.server.lock:
            self.server.requests.append(target)

    def getresponse(self) -> _Response:
        status, body = self.server.routes.get(self.target.rsplit("/", 1)[-1], (404, b""))
        return _Response(status, body)

    def close(self) -> None:
        pass


class _Server:
    def __init__(self) -> None:
        self.routes: dict[str, tuple[int, bytes]] = {}
        self.requests: list[str] = []
        self.lock = threading.Lock()

    def factory(self, *_args: Any, **_kwargs: Any) -> _Connection:
        return _Connection(self)


@pytest.fixture
def server(monkeypatch):
    first_wave.reset()
    fake = _Server()
    monkeypatch.setattr(first_wave, "_connection_factory", fake.factory)
    monkeypatch.setattr(first_wave, "_tls_context", lambda: "tls-context")
    yield fake
    first_wave.reset()


def test_prefetch_needs_a_wave(server):
    assert first_wave.prefetch([_url(CHN, "drivers.json")], timeout=5) == []


def test_prefetched_bodies_are_handed_over_once(server):
    body = orjson.dumps({"drivers": [{"driver": "VER"}]})
    server.routes["drivers.json"] = (200, body)
    first_wave.start([_url(AUS, "drivers.json")], timeout=5)
    assert first_wave.take(_url(AUS, "drivers.json")) == body
    urls = [_url(CHN, "drivers.json"), _url(CHN, "session_laptimes.json")]
    assert first_wave.prefetch([*urls, urls[0]], timeout=5) == urls
    assert first_wave.prefetch(urls, timeout=5) == []  # slots exist already
    assert first_wave.pending(urls[0])
    assert first_wave.take(urls[0]) == body
    assert first_wave.take(urls[1]) is None  # a 404: niquests fetches it
    assert not first_wave.pending(urls[0])


def test_prefetch_waits_for_the_wave_client(server, monkeypatch):
    server.routes["drivers.json"] = (200, b"{}")
    started = threading.Event()
    release = threading.Event()

    def _slow_context() -> str:
        started.set()
        release.wait(5)
        return "tls-context"

    monkeypatch.setattr(first_wave, "_tls_context", _slow_context)
    first_wave.start([_url(AUS, "drivers.json")], timeout=5)
    assert started.wait(5)
    url = _url(CHN, "drivers.json")
    assert first_wave.prefetch([url], timeout=5) == [url]
    release.set()
    assert first_wave.take(url) == b"{}"


def test_discarded_slots_are_not_handed_over(server):
    server.routes["drivers.json"] = (200, b"{}")
    first_wave.start([_url(AUS, "drivers.json")], timeout=5)
    url = _url(CHN, "drivers.json")
    first_wave.prefetch([url], timeout=5)
    first_wave.discard([url])
    assert not first_wave.pending(url)
    assert first_wave.take(url) is None


def test_the_sync_transport_takes_a_prefetched_body(server, monkeypatch):
    body = orjson.dumps({"MRData": {"RaceTable": {"Races": []}}})
    server.routes["results.json"] = (200, body)
    server.routes["drivers.json"] = (200, b"{}")
    first_wave.start([_url(AUS, "drivers.json")], timeout=5)
    url = _stats("chinese-grand-prix")
    first_wave.prefetch([url], timeout=5)
    monkeypatch.setattr(payload_loader, "_import_niquests", lambda: pytest.fail("niquests"))
    assert payload_loader.NiquestsTransport().get_json(url) == orjson.loads(body)
    assert not first_wave.pending(url)
    # One request for the file: the transport took the slot instead of asking again.
    assert sum(target.endswith("/results.json") for target in server.requests) == 1
