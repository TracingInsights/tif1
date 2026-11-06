"""The cold ``tif1.ergast`` query path (Corinth C3), offline.

- ``from tif1.ergast import Ergast`` does not import pandas; the pandas
  response classes live in ``tif1.ergast.frames`` and keep their public path.
- A Stats-routed query runs in a thread while the frame modules import; its
  answer or exception is handed back; Jolpica-routed queries run inline.
- A cold season fetches ``events.json`` and the round files the query reads
  (predicted from the packaged schedule) in one ``stats_payloads`` wave.
- The Stats files go over the first wave's stdlib client when niquests has no
  session: ``200`` answers, ``404``/``403`` are the refusals the mirror walk
  expects, anything else goes to the transport.
"""

from __future__ import annotations

import os
import pickle
import subprocess
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path

import pytest

import tif1
from tif1 import first_wave, http_session, payload_loader
from tif1.config import get_config
from tif1.ergast import interface, router, structure
from tif1.ergast import payloads as ergast_payloads
from tif1.ergast.payloads import ErgastPayloadLoader
from tif1.ergast.router import QueryPlan, routes_to_stats
from tif1.ergast.stats import RoundInfo, StatsSource, get_stats_source
from tif1.exceptions import DataNotFoundError, NetworkError

NOW = datetime(2026, 10, 28, tzinfo=UTC)


def _plan(endpoint: str, category: dict, subcategory: dict | None, **selectors) -> QueryPlan:
    source = selectors.pop("source", "auto")
    return QueryPlan(endpoint, "RaceTable", category, subcategory, selectors, None, None, source)


def _results_plan(**selectors) -> QueryPlan:
    return _plan("results", structure.Races_RaceResults, structure.RaceResults, **selectors)


# -- lazy pandas ----------------------------------------------------------------

_CHILD = r"""
import pickle
import sys

from tif1.ergast import Ergast, interface

Ergast()
print("pandas" in sys.modules)
from tif1.ergast import ErgastResultFrame, ErgastSimpleResponse
from tif1.ergast.interface import ErgastResultSeries

print(ErgastSimpleResponse.__module__, ErgastResultFrame.__module__, ErgastResultSeries.__module__)
print(issubclass(ErgastSimpleResponse, interface.ErgastResponseMixin))
frame = ErgastResultFrame({"a": [1, 2]})
print(type(pickle.loads(pickle.dumps(frame))).__name__, type(frame["a"]).__name__)
"""


def test_ergast_import_does_not_load_pandas(tmp_path: Path) -> None:
    package_root = str(Path(tif1.__file__).resolve().parent.parent)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([package_root, *filter(None, [env.get("PYTHONPATH")])])
    env["HOME"] = str(tmp_path)
    env["USERPROFILE"] = str(tmp_path)
    env["TIF1_CACHE_DIR"] = str(tmp_path / "cache")
    completed = subprocess.run(
        [sys.executable, "-c", _CHILD],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        cwd=tmp_path,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines() == [
        "False",
        "tif1.ergast.interface tif1.ergast.interface tif1.ergast.interface",
        "True",
        "ErgastResultFrame ErgastResultSeries",
    ]


def test_frame_classes_resolve_through_interface() -> None:
    from tif1.ergast import frames

    assert interface.ErgastSimpleResponse is frames.ErgastSimpleResponse
    assert interface.ErgastResultFrame is frames.ErgastResultFrame
    frame = interface.ErgastResultFrame({"a": [1]})
    assert pickle.loads(pickle.dumps(frame)).equals(frame)
    with pytest.raises(AttributeError):
        _ = interface.NoSuchResponse  # type: ignore[attr-defined]


# -- overlapped execution -----------------------------------------------------------


def test_routes_to_stats() -> None:
    assert routes_to_stats(_results_plan(season=2024))
    assert routes_to_stats(_results_plan(season="2024", round="3"))
    assert not routes_to_stats(_results_plan(season="current"))
    assert not routes_to_stats(_results_plan(season=2024, source="jolpica"))
    assert not routes_to_stats(_results_plan(season=2024, status="1"))
    assert not routes_to_stats(_plan("drivers", structure.Drivers, None, season=2024))


@pytest.fixture
def pending_imports(monkeypatch):
    """Pretend the frame modules still need importing; record the import's thread."""
    record: dict[str, threading.Thread] = {}

    def _import(result_type: str) -> None:
        record["import"] = threading.current_thread()

    monkeypatch.setattr(interface, "_frame_modules_pending", lambda _result_type: True)
    monkeypatch.setattr(interface, "_import_frame_modules", _import)
    return record


def test_stats_query_runs_beside_the_import(monkeypatch, pending_imports) -> None:
    executed = router.ExecutedQuery({"MRData": {}}, "stats", 1.0)

    def _execute(plan: QueryPlan) -> router.ExecutedQuery:
        pending_imports["execute"] = threading.current_thread()
        return executed

    monkeypatch.setattr(interface, "execute", _execute)
    assert interface._execute_overlapped(_results_plan(season=2024), "pandas") is executed
    assert pending_imports["import"] is threading.current_thread()
    assert pending_imports["execute"] is not threading.current_thread()
    assert pending_imports["execute"].name == "tif1-ergast-query"


def test_overlapped_query_raises_its_error(monkeypatch, pending_imports) -> None:
    error = NetworkError(url="stats/2024/events.json", status_code=None)

    def _execute(plan: QueryPlan) -> router.ExecutedQuery:
        raise error

    monkeypatch.setattr(interface, "execute", _execute)
    with pytest.raises(NetworkError) as caught:
        interface._execute_overlapped(_results_plan(season=2024), "pandas")
    assert caught.value is error
    assert "import" in pending_imports


@pytest.mark.parametrize(
    ("plan", "result_type"),
    [
        (_results_plan(season="current"), "pandas"),  # resolved through Jolpica
        (_plan("drivers", structure.Drivers, None, season=2024), "pandas"),
        (_results_plan(season=2024), "raw"),
    ],
)
def test_jolpica_and_raw_queries_run_inline(monkeypatch, plan, result_type) -> None:
    monkeypatch.setattr(interface, "_frame_modules_pending", lambda rt: rt != "raw")
    events: list[tuple[str, threading.Thread]] = []

    def _import(result_type: str) -> None:
        events.append(("import", threading.current_thread()))

    def _execute(plan: QueryPlan) -> router.ExecutedQuery:
        events.append(("execute", threading.current_thread()))
        return router.ExecutedQuery({"MRData": {}}, "jolpica", 1.0)

    monkeypatch.setattr(interface, "_import_frame_modules", _import)
    monkeypatch.setattr(interface, "execute", _execute)
    interface._execute_overlapped(plan, result_type)
    main = threading.current_thread()
    # A Jolpica-routed query imports first, then fetches (the eager import's order).
    expected = (
        [("execute", main)] if result_type == "raw" else [("import", main), ("execute", main)]
    )
    assert events == expected


def test_frame_modules_pending_once_imported() -> None:
    pytest.importorskip("pandas")
    pytest.importorskip("polars")
    pytest.importorskip("pyarrow")
    assert not interface._frame_modules_pending("pandas")
    assert not interface._frame_modules_pending("polars")
    assert not interface._frame_modules_pending("raw")


# -- one wave for a cold season ---------------------------------------------------------


def test_predicted_files_follow_the_handlers() -> None:
    index = {
        1: RoundInfo(1, "Bahrain Grand Prix", "bahrain-grand-prix", {"date": "2025-03-16"}),
        2: RoundInfo(
            2,
            "Chinese Grand Prix",
            "chinese-grand-prix",
            {"date": "2025-03-23", "Sprint": {}, "Circuit": {"circuitId": "shanghai"}},
        ),
        3: RoundInfo(3, "Future Grand Prix", "future-grand-prix", {"date": "2027-01-01"}),
    }

    def files(endpoint: str, round_number: int | None = None, **sel) -> list[tuple[str, str]]:
        return router._predicted_files(endpoint, index, round_number, sel, NOW)

    both = [("bahrain-grand-prix", "results.json"), ("chinese-grand-prix", "results.json")]
    assert files("results") == both
    assert files("results", 2) == [("chinese-grand-prix", "results.json")]
    assert files("results", circuit="shanghai") == [("chinese-grand-prix", "results.json")]
    assert files("sprint") == [("chinese-grand-prix", "sprint_results.json")]
    assert files("qualifying", 1) == [("bahrain-grand-prix", "quali_results.json")]
    assert files("laps", 1) == [("bahrain-grand-prix", "laptimes.json")]
    assert files("laps") == []
    assert files("pitstops", 2) == [("chinese-grand-prix", "pitstops.json")]
    assert files("driverStandings") == [("chinese-grand-prix", "driverPoints.json")]
    assert files("constructorStandings", 1) == [("bahrain-grand-prix", "teamPoints.json")]
    assert files("constructors") == both
    assert files("races") == []
    assert files("races", driver="max_verstappen") == both
    assert files("circuits", circuit="shanghai") == []
    assert files("circuits", circuit="shanghai", driver="x") == both
    assert files("drivers") == []


def test_packaged_index_predicts_the_stats_slugs() -> None:
    index = StatsSource.packaged_index(2024)
    assert index is not None
    assert len(index) == 24
    assert index[21].slug == "são-paulo-grand-prix"
    assert index[6].has_sprint
    assert StatsSource.packaged_index(1900) is None


@pytest.mark.usefixtures("ergast_offline")
def test_cold_season_fetches_events_and_round_files_in_one_wave(monkeypatch) -> None:
    stats = get_stats_source()
    waves: list[list[tuple[int, str, str]]] = []
    original = ErgastPayloadLoader.stats_payloads

    def _spy(self, refs):
        waves.append(list(refs))
        return original(self, refs)

    monkeypatch.setattr(ErgastPayloadLoader, "stats_payloads", _spy)
    plan = _plan(
        "qualifying",
        structure.Races_QualifyingResults,
        structure.QualifyingResults,
        season=2025,
        round=2,
    )
    first = router.execute(plan, stats=stats)
    assert waves[0] == [
        (2025, "", "events.json"),
        (2025, "chinese-grand-prix", "quali_results.json"),
    ]
    # The handler read the round file from the memo: no second fetch.
    assert waves[1:] == [[(2025, "chinese-grand-prix", "quali_results.json")]]
    transport = stats.loader.transport
    assert sum("quali_results.json" in url for url in transport.calls) == 1
    # Warm: the season index is cached, no speculative wave.
    waves.clear()
    assert router.execute(plan, stats=stats).payload == first.payload
    assert waves == [[(2025, "chinese-grand-prix", "quali_results.json")]]


@pytest.mark.usefixtures("ergast_offline")
def test_mispredicted_slug_costs_a_request_not_the_answer(monkeypatch) -> None:
    stats = get_stats_source()
    plan = _plan(
        "qualifying",
        structure.Races_QualifyingResults,
        structure.QualifyingResults,
        season=2025,
        round=2,
    )
    expected = router.execute(plan, stats=stats).payload
    ergast_payloads.reset_ergast_loader()
    stats = StatsSource()
    stale = {2: RoundInfo(2, "Old Name", "old-name", {"date": "2025-03-23"})}
    monkeypatch.setattr(StatsSource, "packaged_index", staticmethod(lambda _season: stale))
    assert router.execute(plan, stats=stats).payload == expected
    calls = stats.loader.transport.calls
    assert any("/2025/old-name/quali_results.json" in url for url in calls)


@pytest.mark.usefixtures("ergast_offline")
def test_cold_wave_raises_the_events_error_once(monkeypatch, ergast_offline) -> None:
    error = NetworkError(url="2025/events.json", status_code=None)
    ergast_offline.add("2025/events.json", error)
    monkeypatch.setitem(get_config()._config, "max_retries", 0)
    with pytest.raises(NetworkError):
        router.execute(_results_plan(season=2025, round=2), stats=get_stats_source())
    events_calls = [url for url in ergast_offline.calls if url.endswith("/2025/events.json")]
    # One walk over the mirrors (jsDelivr, StaticDelivr), not two.
    assert len(events_calls) == len(set(events_calls))


@pytest.mark.usefixtures("ergast_offline")
def test_no_cold_wave_offline(monkeypatch) -> None:
    monkeypatch.setitem(get_config()._config, "offline_mode", True)
    called: list[int] = []
    monkeypatch.setattr(
        StatsSource, "packaged_index", staticmethod(lambda season: called.append(season))
    )
    with pytest.raises(NetworkError):  # offline, nothing cached
        router.execute(_results_plan(season=2025, source="stats"), stats=get_stats_source())
    assert called == []


# -- Stats files over the stdlib client ------------------------------------------------


@pytest.fixture
def stdlib_client(monkeypatch):
    """A built stdlib client whose answers the test sets; niquests has no session."""
    answers: dict[str, tuple[int | None, bytes | None]] = {}
    fallback: list[str] = []

    class _Client:
        def get_with_status(self, url: str, timeout: float) -> tuple[int | None, bytes | None]:
            assert timeout > 0
            answer = answers.get(url, ConnectionResetError("reset by peer"))
            if isinstance(answer, BaseException):
                raise answer
            return answer

    monkeypatch.setattr(first_wave, "standalone_client", _Client)
    monkeypatch.setattr(http_session, "_shared_session", None)

    def _niquests(self, url: str, *, timeout: float | None = None) -> dict:
        fallback.append(url)
        return {"from": "niquests"}

    monkeypatch.setattr(payload_loader.NiquestsTransport, "get_json", _niquests)
    return answers, fallback


def test_stdlib_get_json_statuses(stdlib_client) -> None:
    answers, fallback = stdlib_client
    get_json = ergast_payloads._stdlib_get_json(payload_loader.NiquestsTransport())
    assert get_json is not None
    answers["https://x/ok.json"] = (200, b'{"MRData": {}}')
    answers["https://x/missing.json"] = (404, None)
    answers["https://x/refused.json"] = (403, None)
    answers["https://x/list.json"] = (200, b"[1]")
    answers["https://x/moved.json"] = (301, None)
    assert get_json("https://x/ok.json", timeout=5) == {"MRData": {}}
    with pytest.raises(DataNotFoundError):
        get_json("https://x/missing.json", timeout=5)
    with pytest.raises(NetworkError) as refused:
        get_json("https://x/refused.json", timeout=5)
    assert refused.value.context["status_code"] == 403
    assert get_json("https://x/list.json", timeout=5) == {"from": "niquests"}
    assert get_json("https://x/moved.json", timeout=5) == {"from": "niquests"}
    assert get_json("https://x/error.json", timeout=5) == {"from": "niquests"}
    assert fallback == ["https://x/list.json", "https://x/moved.json", "https://x/error.json"]


def test_stdlib_get_json_only_for_cold_niquests_transport(monkeypatch, stdlib_client) -> None:
    assert ergast_payloads._stdlib_get_json(payload_loader.InMemoryTransport({})) is None
    monkeypatch.setitem(get_config()._config, "cdn_first_wave", False)
    assert ergast_payloads._stdlib_get_json(payload_loader.NiquestsTransport()) is None
    monkeypatch.setitem(get_config()._config, "cdn_first_wave", True)
    monkeypatch.setattr(http_session, "_shared_session", object())
    monkeypatch.setattr(http_session, "warm_connections_pending", lambda: False)
    assert ergast_payloads._stdlib_get_json(payload_loader.NiquestsTransport()) is None
    monkeypatch.setattr(http_session, "_shared_session", None)
    monkeypatch.setattr(first_wave, "standalone_client", lambda: None)
    assert ergast_payloads._stdlib_get_json(payload_loader.NiquestsTransport()) is None


def test_stats_mirror_walk_over_the_stdlib(stdlib_client) -> None:
    answers, fallback = stdlib_client
    loader = ErgastPayloadLoader(transport=payload_loader.NiquestsTransport(), enable_cache=False)
    repo, branch = loader.repo()
    sources = [source for source in loader.cdn_manager.get_sources() if source.serves(repo)]
    assert len(sources) >= 2
    urls = [source.format_repo_url(repo, branch, "2024/events.json") for source in sources]
    answers[urls[0]] = (404, None)
    answers[urls[-1]] = (200, b'{"MRData": {"total": "1"}}')
    payload, persist = loader._fetch_stats("2024/events.json")
    assert payload == {"MRData": {"total": "1"}}
    assert persist
    assert fallback == urls[1:-1]  # any mirror the test did not answer went to niquests


def test_standalone_client_is_built_once_and_never_opens_the_bridge(monkeypatch) -> None:
    first_wave.reset()
    contexts: list[int] = []

    def _context() -> str:
        contexts.append(1)
        return "tls-context"

    monkeypatch.setattr(first_wave, "_tls_context", _context)
    monkeypatch.setattr(http_session, "_shared_session", None)
    try:
        client = first_wave.standalone_client()
        assert client is not None
        assert client.context == "tls-context"
        assert first_wave.standalone_client() is client
        assert contexts == [1]
        # The session path's bridge and single-request seam stay closed.
        assert not first_wave.has_client()
        assert not first_wave.bridge_open()
    finally:
        first_wave.reset()
    assert first_wave._shared is None


def test_standalone_client_reuses_the_wave_client(monkeypatch) -> None:
    first_wave.reset()
    wave_client = object()
    monkeypatch.setattr(first_wave, "_client", wave_client)
    monkeypatch.setattr(first_wave, "_tls_context", lambda: pytest.fail("no second context"))
    assert first_wave.standalone_client() is wave_client


def test_standalone_client_without_a_trust_store_is_tried_once(monkeypatch) -> None:
    first_wave.reset()
    contexts: list[int] = []

    def _context() -> None:
        contexts.append(1)

    monkeypatch.setattr(first_wave, "_tls_context", _context)
    try:
        assert first_wave.standalone_client() is None
        assert first_wave.standalone_client() is None
        assert contexts == [1]
    finally:
        first_wave.reset()
