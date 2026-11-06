"""Cold Ergast-only sessions (Corinth C2): no requests for tables a season cannot
have, the Stats files in the first wave, a round's files fetched in one wave.

Offline: the Ergast fixtures behind an in-memory transport, the first wave replaced
by a recorder or a fake connection factory.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import pytest

import tif1
from tif1 import core, first_wave
from tif1.config import get_config
from tif1.core_utils.constants import ERGAST_FIRST_LAPS_SEASON, ERGAST_FIRST_PIT_STOPS_SEASON
from tif1.ergast.payloads import ErgastPayloadLoader
from tif1.payload_loader import InMemoryTransport

STATS = "https://cdn.jsdelivr.net/gh/TracingInsights-Archive/Stats@main"


def _record_requests(transport: InMemoryTransport, monkeypatch) -> list[str]:
    urls: list[str] = []
    original = transport.get_json

    def get_json(url: str, *, timeout=None):
        urls.append(url)
        return original(url, timeout=timeout)

    monkeypatch.setattr(transport, "get_json", get_json)
    return urls


def test_table_coverage_follows_ergast_seasons():
    assert ERGAST_FIRST_LAPS_SEASON == 1996
    assert ERGAST_FIRST_PIT_STOPS_SEASON == 2011
    assert not core._ergast_table_covers("laps", 1995)
    assert core._ergast_table_covers("laps", 1996)
    assert not core._ergast_table_covers("pitstops", 2010)
    assert core._ergast_table_covers("pitstops", 2011)
    for endpoint in ("results", "qualifying", "sprint"):
        assert core._ergast_table_covers(endpoint, 1950)


def test_pre_1996_race_never_asks_for_lap_timings_or_pit_stops(ergast_offline, monkeypatch):
    urls = _record_requests(ergast_offline, monkeypatch)
    session = tif1.get_session(1950, "British Grand Prix", "Race")
    assert session.laps.empty
    assert session.pit_stops.empty
    assert list(session.pit_stops.columns) == [
        "DriverNumber",
        "Driver",
        "DriverId",
        "Stop",
        "Lap",
        "TimeOfDay",
        "Duration",
    ]
    assert len(session.results) > 0
    assert urls, "the classification is still fetched"
    assert not any("laptimes" in url or "laps.json" in url for url in urls)
    assert not any("pitstops" in url for url in urls)


def test_round_files_are_fetched_in_one_wave(ergast_offline, monkeypatch):
    get_config().set("ergast_fallback", False)
    waves: list[list[tuple[int, str, str]]] = []
    original = ErgastPayloadLoader.stats_payloads

    def stats_payloads(self, refs):
        waves.append(list(refs))
        return original(self, refs)

    monkeypatch.setattr(ErgastPayloadLoader, "stats_payloads", stats_payloads)
    urls = _record_requests(ergast_offline, monkeypatch)
    session = tif1.get_session(2025, "Chinese Grand Prix", "Race", enable_cache=False)
    session._ergast_only = True  # a 2025 race on the pre-2018 code path
    laps = session.laps
    assert len(laps) == 1065
    assert waves[0] == [
        (2025, "chinese-grand-prix", "results.json"),
        (2025, "chinese-grand-prix", "laptimes.json"),
        (2025, "chinese-grand-prix", "pitstops.json"),
    ]
    # The readers answer from the memo the wave filled: one request per file.
    files = [url.rsplit("/", 1)[-1] for url in urls]
    assert sorted(files) == ["events.json", "laptimes.json", "pitstops.json", "results.json"]
    norris = laps[laps["Driver"] == "NOR"].sort_values("LapNumber")
    assert norris["LapTime"].iloc[0] == pd.Timedelta(seconds=99.268)
    assert norris["Stint"].max() >= 2  # the pit stops reached the laps


def test_round_prefetch_skips_a_memoized_classification_and_offline(ergast_offline, monkeypatch):
    waves: list[list[tuple[int, str, str]]] = []

    def stats_payloads(_self, refs):
        waves.append(list(refs))
        return []

    monkeypatch.setattr(ErgastPayloadLoader, "stats_payloads", stats_payloads)
    session = tif1.get_session(2025, "Chinese Grand Prix", "Race", enable_cache=False)
    session._ergast_loaded = True
    session._prefetch_ergast_tables(("results", "laps", "pitstops"))
    assert waves == [
        [
            (2025, "chinese-grand-prix", "laptimes.json"),
            (2025, "chinese-grand-prix", "pitstops.json"),
        ]
    ]
    monkeypatch.setitem(get_config()._config, "offline_mode", True)
    session._prefetch_ergast_tables(("results", "laps", "pitstops"))
    assert len(waves) == 1
    # One table left is no wave: its reader fetches it as before.
    old = tif1.get_session(1998, "Monaco Grand Prix", "Race", enable_cache=False)
    monkeypatch.setitem(get_config()._config, "offline_mode", False)
    old._ergast_loaded = True
    old._prefetch_ergast_tables(("results", "laps", "pitstops"))
    assert len(waves) == 1


@pytest.mark.parametrize(
    ("year", "gp", "session_name", "files"),
    [
        (1998, "Monaco Grand Prix", "Race", ["results.json", "laptimes.json"]),
        (1975, "British Grand Prix", "Race", ["results.json"]),
        (2012, "Italian Grand Prix", "Race", ["results.json", "laptimes.json", "pitstops.json"]),
        (1998, "Monaco Grand Prix", "Qualifying", ["quali_results.json"]),
    ],
)
def test_ergast_first_wave_urls(year, gp, session_name, files):
    slug = gp.lower().replace(" ", "-")
    assert core._ergast_first_wave_urls(year, gp, session_name) == [
        f"{STATS}/{year}/events.json",
        *[f"{STATS}/{year}/{slug}/{name}" for name in files],
    ]


@pytest.fixture
def wave(monkeypatch):
    """Turn the pre-connect on and record what the first wave is started with."""
    monkeypatch.setitem(get_config()._config, "cdn_preconnect", True)
    monkeypatch.setattr(core, "_preconnect_started", False)
    monkeypatch.setattr(core, "THREADS_AVAILABLE", True)
    calls: list[tuple[list[str], float, float | None]] = []

    def _record(urls: list[str], *, timeout: float, wait_limit: float | None = None) -> None:
        calls.append((list(urls), timeout, wait_limit))

    monkeypatch.setattr(first_wave, "start", _record)
    return calls


def test_uncached_ergast_only_session_starts_the_stats_wave(wave):
    assert core._preconnect_cdn(1998, "Monaco Grand Prix", "Race", None) is False
    assert len(wave) == 1
    urls, timeout, wait_limit = wave[0]
    assert urls == core._ergast_first_wave_urls(1998, "Monaco Grand Prix", "Race")
    # The Stats files come from cold edges: the wave is waited for up to the timeout.
    assert wait_limit == float(timeout)


def test_timing_session_wave_is_unchanged(wave):
    core._preconnect_cdn(2025, "Bahrain Grand Prix", "Race", None)
    urls, _timeout, wait_limit = wave[0]
    assert [url.rsplit("/", 1)[-1] for url in urls] == list(core._FIRST_WAVE_PATHS)
    assert wait_limit is None


_FUZZY_CHILD = """
import sys
import tif1
from tif1 import fuzzy
session = tif1.get_session(1975, "British", "R", lib="polars")
assert session.event_name == "British Grand Prix", session.event_name
assert fuzzy.fuzzy_matcher("bahrain", [["Bahrain GP", "Sakhir"], ["Testing", "Sakhir"]]) == (
    0,
    True,
)
print("numpy" in sys.modules, "pandas" in sys.modules)
"""


def test_fuzzy_event_names_never_import_numpy(tmp_path):
    # Corinth C2: a fuzzy name ("British") on a polars session used to import
    # numpy (~100-190 ms) for the fuzzy matcher's ratio matrix.
    package_root = str(Path(tif1.__file__).resolve().parent.parent)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([package_root, *filter(None, [env.get("PYTHONPATH")])])
    env.update(
        HOME=str(tmp_path),
        USERPROFILE=str(tmp_path),
        TIF1_CACHE_DIR=str(tmp_path / "cache"),
        TIF1_OFFLINE_MODE="true",
        TIF1_PRELOAD_IMPORTS="0",
    )
    completed = subprocess.run(
        [sys.executable, "-c", _FUZZY_CHILD],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        cwd=tmp_path,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.split() == ["False", "False"]


def test_fuzzy_ties_keep_the_first_best_row():
    from tif1.fuzzy import _fuzzy_match

    # No substring hit: every row ties at 0 and the first wins, as numpy's argmax.
    assert _fuzzy_match("zzz", [["aaa"], ["bbb"]]) == (0, False, 0.0)
    # A tie on a shared feature is broken on the features only one row has.
    reference = [["Pre-Season Testing", "Sakhir"], ["Bahrain Grand Prix", "Sakhir"]]
    index, exact, score = _fuzzy_match("sakhirbahrain", reference)
    assert (index, exact) == (1, False)
    assert isinstance(score, float)


def test_wave_wait_limit_sets_the_take_deadline(monkeypatch):
    first_wave.reset()
    monkeypatch.setattr(first_wave, "_tls_context", lambda: None)  # no connections
    monkeypatch.setattr(first_wave, "_run_wave", lambda *_args: None)
    try:
        first_wave.start(["https://cdn.jsdelivr.net/a.json"], timeout=5, wait_limit=20.0)
        first_wave.start(["https://cdn.jsdelivr.net/b.json"], timeout=5)
        now = time.monotonic()
        long_wait = first_wave._slots["https://cdn.jsdelivr.net/a.json"].deadline - now
        default = first_wave._slots["https://cdn.jsdelivr.net/b.json"].deadline - now
        assert 19 < long_wait <= 20
        assert first_wave.WAIT_LIMIT - 1 < default <= first_wave.WAIT_LIMIT
    finally:
        first_wave.reset()
