"""Session bundles (Corinth C10): the ``session.v1.bin`` codec and its reader.

Offline: the bundle is served by an ``InMemoryTransport`` on the shared URL
loader, the per-file JSON by a stub of the niquests path that records every
URL it is asked for.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import orjson
import pytest

from tif1 import async_fetch, core, first_wave, session_bundle
from tif1.cache import get_cache
from tif1.config import get_config
from tif1.core_utils.session_bundle import (
    BUNDLE_FILE,
    MAGIC,
    SESSION_MEMBERS,
    SessionBundleError,
    decode_bundle,
    encode_bundle,
)
from tif1.ergast.payloads import ErgastPayloadLoader, cache_only
from tif1.exceptions import DataNotFoundError
from tif1.payload_loader import InMemoryTransport, PayloadLoader, get_url_loader

YEAR, GP, SESSION = 2025, "Test%20Grand%20Prix", "Race"
FILES: dict[str, dict[str, Any]] = {
    "drivers.json": {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull Racing"}]},
    "session_laptimes.json": {"VER": {"lap": [1, 2], "time": [95.1, 94.2]}},
    "session_info.json": {"t0_date": "2025-04-13T14:03:01.000000Z"},
    "weather.json": {"wT": [0.0], "wAT": [29.1]},
    "rcm.json": {"time": ["2025-04-13T15:00:00"], "msg": ["GREEN LIGHT"]},
}
STATS_RESULTS = "stats/2025/test-grand-prix/results.json"
STATS_EVENTS = "stats/2025/events.json"
STATS: dict[str, dict[str, Any]] = {
    STATS_EVENTS: {"MRData": {"total": "1", "RaceTable": {"Races": [{"round": "4"}]}}},
    STATS_RESULTS: {"MRData": {"total": "1", "RaceTable": {"Races": [{"Results": [1]}]}}},
}


def _bytes(payload: dict[str, Any]) -> bytes:
    # Spaces and key order an orjson round trip would not keep: members stay byte-exact.
    return orjson.dumps(payload, option=orjson.OPT_INDENT_2)


def _bundle(*, absent: tuple[str, ...] = (), built_at: float | None = None, **skip: bool) -> bytes:
    entries = {name: _bytes(body) for name, body in FILES.items() if name not in absent}
    entries.update({name: _bytes(body) for name, body in STATS.items()})
    entries = {name: body for name, body in entries.items() if not skip.get(name.split(".")[0])}
    return encode_bundle(
        entries, absent=absent, built_at=time.time() if built_at is None else built_at, level=3
    )


# -- codec -------------------------------------------------------------------------


def test_a_bundle_round_trips_its_members_byte_for_byte():
    entries = {name: _bytes(body) for name, body in FILES.items()}
    blob = encode_bundle(entries, absent=["extra.json"], built_at=1234.5, level=3)
    assert blob.startswith(MAGIC)
    bundle = decode_bundle(blob)
    assert list(bundle.entries) == list(entries)
    assert all(bundle.entries[name] == body for name, body in entries.items())
    assert bundle.absent == frozenset({"extra.json"})
    assert bundle.built_at == 1234.5


def test_an_empty_member_and_an_empty_bundle_round_trip():
    bundle = decode_bundle(encode_bundle({"weather.json": b""}, built_at=0.0, level=1))
    assert bundle.entries == {"weather.json": b""}
    assert decode_bundle(encode_bundle({}, built_at=0.0)).entries == {}


@pytest.mark.parametrize(
    "mutate",
    [
        lambda b: b"NOTABNDL" + b[8:],
        lambda b: b[:-9],
        lambda b: b[:40] + bytes([b[40] ^ 0xFF]) + b[41:],
        lambda b: b[:8] + (1 << 40).to_bytes(8, "little") + b[16:],
        lambda b: b[:10],
    ],
    ids=["magic", "truncated", "flipped-byte", "huge-size", "short"],
)
def test_a_damaged_bundle_is_rejected(mutate):
    blob = encode_bundle({"drivers.json": _bytes(FILES["drivers.json"])}, built_at=1.0, level=3)
    with pytest.raises(SessionBundleError):
        decode_bundle(mutate(blob))


def test_a_bundle_of_another_format_or_with_wrong_sizes_is_rejected(monkeypatch):
    from tif1.core_utils import session_bundle as codec

    monkeypatch.setattr(codec, "FORMAT_VERSION", 2)
    future = encode_bundle({"drivers.json": b"{}"}, built_at=1.0, level=1)
    monkeypatch.undo()
    with pytest.raises(SessionBundleError, match="format"):
        decode_bundle(future)
    head = orjson.dumps({"format": 1, "built_at": 1.0, "entries": [["a", 5]], "absent": []})
    raw = len(head).to_bytes(4, "little") + head + b"abc"
    blob = MAGIC + len(raw).to_bytes(8, "little") + codec._zstd_compress(raw, 1)
    with pytest.raises(SessionBundleError, match="sizes"):
        decode_bundle(blob)


def test_duplicate_or_conflicting_names_are_refused():
    with pytest.raises(SessionBundleError):
        encode_bundle({"a": b"1"}, absent=["a"], built_at=0.0)


# -- reader ------------------------------------------------------------------------


class _Response:
    def __init__(self, status_code: int, body: bytes) -> None:
        self.status_code = status_code
        self.content = body
        self.headers: dict[str, str] = {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class _Stub:
    """Bundles over an InMemoryTransport, the per-file JSON over a fake niquests path."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, bundle: Any | None) -> None:
        self.json_urls: list[str] = []
        files = {} if bundle is None else {f"{GP}/{SESSION}/{BUNDLE_FILE}": bundle}
        self.transport = InMemoryTransport(files)
        monkeypatch.setattr(get_url_loader(), "transport", self.transport)
        monkeypatch.setattr(async_fetch, "_get_async_session", lambda: object())

        def get_resolved(_client: Any, url: str, _timeout: Any) -> _Response:
            self.json_urls.append(url)
            name = url.rsplit("/", 1)[1]
            if name not in FILES:
                return _Response(404, b"")
            return _Response(200, _bytes(FILES[name]))

        monkeypatch.setattr(async_fetch, "_get_resolved", get_resolved)
        config = get_config()._config
        monkeypatch.setitem(config, "offline_mode", False)
        monkeypatch.setitem(config, "session_bundle", True)
        monkeypatch.setitem(config, "cdn_hedge", False)

    @property
    def bundle_urls(self) -> list[str]:
        return [url for url in self.transport.calls if url.endswith(BUNDLE_FILE)]

    def fetch(self, paths: list[str], **kwargs: Any) -> list[Any]:
        kwargs.setdefault("use_cache", False)
        kwargs.setdefault("write_cache", False)
        requests = [(YEAR, GP, SESSION, path) for path in paths]
        return asyncio.run(async_fetch.fetch_multiple_async(requests, **kwargs))


@pytest.fixture(autouse=True)
def _forget_bundles():
    session_bundle.reset()
    yield
    session_bundle.reset()


def test_a_cold_wave_takes_every_member_from_one_bundle(monkeypatch):
    stub = _Stub(monkeypatch, _bundle())
    first = stub.fetch(["drivers.json", "session_laptimes.json"])
    assert first == [FILES["drivers.json"], FILES["session_laptimes.json"]]
    later = stub.fetch(["weather.json", "rcm.json"])
    assert later == [FILES["weather.json"], FILES["rcm.json"]]
    assert len(stub.bundle_urls) == 1
    assert stub.json_urls == []


def test_the_sync_loader_takes_its_file_from_the_bundle(monkeypatch):
    stub = _Stub(monkeypatch, _bundle())
    loader = PayloadLoader(YEAR, GP, SESSION, transport=InMemoryTransport({}), enable_cache=False)
    assert loader.get("session_info.json") == FILES["session_info.json"]
    assert loader.transport.calls == []  # type: ignore[attr-defined]
    assert len(stub.bundle_urls) == 1


def test_bundles_off_by_default_fetch_the_files(monkeypatch):
    stub = _Stub(monkeypatch, _bundle())
    monkeypatch.setitem(get_config()._config, "session_bundle", False)
    assert get_config().get("session_bundle") is False
    assert stub.fetch(["drivers.json"]) == [FILES["drivers.json"]]
    assert stub.bundle_urls == []
    assert len(stub.json_urls) == 1


def test_a_missing_bundle_falls_back_to_the_files_and_is_remembered(monkeypatch):
    stub = _Stub(monkeypatch, None)
    assert stub.fetch(["drivers.json", "session_laptimes.json"]) == [
        FILES["drivers.json"],
        FILES["session_laptimes.json"],
    ]
    assert len(stub.bundle_urls) == 1  # the primary CDN's 404 decides
    assert len(stub.json_urls) == 2
    assert get_cache().is_known_missing(f"{YEAR}/{GP}/{SESSION}/{BUNDLE_FILE}")
    stub.fetch(["weather.json"])
    assert len(stub.bundle_urls) == 1  # no other probe in this process
    session_bundle.reset()  # a new process: the persisted verdict answers
    stub.fetch(["rcm.json"])
    assert len(stub.bundle_urls) == 1


@pytest.mark.parametrize("entry", [b"not a bundle", RuntimeError, TimeoutError])
def test_an_unusable_bundle_falls_back_to_the_files(monkeypatch, entry):
    stub = _Stub(monkeypatch, entry)
    assert stub.fetch(["drivers.json"]) == [FILES["drivers.json"]]
    assert len(stub.json_urls) == 1
    assert not get_cache().is_known_missing(f"{YEAR}/{GP}/{SESSION}/{BUNDLE_FILE}")


def test_an_absent_member_is_not_found_without_a_request(monkeypatch):
    stub = _Stub(monkeypatch, _bundle(absent=("session_info.json",)))
    assert stub.fetch(["session_info.json"], use_cache=True, write_cache=True) == [None]
    assert stub.json_urls == []
    assert get_cache().is_known_missing(f"{YEAR}/{GP}/{SESSION}/session_info.json")
    loader = PayloadLoader(YEAR, GP, SESSION, transport=InMemoryTransport({}), enable_cache=False)
    with pytest.raises(DataNotFoundError):
        loader.get("session_info.json")


def test_a_member_the_bundle_lacks_is_fetched_alone(monkeypatch):
    stub = _Stub(monkeypatch, _bundle(weather=True))
    assert stub.fetch(["weather.json"]) == [FILES["weather.json"]]
    assert [url.rsplit("/", 1)[1] for url in stub.json_urls] == ["weather.json"]


def test_other_paths_never_ask_for_the_bundle(monkeypatch):
    stub = _Stub(monkeypatch, _bundle())
    stub.fetch(["VER/laptimes.json"])
    assert stub.bundle_urls == []
    assert set(SESSION_MEMBERS) == set(FILES)


def test_the_bundle_fills_the_payload_tier_as_the_files_would(monkeypatch):
    stub = _Stub(monkeypatch, _bundle(absent=("rcm.json",), built_at=1_700_000_000.0))
    stub.fetch(["drivers.json"], write_cache=True)
    from tif1.cache import _join_frame_writers

    _join_frame_writers()
    cache = get_cache()
    for name in ("drivers.json", "session_laptimes.json", "session_info.json", "weather.json"):
        key = f"{YEAR}/{GP}/{SESSION}/{name}"
        assert cache.get(key) == FILES[name]
        assert not cache.is_known_missing(key)
    assert cache.is_known_missing(f"{YEAR}/{GP}/{SESSION}/rcm.json")
    entry = cache.get_entry("json", STATS_RESULTS)
    assert entry == {"fetched_at": 1_700_000_000.0, "payload": STATS[STATS_RESULTS]}


def test_write_cache_off_leaves_the_tiers_alone(monkeypatch):
    stub = _Stub(monkeypatch, _bundle())
    stub.fetch(["drivers.json"])
    from tif1.cache import _join_frame_writers

    _join_frame_writers()
    assert not get_cache().has_json_entry(f"{YEAR}/{GP}/{SESSION}/drivers.json")
    assert get_cache().get_entry("json", STATS_RESULTS) is None


def test_the_ergast_tier_reads_the_bundles_stats_entries(monkeypatch):
    stub = _Stub(monkeypatch, _bundle())
    stub.fetch(["drivers.json"])
    stats_transport = InMemoryTransport({})
    loader = ErgastPayloadLoader(transport=stats_transport)
    with cache_only():  # answered without a fetch
        assert loader.stats_payload(2025, "test-grand-prix", "results.json") == STATS[STATS_RESULTS]
        assert loader.is_fresh(2025, "", "events.json")
    assert stats_transport.calls == []


def test_a_current_season_stats_entry_ages_from_the_bundle_build(monkeypatch):
    stub = _Stub(monkeypatch, None)
    now = time.time()
    current = time.gmtime(now).tm_year
    names = {f"stats/{current}/test-grand-prix/results.json": STATS[STATS_RESULTS]}
    for built_at, fresh in ((now - 60.0, True), (now - 7 * 3600.0, False)):
        session_bundle.reset()
        from tif1.ergast import payloads

        payloads.clear_memo()
        blob = encode_bundle(
            {name: _bytes(body) for name, body in names.items()}, built_at=built_at, level=1
        )
        stub.transport.add(f"{GP}/{SESSION}/{BUNDLE_FILE}", blob)
        stub.fetch(["drivers.json"])
        loader = ErgastPayloadLoader(transport=InMemoryTransport({}), enable_cache=False)
        assert loader.is_fresh(current, "test-grand-prix", "results.json") is fresh


def test_the_first_wave_hands_over_the_bundle_with_its_status(monkeypatch):
    stub = _Stub(monkeypatch, None)
    url = f"https://cdn.jsdelivr.net/gh/TracingInsights/{YEAR}@main/{GP}/{SESSION}/{BUNDLE_FILE}"
    slot = first_wave._Slot(time.monotonic() + 5)
    slot.status, slot.body = 200, _bundle()
    slot.done.set()
    monkeypatch.setitem(first_wave._slots, url, slot)
    assert stub.fetch(["drivers.json"]) == [FILES["drivers.json"]]
    assert stub.bundle_urls == []  # no request beside the wave's
    session_bundle.reset()
    slot = first_wave._Slot(time.monotonic() + 5)
    slot.status = 404
    slot.done.set()
    monkeypatch.setitem(first_wave._slots, url, slot)
    assert stub.fetch(["drivers.json"]) == [FILES["drivers.json"]]
    assert stub.bundle_urls == []  # the wave's 404 decides
    assert get_cache().is_known_missing(f"{YEAR}/{GP}/{SESSION}/{BUNDLE_FILE}")


def test_take_status_reports_the_answer_once():
    url = "https://cdn.example/x"
    slot = first_wave._Slot(time.monotonic() + 5)
    slot.status = 404
    slot.done.set()
    first_wave._slots[url] = slot
    try:
        assert first_wave.take_status(url) == (404, None)
        assert first_wave.take_status(url) == (None, None)
    finally:
        first_wave._slots.pop(url, None)


def test_the_cold_first_wave_asks_for_the_bundle(monkeypatch):
    config = get_config()._config
    monkeypatch.setitem(config, "cdn_preconnect", True)
    monkeypatch.setitem(config, "session_bundle", True)
    monkeypatch.setattr(core, "_preconnect_started", False)
    monkeypatch.setattr(core, "THREADS_AVAILABLE", True)
    calls: list[list[str]] = []

    def _record(urls: list[str], *, timeout: float) -> None:
        assert timeout >= 1
        calls.append(list(urls))

    monkeypatch.setattr(first_wave, "start", _record)
    core.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    assert [[url.rsplit("/", 1)[-1] for url in urls] for urls in calls] == [[BUNDLE_FILE]]


def test_the_reader_and_codec_import_neither_pandas_nor_numpy():
    import subprocess
    import sys

    code = (
        "import sys, tif1.session_bundle, tif1.core_utils.session_bundle\n"
        "loaded = [m for m in ('pandas', 'numpy', 'polars') if m in sys.modules]\n"
        "assert not loaded, loaded\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_the_env_switch_turns_bundles_on(monkeypatch):
    from tif1.config import Config

    monkeypatch.setenv("TIF1_SESSION_BUNDLE", "true")
    config = get_config()
    snapshot = dict(config._config)
    try:
        Config._load_env(config)
        assert config.get("session_bundle") is True
    finally:
        config._config.clear()
        config._config.update(snapshot)
    assert config.get("session_bundle") is False
