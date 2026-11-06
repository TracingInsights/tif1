"""The lazy-telemetry stream (Elis E10): cold lazy waves on one prioritised event loop.

``PriorityFetchStream`` runs the requests a caller waits for before any
background request, fetches every key once whatever its priority, and never
leaves a waiter hanging. On a session, a lazy wave that follows the driver
the previous wave predicted (laps-row order) queues the next driver in the
background; the frames stay the ones the eager load builds. Every payload is
served from memory.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections import Counter
from typing import Any

import pandas as pd
import pytest

from tif1 import async_fetch, core
from tif1.config import get_config
from tif1.core_utils.telemetry_stream import PriorityFetchStream
from tif1.exceptions import NetworkError

from .test_polars_session_wiring import TEL_SAMPLES, _Cdn

VER_LAPS = ["VER/1_tel.json", "VER/2_tel.json", "VER/3_tel.json"]
HAM_LAPS = ["HAM/1_tel.json", "HAM/2_tel.json", "HAM/3_tel.json"]


class _GatedFetch:
    """A fake fetch: records the start order, holds every request until released."""

    def __init__(self) -> None:
        self.started: list[Any] = []
        self.release = threading.Event()
        self.lock = threading.Lock()

    async def __call__(self, request: Any) -> Any:
        with self.lock:
            self.started.append(request)
        while not self.release.is_set():
            await asyncio.sleep(0.001)
        if request == "boom":
            raise ValueError("bad payload")
        return {"payload": request}


def _wait(futures: list[concurrent.futures.Future[Any]]) -> list[Any]:
    _done, not_done = concurrent.futures.wait(futures, timeout=5)
    assert not not_done
    return [future.result() for future in futures]


class TestPriorityFetchStream:
    def test_foreground_keys_start_before_background_keys(self):
        fetch = _GatedFetch()
        stream = PriorityFetchStream(fetch, max_concurrent=1, background_concurrent=1)
        stream.prefetch([("b1", "b1"), ("b2", "b2")])
        futures = stream.request([("f1", "f1"), ("f2", "f2")])
        fetch.release.set()
        assert _wait(futures) == [{"payload": "f1"}, {"payload": "f2"}]
        assert stream.wait_idle(5)
        # b1 may have started before the foreground keys arrived; f1/f2 never wait behind b2.
        order = fetch.started
        assert order.index("f1") < order.index("b2")
        assert order.index("f2") < order.index("b2")

    def test_a_key_is_fetched_once_whatever_its_priority(self):
        fetch = _GatedFetch()
        stream = PriorityFetchStream(fetch, max_concurrent=4, background_concurrent=4)
        stream.prefetch([("a", "a"), ("b", "b")])
        first = stream.request([("a", "a")])
        second = stream.request([("a", "a"), ("b", "b")])
        fetch.release.set()
        assert _wait(first + second) == [{"payload": "a"}] * 2 + [{"payload": "b"}]
        assert stream.wait_idle(5)
        assert Counter(fetch.started) == {"a": 1, "b": 1}
        # A finished key is served from the stream's results.
        assert _wait(stream.request([("a", "a")])) == [{"payload": "a"}]
        assert Counter(fetch.started) == {"a": 1, "b": 1}

    def test_a_queued_background_key_is_promoted(self):
        fetch = _GatedFetch()
        stream = PriorityFetchStream(fetch, max_concurrent=1, background_concurrent=1)
        blocker = stream.request([("x", "x")])
        stream.prefetch([("b1", "b1"), ("b2", "b2"), ("b3", "b3")])
        promoted = stream.request([("b3", "b3")])
        fetch.release.set()
        _wait(blocker + promoted)
        assert stream.wait_idle(5)
        assert fetch.started.index("b3") < fetch.started.index("b1")

    def test_prefetch_replaces_the_background_queue(self):
        fetch = _GatedFetch()
        stream = PriorityFetchStream(fetch, max_concurrent=1, background_concurrent=1)
        blocker = stream.request([("x", "x")])
        stream.prefetch([("old", "old")])
        stream.prefetch([("new", "new")])
        fetch.release.set()
        _wait(blocker)
        assert stream.wait_idle(5)
        assert fetch.started == ["x", "new"]
        assert set(stream.results()) == {"x", "new"}

    def test_background_concurrency_cap(self):
        fetch = _GatedFetch()
        stream = PriorityFetchStream(fetch, max_concurrent=8, background_concurrent=2)
        stream.prefetch([(i, i) for i in range(6)])
        deadline = threading.Event()
        deadline.wait(0.1)
        assert len(fetch.started) == 2
        fetch.release.set()
        assert stream.wait_idle(5)
        assert sorted(fetch.started) == list(range(6))

    def test_zero_background_slots_fetch_only_foreground_keys(self):
        fetch = _GatedFetch()
        fetch.release.set()
        stream = PriorityFetchStream(fetch, max_concurrent=4, background_concurrent=0)
        stream.prefetch([("b", "b")])
        assert _wait(stream.request([("f", "f")])) == [{"payload": "f"}]
        assert stream.wait_idle(5)
        assert fetch.started == ["f"]

    def test_a_failure_resolves_to_none(self, caplog):
        fetch = _GatedFetch()
        fetch.release.set()
        stream = PriorityFetchStream(fetch, max_concurrent=2, background_concurrent=2)
        assert _wait(stream.request([("bad", "boom"), ("ok", "ok")])) == [None, {"payload": "ok"}]
        assert "bad payload" in caplog.text

    def test_close_drops_the_queues_and_returns_the_requests_in_flight(self):
        fetch = _GatedFetch()
        stream = PriorityFetchStream(fetch, max_concurrent=1, background_concurrent=1)
        running = stream.request([("a", "a")])
        queued = stream.request([("b", "b")])
        stream.prefetch([("c", "c")])
        while not fetch.started:
            threading.Event().wait(0.001)
        in_flight = stream.close()
        assert in_flight == running
        assert queued[0].result(timeout=1) is None  # never left hanging
        fetch.release.set()
        assert _wait(in_flight) == [{"payload": "a"}]
        assert stream.wait_idle(5)
        assert fetch.started == ["a"]
        assert stream.results() == {"a": {"payload": "a"}}
        assert _wait(stream.request([("d", "d")])) == [None]  # closed: nothing new starts

    def test_close_with_cancel_cancels_the_requests_in_flight(self):
        fetch = _GatedFetch()  # never released
        stream = PriorityFetchStream(fetch, max_concurrent=2, background_concurrent=2)
        futures = stream.request([("a", "a"), ("b", "b")])
        while len(fetch.started) < 2:
            threading.Event().wait(0.001)
        stream.close(cancel=True)
        assert _wait(futures) == [None, None]
        assert stream.wait_idle(5)


# -- the session side ---------------------------------------------------------------


@pytest.fixture
def cdn(monkeypatch) -> _Cdn:
    served = _Cdn(monkeypatch)
    monkeypatch.setattr(async_fetch, "fetch_multiple_async", core.fetch_multiple_async)
    monkeypatch.setitem(get_config()._config, "lazy_telemetry_lookahead", 1)
    return served


def _telemetry_paths(cdn: _Cdn) -> list[str]:
    return [path for batch in cdn.batches for path in batch if "_tel" in path]


def _lap(session: core.Session, driver: str, number: int) -> Any:
    laps = session.laps.pick_drivers(driver)
    return next(lap for _, lap in laps.iterlaps() if int(lap["LapNumber"]) == number)


def _assert_same_frame(left: Any, right: Any, lib: str) -> None:
    assert type(left) is type(right)
    if lib == "polars":
        assert left.schema == right.schema
        assert left.equals(right)
    else:
        pd.testing.assert_frame_equal(pd.DataFrame(left), pd.DataFrame(right), check_exact=True)


def _lazy_session(cdn: _Cdn, lib: str, *, predicted: str | None) -> core.Session:
    session = cdn.session(lib)
    session.load(telemetry="lazy", weather=False, messages=False)
    assert session._lazy_telemetry_order == ["VER", "HAM"]
    # Two drivers only: pretend an earlier wave predicted VER, so VER's wave
    # counts as sequential access (or, with ``None``, as the first wave).
    session._lazy_lookahead_next = predicted
    return session


@pytest.mark.parametrize("lib", ["pandas", "polars"])
class TestSessionStream:
    def test_a_sequential_wave_queues_the_next_driver(self, cdn, lib):
        session = _lazy_session(cdn, lib, predicted="VER")
        _ = _lap(session, "VER", 1).telemetry
        stream = session._lazy_stream
        assert stream is not None
        assert stream.wait_idle(5)
        assert Counter(_telemetry_paths(cdn)) == Counter(VER_LAPS + HAM_LAPS)
        # HAM is still pending: its frames are built from the stream's payloads.
        assert set(session._lazy_telemetry_refs) == {"HAM"}
        _ = session.laps.pick_drivers("HAM").telemetry
        assert Counter(_telemetry_paths(cdn)) == Counter(VER_LAPS + HAM_LAPS)

    def test_a_first_wave_queues_nothing(self, cdn, lib):
        session = _lazy_session(cdn, lib, predicted=None)
        _ = session.laps.pick_drivers("VER").telemetry
        assert session._lazy_stream is not None
        assert session._lazy_stream.wait_idle(5)
        assert sorted(_telemetry_paths(cdn)) == VER_LAPS
        assert session._lazy_lookahead_next == "HAM"

    def test_an_unpredicted_wave_queues_nothing(self, cdn, lib):
        session = _lazy_session(cdn, lib, predicted="HAM")
        _ = session.laps.pick_drivers("VER").telemetry
        assert session._lazy_stream is not None
        assert session._lazy_stream.wait_idle(5)
        assert sorted(_telemetry_paths(cdn)) == VER_LAPS

    def test_frames_equal_the_eager_load(self, cdn, lib):
        lazy = _lazy_session(cdn, lib, predicted="VER")
        eager = cdn.session(lib)
        eager.load(telemetry="eager", weather=False, messages=False)
        for driver in ("VER", "HAM"):
            for number in (1, 2, 3):
                _assert_same_frame(
                    _lap(eager, driver, number).telemetry, _lap(lazy, driver, number).telemetry, lib
                )
        _assert_same_frame(
            eager.laps.pick_drivers("HAM").telemetry, lazy.laps.pick_drivers("HAM").telemetry, lib
        )

    def test_fetch_all_takes_the_stream_payloads(self, cdn, lib):
        session = _lazy_session(cdn, lib, predicted="VER")
        _ = _lap(session, "VER", 1).telemetry
        stream = session._lazy_stream
        assert stream is not None
        assert stream.wait_idle(5)
        lazy_all = session.fetch_all_laps_telemetry()
        assert session._lazy_stream is None
        assert stream.closed
        assert session._lazy_telemetry_refs == {}
        # HAM came from the stream: nothing is fetched twice.
        assert Counter(_telemetry_paths(cdn)) == Counter(VER_LAPS + HAM_LAPS)

        eager = cdn.session(lib)
        eager.load(telemetry="eager", weather=False, messages=False)
        _ = _lap(eager, "VER", 1).telemetry
        eager_all = eager.fetch_all_laps_telemetry()
        assert list(lazy_all) == list(eager_all)
        for ref, frame in eager_all.items():
            _assert_same_frame(frame, lazy_all[ref], lib)

    def test_fetch_all_keeps_laps_row_order_without_a_network_batch(self, cdn, lib):
        # HAM touched first; the stream brings VER, so fetch_all fetches nothing
        # and maps HAM's memoized frames and VER's payload frames together.
        session = _lazy_session(cdn, lib, predicted="HAM")
        _ = _lap(session, "HAM", 1).telemetry
        assert session._lazy_stream is not None
        assert session._lazy_stream.wait_idle(5)
        lazy_all = session.fetch_all_laps_telemetry()
        assert Counter(_telemetry_paths(cdn)) == Counter(VER_LAPS + HAM_LAPS)
        eager = cdn.session(lib)
        eager.load(telemetry="eager", weather=False, messages=False)
        assert list(lazy_all) == list(eager.fetch_all_laps_telemetry())

    def test_every_pending_driver_with_lookahead_minus_one(self, cdn, lib, monkeypatch):
        monkeypatch.setitem(get_config()._config, "lazy_telemetry_lookahead", -1)
        session = _lazy_session(cdn, lib, predicted="VER")
        _ = _lap(session, "VER", 2).telemetry
        assert session._lazy_stream is not None
        assert session._lazy_stream.wait_idle(5)
        assert Counter(_telemetry_paths(cdn)) == Counter(VER_LAPS + HAM_LAPS)


def _lone_paths(cdn: _Cdn) -> list[str]:
    """Telemetry files fetched one at a time (the sync transport), as ``DRIVER/N_tel.json``."""
    return ["/".join(url.rsplit("/", 2)[-2:]) for url in cdn.transport.calls if "_tel" in url]


@pytest.mark.parametrize("lib", ["pandas", "polars"])
class TestLoneFirstTouch:
    """Naxos N11: a cold driver's first touch asking for one lap fetches that lap only."""

    def test_one_lap_then_the_rest_in_one_wave(self, cdn, lib):
        session = _lazy_session(cdn, lib, predicted=None)
        tel = _lap(session, "VER", 2).telemetry
        assert len(tel) == TEL_SAMPLES
        assert _lone_paths(cdn) == ["VER/2_tel.json"]
        assert _telemetry_paths(cdn) == []
        assert session._lazy_stream is None
        assert set(session._lazy_telemetry_refs) == {"VER", "HAM"}
        _ = _lap(session, "VER", 1).telemetry  # the second touch: the wave
        assert session._lazy_stream is not None
        assert session._lazy_stream.wait_idle(5)
        assert sorted(_telemetry_paths(cdn)) == ["VER/1_tel.json", "VER/3_tel.json"]
        assert set(session._lazy_telemetry_refs) == {"HAM"}
        _ = _lap(session, "VER", 3).get_car_data()
        assert _lone_paths(cdn) == ["VER/2_tel.json"]
        assert sorted(_telemetry_paths(cdn)) == ["VER/1_tel.json", "VER/3_tel.json"]

    def test_frames_equal_the_eager_load(self, cdn, lib):
        lazy = _lazy_session(cdn, lib, predicted=None)
        eager = cdn.session(lib)
        eager.load(telemetry="eager", weather=False, messages=False)
        _assert_same_frame(_lap(eager, "HAM", 3).telemetry, _lap(lazy, "HAM", 3).telemetry, lib)
        _assert_same_frame(
            _lap(eager, "VER", 2).get_car_data(), _lap(lazy, "VER", 2).get_car_data(), lib
        )
        assert _lone_paths(cdn) == ["HAM/3_tel.json", "VER/2_tel.json"]
        # The merged frame takes the lone lap's memoized frame and a wave for the rest.
        _assert_same_frame(
            eager.laps.pick_drivers("HAM").telemetry, lazy.laps.pick_drivers("HAM").telemetry, lib
        )
        assert _lone_paths(cdn) == ["HAM/3_tel.json", "VER/2_tel.json"]

    def test_the_predicted_driver_takes_the_wave(self, cdn, lib):
        session = _lazy_session(cdn, lib, predicted="VER")
        _ = _lap(session, "VER", 2).telemetry
        assert _lone_paths(cdn) == []
        assert session._lazy_stream is not None
        assert session._lazy_stream.wait_idle(5)

    def test_a_lap_the_stream_holds_joins_it(self, cdn, lib):
        session = _lazy_session(cdn, lib, predicted="VER")
        _ = _lap(session, "VER", 1).telemetry  # sequential: HAM is queued behind it
        assert session._lazy_stream is not None
        assert session._lazy_stream.wait_idle(5)
        session._lazy_lookahead_next = None  # not the predicted driver any more
        _ = _lap(session, "HAM", 2).telemetry
        assert _lone_paths(cdn) == []
        assert Counter(_telemetry_paths(cdn)) == Counter(VER_LAPS + HAM_LAPS)

    def test_which_touches_are_lone(self, cdn, lib, monkeypatch):
        session = _lazy_session(cdn, lib, predicted=None)
        assert session._lazy_lone_lap([("VER", 1), ("VER", 2)]) is None  # two laps
        assert session._lazy_lone_lap([("XXX", 1)]) is None  # not pending
        monkeypatch.setattr(session, "_lazy_telemetry_from_cache", lambda: True)
        assert session._lazy_lone_lap([("VER", 1)]) is None  # the cache serves it
        monkeypatch.setattr(session, "_lazy_telemetry_from_cache", lambda: False)
        assert session._lazy_lone_lap([("VER", 1)]) == ("VER", 1)
        assert session._lazy_lone_lap([("VER", 2)]) is None  # the driver's second touch
        assert session._lazy_lone_lap([("HAM", 1)]) == ("HAM", 1)

    def test_a_failed_lone_fetch_leaves_the_driver_to_its_wave(self, cdn, lib):
        cdn.transport.add("VER/2_tel.json", NetworkError(url="VER/2_tel.json"))
        session = _lazy_session(cdn, lib, predicted=None)
        session._ensure_lazy_telemetry([("VER", 2)])
        assert set(_lone_paths(cdn)) == {"VER/2_tel.json"}  # each CDN once
        assert not session._memo.contains("telemetry_df", ("VER", 2))
        assert set(session._lazy_telemetry_refs) == {"VER", "HAM"}
        session._ensure_lazy_telemetry([("VER", 1)])
        assert session._lazy_stream is not None
        assert session._lazy_stream.wait_idle(5)
        assert sorted(_telemetry_paths(cdn)) == VER_LAPS
        assert session._memo.contains("telemetry_df", ("VER", 2))


def test_a_lone_lap_payload_goes_to_the_telemetry_tier_writer(cdn, monkeypatch):
    flushed: list[list[tuple[Any, ...]]] = []
    monkeypatch.setattr(async_fetch, "schedule_telemetry_writes", flushed.append)
    session = _lazy_session(cdn, "pandas", predicted=None)
    monkeypatch.setattr(session, "enable_cache", True)
    monkeypatch.setattr(session, "_session_cache_available", lambda: False)
    session._ensure_lazy_telemetry([("VER", 2)])
    assert [row[3:5] for batch in flushed for row in batch] == [("VER", 2)]
    assert session._memo.contains("telemetry_df", ("VER", 2))
    assert session._get_telemetry_payload("VER", 2) is not None


def test_lookahead_zero_keeps_one_batch_per_driver(cdn, monkeypatch):
    """``lazy_telemetry_lookahead=0``: no stream, E1's one ``fetch_multiple_async`` per wave."""
    monkeypatch.setitem(get_config()._config, "lazy_telemetry_lookahead", 0)
    session = _lazy_session(cdn, "pandas", predicted="VER")
    _ = _lap(session, "VER", 1).telemetry
    assert session._lazy_stream is None
    assert [batch for batch in cdn.batches if any("_tel" in p for p in batch)] == [VER_LAPS]


def test_cached_sessions_do_not_use_the_stream(cdn, monkeypatch):
    session = _lazy_session(cdn, "polars", predicted="VER")
    monkeypatch.setattr(session, "_lazy_telemetry_from_cache", lambda: True)
    assert session._lazy_telemetry_stream(ultra_cold=False) is None


def test_stream_writes_are_handed_to_the_background_writer(cdn, monkeypatch):
    flushed: list[list[tuple[Any, ...]]] = []
    monkeypatch.setattr(async_fetch, "schedule_telemetry_writes", flushed.append)
    seen: list[dict[str, Any]] = []

    async def fetch_multiple(requests, **kwargs):
        if any("_tel" in request[3] for request in requests):
            seen.append(kwargs)
        writes = kwargs.get("telemetry_writes")
        if writes is not None:
            writes.extend(("row", request[3]) for request in requests)
        return [cdn.payloads.get(request[3]) for request in requests]

    monkeypatch.setattr(core, "fetch_multiple_async", fetch_multiple)
    session = _lazy_session(cdn, "pandas", predicted=None)
    _ = session.laps.pick_drivers("VER").telemetry
    assert len(seen) == 3
    assert all(kwargs["telemetry_writes"] is session._lazy_stream_writes for kwargs in seen)
    assert session._lazy_stream is not None
    assert session._lazy_stream.wait_idle(5)
    assert sorted(row[1] for batch in flushed for row in batch) == VER_LAPS
    assert session._lazy_stream_writes == []


def test_fetch_multiple_async_collects_into_a_caller_list(monkeypatch):
    """``telemetry_writes``: the rows go to the caller's list, no flush is scheduled."""
    scheduled: list[Any] = []
    monkeypatch.setattr(async_fetch, "schedule_telemetry_writes", scheduled.append)

    async def fake_gather(requests, **_kwargs):
        collector = async_fetch._deferred_telemetry_writes.get()
        assert collector is not None
        collector.append(("row",))
        return [{"tel": {}} for _ in requests]

    monkeypatch.setattr(async_fetch, "_gather_fetches", fake_gather)
    writes: list[tuple[Any, ...]] = []
    result = asyncio.run(
        async_fetch.fetch_multiple_async(
            [(2025, "gp", "R", "VER/1_tel.json")], telemetry_writes=writes
        )
    )
    assert result == [{"tel": {}}]
    assert writes == [("row",)]
    assert scheduled == []
