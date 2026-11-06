"""Naxos N9: a bulk wave's payload-tier rows are written while the wave fetches.

``fetch_multiple_async(defer_telemetry_writes=True)`` used to collect every
row and flush them in one writer thread after the gather, where the flush
fought the GIL-bound frame build. The batch now hands its rows to a
``_StreamedTelemetryWrites`` sink whose writer thread persists them as they
arrive; without threads (Pyodide) the list + post-gather flush remains.
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from tif1 import async_fetch
from tif1.cache import Cache, _join_frame_writers


def _plain(payload):
    """Telemetry payloads may carry numpy arrays when tif1-native is installed."""
    import numpy as np

    if isinstance(payload, dict):
        return {k: _plain(v) for k, v in payload.items()}
    if isinstance(payload, np.ndarray):
        return payload.tolist()
    return payload


def _row(lap: int) -> tuple:
    return (2026, "GP", "Race", "VER", lap, {"speed": [float(lap)] * 8})


def _wait_for(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


@pytest.fixture
def cache(tmp_path):
    c = Cache(tmp_path)
    yield c
    _join_frame_writers()
    c.close()


def _has(cache: Cache, lap: int) -> bool:
    return cache.get_telemetry(2026, "GP", "Race", "VER", lap) is not None


def test_rows_are_written_before_close(cache):
    sink = async_fetch._StreamedTelemetryWrites(cache)
    for lap in range(1, 4):
        sink.append(_row(lap))
    # Persisted while the batch is still "fetching" (no close yet).
    assert _wait_for(lambda: all(_has(cache, lap) for lap in range(1, 4)))
    sink.close()
    _join_frame_writers()
    for lap in range(1, 4):
        assert _plain(cache.get_telemetry(2026, "GP", "Race", "VER", lap)) == {
            "speed": [float(lap)] * 8
        }


def test_close_ends_the_writer_and_join_returns(cache):
    sink = async_fetch._StreamedTelemetryWrites(cache)
    sink.append(_row(1))
    sink.close()
    t0 = time.perf_counter()
    _join_frame_writers()
    assert time.perf_counter() - t0 < async_fetch._STREAMED_WRITES_IDLE_S
    assert _has(cache, 1)
    assert not any(t.name == "tif1-frame-write" and t.is_alive() for t in threading.enumerate())


def test_unclosed_sink_cannot_hold_the_exit_join(cache, monkeypatch):
    monkeypatch.setattr(async_fetch, "_STREAMED_WRITES_IDLE_S", 0.05)
    sink = async_fetch._StreamedTelemetryWrites(cache)
    sink.append(_row(1))
    t0 = time.perf_counter()
    _join_frame_writers()  # never closed: the writer exits once idle
    assert time.perf_counter() - t0 < 2.0
    assert _has(cache, 1)
    # A row after the writer went idle starts a new one.
    sink.append(_row(2))
    assert _wait_for(lambda: _has(cache, 2))
    sink.close()
    _join_frame_writers()


def test_write_errors_are_skipped(cache):
    calls: list[int] = []

    def flaky(year, gp, session, driver, lap, payload):
        calls.append(lap)
        if lap == 1:
            raise ValueError("boom")
        Cache.set_telemetry(cache, year, gp, session, driver, lap, payload)

    cache.set_telemetry = flaky
    sink = async_fetch._StreamedTelemetryWrites(cache)
    sink.append(_row(1))
    sink.append(_row(2))
    sink.close()
    _join_frame_writers()
    assert calls == [1, 2]
    assert _has(cache, 2)


def _run_batch(monkeypatch, cache, *, threads: bool):
    """Run fetch_multiple_async over a fake gather that persists two laps."""
    monkeypatch.setattr(async_fetch, "THREADS_AVAILABLE", threads)
    monkeypatch.setattr(async_fetch, "get_cache", lambda: cache)
    seen: dict = {}
    scheduled: list = []
    real_schedule = async_fetch.schedule_telemetry_writes

    def schedule(entries):
        scheduled.append(list(entries))
        real_schedule(entries)

    monkeypatch.setattr(async_fetch, "schedule_telemetry_writes", schedule)

    async def fake_gather(requests, **_kw):
        loop = asyncio.get_running_loop()
        seen["collector"] = async_fetch._deferred_telemetry_writes.get()
        results = []
        for _, _, _, path in requests:
            lap = int(path.split("/")[1].split("_")[0])
            payload = {"speed": [float(lap)] * 8}
            await async_fetch._persist_telemetry_payload(
                cache, loop, None, 2026, "GP", "Race", path, payload
            )
            results.append({"tel": payload})
        if threads:
            # The rows land while the gather is still running.
            deadline = time.monotonic() + 5.0
            while not (_has(cache, 1) and _has(cache, 2)) and time.monotonic() < deadline:
                await asyncio.sleep(0.005)
            seen["written_during_gather"] = _has(cache, 1) and _has(cache, 2)
        return results

    monkeypatch.setattr(async_fetch, "_gather_fetches", fake_gather)
    requests = [(2026, "GP", "Race", f"VER/{lap}_tel.json") for lap in (1, 2)]
    results = asyncio.run(
        async_fetch.fetch_multiple_async(
            requests,
            use_cache=False,
            write_cache=True,
            validate_payload=False,
            defer_telemetry_writes=True,
        )
    )
    _join_frame_writers()
    return results, seen, scheduled


def test_batch_streams_rows_during_the_gather(monkeypatch, cache):
    results, seen, scheduled = _run_batch(monkeypatch, cache, threads=True)
    assert [r["tel"]["speed"][0] for r in results] == [1.0, 2.0]
    assert isinstance(seen["collector"], async_fetch._StreamedTelemetryWrites)
    assert seen["written_during_gather"]
    assert scheduled == []  # no post-gather flush
    assert async_fetch._deferred_telemetry_writes.get() is None


def test_batch_without_threads_flushes_after_the_gather(monkeypatch, cache):
    results, seen, scheduled = _run_batch(monkeypatch, cache, threads=False)
    assert len(results) == 2
    assert isinstance(seen["collector"], list)
    assert [[row[4] for row in batch] for batch in scheduled] == [[1, 2]]
    assert _has(cache, 1)
    assert _has(cache, 2)


def test_failed_gather_closes_the_sink(monkeypatch, cache):
    monkeypatch.setattr(async_fetch, "get_cache", lambda: cache)
    sinks: list = []

    async def failing_gather(requests, **_kw):
        sink = async_fetch._deferred_telemetry_writes.get()
        sinks.append(sink)
        sink.append(_row(7))
        raise RuntimeError("gather failed")

    monkeypatch.setattr(async_fetch, "_gather_fetches", failing_gather)
    with pytest.raises(RuntimeError):
        asyncio.run(
            async_fetch.fetch_multiple_async(
                [(2026, "GP", "Race", "VER/7_tel.json")],
                use_cache=False,
                write_cache=True,
                validate_payload=False,
                defer_telemetry_writes=True,
            )
        )
    assert sinks[0]._closed
    t0 = time.perf_counter()
    _join_frame_writers()
    assert time.perf_counter() - t0 < async_fetch._STREAMED_WRITES_IDLE_S
    assert _has(cache, 7)
