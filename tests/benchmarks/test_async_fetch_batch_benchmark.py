"""Benchmarks for async fetch batching overhead."""

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

import tif1.async_fetch as async_fetch_module
import tif1.config as config_module
from tif1.async_fetch import DataNotFoundError, fetch_multiple_async

RequestTuple = tuple[int, str, str, str]
Fetcher = Callable[[int, str, str, str], Any]


def _build_requests(size: int = 3000) -> list[RequestTuple]:
    return [(2025, "Test%20GP", "Race", f"D{i % 20:02d}/laptimes_{i}.json") for i in range(size)]


class _StubConfig:
    def __init__(self, max_concurrent_requests: int):
        self.max_concurrent_requests = max_concurrent_requests

    def get(self, key: str, default=None):
        if key == "max_concurrent_requests":
            return self.max_concurrent_requests
        return default


async def _stub_fetch_success(*_req, **_kwargs):
    return {"ok": True}


async def _stub_fetch_mixed(*req):
    path = req[3]
    idx = int(path.rsplit("_", 1)[-1].split(".")[0])
    if idx % 13 == 0:
        raise RuntimeError("boom")
    if idx % 7 == 0:
        raise DataNotFoundError(year=req[0], event=req[1], session=req[2])
    return {"ok": idx}


def _mute_warning(*_args, **_kwargs):
    return None


async def _legacy_fetch_multiple_async(
    requests: list[RequestTuple], max_concurrent: int, fetcher: Fetcher
) -> list[dict[str, Any] | None]:
    semaphore = asyncio.Semaphore(max_concurrent)

    async def fetch_with_semaphore(req):
        async with semaphore:
            return await fetcher(*req)

    tasks = [fetch_with_semaphore(req) for req in requests]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    processed = []
    for req, result in zip(requests, results):
        if isinstance(result, Exception):
            if not isinstance(result, DataNotFoundError):
                async_fetch_module.logger.warning(
                    f"Failed to fetch {req}: {type(result).__name__}: {result}"
                )
            processed.append(None)
        else:
            processed.append(result)

    return processed


async def _candidate_fetch_multiple_async(
    requests: list[RequestTuple], max_concurrent: int, fetcher: Fetcher
) -> list[dict[str, Any] | None]:
    if not requests:
        return []

    if max_concurrent >= len(requests):
        results = await asyncio.gather(*(fetcher(*req) for req in requests), return_exceptions=True)
    else:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(req):
            async with semaphore:
                return await fetcher(*req)

        tasks = [fetch_with_semaphore(req) for req in requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    processed = []
    for req, result in zip(requests, results):
        if isinstance(result, Exception):
            if not isinstance(result, DataNotFoundError):
                async_fetch_module.logger.warning(
                    f"Failed to fetch {req}: {type(result).__name__}: {result}"
                )
            processed.append(None)
        else:
            processed.append(result)

    return processed


def test_fetch_multiple_candidate_parity_success():
    requests = _build_requests(400)

    legacy = asyncio.run(_legacy_fetch_multiple_async(requests, 1000, _stub_fetch_success))
    candidate = asyncio.run(_candidate_fetch_multiple_async(requests, 1000, _stub_fetch_success))

    assert candidate == legacy


def test_fetch_multiple_candidate_parity_mixed(monkeypatch):
    requests = _build_requests(400)
    monkeypatch.setattr(async_fetch_module.logger, "warning", _mute_warning)

    legacy = asyncio.run(_legacy_fetch_multiple_async(requests, 1000, _stub_fetch_mixed))
    candidate = asyncio.run(_candidate_fetch_multiple_async(requests, 1000, _stub_fetch_mixed))

    assert candidate == legacy


@pytest.mark.benchmark(group="async_fetch_batch")
class TestAsyncFetchBatchBenchmark:
    def test_legacy_unconstrained(self, benchmark, monkeypatch):
        requests = _build_requests()
        monkeypatch.setattr(async_fetch_module.logger, "warning", _mute_warning)

        results = benchmark(
            lambda: asyncio.run(_legacy_fetch_multiple_async(requests, 10_000, _stub_fetch_success))
        )
        assert len(results) == len(requests)

    def test_candidate_unconstrained(self, benchmark, monkeypatch):
        requests = _build_requests()
        monkeypatch.setattr(async_fetch_module.logger, "warning", _mute_warning)

        results = benchmark(
            lambda: asyncio.run(
                _candidate_fetch_multiple_async(requests, 10_000, _stub_fetch_success)
            )
        )
        assert len(results) == len(requests)

    def test_production_unconstrained(self, benchmark, monkeypatch):
        requests = _build_requests()
        monkeypatch.setattr(async_fetch_module, "fetch_json_async", _stub_fetch_success)
        monkeypatch.setattr(config_module, "get_config", lambda: _StubConfig(10_000))
        monkeypatch.setattr(async_fetch_module.logger, "warning", _mute_warning)

        results = benchmark(lambda: asyncio.run(fetch_multiple_async(requests)))
        assert len(results) == len(requests)
