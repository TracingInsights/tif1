"""Benchmarks for async_fetch cache-hit path."""

import asyncio
import json

import pytest

import tif1.async_fetch as async_fetch_module
from tif1.async_fetch import _get_executor, close_executor
from tif1.cache import Cache


def _memory_lookup(cache: Cache, cache_key: str):
    json_data = None
    with cache._memory_cache_lock:
        json_data = cache._memory_cache.get(cache_key)
        if json_data is not None:
            cache._memory_cache.move_to_end(cache_key)
    return json.loads(json_data) if json_data is not None else None


async def _legacy_fetch_cache_hit(cache: Cache, cache_key: str):
    loop = asyncio.get_running_loop()
    executor = _get_executor()
    return await loop.run_in_executor(executor, cache.get, cache_key)


async def _candidate_fetch_cache_hit(cache: Cache, cache_key: str):
    cached = _memory_lookup(cache, cache_key)
    if cached is not None:
        return cached

    loop = asyncio.get_running_loop()
    executor = _get_executor()
    return await loop.run_in_executor(executor, cache.get, cache_key)


def _run_legacy_loop(cache: Cache, cache_key: str, iterations: int = 2_000):
    async def runner():
        hits = 0
        for _ in range(iterations):
            if await _legacy_fetch_cache_hit(cache, cache_key) is not None:
                hits += 1
        return hits

    return asyncio.run(runner())


def _run_candidate_loop(cache: Cache, cache_key: str, iterations: int = 2_000):
    async def runner():
        hits = 0
        for _ in range(iterations):
            if await _candidate_fetch_cache_hit(cache, cache_key) is not None:
                hits += 1
        return hits

    return asyncio.run(runner())


def _run_production_loop(iterations: int = 2_000):
    async def runner():
        hits = 0
        for _ in range(iterations):
            if await async_fetch_module.fetch_json_async(2025, "Test%20GP", "Race", "drivers.json"):
                hits += 1
        return hits

    return asyncio.run(runner())


@pytest.fixture(autouse=True)
def reset_async_fetch_executor():
    close_executor()
    yield
    close_executor()


@pytest.mark.benchmark(group="async_fetch_cache_hit")
def test_fetch_json_cache_hit_candidate_parity(tmp_path):
    cache = Cache(tmp_path)
    cache_key = "2025/Test%20GP/Race/drivers.json"
    payload = {"drivers": [{"driver": "VER"}]}
    cache.set(cache_key, payload)

    legacy = _run_legacy_loop(cache, cache_key, iterations=100)
    candidate = _run_candidate_loop(cache, cache_key, iterations=100)
    assert candidate == legacy == 100


@pytest.mark.benchmark(group="async_fetch_cache_hit")
class TestAsyncFetchCacheHitBenchmark:
    def test_legacy_cache_hit_path(self, benchmark, tmp_path):
        cache = Cache(tmp_path)
        cache_key = "2025/Test%20GP/Race/drivers.json"
        cache.set(cache_key, {"drivers": [{"driver": "VER"}], "meta": {"round": 24}})

        hits = benchmark(_run_legacy_loop, cache, cache_key)
        assert hits == 2_000

    def test_candidate_cache_hit_path(self, benchmark, tmp_path):
        cache = Cache(tmp_path)
        cache_key = "2025/Test%20GP/Race/drivers.json"
        cache.set(cache_key, {"drivers": [{"driver": "VER"}], "meta": {"round": 24}})

        hits = benchmark(_run_candidate_loop, cache, cache_key)
        assert hits == 2_000

    def test_production_cache_hit_path(self, benchmark, monkeypatch, tmp_path):
        cache = Cache(tmp_path)
        cache_key = "2025/Test%20GP/Race/drivers.json"
        cache.set(cache_key, {"drivers": [{"driver": "VER"}], "meta": {"round": 24}})
        monkeypatch.setattr(async_fetch_module, "get_cache", lambda: cache)

        hits = benchmark(_run_production_loop)
        assert hits == 2_000
