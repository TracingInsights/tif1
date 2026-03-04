"""Benchmarks for cache hot-read performance."""

import json

import pytest

from tif1.cache import Cache


def _legacy_get(cache: Cache, key: str):
    if cache.conn is None:
        return None
    with cache._sqlite_lock:
        result = cache.conn.execute("SELECT data FROM cache WHERE key = ?", (key,)).fetchone()
    return json.loads(result[0]) if result else None


def _legacy_get_telemetry(cache: Cache, year: int, gp: str, session: str, driver: str, lap: int):
    if cache.conn is None:
        return None
    with cache._sqlite_lock:
        result = cache.conn.execute(
            "SELECT data FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? AND driver = ? AND lap = ?",
            (year, gp, session, driver, lap),
        ).fetchone()
    return json.loads(result[0]) if result else None


def _run_legacy_get_loop(cache: Cache, key: str, iterations: int = 3_000) -> int:
    hits = 0
    for _ in range(iterations):
        if _legacy_get(cache, key) is not None:
            hits += 1
    return hits


def _run_optimized_get_loop(cache: Cache, key: str, iterations: int = 3_000) -> int:
    hits = 0
    for _ in range(iterations):
        if cache.get(key) is not None:
            hits += 1
    return hits


def _run_legacy_telemetry_loop(
    cache: Cache, year: int, gp: str, session: str, driver: str, lap: int, iterations: int = 2_000
) -> int:
    hits = 0
    for _ in range(iterations):
        if _legacy_get_telemetry(cache, year, gp, session, driver, lap) is not None:
            hits += 1
    return hits


def _run_optimized_telemetry_loop(
    cache: Cache, year: int, gp: str, session: str, driver: str, lap: int, iterations: int = 2_000
) -> int:
    hits = 0
    for _ in range(iterations):
        if cache.get_telemetry(year, gp, session, driver, lap) is not None:
            hits += 1
    return hits


def test_cache_memory_lookup_parity(tmp_path):
    cache = Cache(tmp_path)
    data = {"drivers": ["VER", "HAM"], "meta": {"round": 24}}
    cache.set("session_key", data)

    assert cache.get("session_key") == _legacy_get(cache, "session_key")


def test_cache_memory_lookup_telemetry_parity(tmp_path):
    cache = Cache(tmp_path)
    tel = {"speed": [100, 200, 250], "throttle": [0.1, 0.8, 1.0]}
    key = (2025, "Abu Dhabi Grand Prix", "Race", "VER", 1)
    cache.set_telemetry(*key, tel)

    assert cache.get_telemetry(*key) == _legacy_get_telemetry(cache, *key)


@pytest.mark.benchmark(group="cache_hot_read")
class TestCacheMemoryLookupBenchmark:
    def test_legacy_get_hot_key(self, benchmark, tmp_path):
        cache = Cache(tmp_path)
        cache.set("hot_key", {"value": "x" * 200})

        hits = benchmark(_run_legacy_get_loop, cache, "hot_key")
        assert hits == 3_000

    def test_optimized_get_hot_key(self, benchmark, tmp_path):
        cache = Cache(tmp_path)
        cache.set("hot_key", {"value": "x" * 200})
        cache.get("hot_key")

        hits = benchmark(_run_optimized_get_loop, cache, "hot_key")
        assert hits == 3_000

    def test_legacy_get_telemetry_hot_key(self, benchmark, tmp_path):
        cache = Cache(tmp_path)
        key = (2025, "Abu Dhabi Grand Prix", "Race", "VER", 1)
        cache.set_telemetry(*key, {"speed": list(range(120)), "rpm": list(range(120))})

        hits = benchmark(_run_legacy_telemetry_loop, cache, *key)
        assert hits == 2_000

    def test_optimized_get_telemetry_hot_key(self, benchmark, tmp_path):
        cache = Cache(tmp_path)
        key = (2025, "Abu Dhabi Grand Prix", "Race", "VER", 1)
        cache.set_telemetry(*key, {"speed": list(range(120)), "rpm": list(range(120))})
        cache.get_telemetry(*key)

        hits = benchmark(_run_optimized_telemetry_loop, cache, *key)
        assert hits == 2_000

    def test_legacy_get_missing_key(self, benchmark, tmp_path):
        cache = Cache(tmp_path)
        misses = benchmark(lambda: _run_legacy_get_loop(cache, "missing_key", 3_000))
        assert misses == 0

    def test_optimized_get_missing_key(self, benchmark, tmp_path):
        cache = Cache(tmp_path)
        misses = benchmark(lambda: _run_optimized_get_loop(cache, "missing_key", 3_000))
        assert misses == 0
