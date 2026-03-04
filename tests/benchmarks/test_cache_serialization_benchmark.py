"""Benchmarks for cache JSON serialization hot paths."""

from __future__ import annotations

import json

import pytest

from tif1.cache import Cache


def _build_payload(size: int = 4000, unicode_labels: bool = False) -> dict:
    """Build deterministic cache payload."""
    labels = [f"DRV{idx % 20:02d}" for idx in range(size)]
    if unicode_labels:
        labels = [f"São-{label}" for label in labels]

    return {
        "drivers": labels,
        "lap_times": [90.0 + (idx % 50) * 0.05 for idx in range(size)],
        "flags": [bool(idx % 2) for idx in range(size)],
        "meta": {"season": 2025, "event": "Abu Dhabi Grand Prix"},
    }


def _legacy_serialize_json(data: dict) -> str:
    """Legacy cache JSON serialization baseline."""
    return json.dumps(data)


def _candidate_serialize_json(data: dict) -> str:
    """Candidate compact serializer evaluated for performance."""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _legacy_cache_set(cache: Cache, key: str, data: dict) -> None:
    """Legacy cache write implementation baseline."""
    json_data = _legacy_serialize_json(data)
    with cache._sqlite_lock:
        cache.conn.execute("INSERT OR REPLACE INTO cache VALUES (?, ?)", (key, json_data))
        cache._pending_writes += 1
        cache._commit_if_needed()


def _optimized_cache_set(cache: Cache, key: str, data: dict) -> None:
    """Optimized cache write implementation."""
    json_data = _candidate_serialize_json(data)
    with cache._sqlite_lock:
        cache.conn.execute("INSERT OR REPLACE INTO cache VALUES (?, ?)", (key, json_data))
        cache._pending_writes += 1
        cache._commit_if_needed()


def test_cache_serialization_semantics_match_legacy():
    """Optimized serialization should preserve JSON content semantics."""
    payload = _build_payload(size=200, unicode_labels=True)
    legacy = _legacy_serialize_json(payload)
    optimized = _candidate_serialize_json(payload)

    assert json.loads(optimized) == json.loads(legacy)
    assert len(optimized) <= len(legacy)


@pytest.mark.benchmark
class TestCacheSerializationBenchmarks:
    """Benchmark cache serialization behavior."""

    def test_benchmark_cache_json_serialize_legacy_ascii(self, benchmark):
        """Benchmark legacy JSON serialization on ASCII-like payload."""
        payload = _build_payload(unicode_labels=False)
        result = benchmark(_legacy_serialize_json, payload)
        assert result

    def test_benchmark_cache_json_serialize_optimized_ascii(self, benchmark):
        """Benchmark optimized JSON serialization on ASCII-like payload."""
        payload = _build_payload(unicode_labels=False)
        result = benchmark(_candidate_serialize_json, payload)
        assert result

    def test_benchmark_cache_json_serialize_legacy_unicode(self, benchmark):
        """Benchmark legacy JSON serialization on unicode-heavy payload."""
        payload = _build_payload(unicode_labels=True)
        result = benchmark(_legacy_serialize_json, payload)
        assert result

    def test_benchmark_cache_json_serialize_optimized_unicode(self, benchmark):
        """Benchmark optimized JSON serialization on unicode-heavy payload."""
        payload = _build_payload(unicode_labels=True)
        result = benchmark(_candidate_serialize_json, payload)
        assert result

    def test_benchmark_cache_set_legacy(self, benchmark, tmp_path):
        """Benchmark legacy cache write path."""
        cache = Cache(tmp_path)
        payload = _build_payload(unicode_labels=True)

        benchmark(_legacy_cache_set, cache, "payload_key", payload)

        cache.close()

    def test_benchmark_cache_set_optimized(self, benchmark, tmp_path):
        """Benchmark optimized cache write path."""
        cache = Cache(tmp_path)
        payload = _build_payload(unicode_labels=True)

        benchmark(_optimized_cache_set, cache, "payload_key", payload)

        cache.close()
