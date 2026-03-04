"""Benchmarks for core driver lookup paths."""

from __future__ import annotations

import pytest

from tif1.core import Session


def _build_driver_payload(count: int = 4000) -> list[dict]:
    """Build deterministic mock drivers payload."""
    return [{"driver": f"DRV{i:04d}", "team": f"Team {i}"} for i in range(count)]


def _build_lookup_queries(drivers_payload: list[dict], count: int = 1000) -> list[str]:
    """Build deterministic mixed hit/miss query set."""
    drivers = [d["driver"] for d in drivers_payload]
    half = count // 2

    hits = [drivers[(idx * 29) % len(drivers)] for idx in range(half)]
    misses = [f"MISS{idx:05d}" for idx in range(count - half)]
    return hits + misses


def _legacy_has_driver(drivers_payload: list[dict], driver: str) -> bool:
    """Legacy per-call driver validation behavior."""
    driver_codes = [d.get("driver") for d in drivers_payload if isinstance(d, dict)]
    return driver in driver_codes


def _legacy_batch_lookup(drivers_payload: list[dict], queries: list[str]) -> int:
    """Run legacy lookup logic over a query batch."""
    return sum(1 for query in queries if _legacy_has_driver(drivers_payload, query))


def _optimized_batch_lookup(session: Session, queries: list[str]) -> int:
    """Run optimized lookup logic over a query batch."""
    return sum(1 for query in queries if session._has_driver_code(query))


def test_driver_lookup_semantics_match_legacy():
    """Optimized lookup should match legacy lookup results."""
    drivers_payload = _build_driver_payload(1000)
    queries = _build_lookup_queries(drivers_payload, 200)

    session = Session(2025, "Test GP", "Race", enable_cache=False)
    session._drivers = drivers_payload

    legacy_found = _legacy_batch_lookup(drivers_payload, queries)
    optimized_found = _optimized_batch_lookup(session, queries)

    assert optimized_found == legacy_found


@pytest.mark.benchmark
class TestCoreDriverBenchmarks:
    """Benchmark driver lookup performance."""

    def test_benchmark_driver_lookup_legacy(self, benchmark):
        """Benchmark legacy list-based lookup."""
        drivers_payload = _build_driver_payload(4000)
        queries = _build_lookup_queries(drivers_payload, 1000)

        result = benchmark(_legacy_batch_lookup, drivers_payload, queries)
        assert result == 500

    def test_benchmark_driver_lookup_optimized(self, benchmark):
        """Benchmark optimized set-based lookup with cached driver codes."""
        drivers_payload = _build_driver_payload(4000)
        queries = _build_lookup_queries(drivers_payload, 1000)

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        session._drivers = drivers_payload
        # Warm cache to emulate repeated lookups on the same session.
        session._has_driver_code("DRV0000")

        result = benchmark(_optimized_batch_lookup, session, queries)
        assert result == 500
