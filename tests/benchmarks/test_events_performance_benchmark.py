"""Benchmarks for events/session lookup paths."""

from __future__ import annotations

import pytest

from tif1.events import (
    _build_events_for_year,
    _build_sessions_for_event,
    get_events,
    get_sessions,
)


def test_get_events_returns_fresh_schedule_from_cache():
    """Cached get_events should still return a fresh schedule each call."""
    events_first = get_events(2025)
    events_first.loc[events_first.index[0], "EventName"] = "Injected Event"

    events_second = get_events(2025)
    assert "Injected Event" not in events_second["EventName"].tolist()


def test_get_sessions_returns_fresh_list_from_cache():
    """Cached get_sessions should still return a fresh list each call."""
    sessions_first = get_sessions(2025, "Chinese Grand Prix")
    sessions_first.append("Injected Session")

    sessions_second = get_sessions(2025, "Chinese Grand Prix")
    assert "Injected Session" not in sessions_second


@pytest.mark.benchmark
class TestEventsBenchmarks:
    """Benchmark events/session lookup performance."""

    def test_benchmark_get_events_legacy_build(self, benchmark):
        """Benchmark legacy per-call events construction."""
        result = benchmark(_build_events_for_year, 2025)
        assert result

    def test_benchmark_get_events_cached(self, benchmark):
        """Benchmark cached events lookup."""
        get_events(2025)  # warm cache
        result = benchmark(get_events, 2025)
        assert result

    def test_benchmark_get_sessions_legacy_build(self, benchmark):
        """Benchmark legacy per-call sessions construction."""
        result = benchmark(_build_sessions_for_event, 2025, "Chinese Grand Prix")
        assert result

    def test_benchmark_get_sessions_cached(self, benchmark):
        """Benchmark cached sessions lookup."""
        get_sessions(2025, "Chinese Grand Prix")  # warm cache
        result = benchmark(get_sessions, 2025, "Chinese Grand Prix")
        assert result
