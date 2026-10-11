"""Invalidation and negative-verdict contracts across every SQLite table.

Regression coverage for the docs audit findings: ``clear()`` and
``invalidate("telemetry")`` must drop the polars frame table as well as the
pandas one, ``invalidate("memory")`` must not lose persisted negative
verdicts, and a successful ``set()`` must remove a stale verdict from SQLite
so it cannot resurface in the next process.
"""

from __future__ import annotations

import pytest

from tif1.cache import _FRAME_TABLES, Cache

pytestmark = pytest.mark.unit

ROW = (2024, "GP", "Race", "VER", 1)


def _insert_frame_rows(cache: Cache) -> None:
    assert cache.conn is not None
    for table in _FRAME_TABLES.values():
        cache.conn.execute(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?)", (*ROW, b"blob"))
    cache.conn.commit()


def _count(cache: Cache, table: str) -> int:
    assert cache.conn is not None
    return cache.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_clear_drops_both_frame_tables(tmp_path) -> None:
    cache = Cache(tmp_path)
    _insert_frame_rows(cache)
    assert {_count(cache, t) for t in _FRAME_TABLES.values()} == {1}
    cache.clear()
    assert {_count(cache, t) for t in _FRAME_TABLES.values()} == {0}
    cache.close()


def test_invalidate_telemetry_drops_both_frame_tables_and_keeps_json(tmp_path) -> None:
    cache = Cache(tmp_path)
    _insert_frame_rows(cache)
    cache.set("2024/GP/Race/drivers.json", {"drivers": []})
    cache.invalidate("telemetry")
    assert {_count(cache, t) for t in _FRAME_TABLES.values()} == {0}
    # The JSON row must survive in SQLite, not just in the memory tier.
    assert _count(cache, "cache") == 1
    cache.close()
    reopened = Cache(tmp_path)
    assert reopened.get("2024/GP/Race/drivers.json") == {"drivers": []}
    reopened.close()


def test_set_removes_persisted_verdict_before_map_is_loaded(tmp_path) -> None:
    # Record a verdict in one process, then in a fresh process write the
    # payload before anything has loaded the verdict map.
    key = "2024/GP/Race/weather.json"
    first = Cache(tmp_path)
    first.record_missing(key)
    first.close()

    second = Cache(tmp_path)
    assert second._missing_payloads is None  # map not loaded yet
    second.set(key, {"ok": True})
    assert not second.is_known_missing(key)
    second.close()

    third = Cache(tmp_path)
    assert not third.is_known_missing(key)
    third.close()


def test_invalidate_memory_keeps_persisted_negative_verdicts(tmp_path) -> None:
    cache = Cache(tmp_path)
    cache.record_missing("2024/GP/Race/weather.json")
    cache.invalidate("memory")
    # The in-memory mirror was dropped; the verdict reloads from SQLite.
    assert cache.is_known_missing("2024/GP/Race/weather.json")
    cache.close()


def test_set_removes_stale_verdict_from_sqlite(tmp_path) -> None:
    cache = Cache(tmp_path)
    key = "2024/GP/Race/weather.json"
    cache.record_missing(key)
    assert cache.is_known_missing(key)
    cache.set(key, {"ok": True})
    assert not cache.is_known_missing(key)
    cache.close()

    # A fresh process (new Cache on the same directory) must not see the verdict.
    reopened = Cache(tmp_path)
    assert not reopened.is_known_missing(key)
    assert reopened.get(key) == {"ok": True}
    reopened.close()
