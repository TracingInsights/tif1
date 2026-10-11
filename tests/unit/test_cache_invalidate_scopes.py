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
    assert cache.get("2024/GP/Race/drivers.json") == {"drivers": []}
    cache.close()


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
