"""Tests for cache module."""

import logging
import sqlite3

import pytest

from tif1.cache import (
    FASTEST_LAP_REF_MISS,
    Cache,
    LRUCache,
    SessionMemo,
    clear_lap_cache,
    get_backend_lap_cache,
    get_cache,
)


class TestCache:
    """Test Cache class."""

    def test_cache_init_default(self, tmp_path, monkeypatch):
        """Test cache initialization with the configured environment path."""
        from tif1.config import Config

        monkeypatch.setattr(Config, "_instance", None)
        monkeypatch.setenv("TIF1_CACHE_DIR", str(tmp_path / ".tif1" / "cache"))
        cache = Cache()
        assert cache.cache_dir == tmp_path / ".tif1" / "cache"
        assert cache.cache_dir.exists()

    def test_cache_init_uses_config_cache_dir(self, tmp_path, monkeypatch):
        """Test cache initialization uses config cache_dir when env var is unset."""
        monkeypatch.delenv("TIF1_CACHE_DIR", raising=False)
        configured_dir = tmp_path / "configured_cache"

        class StubConfig:
            def get(self, key, default=None):
                if key == "cache_dir":
                    return str(configured_dir)
                return default

        monkeypatch.setattr("tif1.config.get_config", lambda: StubConfig())
        cache = Cache()
        assert cache.cache_dir == configured_dir
        assert cache.cache_dir.exists()

    def test_cache_init_custom(self, tmp_path):
        """Test cache initialization with custom path."""
        custom_dir = tmp_path / "custom_cache"
        cache = Cache(custom_dir)
        assert cache.cache_dir == custom_dir
        assert cache.cache_dir.exists()

    def test_cache_init_none_config_falls_back_to_default(self, tmp_path, monkeypatch):
        """A None cache_dir config (e.g. "cache_dir": null in .tif1rc) must not
        become a literal "./None" directory via str(None)."""
        from tif1.config import Config

        monkeypatch.setattr(Config, "_instance", None)
        monkeypatch.delenv("TIF1_CACHE_DIR", raising=False)
        fake_default = tmp_path / "default-cache"
        monkeypatch.setattr("tif1.config._default_cache_dir", lambda: fake_default)
        config = Config()
        config.set("cache_dir", None)

        cache = Cache()
        assert cache.cache_dir == fake_default
        assert cache.cache_dir.name != "None"

    def test_cache_set_get(self, tmp_path):
        """Test setting and getting cache data."""
        cache = Cache(tmp_path)
        test_data = {"drivers": ["VER", "HAM"], "count": 2}

        cache.set("test_key", test_data)
        result = cache.get("test_key")

        assert result == test_data

    def test_cache_get_missing(self, tmp_path):
        """Test getting non-existent cache key."""
        cache = Cache(tmp_path)
        result = cache.get("missing_key")
        assert result is None

    def test_cache_sqlite_backend(self, tmp_path):
        """Test SQLite backend."""
        cache = Cache(tmp_path)
        test_data = {"data": "x" * 1000}

        cache.set("sqlite_test", test_data)
        result = cache.get("sqlite_test")

        assert result == test_data
        assert cache.db_path.exists()
        assert cache.db_path.suffix == ".sqlite"

    def test_cache_batches_commits(self, tmp_path):
        """Test cache writes are batched instead of committed every write."""
        cache = Cache(tmp_path)
        # Pin the interval so the test checks the batching mechanism, not the
        # configured default (see config cache_commit_interval).
        cache._commit_interval = 5

        for idx in range(4):
            cache.set(f"key-{idx}", {"data": idx})

        assert cache._pending_writes == 4

        cache.set("key-4", {"data": 4})
        assert cache._pending_writes == 0

    def test_cache_close_flushes_pending_writes(self, tmp_path):
        """Test pending writes are committed when cache closes."""
        cache = Cache(tmp_path)
        cache.set("flush-key", {"value": 1})
        assert cache._pending_writes == 1

        db_path = cache.db_path
        cache.close()

        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute("SELECT data FROM cache WHERE key = ?", ("flush-key",)).fetchone()
        finally:
            conn.close()

        assert row is not None

    def test_cache_clear(self, tmp_path):
        """Test clearing cache."""
        cache = Cache(tmp_path)
        cache.set("key1", {"data": 1})
        cache.set("key2", {"data": 2})

        assert cache.get("key1") is not None
        assert cache.get("key2") is not None

        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None

    def test_cache_telemetry(self, tmp_path):
        """Test telemetry cache operations."""
        cache = Cache(tmp_path)
        test_data = {"speed": [100, 200, 300]}

        cache.set_telemetry(2025, "Abu Dhabi", "Race", "VER", 1, test_data)
        result = cache.get_telemetry(2025, "Abu Dhabi", "Race", "VER", 1)

        assert result == test_data

        # Test miss
        miss = cache.get_telemetry(2025, "Abu Dhabi", "Race", "HAM", 1)
        assert miss is None

    def test_cache_clear_removes_telemetry(self, tmp_path):
        """Test clear removes telemetry cache entries."""
        cache = Cache(tmp_path)
        cache.set_telemetry(2025, "Abu Dhabi", "Race", "VER", 1, {"speed": [100, 200]})
        assert cache.get_telemetry(2025, "Abu Dhabi", "Race", "VER", 1) is not None

        cache.clear()

        assert cache.get_telemetry(2025, "Abu Dhabi", "Race", "VER", 1) is None

    def test_has_session_data_detects_json_rows(self, tmp_path):
        """Session probe should detect cache rows for matching session prefix."""
        cache = Cache(tmp_path)
        cache.set("2025/Abu%20Dhabi%20Grand%20Prix/Race/drivers.json", {"drivers": []})

        assert cache.has_session_data(2025, "Abu%20Dhabi%20Grand%20Prix", "Race") is True
        assert cache.has_session_data(2025, "Abu%20Dhabi%20Grand%20Prix", "Qualifying") is False

    def test_has_session_data_detects_telemetry_rows(self, tmp_path):
        """Session probe should detect telemetry rows even without JSON rows."""
        cache = Cache(tmp_path)
        cache.set_telemetry(
            2025,
            "Abu%20Dhabi%20Grand%20Prix",
            "Race",
            "VER",
            1,
            {"speed": [100, 200]},
        )

        assert cache.has_session_data(2025, "Abu%20Dhabi%20Grand%20Prix", "Race") is True
        assert cache.has_session_data(2025, "Abu%20Dhabi%20Grand%20Prix", "Qualifying") is False

    def test_get_cache_singleton(self):
        """Test that get_cache returns singleton."""
        cache1 = get_cache()
        cache2 = get_cache()
        assert cache1 is cache2


class TestGetFromMemory:
    """Tests for _get_from_memory."""

    def test_hit_returns_data(self, tmp_path):
        """Memory cache hit returns deserialized data."""
        cache = Cache(tmp_path)
        cache.set("mem_key", {"hello": "world"})
        result = cache._get_from_memory("mem_key")
        assert result == {"hello": "world"}

    def test_miss_returns_none(self, tmp_path):
        """Memory cache miss returns None."""
        cache = Cache(tmp_path)
        result = cache._get_from_memory("nonexistent")
        assert result is None

    def test_conn_none_returns_none(self, tmp_path):
        """Returns None when conn is None."""
        cache = Cache(tmp_path)
        cache.conn = None
        result = cache._get_from_memory("any_key")
        assert result is None


class TestMemoryCacheEviction:
    """Test LRU eviction in memory cache."""

    def test_eviction_beyond_limit(self, tmp_path):
        """Items beyond maxsize are evicted (oldest first)."""
        cache = Cache(tmp_path)
        limit = 10
        # Manually shrink the tier limit to test the eviction pattern
        cache._memory_cache.maxsize = limit
        for i in range(limit + 5):
            cache._memory_cache.set(f"key-{i}", f'"{i}"')
        assert len(cache._memory_cache) == limit
        # Oldest keys should be evicted
        assert "key-0" not in cache._memory_cache
        assert f"key-{limit + 4}" in cache._memory_cache


class TestGetConnNone:
    """Test get with conn=None."""

    def test_get_returns_none_when_no_conn(self, tmp_path):
        """get() returns None when conn is None."""
        cache = Cache(tmp_path)
        cache.conn = None
        assert cache.get("any") is None


class TestSetEdgeCases:
    """Test set with read_only and conn=None."""

    def test_set_read_only_does_nothing(self, tmp_path):
        """set() with read_only=True skips writes."""
        cache = Cache(tmp_path)
        cache.read_only = True
        cache.set("key", {"data": 1})
        cache.read_only = False
        assert cache.get("key") is None

    def test_set_conn_none_does_nothing(self, tmp_path):
        """set() with conn=None skips writes."""
        cache = Cache(tmp_path)
        cache.conn = None
        cache.set("key", {"data": 1})
        # Re-init to verify nothing was written
        cache2 = Cache(tmp_path)
        assert cache2.get("key") is None


class TestTelemetryMemoryCache:
    """Test telemetry memory cache operations."""

    def test_telemetry_memory_hit(self, tmp_path):
        """Telemetry set then get returns data from memory cache."""
        cache = Cache(tmp_path)
        data = {"speed": [100, 200, 300]}
        cache.set_telemetry(2025, "Bahrain", "Race", "VER", 1, data)
        # Key should be in memory cache
        cache_key = (2025, "Bahrain", "Race", "VER", 1)
        assert cache_key in cache._memory_telemetry_cache
        result = cache.get_telemetry(2025, "Bahrain", "Race", "VER", 1)
        assert result == data

    def test_set_telemetry_read_only_does_nothing(self, tmp_path):
        """set_telemetry() with read_only=True skips writes."""
        cache = Cache(tmp_path)
        cache.read_only = True
        cache.set_telemetry(2025, "Bahrain", "Race", "VER", 1, {"speed": [100]})
        cache.read_only = False
        assert cache.get_telemetry(2025, "Bahrain", "Race", "VER", 1) is None


class TestClearEdgeCases:
    """Test clear with read_only and conn=None."""

    def test_clear_read_only_logs_warning(self, tmp_path, caplog):
        """clear() with read_only=True logs a warning."""
        cache = Cache(tmp_path)
        cache.read_only = True
        with caplog.at_level(logging.WARNING):
            cache.clear()
        assert "Cannot clear cache" in caplog.text

    def test_clear_conn_none_logs_warning(self, tmp_path, caplog):
        """clear() with conn=None logs a warning."""
        cache = Cache(tmp_path)
        cache.conn = None
        with caplog.at_level(logging.WARNING):
            cache.clear()
        assert "Cannot clear cache" in caplog.text


class TestHasSessionData:
    """Test has_session_data edge cases."""

    def test_conn_none_returns_false(self, tmp_path):
        """has_session_data returns False when conn is None."""
        cache = Cache(tmp_path)
        cache.conn = None
        assert cache.has_session_data(2025, "Bahrain", "Race") is False

    def test_memory_cache_hit(self, tmp_path):
        """has_session_data finds matching prefix in memory cache."""
        cache = Cache(tmp_path)
        cache.set("2025/Bahrain/Race/drivers.json", {"drivers": []})
        # Clear SQLite to ensure it's the memory cache hit path
        conn = cache.conn
        assert conn is not None
        conn.execute("DELETE FROM cache")
        conn.commit()
        assert cache.has_session_data(2025, "Bahrain", "Race") is True


class TestAsyncOperations:
    """Test async wrappers."""

    async def test_get_async(self, tmp_path):
        """get_async returns cached data."""
        cache = Cache(tmp_path)
        cache.set("async_key", {"value": 42})
        result = await cache.get_async("async_key")
        assert result == {"value": 42}

    async def test_set_async(self, tmp_path):
        """set_async stores data retrievable by get."""
        cache = Cache(tmp_path)
        await cache.set_async("async_set_key", {"value": 99})
        result = cache.get("async_set_key")
        assert result == {"value": 99}

    async def test_get_telemetry_async(self, tmp_path):
        """get_telemetry_async returns cached telemetry."""
        cache = Cache(tmp_path)
        data = {"speed": [150]}
        cache.set_telemetry(2025, "Jeddah", "FP1", "HAM", 3, data)
        result = await cache.get_telemetry_async(2025, "Jeddah", "FP1", "HAM", 3)
        assert result == data

    async def test_set_telemetry_async(self, tmp_path):
        """set_telemetry_async stores data retrievable by get_telemetry."""
        cache = Cache(tmp_path)
        data = {"throttle": [80, 90]}
        await cache.set_telemetry_async(2025, "Jeddah", "FP1", "LEC", 5, data)
        result = cache.get_telemetry(2025, "Jeddah", "FP1", "LEC", 5)
        assert result == data


class TestCloseEdgeCases:
    """Test close on already-closed cache."""

    def test_close_already_closed(self, tmp_path):
        """close() on already-closed cache (conn=None) is a no-op."""
        cache = Cache(tmp_path)
        cache.close()
        assert cache.conn is None
        # Second close should not raise
        cache.close()
        assert cache.conn is None


class TestTelemetryBatchCoverage:
    """Target uncovered telemetry batch and cleanup paths."""

    def test_get_telemetry_batch_memory_and_sqlite(self, tmp_path):
        cache = Cache(tmp_path)
        cache.set_telemetry(2025, "Test GP", "Race", "VER", 1, {"speed": [300]})
        cache.set_telemetry(2025, "Test GP", "Race", "HAM", 2, {"speed": [290]})

        # Clear one entry from memory so SQLite batch path is exercised.
        with cache._memory_cache_lock:
            cache._memory_telemetry_cache.pop((2025, "Test GP", "Race", "HAM", 2), None)

        result = cache.get_telemetry_batch(2025, "Test GP", "Race", [("VER", 1), ("HAM", 2)])
        assert ("VER", 1) in result
        assert ("HAM", 2) in result

    def test_get_telemetry_batch_empty_and_conn_none(self, tmp_path):
        cache = Cache(tmp_path)
        assert cache.get_telemetry_batch(2025, "Test GP", "Race", []) == {}
        cache.conn = None
        assert cache.get_telemetry_batch(2025, "Test GP", "Race", [("VER", 1)]) == {}

    def test_get_telemetry_batch_handles_sqlite_errors(self, tmp_path, monkeypatch):
        cache = Cache(tmp_path)

        class BrokenConn:
            def execute(self, query, params):  # noqa: ARG002
                raise sqlite3.Error("boom")

        monkeypatch.setattr(cache, "conn", BrokenConn())
        out = cache.get_telemetry_batch(2025, "Test GP", "Race", [("VER", 1)])
        assert out == {}

    async def test_get_telemetry_batch_async(self, tmp_path):
        cache = Cache(tmp_path)
        cache.set_telemetry(2025, "Test GP", "Race", "NOR", 7, {"speed": [301]})
        out = await cache.get_telemetry_batch_async(2025, "Test GP", "Race", [("NOR", 7)])
        assert ("NOR", 7) in out

    def test_set_telemetry_handles_json_failure(self, tmp_path, monkeypatch):
        cache = Cache(tmp_path)
        monkeypatch.setattr(
            "tif1.cache.json_dumps", lambda _data: (_ for _ in ()).throw(TypeError("x"))
        )
        # Should be swallowed by write-skip path.
        cache.set_telemetry(2025, "Test GP", "Race", "ALO", 9, {"speed": [280]})
        assert cache.get_telemetry(2025, "Test GP", "Race", "ALO", 9) is None

    def test_has_session_data_true_when_sqlite_cache_hit(self, tmp_path):
        cache = Cache(tmp_path)
        cache.set("2025/Test%20GP/Race/drivers.json", {"drivers": [{"driver": "VER"}]})
        # Clear memory cache so SQLite path is used.
        with cache._memory_cache_lock:
            cache._memory_cache.clear()
        assert cache.has_session_data(2025, "Test%20GP", "Race") is True

    def test_cleanup_cache_resets_singleton(self, tmp_path, monkeypatch):
        import tif1.cache as cache_module

        local_cache = Cache(tmp_path)
        monkeypatch.setattr(cache_module, "_cache", local_cache)
        cache_module._cleanup_cache()
        assert cache_module._cache is None

    def test_has_session_data_probe_failure_defaults_true(self, tmp_path, monkeypatch):
        cache = Cache(tmp_path)

        class BrokenConn:
            def execute(self, query, params):  # noqa: ARG002
                raise sqlite3.Error("probe failed")

        monkeypatch.setattr(cache, "conn", BrokenConn())
        assert cache.has_session_data(2025, "Broken", "Race") is True

    def test_get_telemetry_handles_sqlite_error(self, tmp_path, monkeypatch):
        cache = Cache(tmp_path)

        class BrokenConn:
            def execute(self, query, params):  # noqa: ARG002
                raise sqlite3.Error("telemetry read failed")

        monkeypatch.setattr(cache, "conn", BrokenConn())
        assert cache.get_telemetry(2025, "Broken", "Race", "VER", 1) is None

    def test_get_telemetry_sqlite_hit_updates_memory(self, tmp_path):
        cache = Cache(tmp_path)
        cache.set_telemetry(2025, "Monza", "Race", "VER", 12, {"speed": [333]})

        key = (2025, "Monza", "Race", "VER", 12)
        with cache._memory_cache_lock:
            cache._memory_telemetry_cache.pop(key, None)
            assert key not in cache._memory_telemetry_cache

        out = cache.get_telemetry(2025, "Monza", "Race", "VER", 12)
        assert out == {"speed": [333]}
        assert key in cache._memory_telemetry_cache

    def test_get_telemetry_conn_none_returns_none(self, tmp_path):
        cache = Cache(tmp_path)
        cache.conn = None
        assert cache.get_telemetry(2025, "Nowhere", "Race", "VER", 1) is None


class TestLRUCache:
    """Test the single LRUCache implementation (canonical home: tif1.cache)."""

    def test_init(self):
        cache = LRUCache(maxsize=5)
        assert cache.maxsize == 5
        assert len(cache.cache) == 0

    def test_get_miss(self):
        cache = LRUCache()
        assert cache.get("missing") is None

    def test_set_and_get(self):
        cache = LRUCache()
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_eviction(self):
        cache = LRUCache(maxsize=2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        assert cache.get("a") is None
        assert cache.get("b") == 2
        assert cache.get("c") == 3

    def test_move_to_end_on_get(self):
        cache = LRUCache(maxsize=2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.get("a")  # Move "a" to end
        cache.set("c", 3)  # Should evict "b"
        assert cache.get("a") == 1
        assert cache.get("b") is None
        assert cache.get("c") == 3

    def test_update_existing_key(self):
        cache = LRUCache(maxsize=2)
        cache.set("a", 1)
        cache.set("a", 2)
        assert cache.get("a") == 2
        assert len(cache.cache) == 1

    def test_clear(self):
        cache = LRUCache()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.clear()
        assert cache.get("a") is None
        assert cache.get("b") is None

    def test_lock_free_get_does_not_reorder(self):
        """ordered=False reads skip the lock and leave LRU order untouched."""
        cache = LRUCache(maxsize=2)
        cache.set("a", 1)
        cache.set("b", 2)
        assert cache.get("a", ordered=False) == 1
        cache.set("c", 3)  # Evicts "a" because the lock-free read did not refresh it
        assert cache.get("a") is None
        assert cache.get("b") == 2
        assert cache.get("c") == 3

    def test_dunder_protocols_and_pop(self):
        cache = LRUCache(maxsize=3)
        cache.set("a", 1)
        assert "a" in cache
        assert len(cache) == 1
        assert list(iter(cache)) == ["a"]
        assert cache.pop("a") == 1
        assert cache.pop("a") is None
        assert len(cache) == 0

    def test_shared_external_lock(self):
        """LRU instances can share an externally-owned lock."""
        import threading

        shared = threading.Lock()
        first = LRUCache(maxsize=2, lock=shared)
        second = LRUCache(maxsize=2, lock=shared)
        first.set("k", 1)
        second.set("k", 2)
        assert first.get("k") == 1
        assert second.get("k") == 2


class TestBackendLapCaches:
    """Test the process-global pandas/polars lap cache tier."""

    def test_get_backend_lap_cache_isolated_per_lib(self):
        from tif1.cache import _global_lap_cache, _global_lap_cache_polars

        assert get_backend_lap_cache("pandas") is _global_lap_cache
        assert get_backend_lap_cache("polars") is _global_lap_cache_polars

    def test_clear_lap_cache_clears_both_backends(self):
        from tif1.cache import _global_lap_cache, _global_lap_cache_polars

        _global_lap_cache.set("k", "pandas")
        _global_lap_cache_polars.set("k", "polars")
        clear_lap_cache()
        assert _global_lap_cache.get("k") is None
        assert _global_lap_cache_polars.get("k") is None

    def test_core_aliases(self):
        """tif1.core keeps clearing the same (tif1.cache) caches."""
        import tif1.core as core_module
        from tif1.cache import _global_lap_cache

        assert core_module.clear_lap_cache is clear_lap_cache
        assert core_module.LRUCache is LRUCache

        _global_lap_cache.set("k", 1)
        core_module.clear_lap_cache()
        assert _global_lap_cache.get("k") is None


class TestSessionMemo:
    """Test the per-session memo tier."""

    def test_json_kind_path_filtering(self):
        memo = SessionMemo()
        assert memo.set("json", "drivers.json", {"drivers": []}) is True
        assert memo.set("json", "VER/laptimes.json", {"lap": [1]}) is True
        # Unknown top-level paths are rejected to avoid memory bloat
        assert memo.set("json", "mystery.json", {"x": 1}) is False
        # Non-dict payloads are rejected entirely
        assert memo.set("json", "drivers.json", [1, 2, 3]) is False

        assert memo.get("json", "drivers.json") == {"drivers": []}
        assert memo.get("json", "VER/laptimes.json") == {"lap": [1]}
        assert memo.get("json", "mystery.json") is None
        assert memo.kind_size("json") == 2

    def test_telemetry_payload_kind_requires_non_empty_dict(self):
        memo = SessionMemo()
        assert memo.set("telemetry_payload", ("VER", 1), {"speed": [300]}) is True
        assert memo.set("telemetry_payload", ("VER", 2), {}) is False
        assert memo.set("telemetry_payload", ("VER", 3), None) is False
        assert memo.get("telemetry_payload", ("VER", 1)) == {"speed": [300]}
        assert memo.get("telemetry_payload", ("VER", 3)) is None

    def test_telemetry_df_kind_accepts_any_value(self):
        memo = SessionMemo()
        sentinel = object()
        assert memo.set("telemetry_df", ("VER", 1), sentinel) is True
        assert memo.get("telemetry_df", ("VER", 1)) is sentinel
        assert memo.contains("telemetry_df", ("VER", 1))
        assert not memo.contains("telemetry_df", ("HAM", 1))
        assert memo.kind_size("telemetry_df") == 1

    def test_items_and_unknown_kind(self):
        memo = SessionMemo()
        memo.set("json", "drivers.json", {"drivers": []})
        assert dict(memo.items("json")) == {"drivers.json": {"drivers": []}}
        with pytest.raises(ValueError, match="Unknown session memo kind"):
            memo.get("bogus", "x")
        with pytest.raises(ValueError, match="Unknown session memo kind"):
            memo.set("bogus", "x", 1)

    def test_fastest_lap_ref_source_tracking(self):
        memo = SessionMemo()
        laps_src, drivers_src = object(), object()

        # Misses return the sentinel until memoized
        assert memo.get_fastest_lap_ref_if_current("laps", id(laps_src)) is FASTEST_LAP_REF_MISS
        memo.set_fastest_lap_ref(("VER", 5), source_kind="laps", source_id=id(laps_src))
        assert memo.get_fastest_lap_ref_if_current("laps", id(laps_src)) == ("VER", 5)
        # The other source kind's tag was cleared, so it misses
        assert (
            memo.get_fastest_lap_ref_if_current("drivers", id(drivers_src)) is FASTEST_LAP_REF_MISS
        )
        # Stale laps source id also misses
        assert memo.get_fastest_lap_ref_if_current("laps", id(object())) is FASTEST_LAP_REF_MISS

        # Setting via drivers clears the laps tag
        memo.set_fastest_lap_ref(("HAM", 2), source_kind="drivers", source_id=id(drivers_src))
        assert memo.get_fastest_lap_ref_if_current("drivers", id(drivers_src)) == ("HAM", 2)
        assert memo.get_fastest_lap_ref_if_current("laps", id(laps_src)) is FASTEST_LAP_REF_MISS

        # A None ref stores no source tag (stale sources can never match)
        memo.set_fastest_lap_ref(None, source_kind="drivers", source_id=id(drivers_src))
        assert memo.fastest_lap_ref is None
        assert (
            memo.get_fastest_lap_ref_if_current("drivers", id(drivers_src)) is FASTEST_LAP_REF_MISS
        )

    def test_telemetry_failure_tracking(self):
        memo = SessionMemo()
        assert memo.telemetry_failure_count("VER") == 0
        assert memo.record_telemetry_failure("VER") == 1
        assert memo.record_telemetry_failure("VER") == 2
        assert not memo.is_telemetry_unavailable("VER")
        # Third failure marks the driver unavailable
        assert memo.record_telemetry_failure("VER") == 3
        assert memo.is_telemetry_unavailable("VER")
        assert not memo.is_telemetry_unavailable("HAM")

        assert not memo.is_failure_suppressed("VER")
        memo.suppress_failure_warnings("VER")
        assert memo.is_failure_suppressed("VER")

    def test_has_session_data_probe_result(self):
        memo = SessionMemo()
        assert memo.has_session_data is None
        memo.has_session_data = True
        assert memo.has_session_data is True

    def test_clear_resets_everything(self):
        memo = SessionMemo()
        memo.set("json", "drivers.json", {"drivers": []})
        memo.set("telemetry_payload", ("VER", 1), {"speed": [300]})
        memo.set("telemetry_df", ("VER", 1), object())
        memo.set_fastest_lap_ref(("VER", 5), source_kind="laps", source_id=1)
        memo.has_session_data = False
        memo.record_telemetry_failure("VER")
        memo.suppress_failure_warnings("VER")

        memo.clear()
        assert memo.kind_size("json") == 0
        assert memo.kind_size("telemetry_payload") == 0
        assert memo.kind_size("telemetry_df") == 0
        assert memo.fastest_lap_ref is None
        assert memo.fastest_lap_ref_laps_source_id is None
        assert memo.fastest_lap_ref_driver_source_id is None
        assert memo.has_session_data is None
        assert memo.telemetry_failure_count("VER") == 0
        assert not memo.is_telemetry_unavailable("VER")
        assert not memo.is_failure_suppressed("VER")


class TestKindBasedInterface:
    """Test the unified Cache.get_entry / set_entry / invalidate interface."""

    def test_json_round_trip(self, tmp_path):
        cache = Cache(tmp_path)
        cache.set_entry("json", "2025/Monza/Race/drivers.json", {"drivers": ["VER"]})
        assert cache.get_entry("json", "2025/Monza/Race/drivers.json") == {"drivers": ["VER"]}
        cache.close()

    def test_telemetry_round_trip(self, tmp_path):
        cache = Cache(tmp_path)
        key = (2025, "Monza", "Race", "VER", 1)
        cache.set_entry("telemetry", key, {"speed": [300]})
        assert cache.get_entry("telemetry", key) == {"speed": [300]}
        cache.close()

    def test_wrappers_match_unified_interface(self, tmp_path):
        cache = Cache(tmp_path)
        cache.set("k", {"v": 1})
        assert cache.get_entry("json", "k") == cache.get("k") == {"v": 1}
        cache.set_telemetry(2025, "Monza", "Race", "VER", 1, {"speed": [300]})
        assert cache.get_entry(
            "telemetry", (2025, "Monza", "Race", "VER", 1)
        ) == cache.get_telemetry(2025, "Monza", "Race", "VER", 1)
        cache.close()

    def test_unknown_kind_rejected(self, tmp_path):
        cache = Cache(tmp_path)
        with pytest.raises(ValueError, match="Unknown cache kind"):
            cache.get_entry("bogus", "k")
        with pytest.raises(ValueError, match="Unknown cache kind"):
            cache.set_entry("bogus", "k", {"v": 1})
        cache.close()

    def test_invalidate_memory_keeps_sqlite(self, tmp_path):
        cache = Cache(tmp_path)
        cache.set("k", {"v": 1})
        cache.invalidate("memory")
        assert len(cache._memory_cache) == 0
        # SQLite tier still serves the entry
        assert cache.get("k") == {"v": 1}
        cache.close()

    def test_invalidate_json_only(self, tmp_path):
        cache = Cache(tmp_path)
        cache.set("k", {"v": 1})
        cache.set_telemetry(2025, "Monza", "Race", "VER", 1, {"speed": [300]})
        cache.invalidate("json")
        assert cache.get("k") is None
        assert cache.get_telemetry(2025, "Monza", "Race", "VER", 1) == {"speed": [300]}
        cache.close()

    def test_invalidate_telemetry_only(self, tmp_path):
        cache = Cache(tmp_path)
        cache.set("k", {"v": 1})
        cache.set_telemetry(2025, "Monza", "Race", "VER", 1, {"speed": [300]})
        cache.invalidate("telemetry")
        assert cache.get("k") == {"v": 1}
        assert cache.get_telemetry(2025, "Monza", "Race", "VER", 1) is None
        cache.close()

    def test_invalidate_all_matches_clear(self, tmp_path):
        cache = Cache(tmp_path)
        cache.set("k", {"v": 1})
        cache.set_telemetry(2025, "Monza", "Race", "VER", 1, {"speed": [300]})
        cache.invalidate("all")
        assert cache.get("k") is None
        assert cache.get_telemetry(2025, "Monza", "Race", "VER", 1) is None
        assert len(cache._memory_cache) == 0
        assert len(cache._memory_telemetry_cache) == 0
        cache.close()

    def test_invalidate_unknown_scope_rejected(self, tmp_path):
        cache = Cache(tmp_path)
        with pytest.raises(ValueError, match="Unknown invalidate scope"):
            cache.invalidate("bogus")
        cache.close()
