"""Tests for cache module."""

import logging
import sqlite3

from tif1.cache import Cache, get_cache


class TestCache:
    """Test Cache class."""

    def test_cache_init_default(self, tmp_path, monkeypatch):
        """Test cache initialization with default path."""
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

        for idx in range(24):
            cache.set(f"key-{idx}", {"data": idx})

        assert cache._pending_writes == 24

        cache.set("key-24", {"data": 24})
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
        """Items beyond _MEMORY_CACHE_MAX_ITEMS are evicted (oldest first)."""
        cache = Cache(tmp_path)
        limit = 10
        # Manually set a smaller limit to test eviction pattern
        for i in range(limit + 5):
            cache._remember_memory_entry(cache._memory_cache, f"key-{i}", f'"{i}"', limit)
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
        cache.conn.execute("DELETE FROM cache")
        cache.conn.commit()
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
