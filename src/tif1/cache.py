"""Caching functionality with SQLite and async support."""

import asyncio
import atexit
import logging
import os
import sqlite3
import sys
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any

from .core_utils.json_utils import json_dumps, json_loads

logger = logging.getLogger(__name__)


def _default_cache_dir() -> Path:
    """Return the OS-dependent default cache directory.

    - Windows: ``%LOCALAPPDATA%/Temp/tif1``
    - macOS:   ``~/Library/Caches/tif1``
    - Linux:   ``~/.cache/tif1`` if ``~/.cache`` exists, else ``~/.tif1``
    """
    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA")
        if local:
            return Path(local) / "Temp" / "tif1"
        return Path.home() / "AppData" / "Local" / "Temp" / "tif1"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "tif1"
    dot_cache = Path.home() / ".cache"
    if dot_cache.is_dir():
        return dot_cache / "tif1"
    return Path.home() / ".tif1"


TelemetryCacheKey = tuple[int, str, str, str, int]


class Cache:
    """Cache with SQLite backend and async support."""

    def __init__(self, cache_dir: Path | None = None):
        """Initialize cache with optional custom directory.

        Args:
            cache_dir: Cache directory path
        """
        if cache_dir is None:
            env_cache_dir = os.getenv("TIF1_CACHE_DIR")
            if env_cache_dir:
                cache_dir = Path(env_cache_dir).expanduser()
            else:
                from .config import get_config

                config = get_config()
                configured_path = config.get("cache_dir", str(_default_cache_dir()))
                cache_dir = Path(str(configured_path)).expanduser()

        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        try:
            # Harden permissions to 0o700 (user read/write/execute only)
            self.cache_dir.chmod(0o700)
        except OSError:
            logger.debug(f"Failed to set restricted permissions on {self.cache_dir}")

        self.conn = None
        self.read_only = False
        self._pending_writes = 0
        self._memory_cache_lock = threading.Lock()  # Lock for memory cache operations
        self._sqlite_lock = threading.Lock()  # Lock for SQLite operations
        self._memory_cache: OrderedDict[str, str] = OrderedDict()
        self._memory_telemetry_cache: OrderedDict[TelemetryCacheKey, str] = OrderedDict()

        # Load config values for cache constants
        from .config import get_config

        config = get_config()
        self._commit_interval = config.get("cache_commit_interval", 25)
        self._sqlite_timeout = config.get("sqlite_timeout", 30.0)
        self._memory_cache_max_items = config.get("memory_cache_max_items", 1024)
        self._memory_telemetry_cache_max_items = config.get(
            "memory_telemetry_cache_max_items", 2048
        )

        self._init_sqlite()

    def _init_sqlite(self):
        """Initialize SQLite backend with WAL mode for better concurrency."""
        self.db_path = self.cache_dir / "cache.sqlite"
        conn = None
        try:
            conn = sqlite3.connect(
                str(self.db_path), check_same_thread=False, timeout=self._sqlite_timeout
            )

            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache

            conn.execute("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, data TEXT)")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_cache (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    driver TEXT,
                    lap INTEGER,
                    data TEXT,
                    PRIMARY KEY (year, gp, session, driver, lap)
                )
            """)
            conn.commit()

            # Only assign to self.conn after full success
            self.conn = conn
            self._pending_writes = 0
            logger.debug(f"SQLite cache initialized at {self.db_path} with WAL mode")
        except (OSError, sqlite3.Error, TypeError, ValueError) as e:
            # Cleanup on failure
            if conn is not None:
                try:
                    conn.close()
                except (OSError, sqlite3.Error):
                    pass
            self.conn = None
            logger.warning(f"SQLite cache unavailable: {e}")

    def _commit_if_needed(self, force: bool = False) -> None:
        """Commit batched writes periodically to reduce fsync overhead."""
        if self.conn is None:
            return
        if force or self._pending_writes >= self._commit_interval:
            self.conn.commit()
            self._pending_writes = 0

    def _remember_memory_entry(
        self, memory_cache: OrderedDict, key: Any, json_data: str, limit: int
    ) -> None:
        """Store JSON text in bounded in-memory LRU cache."""
        if key in memory_cache:
            memory_cache.move_to_end(key)
        memory_cache[key] = json_data
        if len(memory_cache) > limit:
            memory_cache.popitem(last=False)

    def _get_from_memory(self, key: str) -> Any | None:
        """Get cache entry from in-memory LRU only (no SQLite access).

        Uses truly lock-free reads for maximum concurrency. OrderedDict.get() is atomic
        in CPython due to the GIL. No LRU updates on reads to avoid lock contention.
        LRU ordering is maintained only through writes.

        Args:
            key: Cache key to lookup

        Returns:
            Cached data or None if not found
        """
        if self.conn is None:
            return None
        try:
            # Completely lock-free read - no LRU update
            json_data = self._memory_cache.get(key)

            if json_data is not None:
                return json_loads(json_data)

            return None
        except (RuntimeError, TypeError, ValueError) as e:
            logger.debug("Memory cache read error for %s: %s", key, e)
            return None

    def get(self, key: str) -> Any | None:
        """Get cached data (thread-safe).

        Uses lock-free memory cache reads for performance. Only acquires lock
        for SQLite access and LRU updates.

        Args:
            key: Cache key to lookup

        Returns:
            Cached data or None if not found
        """
        if self.conn is None:
            return None
        try:
            # Try lock-free memory read first
            result = self._get_from_memory(key)
            if result is not None:
                logger.debug("Cache hit (memory): %s", key)
                return result

            # Memory cache miss - check SQLite with lock
            json_data = None
            with self._sqlite_lock:
                result = self.conn.execute(
                    "SELECT data FROM cache WHERE key = ?", (key,)
                ).fetchone()
                if result:
                    json_data = result[0]

            # Update memory cache outside SQLite lock
            if json_data is not None:
                with self._memory_cache_lock:
                    self._remember_memory_entry(
                        self._memory_cache, key, json_data, self._memory_cache_max_items
                    )
                logger.debug("Cache hit (SQLite): %s", key)
                return json_loads(json_data)

            logger.debug("Cache miss: %s", key)
            return None
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Cache read error for %s: %s", key, e)
            return None

    async def get_async(self, key: str) -> Any | None:
        """Get cached data asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get, key)

    def set(self, key: str, data: Any) -> None:
        """Set cached data (thread-safe).

        Optimized to update memory cache first (fast, <1ms) before SQLite (slower).
        This ensures minimal lock duration for memory operations.
        """
        if self.conn is None or self.read_only:
            return
        try:
            json_data = json_dumps(data)

            # Update memory cache first (fast operation, <1ms)
            with self._memory_cache_lock:
                self._remember_memory_entry(
                    self._memory_cache, key, json_data, self._memory_cache_max_items
                )

            # Then update SQLite (slower operation)
            with self._sqlite_lock:
                self.conn.execute("INSERT OR REPLACE INTO cache VALUES (?, ?)", (key, json_data))
                self._pending_writes += 1
                self._commit_if_needed()

            logger.debug("Cached: %s", key)
        except (RuntimeError, TypeError, ValueError, sqlite3.Error):
            logger.debug("Cache write skipped: %s", key)

    async def set_async(self, key: str, data: Any) -> None:
        """Set cached data asynchronously."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.set, key, data)

    def get_telemetry(self, year: int, gp: str, session: str, driver: str, lap: int) -> Any | None:
        """Get cached telemetry data (thread-safe).

        Uses lock-free memory cache reads for performance. Only acquires lock
        for SQLite access and LRU updates.

        Args:
            year: Season year
            gp: Grand Prix identifier
            session: Session type
            driver: Driver code
            lap: Lap number

        Returns:
            Cached telemetry data or None if not found
        """
        if self.conn is None:
            return None
        try:
            cache_key = (year, gp, session, driver, lap)

            # Completely lock-free memory cache read - no LRU update
            json_data = self._memory_telemetry_cache.get(cache_key)

            if json_data is not None:
                logger.debug("Telemetry cache hit: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
                return json_loads(json_data)

            # Memory cache miss - check SQLite with lock
            with self._sqlite_lock:
                result = self.conn.execute(
                    "SELECT data FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? AND driver = ? AND lap = ?",
                    cache_key,
                ).fetchone()
                if result:
                    json_data = result[0]

            # Update memory cache outside SQLite lock
            if json_data is not None:
                with self._memory_cache_lock:
                    self._remember_memory_entry(
                        self._memory_telemetry_cache,
                        cache_key,
                        json_data,
                        self._memory_telemetry_cache_max_items,
                    )

            if json_data is not None:
                logger.debug("Telemetry cache hit: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
                return json_loads(json_data)

            return None
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Telemetry cache read error: %s", e)
            return None

    async def get_telemetry_async(
        self, year: int, gp: str, session: str, driver: str, lap: int
    ) -> Any | None:
        """Get cached telemetry data asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get_telemetry, year, gp, session, driver, lap)

    def get_telemetry_batch(
        self, year: int, gp: str, session: str, driver_laps: list[tuple[str, int]]
    ) -> dict[tuple[str, int], Any]:
        """Get multiple cached telemetry entries in a single batch (thread-safe)."""
        if self.conn is None or not driver_laps:
            return {}

        results = {}
        misses = []

        # 1. Try memory cache first (lock-free)
        for driver, lap in driver_laps:
            key = (year, gp, session, driver, lap)
            json_data = self._memory_telemetry_cache.get(key)
            if json_data:
                results[(driver, lap)] = json_loads(json_data)
            else:
                misses.append((driver, lap))

        if not misses:
            return results

        # 2. Check SQLite for misses using IN clause for batch lookup
        try:
            with self._sqlite_lock:
                placeholders = ", ".join(["(?, ?)" for _ in misses])
                params = [year, gp, session]
                for driver_code, lap_num in misses:
                    params.extend([driver_code, lap_num])
                query = f"SELECT driver, lap, data FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? AND (driver, lap) IN ({placeholders})"
                rows = self.conn.execute(query, params).fetchall()

            if rows:
                with self._memory_cache_lock:
                    for driver_code, lap_num, json_data in rows:
                        self._remember_memory_entry(
                            self._memory_telemetry_cache,
                            (year, gp, session, driver_code, lap_num),
                            json_data,
                            self._memory_telemetry_cache_max_items,
                        )
                        results[(driver_code, lap_num)] = json_loads(json_data)
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Telemetry batch cache read error: %s", e)

        return results

    async def get_telemetry_batch_async(
        self, year: int, gp: str, session: str, driver_laps: list[tuple[str, int]]
    ) -> dict[tuple[str, int], Any]:
        """Get multiple cached telemetry entries asynchronously in a single batch."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self.get_telemetry_batch, year, gp, session, driver_laps
        )

    def set_telemetry(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: Any
    ) -> None:
        """Set cached telemetry data (thread-safe).

        Optimized to update memory cache first (fast, <1ms) before SQLite (slower).
        This ensures minimal lock duration for memory operations.
        """
        if self.conn is None or self.read_only:
            return
        try:
            json_data = json_dumps(data)
            cache_key = (year, gp, session, driver, lap)

            # Update memory cache first (fast operation, <1ms)
            with self._memory_cache_lock:
                self._remember_memory_entry(
                    self._memory_telemetry_cache,
                    cache_key,
                    json_data,
                    self._memory_telemetry_cache_max_items,
                )

            # Then update SQLite (slower operation)
            with self._sqlite_lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO telemetry_cache VALUES (?, ?, ?, ?, ?, ?)",
                    (*cache_key, json_data),
                )
                self._pending_writes += 1
                self._commit_if_needed()

            logger.debug("Telemetry cached: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Telemetry cache write skipped: %s", e)

    async def set_telemetry_async(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: Any
    ) -> None:
        """Set cached telemetry data asynchronously."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.set_telemetry, year, gp, session, driver, lap, data)

    def has_session_data(self, year: int, gp: str, session: str) -> bool:
        """Return True when JSON or telemetry cache contains entries for a session."""
        if self.conn is None:
            return False

        prefix = f"{year}/{gp}/{session}/"
        try:
            # Check memory cache first (lock-free read)
            if any(str(key).startswith(prefix) for key in self._memory_cache):
                return True

            # Check SQLite with appropriate lock
            with self._sqlite_lock:
                cache_hit = self.conn.execute(
                    "SELECT 1 FROM cache WHERE key LIKE ? LIMIT 1",
                    (f"{prefix}%",),
                ).fetchone()
                if cache_hit is not None:
                    return True

                telemetry_hit = self.conn.execute(
                    "SELECT 1 FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? LIMIT 1",
                    (year, gp, session),
                ).fetchone()
                return telemetry_hit is not None
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug(
                "Session cache availability probe failed for %s/%s/%s: %s",
                year,
                gp,
                session,
                e,
            )
            # Default to True on probe failures to preserve cache-read behavior.
            return True

    def clear(self) -> None:
        """Clear all cached data."""
        if self.conn is None or self.read_only:
            logger.warning("Cannot clear cache (no connection or read-only mode)")
            return

        # Acquire both locks to ensure consistency
        with self._sqlite_lock:
            self.conn.execute("DELETE FROM cache")
            self.conn.execute("DELETE FROM telemetry_cache")
            self.conn.commit()
            self._pending_writes = 0

        with self._memory_cache_lock:
            self._memory_cache.clear()
            self._memory_telemetry_cache.clear()

        logger.info("Cache cleared")

    def close(self) -> None:
        """Close database connection."""
        # Acquire both locks to ensure clean shutdown.
        # Snapshot `self.conn` inside the sqlite lock so concurrent close() calls
        # cannot race into `None.close()`.
        with self._sqlite_lock:
            conn = self.conn
            if conn is not None:
                try:
                    self._commit_if_needed(force=True)
                    conn.close()
                except (OSError, RuntimeError, sqlite3.Error) as e:
                    logger.warning("Error closing cache connection: %s", e)
                finally:
                    self.conn = None

        with self._memory_cache_lock:
            self._memory_cache.clear()
            self._memory_telemetry_cache.clear()

        if conn is not None:
            logger.debug("Cache connection closed")

    def __del__(self):
        """Destructor to ensure connection is closed."""
        self.close()


_cache = None
_cache_lock = threading.Lock()


def get_cache() -> Cache:
    """Get global cache instance (lazy initialization, thread-safe)."""
    global _cache
    if _cache is not None:
        return _cache

    with _cache_lock:
        if _cache is None:
            _cache = Cache()
        return _cache


def _cleanup_cache() -> None:
    """Cleanup global cache on exit."""
    global _cache
    if _cache is not None:
        _cache.close()
        _cache = None


atexit.register(_cleanup_cache)
