"""FastF1 compatibility shims for top-level APIs."""

from __future__ import annotations

import logging
import os
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any

from .config import get_config
from .exceptions import NetworkError

logger = logging.getLogger(__name__)


class _NoCacheContext(AbstractContextManager[None]):
    """Context manager that temporarily disables caching."""

    def __enter__(self) -> None:
        Cache.set_disabled()

    def __exit__(self, *exc_info: object) -> None:
        Cache.set_enabled()


class Cache:
    """FastF1-compatible cache entrypoint."""

    _cache_dir: Path | None = None
    _disable_depth: int = 0
    _enable_cache_before_disable: bool | None = None
    _offline_mode: bool = False
    _ci_mode: bool = False

    @classmethod
    def _resolve_cache_dir(cls, cache_dir: str | Path | None) -> Path:
        if cache_dir is not None:
            return Path(cache_dir).expanduser()
        if cls._cache_dir is not None:
            return cls._cache_dir
        configured = get_config().get("cache_dir", None)
        if configured is not None:
            return Path(str(configured)).expanduser()
        from .cache import _default_cache_dir

        return _default_cache_dir()

    @classmethod
    def enable_cache(
        cls,
        cache_dir: str | Path | None = None,
        ignore_version: bool = True,
        force_renew: bool = False,
        use_requests_cache: bool = True,
    ) -> Any:
        """Enable tif1 caching with a FastF1-compatible API.

        Args:
            cache_dir: Cache directory path. If None, uses configured cache directory.
            ignore_version: Accepted for API compatibility.
            force_renew: If True, clears existing cache data after enabling.
            use_requests_cache: Accepted for API compatibility.

        Returns:
            The configured global tif1 cache instance.
        """
        _ = (ignore_version, force_renew, use_requests_cache)
        resolved_cache_dir = cls._resolve_cache_dir(cache_dir)
        resolved_cache_dir.mkdir(parents=True, exist_ok=True)

        from . import cache as cache_module

        cls._cache_dir = resolved_cache_dir
        cls._disable_depth = 0
        cls._enable_cache_before_disable = None

        config = get_config()
        config.set("cache_dir", str(resolved_cache_dir))
        config.set("enable_cache", True)

        if cache_module._cache is not None:
            cache_module._cache.close()
            cache_module._cache = None
        cache_instance = cache_module.get_cache()
        if force_renew:
            cache_instance.clear()
        return cache_instance

    @classmethod
    def clear_cache(cls, cache_dir: str | Path | None = None, deep: bool = False) -> None:
        """Clear cached data.

        Args:
            cache_dir: Optional cache directory. Uses active cache directory when omitted.
            deep: If True, also removes legacy FastF1 HTTP cache database files.
        """
        from . import cache as cache_module
        from .cache import Cache as TIF1Cache

        # If no cache_dir specified and there's an active cache, use it directly
        if cache_dir is None:
            active_cache = cache_module._cache
            if active_cache is None:
                active_cache = cache_module.get_cache()
            active_cache.clear()
            return

        resolved_cache_dir = cls._resolve_cache_dir(cache_dir)
        if not resolved_cache_dir.is_dir():
            raise NotADirectoryError(str(resolved_cache_dir))

        active_cache = cache_module._cache
        if (
            active_cache is not None
            and Path(active_cache.cache_dir).resolve() == resolved_cache_dir.resolve()
        ):
            active_cache.clear()
        else:
            local_cache = TIF1Cache(cache_dir=resolved_cache_dir)
            try:
                local_cache.clear()
            finally:
                local_cache.close()

        if not deep:
            return

        legacy_files = (
            "fastf1_http_cache.sqlite",
            "fastf1_http_cache.sqlite-shm",
            "fastf1_http_cache.sqlite-wal",
        )
        for file_name in legacy_files:
            file_path = resolved_cache_dir / file_name
            if file_path.is_file():
                try:
                    file_path.unlink()
                except OSError as exc:
                    logger.warning("Failed to remove legacy cache file %s: %s", file_path, exc)

    @classmethod
    def disabled(cls) -> _NoCacheContext:
        """Temporarily disable caching in a context manager."""
        return _NoCacheContext()

    @classmethod
    def set_disabled(cls) -> None:
        """Disable caching globally until `set_enabled` is called."""
        config = get_config()
        if cls._disable_depth == 0:
            cls._enable_cache_before_disable = bool(config.get("enable_cache", True))
            config.set("enable_cache", False)
        cls._disable_depth += 1

    @classmethod
    def set_enabled(cls) -> None:
        """Re-enable caching after it was disabled with `set_disabled`."""
        if cls._disable_depth == 0:
            return
        cls._disable_depth -= 1
        if cls._disable_depth > 0:
            return

        config = get_config()
        restore_value = (
            True if cls._enable_cache_before_disable is None else cls._enable_cache_before_disable
        )
        config.set("enable_cache", restore_value)
        cls._enable_cache_before_disable = None

    @classmethod
    def offline_mode(cls, enabled: bool) -> None:
        """Enable or disable offline mode.

        In offline mode, tif1 uses only persisted cache data and performs no network requests.
        """
        cls._offline_mode = bool(enabled)
        get_config().set("offline_mode", cls._offline_mode)

    @classmethod
    def ci_mode(cls, enabled: bool) -> None:
        """Enable or disable CI mode.

        CI mode disables tif1 parsed-data caching to reduce filesystem writes.
        """
        cls._ci_mode = bool(enabled)
        get_config().set("ci_mode", cls._ci_mode)

    @classmethod
    def get_cache_info(cls) -> tuple[str | None, int | None]:
        """Return cache location and total on-disk size in bytes."""
        cache_dir = cls._cache_dir
        if cache_dir is None:
            return None, None

        if not cache_dir.exists():
            return str(cache_dir), 0

        size = 0
        for path, _dirs, files in os.walk(cache_dir):
            for name in files:
                file_path = Path(path) / name
                size += file_path.stat().st_size
        return str(cache_dir), size

    @classmethod
    def requests_get(cls, url: str, **kwargs: Any) -> Any:
        """Issue an HTTP GET request through tif1's shared session."""
        if cls._offline_mode:
            raise NetworkError(url=url, status_code=None)

        from .http_session import get_session as get_http_session

        timeout = kwargs.pop("timeout", get_config().get("timeout", 8))
        return get_http_session().get(url, timeout=timeout, **kwargs)

    @classmethod
    def requests_post(cls, url: str, data: Any = None, **kwargs: Any) -> Any:
        """Issue an HTTP POST request through tif1's shared session."""
        if cls._offline_mode:
            raise NetworkError(url=url, status_code=None)

        from .http_session import get_session as get_http_session

        timeout = kwargs.pop("timeout", get_config().get("timeout", 8))
        return get_http_session().post(url, data=data, timeout=timeout, **kwargs)

    @classmethod
    def delete_response(cls, url: str) -> None:
        """Invalidate a URL response from stage-1 cache.

        tif1 does not currently implement FastF1's stage-1 HTTP response cache, so this
        operation is a no-op.
        """
        logger.debug(
            "delete_response(%s) is not implemented because tif1 has no stage-1 HTTP cache",
            url,
        )

    @staticmethod
    def _reset_tif1_cache_instance() -> None:
        """Reset the global tif1 cache singleton.

        This utility is intended for tests.
        """
        from . import cache as cache_module

        if cache_module._cache is not None:
            cache_module._cache.close()
            cache_module._cache = None


def set_log_level(level: int = logging.WARNING) -> None:
    """FastF1-compatible logger configuration."""
    logging.getLogger("tif1").setLevel(level)
