"""Configuration management for tif1."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _to_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def _to_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


class Config:
    """Configuration manager with file support."""

    _instance: Optional["Config"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized"):
            return
        self._initialized = True

        # Default configuration
        self._config = {
            "cache_dir": str(Path.home() / ".tif1" / "cache"),
            "log_level": "WARNING",
            "timeout": 30,
            "max_retries": 3,
            "retry_backoff_factor": 2.0,
            "retry_jitter": True,
            "retry_jitter_max": 0.0,
            "max_retry_delay": 60.0,
            "circuit_breaker_threshold": 5,
            "circuit_breaker_timeout": 60,
            "http2_max_connections": 10,
            "http2_max_pool_size": 20,
            "max_workers": 20,
            "max_concurrent_requests": 20,
            "enable_cache": True,
            "offline_mode": False,
            "ci_mode": False,
            "lib": "pandas",
            "polars_lap_categorical": False,
            # Cache configuration constants
            "cache_commit_interval": 25,
            "sqlite_timeout": 30.0,
            "memory_cache_max_items": 1024,
            "memory_telemetry_cache_max_items": 2048,
            # HTTP session configuration constants
            "keepalive_timeout": 120,
            "keepalive_max_requests": 1000,
            "connection_stats_log_interval": 60.0,
            # Pool exhaustion backoff configuration
            "pool_exhaustion_backoff_base": 0.01,
            "pool_exhaustion_backoff_max": 0.5,
            "pool_exhaustion_backoff_jitter": 0.01,
            "http_multiplexed": True,
            "http_disable_http3": False,
            "user_agent": "tif1/0.1.0 (https://github.com/TracingInsights/tif1)",
            "cdns": [
                "https://cdn.jsdelivr.net/gh/TracingInsights",
            ],
            "cdn_use_minification": False,
            "validate_data": False,
            "validate_lap_times": False,
            "validate_telemetry": False,
            "ultra_cold_start": True,
            "ultra_cold_background_cache_fill": False,
            "ultra_cold_skip_retries": True,
            "prefetch_driver_laps_on_get_driver": True,
            "prefetch_all_telemetry_on_first_lap_request": False,
            "prefetch_all_telemetry_after_laps_load": False,
            "telemetry_prefetch_max_concurrent_requests": 32,
            # Process-pool JSON parsing hurts telemetry-heavy cold starts due to IPC overhead.
            # Keep disabled by default; advanced users can still enable via config/env.
            "json_parse_workers": 0,
            "http_resolvers": ["standard", "doh://cloudflare", "doh://google"],
        }

        # Load from config file
        self._load_config()

        # Override with environment variables
        self._load_env()

    def _load_config(self):
        """Load configuration from .tif1rc file."""
        explicit_config = os.getenv("TIF1_CONFIG_FILE")
        try:
            trust_cwd_config = _to_bool(os.getenv("TIF1_TRUST_CWD_CONFIG", "false"))
        except ValueError:
            logger.warning("Invalid TIF1_TRUST_CWD_CONFIG value, defaulting to false")
            trust_cwd_config = False

        config_paths: list[Path] = []
        if explicit_config:
            config_paths.append(Path(explicit_config).expanduser())
        if trust_cwd_config:
            config_paths.append(Path.cwd() / ".tif1rc")
        # Use HOME environment variable if set, otherwise use Path.home()
        home_path = Path(os.environ.get("HOME", Path.home()))
        config_paths.append(home_path / ".tif1rc")

        for path in config_paths:
            if path.exists():
                try:
                    with open(path, encoding="utf-8") as f:
                        user_config = json.load(f)
                    if not isinstance(user_config, dict):
                        logger.warning(f"Config file {path} must contain a JSON object")
                        continue
                    self._config.update(user_config)
                    logger.info(f"Loaded config from {path}")
                    break
                except Exception as e:
                    logger.warning(f"Failed to load config from {path}: {e}")

    def _load_env(self):
        """Load configuration from environment variables."""
        env_mapping = {
            "TIF1_CACHE_DIR": "cache_dir",
            "TIF1_LOG_LEVEL": "log_level",
            "TIF1_TIMEOUT": ("timeout", int),
            "TIF1_MAX_RETRIES": ("max_retries", int),
            "TIF1_RETRY_BACKOFF_FACTOR": ("retry_backoff_factor", float),
            "TIF1_RETRY_JITTER": ("retry_jitter", _to_bool),
            "TIF1_RETRY_JITTER_MAX": ("retry_jitter_max", float),
            "TIF1_MAX_RETRY_DELAY": ("max_retry_delay", float),
            "TIF1_CIRCUIT_BREAKER_THRESHOLD": ("circuit_breaker_threshold", int),
            "TIF1_CIRCUIT_BREAKER_TIMEOUT": ("circuit_breaker_timeout", int),
            "TIF1_HTTP2_MAX_CONNECTIONS": ("http2_max_connections", int),
            "TIF1_HTTP2_MAX_POOL_SIZE": ("http2_max_pool_size", int),
            "TIF1_POOL_CONNECTIONS": ("pool_connections", int),
            "TIF1_POOL_MAXSIZE": ("pool_maxsize", int),
            "TIF1_MAX_WORKERS": ("max_workers", int),
            "TIF1_MAX_CONCURRENT_REQUESTS": ("max_concurrent_requests", int),
            "TIF1_ENABLE_CACHE": ("enable_cache", _to_bool),
            "TIF1_OFFLINE_MODE": ("offline_mode", _to_bool),
            "TIF1_CI_MODE": ("ci_mode", _to_bool),
            "TIF1_LIB": "lib",
            "TIF1_POLARS_LAP_CATEGORICAL": ("polars_lap_categorical", _to_bool),
            "TIF1_CACHE_COMMIT_INTERVAL": ("cache_commit_interval", int),
            "TIF1_SQLITE_TIMEOUT": ("sqlite_timeout", float),
            "TIF1_MEMORY_CACHE_MAX_ITEMS": ("memory_cache_max_items", int),
            "TIF1_MEMORY_TELEMETRY_CACHE_MAX_ITEMS": ("memory_telemetry_cache_max_items", int),
            "TIF1_KEEPALIVE_TIMEOUT": ("keepalive_timeout", int),
            "TIF1_KEEPALIVE_MAX_REQUESTS": ("keepalive_max_requests", int),
            "TIF1_CONNECTION_STATS_LOG_INTERVAL": ("connection_stats_log_interval", float),
            "TIF1_POOL_EXHAUSTION_BACKOFF_BASE": ("pool_exhaustion_backoff_base", float),
            "TIF1_POOL_EXHAUSTION_BACKOFF_MAX": ("pool_exhaustion_backoff_max", float),
            "TIF1_POOL_EXHAUSTION_BACKOFF_JITTER": ("pool_exhaustion_backoff_jitter", float),
            "TIF1_HTTP_MULTIPLEXED": ("http_multiplexed", _to_bool),
            "TIF1_HTTP_DISABLE_HTTP3": ("http_disable_http3", _to_bool),
            "TIF1_CDNS": ("cdns", _to_list),
            "TIF1_VALIDATE_DATA": ("validate_data", _to_bool),
            "TIF1_VALIDATE_LAP_TIMES": ("validate_lap_times", _to_bool),
            "TIF1_VALIDATE_TELEMETRY": ("validate_telemetry", _to_bool),
            "TIF1_ULTRA_COLD_START": ("ultra_cold_start", _to_bool),
            "TIF1_ULTRA_COLD_BACKGROUND_CACHE_FILL": (
                "ultra_cold_background_cache_fill",
                _to_bool,
            ),
            "TIF1_ULTRA_COLD_SKIP_RETRIES": ("ultra_cold_skip_retries", _to_bool),
            "TIF1_PREFETCH_DRIVER_LAPS_ON_GET_DRIVER": (
                "prefetch_driver_laps_on_get_driver",
                _to_bool,
            ),
            "TIF1_PREFETCH_ALL_TELEMETRY_ON_FIRST_LAP_REQUEST": (
                "prefetch_all_telemetry_on_first_lap_request",
                _to_bool,
            ),
            "TIF1_PREFETCH_ALL_TELEMETRY_AFTER_LAPS_LOAD": (
                "prefetch_all_telemetry_after_laps_load",
                _to_bool,
            ),
            "TIF1_TELEMETRY_PREFETCH_MAX_CONCURRENT_REQUESTS": (
                "telemetry_prefetch_max_concurrent_requests",
                int,
            ),
            "TIF1_JSON_PARSE_WORKERS": ("json_parse_workers", int),
            "TIF1_HTTP_RESOLVERS": ("http_resolvers", _to_list),
        }

        for env_var, config_key in env_mapping.items():
            value = os.getenv(env_var)
            if value is not None:
                if isinstance(config_key, tuple):
                    key, converter = config_key
                    try:
                        self._config[key] = converter(value)
                    except Exception as e:
                        logger.warning(f"Failed to convert {env_var}={value}: {e}")
                else:
                    self._config[config_key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with validation."""
        value = self._config.get(key, default)

        # Validate numeric values (must be positive)
        if key in (
            "timeout",
            "max_workers",
            "pool_connections",
            "pool_maxsize",
            "max_concurrent_requests",
            "max_retry_delay",
            "circuit_breaker_threshold",
            "circuit_breaker_timeout",
            "http2_max_connections",
            "http2_max_pool_size",
            "cache_commit_interval",
            "memory_cache_max_items",
            "memory_telemetry_cache_max_items",
            "keepalive_timeout",
            "keepalive_max_requests",
            "telemetry_prefetch_max_concurrent_requests",
        ):
            if value is not None and (not isinstance(value, int | float) or value <= 0):
                logger.warning(f"Invalid {key}={value}, using default={default}")
                return default

        # Validate max_retries (can be 0 or positive)
        if key == "max_retries":
            if value is not None and (not isinstance(value, int | float) or value < 0):
                logger.warning(f"Invalid {key}={value}, using default={default}")
                return default

        if key == "json_parse_workers":
            if value is not None and (not isinstance(value, int) or value < 0):
                logger.warning(f"Invalid {key}={value}, using default={default}")
                return default

        # Validate float values (can be positive floats)
        if key in (
            "sqlite_timeout",
            "retry_jitter_max",
            "connection_stats_log_interval",
            "pool_exhaustion_backoff_base",
            "pool_exhaustion_backoff_max",
            "pool_exhaustion_backoff_jitter",
        ):
            if value is not None and (not isinstance(value, int | float) or value <= 0):
                logger.warning(f"Invalid {key}={value}, using default={default}")
                return default

        # Validate backoff factor
        if key == "retry_backoff_factor":
            if value is not None and (not isinstance(value, int | float) or value < 1.0):
                logger.warning(f"Invalid {key}={value}, using default={default}")
                return default

        if key == "lib":
            if value not in {"pandas", "polars"}:
                logger.warning(f"Invalid lib={value}, using default={default}")
                return default

        if key == "cache_dir" and isinstance(value, str):
            return str(Path(value).expanduser())

        if key == "cdns":
            if not isinstance(value, list):
                logger.warning(f"Invalid cdns={value}, using default={default}")
                return default
            valid_cdns = [
                cdn for cdn in value if isinstance(cdn, str) and cdn.startswith("https://")
            ]
            if not valid_cdns and default is not None:
                logger.warning(f"No valid HTTPS cdns found in {value}, using default={default}")
                return default
            return valid_cdns

        return value

    def set(self, key: str, value: Any):
        """Set configuration value."""
        self._config[key] = value

    def save(self, path: Path | None = None):
        """Save configuration to file."""
        if path is None:
            path = Path.home() / ".tif1rc"

        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self._config, f, indent=2)
            logger.info(f"Saved config to {path}")
        except Exception as e:
            logger.error(f"Failed to save config to {path}: {e}")


def get_config() -> Config:
    """Get global configuration instance."""
    return Config()
