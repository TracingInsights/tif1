"""Shared HTTP session management for tif1."""

import atexit
import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

# Deferred imports
_niquests = None

# Shared session state
_shared_session: Any | None = None
_session_lock = threading.Lock()

#: Resolver specifiers in niquests' ``<scheme>+<provider>://`` form: standard
#: DNS first, then DNS-over-HTTPS fallbacks. (``doh://cloudflare`` is parsed as
#: a DoH server literally named "cloudflare" and never resolves anything.)
DEFAULT_RESOLVERS: tuple[str, ...] = ("standard", "doh+cloudflare://", "doh+google://")

# Pre-0.8 config files spelled the DoH fallbacks as ``doh://<provider>``.
_LEGACY_RESOLVER_SPELLINGS = {
    "doh://cloudflare": "doh+cloudflare://",
    "doh://google": "doh+google://",
    "doh://quad9": "doh+quad9://",
    "doh://adguard": "doh+adguard://",
    "doh://nextdns": "doh+nextdns://",
}


def _normalize_resolver(resolver: str) -> str:
    """Map the legacy ``doh://<provider>`` spelling to niquests' ``doh+<provider>://``."""
    return _LEGACY_RESOLVER_SPELLINGS.get(resolver.lower(), resolver)


_user_agent_value: str | None = None


def user_agent() -> str:
    """``tif1/{version} (+homepage)``: the User-Agent of every tif1 request.

    The Jolpica API refuses requests without a descriptive User-Agent; the
    CDNs ignore it. The version lookup (importlib.metadata) runs once.
    """
    global _user_agent_value
    if _user_agent_value is None:
        try:
            from importlib.metadata import version

            tif1_version = version("tif1")
        except Exception:
            tif1_version = "0.0.0"
        _user_agent_value = f"tif1/{tif1_version} (+https://github.com/TracingInsights/tif1)"
    return _user_agent_value


def _response_reused_connection(response: Any) -> bool:
    """Whether a niquests response rode an already-open pooled connection.

    niquests records the handshake timings on ``response.conn_info``; a reused
    connection performs no new TCP/TLS handshake, so ``established_latency``
    and ``tls_handshake_latency`` are zero (or absent). Unknown shapes (mocks,
    other clients) count as not reused.
    """
    info = getattr(response, "conn_info", None)
    if info is None:
        return False
    established = getattr(info, "established_latency", None)
    handshake = getattr(info, "tls_handshake_latency", None)
    return not established and not handshake


# Connection reuse metrics
_connection_metrics = {
    "total_requests": 0,
    "connections_created": 0,
    "connections_reused": 0,
    "last_log_time": 0.0,
}
_metrics_lock = threading.Lock()


def _import_niquests():
    """Lazy import niquests."""
    global _niquests
    if _niquests is None:
        import niquests

        _niquests = niquests
    return _niquests


def _create_session() -> Any:
    """Create HTTP session with optimized settings and DoH fallback.

    Returns:
        niquests.Session configured with connection pooling and DoH resolver

    Raises:
        NetworkError: If session creation fails with all resolvers
    """
    from .config import get_config
    from .exceptions import NetworkError

    niquests = _import_niquests()
    config = get_config()
    multiplexed = bool(config.get("http_multiplexed", False))
    disable_http3 = bool(config.get("http_disable_http3", False))

    # Dynamic pool sizing based on concurrency.
    target_concurrency = max(
        config.get("max_workers", 20),
        config.get("max_concurrent_requests", 20),
        config.get("telemetry_prefetch_max_concurrent_requests", 128),
    )

    # Dynamic pool sizing based on concurrency
    # If explicit pool_connections is set, use it; otherwise calculate from max_workers
    if config.get("pool_connections") is not None:
        pool_connections = max(10, config.get("pool_connections"))
    else:
        # Calculate based on effective target concurrency with minimum of 256.
        pool_connections = max(256, target_concurrency)

    # If explicit pool_maxsize is set, use it; otherwise set to 4x connections for bursts
    # Ensure minimum of 512 for optimal concurrent requests
    if config.get("pool_maxsize") is not None:
        pool_maxsize = max(512, pool_connections, config.get("pool_maxsize"))
    else:
        # Set to 4x connections for burst handling, minimum 512
        pool_maxsize = max(512, pool_connections * 4)

    configured_resolvers = config.get("http_resolvers", list(DEFAULT_RESOLVERS))
    resolvers: list[str | None] = []

    if isinstance(configured_resolvers, list):
        for resolver in configured_resolvers:
            if not isinstance(resolver, str):
                continue
            normalized = resolver.strip().lower()
            if normalized in {"standard", "default", "none", ""}:
                resolvers.append(None)
            else:
                resolvers.append(_normalize_resolver(resolver.strip()))

    if not resolvers:
        resolvers = [None, *DEFAULT_RESOLVERS[1:]]

    for resolver in resolvers:
        session = None
        try:
            if resolver:
                session = niquests.Session(
                    multiplexed=multiplexed,
                    resolver=resolver,
                    disable_http3=disable_http3,
                )
                logger.debug(f"HTTP session initialized with {resolver}")
            else:
                session = niquests.Session(
                    multiplexed=multiplexed,
                    disable_http3=disable_http3,
                )
                logger.debug("HTTP session initialized with standard DNS")
            # Skip env/netrc resolution on every request; this is costly at telemetry scale.
            session.trust_env = False
            # ... but resolve the proxies once: the environment variables on
            # every platform and the registry setting on Windows, which
            # trust_env=False would otherwise ignore.
            from urllib.request import getproxies

            proxies = getproxies()
            if proxies:
                session.proxies = proxies

            # The adapter owns name resolution in niquests, so the resolver
            # must be handed to the replacement adapter or it is silently lost.
            # Argos A7: the replacement adapter owns protocol negotiation
            # too, so the HTTP/3 switch must be handed to it as well or the
            # ``http_disable_http3`` knob is silently lost (E7 could not
            # measure an h2-only variant for exactly this reason).
            session.mount(
                "https://",
                niquests.adapters.HTTPAdapter(
                    pool_connections=pool_connections,
                    pool_maxsize=pool_maxsize,
                    pool_block=False,
                    resolver=resolver,
                    disable_http3=disable_http3,
                ),
            )
            # Enable connection keep-alive with timeout and max requests
            keepalive_timeout = config.get("keepalive_timeout", 120)
            keepalive_max_requests = config.get("keepalive_max_requests", 1000)
            session.headers.update(
                {
                    "User-Agent": user_agent(),
                    "Connection": "keep-alive",
                    "Keep-Alive": f"timeout={keepalive_timeout}, max={keepalive_max_requests}",
                }
            )
            return session
        except Exception as e:
            # Release connection resources on failure
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass
            logger.warning(f"Failed to initialize with {resolver or 'standard DNS'}: {e}")

    raise NetworkError(url="session_init", status_code=None)


def get_session() -> Any:
    """Get or create shared HTTP session (thread-safe).

    Returns:
        Shared niquests.Session instance
    """
    global _shared_session
    if _shared_session is not None:
        return _shared_session

    with _session_lock:
        if _shared_session is not None:
            return _shared_session
        _shared_session = _create_session()
        _track_connection_created()
        return _shared_session


def _track_connection_created() -> None:
    """Track that a new connection pool was created."""
    with _metrics_lock:
        _connection_metrics["connections_created"] += 1


def _track_request(reused: bool = False) -> None:
    """Track a request and whether it reused a connection.

    Args:
        reused: Whether the request reused an existing connection
    """
    with _metrics_lock:
        _connection_metrics["total_requests"] += 1
        if reused:
            _connection_metrics["connections_reused"] += 1

        # Log statistics periodically
        from .config import get_config

        config = get_config()
        log_interval = config.get("connection_stats_log_interval", 60.0)
        current_time = time.monotonic()
        if current_time - _connection_metrics["last_log_time"] >= log_interval:
            _log_connection_stats()
            _connection_metrics["last_log_time"] = current_time


def _log_connection_stats() -> None:
    """Log connection pool statistics (must be called with _metrics_lock held)."""
    total = _connection_metrics["total_requests"]
    reused = _connection_metrics["connections_reused"]
    created = _connection_metrics["connections_created"]

    if total > 0:
        reuse_rate = (reused / total) * 100
        logger.info(
            f"Connection pool stats: {total} requests, "
            f"{reused} reused ({reuse_rate:.1f}%), "
            f"{created} pools created"
        )
    else:
        logger.debug("Connection pool stats: No requests yet")


def get_connection_stats() -> dict[str, Any]:
    """Get current connection pool statistics.

    Returns:
        Dictionary with connection metrics:
        - total_requests: Total number of requests made
        - connections_reused: Number of requests that reused connections
        - connections_created: Number of connection pools created
        - reuse_rate: Percentage of requests that reused connections (0-100)
    """
    with _metrics_lock:
        total = _connection_metrics["total_requests"]
        reused = _connection_metrics["connections_reused"]
        created = _connection_metrics["connections_created"]
        reuse_rate = (reused / total * 100) if total > 0 else 0.0

        return {
            "total_requests": total,
            "connections_reused": reused,
            "connections_created": created,
            "reuse_rate": reuse_rate,
        }


def reset_connection_stats() -> None:
    """Reset connection statistics (useful for testing)."""
    with _metrics_lock:
        _connection_metrics["total_requests"] = 0
        _connection_metrics["connections_reused"] = 0
        _connection_metrics["connections_created"] = 0
        _connection_metrics["last_log_time"] = 0.0


def close_session() -> None:
    """Close shared HTTP session and cleanup resources."""
    global _shared_session
    with _session_lock:
        if _shared_session is not None:
            try:
                _shared_session.close()
            except Exception as e:
                logger.warning(f"Error closing HTTP session: {e}")
            finally:
                _shared_session = None
            logger.debug("Shared HTTP session closed")


# Register cleanup on module unload
atexit.register(close_session)
