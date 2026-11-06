"""The first wave of a cold load over the standard library (Elis E2).

``import niquests`` costs ~120-145 ms of CPU in every cold process, and its
first connection runs the OCSP revocation check before a request may go out
(~350 ms from ``get_session`` to the warm connection measured on a 16 ms RTT
link). The first wave of a cold load is two small files (``drivers.json`` and
``session_laptimes.json``) from the primary CDN, so ``get_session`` starts
them here instead: ``http.client`` over ``ssl`` with the trust store niquests
uses (``wassima``), the same User-Agent and gzip, in daemon threads that wait
on the network while pandas or polars imports on the main thread.

The fetch pipeline consumes a body through :func:`take` before it touches
niquests; only a ``200`` with a decodable body is handed over. Anything else
(another status, a redirect, a timeout, a TLS or DNS failure, a configured
proxy, no ``wassima``) hands back ``None`` and the request goes through
niquests exactly as before, so the CDN fallback, the 403/404 semantics, the
retries and the hedging stay in :mod:`tif1.cdn` / :mod:`tif1.async_fetch`. A
body is handed over once; later requests of the same URL go to niquests.

The wave's connections stay open: until a niquests session exists, the sync
transport sends single requests over them (:func:`fetch`; the fastest lap's
telemetry file, the Ergast classification), so a cold script that never
starts a telemetry wave never imports niquests. The first telemetry wave
starts on them too (Elis E9, :func:`bridge_open`): its requests go over
stdlib connections, and once the bridge has served :data:`BRIDGE_WARM_AFTER`
requests niquests imports and connects in a daemon thread (Naxos N8: a
one-driver wave ends on the bridge before niquests could take it over, and
the import's CPU slowed the wave's receive threads); once niquests is warm
the rest of the wave and every later one run on it (HTTP/2, hedging, the
OCSP check). When the wave cannot run (a proxy, no ``wassima``), its thread
warms niquests connections instead.

A process that never ran a wave builds the same client on demand
(:func:`ensure_client`, Corinth C4) for its single requests before niquests
exists: the f1schedule calendar of the current season (fetched on a cold
``get_session``, refreshed in the background once ``schedule_ttl_hours``
passed) and the Ergast Stats files of a warm session past
``ergast_ttl_hours``. Through niquests those paid its import and, for
raw.githubusercontent.com, a ~250 KB certificate revocation list downloaded
and parsed in every process (~0.2 s), on the main thread when cold and
under the GIL beside the pandas import when refreshing.

A cold ``tif1.ergast`` query (no ``get_session``, so no wave) fetches its
Stats files over :func:`standalone_client` (Corinth C3). Both on-demand uses
share one process-wide stdlib client (:func:`_build_client`), which a first
wave reuses; only a wave sets ``_client``, the wave's client, which opens the
telemetry bridge (:func:`bridge_open`): its niquests hand-over needs the
wave's URLs, so single requests never open it.

Security note: the stdlib requests verify the certificate chain and the host
name against the same roots as niquests, but do not run niquests' OCSP
revocation check (``http_revocation_check``), which is soft-fail there.
``cdn_first_wave=False`` sends every request through niquests.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

#: Seconds after the wave began that a request still waits for its body. A
#: healthy fetch of the first-wave files takes ~0.1-0.2 s; past this bound
#: the request goes to niquests instead (the stdlib result is dropped).
WAIT_LIMIT = 3.0

#: Requests the bridge serves before it starts the niquests warm-up (Naxos
#: N8). More than one driver's laps of any session (a race has at most ~80),
#: so a one-driver telemetry wave (~0.5 s cold) runs on the stdlib alone: the
#: warm-up (the niquests import, ~120-145 ms of CPU under the GIL, then the
#: session and its OCSP-checked connections) never finished before such a
#: wave did, and it slowed the wave's receive threads. A longer wave (a
#: second driver, ``fetch_all``) starts the warm-up here and moves to
#: niquests as before.
BRIDGE_WARM_AFTER = 80


class _Slot:
    """One in-flight first-wave fetch: set ``done`` once ``body`` and ``status`` are final."""

    __slots__ = ("body", "deadline", "done", "status")

    def __init__(self, deadline: float) -> None:
        self.done = threading.Event()
        self.body: bytes | None = None
        self.status: int | None = None
        self.deadline = deadline


_slots: dict[str, _Slot] = {}
_slots_lock = threading.Lock()

#: ``(host, port, timeout, context) -> connection`` (tests swap in a fake).
_connection_factory: Callable[..., Any] | None = None

#: The wave's URLs and timeout, which the niquests warm-up reuses (Elis E9;
#: a HEAD on them opens connections to the same CDN host).
_warm_urls: list[str] = []
_warm_timeout = 10.0
_warm_started = False
#: Requests the bridge has served (:func:`bridge_open` answered True).
_bridged = 0
#: :func:`ensure_client` may build the client (the unit suite turns it off:
#: its sockets are blocked, so the attempt would only fall back to niquests).
_on_demand = True
#: The client cannot be built in this process (a proxy, no ``wassima``).
_client_unavailable = False
_client_lock = threading.Lock()
#: Set once the wave's thread has built its client or given up on it (the
#: lookahead's fetches of :func:`prefetch` wait on it).
_client_settled = threading.Event()


def start(urls: list[str], *, timeout: float, wait_limit: float | None = None) -> None:
    """Fetch ``urls`` over the standard library in daemon threads.

    One thread builds the TLS context (the ``ssl``/``http.client`` imports and
    the trust store, ~40 ms of CPU) and fetches the first URL; one more
    thread per further URL. Never raises: a failure leaves the slot empty and
    the request goes to niquests.

    Args:
        urls: The wave's URLs.
        timeout: Socket timeout of each request.
        wait_limit: Seconds after the start that :func:`take` still waits for
            a body (default :data:`WAIT_LIMIT`). The Ergast-only wave (Corinth
            C2) waits up to the request timeout: its Stats files come from
            cold jsDelivr edges (up to ~2 s each) and nothing else would fetch
            them sooner.
    """
    global _warm_urls, _warm_timeout
    deadline = time.monotonic() + (WAIT_LIMIT if wait_limit is None else wait_limit)
    slots = [(url, _Slot(deadline)) for url in urls]
    if not slots:
        return
    _warm_urls, _warm_timeout = list(urls), timeout
    with _slots_lock:
        _slots.update(slots)
    threading.Thread(
        target=_run_wave, args=(slots, timeout), name="tif1-first-wave", daemon=True
    ).start()


def pending(url: str) -> bool:
    """Whether a first-wave body for ``url`` may still be handed over (no wait)."""
    return url in _slots


def take(url: str) -> bytes | None:
    """The first-wave body of ``url`` (waits for it, bounded), or None.

    None when ``url`` was not part of the wave, was handed over already, or
    did not arrive as a ``200`` with a decodable body by the wave's deadline.
    """
    return take_status(url)[1]


def take_status(url: str) -> tuple[int | None, bytes | None]:
    """:func:`take` with the HTTP status: ``(200, body)``, ``(404, None)``, ...

    ``(None, None)`` when ``url`` was not part of the wave, was handed over
    already, failed at the transport level or missed the wave's deadline. The
    session bundle (Corinth C10) reads its first-wave answer through it, so a
    session without a bundle learns it from the 404 without another request.
    """
    if not _slots:
        return None, None
    with _slots_lock:
        slot = _slots.pop(url, None)
    if slot is None:
        return None, None
    if not slot.done.wait(max(0.0, slot.deadline - time.monotonic())):
        logger.debug("First-wave fetch of %s missed its window", url)
        return None, None
    return slot.status, slot.body


def prefetch(urls: list[str], *, timeout: float) -> list[str]:
    """Fetch ``urls`` into slots over the wave's connections, one daemon thread each.

    The session lookahead of a season loop (Corinth C1,
    :mod:`tif1.session_lookahead`): the bodies are handed over through
    :func:`pending`/:func:`take` exactly like the first wave's, so a later
    load of those files takes them instead of requesting them. Only runs once
    a wave was started in this process (its client is reused; a request
    waits, bounded, while that client is still being built). URLs that
    already have a slot are skipped. Never raises.

    Returns:
        The URLs that got a slot (``[]`` when no wave ran).
    """
    if _client is None and not _warm_urls:
        return []
    deadline = time.monotonic() + WAIT_LIMIT
    with _slots_lock:
        fresh = [(url, _Slot(deadline)) for url in dict.fromkeys(urls) if url not in _slots]
        _slots.update(fresh)
    if not fresh:
        return []
    try:
        # One thread start on the caller's thread (each is a GIL handoff);
        # that thread starts the others.
        threading.Thread(
            target=_run_prefetch, args=(fresh, timeout), name="tif1-lookahead", daemon=True
        ).start()
    except RuntimeError:  # interpreter shutting down
        _release(fresh)
    return [url for url, _slot in fresh]


def discard(urls: list[str]) -> None:
    """Drop the slots of ``urls`` that were not taken (a lookahead the script left)."""
    with _slots_lock:
        for url in urls:
            _slots.pop(url, None)


def _run_prefetch(slots: list[tuple[str, _Slot]], timeout: float) -> None:
    if _client is None:
        _client_settled.wait(max(0.0, slots[0][1].deadline - time.monotonic()))
    client = _client
    if client is None:
        _release(slots)
        return
    for index, (url, slot) in enumerate(slots[1:], 1):
        try:
            threading.Thread(
                target=_fetch_into,
                args=(client, url, slot, timeout),
                name="tif1-lookahead",
                daemon=True,
            ).start()
        except RuntimeError:  # interpreter shutting down
            _release(slots[index:])
            break
    _fetch_into(client, *slots[0], timeout)


def warm_niquests() -> None:
    """Import niquests and warm its connections in a daemon thread, once (Elis E9).

    Called by the bridge (:func:`bridge_open`) once it has served
    :data:`BRIDGE_WARM_AFTER` requests: the import (~120 ms of CPU) and the
    handshakes then run while the main thread waits on the network, and the
    rest of a long wave (and every later one) moves to niquests once its
    connections are warm. Not started when the wave did not run (its thread
    then warmed niquests already).
    """
    global _warm_started
    if _warm_started or _client is None or not _warm_urls:
        return
    with _slots_lock:
        if _warm_started:
            return
        _warm_started = True
    threading.Thread(
        target=_run_niquests_warmup,
        args=(list(_warm_urls), _warm_timeout),
        name="tif1-niquests-warmup",
        daemon=True,
    ).start()


def bridge_open() -> bool:
    """Whether a telemetry-wave request should go over the wave's stdlib connections.

    True while the wave's client exists and niquests is not ready (no
    session yet, or its warm-up still connecting); each True counts as a
    bridged request, and the one past :data:`BRIDGE_WARM_AFTER` starts the
    niquests warm-up (:func:`warm_niquests`). Never waits.
    """
    global _bridged
    if _client is None:
        return False
    from . import http_session

    if http_session._shared_session is not None and not http_session.warm_connections_pending():
        return False
    with _slots_lock:
        _bridged += 1
        bridged = _bridged
    if bridged > BRIDGE_WARM_AFTER:
        warm_niquests()
    return True


def _run_niquests_warmup(urls: list[str], timeout: float) -> None:
    from . import http_session

    if http_session._shared_session is not None:
        return
    try:
        http_session.warm_connections(urls, timeout=timeout)
    except Exception as e:  # best effort, never surfaces
        logger.debug("niquests warm-up skipped: %s", e)


def reset() -> None:
    """Forget every slot and the wave's client (tests)."""
    global _bridged, _client, _client_unavailable, _shared, _warm_started, _warm_urls
    _client_unavailable = False
    _warm_started = False
    _warm_urls = []
    _bridged = 0
    _client_settled.clear()
    with _slots_lock:
        _slots.clear()
    client, _client = _client, None
    shared, _shared = _shared, None
    for idle in {id(c): c for c in (client, shared) if c is not None}.values():
        idle.close()


def _release(slots: list[tuple[str, _Slot]]) -> None:
    for _url, slot in slots:
        slot.done.set()


def _run_wave(slots: list[tuple[str, _Slot]], timeout: float) -> None:
    # A cold ``get_session`` that fetched the calendar built the client already.
    global _client
    client = _build_client()
    if client is None:
        _client_settled.set()
        _release(slots)
        # The wave cannot match niquests' setup here: warm niquests
        # connections for the same files instead (the Olympia pre-connect).
        from . import http_session

        try:
            http_session.warm_connections([url for url, _slot in slots], timeout=timeout)
        except Exception as e:  # best effort, never surfaces
            logger.debug("CDN pre-connect skipped: %s", e)
        return
    _client = client
    _client_settled.set()
    for url, slot in slots[1:]:
        threading.Thread(
            target=_fetch_into,
            args=(client, url, slot, timeout),
            name="tif1-first-wave",
            daemon=True,
        ).start()
    _fetch_into(client, *slots[0], timeout)


def _build_client() -> _StdlibClient | None:
    """The shared stdlib client, built once per process (None where it cannot match niquests).

    A first wave adopts it as ``_client``; single requests use it through
    :func:`ensure_client` / :func:`standalone_client` without opening the
    telemetry bridge.
    """
    global _client_unavailable, _shared
    client = _shared
    if client is not None or _client_unavailable:
        return client
    with _client_lock:
        if _shared is not None or _client_unavailable:
            return _shared
        try:
            context = _tls_context()
        except Exception as e:  # never surfaces: niquests takes the requests
            logger.debug("Stdlib client skipped: %s", e)
            context = None
        if context is None:
            _client_unavailable = True
            return None
        from .http_session import user_agent

        _shared = _StdlibClient(
            context,
            {"User-Agent": user_agent(), "Accept": "*/*", "Accept-Encoding": "gzip, deflate"},
        )
        return _shared


def ensure_client() -> bool:
    """Whether single requests can go over the stdlib client, building it if needed.

    For the sync transport when no niquests session exists (Corinth C4): a
    process that never ran a first wave (a warm session refreshing its
    calendar or Ergast files, a cold ``get_session`` fetching the calendar)
    builds the client here, on the calling thread (the ``ssl`` import and the
    trust store, a few tens of milliseconds of CPU, once), instead of
    importing niquests. False with ``cdn_first_wave`` off, a proxy or no
    ``wassima``; never raises.
    """
    if _client is not None or _shared is not None:
        return True
    if not _on_demand or _client_unavailable:
        return False
    from .config import get_config

    if not bool(get_config().get("cdn_first_wave", True)):
        return False
    return _build_client() is not None


def _tls_context() -> Any:
    """niquests' verification setup on the stdlib, or None where it cannot match it.

    niquests verifies against ``wassima.generate_ca_bundle()`` (the system
    trust store; ``trust_env`` is off, so no ``SSL_CERT_FILE``) and routes
    through the proxies ``urllib.request.getproxies()`` reports: with a
    proxy, or without ``wassima``, the wave is skipped.
    """
    from urllib.request import getproxies

    proxies = getproxies()
    if proxies.get("https") or proxies.get("all"):
        return None
    try:
        import wassima
    except ImportError:
        return None
    import ssl

    context = ssl.create_default_context(cadata=wassima.generate_ca_bundle())
    context.set_alpn_protocols(["http/1.1"])
    return context


def _fetch_into(client: _StdlibClient, url: str, slot: _Slot, timeout: float) -> None:
    try:
        status, body = client.get_with_status(url, timeout)
        slot.status = status
        slot.body = body if status == 200 else None
    except Exception as e:  # never surfaces: niquests fetches the file
        logger.debug("First-wave fetch of %s failed: %s", url, e)
    finally:
        slot.done.set()


class _StdlibClient:
    """GETs over ``http.client`` with the wave's TLS context, keeping connections alive."""

    def __init__(self, context: Any, headers: dict[str, str]) -> None:
        self.context = context
        self.headers = headers
        self._idle: dict[tuple[str, int], list[Any]] = {}
        self._lock = threading.Lock()

    def get(self, url: str, timeout: float) -> bytes | None:
        """The decoded body of a ``200`` for ``url``, else None (raises on transport errors).

        An idle kept-alive connection the server has closed meanwhile is
        replaced by a fresh one once.
        """
        status, body = self.get_with_status(url, timeout)
        return body if status == 200 else None

    def get_with_status(self, url: str, timeout: float) -> tuple[int | None, bytes | None]:
        """``(status, decoded body)`` of ``url`` (body None unless decodable); raises on errors."""
        from urllib.parse import urlsplit

        parts = urlsplit(url)
        if parts.scheme != "https" or not parts.hostname:
            return None, None
        key = (parts.hostname, parts.port or 443)
        target = parts.path + (f"?{parts.query}" if parts.query else "")
        with self._lock:
            idle = self._idle.get(key)
            connection = idle.pop() if idle else None
        if connection is not None:
            try:
                return self._request(key, connection, target, timeout)
            except Exception as e:  # stale keep-alive connection (RemoteDisconnected, reset)
                logger.debug("Idle first-wave connection dropped: %s", e)
        factory = _connection_factory
        if factory is None:
            import http.client

            factory = http.client.HTTPSConnection
        connection = factory(key[0], key[1], timeout=timeout, context=self.context)
        return self._request(key, connection, target, timeout)

    def _request(
        self, key: tuple[str, int], connection: Any, target: str, timeout: float
    ) -> tuple[int | None, bytes | None]:
        reusable = False
        try:
            if getattr(connection, "sock", None) is not None:
                connection.sock.settimeout(timeout)
            connection.request("GET", target, headers=self.headers)
            response = connection.getresponse()
            body = response.read()
            reusable = not response.will_close
            status = response.status
            if status != 200:
                return status, None
            encoding = (response.getheader("Content-Encoding") or "").strip().lower()
        finally:
            if reusable:
                with self._lock:
                    self._idle.setdefault(key, []).append(connection)
            else:
                connection.close()
        return status, _decode(body, encoding)

    def close(self) -> None:
        """Close the idle connections."""
        with self._lock:
            idle, self._idle = self._idle, {}
        for connections in idle.values():
            for connection in connections:
                connection.close()


#: The wave's client once its TLS context is built (``None`` before, and
#: when the wave was skipped). Its presence opens the telemetry bridge.
_client: _StdlibClient | None = None
#: The process-wide stdlib client (:func:`_build_client`): the wave's when a
#: wave ran, else built on demand for single requests.
_shared: _StdlibClient | None = None


def has_client() -> bool:
    """Whether the wave built its stdlib client (its TLS context is ready)."""
    return _client is not None


def standalone_client() -> _StdlibClient | None:
    """A stdlib client for single requests outside a session load, or None (Corinth C3).

    A cold ``tif1.ergast`` query never runs ``get_session``, so no first wave
    built a client; its Stats files (``events.json`` and the per-round files,
    jsDelivr) go over this one instead of importing niquests (~120-145 ms of
    CPU, which competed for the GIL with the pandas import the query
    overlaps) and opening OCSP-checked connections. The wave's client (and its
    kept-alive connections) when a wave ran, else the shared one
    (:func:`_build_client`), with the wave's setup: same trust store, same
    User-Agent, same security trade-off (no OCSP check). None where the wave
    cannot match niquests' setup (a proxy, no ``wassima``): the requests then
    go to niquests. It never opens the telemetry bridge.
    """
    client = _client
    if client is not None:
        return client
    return _build_client()


def _single_request_client() -> _StdlibClient | None:
    """The client of :func:`fetch` / :func:`fetch_status`: the wave's, else the shared one."""
    client = _client
    return client if client is not None else _shared


def fetch(url: str, *, timeout: float) -> bytes | None:
    """GET ``url`` over the wave's kept-alive stdlib connections (single requests).

    For the sync transport before any niquests session exists: a single
    request (the fastest lap's telemetry file, the Ergast classification)
    then needs no HTTP-stack import and no new TLS handshake. None, and the
    caller goes to niquests, when no stdlib client exists or the answer is not
    a ``200`` with a decodable body; never raises.
    """
    client = _single_request_client()
    if client is None:
        return None
    try:
        return client.get(url, timeout)
    except Exception as e:  # never surfaces: niquests fetches the file
        logger.debug("Stdlib fetch of %s failed: %s", url, e)
        return None


def fetch_status(url: str, *, timeout: float) -> tuple[int | None, bytes | None]:
    """:func:`fetch` with the status: ``(200, body)``, ``(404, None)``, ...; ``(None, None)``
    when the wave did not run or the request failed. Never raises.

    The columnar telemetry files (Gortyn G3) use it so that a session without
    files learns it from the 404 over the stdlib connections, without waiting
    for niquests.
    """
    client = _single_request_client()
    if client is None:
        return None, None
    try:
        return client.get_with_status(url, timeout)
    except Exception as e:  # never surfaces: niquests fetches the file
        logger.debug("Stdlib fetch of %s failed: %s", url, e)
        return None, None


def _decode(body: bytes, encoding: str) -> bytes | None:
    """Undo a gzip/deflate Content-Encoding (None for any other coding)."""
    if encoding in ("", "identity"):
        return body
    import zlib

    if encoding in ("gzip", "x-gzip"):
        return zlib.decompress(body, 16 + zlib.MAX_WBITS)
    if encoding == "deflate":
        try:
            return zlib.decompress(body)
        except zlib.error:  # raw deflate without the zlib header
            return zlib.decompress(body, -zlib.MAX_WBITS)
    return None
