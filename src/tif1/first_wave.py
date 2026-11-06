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
    """One in-flight first-wave fetch: set ``done`` once ``body`` is final."""

    __slots__ = ("body", "deadline", "done")

    def __init__(self, deadline: float) -> None:
        self.done = threading.Event()
        self.body: bytes | None = None
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


def start(urls: list[str], *, timeout: float) -> None:
    """Fetch ``urls`` over the standard library in daemon threads.

    One thread builds the TLS context (the ``ssl``/``http.client`` imports and
    the trust store, ~40 ms of CPU) and fetches the first URL; one more
    thread per further URL. Never raises: a failure leaves the slot empty and
    the request goes to niquests.
    """
    global _warm_urls, _warm_timeout
    deadline = time.monotonic() + WAIT_LIMIT
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
    if not _slots:
        return None
    with _slots_lock:
        slot = _slots.pop(url, None)
    if slot is None:
        return None
    if not slot.done.wait(max(0.0, slot.deadline - time.monotonic())):
        logger.debug("First-wave fetch of %s missed its window", url)
        return None
    return slot.body


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
    global _bridged, _client, _warm_started, _warm_urls
    _warm_started = False
    _warm_urls = []
    _bridged = 0
    with _slots_lock:
        _slots.clear()
    client, _client = _client, None
    if client is not None:
        client.close()


def _release(slots: list[tuple[str, _Slot]]) -> None:
    for _url, slot in slots:
        slot.done.set()


def _run_wave(slots: list[tuple[str, _Slot]], timeout: float) -> None:
    global _client
    try:
        context = _tls_context()
    except Exception as e:  # never surfaces: niquests fetches the files
        logger.debug("First wave skipped: %s", e)
        context = None
    if context is None:
        _release(slots)
        # The wave cannot match niquests' setup here: warm niquests
        # connections for the same files instead (the Olympia pre-connect).
        from . import http_session

        try:
            http_session.warm_connections([url for url, _slot in slots], timeout=timeout)
        except Exception as e:  # best effort, never surfaces
            logger.debug("CDN pre-connect skipped: %s", e)
        return
    from .http_session import user_agent

    client = _StdlibClient(
        context,
        {"User-Agent": user_agent(), "Accept": "*/*", "Accept-Encoding": "gzip, deflate"},
    )
    _client = client
    for url, slot in slots[1:]:
        threading.Thread(
            target=_fetch_into,
            args=(client, url, slot, timeout),
            name="tif1-first-wave",
            daemon=True,
        ).start()
    _fetch_into(client, *slots[0], timeout)


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
        slot.body = client.get(url, timeout)
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
#: when the wave was skipped).
_client: _StdlibClient | None = None


def has_client() -> bool:
    """Whether the wave built its stdlib client (its TLS context is ready)."""
    return _client is not None


def fetch(url: str, *, timeout: float) -> bytes | None:
    """GET ``url`` over the wave's kept-alive stdlib connections (single requests).

    For the sync transport before any niquests session exists: a single
    request (the fastest lap's telemetry file, the Ergast classification)
    then needs no HTTP-stack import and no new TLS handshake. None, and the
    caller goes to niquests, when the wave did not run or the answer is not a
    ``200`` with a decodable body; never raises.
    """
    client = _client
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
    client = _client
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
