"""Persisted OCSP verdicts: niquests' certificate revocation check across processes.

niquests checks the revocation status of every new TLS certificate it meets
(OCSP, Olympia H11): before the first request of a process may go out it
imports qh3 (the QUIC stack, ~32 ms of CPU) and posts to the CA's responder
(~100 ms). The answer is valid for hours to days (``nextUpdate``) and niquests
memoises it in an in-process ``InMemoryRevocationStatus`` keyed by the SHA-256
fingerprint of the leaf certificate, which supports pickling (``__getstate__``
/ ``__setstate__``, ``support_pickle()``) for exactly this purpose.

Elis E3 keeps the GOOD verdicts in the SQLite ``json`` tier (one row,
:data:`CACHE_KEY`) and restores them into the shared session's cache before its
first connection, so the next process skips the responder round trip for a
certificate it has already checked. Only answers that are SUCCESSFUL, GOOD,
signed by the issuer niquests verified them with, and valid for at least
:data:`MIN_REMAINING_SECONDS` more are written or restored; REVOKED and UNKNOWN
answers are never persisted. A restored entry only matches the certificate it
was issued for (niquests looks entries up by leaf fingerprint), so a CDN that
rotates its certificate gets a fresh check.

The OCSP code needs only ``qh3._hazmat`` (a 2 ms extension module), but the
``from qh3._hazmat import ...`` statement runs the ``qh3`` package's
``__init__`` first, which imports the whole QUIC stack. :func:`preload_hazmat`
loads the extension under its own name without that ``__init__``; a later
``import qh3`` (HTTP/3) still runs normally and reuses the loaded module.
"""

from __future__ import annotations

import base64
import importlib.machinery
import importlib.util
import logging
import os
import sys
import threading
import time
from pathlib import Path
from types import ModuleType
from typing import Any

logger = logging.getLogger(__name__)

#: SQLite ``json``-tier key of the persisted verdicts.
CACHE_KEY = "tif1/ocsp_verdicts"
#: Row layout version; a row of another version is ignored.
FORMAT_VERSION = 1
#: Entries kept in the row. niquests stops checking (``hold``) once its cache
#: holds 10 entries collected within a second, so a restored cache must stay
#: below that; tif1 talks to a handful of hosts.
MAX_ENTRIES = 8
#: A verdict older than this is never restored, whatever its ``nextUpdate``.
MAX_AGE_SECONDS = 7 * 86400
#: A verdict that expires within this many seconds is neither written nor restored.
MIN_REMAINING_SECONDS = 300

_OCSP_MODULE = "niquests.extensions.revocation._ocsp"

#: Fingerprints already in the row (restored or written by this process), or
#: judged not persistable; :func:`persist` skips them.
_seen: set[str] = set()
_lock = threading.Lock()


def preload_hazmat() -> None:
    """Load ``qh3._hazmat`` without running the ``qh3`` package ``__init__``.

    Best effort: any failure leaves the normal import path to niquests.
    """
    if "qh3._hazmat" in sys.modules or "qh3" in sys.modules:
        return
    try:
        spec = importlib.util.find_spec("qh3")
        if spec is None or not spec.submodule_search_locations:
            return
        for location in spec.submodule_search_locations:
            for suffix in importlib.machinery.EXTENSION_SUFFIXES:
                candidate = Path(location) / f"_hazmat{suffix}"
                if candidate.is_file():
                    path = os.fspath(candidate)
                    loader = importlib.machinery.ExtensionFileLoader("qh3._hazmat", path)
                    module_spec = importlib.util.spec_from_file_location(
                        "qh3._hazmat", path, loader=loader
                    )
                    if module_spec is None:
                        return
                    module = importlib.util.module_from_spec(module_spec)
                    sys.modules["qh3._hazmat"] = module
                    loader.exec_module(module)
                    return
    except Exception as e:  # never block a session on the shortcut
        sys.modules.pop("qh3._hazmat", None)
        logger.debug("qh3._hazmat preload skipped: %s", e)


def _ocsp_module() -> ModuleType | None:
    """niquests' OCSP module (imports qh3's hazmat layer), or None when unavailable."""
    module = sys.modules.get(_OCSP_MODULE)
    if module is not None:
        return module
    preload_hazmat()
    try:
        import niquests.extensions.revocation._ocsp as module
    except ImportError:
        return None
    if not module.InMemoryRevocationStatus.support_pickle():
        return None
    return module


def _good_until(module: ModuleType, response: Any, now: float) -> int | None:
    """``nextUpdate`` of a SUCCESSFUL, GOOD answer valid for a while yet, else None."""
    if response.response_status != module.OCSPResponseStatus.SUCCESSFUL:
        return None
    if response.certificate_status != module.OCSPCertStatus.GOOD:
        return None
    next_update = response.next_update
    if not next_update or next_update - now < MIN_REMAINING_SECONDS:
        return None
    return int(next_update)


def _row_version() -> str:
    niquests = sys.modules.get("niquests")
    return f"{FORMAT_VERSION}:{getattr(niquests, '__version__', '')}"


def _read_entries(cache: Any, now: float) -> dict[str, dict[str, Any]]:
    """The row's entries that are still restorable (by their stored metadata)."""
    row = cache.get_entry("json", CACHE_KEY)
    if not isinstance(row, dict) or row.get("version") != _row_version():
        return {}
    entries = row.get("entries")
    if not isinstance(entries, dict):
        return {}
    live: dict[str, dict[str, Any]] = {}
    for fingerprint, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        next_update = entry.get("next_update")
        saved_at = entry.get("saved_at")
        if not isinstance(next_update, int) or not isinstance(saved_at, int | float):
            continue
        if next_update - now < MIN_REMAINING_SECONDS or now - saved_at > MAX_AGE_SECONDS:
            continue
        if isinstance(entry.get("response"), str) and isinstance(entry.get("issuer"), str):
            live[fingerprint] = entry
    return live


def seed(session: Any, cache: Any) -> int:
    """Restore the persisted verdicts into ``session``'s revocation cache.

    Call before the session's first connection. Entries are re-validated after
    decoding (SUCCESSFUL, GOOD, ``nextUpdate`` ahead); niquests then serves a
    restored entry only to the certificate it was issued for.

    Returns:
        The number of verdicts restored (0 when there was nothing to restore).
    """
    if getattr(session, "_ocsp_cache", None) is not None:
        return 0
    now = time.time()
    entries = _read_entries(cache, now)
    if not entries:
        return 0
    module = _ocsp_module()
    if module is None:
        return 0
    store: dict[str, bytes] = {}
    issuers: dict[str, bytes] = {}
    for fingerprint, entry in entries.items():
        try:
            response = base64.b64decode(entry["response"])
            issuer = base64.b64decode(entry["issuer"])
            if _good_until(module, module.OCSPResponse.deserialize(response), now) is None:
                continue
            module.Certificate.deserialize(issuer)
        except Exception:  # an undecodable entry is skipped, never fatal
            continue
        store[fingerprint] = response
        issuers[fingerprint] = issuer
    if not store:
        return 0
    revocation_cache = module.InMemoryRevocationStatus()
    revocation_cache.__setstate__(
        {"_max_size": 2048, "_store": store, "_issuers_map": issuers, "_failure_count": 0}
    )
    session._ocsp_cache = revocation_cache
    for adapter in session.adapters.values():
        if hasattr(adapter, "_ocsp_cache"):
            adapter._ocsp_cache = revocation_cache
    with _lock:
        _seen.update(store)
    return len(store)


def persist(session: Any, cache: Any) -> int:
    """Write the GOOD verdicts ``session`` collected since the last call to the row.

    Cheap when nothing is new (one pass over niquests' fingerprints). Every new
    answer is re-authenticated against its issuer before it is written, so an
    answer niquests accepted without a signature check (a proxy or a
    private-address peer) never outlives its process.

    Returns:
        The number of verdicts written.
    """
    revocation_cache = getattr(session, "_ocsp_cache", None)
    module = sys.modules.get(_OCSP_MODULE)
    if revocation_cache is None or module is None:
        return 0
    if not isinstance(revocation_cache, module.InMemoryRevocationStatus):
        return 0
    with _lock:
        state = revocation_cache.__getstate__()
        responses: dict[str, bytes] = state.get("_store", {})
        issuers: dict[str, bytes] = state.get("_issuers_map", {})
        new = [fingerprint for fingerprint in responses if fingerprint not in _seen]
        if not new:
            return 0
        now = time.time()
        fresh: dict[str, dict[str, Any]] = {}
        for fingerprint in new:
            _seen.add(fingerprint)
            issuer_der = issuers.get(fingerprint)
            if issuer_der is None:
                continue
            try:
                response = module.OCSPResponse.deserialize(responses[fingerprint])
                next_update = _good_until(module, response, now)
                if next_update is None:
                    continue
                issuer = module.Certificate.deserialize(issuer_der)
                if not response.authenticate_for(issuer.public_bytes()):
                    continue
            except Exception:  # unsupported signature, undecodable answer: not persisted
                continue
            fresh[fingerprint] = {
                "response": base64.b64encode(responses[fingerprint]).decode("ascii"),
                "issuer": base64.b64encode(issuer_der).decode("ascii"),
                "next_update": next_update,
                "saved_at": int(now),
            }
        if not fresh:
            return 0
        entries = _read_entries(cache, now)
        entries.update(fresh)
        newest = sorted(entries.items(), key=lambda item: item[1]["saved_at"], reverse=True)
        cache.set_entry(
            "json",
            CACHE_KEY,
            {"version": _row_version(), "entries": dict(newest[:MAX_ENTRIES])},
        )
        return len(fresh)
