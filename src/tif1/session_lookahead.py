"""Next-round lookahead for scripts that walk a season (Corinth C1).

A cold season loop (``for r in rounds: get_session(2025, r, "R").laps``) used
to pay every round's requests on its critical path, one round after the
other: the first-wave files (``drivers.json``, the session laptimes, and
``weather.json``/``rcm.json`` after ``load()``), then the Stats
classification once ``results`` was read (~15 ms each on a warm CDN edge,
0.2-1.5 s each when jsDelivr has to fetch the file from GitHub first).

:func:`on_get_session` notes every session ``get_session`` hands out. When a
script asks for round ``r`` of a season right after round ``r - 1`` (same
season, session and schedule backend), it fetches, in the background, the
session files round ``r - 1`` fetched over the network (:func:`note_fetch`,
called by the CDN fallback loops; telemetry is never part of it) for round
``r + 1``, plus the Stats file of the classification when round ``r - 1``
read one (:func:`note_classification`), and the classification of round
``r`` itself when no earlier lookahead fetched it. At most one session
ahead; nothing for a session the cache holds, for a session that has not
started yet, or after a round whose files came from the cache.

The bodies land in :mod:`tif1.first_wave` slots over its kept-alive stdlib
connections (no parse, no cache write and no GIL-heavy work off the main
thread), so the next round's load takes them through the same hand-over as a
first wave; anything but a ``200`` with a JSON object goes to niquests as
before. A script that stops early leaves at most one round of files unused
(~0.4 MB); the slots of a round nobody asked for are dropped when the
sequence moves on or breaks. Off with ``session_lookahead`` False
(``TIF1_SESSION_LOOKAHEAD``), in offline and CI mode, without
``cdn_preconnect`` and before :data:`~tif1.core_utils.constants.F1_API_FIRST_YEAR`.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


class _Record:
    """One session ``get_session`` handed out, and what its loads fetched."""

    __slots__ = (
        "backend",
        "classification",
        "gp",
        "paths",
        "round",
        "session",
        "session_name",
        "slots",
        "year",
    )

    def __init__(
        self, year: int, session_name: str, backend: Any, round_number: int, gp: str, session: str
    ) -> None:
        self.year = year
        self.session_name = session_name
        self.backend = backend
        self.round = round_number
        #: URL-encoded coordinates (the CDN paths and cache keys use them).
        self.gp = gp
        self.session = session
        #: Session-level files (``drivers.json``, ...) fetched over the network.
        self.paths: set[str] = set()
        #: Ergast endpoint of the classification the session read, if any.
        self.classification: str | None = None
        #: Lookahead slots fetched for this session (dropped once it is passed).
        self.slots: list[str] = []


_lock = threading.Lock()
_current: _Record | None = None
#: Lookahead slots fetched for the round after :data:`_current`.
_next_slots: list[str] = []


def reset() -> None:
    """Forget the sequence and drop its slots (tests)."""
    global _current
    from . import first_wave

    with _lock:
        stale = [*(_current.slots if _current is not None else ()), *_next_slots]
        _current = None
        _next_slots.clear()
    first_wave.discard(stale)


def note_fetch(year: int, gp: str, session: str, path: str) -> None:
    """Record that the current session fetched ``path`` over the network.

    Called by :meth:`tif1.cdn.CDNManager.try_sources` and its async twin for
    every request; only session-level files (no ``/`` in the path) count.
    """
    record = _current
    if (
        record is not None
        and "/" not in path
        and record.session == session
        and record.gp == gp
        and record.year == year
    ):
        record.paths.add(path)


def note_classification(year: int, gp: str, session: str, endpoint: str) -> None:
    """Record that the current session read its Ergast classification (``endpoint``)."""
    record = _current
    if record is not None and record.session == session and record.gp == gp and record.year == year:
        record.classification = endpoint


def on_get_session(
    year: int, gp_name: str, session_name: str, backend: Any, enable_cache: bool
) -> bool | None:
    """Note the session ``get_session`` is about to hand out; start the lookahead.

    Args:
        year: Season.
        gp_name: Resolved event name.
        session_name: Resolved session name.
        backend: Schedule backend ``get_session`` resolved the event with.
        enable_cache: Whether the session reads the persistent cache.

    Returns:
        The persistent-cache probe of this session when the lookahead ran it
        (``get_session`` hands it to the session), else None. Never raises.
    """
    global _current
    try:
        from .config import get_config
        from .core_utils.constants import F1_API_FIRST_YEAR
        from .core_utils.runtime import THREADS_AVAILABLE

        config = get_config()
        if (
            not THREADS_AVAILABLE
            or year < F1_API_FIRST_YEAR
            or not bool(config.get("session_lookahead", True))
            or not bool(config.get("cdn_preconnect", True))
            or bool(config.get("offline_mode", False))
            or bool(config.get("ci_mode", False))
        ):
            return None
        from .core_utils.helpers import _encode_url_component
        from .events import _event_round_number

        round_number = int(_event_round_number(year, gp_name, backend))
        record = _Record(
            year,
            session_name,
            backend,
            round_number,
            _encode_url_component(gp_name),
            _encode_url_component(session_name),
        )
        with _lock:
            previous, _current = _current, record
            own_slots = list(_next_slots)
            _next_slots.clear()
        stale = list(previous.slots) if previous is not None else []
        sequential = (
            previous is not None
            and round_number > 0
            and previous.year == year
            and previous.session_name == session_name
            and previous.backend == backend
            and previous.round + 1 == round_number
            and bool(previous.paths)
        )
        if not sequential:
            _discard([*stale, *own_slots])
            return None
        assert previous is not None
        record.slots = own_slots
        _discard(stale)
        return _start(record, previous, enable_cache)
    except Exception as e:  # best effort, never surfaces
        logger.debug("Session lookahead skipped: %s", e)
        return None


def _discard(urls: list[str]) -> None:
    if urls:
        from . import first_wave

        first_wave.discard(urls)


def _start(record: _Record, previous: _Record, enable_cache: bool) -> bool | None:
    """Fetch this round's missing classification and the next round's files."""
    from . import first_wave
    from .config import get_config

    cache = None
    probe: bool | None = None
    if enable_cache:
        from .cache import get_cache

        cache = get_cache()
        probe = bool(cache.has_session_data(record.year, record.gp, record.session))
        if probe:
            return probe  # a cached round: its successor is likely cached too
    own: list[str] = []
    if previous.classification is not None:
        url = _stats_url(record.year, record.round, previous.classification)
        if url is not None and url not in record.slots:
            own.append(url)
    upcoming = _next_round_urls(record, previous, cache)
    if not own and not upcoming:
        return probe
    config = get_config()
    timeout = max(1.0, float(config.get("timeout", 10)))
    fetched = set(first_wave.prefetch([*own, *upcoming], timeout=timeout))
    record.slots.extend(url for url in own if url in fetched)
    ahead = [url for url in upcoming if url in fetched]
    with _lock:
        if _current is record:
            _next_slots.extend(ahead)
            ahead = []
    _discard(ahead)  # the sequence moved on meanwhile
    return probe


def _next_round_urls(record: _Record, previous: _Record, cache: Any) -> list[str]:
    """Primary-CDN URLs of round ``record.round + 1`` that round ``previous`` fetched."""
    from .cdn import get_cdn_manager
    from .core_utils.helpers import _encode_url_component
    from .events import _get_sessions_cached, _resolve_event_round

    year, backend, session_name = record.year, record.backend, record.session_name
    next_round: Any = record.round + 1  # an int is a numbers.Real (ty cannot tell)
    try:
        next_name = _resolve_event_round(year, next_round, backend)
    except ValueError:  # the last round of the season
        return []
    sessions = _get_sessions_cached(year, next_name, backend)
    if session_name not in sessions or not _session_started(
        year, next_name, sessions.index(session_name) + 1, backend
    ):
        return []
    gp = _encode_url_component(next_name)
    session = record.session
    if cache is not None and cache.has_session_data(year, gp, session):
        return []
    sources = get_cdn_manager().get_sources()
    if not sources:
        return []
    urls = [sources[0].format_url(year, gp, session, path) for path in sorted(previous.paths)]
    if previous.classification is not None:
        stats = _stats_url(year, record.round + 1, previous.classification)
        if stats is not None:
            urls.append(stats)
    return urls


def _session_started(year: int, event_name: str, number: int, backend: Any) -> bool:
    """False when the schedule dates session ``number`` of the event in the future."""
    from .events import _get_year_schedule, _record_metadata

    metadata = _record_metadata(_get_year_schedule(year, backend)).get(event_name)
    value = metadata.get(f"Session{number}DateUtc") if isinstance(metadata, dict) else None
    if not isinstance(value, str):
        return True
    try:
        start = datetime.fromisoformat(value)
    except ValueError:
        return True
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    return start <= datetime.now(UTC)


def _stats_url(year: int, round_number: int, endpoint: str) -> str | None:
    """Primary-mirror URL of a round's Stats classification file, None when not needed.

    None when the season index is not cached (no request just to find the
    file), the round is unknown, or the file is answered from the cache tiers.
    """
    from .cdn import get_cdn_manager
    from .ergast.payloads import get_ergast_loader, stats_path
    from .ergast.stats import STATS_FILES, get_stats_source

    source = get_stats_source()
    if not source.season_index_cached(year):
        return None
    info = source.round_info(year, round_number)
    file_name = STATS_FILES.get(endpoint)
    if info is None or file_name is None:
        return None
    loader = get_ergast_loader()
    if loader.is_fresh(year, info.slug, file_name):
        return None
    repo, branch = loader.repo()
    sources = [s for s in get_cdn_manager().get_sources() if s.serves(repo)]
    if not sources:
        return None
    return sources[0].format_repo_url(repo, branch, stats_path(year, info.slug, file_name))
