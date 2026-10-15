"""CDN fallback system with multiple sources.

The :class:`CDNManager` owns the source-ordering and fallback loop for both
the sync pipeline (:meth:`CDNManager.try_sources`, used by
``tif1.payload_loader.PayloadLoader``) and the async fan-out pipeline
(:meth:`CDNManager.try_sources_async`, used by ``tif1.async_fetch``), so CDN
fallback exists exactly once in the codebase.

Default priority order: jsDelivr (primary), Hugging Face buckets, StaticDelivr
(last resort). A 404 from one CDN falls through to the next: mirrors can be
stale or divergent, so a payload missing on one CDN may resolve on another.
In the async pipeline the remaining sources are raced concurrently after a
404 (missing-file walks pay one latency, not one per CDN); the happy path is
untouched. When every source refused the request with a 4xx client error
(404 included) the fallback raises :class:`DataNotFoundError` — no mirror
holds the file, so retrying cannot help — instead of burning the retry/backoff
budget on files that are missing everywhere.

"""

import logging
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .exceptions import DataNotFoundError, InvalidDataError, NetworkError, TIF1Error

if TYPE_CHECKING:
    import asyncio

logger = logging.getLogger(__name__)

# Thebes TH3: latency-aware routing + straggler hedging.
#
# Per-source request latency is tracked as an exponentially weighted moving
# average. Sources keep their configured order unless one has measured
# clearly slower than a lower-priority mirror (both with enough samples), in
# which case it is demoted for the rest of the process. A request on the
# first source that has not answered within the hedge delay (a multiple of
# that source's EWMA, clamped) is re-issued on the next source and the first
# answer wins; the abandoned request is counted against the slow source so
# its EWMA stays honest. Only stragglers pay the extra request - the happy
# path sends exactly one request per file, unlike the per-request hedging of
# every CDN that Experiment 11 rejected.
_LATENCY_EWMA_ALPHA = 0.2
_RERANK_MIN_SAMPLES = 8
_RERANK_FACTOR = 2.0
_HEDGE_FACTOR = 2.0
_HEDGE_MIN_DELAY = 1.0
_HEDGE_MAX_DELAY_DEFAULT = 4.0


@dataclass
class CDNSource:
    """CDN source configuration."""

    name: str
    base_url: str
    priority: int = 0
    enabled: bool = True
    use_minification: bool = False

    def format_url(self, year: int, gp: str, session: str, path: str) -> str:
        """Format URL for this CDN.

        jsDelivr format: https://cdn.jsdelivr.net/gh/user/repo@branch/path
        StaticDelivr format: https://cdn.staticdelivr.com/gh/user/repo/branch/path
        Hugging Face bucket format: https://huggingface.co/buckets/user/bucket/resolve/path

        ``use_minification`` requests ``file.min.json`` instead of ``file.json``;
        it is meant for a custom mirror that serves minified copies. The
        default CDNs ignore the global ``cdn_use_minification`` option (see
        ``CDNManager``): jsDelivr minifies only JavaScript and CSS, so the
        request would answer 404 everywhere and only add a wasted round trip.
        """
        if self.use_minification and path.endswith(".json"):
            path = path.replace(".json", ".min.json")

        lowered = self.base_url.lower()
        # Hugging Face buckets mirror the GitHub repos without a branch segment;
        # files are served through the /resolve/ endpoint.
        if "huggingface" in lowered:
            return f"{self.base_url}/{year}/resolve/{gp}/{session}/{path}"
        # StaticDelivr uses /branch/ format, jsDelivr uses @branch format
        if "staticdelivr" in lowered:
            return f"{self.base_url}/{year}/main/{gp}/{session}/{path}"
        return f"{self.base_url}/{year}@main/{gp}/{session}/{path}"


class CDNManager:
    """Manage multiple CDN sources with fallback and rate limiting.

    The module-level manager (:func:`get_cdn_manager`) reads ``cdns`` and
    ``cdn_use_minification`` from the config when ``tif1.cdn`` is imported;
    call :meth:`reload` after ``config.set(...)`` to apply a change.
    """

    def __init__(self):
        self.reload()

    def reload(self) -> None:
        """Rebuild the source list from the current config and reset failure counts."""
        from .config import get_config

        config = get_config()
        default_sources = [
            "https://cdn.jsdelivr.net/gh/TracingInsights",
            "https://huggingface.co/buckets/tracinginsights",
            "https://cdn.staticdelivr.com/gh/TracingInsights",
        ]
        configured_sources = config.get("cdns", default_sources) or default_sources
        use_minification = config.get("cdn_use_minification", False)

        self.sources = []
        for i, base_url in enumerate(configured_sources, start=1):
            if not isinstance(base_url, str) or not base_url.startswith("https://"):
                logger.warning(f"Skipping invalid CDN URL: {base_url}")
                continue
            if "raw.githubusercontent.com" in base_url:
                logger.warning(f"Skipping unsupported CDN URL: {base_url}")
                continue
            name = self._name_for_url(base_url, i)
            if any(s.name == name for s in self.sources):
                name = f"{name} {i}"
            # None of the default CDNs serves .min.json (jsDelivr minifies only
            # JS/CSS; Hugging Face and StaticDelivr mirror the repo as-is), so a
            # minified request is a guaranteed 404 and a wasted round trip there.
            # The option only applies to custom mirrors.
            source_minification = use_minification and not self._is_default_cdn(base_url)
            self.sources.append(
                CDNSource(
                    name=name,
                    base_url=base_url.rstrip("/"),
                    priority=i,
                    use_minification=source_minification,
                )
            )

        if not self.sources:
            logger.warning("No valid CDNs configured, using defaults")
            self.sources = [
                CDNSource(
                    name="jsDelivr",
                    base_url=default_sources[0],
                    priority=1,
                    use_minification=False,
                ),
                CDNSource(
                    name="HuggingFace",
                    base_url=default_sources[1],
                    priority=2,
                    use_minification=False,
                ),
                CDNSource(
                    name="StaticDelivr",
                    base_url=default_sources[2],
                    priority=3,
                    use_minification=False,
                ),
            ]

        self._failure_counts = {source.name: 0 for source in self.sources}
        self._max_failures = 3
        self._latency_lock = threading.Lock()
        self._latency_ewma: dict[str, float] = {}
        self._latency_samples: dict[str, int] = {}
        self._hedge_enabled = bool(config.get("cdn_hedge", True))
        max_delay = config.get("cdn_hedge_max_delay", _HEDGE_MAX_DELAY_DEFAULT)
        self._hedge_max_delay = (
            float(max_delay)
            if isinstance(max_delay, int | float) and max_delay > 0
            else _HEDGE_MAX_DELAY_DEFAULT
        )

    @staticmethod
    def _is_default_cdn(base_url: str) -> bool:
        """True for jsDelivr, Hugging Face and StaticDelivr, none of which serves ``.min.json``."""
        lowered = base_url.lower()
        return any(host in lowered for host in ("jsdelivr", "huggingface", "staticdelivr"))

    @staticmethod
    def _name_for_url(base_url: str, index: int) -> str:
        if "jsdelivr" in base_url:
            return "jsDelivr"
        if "staticdelivr" in base_url:
            return "StaticDelivr"
        if "huggingface" in base_url:
            return "HuggingFace"
        return f"CDN {index}"

    def add_source(self, source: CDNSource):
        """Add a CDN source."""
        self.sources.append(source)
        self.sources.sort(key=lambda x: x.priority)
        self._failure_counts[source.name] = 0

    def get_sources(self) -> list[CDNSource]:
        """Get enabled CDN sources in routing order.

        Configured priority order, except that a source measured at more
        than ``_RERANK_FACTOR`` times the latency of a lower-priority source
        (both with at least ``_RERANK_MIN_SAMPLES`` requests) is moved behind
        it. Sources without enough samples keep their configured position.
        """
        enabled = [
            s
            for s in self.sources
            if s.enabled and self._failure_counts[s.name] < self._max_failures
        ]
        if len(enabled) < 2 or not self._latency_ewma:
            return enabled
        with self._latency_lock:
            measured = {
                name: self._latency_ewma[name]
                for name in self._latency_ewma
                if self._latency_samples.get(name, 0) >= _RERANK_MIN_SAMPLES
            }
        if len(measured) < 2:
            return enabled
        ordered = list(enabled)
        changed = True
        while changed:
            changed = False
            for i in range(len(ordered) - 1):
                left, right = ordered[i], ordered[i + 1]
                if (
                    left.name in measured
                    and right.name in measured
                    and measured[left.name] > _RERANK_FACTOR * measured[right.name]
                ):
                    ordered[i], ordered[i + 1] = right, left
                    changed = True
        return ordered

    def record_latency(self, source_name: str, seconds: float) -> None:
        """Feed one request latency (successful or abandoned) into the source's EWMA."""
        if seconds < 0:
            return
        with self._latency_lock:
            current = self._latency_ewma.get(source_name)
            if current is None:
                self._latency_ewma[source_name] = seconds
            else:
                self._latency_ewma[source_name] = current + _LATENCY_EWMA_ALPHA * (
                    seconds - current
                )
            self._latency_samples[source_name] = self._latency_samples.get(source_name, 0) + 1

    def latency_estimate(self, source_name: str) -> float | None:
        """Current EWMA latency of a source in seconds, or None before any sample."""
        with self._latency_lock:
            return self._latency_ewma.get(source_name)

    def hedge_delay(self, source_name: str) -> float | None:
        """Seconds to wait on ``source_name`` before re-issuing a request elsewhere.

        ``None`` when hedging is disabled. A source with no samples hedges
        at the maximum delay, so a warm edge (tens of milliseconds) never
        hedges and a cold edge (seconds) starts hedging on its first wave.
        """
        if not self._hedge_enabled:
            return None
        estimate = self.latency_estimate(source_name)
        if estimate is None:
            return self._hedge_max_delay
        return min(self._hedge_max_delay, max(_HEDGE_MIN_DELAY, _HEDGE_FACTOR * estimate))

    def mark_failure(self, source_name: str):
        """Mark a CDN source as failed."""
        self._failure_counts[source_name] += 1
        if self._failure_counts[source_name] >= self._max_failures:
            logger.warning(
                f"CDN source '{source_name}' disabled after {self._max_failures} failures"
            )

    def mark_success(self, source_name: str, latency: float | None = None):
        """Mark a CDN source as successful (optionally recording the request latency)."""
        self._failure_counts[source_name] = 0
        if latency is not None:
            self.record_latency(source_name, latency)

    def reset(self):
        """Reset all failure counts and latency estimates."""
        self._failure_counts = {source.name: 0 for source in self.sources}
        with self._latency_lock:
            self._latency_ewma.clear()
            self._latency_samples.clear()

    def try_sources(
        self, year: int, gp: str, session: str, path: str, fetch_func: Callable[[str], Any]
    ) -> Any:
        """Try fetching from CDN sources with fallback.

        A 404 (:class:`DataNotFoundError`) falls through to the next source:
        mirrors can be stale or divergent, so the payload may exist elsewhere.
        Any other failure marks the source down and falls through. When every
        source was tried, ``DataNotFoundError`` is raised if all refusals were
        4xx client errors (404 included) — no mirror holds the file, so
        retrying cannot help; otherwise :class:`NetworkError`.
        :class:`InvalidDataError` propagates immediately; invalid payloads
        will not improve on another CDN.
        """
        sources = self.get_sources()

        if not sources:
            raise NetworkError(url=f"{year}/{gp}/{session}/{path}", status_code=None)

        last_exception = None
        statuses: list[int | None] = []

        for source in sources:
            try:
                url = source.format_url(year, gp, session, path)
                logger.debug(f"Trying CDN: {source.name} - {url}")
                started = time.monotonic()
                result = fetch_func(url)
                self.mark_success(source.name, time.monotonic() - started)
                return result
            except DataNotFoundError:
                # 404 can be CDN-specific (stale cache, divergent mirror);
                # it is not a health signal, so no failure count.
                logger.debug(f"CDN {source.name} returned 404 for {path}, trying next source")
                statuses.append(404)
            except InvalidDataError:
                raise
            except Exception as e:
                status = self._response_status(e)
                if CDNManager._is_file_refusal(status):
                    # Argos A11: a 403 is per-file on jsDelivr ("Package size
                    # exceeded the configured limit of 50 MB" for files its
                    # edge has not cached) - like a 404 it says nothing about
                    # the mirror's health, so it must not count towards
                    # disabling the source for the rest of the process.
                    logger.debug(f"CDN {source.name} refused {path} ({status}), trying next source")
                else:
                    logger.warning(f"CDN {source.name} failed: {e}")
                    self.mark_failure(source.name)
                last_exception = e
                statuses.append(status)

        # Called via the class so Mock-bound instances keep the real decision logic.
        if CDNManager._all_client_refusals(statuses):
            raise DataNotFoundError(year=year, event=gp, session=session)

        raise NetworkError(
            url=f"{year}/{gp}/{session}/{path}",
            status_code=self._response_status(last_exception),
        )

    @staticmethod
    def _is_file_refusal(status: int | None) -> bool:
        """True for statuses that refuse one file without saying the mirror is down.

        404: the mirror does not (yet) hold the file. 403: jsDelivr's answer for
        a file it cannot fetch from a repository over its 50 MB package limit -
        intermittent and per-file (files already in its cache keep serving).
        Neither is a health signal; both fall through to the next source.
        """
        return status in (403, 404)

    async def try_sources_async(
        self,
        year: int,
        gp: str,
        session: str,
        path: str,
        fetch_func: Callable[[CDNSource, str], Awaitable[Any]],
    ) -> Any:
        """Async variant of :meth:`try_sources` for the fan-out pipeline.

        Iterates enabled sources in priority order, awaiting ``fetch_func``
        per source URL. A 404 (:class:`DataNotFoundError`) falls through to
        the next source without marking it failed; any other failure marks the
        source down and falls through. When every source was tried,
        ``DataNotFoundError`` is raised if all refusals were 4xx client errors
        (404 included) — no mirror holds the file, so retrying cannot help;
        otherwise :class:`NetworkError`. :class:`InvalidDataError` propagates
        immediately.

        Args:
            year: Season year.
            gp: Grand Prix identifier.
            session: Session identifier.
            path: Session-relative payload path.
            fetch_func: Async callable taking ``(source, url)`` and returning
                the parsed payload, raising on failure.
        """
        sources = self.get_sources()

        if not sources:
            raise NetworkError(url=f"{year}/{gp}/{session}/{path}", status_code=None)

        last_exception = None
        statuses: list[int | None] = []

        for index, source in enumerate(sources):
            try:
                url = source.format_url(year, gp, session, path)
                logger.debug(f"Trying CDN: {source.name} - {url}")
                # Thebes TH3: the primary attempt is hedged on the next source
                # past the hedge delay; a primary refusal or failure is
                # re-raised into the handlers below. Stand-in managers that
                # borrow this loop without the hedge machinery stay sequential.
                if index == 0 and len(sources) > 1 and isinstance(self, CDNManager):
                    payload, primary_error = await self._hedged_primary_async(
                        source, sources[1], url, year, gp, session, path, fetch_func
                    )
                    if primary_error is None:
                        return payload
                    raise primary_error
                started = time.monotonic()
                result = await fetch_func(source, url)
                self.mark_success(source.name, time.monotonic() - started)
                return result
            except DataNotFoundError:
                # Mirror may be stale or divergent; race the remaining sources
                # concurrently instead of paying each latency in series.
                statuses.append(404)
                remaining = sources[index + 1 :]
                if not remaining:
                    break
                result, race_error, race_statuses = await self._race_remaining_async(
                    year, gp, session, path, fetch_func, remaining
                )
                statuses.extend(race_statuses)
                if result is not None:
                    return result
                if race_error is not None:
                    last_exception = race_error
                break  # every source has now been tried
            except InvalidDataError:
                raise
            except Exception as e:
                status = self._response_status(e)
                if not CDNManager._is_file_refusal(status):
                    logger.warning(f"CDN {source.name} failed: {type(e).__name__}: {e}")
                    self.mark_failure(source.name)
                    last_exception = e
                    statuses.append(status)
                    continue
                # Argos A11: a per-file refusal (403 on jsDelivr's over-limit
                # repository, or a 404 surfaced as an HTTP error) is handled
                # exactly like the DataNotFoundError branch above - no health
                # penalty, race the remaining sources.
                logger.debug(f"CDN {source.name} refused {path} ({status}), racing remaining")
                last_exception = e
                statuses.append(status)
                remaining = sources[index + 1 :]
                if not remaining:
                    break
                result, race_error, race_statuses = await self._race_remaining_async(
                    year, gp, session, path, fetch_func, remaining
                )
                statuses.extend(race_statuses)
                if result is not None:
                    return result
                if race_error is not None:
                    last_exception = race_error
                break

        # Called via the class so Mock-bound instances keep the real decision logic.
        if CDNManager._all_client_refusals(statuses):
            raise DataNotFoundError(year=year, event=gp, session=session)

        raise NetworkError(
            url=f"{year}/{gp}/{session}/{path}",
            status_code=self._response_status(last_exception),
        )

    async def _hedged_primary_async(
        self,
        primary: CDNSource,
        backup: CDNSource,
        url: str,
        year: int,
        gp: str,
        session: str,
        path: str,
        fetch_func: Callable[[CDNSource, str], Awaitable[Any]],
    ) -> tuple[Any, BaseException | None]:
        """Fetch from ``primary``; past the hedge delay also try ``backup``.

        Returns ``(payload, None)`` when either source succeeded, or
        ``(None, error)`` with the primary's own refusal/transport error for
        the sequential loop to handle. A backup that answers first with a
        404/403 or an error is ignored (the primary still decides); a backup
        that succeeds first wins, the primary's request is abandoned and
        counted as a latency sample of the hedge delay (so a slow mirror's
        estimate stays honest), and ``InvalidDataError`` from either source
        propagates.
        """
        import asyncio  # lazy: only async/network paths need it

        awaitable = fetch_func(primary, url)
        delay = self.hedge_delay(primary.name)
        started = time.monotonic()
        if delay is None or not (
            asyncio.iscoroutine(awaitable) or isinstance(awaitable, asyncio.Future)
        ):
            try:
                payload = await awaitable
            except Exception as e:
                return None, e
            self.mark_success(primary.name, time.monotonic() - started)
            return payload, None

        primary_task = asyncio.ensure_future(awaitable)
        done, _pending = await asyncio.wait({primary_task}, timeout=delay)
        if primary_task in done:
            return self._settle_primary(primary, primary_task, started)

        # Straggler: re-issue on the backup; the first usable answer wins.
        self.record_latency(primary.name, delay)
        logger.debug(
            f"CDN {primary.name} slow on {path} (> {delay:.1f}s), hedging on {backup.name}"
        )
        backup_started = time.monotonic()
        backup_task = asyncio.ensure_future(
            fetch_func(backup, backup.format_url(year, gp, session, path))
        )
        pending = {primary_task, backup_task}
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if backup_task in done and not backup_task.cancelled():
                error = backup_task.exception()
                if error is None:
                    self.mark_success(backup.name, time.monotonic() - backup_started)
                    if not primary_task.done():
                        primary_task.cancel()
                    elif primary_task.exception() is not None:
                        # The primary failed meanwhile: a transport error is a
                        # health signal even though the backup answered.
                        self._note_primary_failure(primary, primary_task.exception())
                    return backup_task.result(), None
                if isinstance(error, InvalidDataError):
                    if not primary_task.done():
                        primary_task.cancel()
                    return None, error
                if not isinstance(error, DataNotFoundError) and not self._is_file_refusal(
                    self._response_status(error)
                ):
                    logger.warning(f"CDN {backup.name} failed: {type(error).__name__}: {error}")
                    self.mark_failure(backup.name)
                # A refusal or failure on the backup: the primary decides.
            if primary_task in done:
                if backup_task.done() or primary_task.exception() is None:
                    if not backup_task.done():
                        backup_task.cancel()
                    return self._settle_primary(primary, primary_task, started)
                # Primary failed while the backup is still running: wait for it.
        return self._settle_primary(primary, primary_task, started)

    def _note_primary_failure(self, primary: CDNSource, error: BaseException | None) -> None:
        if error is None or isinstance(error, DataNotFoundError | InvalidDataError):
            return
        if self._is_file_refusal(self._response_status(error)):
            return
        logger.warning(f"CDN {primary.name} failed: {type(error).__name__}: {error}")
        self.mark_failure(primary.name)

    def _settle_primary(
        self, primary: CDNSource, task: "asyncio.Future", started: float
    ) -> tuple[Any, BaseException | None]:
        error = task.exception()
        if error is None:
            self.mark_success(primary.name, time.monotonic() - started)
            return task.result(), None
        return None, error

    async def _race_remaining_async(
        self,
        year: int,
        gp: str,
        session: str,
        path: str,
        fetch_func: Callable[[CDNSource, str], Awaitable[Any]],
        remaining: list[CDNSource],
    ) -> tuple[Any | None, Exception | None, list[int | None]]:
        """Fetch the remaining sources concurrently after a primary 404.

        Returns ``(result, last_error, statuses)``: ``result`` is the first
        successful payload (that source is marked up), or None when every
        remaining source also failed — with ``last_error`` holding the last
        transport error (source marked down, mirroring the sequential loop)
        and ``statuses`` holding each raced source's refusal status (404 for
        :class:`DataNotFoundError`, best-effort status otherwise). 404
        responses are tiny, so loser requests are effectively free.
        :class:`InvalidDataError` propagates immediately, exactly like the
        sequential loop.
        """

        import asyncio  # lazy: only async/network paths need it

        class _RaceFailureError(Exception):
            def __init__(self, source_name: str, error: Exception) -> None:
                super().__init__(source_name)
                self.source_name = source_name
                self.error = error

        async def _fetch_one(source: CDNSource) -> tuple[CDNSource, Any]:
            try:
                payload = await fetch_func(source, source.format_url(year, gp, session, path))
            except Exception as e:
                raise _RaceFailureError(source.name, e) from e
            return source, payload

        tasks = [asyncio.ensure_future(_fetch_one(source)) for source in remaining]
        last_error: Exception | None = None
        statuses: list[int | None] = []
        try:
            for coro in asyncio.as_completed(tasks):
                try:
                    source, payload = await coro
                except _RaceFailureError as race_failure:
                    error = race_failure.error
                    if isinstance(error, DataNotFoundError):
                        statuses.append(404)
                        continue
                    if isinstance(error, InvalidDataError):
                        raise error from race_failure
                    status = self._response_status(error)
                    if CDNManager._is_file_refusal(status):
                        # Per-file refusal (Argos A11): no health penalty.
                        logger.debug(f"CDN {race_failure.source_name} refused the file ({status})")
                    else:
                        logger.warning(
                            f"CDN {race_failure.source_name} failed: {type(error).__name__}: {error}"
                        )
                        self.mark_failure(race_failure.source_name)
                    last_error = error
                    statuses.append(status)
                    continue
                self.mark_success(source.name)
                return payload, None, []
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
        return None, last_error, statuses

    @staticmethod
    def _response_status(exception: BaseException | None) -> int | None:
        """Best-effort HTTP status carried by an exception, if any."""
        if exception is None:
            return None
        response = getattr(exception, "response", None)
        status = getattr(response, "status_code", None)
        if status is None and isinstance(exception, TIF1Error):
            # The sync transport raises NetworkError(status_code=...) without a response.
            status = exception.context.get("status_code")
        return status if isinstance(status, int) else None

    @staticmethod
    def _all_client_refusals(statuses: list[int | None]) -> bool:
        """True when every source refused with a 4xx client error.

        A ``None`` status (transport error, timeout), a 5xx, or a 4xx that means
        "try again later" (408, 429) means a mirror might still serve the
        payload, so the caller must stay retryable instead of recording the file
        as missing. Non-int entries (defensive: mocked seams) also count as
        non-refusals.
        """
        return bool(statuses) and all(
            isinstance(s, int) and 400 <= s < 500 and s not in (408, 429) for s in statuses
        )


_cdn_manager = CDNManager()


def get_cdn_manager() -> CDNManager:
    """Get global CDN manager instance."""
    return _cdn_manager
