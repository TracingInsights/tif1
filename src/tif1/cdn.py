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

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .exceptions import DataNotFoundError, InvalidDataError, NetworkError

logger = logging.getLogger(__name__)


@dataclass
class CDNSource:
    """CDN source configuration."""

    name: str
    base_url: str
    priority: int = 0
    enabled: bool = True
    use_minification: bool = False

    def format_url(self, year: int, gp: str, session: str, path: str) -> str:
        """Format URL for this CDN with optional minification support.

        jsDelivr format: https://cdn.jsdelivr.net/gh/user/repo@branch/path
        StaticDelivr format: https://cdn.staticdelivr.com/gh/user/repo/branch/path
        Hugging Face bucket format: https://huggingface.co/buckets/user/bucket/resolve/path

        jsDelivr supports automatic minification by appending .min before the extension.
        This can reduce file sizes by 20-40% for JSON files.
        """
        # jsDelivr minification: file.json -> file.min.json
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
    """Manage multiple CDN sources with fallback and rate limiting."""

    def __init__(self):
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
            # Hugging Face does not minify; a .min.json request would 404 and
            # DataNotFoundError aborts the fallback chain, so never minify there.
            source_minification = use_minification and "huggingface" not in base_url.lower()
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
                    use_minification=use_minification,
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
                    use_minification=use_minification,
                ),
            ]

        self._failure_counts = {source.name: 0 for source in self.sources}
        self._max_failures = 3

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
        """Get enabled CDN sources sorted by priority."""
        return [
            s
            for s in self.sources
            if s.enabled and self._failure_counts[s.name] < self._max_failures
        ]

    def mark_failure(self, source_name: str):
        """Mark a CDN source as failed."""
        self._failure_counts[source_name] += 1
        if self._failure_counts[source_name] >= self._max_failures:
            logger.warning(
                f"CDN source '{source_name}' disabled after {self._max_failures} failures"
            )

    def mark_success(self, source_name: str):
        """Mark a CDN source as successful."""
        self._failure_counts[source_name] = 0

    def reset(self):
        """Reset all failure counts."""
        self._failure_counts = {source.name: 0 for source in self.sources}

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
                result = fetch_func(url)
                self.mark_success(source.name)
                return result
            except DataNotFoundError:
                # 404 can be CDN-specific (stale cache, divergent mirror);
                # it is not a health signal, so no failure count.
                logger.debug(f"CDN {source.name} returned 404 for {path}, trying next source")
                statuses.append(404)
            except InvalidDataError:
                raise
            except Exception as e:
                logger.warning(f"CDN {source.name} failed: {e}")
                self.mark_failure(source.name)
                last_exception = e
                statuses.append(self._response_status(e))

        # Called via the class so Mock-bound instances keep the real decision logic.
        if CDNManager._all_client_refusals(statuses):
            raise DataNotFoundError(year=year, event=gp, session=session)

        raise NetworkError(
            url=f"{year}/{gp}/{session}/{path}",
            status_code=self._response_status(last_exception),
        )

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
                result = await fetch_func(source, url)
                self.mark_success(source.name)
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
                logger.warning(f"CDN {source.name} failed: {type(e).__name__}: {e}")
                if self._response_status(e) != 404:
                    self.mark_failure(source.name)
                last_exception = e
                statuses.append(self._response_status(e))

        # Called via the class so Mock-bound instances keep the real decision logic.
        if CDNManager._all_client_refusals(statuses):
            raise DataNotFoundError(year=year, event=gp, session=session)

        raise NetworkError(
            url=f"{year}/{gp}/{session}/{path}",
            status_code=self._response_status(last_exception),
        )

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
                    logger.warning(
                        f"CDN {race_failure.source_name} failed: {type(error).__name__}: {error}"
                    )
                    status = self._response_status(error)
                    if status != 404:
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
        return status if isinstance(status, int) else None

    @staticmethod
    def _all_client_refusals(statuses: list[int | None]) -> bool:
        """True when every source refused with a 4xx client error.

        A ``None`` status (transport error, timeout) or a 5xx means a mirror
        might still serve the payload, so the caller must stay retryable.
        Non-int entries (defensive: mocked seams) also count as non-refusals.
        """
        return bool(statuses) and all(
            s is not None and isinstance(s, int) and 400 <= s < 500 for s in statuses
        )


_cdn_manager = CDNManager()


def get_cdn_manager() -> CDNManager:
    """Get global CDN manager instance."""
    return _cdn_manager
