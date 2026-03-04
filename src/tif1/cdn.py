"""CDN fallback system with multiple sources."""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .exceptions import DataNotFoundError, NetworkError

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

        jsDelivr supports automatic minification by appending .min before the extension.
        This can reduce file sizes by 20-40% for JSON files.
        """
        # jsDelivr minification: file.json -> file.min.json
        if self.use_minification and path.endswith(".json"):
            path = path.replace(".json", ".min.json")
        return f"{self.base_url}/{year}@main/{gp}/{session}/{path}"


class CDNManager:
    """Manage multiple CDN sources with fallback."""

    def __init__(self):
        from .config import get_config

        config = get_config()
        default_sources = [
            "https://cdn.jsdelivr.net/gh/TracingInsights",
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
            self.sources.append(
                CDNSource(
                    name=name,
                    base_url=base_url.rstrip("/"),
                    priority=i,
                    use_minification=use_minification,
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
            ]

        self._failure_counts = {source.name: 0 for source in self.sources}
        self._max_failures = 3

    @staticmethod
    def _name_for_url(base_url: str, index: int) -> str:
        if "jsdelivr" in base_url:
            return "jsDelivr"
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
        """Try fetching from CDN sources with fallback."""
        sources = self.get_sources()

        if not sources:
            raise NetworkError(url=f"{year}/{gp}/{session}/{path}", status_code=None)

        last_exception = None

        for source in sources:
            try:
                url = source.format_url(year, gp, session, path)
                logger.debug(f"Trying CDN: {source.name} - {url}")
                result = fetch_func(url)
                self.mark_success(source.name)
                return result
            except DataNotFoundError:
                # 404 means data genuinely doesn't exist for the requested resource.
                raise
            except Exception as e:
                logger.warning(f"CDN {source.name} failed: {e}")
                self.mark_failure(source.name)
                last_exception = e

        raise NetworkError(
            url=f"{year}/{gp}/{session}/{path}",
            status_code=getattr(getattr(last_exception, "response", None), "status_code", None),
        )


_cdn_manager = CDNManager()


def get_cdn_manager() -> CDNManager:
    """Get global CDN manager instance."""
    return _cdn_manager
