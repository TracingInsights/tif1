"""Tests for CDN fallback behavior."""

from unittest.mock import patch

import pytest

from tif1.cdn import CDNManager, CDNSource
from tif1.exceptions import DataNotFoundError, NetworkError


class _StubConfig:
    def __init__(self, cdns):
        self._cdns = cdns

    def get(self, key, default=None):
        if key == "cdns":
            return self._cdns
        return default


class TestCDNManager:
    def test_uses_configured_cdns(self):
        configured_cdns = [
            "https://example.invalid/cdn-a",
            "https://example.invalid/cdn-b",
        ]

        with patch("tif1.config.get_config", return_value=_StubConfig(configured_cdns)):
            manager = CDNManager()

        assert [source.base_url for source in manager.sources] == configured_cdns

    def test_data_not_found_is_not_converted_to_network_error(self):
        manager = CDNManager()
        manager.sources = [
            CDNSource(name="Primary", base_url="https://example.invalid", priority=1)
        ]
        manager._failure_counts = {"Primary": 0}

        def _raise_not_found(_url):
            raise DataNotFoundError(year=2025, event="Test GP", session="Race")

        with pytest.raises(DataNotFoundError):
            manager.try_sources(2025, "Test%20GP", "Race", "drivers.json", _raise_not_found)

    def test_raises_network_error_when_no_sources_available(self):
        manager = CDNManager()
        manager.sources = []
        manager._failure_counts = {}

        with pytest.raises(NetworkError):
            manager.try_sources(2025, "Test%20GP", "Race", "drivers.json", lambda _url: {})


class TestCDNSourceFormatUrl:
    """Test CDNSource.format_url for different URL patterns."""

    def test_jsdelivr_url(self):
        source = CDNSource(name="jsDelivr", base_url="https://cdn.jsdelivr.net/gh/Archive")
        url = source.format_url(2024, "Bahrain", "Race", "drivers.json")
        assert url == "https://cdn.jsdelivr.net/gh/Archive/2024@main/Bahrain/Race/drivers.json"

    def test_generic_url(self):
        source = CDNSource(name="Custom", base_url="https://my-cdn.example.com/data")
        url = source.format_url(2024, "Bahrain", "Race", "drivers.json")
        assert url == "https://my-cdn.example.com/data/2024@main/Bahrain/Race/drivers.json"


class TestCDNManagerNameForUrl:
    """Test CDNManager._name_for_url static method."""

    def test_jsdelivr(self):
        assert CDNManager._name_for_url("https://cdn.jsdelivr.net/gh/Foo", 1) == "jsDelivr"

    def test_generic(self):
        assert CDNManager._name_for_url("https://example.com/cdn", 3) == "CDN 3"


class TestCDNManagerAddSource:
    """Test CDNManager.add_source."""

    def test_add_source_sorts_by_priority(self):
        with patch(
            "tif1.config.get_config", return_value=_StubConfig(["https://cdn.jsdelivr.net/gh/X"])
        ):
            manager = CDNManager()

        new_source = CDNSource(name="Early", base_url="https://early.example.com", priority=0)
        manager.add_source(new_source)
        assert manager.sources[0].name == "Early"
        assert manager._failure_counts["Early"] == 0


class TestCDNManagerGetSources:
    """Test CDNManager.get_sources filtering."""

    def test_filters_disabled_sources(self):
        with patch(
            "tif1.config.get_config", return_value=_StubConfig(["https://cdn.jsdelivr.net/gh/X"])
        ):
            manager = CDNManager()

        manager.sources[0].enabled = False
        assert len(manager.get_sources()) == 0

    def test_filters_failed_sources(self):
        with patch(
            "tif1.config.get_config", return_value=_StubConfig(["https://cdn.jsdelivr.net/gh/X"])
        ):
            manager = CDNManager()

        name = manager.sources[0].name
        manager._failure_counts[name] = 3
        assert len(manager.get_sources()) == 0


class TestCDNManagerFailureTracking:
    """Test mark_failure, mark_success, and reset."""

    def _make_manager(self):
        with patch(
            "tif1.config.get_config",
            return_value=_StubConfig(
                ["https://cdn.jsdelivr.net/gh/X", "https://example.com/cdn/Y"]
            ),
        ):
            return CDNManager()

    def test_mark_failure_increments(self):
        manager = self._make_manager()
        name = manager.sources[0].name
        manager.mark_failure(name)
        assert manager._failure_counts[name] == 1

    def test_mark_failure_disables_after_max(self, caplog):
        manager = self._make_manager()
        name = manager.sources[0].name
        with caplog.at_level("WARNING"):
            for _ in range(3):
                manager.mark_failure(name)
        assert manager._failure_counts[name] == 3
        assert len(manager.get_sources()) == 1
        assert any("disabled" in msg for msg in caplog.messages)

    def test_mark_success_resets(self):
        manager = self._make_manager()
        name = manager.sources[0].name
        manager.mark_failure(name)
        manager.mark_failure(name)
        manager.mark_success(name)
        assert manager._failure_counts[name] == 0

    def test_reset_all(self):
        manager = self._make_manager()
        for source in manager.sources:
            manager.mark_failure(source.name)
            manager.mark_failure(source.name)
        manager.reset()
        for source in manager.sources:
            assert manager._failure_counts[source.name] == 0


class TestCDNManagerTrySources:
    """Test try_sources with all sources failing."""

    def test_all_sources_fail_raises_network_error(self):
        with patch(
            "tif1.config.get_config", return_value=_StubConfig(["https://cdn.jsdelivr.net/gh/X"])
        ):
            manager = CDNManager()

        def _raise(_url):
            raise ConnectionError("down")

        with pytest.raises(NetworkError):
            manager.try_sources(2024, "Bahrain", "Race", "drivers.json", _raise)

    def test_success_on_second_source(self):
        with patch(
            "tif1.config.get_config",
            return_value=_StubConfig(
                ["https://cdn.jsdelivr.net/gh/X", "https://example.com/cdn/Y"]
            ),
        ):
            manager = CDNManager()

        call_count = 0

        def _fetch(url):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("first failed")
            return {"data": True}

        result = manager.try_sources(2024, "Bahrain", "Race", "drivers.json", _fetch)
        assert result == {"data": True}

    def test_data_not_found_propagates(self):
        with patch(
            "tif1.config.get_config",
            return_value=_StubConfig(
                ["https://cdn.jsdelivr.net/gh/X", "https://example.com/cdn/Y"]
            ),
        ):
            manager = CDNManager()

        def _raise(_url):
            raise DataNotFoundError(year=2024, event="Test", session="Race")

        with pytest.raises(DataNotFoundError):
            manager.try_sources(2024, "Test", "Race", "drivers.json", _raise)


class TestCDNManagerInit:
    """Test CDNManager.__init__ edge cases."""

    def test_invalid_non_https_urls_skipped(self, caplog):
        with (
            caplog.at_level("WARNING"),
            patch(
                "tif1.config.get_config",
                return_value=_StubConfig(
                    ["http://insecure.example.com", "https://valid.example.com"]
                ),
            ),
        ):
            manager = CDNManager()
        assert len(manager.sources) == 1
        assert manager.sources[0].base_url == "https://valid.example.com"
        assert any("Skipping invalid CDN URL" in msg for msg in caplog.messages)

    def test_raw_githubusercontent_url_skipped(self, caplog):
        with (
            caplog.at_level("WARNING"),
            patch(
                "tif1.config.get_config",
                return_value=_StubConfig(
                    ["https://raw.githubusercontent.com/X", "https://cdn.jsdelivr.net/gh/Y"]
                ),
            ),
        ):
            manager = CDNManager()
        assert len(manager.sources) == 1
        assert manager.sources[0].base_url == "https://cdn.jsdelivr.net/gh/Y"
        assert any("Skipping unsupported CDN URL" in msg for msg in caplog.messages)

    def test_duplicate_names_get_suffix(self):
        with patch(
            "tif1.config.get_config",
            return_value=_StubConfig(
                [
                    "https://cdn.jsdelivr.net/gh/A",
                    "https://cdn.jsdelivr.net/gh/B",
                ]
            ),
        ):
            manager = CDNManager()
        names = [s.name for s in manager.sources]
        assert len(set(names)) == 2

    def test_empty_list_falls_back_to_defaults(self):
        with patch("tif1.config.get_config", return_value=_StubConfig([])):
            manager = CDNManager()
        assert len(manager.sources) == 1
        assert manager.sources[0].name == "jsDelivr"

    def test_trailing_slash_stripped(self):
        with patch(
            "tif1.config.get_config", return_value=_StubConfig(["https://example.com/cdn/"])
        ):
            manager = CDNManager()
        assert manager.sources[0].base_url == "https://example.com/cdn"
