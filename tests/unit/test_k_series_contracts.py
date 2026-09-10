"""Tests for the K-series performance contracts.

- K9: all-4xx CDN exhaustion raises DataNotFoundError (no retry storm);
      non-4xx failures keep the retryable NetworkError.
- K3: zstd SQLite codec round-trip; legacy zlib rows stay readable.
- K4/K1: telemetry bulk fetch writes a single tier, deferred out of the
  fetch slots; ultra-cold sessions persist so the next process is warm.
"""

from __future__ import annotations

import asyncio
import zlib

import pytest

from tif1.async_fetch import fetch_multiple_async
from tif1.cache import Cache, _decode_sqlite_value, _encode_sqlite_value
from tif1.cdn import CDNManager
from tif1.exceptions import DataNotFoundError, NetworkError


def _manager_with_statuses(statuses: list[int | None]):
    """Build a CDNManager whose sources fail with the given HTTP statuses."""
    manager = CDNManager()
    sources = []
    for i, status in enumerate(statuses):
        src = type("Src", (), {})()
        src.name = f"src{i}"
        src.enabled = True
        src.priority = i
        src.format_url = lambda year, gp, session, path, _s=None: "https://x"  # noqa: ARG005
        src.fail_status = status
        sources.append(src)
    manager.sources = sources
    manager._failure_counts = {s.name: 0 for s in sources}

    def _fetch(source, url):
        async def _inner():
            status = source.fail_status
            if status == 404:
                raise DataNotFoundError(year=2026, event="GP", session="Race")
            err = NetworkError(url=url, status_code=status)
            if status is not None:
                err.response = type("R", (), {"status_code": status})()
            raise err

        return _inner()

    return manager, _fetch


class TestK9AllClientRefusal:
    def test_all_404_raises_data_not_found(self):
        manager, fetch = _manager_with_statuses([404, 404, 404])
        with pytest.raises(DataNotFoundError):
            asyncio.run(manager.try_sources_async(2026, "GP", "Race", "p.json", fetch))

    def test_403_plus_404_mix_raises_data_not_found(self):
        """The observed live mechanism: primary 403s, mirrors 404."""
        manager, fetch = _manager_with_statuses([403, 404, 404])
        with pytest.raises(DataNotFoundError):
            asyncio.run(manager.try_sources_async(2026, "GP", "Race", "p.json", fetch))

    def test_5xx_keeps_retryable_network_error(self):
        manager, fetch = _manager_with_statuses([503, 404, 404])
        with pytest.raises(NetworkError):
            asyncio.run(manager.try_sources_async(2026, "GP", "Race", "p.json", fetch))

    def test_transport_error_keeps_retryable_network_error(self):
        manager, fetch = _manager_with_statuses([None, 404, 404])
        with pytest.raises(NetworkError):
            asyncio.run(manager.try_sources_async(2026, "GP", "Race", "p.json", fetch))

    def test_sync_variant_all_4xx_raises_data_not_found(self):
        manager, _fetch = _manager_with_statuses([403, 404])

        def sync_fetch(url):
            err = NetworkError(url=url, status_code=403)
            err.response = type("R", (), {"status_code": 403})()
            raise err

        manager.sources = manager.sources[:1]
        with pytest.raises(DataNotFoundError):
            manager.try_sources(2026, "GP", "Race", "p.json", sync_fetch)


class TestK3ZstdCodec:
    def test_round_trip_large_blob(self):
        blob = b'{"speed": [' + b"1," * 9000 + b"1]}"
        assert len(blob) >= 4096
        stored = _encode_sqlite_value(blob)
        assert isinstance(stored, bytes)
        assert _decode_sqlite_value(stored) == blob

    def test_small_blob_stays_plain_text(self):
        stored = _encode_sqlite_value(b'{"a": 1}')
        assert stored == '{"a": 1}'
        assert _decode_sqlite_value(stored) == '{"a": 1}'

    def test_legacy_zlib_row_stays_readable(self):
        blob = b'{"speed": [' + b"2," * 9000 + b"2]}"
        legacy = zlib.compress(blob, 3)
        assert _decode_sqlite_value(legacy) == blob

    def test_cache_telemetry_round_trip(self, tmp_path):
        cache = Cache(tmp_path)
        payload = {"speed": [300.0] * 500}
        cache.set_telemetry(2026, "GP", "Race", "VER", 1, payload)
        # Clear memory tiers so the read exercises the SQLite tier.
        with cache._memory_cache_lock:
            cache._memory_telemetry_cache.clear()
            cache._parsed_telemetry_cache.clear()
        assert cache.get_telemetry(2026, "GP", "Race", "VER", 1) == payload


class TestK4K1DeferredSingleTierWrites:
    def test_bulk_fetch_defers_and_flushes_single_tier(self, tmp_path, monkeypatch):
        cache = Cache(tmp_path)
        calls: list[str] = []

        real_set_telemetry = cache.set_telemetry
        real_set_raw = cache.set_raw

        def counting_set_telemetry(*args, **kwargs):
            calls.append("telemetry")
            return real_set_telemetry(*args, **kwargs)

        def counting_set_raw(*args, **kwargs):
            calls.append("raw")
            return real_set_raw(*args, **kwargs)

        monkeypatch.setattr(cache, "set_telemetry", counting_set_telemetry)
        monkeypatch.setattr(cache, "set_raw", counting_set_raw)
        monkeypatch.setattr("tif1.async_fetch.get_cache", lambda: cache)

        tel = {"speed": [300.0] * 200}

        class _Resp:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {"tel": tel}

        class _Session:
            def get(self, url, timeout=None):  # noqa: ARG002
                return _Resp()

        import tif1.async_fetch as af

        monkeypatch.setattr(af, "_get_async_session", lambda: _Session())
        monkeypatch.setattr(af, "_get_json_parse_executor", lambda: None)
        monkeypatch.setattr("tif1.http_session._track_request", lambda reused=True: None)  # noqa: ARG005
        monkeypatch.setattr(
            "tif1.config.get_config",
            lambda: type(
                "C",
                (),
                {
                    "get": classmethod(
                        lambda cls, key, default=None: {  # noqa: ARG005
                            "ci_mode": False,
                            "offline_mode": False,
                            "max_retries": 0,
                            "timeout": 5,
                            "max_concurrent_requests": 20,
                            "validate_data": False,
                            "validate_lap_times": False,
                            "validate_telemetry": False,
                            "retry_backoff_factor": 2.0,
                            "retry_jitter": False,
                            "max_retry_delay": 60.0,
                            "retry_jitter_max": 0.0,
                        }.get(key, default)
                    )
                },
            )(),
        )
        monkeypatch.setattr(
            "tif1.retry.get_circuit_breaker",
            lambda: type(
                "CB",
                (),
                {
                    "check_and_update_state": classmethod(lambda cls: (True, "closed")),  # noqa: ARG005
                    "record_success": classmethod(lambda cls: None),  # noqa: ARG005
                    "record_failure": classmethod(lambda cls: None),  # noqa: ARG005
                },
            )(),
        )

        results = asyncio.run(
            fetch_multiple_async(
                [(2026, "GP", "Race", "VER/1_tel.json")],
                use_cache=False,
                write_cache=True,
                validate_payload=False,
                defer_telemetry_writes=True,
            )
        )

        assert isinstance(results[0], dict)
        assert calls == ["telemetry"]
        assert cache.get_telemetry(2026, "GP", "Race", "VER", 1) == tel

    def test_ultra_cold_session_persists_for_next_process(self, tmp_path):
        """K1: default-config cold loads write the cache (read skip only)."""
        cache = Cache(tmp_path)
        from tif1.core import _cache_telemetry_payload

        data = {"tel": {"speed": [280.0] * 100}}
        assert _cache_telemetry_payload(cache, 2026, "GP", "Race", "VER/1_tel.json", data)
        assert cache.get_telemetry(2026, "GP", "Race", "VER", 1) == {"speed": [280.0] * 100}
