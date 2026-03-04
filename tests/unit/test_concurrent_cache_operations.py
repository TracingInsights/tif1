"""Tests for concurrent cache operations in async methods."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tif1.core import Session


class TestConcurrentCacheOperations:
    """Test that async methods use concurrent cache operations."""

    @pytest.mark.asyncio
    async def test_fetch_telemetry_batch_from_refs_async_uses_batch(self):
        """Test that _fetch_telemetry_batch_from_refs_async uses batch cache ops."""
        session = Session(2025, "Test GP", "Race", lib="pandas", enable_cache=True)

        # Mock cache with async methods
        mock_cache = MagicMock()
        mock_cache.get_telemetry_batch_async = AsyncMock(return_value={})

        fastest_refs = [
            ("VER", 10),
            ("HAM", 12),
            ("LEC", 15),
        ]

        with (
            patch("tif1.core.get_cache", return_value=mock_cache),
            patch.object(session, "_session_cache_available", return_value=True),
            patch.object(session, "_get_telemetry_payload", return_value=None),
        ):
            requests, lap_info, _tels = await session._fetch_telemetry_batch_from_refs_async(
                fastest_refs, skip_cache=False
            )

        # Verify batch call was made once
        assert mock_cache.get_telemetry_batch_async.call_count == 1

        # Verify all refs resulted in fetch requests (cache misses)
        assert len(requests) == 3
        assert len(lap_info) == 3
        assert lap_info == [("VER", 10), ("HAM", 12), ("LEC", 15)]

    @pytest.mark.asyncio
    async def test_fetch_telemetry_batch_from_refs_async_cache_hits(self):
        """Test that cache hits are processed correctly with batch operations."""
        session = Session(2025, "Test GP", "Race", lib="pandas", enable_cache=True)

        # Mock cache with some hits and some misses
        mock_cache = MagicMock()
        cached_data = {("VER", 10): {"Time": [0.1, 0.2], "Speed": [300, 310]}}
        mock_cache.get_telemetry_batch_async = AsyncMock(return_value=cached_data)

        fastest_refs = [
            ("VER", 10),  # Cache hit
            ("HAM", 12),  # Cache miss
            ("LEC", 15),  # Cache miss
        ]

        with patch("tif1.core.get_cache", return_value=mock_cache):
            with patch.object(session, "_session_cache_available", return_value=True):
                with patch.object(session, "_get_telemetry_payload", return_value=None):
                    with patch("tif1.core._create_telemetry_df") as mock_create_df:
                        mock_create_df.return_value = MagicMock()  # Mock DataFrame

                        (
                            requests,
                            lap_info,
                            tels,
                        ) = await session._fetch_telemetry_batch_from_refs_async(
                            fastest_refs, skip_cache=False
                        )

        # Verify batch call was made once
        assert mock_cache.get_telemetry_batch_async.call_count == 1

        # Verify cache hit resulted in telemetry DataFrame
        assert len(tels) == 1

        # Verify cache misses resulted in fetch requests
        assert len(requests) == 2
        assert lap_info == [("HAM", 12), ("LEC", 15)]

    @pytest.mark.asyncio
    async def test_fetch_telemetry_batch_from_refs_async_handles_cache_errors(self):
        """Test that cache errors are handled gracefully in batch operations."""
        session = Session(2025, "Test GP", "Race", lib="pandas", enable_cache=True)

        # Mock cache that raises errors
        mock_cache = MagicMock()
        mock_cache.get_telemetry_batch_async = AsyncMock(side_effect=RuntimeError("Cache error"))

        fastest_refs = [
            ("VER", 10),
            ("HAM", 12),
            ("LEC", 15),
        ]

        with patch("tif1.core.get_cache", return_value=mock_cache):
            with patch.object(session, "_session_cache_available", return_value=True):
                with patch.object(session, "_get_telemetry_payload", return_value=None):
                    # Should not raise, errors are caught
                    (
                        requests,
                        lap_info,
                        _tels,
                    ) = await session._fetch_telemetry_batch_from_refs_async(
                        fastest_refs, skip_cache=False
                    )

        # All refs should result in fetch requests (including the ones affected by error)
        assert len(requests) == 3
        assert len(lap_info) == 3

    @pytest.mark.asyncio
    async def test_fetch_telemetry_batch_async_uses_concurrent_cache(self):
        """Test that _fetch_telemetry_batch_async uses the batch cache method."""
        session = Session(2025, "Test GP", "Race", lib="pandas", enable_cache=True)

        # Create mock DataFrame with driver and lap data
        import pandas as pd

        fastest_laps = pd.DataFrame(
            {
                "Driver": ["VER", "HAM", "LEC"],
                "LapNumber": [10, 12, 15],
            }
        )

        mock_cache = MagicMock()
        mock_cache.get_telemetry_batch_async = AsyncMock(return_value={})

        with patch("tif1.core.get_cache", return_value=mock_cache):
            with patch.object(session, "_session_cache_available", return_value=True):
                with patch.object(session, "_get_telemetry_payload", return_value=None):
                    requests, _lap_info, _tels = await session._fetch_telemetry_batch_async(
                        fastest_laps, skip_cache=False
                    )

        # Verify batch cache operations were used
        assert mock_cache.get_telemetry_batch_async.call_count == 1
        assert len(requests) == 3

    @pytest.mark.asyncio
    async def test_get_fastest_laps_tels_async_uses_concurrent_cache(self, monkeypatch):
        """Test that get_fastest_laps_tels_async uses batch cache operations."""
        from tif1.config import get_config

        config = get_config()
        monkeypatch.setitem(config._config, "ultra_cold_start", False)

        session = Session(2025, "Test GP", "Race", lib="pandas", enable_cache=True)

        # Mock the dependencies
        mock_cache = MagicMock()
        mock_cache.get_telemetry_batch_async = AsyncMock(return_value={})

        with patch("tif1.core.get_cache", return_value=mock_cache):
            with patch.object(session, "_session_cache_available", return_value=True):
                with patch.object(session, "_get_telemetry_payload", return_value=None):
                    with patch.object(session, "_resolve_ultra_cold_mode", return_value=False):
                        with patch.object(
                            session, "_get_fastest_lap_refs_from_raw_async"
                        ) as mock_refs:
                            mock_refs.return_value = [("VER", 10), ("HAM", 12)]

                            with patch("tif1.core.fetch_multiple_async") as mock_fetch:
                                mock_fetch.return_value = [
                                    {"Time": [0.1], "Speed": [300]},
                                    {"Time": [0.2], "Speed": [310]},
                                ]

                                with patch("tif1.core._create_telemetry_df") as mock_create_df:
                                    import pandas as pd

                                    mock_create_df.return_value = pd.DataFrame({"Time": [0.1]})

                                    await session.get_fastest_laps_tels_async(
                                        by_driver=True, drivers=["VER", "HAM"]
                                    )

        # Verify batch cache operations were used
        assert mock_cache.get_telemetry_batch_async.call_count == 1

    @pytest.mark.asyncio
    async def test_concurrent_cache_operations_performance(self):
        """Test that batch cache operations are fast."""
        session = Session(2025, "Test GP", "Race", lib="pandas", enable_cache=True)

        # Mock cache with artificial delay
        mock_cache = MagicMock()

        async def mock_get_telemetry_batch_with_delay(year, gp, session_name, refs):
            await asyncio.sleep(0.1)  # Simulate I/O delay
            return {}

        mock_cache.get_telemetry_batch_async = AsyncMock(
            side_effect=mock_get_telemetry_batch_with_delay
        )

        fastest_refs = [
            ("VER", 10),
            ("HAM", 12),
            ("LEC", 15),
            ("NOR", 8),
            ("PER", 11),
        ]

        with patch("tif1.core.get_cache", return_value=mock_cache):
            with patch.object(session, "_session_cache_available", return_value=True):
                with patch.object(session, "_get_telemetry_payload", return_value=None):
                    start_time = asyncio.get_event_loop().time()

                    (
                        _requests,
                        _lap_info,
                        _tels,
                    ) = await session._fetch_telemetry_batch_from_refs_async(
                        fastest_refs, skip_cache=False
                    )

                    end_time = asyncio.get_event_loop().time()

        # Verify batch cache call was made
        assert mock_cache.get_telemetry_batch_async.call_count == 1

        # Verify call was fast (total time should be ~0.1s)
        total_time = end_time - start_time
        assert total_time < 0.3, f"Operations took {total_time}s, expected fast execution"
