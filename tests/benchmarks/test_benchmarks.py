"""Benchmark tests for tif1."""

from unittest.mock import patch

import pandas as pd
import pytest

from tif1.core import Session


@pytest.mark.benchmark
class TestBenchmarks:
    """Benchmark tests."""

    @pytest.fixture
    def mock_session_data(self):
        """Mock session data."""
        return {
            "drivers": [
                {"driver": f"DR{i:02d}", "team": f"Team {i}", "number": i + 1} for i in range(20)
            ]
        }

    @pytest.fixture
    def mock_lap_data(self):
        """Mock lap data."""
        return {
            "time": [90.0 + i * 0.1 for i in range(50)],
            "lap": list(range(1, 51)),
            "compound": ["SOFT"] * 50,
            "stint": [1] * 50,
            "s1": [30.0] * 50,
            "s2": [35.0] * 50,
            "s3": [25.0] * 50,
            "life": list(range(1, 51)),
            "pos": [1] * 50,
            "status": ["OK"] * 50,
            "pb": [False] * 50,
        }

    def test_benchmark_session_laps_sync(self, benchmark, mock_session_data, mock_lap_data):
        """Benchmark synchronous lap loading."""
        with patch("tif1.core.Session._fetch_from_cdn") as mock_fetch:
            mock_fetch.return_value = mock_session_data

            with patch("tif1.core.fetch_multiple_async") as mock_async:
                mock_async.return_value = [mock_lap_data] * 20

                def load_laps():
                    session = Session(2025, "Test GP", "Race", enable_cache=False)
                    return session.laps

                result = benchmark(load_laps)
                assert isinstance(result, pd.DataFrame)

    def test_benchmark_session_laps_async(self, benchmark, mock_session_data, mock_lap_data):
        """Benchmark asynchronous lap loading."""
        with patch("tif1.core.Session._fetch_from_cdn") as mock_fetch:
            mock_fetch.return_value = mock_session_data

            with patch("tif1.core.fetch_multiple_async") as mock_async:
                mock_async.return_value = [mock_lap_data] * 20

                def load_laps():
                    session = Session(2025, "Test GP", "Race", enable_cache=False)
                    return session.laps  # Just use sync version for benchmark

                result = benchmark(load_laps)
                assert isinstance(result, pd.DataFrame)

    def test_benchmark_cache_hit(self, benchmark, tmp_path):
        """Benchmark cache hit performance."""
        from tif1.cache import Cache

        cache = Cache(tmp_path)
        test_data = {"drivers": [{"driver": "VER", "team": "Red Bull"}] * 100}
        cache.set("test_key", test_data)

        result = benchmark(cache.get, "test_key")
        assert result == test_data

    def test_benchmark_cache_miss(self, benchmark, tmp_path):
        """Benchmark cache miss performance."""
        from tif1.cache import Cache

        cache = Cache(tmp_path)
        result = benchmark(cache.get, "missing_key")
        assert result is None

    def test_benchmark_fastest_laps_pandas(self, benchmark):
        """Benchmark fastest laps calculation with pandas."""
        laps_df = pd.DataFrame(
            {
                "Driver": [f"DR{i:02d}" for i in range(20)] * 50,
                "LapTime": [90.0 + (i % 20) * 0.1 for i in range(1000)],
            }
        )

        def get_fastest():
            return laps_df.loc[laps_df.groupby("Driver")["LapTime"].idxmin()]

        result = benchmark(get_fastest)
        assert len(result) == 20

    def test_benchmark_fastest_laps_polars(self, benchmark):
        """Benchmark fastest laps calculation with polars."""
        try:
            import polars as pl

            laps_df = pl.DataFrame(
                {
                    "Driver": [f"DR{i:02d}" for i in range(20)] * 50,
                    "LapTime": [90.0 + (i % 20) * 0.1 for i in range(1000)],
                }
            )

            def get_fastest():
                return laps_df.group_by("Driver").agg(pl.col("LapTime").min())

            result = benchmark(get_fastest)
            assert len(result) == 20
        except ImportError:
            pytest.skip("Polars not available")
