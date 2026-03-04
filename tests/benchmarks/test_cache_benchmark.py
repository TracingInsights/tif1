"""Benchmark comparison: File cache vs SQLite."""

import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

# Check for parquet support (requires pyarrow or fastparquet)
try:
    import pyarrow as pa  # noqa: F401

    HAS_PARQUET = True
except ImportError:
    try:
        import fastparquet  # noqa: F401

        HAS_PARQUET = True
    except ImportError:
        HAS_PARQUET = False


@pytest.fixture
def sample_laps_data():
    """Generate realistic lap data."""
    drivers = ["VER", "PER", "HAM", "RUS", "LEC", "SAI", "NOR", "PIA", "ALO", "STR"]
    data = []
    for pos, driver in enumerate(drivers, start=1):
        data.extend(
            {
                "driver": driver,
                "team": f"Team_{driver}",
                "lap": lap,
                "time": 90.0 + lap * 0.1,
                "compound": "SOFT" if lap < 20 else "MEDIUM",
                "stint": 1 if lap < 20 else 2,
                "s1": 30.0,
                "s2": 35.0,
                "s3": 25.0,
                "life": lap,
                "pos": pos,
                "status": "OK",
                "pb": lap == 10,
            }
            for lap in range(1, 51)
        )
    return pd.DataFrame(data)


@pytest.fixture
def sample_telemetry_data():
    """Generate realistic telemetry data."""
    return pd.DataFrame(
        {
            "time": [i * 0.01 for i in range(1000)],  # 1000 points
            "rpm": [15000 + i * 10 for i in range(1000)],
            "speed": [200 + i * 0.5 for i in range(1000)],
            "gear": [6] * 1000,
            "throttle": [100.0] * 1000,
            "brake": [0] * 1000,
            "drs": [1] * 1000,
        }
    )


@pytest.fixture
def temp_dirs():
    """Create temporary directories for each cache type."""
    file_dir = Path(tempfile.mkdtemp())
    sqlite_dir = Path(tempfile.mkdtemp())

    yield file_dir, sqlite_dir

    shutil.rmtree(file_dir, ignore_errors=True)
    shutil.rmtree(sqlite_dir, ignore_errors=True)


class FileCache:
    """File-based cache (current implementation)."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True)

    def save_laps(self, year: int, gp: str, session: str, df: pd.DataFrame):
        key = f"{year}_{gp}_{session}_laps"
        path = self.cache_dir / f"{key}.parquet"
        df.to_parquet(path, compression="gzip")

    def load_laps(self, year: int, gp: str, session: str):
        key = f"{year}_{gp}_{session}_laps"
        path = self.cache_dir / f"{key}.parquet"
        if path.exists():
            return pd.read_parquet(path)
        return None

    def save_telemetry(
        self, year: int, gp: str, session: str, driver: str, lap: int, df: pd.DataFrame
    ):
        key = f"{year}_{gp}_{session}_{driver}_{lap}_telemetry"
        path = self.cache_dir / f"{key}.parquet"
        df.to_parquet(path, compression="gzip")

    def load_telemetry(self, year: int, gp: str, session: str, driver: str, lap: int):
        key = f"{year}_{gp}_{session}_{driver}_{lap}_telemetry"
        path = self.cache_dir / f"{key}.parquet"
        if path.exists():
            return pd.read_parquet(path)
        return None


class SQLiteCache:
    """SQLite-based cache (like fastf1)."""

    def __init__(self, cache_dir: Path):
        import sqlite3

        self.conn = sqlite3.connect(str(cache_dir / "cache.db"))
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS laps (
                year INTEGER, gp TEXT, session TEXT,
                driver TEXT, team TEXT, lap INTEGER,
                time REAL, compound TEXT, stint INTEGER,
                s1 REAL, s2 REAL, s3 REAL,
                life INTEGER, pos INTEGER, status TEXT, pb INTEGER
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                year INTEGER, gp TEXT, session TEXT,
                driver TEXT, lap INTEGER,
                time REAL, rpm REAL, speed REAL,
                gear INTEGER, throttle REAL, brake INTEGER, drs INTEGER
            )
        """)
        self.conn.commit()

    def save_laps(self, year: int, gp: str, session: str, df: pd.DataFrame):
        df_copy = df.copy()
        df_copy["year"] = year
        df_copy["gp"] = gp
        df_copy["session"] = session
        self.conn.execute(
            "DELETE FROM laps WHERE year = ? AND gp = ? AND session = ?", (year, gp, session)
        )
        df_copy.to_sql("laps", self.conn, if_exists="append", index=False)
        self.conn.commit()

    def load_laps(self, year: int, gp: str, session: str):
        result = pd.read_sql_query(
            "SELECT driver, team, lap, time, compound, stint, s1, s2, s3, life, pos, status, pb "
            "FROM laps WHERE year = ? AND gp = ? AND session = ?",
            self.conn,
            params=(year, gp, session),
        )
        return result if not result.empty else None

    def save_telemetry(
        self, year: int, gp: str, session: str, driver: str, lap: int, df: pd.DataFrame
    ):
        df_copy = df.copy()
        df_copy["year"] = year
        df_copy["gp"] = gp
        df_copy["session"] = session
        df_copy["driver"] = driver
        df_copy["lap"] = lap
        self.conn.execute(
            "DELETE FROM telemetry WHERE year = ? AND gp = ? AND session = ? AND driver = ? AND lap = ?",
            (year, gp, session, driver, lap),
        )
        df_copy.to_sql("telemetry", self.conn, if_exists="append", index=False)
        self.conn.commit()

    def load_telemetry(self, year: int, gp: str, session: str, driver: str, lap: int):
        result = pd.read_sql_query(
            "SELECT time, rpm, speed, gear, throttle, brake, drs "
            "FROM telemetry WHERE year = ? AND gp = ? AND session = ? AND driver = ? AND lap = ?",
            self.conn,
            params=(year, gp, session, driver, lap),
        )
        return result if not result.empty else None


@pytest.mark.benchmark
class TestCacheBenchmark:
    """Benchmark different cache implementations."""

    @pytest.mark.skipif(not HAS_PARQUET, reason="pyarrow or fastparquet required")
    def test_file_cache_write_laps(self, benchmark, temp_dirs, sample_laps_data):
        """Benchmark file cache write performance for laps."""
        cache = FileCache(temp_dirs[0])
        benchmark(cache.save_laps, 2025, "Test_GP", "Race", sample_laps_data)

    def test_sqlite_cache_write_laps(self, benchmark, temp_dirs, sample_laps_data):
        """Benchmark SQLite cache write performance for laps."""
        cache = SQLiteCache(temp_dirs[1])
        benchmark(cache.save_laps, 2025, "Test_GP", "Race", sample_laps_data)

    @pytest.mark.skipif(not HAS_PARQUET, reason="pyarrow or fastparquet required")
    def test_file_cache_read_laps(self, benchmark, temp_dirs, sample_laps_data):
        """Benchmark file cache read performance for laps."""
        cache = FileCache(temp_dirs[0])
        cache.save_laps(2025, "Test_GP", "Race", sample_laps_data)
        benchmark(cache.load_laps, 2025, "Test_GP", "Race")

    def test_sqlite_cache_read_laps(self, benchmark, temp_dirs, sample_laps_data):
        """Benchmark SQLite cache read performance for laps."""
        cache = SQLiteCache(temp_dirs[1])
        cache.save_laps(2025, "Test_GP", "Race", sample_laps_data)
        benchmark(cache.load_laps, 2025, "Test_GP", "Race")

    @pytest.mark.skipif(not HAS_PARQUET, reason="pyarrow or fastparquet required")
    def test_file_cache_write_telemetry(self, benchmark, temp_dirs, sample_telemetry_data):
        """Benchmark file cache write performance for telemetry."""
        cache = FileCache(temp_dirs[0])
        benchmark(cache.save_telemetry, 2025, "Test_GP", "Race", "VER", 10, sample_telemetry_data)

    def test_sqlite_cache_write_telemetry(self, benchmark, temp_dirs, sample_telemetry_data):
        """Benchmark SQLite cache write performance for telemetry."""
        cache = SQLiteCache(temp_dirs[1])
        benchmark(cache.save_telemetry, 2025, "Test_GP", "Race", "VER", 10, sample_telemetry_data)

    @pytest.mark.skipif(not HAS_PARQUET, reason="pyarrow or fastparquet required")
    def test_file_cache_read_telemetry(self, benchmark, temp_dirs, sample_telemetry_data):
        """Benchmark file cache read performance for telemetry."""
        cache = FileCache(temp_dirs[0])
        cache.save_telemetry(2025, "Test_GP", "Race", "VER", 10, sample_telemetry_data)
        benchmark(cache.load_telemetry, 2025, "Test_GP", "Race", "VER", 10)

    def test_sqlite_cache_read_telemetry(self, benchmark, temp_dirs, sample_telemetry_data):
        """Benchmark SQLite cache read performance for telemetry."""
        cache = SQLiteCache(temp_dirs[1])
        cache.save_telemetry(2025, "Test_GP", "Race", "VER", 10, sample_telemetry_data)
        benchmark(cache.load_telemetry, 2025, "Test_GP", "Race", "VER", 10)

    @pytest.mark.skipif(not HAS_PARQUET, reason="pyarrow or fastparquet required")
    def test_cache_size_comparison(self, temp_dirs, sample_laps_data, sample_telemetry_data):
        """Compare cache sizes on disk."""
        file_cache = FileCache(temp_dirs[0])
        sqlite_cache = SQLiteCache(temp_dirs[1])

        # Save same data to all caches
        for cache in [file_cache, sqlite_cache]:
            cache.save_laps(2025, "Test_GP", "Race", sample_laps_data)
            for driver in ["VER", "HAM", "LEC"]:
                cache.save_telemetry(2025, "Test_GP", "Race", driver, 10, sample_telemetry_data)

        # Calculate sizes
        def get_dir_size(path):
            return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

        file_size = get_dir_size(temp_dirs[0])
        sqlite_size = get_dir_size(temp_dirs[1])

        print(f"\n{'=' * 60}")
        print("Cache Size Comparison:")
        print(f"{'=' * 60}")
        print(f"File Cache:   {file_size:>10,} bytes ({file_size / 1024:.2f} KB)")
        print(f"SQLite Cache: {sqlite_size:>10,} bytes ({sqlite_size / 1024:.2f} KB)")
        print(f"{'=' * 60}")
        print(f"SQLite vs File: {(sqlite_size / file_size - 1) * 100:+.1f}%")
        print(f"{'=' * 60}\n")
