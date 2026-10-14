"""Example: The SQLite cache and how to choose its location."""

import tif1

# tif1 keeps its persistent cache in one SQLite database, cache.sqlite, in the
# cache directory. Set the directory in one of these ways (before the first
# get_cache() or session load):

# Method 1: Set via configuration file (~/.tif1rc or ./.tif1rc)
# {
#   "cache_dir": "/path/to/tif1-cache"
# }

# Method 2: Set via environment variable
# export TIF1_CACHE_DIR=/path/to/tif1-cache

# Method 3: Set programmatically
# tif1.get_config().set("cache_dir", "/path/to/tif1-cache")

cache = tif1.get_cache()
print(f"Cache directory: {cache.cache_dir}")
print(f"Cache database: {cache.db_path}")

# Use tif1 normally - payloads and processed frames are cached in SQLite
session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
print(f"\nDrivers: {len(session.drivers_df)}")
print(f"Laps shape: {session.laps.shape}")

print(f"\nSession in cache: {cache.has_session_data(2025, 'Abu Dhabi Grand Prix', 'Practice 1')}")
# The database runs in WAL mode, so count cache.sqlite-wal and -shm too
db_bytes = sum(f.stat().st_size for f in cache.cache_dir.glob("cache.sqlite*"))
print(f"Database size: {db_bytes / 1024 / 1024:.2f} MB")
