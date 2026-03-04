"""Example: Using SQLite cache lib instead of DuckDB."""

import tif1

# Method 1: Set via configuration file (~/.tif1rc)
# {
#   "cache_backend": "sqlite"
# }

# Method 2: Set via environment variable
# export TIF1_CACHE_BACKEND=sqlite

# Method 3: Set programmatically
config = tif1.get_config()
config.set("cache_backend", "sqlite")

# Now get_cache() will use SQLite
cache = tif1.get_cache()
print(f"Cache lib: {cache.lib}")
print(f"Cache location: {cache.db_path}")

# Use tif1 normally - it will use SQLite for caching
session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
print(f"\nDrivers: {len(session.drivers_df)}")
print(f"Laps shape: {session.laps.shape}")
