"""Cache management example."""

import time

import tif1


def cache_size_mb(cache):
    """Return the size of the SQLite cache files (database, WAL and SHM) in MB."""
    return sum(f.stat().st_size for f in cache.cache_dir.glob("cache.sqlite*")) / 1024 / 1024


print("=" * 60)
print("CACHE MANAGEMENT")
print("=" * 60)

# Get cache instance
cache = tif1.get_cache()
print(f"\nCache location: {cache.cache_dir}")
print(f"Cache database: {cache.db_path}")

# Check cache size before
print("\n" + "-" * 60)
print("CACHE STATUS (BEFORE)")
print("-" * 60)
print(f"Total size: {cache_size_mb(cache):.2f} MB")
print(f"Session cached: {cache.has_session_data(2025, 'Abu Dhabi Grand Prix', 'Practice 1')}")

# Load data (will cache)
print("\n" + "-" * 60)
print("LOADING SESSION (with cache enabled)")
print("-" * 60)
start = time.time()
session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
laps = session.laps
first_load_time = time.time() - start
print(f"✓ Loaded {len(laps)} laps in {first_load_time:.2f}s")

# Load again (from cache)
print("\n" + "-" * 60)
print("LOADING AGAIN (from cache)")
print("-" * 60)
start = time.time()
session2 = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
laps2 = session2.laps
cached_load_time = time.time() - start
print(f"✓ Loaded {len(laps2)} laps in {cached_load_time:.2f}s")
speedup = first_load_time / cached_load_time if cached_load_time > 0 else 0
print(f"Speedup: {speedup:.1f}x faster from cache")

# Check cache size after
print("\n" + "-" * 60)
print("CACHE STATUS (AFTER)")
print("-" * 60)
print(f"Session cached: {cache.has_session_data(2025, 'Abu Dhabi Grand Prix', 'Practice 1')}")

# Disable caching for a session
print("\n" + "-" * 60)
print("LOADING WITHOUT CACHE")
print("-" * 60)
start = time.time()
session3 = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", enable_cache=False)
laps3 = session3.laps
no_cache_time = time.time() - start
print(f"✓ Loaded {len(laps3)} laps in {no_cache_time:.2f}s (no cache)")

# Clear cache (this deletes every cached entry, for all sessions)
print("\n" + "-" * 60)
print("CLEARING CACHE")
print("-" * 60)
cache.clear()
print("✓ Cache cleared!")

# Verify the session is gone
print(
    "Session cached after clear: "
    f"{cache.has_session_data(2025, 'Abu Dhabi Grand Prix', 'Practice 1')}"
)

print("\n" + "=" * 60)
print("💡 TIP: Enable cache for faster repeated access to the same data!")
print("=" * 60)
