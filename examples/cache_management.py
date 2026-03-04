"""Cache management example."""

import tif1

print("=" * 60)
print("CACHE MANAGEMENT")
print("=" * 60)

# Get cache instance
cache = tif1.get_cache()
print(f"\nCache location: {cache.cache_dir}")

# Check cache size before
print("\n" + "-" * 60)
print("CACHE STATUS (BEFORE)")
print("-" * 60)
cache_files = list(cache.cache_dir.glob("*.json.gz"))
print(f"Cached files: {len(cache_files)}")

if cache_files:
    total_size = sum(f.stat().st_size for f in cache_files)
    print(f"Total size: {total_size / 1024 / 1024:.2f} MB")
    print(f"Average file size: {total_size / len(cache_files) / 1024:.2f} KB")
else:
    print("Cache is empty")

# Load data (will cache)
print("\n" + "-" * 60)
print("LOADING SESSION (with cache enabled)")
print("-" * 60)
import time

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
cache_files_after = list(cache.cache_dir.glob("*.json.gz"))
print(f"Cached files: {len(cache_files_after)}")
if cache_files_after:
    total_size = sum(f.stat().st_size for f in cache_files_after)
    print(f"Total size: {total_size / 1024 / 1024:.2f} MB")

# Disable caching for a session
print("\n" + "-" * 60)
print("LOADING WITHOUT CACHE")
print("-" * 60)
start = time.time()
session3 = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", enable_cache=False)
laps3 = session3.laps
no_cache_time = time.time() - start
print(f"✓ Loaded {len(laps3)} laps in {no_cache_time:.2f}s (no cache)")

# Clear cache
print("\n" + "-" * 60)
print("CLEARING CACHE")
print("-" * 60)
cache.clear()
print("✓ Cache cleared!")

# Verify cache is empty
cache_files_final = list(cache.cache_dir.glob("*.json.gz"))
print(f"Cached files after clear: {len(cache_files_final)}")

print("\n" + "=" * 60)
print("💡 TIP: Enable cache for faster repeated access to the same data!")
print("=" * 60)
