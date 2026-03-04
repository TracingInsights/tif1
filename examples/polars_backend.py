"""Polars lib example (2x faster for large datasets)."""

import time

import tif1

print("=" * 60)
print("POLARS vs PANDAS BACKEND COMPARISON")
print("=" * 60)

# Clear cache for fair comparison
cache = tif1.get_cache()
cache.clear()

# Test with Polars lib
print("\n1. LOADING WITH POLARS BACKEND")
print("-" * 60)
start = time.time()
session_polars = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", lib="polars")
laps_polars = session_polars.laps
polars_time = time.time() - start

print(f"Lib: {session_polars.lib}")
print(f"Total laps: {len(laps_polars)}")
print(f"DataFrame type: {type(laps_polars).__name__}")
print(f"Load time: {polars_time:.2f}s")

# Polars operations are faster
print("\nFastest laps by driver (using Polars):")
import polars as pl

fastest_by_driver = (
    laps_polars.group_by("Driver")
    .agg(
        [
            pl.col("LapTime").min().alias("fastest_time"),
            pl.col("LapNumber").count().alias("lap_count"),
        ]
    )
    .sort("fastest_time")
)
print(fastest_by_driver.head(10))

# Clear cache again
cache.clear()

# Test with Pandas lib
print("\n" + "=" * 60)
print("2. LOADING WITH PANDAS BACKEND")
print("-" * 60)
start = time.time()
session_pandas = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", lib="pandas")
laps_pandas = session_pandas.laps
pandas_time = time.time() - start

print(f"Lib: {session_pandas.lib}")
print(f"Total laps: {len(laps_pandas)}")
print(f"DataFrame type: {type(laps_pandas).__name__}")
print(f"Load time: {pandas_time:.2f}s")

# Pandas operations
print("\nFastest laps by driver (using Pandas):")
fastest_pandas = (
    laps_pandas.groupby("Driver", observed=True)
    .agg({"LapTime": "min", "LapNumber": "count"})
    .rename(columns={"LapTime": "fastest_time", "LapNumber": "lap_count"})
    .sort_values("fastest_time")
)
print(fastest_pandas.head(10))

# Performance comparison
print("\n" + "=" * 60)
print("PERFORMANCE COMPARISON")
print("=" * 60)
print(f"Polars load time: {polars_time:.2f}s")
print(f"Pandas load time: {pandas_time:.2f}s")
if polars_time > 0:
    speedup = pandas_time / polars_time
    print(f"Speedup: {speedup:.1f}x faster with Polars")

# Convert between backends if needed
print("\n" + "=" * 60)
print("CONVERTING BETWEEN BACKENDS")
print("=" * 60)
print("Converting Polars to Pandas...")
laps_converted = laps_polars.to_pandas()
print(f"✓ Converted to {type(laps_converted).__name__}")
print(f"Shape: {laps_converted.shape}")

print("\n💡 TIP: Use Polars for large datasets and complex aggregations!")
