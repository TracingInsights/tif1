"""Polars backend example: load laps as a polars DataFrame and compare with pandas."""

import time

import polars as pl

import tif1

print("=" * 60)
print("POLARS vs PANDAS BACKEND COMPARISON")
print("=" * 60)

# enable_cache=False makes both sessions fetch from the CDN (a cold load)
# without touching the persistent cache.

# Test with Polars lib
print("\n1. LOADING WITH POLARS BACKEND")
print("-" * 60)
start = time.time()
session_polars = tif1.get_session(
    2025, "Abu Dhabi Grand Prix", "Practice 1", lib="polars", enable_cache=False
)
laps_polars = session_polars.laps
polars_time = time.time() - start

print(f"Lib: {session_polars.lib}")
print(f"Total laps: {len(laps_polars)}")
print(f"DataFrame type: {type(laps_polars).__name__}")
print(f"Load time: {polars_time:.2f}s")

# Aggregate with the polars expression API
print("\nFastest laps by driver (using Polars):")
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

# Test with Pandas lib
print("\n" + "=" * 60)
print("2. LOADING WITH PANDAS BACKEND")
print("-" * 60)
start = time.time()
session_pandas = tif1.get_session(
    2025, "Abu Dhabi Grand Prix", "Practice 1", lib="pandas", enable_cache=False
)
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
    print(f"Pandas/Polars time ratio: {pandas_time / polars_time:.1f}")
print("(The first load also opens the HTTPS connection; timings vary per run.)")

# Convert between backends if needed
print("\n" + "=" * 60)
print("CONVERTING BETWEEN BACKENDS")
print("=" * 60)
print("Converting Polars to Pandas...")
laps_converted = laps_polars.to_pandas()
print(f"✓ Converted to {type(laps_converted).__name__}")
print(f"Shape: {laps_converted.shape}")

print("\n💡 TIP: Use Polars for large datasets and complex aggregations!")
