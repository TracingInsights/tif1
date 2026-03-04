#!/usr/bin/env python3
"""Example: Fast telemetry fetching with parallel requests."""

import time

import tif1

# Get a race session
session = tif1.get_session(2025, "Las Vegas Grand Prix", "Race")

print("=" * 60)
print("Fast Telemetry Fetching Example")
print("=" * 60)

# Example 1: Get overall fastest lap telemetry
print("\n1. Overall Fastest Lap Telemetry")
print("-" * 60)
start = time.time()
fastest_tel = session.get_fastest_lap_tel()
elapsed = time.time() - start
print(f"✓ Fetched in {elapsed:.2f}s")
print(f"  Shape: {fastest_tel.shape}")
print(f"  Columns: {list(fastest_tel.columns[:5])}...")

# Example 2: Get all drivers' fastest lap telemetry (parallel!)
print("\n2. All Drivers' Fastest Lap Telemetry (Parallel)")
print("-" * 60)
start = time.time()
all_fastest_tels = session.get_fastest_laps_tels(by_driver=True)
elapsed = time.time() - start
num_drivers = len(all_fastest_tels["Driver"].unique())
print(f"✓ Fetched {num_drivers} drivers in {elapsed:.2f}s")
print(f"  Average: {elapsed / num_drivers:.3f}s per driver")
print(f"  Total telemetry points: {len(all_fastest_tels):,}")

# Example 3: Get specific drivers' telemetry
print("\n3. Top 3 Drivers' Fastest Lap Telemetry")
print("-" * 60)
start = time.time()
top3_tels = session.get_fastest_laps_tels(by_driver=True, drivers=["VER", "HAM", "LEC"])
elapsed = time.time() - start
print(f"✓ Fetched 3 drivers in {elapsed:.2f}s")
print(f"  Drivers: {list(top3_tels['Driver'].unique())}")

# Example 4: Single driver fastest lap telemetry
print("\n4. Single Driver Fastest Lap Telemetry")
print("-" * 60)
start = time.time()
ver = session.get_driver("VER")
ver_tel = ver.get_fastest_lap_tel()
elapsed = time.time() - start
print(f"✓ Fetched VER in {elapsed:.2f}s")
print(f"  Max speed: {ver_tel['Speed'].max():.1f} km/h")
print(f"  Max RPM: {ver_tel['RPM'].max():.0f}")

# Example 5: Compare speeds across drivers
print("\n5. Speed Comparison Across Drivers")
print("-" * 60)
speed_comparison = (
    all_fastest_tels.groupby("Driver")["Speed"].max().sort_values(ascending=False).head(5)
)
print("Top 5 max speeds:")
for driver, speed in speed_comparison.items():
    print(f"  {driver}: {speed:.1f} km/h")

print("\n" + "=" * 60)
print("✅ All examples completed successfully!")
print("=" * 60)
