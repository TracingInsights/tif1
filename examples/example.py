"""Simple example usage of tif1."""

import tif1

# Get a session
session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")

# Get all drivers
print("Drivers:", session.drivers_df["Driver"].tolist())

# Get all laps
laps = session.laps
print(f"\nTotal laps: {len(laps)}")
print(laps[["Driver", "LapNumber", "LapTime", "Compound"]].head())

# Get specific driver
ver = session.get_driver("VER")
print(f"\nVER laps: {len(ver.laps)}")

# Get telemetry for a lap
lap = ver.get_lap(19)
telemetry = lap.telemetry
print(f"\nLap 19 telemetry: {len(telemetry)} points")
print(telemetry[["Time", "Speed", "Throttle", "Brake"]].head())
