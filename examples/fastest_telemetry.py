"""Fastest telemetry analysis example."""

import tif1

# Get session
session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Qualifying")

print("=" * 60)
print("FASTEST TELEMETRY ANALYSIS")
print("=" * 60)

# Get telemetry from overall fastest lap
print("\n1. OVERALL FASTEST LAP TELEMETRY")
print("-" * 60)
fastest_tel = session.get_fastest_lap_tel()
if len(fastest_tel) > 0:
    print(f"Telemetry points: {len(fastest_tel)}")
    print(fastest_tel[["Time", "Speed", "Throttle", "Brake"]].head(10))

# Get telemetry from fastest lap per driver
print("\n2. FASTEST TELEMETRY PER DRIVER")
print("-" * 60)
fastest_tels = session.get_fastest_laps_tels(by_driver=True)
if len(fastest_tels) > 0:
    print(f"Total telemetry points: {len(fastest_tels)}")
    print(f"Drivers: {fastest_tels['Driver'].nunique()}")

# Get telemetry for specific drivers only
print("\n3. SELECTED DRIVERS TELEMETRY (VER, HAM, LEC)")
print("-" * 60)
top3_tels = session.get_fastest_laps_tels(by_driver=True, drivers=["VER", "HAM", "LEC"])
if len(top3_tels) > 0:
    print(f"Total telemetry points: {len(top3_tels)}")
    print(f"Drivers: {list(top3_tels['Driver'].unique())}")

# Get driver's fastest lap telemetry
print("\n4. VERSTAPPEN'S FASTEST LAP TELEMETRY")
print("-" * 60)
try:
    ver = session.get_driver("VER")
    ver_tel = ver.get_fastest_lap_tel()
    if len(ver_tel) > 0:
        print(f"Telemetry points: {len(ver_tel)}")
        print(ver_tel[["Time", "Speed", "Throttle", "nGear"]].head(10))

        print(f"\nMax speed: {ver_tel['Speed'].max():.1f} km/h")
except tif1.DriverNotFoundError:
    print("VER not in this session")
