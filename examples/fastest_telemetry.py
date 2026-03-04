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
    if hasattr(fastest_tels, "groupby"):
        print(f"Drivers: {fastest_tels['Driver'].nunique()}")
    else:
        print(f"Drivers: {fastest_tels['Driver'].n_unique()}")

# Get telemetry for specific drivers only
print("\n3. TOP 3 DRIVERS TELEMETRY")
print("-" * 60)
top3_tels = session.get_fastest_laps_tels(by_driver=True, drivers=["VER", "HAM", "LEC"])
if len(top3_tels) > 0:
    print(f"Total telemetry points: {len(top3_tels)}")
    if hasattr(top3_tels, "groupby"):
        print(f"Drivers: {top3_tels['Driver'].unique()}")
    else:
        print(f"Drivers: {top3_tels['Driver'].unique().to_list()}")

# Get driver's fastest lap telemetry
print("\n4. VERSTAPPEN'S FASTEST LAP TELEMETRY")
print("-" * 60)
try:
    ver = session.get_driver("VER")
    ver_tel = ver.get_fastest_lap_tel()
    if len(ver_tel) > 0:
        print(f"Telemetry points: {len(ver_tel)}")
        print(ver_tel[["Time", "Speed", "Throttle", "nGear"]].head(10))

        # Max speed
        if hasattr(ver_tel, "iloc"):
            max_speed = ver_tel["Speed"].max()
        else:
            max_speed = ver_tel["Speed"].max()
        print(f"\nMax speed: {max_speed:.1f} km/h")
except tif1.DriverNotFoundError:
    print("VER not in this session")
