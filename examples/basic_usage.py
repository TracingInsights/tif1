"""Basic usage example for tif1."""

import tif1

# Get a session
session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")

# Get all drivers as DataFrame
print("=" * 50)
print("DRIVERS IN SESSION")
print("=" * 50)
print(session.drivers_df)

# Get laps for all drivers
laps = session.laps
print(f"\n{'=' * 50}")
print(f"ALL LAPS ({len(laps)} total)")
print("=" * 50)
print(laps[["Driver", "LapNumber", "LapTime", "Compound", "Stint"]].head(10))

# Get specific driver data
ver = session.get_driver("VER")
ver_laps = ver.laps
print(f"\n{'=' * 50}")
print(f"VER LAPS ({len(ver_laps)} total)")
print("=" * 50)
print(
    ver_laps[
        ["LapNumber", "LapTime", "Compound", "Sector1Time", "Sector2Time", "Sector3Time"]
    ].head()
)

# Get telemetry for a specific lap
lap_19 = ver.get_lap(19)
telemetry = lap_19.telemetry
print(f"\n{'=' * 50}")
print(f"VER LAP 19 TELEMETRY ({len(telemetry)} points)")
print("=" * 50)
print(telemetry[["Time", "Speed", "Throttle", "Brake", "RPM", "nGear"]].head(10))

# Show position (X/Y/Z in 1/10 m) and acceleration data if available
if "X" in telemetry.columns:
    print(f"\n{'=' * 50}")
    print("POSITION & ACCELERATION DATA")
    print("=" * 50)
    print(
        telemetry[["Time", "X", "Y", "Z", "AccelerationX", "AccelerationY", "AccelerationZ"]].head(
            10
        )
    )

# Show telemetry statistics
print(f"\n{'=' * 50}")
print("TELEMETRY STATISTICS")
print("=" * 50)
print(f"Max Speed: {telemetry['Speed'].max():.1f} km/h")
print(f"Max RPM: {telemetry['RPM'].max():.0f}")
print(f"Max Throttle: {telemetry['Throttle'].max():.1f}%")
if "AccelerationX" in telemetry.columns:
    # AccelerationX is longitudinal, AccelerationY is lateral (both in m/s²)
    print(f"Max Acceleration: {telemetry['AccelerationX'].max():.2f} m/s²")
    print(f"Max Braking: {telemetry['AccelerationX'].min():.2f} m/s²")
    print(f"Max Lateral: {telemetry['AccelerationY'].abs().max():.2f} m/s²")
