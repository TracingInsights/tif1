"""Data exploration example - discover what's available."""

import tif1

print("=" * 60)
print("DATA EXPLORATION GUIDE")
print("=" * 60)

# 1. Explore available years and events
print("\n1. AVAILABLE EVENTS")
print("-" * 60)
events_2025 = tif1.get_events(2025)
print(f"2025 Season: {len(events_2025)} events")
for i, event in enumerate(events_2025["EventName"].head(5), 1):
    print(f"  {i}. {event}")
print(f"  ... and {len(events_2025) - 5} more")

# 2. Explore sessions for an event
print("\n2. AVAILABLE SESSIONS")
print("-" * 60)
sessions = tif1.get_sessions(2025, "Abu Dhabi Grand Prix")
print("Abu Dhabi Grand Prix sessions:")
for session in sessions:
    print(f"  • {session}")

# 3. Explore session data structure
print("\n3. SESSION DATA STRUCTURE")
print("-" * 60)
session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")

# Drivers
print(f"\nDrivers ({len(session.drivers_df)}):")
for _, d in session.drivers_df.head(5).iterrows():
    print(f"  {d['Driver']:3s} - {d['Team']}")
if len(session.drivers_df) > 5:
    print(f"  ... and {len(session.drivers_df) - 5} more")

# Laps data
laps = session.laps
print("\nLaps DataFrame:")
print(f"  Shape: {laps.shape}")
print(f"  Columns: {list(laps.columns)}")
print("\nSample data:")
print(laps.head(3))

# 4. Explore lap data columns
print("\n4. LAP DATA COLUMNS EXPLAINED")
print("-" * 60)
column_info = {
    "LapTime": "Lap time (Timedelta)",
    "LapTimeSeconds": "Lap time in seconds (float)",
    "LapNumber": "Lap number",
    "Compound": "Tire compound (SOFT, MEDIUM, HARD, etc.)",
    "Stint": "Stint number",
    "Sector1Time": "Sector 1 time (Timedelta)",
    "Sector2Time": "Sector 2 time (Timedelta)",
    "Sector3Time": "Sector 3 time (Timedelta)",
    "TyreLife": "Tire age in laps",
    "Position": "Track position",
    "TrackStatus": "Track status codes seen in the lap (1=clear, 2=yellow, 4=SC, 5=red, 6=VSC)",
    "IsPersonalBest": "Personal best lap flag",
    "Driver": "Driver code (3 letters)",
    "Team": "Team name",
}

for col, desc in column_info.items():
    if col in laps.columns:
        print(f"  {col:20s} - {desc}")

# 5. Explore telemetry data
print("\n5. TELEMETRY DATA STRUCTURE")
print("-" * 60)
ver = session.get_driver("VER")
ver_laps = ver.laps

if len(ver_laps) > 0:
    # Get the first lap
    first_lap = int(ver_laps["LapNumber"].iloc[0])
    lap = ver.get_lap(first_lap)
    telemetry = lap.telemetry

    print(f"Telemetry for VER lap {first_lap}:")
    print(f"  Shape: {telemetry.shape}")
    print(f"  Columns: {list(telemetry.columns)}")
    print("\nSample data:")
    print(telemetry.head(3))

# 6. Explore telemetry columns
print("\n6. TELEMETRY COLUMNS EXPLAINED")
print("-" * 60)
telemetry_info = {
    "Time": "Time from lap start (Timedelta)",
    "RPM": "Engine RPM",
    "Speed": "Speed in km/h",
    "nGear": "Gear number (0-8)",
    "Throttle": "Throttle position (0-100%)",
    "Brake": "Brake applied (True/False)",
    "DRS": "DRS flap (0=closed, 1=open)",
    "Distance": "Distance in meters from lap start",
    "RelativeDistance": "Relative distance (0-1 normalized)",
    "X": "X coordinate position (1/10 m)",
    "Y": "Y coordinate position (1/10 m)",
    "Z": "Z coordinate, elevation (1/10 m)",
    "AccelerationX": "Longitudinal acceleration (m/s²)",
    "AccelerationY": "Lateral acceleration (m/s²)",
    "AccelerationZ": "Vertical acceleration (m/s²)",
}

for col, desc in telemetry_info.items():
    if col in telemetry.columns:
        print(f"  {col:20s} - {desc}")

# 7. Data statistics
print("\n7. DATA STATISTICS")
print("-" * 60)
print(f"Total laps in session: {len(laps)}")
print(f"Unique drivers: {laps['Driver'].nunique()}")
print(f"Unique compounds: {list(laps['Compound'].dropna().unique())}")
print(f"Laps with a lap time: {laps['LapTime'].notna().sum()}")

print("\n" + "=" * 60)
print("💡 TIP: Use session.drivers_df to get driver information as a DataFrame!")
print("=" * 60)
