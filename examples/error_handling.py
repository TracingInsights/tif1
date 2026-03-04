"""Error handling example."""

import logging

import tif1

# Enable debug logging to see what's happening
tif1.setup_logging(logging.INFO)

print("=" * 60)
print("ERROR HANDLING EXAMPLES")
print("=" * 60)

# 1. Handle invalid event
print("\n1. HANDLING INVALID EVENT")
print("-" * 60)
try:
    session = tif1.get_session(2025, "Invalid GP", "Practice 1")
    laps = session.laps
except tif1.DataNotFoundError as e:
    print(f"✗ DataNotFoundError: {e}")
    print("  → Check event name with tif1.get_events(2025)")

# 2. Handle invalid session
print("\n2. HANDLING INVALID SESSION")
print("-" * 60)
try:
    session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Invalid Session")
    laps = session.laps
except tif1.DataNotFoundError as e:
    print(f"✗ DataNotFoundError: {e}")
    print("  → Check session name with tif1.get_sessions(2025, 'Abu Dhabi Grand Prix')")

# 3. Handle driver not found
print("\n3. HANDLING DRIVER NOT FOUND")
print("-" * 60)
try:
    session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
    driver = session.get_driver("XXX")  # Invalid driver code
except tif1.DriverNotFoundError as e:
    print(f"✗ DriverNotFoundError: {e}")
    print("  → Check available drivers with session.drivers_df")

# 4. Handle lap not found
print("\n4. HANDLING LAP NOT FOUND")
print("-" * 60)
try:
    session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
    ver = session.get_driver("VER")
    lap = ver.get_lap(999)  # Lap number doesn't exist
except tif1.LapNotFoundError as e:
    print(f"✗ LapNotFoundError: {e}")
    print("  → Check available laps with driver.laps")

# 5. Proper error handling workflow
print("\n5. PROPER ERROR HANDLING WORKFLOW")
print("-" * 60)
try:
    # Get available events
    events = tif1.get_events(2025)
    print(f"✓ Found {len(events)} events for 2025")

    # Get available sessions
    sessions = tif1.get_sessions(2025, "Abu Dhabi Grand Prix")
    print(f"✓ Found {len(sessions)} sessions for Abu Dhabi GP")

    # Get session
    session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
    print("✓ Loaded session")

    # Check available drivers
    drivers_df = session.drivers_df
    print(f"✓ Found {len(drivers_df)} drivers")

    # Get specific driver
    if "VER" in drivers_df["Driver"].values:
        ver = session.get_driver("VER")
        print("✓ Found driver VER")

        # Check available laps
        ver_laps = ver.laps
        if len(ver_laps) > 0:
            print(f"✓ VER has {len(ver_laps)} laps")

            # Get first lap
            first_lap_num = (
                ver_laps["lap"].iloc[0] if hasattr(ver_laps, "iloc") else ver_laps["lap"][0]
            )
            lap = ver.get_lap(int(first_lap_num))
            telemetry = lap.telemetry
            print(f"✓ Loaded telemetry for lap {first_lap_num} ({len(telemetry)} points)")
        else:
            print("✗ No laps available for VER")
    else:
        print("✗ VER not in this session")

except tif1.TIF1Error as e:
    print(f"✗ Error: {e}")
except Exception as e:
    print(f"✗ Unexpected error: {e}")

# 6. Network error handling (simulated)
print("\n6. NETWORK ERROR HANDLING")
print("-" * 60)
print("Network errors are automatically retried (max 3 attempts)")
print("If all retries fail, a NetworkError is raised")
print("Example: tif1.NetworkError: Failed to fetch data from URL")

print("\n" + "=" * 60)
print("💡 TIP: Always validate inputs before making API calls!")
print("=" * 60)
