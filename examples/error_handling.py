"""Error handling example."""

import logging

import tif1

# Log retries, CDN fallbacks and name corrections at INFO level
tif1.setup_logging(logging.INFO)

print("=" * 60)
print("ERROR HANDLING EXAMPLES")
print("=" * 60)

# 1. Handle invalid event
# A misspelled event name is corrected to the closest event (a warning logs the
# correction). A name that resembles no event raises tif1.EventNotFoundError.
print("\n1. HANDLING INVALID EVENT")
print("-" * 60)
session = tif1.get_session(2025, "Abu Dabi", "Practice 1")
print(f"✓ 'Abu Dabi' corrected to {session.event_name}")

try:
    session = tif1.get_session(2025, "Invalid GP", "Race")
except tif1.EventNotFoundError as e:
    print(f"✗ EventNotFoundError: {e}")
    print("  → Check event names with tif1.get_events(2025)")

# A known circuit name for an event that was not held that season
# (EventNotInSeasonError is a subclass of EventNotFoundError)
try:
    session = tif1.get_session(2021, "Suzuka", "Race")
except tif1.EventNotInSeasonError as e:
    print(f"✗ EventNotInSeasonError: {e}")

# 2. Handle invalid session
print("\n2. HANDLING INVALID SESSION")
print("-" * 60)
try:
    session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Invalid Session")
except ValueError as e:
    print(f"✗ ValueError: {e}")
    print("  → Check session names with tif1.get_sessions(2025, 'Abu Dhabi Grand Prix')")

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
            first_lap_num = int(ver_laps["LapNumber"].iloc[0])
            lap = ver.get_lap(first_lap_num)
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
print("Failed requests are retried with backoff (max_retries attempts, default 3)")
print("and then fall back to the next CDN (jsDelivr, Hugging Face, StaticDelivr).")
print("If every CDN fails, a tif1.NetworkError is raised.")
print("If every CDN returns 404, a tif1.DataNotFoundError is raised.")

print("\n" + "=" * 60)
print("💡 TIP: Always validate inputs before making API calls!")
print("=" * 60)
