"""Fastest laps analysis example."""

import tif1

# Get session
session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Qualifying")

print("=" * 60)
print("FASTEST LAPS ANALYSIS")
print("=" * 60)

# Get fastest lap per driver
print("\n1. FASTEST LAP PER DRIVER")
print("-" * 60)
fastest_by_driver = session.get_fastest_laps(by_driver=True)
if len(fastest_by_driver) > 0:
    # Sort by lap time
    fastest_by_driver = fastest_by_driver.sort_values("LapTime")
    print(fastest_by_driver[["Driver", "Team", "LapTime", "Compound"]].head(10))

# Get overall fastest lap
# LapTime is a Timedelta; LapTimeSeconds holds the same time as a float.
print("\n2. OVERALL FASTEST LAP")
print("-" * 60)
overall_fastest = session.get_fastest_laps(by_driver=False)
if len(overall_fastest) > 0:
    fastest = overall_fastest.iloc[0]
    print(f"Driver: {fastest['Driver']} ({fastest['Team']})")
    print(f"Time: {fastest['LapTimeSeconds']:.3f}s")
    print(f"Compound: {fastest['Compound']}")

# Get driver's fastest lap
print("\n3. VERSTAPPEN'S FASTEST LAP")
print("-" * 60)
try:
    ver = session.get_driver("VER")
    ver_fastest = ver.get_fastest_lap()
    if len(ver_fastest) > 0:
        lap = ver_fastest.iloc[0]
        print(f"Lap Number: {int(lap['LapNumber'])}")
        print(f"Time: {lap['LapTimeSeconds']:.3f}s")
        print(f"Compound: {lap['Compound']}")
except tif1.DriverNotFoundError:
    print("VER not in this session")

# Compare teammates
print("\n4. TEAMMATE COMPARISON")
print("-" * 60)
try:
    ham = session.get_driver("HAM")
    lec = session.get_driver("LEC")

    ham_fastest = ham.get_fastest_lap()
    lec_fastest = lec.get_fastest_lap()

    if len(ham_fastest) > 0 and len(lec_fastest) > 0:
        ham_time = ham_fastest["LapTimeSeconds"].iloc[0]
        lec_time = lec_fastest["LapTimeSeconds"].iloc[0]

        delta = abs(ham_time - lec_time)
        faster = "HAM" if ham_time < lec_time else "LEC"

        print("Ferrari (HAM vs LEC):")
        print(f"  HAM: {ham_time:.3f}s")
        print(f"  LEC: {lec_time:.3f}s")
        print(f"  Delta: {delta:.3f}s ({faster} faster)")
except tif1.DriverNotFoundError as e:
    print(f"Driver not found: {e}")

# Filter by specific drivers
print("\n5. SPECIFIC DRIVERS (VER, HAM, LEC)")
print("-" * 60)
specific_drivers = session.get_fastest_laps(by_driver=True, drivers=["VER", "HAM", "LEC"])
if len(specific_drivers) > 0:
    print(specific_drivers[["Driver", "Team", "LapTime", "Compound"]])

# Top 3 comparison
print("\n6. TOP 3 COMPARISON")
print("-" * 60)
if len(fastest_by_driver) >= 3:
    top3 = fastest_by_driver.head(3)
    for _, row in top3.iterrows():
        print(f"{row['Driver']:3s} ({row['Team']:20s}): {row['LapTimeSeconds']:.3f}s")
