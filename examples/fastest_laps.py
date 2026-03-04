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
    if hasattr(fastest_by_driver, "sort_values"):
        fastest_by_driver = fastest_by_driver.sort_values("LapTime")
    print(fastest_by_driver[["Driver", "Team", "LapTime", "Compound"]].head(10))

# Get overall fastest lap
print("\n2. OVERALL FASTEST LAP")
print("-" * 60)
overall_fastest = session.get_fastest_laps(by_driver=False)
if len(overall_fastest) > 0:
    if hasattr(overall_fastest, "iloc"):
        driver = overall_fastest["Driver"].iloc[0]
        team = overall_fastest["Team"].iloc[0]
        lap_time = overall_fastest["LapTime"].iloc[0]
        compound = overall_fastest["Compound"].iloc[0]
    else:
        driver = overall_fastest["Driver"][0]
        team = overall_fastest["Team"][0]
        lap_time = overall_fastest["LapTime"][0]
        compound = overall_fastest["Compound"][0]
    print(f"Driver: {driver} ({team})")
    print(f"Time: {lap_time:.3f}s")
    print(f"Compound: {compound}")

# Get driver's fastest lap
print("\n3. VERSTAPPEN'S FASTEST LAP")
print("-" * 60)
try:
    ver = session.get_driver("VER")
    ver_fastest = ver.get_fastest_lap()
    if len(ver_fastest) > 0:
        if hasattr(ver_fastest, "iloc"):
            lap_time = ver_fastest["LapTime"].iloc[0]
            lap_num = (
                ver_fastest["LapNumber"].iloc[0]
                if "LapNumber" in ver_fastest.columns
                else ver_fastest["lap"].iloc[0]
            )
            compound = (
                ver_fastest["Compound"].iloc[0]
                if "Compound" in ver_fastest.columns
                else ver_fastest["compound"].iloc[0]
            )
        else:
            lap_time = ver_fastest["LapTime"][0]
            lap_num = (
                ver_fastest["LapNumber"][0]
                if "LapNumber" in ver_fastest.columns
                else ver_fastest["lap"][0]
            )
            compound = (
                ver_fastest["Compound"][0]
                if "Compound" in ver_fastest.columns
                else ver_fastest["compound"][0]
            )
        print(f"Lap Number: {lap_num}")
        print(f"Time: {lap_time:.3f}s")
        print(f"Compound: {compound}")
except tif1.DriverNotFoundError:
    print("VER not in this session")

# Compare teammates
print("\n4. TEAMMATE COMPARISON")
print("-" * 60)
try:
    ham = session.get_driver("HAM")
    rus = session.get_driver("RUS")

    ham_fastest = ham.get_fastest_lap()
    rus_fastest = rus.get_fastest_lap()

    if len(ham_fastest) > 0 and len(rus_fastest) > 0:
        if hasattr(ham_fastest, "iloc"):
            ham_time = ham_fastest["LapTime"].iloc[0]
            rus_time = rus_fastest["LapTime"].iloc[0]
        else:
            ham_time = ham_fastest["LapTime"][0]
            rus_time = rus_fastest["LapTime"][0]

        delta = abs(ham_time - rus_time)
        faster = "HAM" if ham_time < rus_time else "RUS"

        print("Mercedes (HAM vs RUS):")
        print(f"  HAM: {ham_time:.3f}s")
        print(f"  RUS: {rus_time:.3f}s")
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
    if hasattr(fastest_by_driver, "iloc"):
        top3 = fastest_by_driver.head(3)
        for idx, row in top3.iterrows():
            print(f"{row['Driver']:3s} ({row['Team']:20s}): {row['LapTime']:.3f}s")
    else:
        top3 = fastest_by_driver.head(3)
        for i in range(len(top3)):
            print(f"{top3['Driver'][i]:3s} ({top3['Team'][i]:20s}): {top3['LapTime'][i]:.3f}s")
