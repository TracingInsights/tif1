"""Generate throttle distance visualization for documentation."""

import matplotlib.pyplot as plt

import tif1

# Setup plotting
tif1.plotting.setup_mpl(color_scheme="fastf1")

# Load session
session = tif1.get_session(2023, "Bahrain Grand Prix", "Q")

# Calculate throttle distance for each driver
drivers = session.laps["Driver"].unique()
throttle_data = []

for driver in drivers:
    try:
        # Get fastest lap for driver
        fastest_lap = session.laps.pick_drivers(driver).pick_fastest()
        telemetry = fastest_lap.get_car_data().add_distance()

        # Calculate distance deltas
        telemetry["Distance_delta"] = telemetry["Distance"].diff()

        # Get circuit length
        circuit_length = telemetry["Distance"].max()

        # Filter for full throttle (>= 98%)
        full_throttle = telemetry[telemetry["Throttle"] >= 98]

        # Calculate percentage of lap at full throttle
        throttle_distance = full_throttle["Distance_delta"].sum()
        throttle_percentage = (throttle_distance / circuit_length) * 100

        throttle_data.append(
            {
                "Driver": driver,
                "Team": fastest_lap["Team"],
                "ThrottlePercentage": round(throttle_percentage, 2),
            }
        )
    except Exception:
        continue

# Sort by throttle percentage
throttle_data.sort(key=lambda x: x["ThrottlePercentage"], reverse=True)

# Map to minimum value to show relative differences
min_percentage = min(d["ThrottlePercentage"] for d in throttle_data)
for d in throttle_data:
    d["PercentageDiff"] = d["ThrottlePercentage"] - min_percentage

# Create visualization
fig, ax = plt.subplots(figsize=(12, 8))

drivers_list = [d["Driver"] for d in throttle_data]
percentages_diff = [d["PercentageDiff"] for d in throttle_data]
percentages_actual = [d["ThrottlePercentage"] for d in throttle_data]
colors = [tif1.plotting.get_team_color(d["Team"], session) for d in throttle_data]

# Create horizontal bar chart
bars = ax.barh(
    drivers_list, percentages_diff, color=colors, alpha=0.8, edgecolor="white", linewidth=1
)

# Add actual value labels
for i, (_driver, percentage) in enumerate(zip(drivers_list, percentages_actual)):
    ax.text(
        percentages_diff[i] + 0.1,
        i,
        f"{percentage:.1f}%",
        va="center",
        fontsize=10,
        fontweight="bold",
    )

# Styling
ax.set_xlabel("Distance at Full Throttle (relative difference)", fontsize=12, fontweight="bold")
ax.set_ylabel("Driver", fontsize=12, fontweight="bold")
ax.set_title(
    "2023 Bahrain GP Qualifying - Full Throttle Distance\n(% of lap distance at ≥98% throttle)",
    fontsize=14,
    fontweight="bold",
    pad=20,
)
ax.invert_yaxis()
ax.grid(axis="x", alpha=0.3, linestyle="--")
ax.set_xlim(left=0)

# Add explanation
fig.text(
    0.5,
    0.02,
    "Higher values indicate more time at full throttle (power-limited tracks)",
    ha="center",
    fontsize=9,
    style="italic",
    alpha=0.7,
)

plt.tight_layout()
plt.savefig("docs/assets/throttle_distance.png", dpi=300, bbox_inches="tight")
print("Generated: docs/assets/throttle_distance.png")
