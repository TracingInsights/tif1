"""Generate position changes chart for race analysis tutorial."""

import sys

import matplotlib.pyplot as plt

sys.path.insert(0, "../../src")

import tif1

# Setup plotting with dark theme
tif1.plotting.setup_mpl(mpl_timedelta_support=False, color_scheme="fastf1")

print("Generating position changes chart...")

# Load the 2023 Bahrain Grand Prix race session
session = tif1.get_session(2023, 1, "R")
laps = session.laps

# Create the figure
fig, ax = plt.subplots(figsize=(8.0, 4.9))

# For each driver, plot their position over the race
for drv in laps["Driver"].unique():
    drv_laps = laps[laps["Driver"] == drv]

    # Get driver abbreviation and styling
    abb = drv_laps["Driver"].iloc[0]
    color = tif1.plotting.get_driver_color(driver=abb, session=session)

    # Plot position vs lap number
    ax.plot(drv_laps["LapNumber"], drv_laps["Position"], label=abb, color=color)

# Configure the plot
ax.set_ylim([20.5, 0.5])
ax.set_yticks([1, 5, 10, 15, 20])
ax.set_xlabel("Lap")
ax.set_ylabel("Position")

# Add legend outside plot area
ax.legend(bbox_to_anchor=(1.0, 1.02))
plt.tight_layout()

# Save the chart
plt.savefig("race_position_changes.png", dpi=150, bbox_inches="tight")
plt.close()

print("✓ Position changes chart saved as race_position_changes.png")
