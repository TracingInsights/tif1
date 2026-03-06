"""Generate speed traces comparison plot for documentation."""

import sys

sys.path.insert(0, "../../src")

import matplotlib.pyplot as plt

import tif1

# Setup plotting
tif1.plotting.setup_mpl(mpl_timedelta_support=True, color_scheme="fastf1")

# Load qualifying session
session = tif1.get_session(2023, "Spanish Grand Prix", "Q")

# Get fastest laps for two drivers
ver_lap = session.laps.pick_drivers("VER").pick_fastest()
ham_lap = session.laps.pick_drivers("HAM").pick_fastest()

# Get telemetry data with distance
ver_tel = ver_lap.get_car_data().add_distance()
ham_tel = ham_lap.get_car_data().add_distance()

# Get team colors
rbr_color = tif1.plotting.get_team_color(ver_lap["Team"], session=session)
mer_color = tif1.plotting.get_team_color(ham_lap["Team"], session=session)

# Create the plot
fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(ver_tel["Distance"], ver_tel["Speed"], color=rbr_color, label="VER", linewidth=2)
ax.plot(ham_tel["Distance"], ham_tel["Speed"], color=mer_color, label="HAM", linewidth=2)

ax.set_xlabel("Distance (m)", fontsize=12)
ax.set_ylabel("Speed (km/h)", fontsize=12)
ax.legend(fontsize=11)
ax.grid(color="w", which="major", axis="both", alpha=0.3)

plt.suptitle(
    f"Fastest Lap Speed Comparison\n{session.event['EventName']} {session.event.year} Qualifying",
    fontsize=14,
    fontweight="bold",
)

plt.tight_layout()

# Save the figure
output_path = "speed_traces.png"
plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
print(f"Chart saved to {output_path}")
plt.close()
