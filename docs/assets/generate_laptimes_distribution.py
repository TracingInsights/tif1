"""Generate lap times distribution visualization for documentation."""
import sys
sys.path.insert(0, '../../src')

import tif1
import seaborn as sns
import matplotlib.pyplot as plt

# Enable Matplotlib patches for plotting timedelta values and load
# tif1's dark color scheme
tif1.plotting.setup_mpl(mpl_timedelta_support=True, color_scheme='fastf1')

# Load the race session
race = tif1.get_session(2023, "Azerbaijan", 'R')
laps = race.laps

# Get all the laps for the point finishers only.
# Filter out slow laps (yellow flag, VSC, pitstops etc.)
# as they distort the graph axis.
point_finishers = race.drivers[:10]
print(f"Point finishers: {point_finishers}")

# Pick quick laps only (filter out slow laps)
driver_laps = laps[laps["Driver"].isin(point_finishers)].copy()

# Filter out slow laps - keep only laps within reasonable time
# Remove deleted laps and pit laps
driver_laps = driver_laps[~driver_laps["Deleted"]]
driver_laps = driver_laps[~driver_laps["PitOutTime"].notna()]
driver_laps = driver_laps[~driver_laps["PitInTime"].notna()]

# Remove outliers (laps > 110% of fastest lap per driver)
def filter_quick_laps(group):
    fastest = group["LapTime"].min()
    return group[group["LapTime"] < fastest * 1.10]

driver_laps = driver_laps.groupby("Driver", group_keys=False).apply(filter_quick_laps)
driver_laps = driver_laps.reset_index(drop=True)

# To plot the drivers by finishing order,
# we need to get their three-letter abbreviations in the finishing order.
finishing_order = [race.get_driver(i)["Abbreviation"] for i in point_finishers]
print(f"Finishing order: {finishing_order}")

# Create the figure
fig, ax = plt.subplots(figsize=(10, 5))

# Seaborn doesn't have proper timedelta support,
# so we have to convert timedelta to float (in seconds)
driver_laps["LapTime(s)"] = driver_laps["LapTime"].dt.total_seconds()

# First create the violin plots to show the distributions
sns.violinplot(data=driver_laps,
               x="Driver",
               y="LapTime(s)",
               hue="Driver",
               inner=None,
               density_norm="area",
               order=finishing_order,
               palette=tif1.plotting.get_driver_color_mapping(session=race),
               legend=False
               )

# Then use the swarm plot to show the actual laptimes
sns.swarmplot(data=driver_laps,
              x="Driver",
              y="LapTime(s)",
              order=finishing_order,
              hue="Compound",
              palette=tif1.plotting.get_compound_mapping(session=race),
              hue_order=["SOFT", "MEDIUM", "HARD"],
              linewidth=0,
              size=4,
              )

# Make the plot more aesthetic
ax.set_xlabel("Driver")
ax.set_ylabel("Lap Time (s)")
plt.suptitle("2023 Azerbaijan Grand Prix Lap Time Distributions")
sns.despine(left=True, bottom=True)

plt.tight_layout()

# Save the figure
output_path = 'laptimes_distribution.png'
plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='#1a1a1a')
print(f"Chart saved to {output_path}")
plt.close()
