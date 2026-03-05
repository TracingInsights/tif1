"""Generate driver lap times scatter plot for documentation."""
import sys
sys.path.insert(0, '../../src')

import tif1
import seaborn as sns
import matplotlib.pyplot as plt

# Setup plotting
tif1.plotting.setup_mpl(mpl_timedelta_support=True, color_scheme='fastf1')

# Load race session
race = tif1.get_session(2023, "Azerbaijan", 'R')
laps = race.laps

# Get driver laps and filter
driver_laps = laps[laps["Driver"] == "ALO"].copy()
fastest_lap = driver_laps["LapTime"].min()
driver_laps = driver_laps[driver_laps["LapTime"] < fastest_lap * 1.07]
driver_laps = driver_laps[~driver_laps["Deleted"]].reset_index(drop=True)

# Create scatter plot
fig, ax = plt.subplots(figsize=(10, 8))

sns.scatterplot(
    data=driver_laps,
    x="LapNumber",
    y="LapTime",
    ax=ax,
    hue="Compound",
    palette=tif1.plotting.get_compound_mapping(session=race),
    s=80,
    linewidth=0,
    legend='auto'
)

# Enhance plot
ax.set_xlabel("Lap Number", fontsize=12)
ax.set_ylabel("Lap Time", fontsize=12)
ax.invert_yaxis()
plt.suptitle("Alonso Lap Times in the 2023 Azerbaijan Grand Prix", 
             fontsize=14, fontweight='bold')
plt.grid(color='w', which='major', axis='both', alpha=0.3)
sns.despine(left=True, bottom=True)

plt.tight_layout()

# Save the figure
output_path = 'driver_laptimes_example.png'
plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='#1a1a1a')
print(f"Chart saved to {output_path}")
plt.close()
