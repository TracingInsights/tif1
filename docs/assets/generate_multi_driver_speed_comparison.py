"""Generate multi-driver speed comparison visualization."""

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import LineCollection

import tif1

# Setup plotting with FastF1 colors
tif1.plotting.setup_mpl(color_scheme='fastf1')

# Configuration
year = 2023
event = "Bahrain"
session_type = "Q"
driver = "VER"
colormap = mpl.cm.plasma

# Load session
session = tif1.get_session(year, event, session_type)
weekend = session.event

# Get fastest lap telemetry
lap = session.laps.pick_driver(driver).pick_fastest()
telemetry = lap.get_car_data()

# Extract data for plotting
x = telemetry['X'].values
y = telemetry['Y'].values
color = telemetry['Speed'].values

# Create line segments for coloring
points = np.array([x, y]).T.reshape(-1, 1, 2)
segments = np.concatenate([points[:-1], points[1:]], axis=1)

# Create visualization with FastF1 styling
fig, ax = plt.subplots(sharex=True, sharey=True, figsize=(12, 6.75))
fig.suptitle(f'{weekend["EventName"]} {year} - {driver} - Speed', size=24, y=0.97, color='white')

# Adjust margins and turn off axis
plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.12)
ax.axis('off')
ax.set_aspect('equal', adjustable='datalim')

# Create background track line
ax.plot(x, y, color='black', linestyle='-', linewidth=16, zorder=0)

# Create a continuous norm to map from data points to colors
norm = plt.Normalize(color.min(), color.max())
lc = LineCollection(segments, cmap=colormap, norm=norm, linestyle='-', linewidth=5)

# Set the values used for colormapping
lc.set_array(color)

# Add line segments to plot
line = ax.add_collection(lc)

# Create color bar as legend
cbaxes = fig.add_axes([0.25, 0.05, 0.5, 0.05])
normlegend = mpl.colors.Normalize(vmin=color.min(), vmax=color.max())
legend = mpl.colorbar.ColorbarBase(cbaxes, norm=normlegend, cmap=colormap, orientation="horizontal")
legend.set_label('Speed (km/h)', color='white', fontsize=12)
legend.ax.xaxis.set_tick_params(color='white')
plt.setp(plt.getp(legend.ax.axes, 'xticklabels'), color='white')

# Save
plt.savefig("docs/assets/multi_driver_speed_comparison.png", dpi=300, bbox_inches="tight", facecolor='#1a1a1a')
print("Generated multi_driver_speed_comparison.png")
