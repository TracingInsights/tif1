"""Generate downforce levels visualization for documentation."""

import matplotlib.pyplot as plt
import tif1

# Setup plotting
tif1.plotting.setup_mpl(color_scheme='fastf1')

# Load session
session = tif1.get_session(2023, 'Monaco Grand Prix', 'Q')

# Calculate downforce metric for each driver
drivers = session.laps['Driver'].unique()
downforce_data = []

for driver in drivers:
    try:
        # Get fastest lap for driver
        fastest_lap = session.laps.pick_drivers(driver).pick_fastest()
        telemetry = fastest_lap.get_car_data().add_distance()
        
        # Calculate metrics
        max_speed = telemetry['Speed'].max()
        lap_time = fastest_lap['LapTime'].total_seconds()
        circuit_length = telemetry['Distance'].max()
        avg_speed = (circuit_length / lap_time) * 3.6  # km/h
        
        # Downforce metric: ratio of average to max speed
        # Higher ratio = more downforce (better cornering, lower top speed)
        downforce_metric = (avg_speed / max_speed) * 100
        
        downforce_data.append({
            'Driver': driver,
            'Team': fastest_lap['Team'],
            'Metric': downforce_metric,
            'MaxSpeed': max_speed,
            'AvgSpeed': avg_speed
        })
    except Exception:
        continue

# Sort by downforce metric
downforce_data.sort(key=lambda x: x['Metric'], reverse=True)

# Map to minimum value to make differences more apparent
min_metric = min(d['Metric'] for d in downforce_data)
for d in downforce_data:
    d['MetricDiff'] = d['Metric'] - min_metric

# Create visualization
fig, ax = plt.subplots(figsize=(12, 8))

drivers_list = [d['Driver'] for d in downforce_data]
metrics_diff = [d['MetricDiff'] for d in downforce_data]
metrics_actual = [d['Metric'] for d in downforce_data]
colors = [tif1.plotting.get_team_color(d['Team'], session) for d in downforce_data]

# Create horizontal bar chart with mapped values
bars = ax.barh(drivers_list, metrics_diff, color=colors, alpha=0.8, edgecolor='white', linewidth=1)

# Add actual value labels
for i, (driver, metric) in enumerate(zip(drivers_list, metrics_actual)):
    ax.text(metrics_diff[i] + 0.02, i, f'{metric:.2f}', va='center', fontsize=10, fontweight='bold')

# Styling
ax.set_xlabel('Downforce Level (relative difference)', fontsize=12, fontweight='bold')
ax.set_ylabel('Driver', fontsize=12, fontweight='bold')
ax.set_title('2023 Monaco GP Qualifying - Downforce Levels\n'
             '(Avg Speed / Max Speed × 100)', 
             fontsize=14, fontweight='bold', pad=20)
ax.invert_yaxis()
ax.grid(axis='x', alpha=0.3, linestyle='--')
ax.set_xlim(left=0)

# Add explanation
fig.text(0.5, 0.02, 
         'Higher values indicate more downforce setup (better cornering, lower top speeds)',
         ha='center', fontsize=9, style='italic', alpha=0.7)

plt.tight_layout()
plt.savefig('docs/assets/downforce_levels.png', dpi=300, bbox_inches='tight')
print("Generated: docs/assets/downforce_levels.png")
