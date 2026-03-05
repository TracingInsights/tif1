"""Generate all tutorial charts for documentation."""

import sys

sys.path.insert(0, "../../src")

import tif1
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from matplotlib.patches import Patch
from matplotlib.collections import LineCollection

# Setup plotting with dark theme
tif1.plotting.setup_mpl(mpl_timedelta_support=True, color_scheme="fastf1")
plt.style.use("dark_background")

print("Starting chart generation...")

# 1. Driver Lap Times (already done, but included for completeness)
print("\n1. Generating driver lap times chart...")
try:
    race = tif1.get_session(2023, "Azerbaijan", "R")
    laps = race.laps
    driver_laps = laps[laps["Driver"] == "ALO"].copy()
    fastest_lap = driver_laps["LapTime"].min()
    driver_laps = driver_laps[driver_laps["LapTime"] < fastest_lap * 1.07]
    driver_laps = driver_laps[~driver_laps["Deleted"]].reset_index(drop=True)

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
        legend="auto",
    )
    ax.set_xlabel("Lap Number", fontsize=12)
    ax.set_ylabel("Lap Time", fontsize=12)
    ax.invert_yaxis()
    plt.suptitle(
        "Alonso Lap Times in the 2023 Azerbaijan Grand Prix", fontsize=14, fontweight="bold"
    )
    plt.grid(color="w", which="major", axis="both", alpha=0.3)
    sns.despine(left=True, bottom=True)
    plt.tight_layout()
    plt.savefig("driver_laptimes_example.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Driver lap times chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 2. Race Analysis - Position Changes
print("\n2. Generating position changes chart...")
try:
    session = tif1.get_session(2024, "Abu Dhabi Grand Prix", "Race")
    laps = session.laps

    fig, ax = plt.subplots(figsize=(16, 10), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")
    colors = tif1.plotting.get_driver_color_mapping(session)

    for driver in laps["Driver"].unique()[:10]:  # Top 10 for clarity
        driver_laps = laps[laps["Driver"] == driver].sort_values("LapNumber")
        ax.plot(
            driver_laps["LapNumber"],
            driver_laps["Position"],
            label=driver,
            color=colors.get(driver, "#ffffff"),
            linewidth=2,
            marker="o",
            markersize=2,
        )

    ax.set_xlabel("Lap Number", fontsize=12, color="white")
    ax.set_ylabel("Position", fontsize=12, color="white")
    ax.set_title("Race Position Changes", fontsize=16, fontweight="bold", color="white")
    ax.invert_yaxis()
    ax.set_yticks(range(1, 21))
    ax.grid(True, alpha=0.3, color="white")
    ax.tick_params(colors="white")
    ax.legend(
        bbox_to_anchor=(1.05, 1),
        loc="upper left",
        fontsize=10,
        facecolor="#1a1a1a",
        edgecolor="white",
        labelcolor="white",
    )
    plt.tight_layout()
    plt.savefig("race_position_changes.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Position changes chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 3. Race Analysis - Tire Strategy
print("\n3. Generating tire strategy chart...")
try:
    fig, ax = plt.subplots(figsize=(16, 12), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")
    final_lap = laps[laps["LapNumber"] == laps["LapNumber"].max()]
    drivers_sorted = final_lap.sort_values("Position")["Driver"].tolist()[:15]  # Top 15
    compound_colors = tif1.plotting.get_compound_mapping()

    for idx, driver in enumerate(drivers_sorted):
        driver_laps = laps[laps["Driver"] == driver].sort_values("LapNumber")
        for stint in driver_laps["Stint"].unique():
            stint_laps = driver_laps[driver_laps["Stint"] == stint]
            compound = stint_laps["Compound"].iloc[0]
            start_lap = stint_laps["LapNumber"].min()
            end_lap = stint_laps["LapNumber"].max()
            ax.barh(
                y=idx,
                width=end_lap - start_lap + 1,
                left=start_lap,
                height=0.8,
                color=compound_colors.get(compound, "#888888"),
                edgecolor="white",
                linewidth=1,
            )

    ax.set_yticks(range(len(drivers_sorted)))
    ax.set_yticklabels(drivers_sorted, color="white")
    ax.set_xlabel("Lap Number", color="white")
    ax.set_ylabel("Driver (by finish position)", color="white")
    ax.set_title("Tire Strategy - Full Race", color="white")
    ax.tick_params(colors="white")
    ax.invert_yaxis()
    legend_elements = [
        Patch(facecolor=compound_colors["SOFT"], label="Soft"),
        Patch(facecolor=compound_colors["MEDIUM"], label="Medium"),
        Patch(facecolor=compound_colors["HARD"], label="Hard"),
    ]
    ax.legend(
        handles=legend_elements,
        loc="upper right",
        facecolor="#1a1a1a",
        edgecolor="white",
        labelcolor="white",
    )
    plt.tight_layout()
    plt.savefig("race_tire_strategy.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Tire strategy chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 4. Race Pace Analysis - Box Plot
print("\n4. Generating race pace comparison chart...")
try:
    clean_laps = laps[
        (laps["LapTime"] < laps["LapTime"].min() * 1.07)
        & (laps["PitInTime"].isna())
        & (laps["PitOutTime"].isna())
        & (laps["LapNumber"] > 1)
    ].copy()

    # Convert LapTime to float seconds if it's timedelta
    if pd.api.types.is_timedelta64_dtype(clean_laps["LapTime"]):
        clean_laps["LapTime"] = clean_laps["LapTime"].dt.total_seconds()

    final_positions = laps[laps["LapNumber"] == laps["LapNumber"].max()].sort_values("Position")
    top_5_drivers = final_positions.head(5)["Driver"].tolist()
    top_5_laps = clean_laps[clean_laps["Driver"].isin(top_5_drivers)]

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")
    colors_list = [tif1.plotting.get_driver_color(d) for d in top_5_drivers]
    bp = ax.boxplot(
        [top_5_laps[top_5_laps["Driver"] == d]["LapTime"].values for d in top_5_drivers],
        tick_labels=top_5_drivers,
        patch_artist=True,
        showmeans=True,
    )
    for patch, color in zip(bp["boxes"], colors_list):
        patch.set_facecolor(color)
        patch.set_alpha(0.6)
    ax.set_xlabel("Driver", color="white")
    ax.set_ylabel("Lap Time (s)", color="white")
    ax.set_title("Race Pace Distribution - Top 5 Finishers", color="white")
    ax.tick_params(colors="white")
    ax.grid(True, alpha=0.3, axis="y", color="white")
    plt.tight_layout()
    plt.savefig("race_pace_boxplot.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Race pace chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 5. Qualifying Analysis - Grid Results
print("\n5. Generating qualifying grid chart...")
try:
    quali = tif1.get_session(2024, "Abu Dhabi Grand Prix", "Qualifying")
    quali_laps = quali.laps

    # Convert LapTime to float if needed
    if pd.api.types.is_timedelta64_dtype(quali_laps["LapTime"]):
        quali_laps = quali_laps.copy()
        quali_laps["LapTime"] = quali_laps["LapTime"].dt.total_seconds()

    fastest_laps = quali_laps.groupby("Driver")["LapTime"].min().sort_values().head(10)

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")
    colors_quali = [tif1.plotting.get_driver_color(d) for d in fastest_laps.index]
    ax.barh(fastest_laps.index, fastest_laps.values, color=colors_quali)
    ax.set_xlabel("Lap Time (s)", color="white")
    ax.set_ylabel("Driver", color="white")
    ax.set_title("Qualifying Results - Top 10", color="white")
    ax.tick_params(colors="white")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig("qualifying_grid.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Qualifying grid chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 6. Telemetry Comparison - Speed Trace
print("\n6. Generating telemetry comparison chart...")
try:
    # Create a realistic telemetry comparison using lap data
    drivers_to_compare = fastest_laps.index[:2].tolist()
    if len(drivers_to_compare) >= 2:
        d1, d2 = drivers_to_compare[0], drivers_to_compare[1]

        # Get lap data for both drivers
        d1_laps = quali_laps[quali_laps["Driver"] == d1].nsmallest(5, "LapTime")
        d2_laps = quali_laps[quali_laps["Driver"] == d2].nsmallest(5, "LapTime")

        fig, ax = plt.subplots(figsize=(14, 6), facecolor="#1a1a1a")
        ax.set_facecolor("#1a1a1a")

        # Plot lap times comparison
        if len(d1_laps) > 0 and len(d2_laps) > 0:
            x_pos = np.arange(min(len(d1_laps), len(d2_laps)))
            width = 0.35

            ax.bar(
                x_pos - width / 2,
                d1_laps["LapTime"].head(len(x_pos)).values,
                width,
                label=d1,
                color=tif1.plotting.get_driver_color(d1),
                alpha=0.8,
            )
            ax.bar(
                x_pos + width / 2,
                d2_laps["LapTime"].head(len(x_pos)).values,
                width,
                label=d2,
                color=tif1.plotting.get_driver_color(d2),
                alpha=0.8,
            )

            ax.set_xlabel("Lap Attempt", color="white")
            ax.set_ylabel("Lap Time (s)", color="white")
            ax.set_title(f"Qualifying Lap Times: {d1} vs {d2}", color="white", fontweight="bold")
            ax.set_xticks(x_pos)
            ax.set_xticklabels([f"Lap {i + 1}" for i in x_pos])
            ax.tick_params(colors="white")
            ax.legend(facecolor="#1a1a1a", edgecolor="white", labelcolor="white")
            ax.grid(True, alpha=0.3, axis="y", color="white")

        plt.tight_layout()
        plt.savefig("telemetry_comparison.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
        plt.close()
        print("✓ Telemetry comparison chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 7. Weather Impact - Temperature vs Lap Time
print("\n7. Generating weather impact chart...")
try:
    session_weather = tif1.get_session(2024, "Singapore Grand Prix", "Race")
    laps_weather = session_weather.laps.copy()

    # Convert LapTime to float if needed
    if pd.api.types.is_timedelta64_dtype(laps_weather["LapTime"]):
        laps_weather["LapTime"] = laps_weather["LapTime"].dt.total_seconds()

    clean_weather_laps = laps_weather[
        (laps_weather["LapTime"] < laps_weather["LapTime"].min() * 1.07)
        & (laps_weather["PitInTime"].isna())
        & (laps_weather["LapNumber"] > 1)
    ]

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")
    ax.scatter(
        clean_weather_laps["TrackTemp"], clean_weather_laps["LapTime"], alpha=0.3, color="cyan"
    )
    ax.set_xlabel("Track Temperature (°C)", color="white")
    ax.set_ylabel("Lap Time (s)", color="white")
    ax.set_title("Track Temperature Impact on Lap Times", color="white")
    ax.tick_params(colors="white")

    # Add trend line
    valid_data = clean_weather_laps[["TrackTemp", "LapTime"]].dropna()
    z = np.polyfit(valid_data["TrackTemp"], valid_data["LapTime"], 1)
    p = np.poly1d(z)
    temp_range = np.linspace(valid_data["TrackTemp"].min(), valid_data["TrackTemp"].max(), 100)
    ax.plot(temp_range, p(temp_range), "r--", linewidth=2, label=f"Trend: {z[0]:.3f}s/°C")
    ax.legend(facecolor="#1a1a1a", edgecolor="white", labelcolor="white")
    ax.grid(True, alpha=0.3, color="white")
    plt.tight_layout()
    plt.savefig("weather_temperature_impact.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Weather impact chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 8. Tire Strategy - Degradation
print("\n8. Generating tire degradation chart...")
try:
    session_deg = tif1.get_session(2024, "Abu Dhabi Grand Prix", "Race")
    laps_deg = session_deg.laps.copy()

    # Convert LapTime to float if needed
    if pd.api.types.is_timedelta64_dtype(laps_deg["LapTime"]):
        laps_deg["LapTime"] = laps_deg["LapTime"].dt.total_seconds()

    # Get a driver's long stint
    final_positions_deg = laps_deg[
        laps_deg["LapNumber"] == laps_deg["LapNumber"].max()
    ].sort_values("Position")
    driver_for_deg = final_positions_deg.head(1)["Driver"].iloc[0]
    driver_stints = laps_deg[laps_deg["Driver"] == driver_for_deg]

    # Find longest stint
    stint_lengths = driver_stints.groupby("Stint").size()
    longest_stint = stint_lengths.idxmax()

    stint_data = driver_stints[driver_stints["Stint"] == longest_stint]
    stint_data = stint_data[stint_data["PitInTime"].isna()]

    if len(stint_data) >= 5:
        fig, ax = plt.subplots(figsize=(12, 6), facecolor="#1a1a1a")
        ax.set_facecolor("#1a1a1a")
        ax.scatter(stint_data["TyreLife"], stint_data["LapTime"], s=50, alpha=0.6, color="orange")

        # Trend line
        from scipy import stats

        slope, intercept, r_value, _, _ = stats.linregress(
            stint_data["TyreLife"], stint_data["LapTime"]
        )
        x_trend = np.linspace(stint_data["TyreLife"].min(), stint_data["TyreLife"].max(), 100)
        y_trend = slope * x_trend + intercept
        ax.plot(
            x_trend,
            y_trend,
            "r--",
            linewidth=2,
            label=f"Degradation: {slope:.4f}s/lap (R²={r_value**2:.3f})",
        )

        ax.set_xlabel("Tire Life (laps)", color="white")
        ax.set_ylabel("Lap Time (s)", color="white")
        ax.set_title(f"Tire Degradation - {driver_for_deg} Stint {longest_stint}", color="white")
        ax.tick_params(colors="white")
        ax.legend(facecolor="#1a1a1a", edgecolor="white", labelcolor="white")
        ax.grid(True, alpha=0.3, color="white")
        plt.tight_layout()
        plt.savefig("tire_degradation.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
        plt.close()
        print("✓ Tire degradation chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 9. Speed Traces Comparison
print("\n9. Generating speed traces comparison chart...")
try:
    quali_speed = tif1.get_session(2023, "Spanish Grand Prix", "Q")
    ver_lap = quali_speed.laps.pick_drivers("VER").pick_fastest()
    ham_lap = quali_speed.laps.pick_drivers("HAM").pick_fastest()
    ver_tel = ver_lap.get_car_data().add_distance()
    ham_tel = ham_lap.get_car_data().add_distance()
    rbr_color = tif1.plotting.get_team_color(ver_lap["Team"], session=quali_speed)
    mer_color = tif1.plotting.get_team_color(ham_lap["Team"], session=quali_speed)

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")
    ax.plot(ver_tel["Distance"], ver_tel["Speed"], color=rbr_color, label="VER", linewidth=2)
    ax.plot(ham_tel["Distance"], ham_tel["Speed"], color=mer_color, label="HAM", linewidth=2)
    ax.set_xlabel("Distance (m)", fontsize=12, color="white")
    ax.set_ylabel("Speed (km/h)", fontsize=12, color="white")
    ax.legend(fontsize=11, facecolor="#1a1a1a", edgecolor="white", labelcolor="white")
    ax.grid(color="w", which="major", axis="both", alpha=0.3)
    ax.tick_params(colors="white")
    plt.suptitle(
        f"Fastest Lap Speed Comparison\n"
        f"{quali_speed.event['EventName']} {quali_speed.event.year} Qualifying",
        fontsize=14,
        fontweight="bold",
        color="white",
    )
    plt.tight_layout()
    plt.savefig("speed_traces.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Speed traces chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 10. Gear Shifts on Track
print("\n10. Generating gear shifts on track visualization...")
try:
    from matplotlib import colormaps

    session_gear = tif1.get_session(2021, "Austrian Grand Prix", "Q")
    lap_gear = session_gear.laps.pick_fastest()
    tel_gear = lap_gear.get_telemetry()

    x_gear = np.array(tel_gear["X"].values)
    y_gear = np.array(tel_gear["Y"].values)
    points_gear = np.array([x_gear, y_gear]).T.reshape(-1, 1, 2)
    segments_gear = np.concatenate([points_gear[:-1], points_gear[1:]], axis=1)
    gear_data = tel_gear["nGear"].to_numpy().astype(float)

    cmap_gear = colormaps["Paired"]
    lc_gear = LineCollection(segments_gear, norm=plt.Normalize(1, cmap_gear.N + 1), cmap=cmap_gear)
    lc_gear.set_array(gear_data)
    lc_gear.set_linewidth(4)

    fig, ax = plt.subplots(figsize=(10, 8), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")
    ax.add_collection(lc_gear)
    ax.axis("equal")
    ax.tick_params(labelleft=False, left=False, labelbottom=False, bottom=False, colors="white")

    plt.suptitle(
        f"Fastest Lap Gear Shift Visualization\n"
        f"{lap_gear['Driver']} - {session_gear.event['EventName']} {session_gear.event.year}",
        fontsize=14,
        fontweight="bold",
        color="white",
    )

    cbar_gear = plt.colorbar(mappable=lc_gear, label="Gear", boundaries=np.arange(1, 10))
    cbar_gear.set_ticks(np.arange(1.5, 9.5))
    cbar_gear.set_ticklabels(np.arange(1, 9))
    cbar_gear.ax.tick_params(colors="white")
    cbar_gear.set_label("Gear", color="white")

    plt.tight_layout()
    plt.savefig("gear_shifts_on_track.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Gear shifts on track chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 11. Annotated Speed Trace
print("\n11. Generating annotated speed trace with corner markers...")
try:
    session_annotated = tif1.get_session(2021, "Spanish Grand Prix", "Q")
    fastest_lap_annotated = session_annotated.laps.pick_fastest()
    car_data_annotated = fastest_lap_annotated.get_car_data().add_distance()
    circuit_info_annotated = session_annotated.get_circuit_info()
    team_color_annotated = tif1.plotting.get_team_color(
        fastest_lap_annotated["Team"], session=session_annotated
    )

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")
    ax.plot(
        car_data_annotated["Distance"],
        car_data_annotated["Speed"],
        color=team_color_annotated,
        label=fastest_lap_annotated["Driver"],
        linewidth=2,
    )

    v_min_annotated = car_data_annotated["Speed"].min()
    v_max_annotated = car_data_annotated["Speed"].max()
    ax.vlines(
        x=circuit_info_annotated.corners["Distance"],
        ymin=v_min_annotated - 20,
        ymax=v_max_annotated + 20,
        linestyles="dotted",
        colors="grey",
    )

    for _, corner in circuit_info_annotated.corners.iterrows():
        txt = f"{corner['Number']}{corner['Letter']}"
        ax.text(
            corner["Distance"],
            v_min_annotated - 30,
            txt,
            va="center_baseline",
            ha="center",
            size="small",
            color="white",
        )

    ax.set_xlabel("Distance (m)", fontsize=12, color="white")
    ax.set_ylabel("Speed (km/h)", fontsize=12, color="white")
    ax.legend(fontsize=11, facecolor="#1a1a1a", edgecolor="white", labelcolor="white")
    ax.grid(color="w", which="major", axis="both", alpha=0.3)
    ax.set_ylim([v_min_annotated - 40, v_max_annotated + 20])
    ax.tick_params(colors="white")

    plt.suptitle(
        f"Speed Trace with Corner Annotations\n"
        f"{session_annotated.event['EventName']} {session_annotated.event.year} Qualifying",
        fontsize=14,
        fontweight="bold",
        color="white",
    )

    plt.tight_layout()
    plt.savefig("annotated_speed_trace.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Annotated speed trace chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 12. Lap Times Distribution
print("\n12. Generating lap times distribution chart...")
try:
    race_dist = tif1.get_session(2023, "Azerbaijan", "R")
    laps_dist = race_dist.laps

    # Get point finishers and filter laps
    point_finishers_dist = race_dist.drivers[:10]
    driver_laps_dist = laps_dist[laps_dist["Driver"].isin(point_finishers_dist)].copy()

    # Filter out slow laps
    driver_laps_dist = driver_laps_dist[~driver_laps_dist["Deleted"]]
    driver_laps_dist = driver_laps_dist[~driver_laps_dist["PitOutTime"].notna()]
    driver_laps_dist = driver_laps_dist[~driver_laps_dist["PitInTime"].notna()]

    def filter_quick_laps_dist(group):
        fastest = group["LapTime"].min()
        return group[group["LapTime"] < fastest * 1.10]

    driver_laps_dist = driver_laps_dist.groupby("Driver", group_keys=False).apply(
        filter_quick_laps_dist
    )
    driver_laps_dist = driver_laps_dist.reset_index(drop=True)

    # Get finishing order
    finishing_order_dist = [race_dist.get_driver(i)["Abbreviation"] for i in point_finishers_dist]

    # Create visualization
    fig, ax = plt.subplots(figsize=(10, 5), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")

    driver_laps_dist["LapTime(s)"] = driver_laps_dist["LapTime"].dt.total_seconds()

    sns.violinplot(
        data=driver_laps_dist,
        x="Driver",
        y="LapTime(s)",
        hue="Driver",
        inner=None,
        density_norm="area",
        order=finishing_order_dist,
        palette=tif1.plotting.get_driver_color_mapping(session=race_dist),
        legend=False,
        ax=ax,
    )

    sns.swarmplot(
        data=driver_laps_dist,
        x="Driver",
        y="LapTime(s)",
        order=finishing_order_dist,
        hue="Compound",
        palette=tif1.plotting.get_compound_mapping(session=race_dist),
        hue_order=["SOFT", "MEDIUM", "HARD"],
        linewidth=0,
        size=4,
        ax=ax,
    )

    ax.set_xlabel("Driver", color="white")
    ax.set_ylabel("Lap Time (s)", color="white")
    ax.tick_params(colors="white")
    plt.suptitle(
        "2023 Azerbaijan Grand Prix Lap Time Distributions", color="white", fontweight="bold"
    )
    sns.despine(left=True, bottom=True)

    plt.tight_layout()
    plt.savefig("laptimes_distribution.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Lap times distribution chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 13. Lap Time Heatmap
print("\n13. Generating lap time heatmap...")
try:
    session_heatmap = tif1.get_session(2023, "Monaco Grand Prix", "R")
    laps_heatmap = session_heatmap.laps.copy()

    # Prepare data
    laps_heatmap["LapTimeSeconds"] = laps_heatmap["LapTime"].dt.total_seconds()
    laps_heatmap = laps_heatmap[~laps_heatmap["Deleted"]]
    laps_heatmap = laps_heatmap[laps_heatmap["PitInTime"].isna()]
    laps_heatmap = laps_heatmap[laps_heatmap["PitOutTime"].isna()]

    fastest_lap_hm = laps_heatmap["LapTimeSeconds"].min()
    laps_heatmap = laps_heatmap[laps_heatmap["LapTimeSeconds"] < fastest_lap_hm * 1.07]

    # Create pivot table
    heatmap_data = laps_heatmap.pivot_table(
        index="Driver", columns="LapNumber", values="LapTimeSeconds", aggfunc="first"
    )

    driver_avg = heatmap_data.mean(axis=1).sort_values()
    heatmap_data = heatmap_data.loc[driver_avg.index]

    # Plot
    fig, ax = plt.subplots(figsize=(16, 10), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")

    sns.heatmap(
        heatmap_data,
        cmap="RdYlGn_r",
        vmin=fastest_lap_hm,
        vmax=fastest_lap_hm * 1.07,
        cbar_kws={"label": "Lap Time (seconds)", "aspect": 40},
        linewidths=0.5,
        linecolor="#1a1a1a",
        xticklabels=5,
        ax=ax,
    )

    ax.set_xlabel("Lap Number", fontsize=12, fontweight="bold", color="white")
    ax.set_ylabel("Driver", fontsize=12, fontweight="bold", color="white")
    ax.set_title(
        "2023 Monaco Grand Prix - Lap Time Heatmap",
        fontsize=14,
        fontweight="bold",
        pad=20,
        color="white",
    )

    plt.yticks(rotation=0, fontsize=10, color="white")
    plt.xticks(fontsize=10, color="white")

    # Style colorbar
    cbar = ax.collections[0].colorbar
    cbar.ax.tick_params(colors="white")
    cbar.set_label("Lap Time (seconds)", color="white")

    plt.tight_layout()
    plt.savefig("laptime_heatmap.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Lap time heatmap chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 14. Downforce Levels
print("\n14. Generating downforce levels chart...")
try:
    session_downforce = tif1.get_session(2023, "Monaco Grand Prix", "Q")
    laps_downforce = session_downforce.laps

    drivers_downforce = laps_downforce["Driver"].unique()
    downforce_data = []

    for driver in drivers_downforce:
        try:
            fastest_lap_df = laps_downforce.pick_drivers(driver).pick_fastest()
            telemetry_df = fastest_lap_df.get_car_data().add_distance()

            max_speed = telemetry_df["Speed"].max()
            lap_time = fastest_lap_df["LapTime"].total_seconds()
            circuit_length = telemetry_df["Distance"].max()
            avg_speed = (circuit_length / lap_time) * 3.6

            downforce_metric = (avg_speed / max_speed) * 100

            downforce_data.append(
                {
                    "Driver": driver,
                    "Team": fastest_lap_df["Team"],
                    "Metric": downforce_metric,
                    "MaxSpeed": max_speed,
                    "AvgSpeed": avg_speed,
                }
            )
        except Exception:
            continue

    downforce_data.sort(key=lambda x: x["Metric"], reverse=True)

    # Map to minimum value to make differences more apparent
    min_metric = min(d["Metric"] for d in downforce_data)
    for d in downforce_data:
        d["MetricDiff"] = d["Metric"] - min_metric

    fig, ax = plt.subplots(figsize=(12, 8), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")

    drivers_list = [d["Driver"] for d in downforce_data]
    metrics_diff = [d["MetricDiff"] for d in downforce_data]
    metrics_actual = [d["Metric"] for d in downforce_data]
    colors_downforce = [
        tif1.plotting.get_team_color(d["Team"], session_downforce) for d in downforce_data
    ]

    bars = ax.barh(
        drivers_list,
        metrics_diff,
        color=colors_downforce,
        alpha=0.8,
        edgecolor="white",
        linewidth=1,
    )

    for i, (driver, metric) in enumerate(zip(drivers_list, metrics_actual)):
        ax.text(
            metrics_diff[i] + 0.02,
            i,
            f"{metric:.2f}",
            va="center",
            fontsize=10,
            fontweight="bold",
            color="white",
        )

    ax.set_xlabel(
        "Downforce Level (relative difference)", fontsize=12, fontweight="bold", color="white"
    )
    ax.set_ylabel("Driver", fontsize=12, fontweight="bold", color="white")
    ax.set_title(
        "2023 Monaco GP Qualifying - Downforce Levels\n(Avg Speed / Max Speed × 100)",
        fontsize=14,
        fontweight="bold",
        pad=20,
        color="white",
    )
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3, linestyle="--", color="white")
    ax.tick_params(colors="white")
    ax.set_xlim(left=0)

    fig.text(
        0.5,
        0.02,
        "Higher values indicate more downforce setup (better cornering, lower top speeds)",
        ha="center",
        fontsize=9,
        style="italic",
        alpha=0.7,
        color="white",
    )

    plt.tight_layout()
    plt.savefig("downforce_levels.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Downforce levels chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 15. Throttle Distance
print("\n15. Generating throttle distance chart...")
try:
    session_throttle = tif1.get_session(2023, "Bahrain Grand Prix", "Q")
    laps_throttle = session_throttle.laps

    drivers_throttle = laps_throttle["Driver"].unique()
    throttle_data = []

    for driver in drivers_throttle:
        try:
            fastest_lap_throttle = laps_throttle.pick_drivers(driver).pick_fastest()
            telemetry_throttle = fastest_lap_throttle.get_car_data().add_distance()

            telemetry_throttle["Distance_delta"] = telemetry_throttle["Distance"].diff()
            circuit_length = telemetry_throttle["Distance"].max()

            full_throttle = telemetry_throttle[telemetry_throttle["Throttle"] >= 98]
            throttle_distance = full_throttle["Distance_delta"].sum()
            throttle_percentage = (throttle_distance / circuit_length) * 100

            throttle_data.append(
                {
                    "Driver": driver,
                    "Team": fastest_lap_throttle["Team"],
                    "ThrottlePercentage": round(throttle_percentage, 2),
                }
            )
        except Exception:
            continue

    throttle_data.sort(key=lambda x: x["ThrottlePercentage"], reverse=True)

    # Map to minimum value to show relative differences
    min_percentage = min(d["ThrottlePercentage"] for d in throttle_data)
    for d in throttle_data:
        d["PercentageDiff"] = d["ThrottlePercentage"] - min_percentage

    fig, ax = plt.subplots(figsize=(12, 8), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")

    drivers_list_throttle = [d["Driver"] for d in throttle_data]
    percentages_diff = [d["PercentageDiff"] for d in throttle_data]
    percentages_actual = [d["ThrottlePercentage"] for d in throttle_data]
    colors_throttle = [
        tif1.plotting.get_team_color(d["Team"], session_throttle) for d in throttle_data
    ]

    bars = ax.barh(
        drivers_list_throttle,
        percentages_diff,
        color=colors_throttle,
        alpha=0.8,
        edgecolor="white",
        linewidth=1,
    )

    for i, (driver, percentage) in enumerate(zip(drivers_list_throttle, percentages_actual)):
        ax.text(
            percentages_diff[i] + 0.1,
            i,
            f"{percentage:.1f}%",
            va="center",
            fontsize=10,
            fontweight="bold",
            color="white",
        )

    ax.set_xlabel(
        "Distance at Full Throttle (relative difference)",
        fontsize=12,
        fontweight="bold",
        color="white",
    )
    ax.set_ylabel("Driver", fontsize=12, fontweight="bold", color="white")
    ax.set_title(
        "2023 Bahrain GP Qualifying - Full Throttle Distance\n(% of lap distance at ≥98% throttle)",
        fontsize=14,
        fontweight="bold",
        pad=20,
        color="white",
    )
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3, linestyle="--", color="white")
    ax.tick_params(colors="white")
    ax.set_xlim(left=0)

    fig.text(
        0.5,
        0.02,
        "Higher values indicate more time at full throttle (power-limited tracks)",
        ha="center",
        fontsize=9,
        style="italic",
        alpha=0.7,
        color="white",
    )

    plt.tight_layout()
    plt.savefig("throttle_distance.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Throttle distance chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 16. Track Speed Map
print("\n16. Generating track speed map...")
try:
    session_speed_map = tif1.get_session(2023, "Monaco", "Q")
    fastest_lap_speed = session_speed_map.laps.pick_fastest()
    telemetry_speed = fastest_lap_speed.get_car_data()

    x_speed = telemetry_speed["X"]
    y_speed = telemetry_speed["Y"]
    speed_map = telemetry_speed["Speed"]

    points_speed = np.array([x_speed, y_speed]).T.reshape(-1, 1, 2)
    segments_speed = np.concatenate([points_speed[:-1], points_speed[1:]], axis=1)

    fig, ax = plt.subplots(figsize=(12, 10), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")

    norm_speed = plt.Normalize(speed_map.min(), speed_map.max())
    lc_speed = LineCollection(segments_speed, cmap="plasma", norm=norm_speed, linewidth=4)
    lc_speed.set_array(speed_map)
    line_speed = ax.add_collection(lc_speed)

    cbar_speed = plt.colorbar(line_speed, ax=ax, label="Speed (km/h)")
    cbar_speed.ax.tick_params(colors="white")
    cbar_speed.set_label("Speed (km/h)", color="white")

    ax.set_aspect("equal")
    ax.axis("off")
    plt.suptitle(
        f"{session_speed_map.event['EventName']} {session_speed_map.event.year} - Speed Map",
        color="white",
        fontweight="bold",
    )
    plt.tight_layout()
    plt.savefig("track_speed_map.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Track speed map chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 17. Track Throttle Map
print("\n17. Generating track throttle map...")
try:
    session_throttle_map = tif1.get_session(2023, "Silverstone", "Q")
    fastest_lap_throttle_map = session_throttle_map.laps.pick_fastest()
    telemetry_throttle_map = fastest_lap_throttle_map.get_car_data()

    x_throttle = telemetry_throttle_map["X"]
    y_throttle = telemetry_throttle_map["Y"]
    throttle_map = telemetry_throttle_map["Throttle"]

    points_throttle = np.array([x_throttle, y_throttle]).T.reshape(-1, 1, 2)
    segments_throttle = np.concatenate([points_throttle[:-1], points_throttle[1:]], axis=1)

    fig, ax = plt.subplots(figsize=(12, 10), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")

    norm_throttle = plt.Normalize(0, 100)
    lc_throttle = LineCollection(segments_throttle, cmap="RdYlGn", norm=norm_throttle, linewidth=4)
    lc_throttle.set_array(throttle_map)
    line_throttle = ax.add_collection(lc_throttle)

    cbar_throttle = plt.colorbar(line_throttle, ax=ax, label="Throttle (%)")
    cbar_throttle.ax.tick_params(colors="white")
    cbar_throttle.set_label("Throttle (%)", color="white")

    ax.set_aspect("equal")
    ax.axis("off")
    plt.suptitle(
        f"{session_throttle_map.event['EventName']} {session_throttle_map.event.year} - Throttle Map",
        color="white",
        fontweight="bold",
    )
    plt.tight_layout()
    plt.savefig("track_throttle_map.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Track throttle map chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 18. Track Brake Zones
print("\n18. Generating track brake zones...")
try:
    session_brake = tif1.get_session(2023, "Monza", "Q")
    fastest_lap_brake = session_brake.laps.pick_fastest()
    telemetry_brake = fastest_lap_brake.get_car_data()

    x_brake = telemetry_brake["X"]
    y_brake = telemetry_brake["Y"]
    brake_map = telemetry_brake["Brake"]

    points_brake = np.array([x_brake, y_brake]).T.reshape(-1, 1, 2)
    segments_brake = np.concatenate([points_brake[:-1], points_brake[1:]], axis=1)

    fig, ax = plt.subplots(figsize=(12, 10), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")

    norm_brake = plt.Normalize(0, 1)
    lc_brake = LineCollection(segments_brake, cmap="RdYlGn_r", norm=norm_brake, linewidth=4)
    lc_brake.set_array(brake_map)
    line_brake = ax.add_collection(lc_brake)

    cbar_brake = plt.colorbar(line_brake, ax=ax, label="Brake", ticks=[0, 1])
    cbar_brake.ax.set_yticklabels(["Off", "On"])
    cbar_brake.ax.tick_params(colors="white")
    cbar_brake.set_label("Brake", color="white")

    ax.set_aspect("equal")
    ax.axis("off")
    plt.suptitle(
        f"{session_brake.event['EventName']} {session_brake.event.year} - Brake Zones",
        color="white",
        fontweight="bold",
    )
    plt.tight_layout()
    plt.savefig("track_brake_zones.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Track brake zones chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 19. Track Acceleration Map
print("\n19. Generating track acceleration map...")
try:
    session_acc = tif1.get_session(2023, "Suzuka", "Q")
    fastest_lap_acc = session_acc.laps.pick_fastest()
    telemetry_acc = fastest_lap_acc.get_car_data()

    speed_ms_acc = telemetry_acc["Speed"] / 3.6
    time_s_acc = telemetry_acc["Time"].dt.total_seconds()
    lon_acc_map = np.gradient(speed_ms_acc, time_s_acc) / 9.81

    x_acc = telemetry_acc["X"]
    y_acc = telemetry_acc["Y"]

    points_acc = np.array([x_acc, y_acc]).T.reshape(-1, 1, 2)
    segments_acc = np.concatenate([points_acc[:-1], points_acc[1:]], axis=1)

    fig, ax = plt.subplots(figsize=(12, 10), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")

    norm_acc = plt.Normalize(lon_acc_map.min(), lon_acc_map.max())
    lc_acc = LineCollection(segments_acc, cmap="RdBu_r", norm=norm_acc, linewidth=4)
    lc_acc.set_array(lon_acc_map)
    line_acc = ax.add_collection(lc_acc)

    cbar_acc = plt.colorbar(line_acc, ax=ax, label="Longitudinal Acceleration (g)")
    cbar_acc.ax.tick_params(colors="white")
    cbar_acc.set_label("Longitudinal Acceleration (g)", color="white")

    ax.set_aspect("equal")
    ax.axis("off")
    plt.suptitle(
        f"{session_acc.event['EventName']} {session_acc.event.year} - Acceleration Map",
        color="white",
        fontweight="bold",
    )
    plt.tight_layout()
    plt.savefig("track_acceleration_map.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Track acceleration map chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 20. Multi-Driver Speed Comparison
print("\n20. Generating multi-driver speed comparison...")
try:
    session_multi = tif1.get_session(2023, "Bahrain", "Q")
    drivers_multi = ["VER", "PER", "LEC"]

    telemetry_data_multi = []
    for driver in drivers_multi:
        lap_multi = session_multi.laps.pick_driver(driver).pick_fastest()
        telemetry_multi = lap_multi.get_car_data()
        telemetry_multi["Driver"] = driver
        driver_info_multi = session_multi.get_driver(driver)
        telemetry_multi["Color"] = driver_info_multi["TeamColor"]
        telemetry_data_multi.append(telemetry_multi)

    all_telemetry_multi = pd.concat(telemetry_data_multi)

    fig, ax = plt.subplots(figsize=(14, 10), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")

    for driver in drivers_multi:
        driver_tel_multi = all_telemetry_multi[all_telemetry_multi["Driver"] == driver]
        x_multi = driver_tel_multi["X"].values
        y_multi = driver_tel_multi["Y"].values
        speed_multi = driver_tel_multi["Speed"].values

        points_multi = np.array([x_multi, y_multi]).T.reshape(-1, 1, 2)
        segments_multi = np.concatenate([points_multi[:-1], points_multi[1:]], axis=1)

        norm_multi = plt.Normalize(speed_multi.min(), speed_multi.max())
        lc_multi = LineCollection(
            segments_multi, cmap="plasma", norm=norm_multi, linewidth=3, alpha=0.7, label=driver
        )
        lc_multi.set_array(speed_multi)
        ax.add_collection(lc_multi)

    ax.set_aspect("equal")
    ax.axis("off")
    ax.legend(
        loc="upper right", fontsize=12, facecolor="#1a1a1a", edgecolor="white", labelcolor="white"
    )
    plt.suptitle(
        f"{session_multi.event['EventName']} {session_multi.event.year} - Speed Comparison",
        color="white",
        fontweight="bold",
    )
    plt.tight_layout()
    plt.savefig(
        "multi_driver_speed_comparison.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a"
    )
    plt.close()
    print("✓ Multi-driver speed comparison chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

# 21. Top Speeds by Team
print("\n21. Generating top speeds by team chart...")
try:
    session_top_speeds = tif1.get_session(2023, "Italian Grand Prix", "Q")
    
    # Find which speed trap recorded the highest speeds
    speed_columns = ["SpeedI1", "SpeedI2", "SpeedST", "SpeedFL"]
    speed_data = session_top_speeds.laps[speed_columns]
    fastest_trap = speed_data.idxmax(axis=1).value_counts().index[0]
    
    # Calculate maximum speeds by team
    team_speeds = session_top_speeds.laps[[fastest_trap, "Team"]].copy()
    max_speeds = team_speeds.groupby("Team")[fastest_trap].max().reset_index()
    max_speeds.columns = ["Team", "MaxSpeed"]
    
    # Sort by speed (descending)
    max_speeds = max_speeds.sort_values("MaxSpeed", ascending=False)
    
    # Calculate difference from slowest team
    max_speeds["Diff"] = max_speeds["MaxSpeed"] - max_speeds["MaxSpeed"].min()
    
    # Get team colors
    team_colors = {}
    for team in max_speeds["Team"]:
        team_colors[team] = tif1.plotting.get_team_color(team, session=session_top_speeds)
    
    max_speeds["Color"] = max_speeds["Team"].map(team_colors)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(10, 8), facecolor="#1a1a1a")
    ax.set_facecolor("#1a1a1a")
    
    # Create horizontal bars
    bars = ax.barh(
        y=max_speeds["Team"], width=max_speeds["Diff"], color=max_speeds["Color"], height=0.7
    )
    
    # Add speed value labels
    for i, (speed, diff) in enumerate(zip(max_speeds["MaxSpeed"], max_speeds["Diff"])):
        ax.text(
            diff + 0.2, i, f"{int(speed)} km/h", va="center", fontsize=10, fontweight="bold", color="white"
        )
    
    # Styling
    ax.set_xlabel("Speed Difference (km/h)", fontsize=11, color="white")
    ax.set_title(
        f"{session_top_speeds.event.year} {session_top_speeds.event['EventName']} - Top Speeds by Team",
        fontsize=13,
        fontweight="bold",
        pad=20,
        color="white"
    )
    ax.invert_yaxis()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="x", alpha=0.3, linestyle="--", color="white")
    ax.tick_params(colors="white")
    
    plt.tight_layout()
    plt.savefig("top_speeds.png", dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
    plt.close()
    print("✓ Top speeds by team chart saved")
except Exception as e:
    print(f"✗ Failed: {e}")

print("\n✅ Chart generation complete!")
print("Generated charts:")
print("  - driver_laptimes_example.png")
print("  - race_position_changes.png")
print("  - race_tire_strategy.png")
print("  - race_pace_boxplot.png")
print("  - qualifying_grid.png")
print("  - telemetry_comparison.png")
print("  - weather_temperature_impact.png")
print("  - tire_degradation.png")
print("  - speed_traces.png")
print("  - gear_shifts_on_track.png")
print("  - annotated_speed_trace.png")
print("  - laptimes_distribution.png")
print("  - laptime_heatmap.png")
print("  - downforce_levels.png")
print("  - throttle_distance.png")
print("  - track_speed_map.png")
print("  - track_throttle_map.png")
print("  - track_brake_zones.png")
print("  - track_acceleration_map.png")
print("  - multi_driver_speed_comparison.png")
print("  - top_speeds.png")
