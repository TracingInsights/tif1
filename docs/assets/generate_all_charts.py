"""Generate all tutorial charts for documentation.

Canonical "regenerate all docs charts" entrypoint: calls the native
``tif1.charts`` functions with the same parameters as the individual
standalone wrappers in this directory, so regenerated PNGs are
content-identical to the wrapper outputs.

Ownership rules:
- ``race_position_changes.png`` is owned by ``generate_position_changes.py``
  and is skipped here (the old orchestrator-only top-10 variant is superseded
  by the native ``plot_position_changes``).
- The three charts without a native counterpart (tire strategy, race pace
  boxplot, weather impact) are produced here directly.
"""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

import tif1

OUT = Path(__file__).resolve().parent
# Docs charts use the TracingInsights-brand dark theme (#011627 background, lime text).
THEME = "default-dark"
SAVE_KW = {"bbox_inches": "tight"}

print("Starting chart generation...")

NATIVE_CHARTS = [
    # (label, callable, args, kwargs)
    (
        "Top speeds by team",
        tif1.charts.plot_top_speeds,
        (2023, "Italian Grand Prix", "Q"),
        {"save_path": OUT / "top_speeds.png", "dpi": 150, "color_scheme": THEME},
    ),
    (
        "Track speed map",
        tif1.charts.plot_track_speed_map,
        (2023, "Monaco", "Q"),
        {"save_path": OUT / "track_speed_map.png", "dpi": 300, "color_scheme": THEME},
    ),
    (
        "Track throttle map",
        tif1.charts.plot_track_throttle_map,
        (2023, "Silverstone", "Q"),
        {"save_path": OUT / "track_throttle_map.png", "dpi": 300, "color_scheme": THEME},
    ),
    (
        "Track brake zones",
        tif1.charts.plot_track_brake_zones,
        (2023, "Monza", "Q"),
        {"save_path": OUT / "track_brake_zones.png", "dpi": 300, "color_scheme": THEME},
    ),
    (
        "Track acceleration map",
        tif1.charts.plot_track_acceleration_map,
        (2023, "Suzuka", "Q"),
        {"save_path": OUT / "track_acceleration_map.png", "dpi": 300, "color_scheme": THEME},
    ),
    (
        "Gear shifts on track",
        tif1.charts.plot_gear_shifts,
        (2021, "Austrian Grand Prix", "Q"),
        {"save_path": OUT / "gear_shifts_on_track.png", "dpi": 150, "color_scheme": THEME},
    ),
    (
        "Annotated speed trace",
        tif1.charts.plot_annotated_speed_trace,
        (2021, "Spanish Grand Prix", "Q"),
        {"save_path": OUT / "annotated_speed_trace.png", "dpi": 150, "color_scheme": THEME},
    ),
    (
        "Speed traces comparison",
        tif1.charts.plot_speed_traces,
        (2023, "Spanish Grand Prix", "Q"),
        {"save_path": OUT / "speed_traces.png", "dpi": 150, "color_scheme": THEME},
    ),
    (
        "Multi-driver speed comparison",
        tif1.charts.plot_multi_driver_speed_comparison,
        (2023, "Bahrain Grand Prix", "Q"),
        {
            "drivers": ["VER", "PER", "LEC"],
            "save_path": OUT / "multi_driver_speed_comparison.png",
            "dpi": 300,
            "color_scheme": THEME,
        },
    ),
    (
        "Lap delta comparison",
        tif1.charts.plot_lap_delta,
        (2024, "Monaco Grand Prix", "R"),
        {
            "drivers": ["VER", "LEC"],
            "save_path": OUT / "lap_delta.png",
            "dpi": 300,
            "color_scheme": THEME,
        },
    ),
    (
        "Telemetry comparison",
        tif1.charts.plot_telemetry_comparison,
        (2024, "Monaco Grand Prix", "Q"),
        {"save_path": OUT / "telemetry_comparison.png", "dpi": 300, "color_scheme": THEME},
    ),
    (
        "G-G diagram",
        tif1.charts.plot_gg_diagram,
        (2024, "Monaco Grand Prix", "Q"),
        {
            "drivers": ["VER", "LEC", "NOR"],
            "save_path": OUT / "gg_diagram.png",
            "dpi": 300,
            "color_scheme": THEME,
        },
    ),
    (
        "Driver lap times",
        tif1.charts.plot_driver_laptimes,
        (2023, "Azerbaijan Grand Prix", "R"),
        {
            "drivers": ["ALO"],
            "save_path": OUT / "driver_laptimes_example.png",
            "dpi": 150,
            "color_scheme": THEME,
        },
    ),
    (
        "Lap times distribution",
        tif1.charts.plot_laptimes_distribution,
        (2023, "Azerbaijan Grand Prix", "R"),
        {"save_path": OUT / "laptimes_distribution.png", "dpi": 150, "color_scheme": THEME},
    ),
    (
        "Lap time heatmap",
        tif1.charts.plot_laptime_heatmap,
        (2023, "Monaco Grand Prix", "R"),
        {"save_path": OUT / "laptime_heatmap.png", "dpi": 300, "color_scheme": THEME},
    ),
    (
        "Qualifying grid",
        tif1.charts.plot_qualifying_grid,
        (2023, "Spanish Grand Prix", "Q"),
        {
            "save_path": OUT / "qualifying_grid.png",
            "dpi": 150,
            "color_scheme": THEME,
        },
    ),
    (
        "Track temperature",
        tif1.charts.plot_track_temperature,
        (2023, "Monaco Grand Prix", "R"),
        {"save_path": OUT / "track_temperature.png", "dpi": 150, "color_scheme": THEME},
    ),
    (
        "Downforce levels",
        tif1.charts.plot_downforce_levels,
        (2023, "Monaco Grand Prix", "Q"),
        {"save_path": OUT / "downforce_levels.png", "dpi": 300, "color_scheme": THEME},
    ),
    (
        "Throttle distance",
        tif1.charts.plot_throttle_distance,
        (2023, "Bahrain Grand Prix", "Q"),
        {"save_path": OUT / "throttle_distance.png", "dpi": 300, "color_scheme": THEME},
    ),
    (
        "Tire degradation",
        tif1.charts.plot_tire_degradation,
        (2024, "Hungarian Grand Prix", "Race"),
        {"save_path": OUT / "tire_degradation.png", "dpi": 300, "color_scheme": THEME},
    ),
]


def _render_native_charts() -> None:
    """Render the 20 native charts (all except the standalone-owned position changes)."""
    for index, (label, func, args, kwargs) in enumerate(NATIVE_CHARTS, start=1):
        print(f"\n{index}. Generating {label} chart...")
        try:
            fig, _ = func(*args, **kwargs)
            plt.close(fig)
            print(f"✓ {label} chart saved")
        except Exception as e:
            print(f"✗ Failed: {e}")


def _render_tire_strategy() -> None:
    """Orchestrator-only output: tire strategy bar chart."""
    tif1.plotting.setup_mpl(color_scheme=THEME)
    print("\nGenerating tire strategy chart...")
    try:
        session = tif1.get_session(2024, "Abu Dhabi Grand Prix", "Race")
        laps = session.laps
        compound_colors = tif1.plotting.get_compound_mapping(session=session)

        final_lap = laps[laps["LapNumber"] == laps["LapNumber"].max()]
        drivers_sorted = final_lap.sort_values("Position")["Driver"].tolist()[:15]

        _, ax = plt.subplots(figsize=(16, 12))
        for idx, driver in enumerate(drivers_sorted):
            driver_laps = laps[laps["Driver"] == driver].sort_values("LapNumber")
            for stint in driver_laps["Stint"].unique():
                stint_laps = driver_laps[driver_laps["Stint"] == stint]
                compound = stint_laps["Compound"].iloc[0]
                ax.barh(
                    y=idx,
                    width=stint_laps["LapNumber"].max() - stint_laps["LapNumber"].min() + 1,
                    left=stint_laps["LapNumber"].min(),
                    height=0.8,
                    color=compound_colors.get(compound, "#888888"),
                    edgecolor="white",
                    linewidth=1,
                )

        ax.set_yticks(range(len(drivers_sorted)))
        ax.set_yticklabels(drivers_sorted)
        ax.set_xlabel("Lap Number")
        ax.set_ylabel("Driver (by finish position)")
        ax.set_title("Tire Strategy - Full Race")
        ax.invert_yaxis()
        legend_elements = [
            Patch(facecolor=compound_colors["SOFT"], label="Soft"),
            Patch(facecolor=compound_colors["MEDIUM"], label="Medium"),
            Patch(facecolor=compound_colors["HARD"], label="Hard"),
        ]
        ax.legend(
            handles=legend_elements,
            loc="upper right",
        )
        plt.tight_layout()
        plt.savefig(OUT / "race_tire_strategy.png", dpi=150, **SAVE_KW)
        plt.close()
        print("✓ Tire strategy chart saved")
    except Exception as e:
        print(f"✗ Failed: {e}")


def _render_race_pace() -> None:
    """Orchestrator-only output: race pace box plot of the top 5 finishers."""
    tif1.plotting.setup_mpl(color_scheme=THEME)
    print("\nGenerating race pace comparison chart...")
    try:
        session = tif1.get_session(2024, "Abu Dhabi Grand Prix", "Race")
        laps = session.laps

        clean_laps = laps[
            (laps["LapTime"] < laps["LapTime"].min() * 1.07)
            & (laps["PitInTime"].isna())
            & (laps["PitOutTime"].isna())
            & (laps["LapNumber"] > 1)
        ].copy()

        if pd.api.types.is_timedelta64_dtype(clean_laps["LapTime"]):
            clean_laps["LapTime"] = clean_laps["LapTime"].dt.total_seconds()

        final_positions = laps[laps["LapNumber"] == laps["LapNumber"].max()].sort_values("Position")
        top_5_drivers = final_positions.head(5)["Driver"].tolist()
        top_5_laps = clean_laps[clean_laps["Driver"].isin(top_5_drivers)]

        _, ax = plt.subplots(figsize=(12, 6))
        colors_list = [tif1.plotting.get_driver_color(d, session=session) for d in top_5_drivers]
        bp = ax.boxplot(
            [top_5_laps[top_5_laps["Driver"] == d]["LapTime"].values for d in top_5_drivers],
            tick_labels=top_5_drivers,
            patch_artist=True,
            showmeans=True,
            # Matplotlib draws whiskers/caps black, invisible on the dark theme.
            whiskerprops={"color": plt.rcParams["text.color"]},
            capprops={"color": plt.rcParams["text.color"]},
            flierprops={"markeredgecolor": plt.rcParams["text.color"]},
        )
        for patch, color in zip(bp["boxes"], colors_list):
            patch.set_facecolor(color)
            patch.set_alpha(0.6)
        ax.set_xlabel("Driver")
        ax.set_ylabel("Lap Time (s)")
        ax.set_title("Race Pace Distribution - Top 5 Finishers")
        ax.grid(True, alpha=0.3, axis="y")
        plt.tight_layout()
        plt.savefig(OUT / "race_pace_boxplot.png", dpi=150, **SAVE_KW)
        plt.close()
        print("✓ Race pace chart saved")
    except Exception as e:
        print(f"✗ Failed: {e}")


def _render_weather_impact() -> None:
    """Orchestrator-only output: track temperature vs lap time scatter + trend."""
    tif1.plotting.setup_mpl(color_scheme=THEME)
    print("\nGenerating weather impact chart...")
    try:
        session = tif1.get_session(2024, "Singapore Grand Prix", "Race")
        laps = session.laps.copy()

        if pd.api.types.is_timedelta64_dtype(laps["LapTime"]):
            laps["LapTime"] = laps["LapTime"].dt.total_seconds()

        clean_laps = laps[
            (laps["LapTime"] < laps["LapTime"].min() * 1.07)
            & (laps["PitInTime"].isna())
            & (laps["LapNumber"] > 1)
        ]

        _, ax = plt.subplots(figsize=(12, 6))
        ax.scatter(clean_laps["TrackTemp"], clean_laps["LapTime"], alpha=0.3, color="cyan")
        ax.set_xlabel("Track Temperature (°C)")
        ax.set_ylabel("Lap Time (s)")
        ax.set_title("Track Temperature Impact on Lap Times")

        valid_data = clean_laps[["TrackTemp", "LapTime"]].dropna()
        z = np.polyfit(valid_data["TrackTemp"], valid_data["LapTime"], 1)
        p = np.poly1d(z)
        temp_range = np.linspace(valid_data["TrackTemp"].min(), valid_data["TrackTemp"].max(), 100)
        ax.plot(temp_range, p(temp_range), "r--", linewidth=2, label=f"Trend: {z[0]:.3f}s/°C")
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(OUT / "weather_temperature_impact.png", dpi=150, **SAVE_KW)
        plt.close()
        print("✓ Weather impact chart saved")
    except Exception as e:
        print(f"✗ Failed: {e}")


if __name__ == "__main__":
    _render_native_charts()
    _render_tire_strategy()
    _render_race_pace()
    _render_weather_impact()

    print("\n✅ Chart generation complete!")
    print("Generated charts:")
    for _, _func, _, kwargs in NATIVE_CHARTS:
        print(f"  - {Path(str(kwargs['save_path'])).name}")
    print("  - race_tire_strategy.png")
    print("  - race_pace_boxplot.png")
    print("  - weather_temperature_impact.png")
    print("  (race_position_changes.png is owned by generate_position_changes.py)")
