"""Native chart functions for tif1.

Every chart takes ``(year, event, session)`` and returns a
``(fig, ax)`` pair. All functions are also re-exported at the top level of
``tif1`` (e.g. ``tif1.plot_top_speeds(...)``).
"""

from .lap_times import (
    plot_driver_laptimes,
    plot_lap_delta,
    plot_laptime_heatmap,
    plot_laptimes_distribution,
    plot_position_changes,
    plot_qualifying_grid,
    plot_track_temperature,
)
from .performance import plot_downforce_levels, plot_throttle_distance, plot_tire_degradation
from .telemetry import (
    plot_annotated_speed_trace,
    plot_gg_diagram,
    plot_speed_traces,
    plot_telemetry_comparison,
)
from .top_speeds import plot_top_speeds
from .track_maps import (
    plot_gear_shifts,
    plot_multi_driver_speed_comparison,
    plot_track_acceleration_map,
    plot_track_brake_zones,
    plot_track_speed_map,
    plot_track_throttle_map,
)

__all__ = [
    "plot_annotated_speed_trace",
    "plot_downforce_levels",
    "plot_driver_laptimes",
    "plot_gear_shifts",
    "plot_gg_diagram",
    "plot_lap_delta",
    "plot_laptime_heatmap",
    "plot_laptimes_distribution",
    "plot_multi_driver_speed_comparison",
    "plot_position_changes",
    "plot_qualifying_grid",
    "plot_speed_traces",
    "plot_telemetry_comparison",
    "plot_throttle_distance",
    "plot_tire_degradation",
    "plot_top_speeds",
    "plot_track_acceleration_map",
    "plot_track_brake_zones",
    "plot_track_speed_map",
    "plot_track_temperature",
    "plot_track_throttle_map",
]
