"""tif1 - A faster alternative to fastf1 with the same bindings."""

import logging
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from .exceptions import (
    CacheError,
    DataNotFoundError,
    DriverNotFoundError,
    InvalidDataError,
    LapNotFoundError,
    NetworkError,
    SessionNotLoadedError,
    TIF1Error,
)

try:
    __version__ = version("tif1")
except PackageNotFoundError:
    __version__ = "0.0.0"
__all__ = [
    "BackendType",
    "CircuitInfo",
    "CompoundType",
    "CacheError",
    "DataNotFoundError",
    "DriverInfoDict",
    "DriverNotFoundError",
    "InvalidDataError",
    "LapDataDict",
    "LapNotFoundError",
    "NetworkError",
    "SessionNotLoadedError",
    "SessionType",
    "Session",
    "Laps",
    "Lap",
    "Driver",
    "Telemetry",
    "TIF1Error",
    "TelemetryDataDict",
    "TrackStatusType",
    "Cache",
    "get_cache",
    "get_cdn_manager",
    "get_circuit_breaker",
    "get_config",
    "get_event",
    "get_event_by_name",
    "get_event_by_round",
    "get_event_schedule",
    "get_events",
    "get_session",
    "get_sessions",
    "reset_circuit_breaker",
    "core",
    "events",
    "session",
    "models",
    "plotting",
    "charts",
    "assets",
    "configure_chart_saving",
    "get_chart_save_config",
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
    "plot_race_launch_ratings",
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
    "setup_logging",
    "set_log_level",
    "utils",
]

_LAZY_EXPORTS = {
    "BackendType": ("tif1.types", "BackendType"),
    "CircuitInfo": ("tif1.models", "CircuitInfo"),
    "CompoundType": ("tif1.types", "CompoundType"),
    "DriverInfoDict": ("tif1.types", "DriverInfoDict"),
    "LapDataDict": ("tif1.types", "LapDataDict"),
    "SessionType": ("tif1.types", "SessionType"),
    "TelemetryDataDict": ("tif1.types", "TelemetryDataDict"),
    "TrackStatusType": ("tif1.types", "TrackStatusType"),
    "Cache": ("tif1.fastf1_compat", "Cache"),
    "Session": ("tif1.session", "Session"),
    "Laps": ("tif1.models", "Laps"),
    "Lap": ("tif1.models", "Lap"),
    "Driver": ("tif1.models", "Driver"),
    "Telemetry": ("tif1.models", "Telemetry"),
    "get_cache": ("tif1.cache", "get_cache"),
    "get_cdn_manager": ("tif1.cdn", "get_cdn_manager"),
    "get_circuit_breaker": ("tif1.retry", "get_circuit_breaker"),
    "get_config": ("tif1.config", "get_config"),
    "get_events": ("tif1.events", "get_events"),
    "get_session": ("tif1.core", "get_session"),
    "set_log_level": ("tif1.fastf1_compat", "set_log_level"),
    "plotting": ("tif1.plotting", None),
    "events": ("tif1.events", None),
    "session": ("tif1.session", None),
    "models": ("tif1.models", None),
    "utils": ("tif1.utils", None),
    "assets": ("tif1.assets", None),
    "core": ("tif1.core", None),
    "get_event": ("tif1.events", "get_event"),
    "get_event_by_name": ("tif1.events", "get_event_by_name"),
    "get_event_by_round": ("tif1.events", "get_event_by_round"),
    "get_event_schedule": ("tif1.events", "get_event_schedule"),
    "get_sessions": ("tif1.events", "get_sessions"),
    "reset_circuit_breaker": ("tif1.retry", "reset_circuit_breaker"),
    "charts": ("tif1.charts", None),
    "configure_chart_saving": ("tif1.charts", "configure_chart_saving"),
    "get_chart_save_config": ("tif1.charts", "get_chart_save_config"),
    "plot_annotated_speed_trace": ("tif1.charts", "plot_annotated_speed_trace"),
    "plot_downforce_levels": ("tif1.charts", "plot_downforce_levels"),
    "plot_driver_laptimes": ("tif1.charts", "plot_driver_laptimes"),
    "plot_gear_shifts": ("tif1.charts", "plot_gear_shifts"),
    "plot_gg_diagram": ("tif1.charts", "plot_gg_diagram"),
    "plot_lap_delta": ("tif1.charts", "plot_lap_delta"),
    "plot_laptime_heatmap": ("tif1.charts", "plot_laptime_heatmap"),
    "plot_laptimes_distribution": ("tif1.charts", "plot_laptimes_distribution"),
    "plot_multi_driver_speed_comparison": ("tif1.charts", "plot_multi_driver_speed_comparison"),
    "plot_position_changes": ("tif1.charts", "plot_position_changes"),
    "plot_qualifying_grid": ("tif1.charts", "plot_qualifying_grid"),
    "plot_race_launch_ratings": ("tif1.charts", "plot_race_launch_ratings"),
    "plot_speed_traces": ("tif1.charts", "plot_speed_traces"),
    "plot_telemetry_comparison": ("tif1.charts", "plot_telemetry_comparison"),
    "plot_throttle_distance": ("tif1.charts", "plot_throttle_distance"),
    "plot_tire_degradation": ("tif1.charts", "plot_tire_degradation"),
    "plot_top_speeds": ("tif1.charts", "plot_top_speeds"),
    "plot_track_acceleration_map": ("tif1.charts", "plot_track_acceleration_map"),
    "plot_track_brake_zones": ("tif1.charts", "plot_track_brake_zones"),
    "plot_track_speed_map": ("tif1.charts", "plot_track_speed_map"),
    "plot_track_temperature": ("tif1.charts", "plot_track_temperature"),
    "plot_track_throttle_map": ("tif1.charts", "plot_track_throttle_map"),
}


def __getattr__(name: str) -> Any:
    """Resolve heavyweight exports on first access."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'tif1' has no attribute {name!r}")

    module_name, attr_name = target
    if attr_name is None:
        value = import_module(module_name)
    else:
        value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def setup_logging(level: int = logging.WARNING) -> None:
    """
    Configure logging for tif1.

    Args:
        level: Logging level (e.g., logging.DEBUG, logging.INFO)

    Example:
        >>> import tif1
        >>> import logging
        >>> tif1.setup_logging(logging.DEBUG)
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("tif1").setLevel(level)
    # Suppress urllib3_future connection warnings
    logging.getLogger("urllib3_future.connectionpool").setLevel(logging.ERROR)
