"""tif1 - A faster alternative to fastf1 with the same bindings."""

import logging
import os
import sys
from importlib import import_module
from typing import TYPE_CHECKING, Any

from .exceptions import (
    AsyncRequiredError,
    CacheError,
    CircuitBreakerOpenError,
    DataNotFoundError,
    DataNotLoadedError,
    DriverNotFoundError,
    ErgastError,
    ErgastInvalidRequestError,
    ErgastJsonError,
    EventNotFoundError,
    EventNotInSeasonError,
    ExactEventNotFoundError,
    FastF1CriticalError,
    FuzzyMatchError,
    InvalidDataError,
    LapNotFoundError,
    NetworkError,
    NoLapDataError,
    RateLimitExceededError,
    SessionNotLoadedError,
    TIF1Error,
)

if TYPE_CHECKING:
    import threading

__all__ = [
    "AsyncRequiredError",
    "BackendType",
    "CircuitInfo",
    "CompoundType",
    "CacheError",
    "DataNotFoundError",
    "DriverInfoDict",
    "CircuitBreakerOpenError",
    "DriverNotFoundError",
    "Ergast",
    "ErgastError",
    "ErgastInvalidRequestError",
    "ErgastJsonError",
    "ErgastResultType",
    "ErgastSourceType",
    "EventNotFoundError",
    "EventNotInSeasonError",
    "ExactEventNotFoundError",
    "InvalidDataError",
    "LapDataDict",
    "LapNotFoundError",
    "NetworkError",
    "RateLimitExceededError",
    "ScheduleBackend",
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
    "DataNotLoadedError",
    "FastF1CriticalError",
    "FuzzyMatchError",
    "NoLapDataError",
    "LoggingManager",
    "get_logger",
    "soft_exceptions",
    "logger",
    "legacy",
    "mvapi",
    "get_circuit_info",
    "PolarsLaps",
    "PolarsLap",
    "PolarsTelemetry",
    "PolarsSessionResults",
    "PolarsDriverResult",
    "PolarsDriver",
    "Cache",
    "get_cache",
    "get_cdn_manager",
    "get_circuit_breaker",
    "get_config",
    "get_constructor_standings",
    "get_driver_standings",
    "get_event",
    "get_event_by_name",
    "get_event_by_round",
    "get_event_schedule",
    "get_events",
    "get_events_remaining",
    "get_session",
    "get_sessions",
    "get_testing_event",
    "get_testing_session",
    "reset_circuit_breaker",
    "core",
    "ergast",
    "events",
    "session",
    "models",
    "plotting",
    "charts",
    "assets",
    "browser",
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
    "CircuitInfo": ("tif1.models_shared", "CircuitInfo"),
    "CompoundType": ("tif1.types", "CompoundType"),
    "DriverInfoDict": ("tif1.types", "DriverInfoDict"),
    "LapDataDict": ("tif1.types", "LapDataDict"),
    "SessionType": ("tif1.types", "SessionType"),
    "ErgastResultType": ("tif1.types", "ErgastResultType"),
    "ErgastSourceType": ("tif1.types", "ErgastSourceType"),
    "ScheduleBackend": ("tif1.types", "ScheduleBackend"),
    "Ergast": ("tif1.ergast", "Ergast"),
    "ergast": ("tif1.ergast", None),
    "get_driver_standings": ("tif1.ergast.standings", "get_driver_standings"),
    "get_constructor_standings": ("tif1.ergast.standings", "get_constructor_standings"),
    "get_events_remaining": ("tif1.events", "get_events_remaining"),
    "get_testing_event": ("tif1.events", "get_testing_event"),
    "get_testing_session": ("tif1.events", "get_testing_session"),
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
    "set_log_level": ("tif1.logger", "set_log_level"),
    "logger": ("tif1.logger", None),
    "legacy": ("tif1.legacy", None),
    "LoggingManager": ("tif1.logger", "LoggingManager"),
    "get_logger": ("tif1.logger", "get_logger"),
    "soft_exceptions": ("tif1.logger", "soft_exceptions"),
    "mvapi": ("tif1.mvapi", None),
    "get_circuit_info": ("tif1.mvapi", "get_circuit_info"),
    "PolarsLaps": ("tif1.models_polars", "PolarsLaps"),
    "PolarsLap": ("tif1.models_polars", "PolarsLap"),
    "PolarsTelemetry": ("tif1.models_polars", "PolarsTelemetry"),
    "PolarsSessionResults": ("tif1.models_polars", "PolarsSessionResults"),
    "PolarsDriverResult": ("tif1.models_polars", "PolarsDriverResult"),
    "PolarsDriver": ("tif1.models_polars", "PolarsDriver"),
    "plotting": ("tif1.plotting", None),
    "events": ("tif1.events", None),
    "session": ("tif1.session", None),
    "models": ("tif1.models", None),
    "utils": ("tif1.utils", None),
    "assets": ("tif1.assets", None),
    "browser": ("tif1.browser", None),
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
    """Resolve heavyweight exports on first access.

    ``__version__`` is resolved here too: the eager ``importlib.metadata``
    lookup cost ~37 ms of the ~47 ms ``import tif1`` (importlib.metadata
    imports email/parser machinery); the lookup runs once on first access
    and is cached in module globals.
    """
    if name == "__version__":
        from importlib.metadata import PackageNotFoundError
        from importlib.metadata import version as _version

        try:
            value = _version("tif1")
        except PackageNotFoundError:
            value = "0.0.0"
        globals()["__version__"] = value
        return value

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


_PRELOAD_ENV = "TIF1_PRELOAD_IMPORTS"
_PRELOAD_MODULES = ("numpy", "pandas", "pyarrow")


def _interactive_interpreter() -> bool:
    """Return True in a REPL, under ``python -i`` or inside an IPython/Jupyter kernel."""
    return "IPython" in sys.modules or hasattr(sys, "ps1") or bool(sys.flags.interactive)


def _preload_wanted() -> bool:
    """Decide whether to warm the third-party imports in the background.

    ``TIF1_PRELOAD_IMPORTS=1`` forces the preload on (scripts, benchmarks),
    ``0`` forces it off; otherwise it runs only in interactive interpreters,
    where the user types the next cell while pandas loads.
    """
    override = os.environ.get(_PRELOAD_ENV)
    if override is not None:
        return override.strip().lower() in {"1", "true", "yes", "on"}
    return _interactive_interpreter()


def _preload_third_party() -> None:
    """Worker body: import the heavy third-party modules ``tif1.core`` needs.

    Third-party modules only. A ``tif1`` submodule imported here would read
    configuration (environment variables) before the user had set it, and could
    deadlock against a main-thread ``import tif1.core``. Import errors are left
    for the main thread, which reports them on its own import.
    """
    for name in _PRELOAD_MODULES:
        try:
            import_module(name)
        except Exception:
            return


def _start_preload() -> "threading.Thread | None":
    """Start the background import of numpy/pandas/pyarrow when it pays off.

    ``import tif1`` is lazy (~7 ms); the first ``get_session`` then imports
    ``tif1.core`` and pays ~220 ms for pandas, numpy and pyarrow. In a notebook
    that import runs while the user types the next cell: a daemon thread
    started here imports the third-party modules, and the later main-thread
    import finds them in ``sys.modules`` (or waits on the module lock, which
    costs nothing extra). Non-interactive processes never start the thread.
    """
    # Pyodide (Emscripten) has no threads; the import is paid on first use.
    if not _preload_wanted() or "pandas" in sys.modules or sys.platform == "emscripten":
        return None
    import threading

    thread = threading.Thread(target=_preload_third_party, name="tif1-preload", daemon=True)
    thread.start()
    return thread


_preload_thread = _start_preload()


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
