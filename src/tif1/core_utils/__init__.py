"""Core utilities for tif1 - constants and helper functions."""

from .constants import (
    CATEGORICAL_COLS,
    LAP_RENAME_MAP,
    MAX_CACHE_SIZE,
    MAX_YEAR,
    MIN_YEAR,
    RACE_CONTROL_RENAME_MAP,
    TELEMETRY_RENAME_MAP,
    WEATHER_RENAME_MAP,
)
from .helpers import (
    DataFrame,
    _apply_categorical,
    _check_cached_telemetry,
    _create_empty_df,
    _create_telemetry_df,
    _encode_url_component,
    _filter_valid_laptimes,
    _get_lap_number,
    _is_empty_df,
    _rename_columns,
    _validate_drivers_list,
    _validate_lap_number,
    _validate_string_param,
    _validate_year,
)

__all__ = [
    # Constants
    "MIN_YEAR",
    "MAX_YEAR",
    "MAX_CACHE_SIZE",
    "LAP_RENAME_MAP",
    "RACE_CONTROL_RENAME_MAP",
    "TELEMETRY_RENAME_MAP",
    "WEATHER_RENAME_MAP",
    "CATEGORICAL_COLS",
    # Type aliases
    "DataFrame",
    # Validators
    "_validate_year",
    "_validate_drivers_list",
    "_validate_lap_number",
    "_validate_string_param",
    # Helpers
    "_encode_url_component",
    "_is_empty_df",
    "_create_empty_df",
    "_filter_valid_laptimes",
    "_rename_columns",
    "_apply_categorical",
    "_get_lap_number",
    "_create_telemetry_df",
    "_check_cached_telemetry",
]
