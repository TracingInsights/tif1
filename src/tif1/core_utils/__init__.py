"""Core utilities for tif1 - constants and helper functions.

The package-level re-exports resolve lazily (PEP 562): ``helpers`` imports
pandas (~340 ms) and every ``from .core_utils.constants import ...`` runs this
``__init__`` first, so eager re-exports made pandas a dependency of modules
that never build a frame (``tif1.cache``, the schedule, ``tif1 cache-info``).
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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

# Re-exported name -> defining submodule (the upper-case constants live in ``constants``).
_LAZY_EXPORTS: dict[str, str] = {
    name: ".constants" if name.isupper() else ".helpers" for name in __all__
}


def __getattr__(name: str) -> Any:
    submodule = _LAZY_EXPORTS.get(name)
    if submodule is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(submodule, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
