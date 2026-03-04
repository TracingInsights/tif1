"""Utility functions for tif1, compatible with fastf1.utils."""

from datetime import timedelta

import pandas as pd


def delta_time(reference_lap, compare_lap):
    """Calculate delta time between two laps."""
    ref_tel = reference_lap.get_telemetry()
    comp_tel = compare_lap.get_telemetry()

    # Simple placeholder implementation
    return pd.Series(), ref_tel, comp_tel


def to_timedelta(x):
    """Convert string or float to timedelta."""
    if isinstance(x, timedelta):
        return x
    if isinstance(x, str):
        if ":" in x and x.count(":") == 1:
            x = "00:" + x
        return pd.to_timedelta(x)
    if isinstance(x, int | float):
        return pd.to_timedelta(x, unit="s")
    return pd.to_timedelta(x)


def to_datetime(x):
    """Convert string to datetime."""
    return pd.to_datetime(x)


def recursive_dict_get(d, *keys, default_none=False):
    """Recursive dict get."""
    for key in keys:
        try:
            d = d[key]
        except (KeyError, TypeError):
            return None if default_none else {}
    return d
