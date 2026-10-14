"""Utility functions for tif1, compatible with fastf1.utils."""

from datetime import timedelta

import numpy as np
import pandas as pd


def _lap_car_data(lap) -> pd.DataFrame:
    """Telemetry frame for a Lap series, a Driver lap, or a frame passed directly."""
    if isinstance(lap, pd.DataFrame):
        tel = lap
    elif hasattr(lap, "get_car_data"):
        tel = lap.get_car_data()
    elif hasattr(lap, "get_telemetry"):
        tel = lap.get_telemetry()
    else:
        tel = getattr(lap, "telemetry", None)
    if tel is None:
        return pd.DataFrame()
    if not isinstance(tel, pd.DataFrame) and hasattr(tel, "to_pandas"):
        tel = tel.to_pandas()  # polars backend
    if "Distance" not in tel.columns and "Speed" in tel.columns:
        from .models import Telemetry

        tel = Telemetry(tel).add_distance()
    return tel


def _seconds(values: pd.Series) -> np.ndarray:
    """Time channel as float seconds (timedelta or numeric input)."""
    if pd.api.types.is_timedelta64_dtype(values):
        return values.dt.total_seconds().to_numpy(dtype="float64")
    return pd.to_numeric(values, errors="coerce").to_numpy(dtype="float64")


def _extend_stream(stream: np.ndarray) -> np.ndarray:
    """Pad one extrapolated sample on each side so the lap edges interpolate."""
    return np.concatenate(
        [[stream[0] - (stream[1] - stream[0])], stream, [stream[-1] + (stream[-1] - stream[-2])]]
    )


def delta_time(reference_lap, compare_lap):
    """Time delta of ``compare_lap`` against ``reference_lap`` along the track.

    Same algorithm as ``fastf1.utils.delta_time``: the comparison lap's time is
    interpolated onto the reference lap's distance axis, so a positive delta at
    a point means the comparison lap reached that point later (is slower).

    Args:
        reference_lap: Lap series (``laps.pick_fastest()``), ``Driver.get_lap``
            result, or a telemetry frame with ``Time`` and ``Distance``.
        compare_lap: Same kinds as ``reference_lap``.

    Returns:
        ``(delta, reference_telemetry, compare_telemetry)``: ``delta`` is a
        float Series of seconds aligned to the reference telemetry's index
        (empty when either lap has fewer than two samples).
    """
    ref_tel = _lap_car_data(reference_lap)
    comp_tel = _lap_car_data(compare_lap)
    needed = {"Time", "Distance"}
    if (
        len(ref_tel) < 2
        or len(comp_tel) < 2
        or not needed.issubset(ref_tel.columns)
        or not needed.issubset(comp_tel.columns)
    ):
        return pd.Series(dtype="float64"), ref_tel, comp_tel

    comp_time = _seconds(comp_tel["Time"])
    comp_dist = pd.to_numeric(comp_tel["Distance"], errors="coerce").to_numpy(dtype="float64")
    valid = np.isfinite(comp_time) & np.isfinite(comp_dist)
    if valid.sum() < 2:
        return pd.Series(dtype="float64"), ref_tel, comp_tel

    ref_time = _seconds(ref_tel["Time"])
    ref_dist = pd.to_numeric(ref_tel["Distance"], errors="coerce").to_numpy(dtype="float64")
    lap_time = np.interp(
        ref_dist, _extend_stream(comp_dist[valid]), _extend_stream(comp_time[valid])
    )
    delta = pd.Series(lap_time - ref_time, index=ref_tel.index, dtype="float64")
    return delta, ref_tel, comp_tel


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
