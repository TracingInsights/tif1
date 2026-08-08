"""Shared acceleration helpers for :mod:`tif1.charts`.

These helpers were extracted verbatim from the standalone
``docs/assets/generate_gg_diagram.py`` script (the numerically safer variant)
and are shared by :func:`tif1.charts.plot_gg_diagram` and
:func:`tif1.charts.plot_telemetry_comparison`.
"""

from __future__ import annotations

import math

import numpy as np


def smooth_derivative(t_in, v_in) -> np.ndarray:
    """Compute a smooth estimation of a derivative using low-noise differentiators.

    Reference: http://holoborodko.com/pavel/numerical-methods/

    Args:
        t_in: Independent variable (time, distance, ...). Pandas timedelta
            values are converted to seconds in place when possible.
        v_in: Dependent variable to differentiate.

    Returns:
        The derivative ``dv/dt`` as a numpy array of the same length.
    """
    t = t_in.copy()
    v = v_in.copy()

    # Transform time to seconds when the input is a pandas timedelta series.
    try:
        if hasattr(t, "dt") and hasattr(t.dt, "total_seconds"):
            t = t.dt.total_seconds()
        else:
            for i in range(t.size):
                t.iloc[i] = t.iloc[i].total_seconds()
    except Exception:
        pass

    t = np.array(t)
    v = np.array(v)

    assert t.size == v.size

    dvdt = np.zeros(t.size)

    # Manually compute boundary points
    dvdt[0] = (v[1] - v[0]) / (t[1] - t[0])
    dvdt[1] = (v[2] - v[0]) / (t[2] - t[0])
    dvdt[2] = (v[3] - v[1]) / (t[3] - t[1])

    n = t.size
    dvdt[n - 1] = (v[n - 1] - v[n - 2]) / (t[n - 1] - t[n - 2])
    dvdt[n - 2] = (v[n - 1] - v[n - 3]) / (t[n - 1] - t[n - 3])
    dvdt[n - 3] = (v[n - 2] - v[n - 4]) / (t[n - 2] - t[n - 4])

    # Compute interior points with the smooth low-noise method
    c = [5.0 / 32.0, 4.0 / 32.0, 1.0 / 32.0]
    for i in range(3, t.size - 3):
        for j in range(1, 4):
            dvdt[i] += 2 * j * c[j - 1] * (v[i + j] - v[i - j]) / (t[i + j] - t[i - j])

    return dvdt


def truncated_remainder(dividend: float, divisor: float) -> float:
    """Calculate truncated remainder."""
    divided_number = dividend / divisor
    divided_number = -int(-divided_number) if divided_number < 0 else int(divided_number)
    remainder = dividend - divisor * divided_number
    return remainder


def transform_to_pipi(input_angle: float) -> tuple[float, int]:
    """Transform an angle into the ``[-pi, pi]`` range.

    Returns:
        Tuple of ``(output_angle, revolutions)``.
    """
    pi = math.pi
    revolutions = int((input_angle + np.sign(input_angle) * pi) / (2 * pi))
    p1 = truncated_remainder(input_angle + np.sign(input_angle) * pi, 2 * pi)
    p2 = (
        np.sign(
            np.sign(input_angle)
            + 2
            * (np.sign(math.fabs((truncated_remainder(input_angle + pi, 2 * pi)) / (2 * pi))) - 1)
        )
        * pi
    )
    output_angle = p1 - p2
    return output_angle, revolutions


def remove_acceleration_outliers(acc: np.ndarray) -> np.ndarray:
    """Remove unrealistic acceleration values (> 7.5 g).

    The first outlier sample is zeroed, interior outliers are carried forward
    from the previous sample, and the last sample falls back to the previous
    value (preserves the envelope shape better than clipping).
    """
    acc_threshold_g = 7.5
    if math.fabs(acc[0]) > acc_threshold_g:
        acc[0] = 0.0
    for i in range(1, acc.size - 1):
        if math.fabs(acc[i]) > acc_threshold_g:
            acc[i] = acc[i - 1]
    if math.fabs(acc[-1]) > acc_threshold_g:
        acc[-1] = acc[-2]
    return acc


def compute_accelerations(telemetry) -> tuple[np.ndarray, np.ndarray]:
    """Calculate longitudinal and lateral accelerations in g.

    Longitudinal acceleration is derived from the speed trace; lateral
    acceleration from the curvature of the ``X/Y`` position over ``Distance``.

    Args:
        telemetry: Telemetry frame with ``Speed``, ``Time``, ``X``, ``Y`` and
            ``Distance`` columns (``add_distance()`` must already be applied).

    Returns:
        Tuple of ``(lon_acc, lat_acc)`` numpy arrays in units of g.
    """
    v = np.array(telemetry["Speed"]) / 3.6
    lon_acc = smooth_derivative(telemetry["Time"], v) / 9.81

    dx = smooth_derivative(telemetry["Distance"], telemetry["X"])
    dy = smooth_derivative(telemetry["Distance"], telemetry["Y"])

    theta = np.zeros(dx.size)
    theta[0] = math.atan2(dy[0], dx[0])
    for i in range(dx.size):
        theta[i] = theta[i - 1] + transform_to_pipi(math.atan2(dy[i], dx[i]) - theta[i - 1])[0]

    kappa = smooth_derivative(telemetry["Distance"], theta)
    lat_acc = v * v * kappa / 9.81

    # Remove outliers
    lon_acc = remove_acceleration_outliers(lon_acc)
    lat_acc = remove_acceleration_outliers(lat_acc)

    return lon_acc, lat_acc
