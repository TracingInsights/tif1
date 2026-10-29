"""fastf1's ``fastf1.legacy`` module: :func:`inject_driver_ahead`.

fastf1 keeps this module for scripts written before ``Telemetry`` gained
``add_driver_ahead``. tif1's telemetry already carries ``DriverAhead`` and
``DistanceToDriverAhead`` (computed upstream), so the shim only collects them.
"""

from typing import Any

__all__ = ["inject_driver_ahead"]


def inject_driver_ahead(session: Any) -> dict[str, Any]:
    """Each driver's position data with ``DriverAhead`` and ``DistanceToDriverAhead``.

    Args:
        session: A tif1 session.

    Returns:
        A dict keyed by driver number (:attr:`Session.drivers` labels): the
        driver's session-long ``session.pos_data`` telemetry, which carries
        the two driver-ahead columns.

    Raises:
        ValueError: No driver of the session has telemetry (fastf1's error).
    """
    result: dict[str, Any] = {}
    for number in session.drivers:
        telemetry = session.pos_data[number].add_driver_ahead()
        result[number] = telemetry
    if not any(_has_rows(telemetry) for telemetry in result.values()):
        raise ValueError("No valid telemetry for calculating distance to driver ahead!")
    return result


def _has_rows(frame: Any) -> bool:
    height = getattr(frame, "height", None)
    return bool(height) if height is not None else not getattr(frame, "empty", True)
