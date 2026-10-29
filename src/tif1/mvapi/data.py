"""MultiViewer payload -> :class:`CircuitInfo` (``fastf1.mvapi.data`` parity)."""

from __future__ import annotations

import logging

from tif1.core_utils.circuit_info import circuit_info_from_multiviewer
from tif1.models import CircuitInfo

from .api import get_circuit

logger = logging.getLogger(__name__)


def get_circuit_info(*, year: int, circuit_key: int, lib: str = "pandas") -> CircuitInfo | None:
    """Load circuit information from the MultiViewer API.

    fastf1's parser: ``corners``, ``marshal_lights`` and ``marshal_sectors``
    as ``X, Y, Number, Letter, Angle, Distance`` frames (``Distance`` missing
    until :meth:`CircuitInfo.add_marker_distance` fits it) and ``rotation``;
    the tif1 extras (``circuit_key``, ``circuit_name``, ``location``,
    ``country``, ``country_code``, ``country_key``, ``pit_loss``, ``track``,
    ``mini_sector_index``) come from the same payload and ``source`` is
    ``"multiviewer"``.

    Args:
        year: The championship year.
        circuit_key: The unique circuit key (defined by the F1 live-timing API).
        lib: ``"pandas"`` (default) or ``"polars"`` frames.

    Returns:
        The circuit info, or ``None`` when the API did not answer.
    """
    data = get_circuit(year=year, circuit_key=circuit_key)
    if not data:
        logger.warning("Failed to load circuit info for circuit %s/%s", circuit_key, year)
        return None
    return circuit_info_from_multiviewer(data, lib=lib)


__all__ = ["CircuitInfo", "get_circuit_info"]
