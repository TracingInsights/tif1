"""MultiViewer API tier (``fastf1.mvapi`` parity).

* :func:`get_circuit` - the raw per-year layout payload (cached permanently).
* :func:`get_circuit_info` - fastf1's :class:`CircuitInfo` parsing of it.
* :func:`get_circuits_index` - ``/api/v1/circuits``, every circuit MultiViewer knows.
* :class:`CircuitInfo` - :class:`tif1.models.CircuitInfo` (same class).
"""

from .api import get_circuit, get_circuits_index
from .data import CircuitInfo, get_circuit_info

__all__ = ["CircuitInfo", "get_circuit", "get_circuit_info", "get_circuits_index"]
