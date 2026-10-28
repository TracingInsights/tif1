"""Ergast data for tif1: ``from tif1.ergast import Ergast`` (fastf1 parity).

The classic Ergast tables (results, qualifying, sprint results, lap timings,
pit stops, standings, schedules, drivers, constructors, circuits, finishing
status, seasons) are served from the ``TracingInsights-Archive/Stats``
repository first and from the Jolpica API only for what Stats cannot answer.
The package imports lazily so ``import tif1.ergast`` does not load pandas.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tif1.ergast.interface import (
        BASE_URL,
        HEADERS,
        TIMEOUT,
        Ergast,
        ErgastMultiResponse,
        ErgastPolarsResponse,
        ErgastRawResponse,
        ErgastResponseMixin,
        ErgastResultFrame,
        ErgastResultSeries,
        ErgastSimpleResponse,
    )
    from tif1.ergast.legacy import fetch_day, fetch_results, fetch_season

__all__ = [
    "BASE_URL",
    "HEADERS",
    "TIMEOUT",
    "Ergast",
    "ErgastMultiResponse",
    "ErgastPolarsResponse",
    "ErgastRawResponse",
    "ErgastResponseMixin",
    "ErgastResultFrame",
    "ErgastResultSeries",
    "ErgastSimpleResponse",
    "fetch_day",
    "fetch_results",
    "fetch_season",
    "interface",
    "jolpica",
    "legacy",
    "payloads",
    "router",
    "stats",
    "structure",
]

_LAZY_EXPORTS: dict[str, tuple[str, str | None]] = {
    "BASE_URL": ("tif1.ergast.interface", "BASE_URL"),
    "HEADERS": ("tif1.ergast.interface", "HEADERS"),
    "TIMEOUT": ("tif1.ergast.interface", "TIMEOUT"),
    "Ergast": ("tif1.ergast.interface", "Ergast"),
    "ErgastMultiResponse": ("tif1.ergast.interface", "ErgastMultiResponse"),
    "ErgastPolarsResponse": ("tif1.ergast.interface", "ErgastPolarsResponse"),
    "ErgastRawResponse": ("tif1.ergast.interface", "ErgastRawResponse"),
    "ErgastResponseMixin": ("tif1.ergast.interface", "ErgastResponseMixin"),
    "ErgastResultFrame": ("tif1.ergast.interface", "ErgastResultFrame"),
    "ErgastResultSeries": ("tif1.ergast.interface", "ErgastResultSeries"),
    "ErgastSimpleResponse": ("tif1.ergast.interface", "ErgastSimpleResponse"),
    "fetch_day": ("tif1.ergast.legacy", "fetch_day"),
    "fetch_results": ("tif1.ergast.legacy", "fetch_results"),
    "fetch_season": ("tif1.ergast.legacy", "fetch_season"),
    "interface": ("tif1.ergast.interface", None),
    "jolpica": ("tif1.ergast.jolpica", None),
    "legacy": ("tif1.ergast.legacy", None),
    "payloads": ("tif1.ergast.payloads", None),
    "router": ("tif1.ergast.router", None),
    "stats": ("tif1.ergast.stats", None),
    "structure": ("tif1.ergast.structure", None),
}


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'tif1.ergast' has no attribute {name!r}")
    module_name, attr_name = target
    module = import_module(module_name)
    value = module if attr_name is None else getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
