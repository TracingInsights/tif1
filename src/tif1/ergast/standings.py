"""Championship standings as frames (``tif1.get_driver_standings`` and friends).

Thin wrappers over the Ergast router: the standings after a given round of a
season (the last completed round when ``round`` is omitted; the final
standings for a past season), as pandas or polars frames with tif1's
PascalCase columns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from tif1.core_utils.ergast_results import build_standings_frame
from tif1.ergast import structure
from tif1.ergast.router import QueryPlan, execute

if TYPE_CHECKING:
    from tif1.types import DataFrame


def _standings_payload(kind: str, year: int, round_number: int | None) -> dict[str, Any]:
    endpoint = "driverStandings" if kind == "driver" else "constructorStandings"
    category = (
        structure.StandingsLists_Driver
        if kind == "driver"
        else structure.StandingsLists_Constructor
    )
    subcategory = structure.DriverStandings if kind == "driver" else structure.ConstructorStandings
    selectors: dict[str, Any] = {"season": year}
    if round_number is not None:
        selectors["round"] = round_number
    return execute(
        QueryPlan(
            endpoint=endpoint,
            table="StandingsTable",
            category=category,
            subcategory=subcategory,
            selectors=selectors,
        )
    ).payload


def _as_lib(frame: Any, lib: Literal["pandas", "polars"] | None) -> DataFrame:
    from tif1.config import get_config

    resolved = lib if lib is not None else get_config().get("lib", "pandas")
    if resolved == "polars":
        from tif1.core_utils.backend_conversion import pandas_to_polars

        return pandas_to_polars(frame)
    return frame


def get_driver_standings(
    year: int,
    round: int | None = None,  # noqa: A002
    *,
    lib: Literal["pandas", "polars"] | None = None,
) -> DataFrame:
    """Drivers' championship standings after ``round`` of ``year``.

    Without ``round`` the standings after the last completed round are
    returned (the final standings of a past season).

    Args:
        year: Championship year (1950 onward).
        round: Championship round (default: last completed round).
        lib: ``"pandas"`` (default) or ``"polars"``.

    Returns:
        One row per driver with ``Position``, ``PositionText``, ``Points``,
        ``Wins``, ``DriverNumber``, ``Abbreviation``, ``DriverId``,
        ``FirstName``, ``LastName``, ``FullName``, ``DateOfBirth``,
        ``DriverNationality``, ``TeamIds``, ``TeamNames`` and ``DriverUrl``.
    """
    return _as_lib(build_standings_frame(_standings_payload("driver", year, round), "driver"), lib)


def get_constructor_standings(
    year: int,
    round: int | None = None,  # noqa: A002
    *,
    lib: Literal["pandas", "polars"] | None = None,
) -> DataFrame:
    """Constructors' championship standings after ``round`` of ``year``.

    Args:
        year: Championship year (1958 onward; earlier seasons had no constructors' title).
        round: Championship round (default: last completed round).
        lib: ``"pandas"`` (default) or ``"polars"``.

    Returns:
        One row per constructor with ``Position``, ``PositionText``,
        ``Points``, ``Wins``, ``TeamId``, ``TeamName``, ``TeamNationality``
        and ``TeamUrl``.
    """
    return _as_lib(
        build_standings_frame(_standings_payload("constructor", year, round), "constructor"), lib
    )


__all__ = ["get_constructor_standings", "get_driver_standings"]
