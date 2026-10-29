"""The :class:`Ergast` interface and its response classes (fastf1 parity).

``from tif1.ergast import Ergast`` is a drop-in replacement for
``fastf1.ergast.Ergast``: the thirteen endpoint methods keep fastf1's
signatures, the flattened column names come from ``tif1.ergast.structure``
and the response objects carry the same paging attributes. Underneath, the
query is routed to the ``TracingInsights-Archive/Stats`` repository first
and to Jolpica only when Stats cannot answer (see ``tif1.ergast.router``).

Differences from fastf1 (all additive):

- ``limit=None`` (the default) returns every row when Stats answered; a
  Jolpica-backed answer keeps the API default of 30. Pass ``limit`` to page.
- ``result_type="polars"`` returns polars frames: :class:`ErgastPolarsResponse`
  (a polars ``DataFrame`` with the paging attributes) for simple endpoints,
  an :class:`ErgastMultiResponse` of polars frames otherwise.
- ``source`` (``"auto"``, ``"stats"``, ``"jolpica"``) pins the data source;
  every response reports the one that answered through ``.source``.
- ``totalRaceTime`` is the true total (from ``Time.millis``) and
  ``totalRaceTimeGap`` the gap to the winner.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, cast

import pandas as pd

from tif1.ergast import structure
from tif1.ergast.jolpica import DEFAULT_BASE_URL, JolpicaClient, build_url
from tif1.ergast.router import QueryPlan, execute
from tif1.http_session import user_agent
from tif1.types import ErgastResultType, ErgastSourceType

if TYPE_CHECKING:
    import polars as pl

BASE_URL = DEFAULT_BASE_URL
TIMEOUT = 5.0
#: The User-Agent every tif1 request carries (fastf1 exposes the same name).
HEADERS = {"User-Agent": user_agent()}


class ErgastResponseMixin:
    """Adds response metadata and paging to every Ergast response object.

    ``total_results``, ``is_complete``, ``get_next_result_page`` and
    ``get_prev_result_page`` behave as in fastf1; ``source`` names the data
    source that answered (``"stats"`` or ``"jolpica"``).
    """

    _internal_names: ClassVar[list[str]] = [
        "_response_headers",
        "_query_filters",
        "_query_metadata",
        "_selectors",
        "_source",
    ]

    def __init__(
        self,
        *args: Any,
        response_headers: dict,
        query_filters: dict,
        metadata: dict,
        selectors: dict,
        source: str = "",
        **kwargs: Any,
    ) -> None:
        cast(Any, super()).__init__(*args, **kwargs)
        self._response_headers = response_headers
        self._query_filters = query_filters
        self._query_metadata = metadata
        self._selectors = selectors
        self._source = source

    @property
    def _ergast_constructor(self) -> type[Ergast]:
        return Ergast

    @property
    def source(self) -> str:
        """The data source that answered: ``"stats"`` or ``"jolpica"``."""
        return self._source

    @property
    def total_results(self) -> int:
        """Total number of results available for the request behind this response."""
        return int(self._response_headers.get("total", 0))

    @property
    def is_complete(self) -> bool:
        """True when the response holds every available result."""
        if int(self._response_headers.get("offset", 0)) != 0:
            return False
        return int(self._response_headers.get("limit", 0)) >= int(
            self._response_headers.get("total", 0)
        )

    def get_next_result_page(
        self,
    ) -> ErgastSimpleResponse | ErgastMultiResponse | ErgastRawResponse:
        """The next page of results within this response's limit.

        Raises:
            ValueError: there is no page after the current one.
        """
        n_last = int(self._response_headers.get("offset", 0)) + int(
            self._response_headers.get("limit", 0)
        )
        if n_last >= int(self._response_headers.get("total", 0)):
            raise ValueError("No more data after this response.")
        return self._ergast_constructor()._build_default_result(
            **self._query_metadata,
            selectors=self._selectors,
            limit=int(self._response_headers.get("limit", 0)),
            offset=n_last,
        )

    def get_prev_result_page(
        self,
    ) -> ErgastSimpleResponse | ErgastMultiResponse | ErgastRawResponse:
        """The previous page of results within this response's limit.

        Raises:
            ValueError: there is no page before the current one.
        """
        n_first = int(self._response_headers.get("offset", 0))
        if n_first <= 0:
            raise ValueError("No more data before this response.")
        limit = int(self._response_headers.get("limit", 0))
        return self._ergast_constructor()._build_default_result(
            **self._query_metadata,
            selectors=self._selectors,
            limit=limit,
            offset=max(n_first - limit, 0),
        )


class ErgastResultFrame(pd.DataFrame):
    """A pandas ``DataFrame`` built from flattened Ergast response data.

    Args:
        data: Passed to the DataFrame constructor (``None`` when ``response`` is given).
        category: A category from ``tif1.ergast.structure`` describing ``response``.
        response: JSON-like Ergast response data to flatten into ``data``.
        auto_cast: Cast values from their string form to the matching type.
    """

    _metadata: ClassVar[list[str]] = []

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow DataFrame-style subclass construction with the ``response`` keywords."""
        return super().__new__(cls)

    def __init__(
        self,
        data: Any = None,
        *,
        category: dict | None = None,
        response: list | None = None,
        auto_cast: bool = True,
        **kwargs: Any,
    ) -> None:
        if data is not None and response is not None:
            raise ValueError(f"Cannot initialize {type(self)} with `data` and `response`.")
        if data is None and response is not None:
            if category is None:
                raise ValueError("`category` is required to build a frame from `response`.")
            data = self._prepare_response(response, category, auto_cast)
        cast(Any, super()).__init__(data, **kwargs)

    @classmethod
    def _prepare_response(cls, response: list, category: dict, auto_cast: bool) -> dict[str, list]:
        return structure.flatten_rows(response, category, auto_cast)

    @property
    def _constructor(self):
        return ErgastResultFrame

    @property
    def _constructor_sliced(self):
        return ErgastResultSeries

    @property
    def base_class_view(self) -> pd.DataFrame:
        """The same data as a plain :class:`pandas.DataFrame` (fastf1 parity)."""
        return pd.DataFrame(self)


class ErgastResultSeries(pd.Series):
    """A pandas ``Series`` row/column of an :class:`ErgastResultFrame`."""

    _metadata: ClassVar[list[str]] = []

    @property
    def _constructor(self):
        return ErgastResultSeries

    @property
    def _constructor_expanddim(self):
        return ErgastResultFrame


class ErgastRawResponse(ErgastResponseMixin, list):
    """The raw JSON-like data part of an Ergast response (a ``list`` with paging).

    Metadata (version, query parameters, response length) is not part of the
    list but drives :class:`ErgastResponseMixin`.

    Args:
        category: A category from ``tif1.ergast.structure`` describing the data.
        auto_cast: Cast values from their string form to the matching type.
    """

    def __init__(self, *, query_result: list, category: dict, auto_cast: bool, **kwargs: Any):
        # The query result may be shared with the cache: never hand out the originals.
        query_result = copy.deepcopy(query_result)
        if auto_cast:
            query_result = structure.auto_cast(query_result, category)
        super().__init__(query_result, **kwargs)


class ErgastSimpleResponse(ErgastResponseMixin, ErgastResultFrame):
    """Simple Ergast result data as a pandas ``DataFrame`` with paging."""

    _internal_names: ClassVar[list[str]] = [
        *getattr(pd.DataFrame, "_internal_names", []),
        *ErgastResponseMixin._internal_names,
    ]
    _internal_names_set: ClassVar[set[str]] = set(_internal_names)

    @property
    def _constructor(self) -> type[ErgastResultFrame]:
        # A slice is no longer a full response: drop the paging mixin.
        return ErgastResultFrame


_POLARS_RESPONSE: type | None = None


def _polars_response_type() -> type:
    """:class:`ErgastPolarsResponse`, built on first use (importing this module never imports polars)."""
    global _POLARS_RESPONSE
    if _POLARS_RESPONSE is None:
        import polars as pl

        class ErgastPolarsResponse(ErgastResponseMixin, pl.DataFrame):
            """Simple Ergast result data as a polars ``DataFrame`` with paging.

            The polars twin of :class:`ErgastSimpleResponse`: ``source``,
            ``total_results``, ``is_complete`` and the page methods behave
            the same. Frames derived from it (``head``, ``filter``, slices)
            are plain polars frames without the metadata, as pandas slices
            of a simple response are plain ``ErgastResultFrame`` objects.
            """

            @classmethod
            def _from_pydf(cls, py_df: Any) -> Any:
                # A derived frame is no longer a full response.
                return pl.DataFrame._from_pydf(py_df)

        ErgastPolarsResponse.__module__ = __name__
        _POLARS_RESPONSE = ErgastPolarsResponse
    return _POLARS_RESPONSE


def __getattr__(name: str) -> Any:
    if name == "ErgastPolarsResponse":
        return _polars_response_type()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class ErgastMultiResponse(ErgastResponseMixin):
    """Complex Ergast result data as multiple frames with paging.

    ``description`` holds one row per element of ``content``; each element of
    ``content`` is the frame of the main response data for that row.
    ``lib="polars"`` makes both polars frames.

    Args:
        response_description: The descriptive part of the response.
        response_data: The content data split from the response, one per description row.
        category: Category of the description (``tif1.ergast.structure``).
        subcategory: Category of the content data.
        auto_cast: Cast values from their string form to the matching type.
        lib: ``"pandas"`` (default) or ``"polars"``.
    """

    def __init__(
        self,
        *args: Any,
        response_description: list,
        response_data: list,
        category: dict,
        subcategory: dict,
        auto_cast: bool,
        lib: Literal["pandas", "polars"] = "pandas",
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        description = ErgastResultFrame(
            response=response_description, category=category, auto_cast=auto_cast
        )
        content = [
            ErgastResultFrame(response=elem, category=subcategory, auto_cast=auto_cast)
            for elem in response_data
        ]
        if lib == "polars":
            from tif1.core_utils.backend_conversion import pandas_to_polars

            self._description: Any = pandas_to_polars(pd.DataFrame(cast(Any, description)))
            self._content: list[Any] = [
                pandas_to_polars(pd.DataFrame(cast(Any, c))) for c in content
            ]
        else:
            self._description = description
            self._content = content
        self.lib = lib

    @property
    def description(self) -> ErgastResultFrame | pl.DataFrame:
        """One row of descriptive data per element of :attr:`content`."""
        return self._description

    @property
    def content(self) -> list[ErgastResultFrame] | list[pl.DataFrame]:
        """The main response data, one frame per row of :attr:`description`."""
        return self._content


class Ergast:
    """Interface to the Ergast data (Stats repository first, Jolpica fallback).

    Args:
        result_type: Default type of the returned objects: ``"pandas"``
            (:class:`ErgastSimpleResponse`/:class:`ErgastMultiResponse`),
            ``"raw"`` (:class:`ErgastRawResponse`) or ``"polars"``
            (:class:`ErgastPolarsResponse`, or an :class:`ErgastMultiResponse`
            of polars frames).
        auto_cast: Cast values from their string form to the matching type.
        limit: Default maximum number of results. ``None`` returns every
            result when the Stats repository answered and the API default
            (30) when Jolpica did.
        source: ``"auto"`` (route per query), ``"stats"`` (Stats only) or
            ``"jolpica"`` (Jolpica only).
    """

    def __init__(
        self,
        result_type: ErgastResultType = "pandas",
        auto_cast: bool = True,
        limit: int | None = None,
        *,
        source: ErgastSourceType = "auto",
    ) -> None:
        if result_type not in ("pandas", "raw", "polars"):
            raise ValueError(f"Unknown result_type {result_type!r}")
        if source not in ("auto", "stats", "jolpica"):
            raise ValueError(f"Unknown source {source!r}")
        self._default_result_type = result_type
        self._default_auto_cast = auto_cast
        self._limit = limit
        self._source = source

    @staticmethod
    def _build_url(
        endpoint: str,
        season: Literal["current"] | int | None = None,
        round: Literal["last"] | int | None = None,  # noqa: A002
        circuit: str | None = None,
        constructor: str | None = None,
        driver: str | None = None,
        grid_position: int | None = None,
        results_position: int | None = None,
        fastest_rank: int | None = None,
        status: str | None = None,
        lap_number: int | None = None,
        stop_number: int | None = None,
        standings_position: int | None = None,
    ) -> str:
        """The Jolpica request URL for ``endpoint`` and selectors (fastf1 parity)."""
        return build_url(
            endpoint,
            season=season,
            round=round,
            circuit=circuit,
            constructor=constructor,
            driver=driver,
            grid_position=grid_position,
            results_position=results_position,
            fastest_rank=fastest_rank,
            status=status,
            lap_number=lap_number,
            stop_number=stop_number,
            standings_position=standings_position,
        )

    @classmethod
    def _get(cls, url: str, params: dict) -> dict | list:
        """Request ``url`` from Jolpica directly (rate-limited, cached)."""
        return JolpicaClient().get(url, params)

    def _build_result(
        self,
        *,
        endpoint: str,
        table: str,
        category: dict,
        subcategory: dict | None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
        selectors: dict | None = None,
        source: str | None = None,
    ) -> Any:
        """Route the query and wrap the answer in the requested response type."""
        selectors = dict(selectors or {})
        result_type = result_type or "pandas"
        auto_cast = True if auto_cast is None else auto_cast
        source = source or self._source

        executed = execute(
            QueryPlan(
                endpoint=endpoint,
                table=table,
                category=category,
                subcategory=subcategory,
                selectors=selectors,
                limit=limit,
                offset=offset,
                source=source,
            )
        )
        mrdata = executed.payload["MRData"]
        body = mrdata.get(table, {})
        response_headers = {k: v for k, v in mrdata.items() if k != table}
        query_result = body.get(category["name"], [])
        query_filters = {k: v for k, v in body.items() if k != category["name"]}

        query_metadata = {
            "endpoint": endpoint,
            "table": table,
            "category": category,
            "subcategory": subcategory,
            "result_type": result_type,
            "auto_cast": auto_cast,
            "source": source,
        }
        common = {
            "response_headers": response_headers,
            "query_filters": query_filters,
            "metadata": query_metadata,
            "selectors": selectors,
            "source": executed.source,
        }

        if result_type == "raw":
            return ErgastRawResponse(
                query_result=query_result, category=category, auto_cast=auto_cast, **common
            )

        lib: Literal["pandas", "polars"] = "polars" if result_type == "polars" else "pandas"
        if subcategory is not None:
            name = subcategory["name"]
            description = [{k: v for k, v in elem.items() if k != name} for elem in query_result]
            content = [elem.get(name, []) for elem in query_result]
            return ErgastMultiResponse(
                response_description=description,
                response_data=content,
                category=category,
                subcategory=subcategory,
                auto_cast=auto_cast,
                lib=lib,
                **common,
            )
        if lib == "polars":
            from tif1.core_utils.backend_conversion import pandas_to_polars

            frame = ErgastResultFrame(response=query_result, category=category, auto_cast=auto_cast)
            polars_frame = pandas_to_polars(pd.DataFrame(cast(Any, frame)))
            return _polars_response_type()(polars_frame, **common)
        return ErgastSimpleResponse(
            response=query_result, category=category, auto_cast=auto_cast, **common
        )

    def _build_default_result(
        self,
        *,
        selectors: dict,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """:meth:`_build_result` with the instance defaults filled in."""
        if result_type is None:
            result_type = self._default_result_type
        if auto_cast is None:
            auto_cast = self._default_auto_cast
        if limit is None:
            limit = self._limit
        return self._build_result(
            result_type=result_type, auto_cast=auto_cast, limit=limit, selectors=selectors, **kwargs
        )

    # ### endpoints with single-result responses ###

    def get_seasons(
        self,
        circuit: str | None = None,
        constructor: str | None = None,
        driver: str | None = None,
        grid_position: int | None = None,
        results_position: int | None = None,
        fastest_rank: int | None = None,
        status: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastSimpleResponse | ErgastRawResponse:
        """Seasons (always from Jolpica: the Stats repository has no season list).

        Args:
            circuit: select a circuit by its circuit id (default: all)
            constructor: select a constructor by its constructor id (default: all)
            driver: select a driver by its driver id (default: all)
            grid_position: select a grid position by its number (default: all)
            results_position: select a finishing result by its position (default: all)
            fastest_rank: select fastest by rank number (default: all)
            status: select by finishing status (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastSimpleResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "circuit": circuit,
            "constructor": constructor,
            "driver": driver,
            "grid_position": grid_position,
            "results_position": results_position,
            "fastest_rank": fastest_rank,
            "status": status,
        }
        return self._build_default_result(
            endpoint="seasons",
            table="SeasonTable",
            category=structure.Seasons,
            subcategory=None,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_race_schedule(
        self,
        season: Literal["current"] | int,
        round: Literal["last"] | int | None = None,  # noqa: A002
        circuit: str | None = None,
        constructor: str | None = None,
        driver: str | None = None,
        grid_position: int | None = None,
        results_position: int | None = None,
        fastest_rank: int | None = None,
        status: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastSimpleResponse | ErgastRawResponse:
        """Race schedule of a season (Stats ``events.json``, Jolpica fallback).

        Args:
            season: select a season by its year
            round: select a round by its number
            circuit: select a circuit by its circuit id (default: all)
            constructor: select a constructor by its constructor id (default: all)
            driver: select a driver by its driver id (default: all)
            grid_position: select a grid position by its number (default: all)
            results_position: select a finishing result by its position (default: all)
            fastest_rank: select fastest by rank number (default: all)
            status: select by finishing status (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastSimpleResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "circuit": circuit,
            "constructor": constructor,
            "driver": driver,
            "grid_position": grid_position,
            "results_position": results_position,
            "fastest_rank": fastest_rank,
            "status": status,
        }
        return self._build_default_result(
            endpoint="races",
            table="RaceTable",
            category=structure.Races_Schedule,
            subcategory=None,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_driver_info(
        self,
        season: Literal["current"] | int | None = None,
        round: Literal["last"] | int | None = None,  # noqa: A002
        circuit: str | None = None,
        constructor: str | None = None,
        driver: str | None = None,
        grid_position: int | None = None,
        results_position: int | None = None,
        fastest_rank: int | None = None,
        status: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastSimpleResponse | ErgastRawResponse:
        """Driver information (always from Jolpica: it lists practice-only drivers too).

        Args:
            season: select a season by its year (default: all)
            round: select a round by its number (default: all)
            circuit: select a circuit by its circuit id (default: all)
            constructor: select a constructor by its constructor id (default: all)
            driver: select a driver by its driver id (default: all)
            grid_position: select a grid position by its number (default: all)
            results_position: select a finishing result by its position (default: all)
            fastest_rank: select fastest by rank number (default: all)
            status: select by finishing status (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastSimpleResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "circuit": circuit,
            "constructor": constructor,
            "driver": driver,
            "grid_position": grid_position,
            "results_position": results_position,
            "fastest_rank": fastest_rank,
            "status": status,
        }
        return self._build_default_result(
            endpoint="drivers",
            table="DriverTable",
            category=structure.Drivers,
            subcategory=None,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_constructor_info(
        self,
        season: Literal["current"] | int | None = None,
        round: Literal["last"] | int | None = None,  # noqa: A002
        circuit: str | None = None,
        constructor: str | None = None,
        driver: str | None = None,
        grid_position: int | None = None,
        results_position: int | None = None,
        fastest_rank: int | None = None,
        status: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastSimpleResponse | ErgastRawResponse:
        """Constructor information (derived from the season's Stats results, Jolpica fallback).

        Args:
            season: select a season by its year (default: all)
            round: select a round by its number (default: all)
            circuit: select a circuit by its circuit id (default: all)
            constructor: select a constructor by its constructor id (default: all)
            driver: select a driver by its driver id (default: all)
            grid_position: select a grid position by its number (default: all)
            results_position: select a finishing result by its position (default: all)
            fastest_rank: select fastest by rank number (default: all)
            status: select by finishing status (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastSimpleResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "circuit": circuit,
            "constructor": constructor,
            "driver": driver,
            "grid_position": grid_position,
            "results_position": results_position,
            "fastest_rank": fastest_rank,
            "status": status,
        }
        return self._build_default_result(
            endpoint="constructors",
            table="ConstructorTable",
            category=structure.Constructors,
            subcategory=None,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_circuits(
        self,
        season: Literal["current"] | int | None = None,
        round: Literal["last"] | int | None = None,  # noqa: A002
        constructor: str | None = None,
        driver: str | None = None,
        grid_position: int | None = None,
        results_position: int | None = None,
        fastest_rank: int | None = None,
        status: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastSimpleResponse | ErgastRawResponse:
        """Circuit information (from the season's Stats schedule, Jolpica fallback).

        Args:
            season: select a season by its year (default: all)
            round: select a round by its number (default: all)
            constructor: select a constructor by its constructor id (default: all)
            driver: select a driver by its driver id (default: all)
            grid_position: select a grid position by its number (default: all)
            results_position: select a finishing result by its position (default: all)
            fastest_rank: select fastest by rank number (default: all)
            status: select by finishing status (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastSimpleResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "constructor": constructor,
            "driver": driver,
            "grid_position": grid_position,
            "results_position": results_position,
            "fastest_rank": fastest_rank,
            "status": status,
        }
        return self._build_default_result(
            endpoint="circuits",
            table="CircuitTable",
            category=structure.Circuits,
            subcategory=None,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_finishing_status(
        self,
        season: Literal["current"] | int | None = None,
        round: Literal["last"] | int | None = None,  # noqa: A002
        circuit: str | None = None,
        constructor: str | None = None,
        driver: str | None = None,
        grid_position: int | None = None,
        results_position: int | None = None,
        fastest_rank: int | None = None,
        status: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastSimpleResponse | ErgastRawResponse:
        """Finishing status counts (always from Jolpica: Stats files carry no ``statusId``).

        Args:
            season: select a season by its year (default: all)
            round: select a round by its number (default: all)
            circuit: select a circuit by its circuit id (default: all)
            constructor: select a constructor by its constructor id (default: all)
            driver: select a driver by its driver id (default: all)
            grid_position: select a grid position by its number (default: all)
            results_position: select a finishing result by its position (default: all)
            fastest_rank: select fastest by rank number (default: all)
            status: select by finishing status (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastSimpleResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "circuit": circuit,
            "constructor": constructor,
            "driver": driver,
            "grid_position": grid_position,
            "results_position": results_position,
            "fastest_rank": fastest_rank,
            "status": status,
        }
        return self._build_default_result(
            endpoint="status",
            table="StatusTable",
            category=structure.Status,
            subcategory=None,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    # ### endpoints with multi-result responses ###

    def get_race_results(
        self,
        season: Literal["current"] | int | None = None,
        round: Literal["last"] | int | None = None,  # noqa: A002
        circuit: str | None = None,
        constructor: str | None = None,
        driver: str | None = None,
        grid_position: int | None = None,
        results_position: int | None = None,
        fastest_rank: int | None = None,
        status: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastMultiResponse | ErgastRawResponse:
        """Race results (Stats ``results.json`` per round, Jolpica fallback).

        Args:
            season: select a season by its year (default: all)
            round: select a round by its number (default: all)
            circuit: select a circuit by its circuit id (default: all)
            constructor: select a constructor by its constructor id (default: all)
            driver: select a driver by its driver id (default: all)
            grid_position: select a grid position by its number (default: all)
            results_position: select a finishing result by its position (default: all)
            fastest_rank: select fastest by rank number (default: all)
            status: select by finishing status (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastMultiResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "circuit": circuit,
            "constructor": constructor,
            "driver": driver,
            "grid_position": grid_position,
            "results_position": results_position,
            "fastest_rank": fastest_rank,
            "status": status,
        }
        return self._build_default_result(
            endpoint="results",
            table="RaceTable",
            category=structure.Races_RaceResults,
            subcategory=structure.RaceResults,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_qualifying_results(
        self,
        season: Literal["current"] | int | None = None,
        round: Literal["last"] | int | None = None,  # noqa: A002
        circuit: str | None = None,
        constructor: str | None = None,
        driver: str | None = None,
        grid_position: int | None = None,
        results_position: int | None = None,
        fastest_rank: int | None = None,
        status: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastMultiResponse | ErgastRawResponse:
        """Qualifying results (Stats ``quali_results.json`` per round, Jolpica fallback).

        Args:
            season: select a season by its year (default: all)
            round: select a round by its number (default: all)
            circuit: select a circuit by its circuit id (default: all)
            constructor: select a constructor by its constructor id (default: all)
            driver: select a driver by its driver id (default: all)
            grid_position: select a grid position by its number (default: all)
            results_position: select a finishing result by its position (default: all)
            fastest_rank: select fastest by rank number (default: all)
            status: select by finishing status (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastMultiResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "circuit": circuit,
            "constructor": constructor,
            "driver": driver,
            "grid_position": grid_position,
            "results_position": results_position,
            "fastest_rank": fastest_rank,
            "status": status,
        }
        return self._build_default_result(
            endpoint="qualifying",
            table="RaceTable",
            category=structure.Races_QualifyingResults,
            subcategory=structure.QualifyingResults,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_sprint_results(
        self,
        season: Literal["current"] | int | None = None,
        round: Literal["last"] | int | None = None,  # noqa: A002
        circuit: str | None = None,
        constructor: str | None = None,
        driver: str | None = None,
        grid_position: int | None = None,
        results_position: int | None = None,
        status: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastMultiResponse | ErgastRawResponse:
        """Sprint results (Stats ``sprint_results.json`` per sprint round, Jolpica fallback).

        Args:
            season: select a season by its year (default: all)
            round: select a round by its number (default: all)
            circuit: select a circuit by its circuit id (default: all)
            constructor: select a constructor by its constructor id (default: all)
            driver: select a driver by its driver id (default: all)
            grid_position: select a grid position by its number (default: all)
            results_position: select a finishing result by its position (default: all)
            status: select by finishing status (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastMultiResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "circuit": circuit,
            "constructor": constructor,
            "driver": driver,
            "grid_position": grid_position,
            "results_position": results_position,
            "status": status,
        }
        return self._build_default_result(
            endpoint="sprint",
            table="RaceTable",
            category=structure.Races_SprintResults,
            subcategory=structure.SprintResults,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_driver_standings(
        self,
        season: Literal["current"] | int | None = None,
        round: Literal["last"] | int | None = None,  # noqa: A002
        driver: str | None = None,
        standings_position: int | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastMultiResponse | ErgastRawResponse:
        """Driver standings (Stats ``driverPoints.json`` of the round, Jolpica fallback).

        Without ``round`` the standings after the last completed round are
        returned (the final standings for a past season).

        Args:
            season: select a season by its year (default: all)
            round: select a round by its number (default: all)
            driver: select a driver by its driver id (default: all)
            standings_position: select a result by position in the standings (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastMultiResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "driver": driver,
            "standings_position": standings_position,
        }
        return self._build_default_result(
            endpoint="driverStandings",
            table="StandingsTable",
            category=structure.StandingsLists_Driver,
            subcategory=structure.DriverStandings,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_constructor_standings(
        self,
        season: Literal["current"] | int | None = None,
        round: Literal["last"] | int | None = None,  # noqa: A002
        constructor: str | None = None,
        standings_position: int | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastMultiResponse | ErgastRawResponse:
        """Constructor standings (Stats ``teamPoints.json`` of the round, Jolpica fallback).

        Without ``round`` the standings after the last completed round are
        returned (the final standings for a past season).

        Args:
            season: select a season by its year (default: all)
            round: select a round by its number (default: all)
            constructor: select a constructor by its constructor id (default: all)
            standings_position: select a result by position in the standings (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastMultiResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "constructor": constructor,
            "standings_position": standings_position,
        }
        return self._build_default_result(
            endpoint="constructorStandings",
            table="StandingsTable",
            category=structure.StandingsLists_Constructor,
            subcategory=structure.ConstructorStandings,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_lap_times(
        self,
        season: Literal["current"] | int,
        round: Literal["last"] | int,  # noqa: A002
        lap_number: int | None = None,
        driver: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastMultiResponse | ErgastRawResponse:
        """Lap timings of one race (Stats ``laptimes.json``, 1996 onward; Jolpica fallback).

        Args:
            season: select a season by its year
            round: select a round by its number
            lap_number: select lap times by a specific lap number (default: all)
            driver: select a driver by its driver id (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastMultiResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "lap_number": lap_number,
            "driver": driver,
        }
        return self._build_default_result(
            endpoint="laps",
            table="RaceTable",
            category=structure.Races_Laps,
            subcategory=structure.Laps,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )

    def get_pit_stops(
        self,
        season: Literal["current"] | int,
        round: Literal["last"] | int,  # noqa: A002
        stop_number: int | None = None,
        lap_number: int | None = None,
        driver: str | None = None,
        result_type: ErgastResultType | None = None,
        auto_cast: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ErgastMultiResponse | ErgastRawResponse:
        """Pit stops of one race (Stats ``pitstops.json``, 2011 onward; Jolpica fallback).

        Args:
            season: select a season by its year
            round: select a round by its number
            stop_number: select pit stops by their stop number (default: all)
            lap_number: select pit stops by a specific lap number (default: all)
            driver: select a driver by its driver id (default: all)
            result_type: overwrites the default result type
            auto_cast: overwrites the default value for ``auto_cast``
            limit: overwrites the default value for ``limit``
            offset: An offset into the result set

        Returns:
            :class:`ErgastMultiResponse` or :class:`ErgastRawResponse`,
            depending on ``result_type``.
        """
        selectors = {
            "season": season,
            "round": round,
            "stop_number": stop_number,
            "lap_number": lap_number,
            "driver": driver,
        }
        return self._build_default_result(
            endpoint="pitstops",
            table="RaceTable",
            category=structure.Races_PitStops,
            subcategory=structure.PitStops,
            result_type=result_type,
            auto_cast=auto_cast,
            limit=limit,
            offset=offset,
            selectors=selectors,
        )


__all__ = [
    "BASE_URL",
    "HEADERS",
    "TIMEOUT",
    "Ergast",
    "ErgastMultiResponse",
    "ErgastRawResponse",
    "ErgastResponseMixin",
    "ErgastResultFrame",
    "ErgastResultSeries",
    "ErgastSimpleResponse",
]
