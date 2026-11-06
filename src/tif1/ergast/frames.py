"""The pandas response classes of :mod:`tif1.ergast.interface` (Corinth C3).

They subclass ``pandas.DataFrame``/``Series``, so defining them imports pandas
(~340-390 ms in a fresh process). They live here and :mod:`tif1.ergast.interface`
resolves them on first use (module ``__getattr__``), so ``from tif1.ergast
import Ergast`` does not import pandas and a cold query can fetch its files
while pandas imports (``interface._execute_overlapped``). Their public path,
``__module__``, repr and pickling stay ``tif1.ergast.interface``.
"""

from __future__ import annotations

from typing import Any, ClassVar, Self, cast

import pandas as pd

from tif1.ergast import structure
from tif1.ergast.interface import ErgastResponseMixin

_PUBLIC_MODULE = "tif1.ergast.interface"


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


for _cls in (ErgastResultFrame, ErgastResultSeries, ErgastSimpleResponse):
    _cls.__module__ = _PUBLIC_MODULE

__all__ = ["ErgastResultFrame", "ErgastResultSeries", "ErgastSimpleResponse"]
