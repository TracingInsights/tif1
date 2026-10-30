"""Deferred module imports that cost nothing once resolved.

A polars session never builds a pandas object, so the modules it runs
(:mod:`tif1.core`, the shared helpers) bind ``pd`` to a :class:`LazyModule`
instead of importing pandas (~250 ms) at module load. The first attribute
read imports the module and rebinds the global it was bound to, so every
later ``pd.X`` in that module is a plain module-global lookup: a pandas
session pays one proxy call per binding module, never one per access.

:func:`loaded_module` answers type checks without importing: no object can
be an instance of a class whose module was never imported.
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Any


class LazyModule(ModuleType):
    """Proxy for module ``target`` bound to ``alias`` in ``namespace``.

    The first attribute read imports ``target`` and replaces the proxy in
    ``namespace`` with the module itself (idempotent, so concurrent first
    reads are harmless: the import lock serializes the import).
    """

    def __init__(self, target: str, namespace: dict[str, Any], alias: str) -> None:
        super().__init__(target)
        self.__dict__["_lazy_target"] = target
        self.__dict__["_lazy_namespace"] = namespace
        self.__dict__["_lazy_alias"] = alias

    def _lazy_load(self) -> ModuleType:
        module = importlib.import_module(self.__dict__["_lazy_target"])
        namespace = self.__dict__["_lazy_namespace"]
        alias = self.__dict__["_lazy_alias"]
        if namespace.get(alias) is self:
            namespace[alias] = module
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._lazy_load(), name)

    def __dir__(self) -> list[str]:
        return dir(self._lazy_load())

    def __repr__(self) -> str:
        return f"<lazy module {self.__dict__['_lazy_target']!r}>"


def lazy_module(target: str, namespace: dict[str, Any], alias: str) -> Any:
    """``target`` bound to ``alias`` in ``namespace``, imported on first attribute read.

    Args:
        target: Absolute module name (``"pandas"``, ``"tif1.models"``).
        namespace: The binding module's ``globals()``.
        alias: The global name the proxy is bound to (rebound to the module).

    Returns:
        The module itself when it is already imported, else a :class:`LazyModule`.
    """
    module = sys.modules.get(target)
    if module is not None:
        return module
    return LazyModule(target, namespace, alias)


def loaded_module(name: str) -> ModuleType | None:
    """Module ``name`` when something already imported it, else ``None`` (never imports)."""
    return sys.modules.get(name)


def is_pandas_frame(obj: Any) -> bool:
    """``isinstance(obj, pandas.DataFrame)`` without importing pandas."""
    pd = sys.modules.get("pandas")
    return pd is not None and isinstance(obj, pd.DataFrame)


def is_pandas_series(obj: Any) -> bool:
    """``isinstance(obj, pandas.Series)`` without importing pandas."""
    pd = sys.modules.get("pandas")
    return pd is not None and isinstance(obj, pd.Series)


__all__ = ["LazyModule", "is_pandas_frame", "is_pandas_series", "lazy_module", "loaded_module"]
