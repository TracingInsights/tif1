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
import re
import sys
from types import ModuleType
from typing import Any

from .gc_control import suspend_gc


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
        module = import_promoted(self.__dict__["_lazy_target"])
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


def import_promoted(target: str) -> ModuleType:
    """``importlib.import_module(target)``, the first import without young GC collections.

    Naxos N10: ``import pandas`` creates ~150 000 GC-tracked containers
    (modules, classes, functions, their dicts) that live as long as the
    process. With cyclic GC on, the import ran ~50 young collections that
    freed nothing and left every object for the collections after it to
    traverse again; it now runs with GC suspended and its objects go
    straight to the oldest generation (:func:`suspend_gc` with ``promote``):
    fresh ``import pandas`` 424 -> 390 ms, and the young collections of the
    work that follows half as long. A module already imported is returned
    as is.
    """
    module = sys.modules.get(target)
    if module is not None:
        return module
    with suspend_gc(promote=True):
        return importlib.import_module(target)


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


class LazyPattern:
    """``re.compile(pattern, flags)`` deferred to the first attribute read.

    The read compiles the pattern and rebinds ``alias`` in ``namespace`` to
    the compiled :class:`re.Pattern`, so every later use of the module global
    is the pattern itself (the same rebinding :class:`LazyModule` does for a
    module). A module-level ``re.compile`` costs ~0.1 ms at import; the
    patterns of the session startup path (Delphi D2) are compiled on first
    use instead.
    """

    __slots__ = ("_alias", "_flags", "_namespace", "_pattern")

    def __init__(self, pattern: str, flags: int, namespace: dict[str, Any], alias: str) -> None:
        self._pattern = pattern
        self._flags = flags
        self._namespace = namespace
        self._alias = alias

    def _compile(self) -> re.Pattern[str]:
        compiled = re.compile(self._pattern, self._flags)
        if self._namespace.get(self._alias) is self:
            self._namespace[self._alias] = compiled
        return compiled

    def __getattr__(self, name: str) -> Any:
        return getattr(self._compile(), name)

    def __repr__(self) -> str:
        return f"<lazy pattern {self._pattern!r}>"


def lazy_pattern(pattern: str, namespace: dict[str, Any], alias: str, flags: int = 0) -> Any:
    """``re.compile(pattern, flags)`` bound to ``alias`` in ``namespace``, compiled on first use.

    Args:
        pattern: The regular expression.
        namespace: The binding module's ``globals()``.
        alias: The global name the proxy is bound to (rebound to the pattern).
        flags: ``re`` flags.

    Returns:
        A :class:`LazyPattern`; module code uses it exactly like the compiled pattern.
    """
    return LazyPattern(pattern, flags, namespace, alias)


def is_pandas_frame(obj: Any) -> bool:
    """``isinstance(obj, pandas.DataFrame)`` without importing pandas."""
    pd = sys.modules.get("pandas")
    return pd is not None and isinstance(obj, pd.DataFrame)


def is_pandas_series(obj: Any) -> bool:
    """``isinstance(obj, pandas.Series)`` without importing pandas."""
    pd = sys.modules.get("pandas")
    return pd is not None and isinstance(obj, pd.Series)


__all__ = [
    "LazyModule",
    "LazyPattern",
    "import_promoted",
    "is_pandas_frame",
    "is_pandas_series",
    "lazy_module",
    "lazy_pattern",
    "loaded_module",
]
