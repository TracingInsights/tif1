"""Runtime-environment facts: Pyodide (Emscripten) detection and the sync bridge.

Pyodide runs CPython on WebAssembly inside a browser tab (JupyterLite,
PyScript, marimo) or Node. Three things differ from a native interpreter and
every other module consults this one instead of probing on its own:

- **No threads.** ``threading.Thread.start()`` raises; every background
  thread and pool is gated on :data:`THREADS_AVAILABLE`.
- **No sockets.** HTTP goes through the browser (``tif1.browser``); the
  default transport is chosen on :data:`IS_EMSCRIPTEN`.
- **One always-running event loop.** ``asyncio.run`` cannot block, so the
  synchronous API bridges to its coroutine through :func:`run_sync_in_browser`
  (JavaScript Promise Integration, when the runtime offers it).

The module imports only the standard library so ``import tif1`` can use it
without cost; the two constants are evaluated once at import.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Coroutine
from types import ModuleType
from typing import Any, Final, TypeVar

from tif1.exceptions import AsyncRequiredError

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: True under Emscripten (Pyodide and other WebAssembly builds of CPython).
IS_EMSCRIPTEN: Final[bool] = sys.platform == "emscripten"

#: False when the runtime cannot start threads (every Pyodide distribution
#: today). Native interpreters always report True.
THREADS_AVAILABLE: Final[bool] = not IS_EMSCRIPTEN or bool(
    getattr(getattr(sys, "_emscripten_info", None), "pthreads", False)
)

_POLARS_MIN_MAJOR = 2
_polars_warned = False


def is_pyodide() -> bool:
    """Return True inside the Pyodide runtime specifically."""
    return IS_EMSCRIPTEN and "pyodide" in sys.modules


def can_block_on_awaitables() -> bool:
    """Return True when synchronous code may block on a coroutine.

    Native interpreters always can (``asyncio.run``). Pyodide can only when
    JavaScript Promise Integration is active for the current call stack,
    which ``pyodide.ffi.can_run_sync`` reports; it is True for code entered
    through ``runPythonAsync`` or an async entry point in a runtime with JSPI
    (Chrome 137+, Node 24 with ``--experimental-wasm-jspi``).
    """
    if not IS_EMSCRIPTEN:
        return True
    try:
        from pyodide.ffi import can_run_sync  # ty: ignore[unresolved-import]
    except ImportError:
        return False
    try:
        return bool(can_run_sync())
    except Exception:  # pragma: no cover - defensive: runtime without the probe
        return False


def run_sync_in_browser(
    factory: Callable[[], Coroutine[Any, Any, T]], operation: str, *, alternative: str
) -> T:
    """Block on the coroutine ``factory`` builds, using JSPI stack switching.

    Args:
        factory: Zero-argument callable returning the coroutine to run. It is
            only called once blocking is known to be possible, so no
            coroutine is created (and left un-awaited) on the error path.
        operation: Name of the synchronous operation, for the error message.
        alternative: The async call to use instead (``"session.laps_async()"``).

    Raises:
        AsyncRequiredError: The runtime cannot block on awaitables here.
    """
    if not can_block_on_awaitables():
        raise AsyncRequiredError(operation=operation, alternative=alternative)
    from pyodide.ffi import run_sync  # ty: ignore[unresolved-import]

    return run_sync(factory())


def import_supported_polars() -> ModuleType | None:
    """Import polars when it is installed and at least version 2, else None.

    Pyodide ships polars 1.x, which tif1 does not support; a mismatched
    version is reported once and treated as not installed, so the backend
    falls back to pandas exactly as when polars is missing.
    """
    global _polars_warned
    try:
        import polars as pl
    except ImportError:
        return None
    version = getattr(pl, "__version__", "")
    try:
        major = int(str(version).split(".", 1)[0])
    except ValueError:
        major = _POLARS_MIN_MAJOR
    if major < _POLARS_MIN_MAJOR:
        if not _polars_warned:
            _polars_warned = True
            logger.warning(
                "polars %s is installed but tif1 needs polars >= %d; using the pandas backend",
                version,
                _POLARS_MIN_MAJOR,
            )
        return None
    return pl


__all__ = [
    "IS_EMSCRIPTEN",
    "THREADS_AVAILABLE",
    "can_block_on_awaitables",
    "import_supported_polars",
    "is_pyodide",
    "run_sync_in_browser",
]
