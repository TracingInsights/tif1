"""fastf1's ``fastf1.logger`` surface under tif1 names.

:class:`LoggingManager` configures the ``tif1`` logger the way fastf1's
manager configures ``fastf1``: a console handler with fastf1's line format on
the package logger, ``set_level`` to move the visible level and ``get_child``
for module loggers. :func:`get_logger`, :func:`set_log_level` and
:func:`soft_exceptions` are the module-level helpers migrated code calls.
Every logger is ``tif1`` or ``tif1.<name>``; nothing here is named ``fastf1``.

Importing this module attaches the console handler to the ``tif1`` logger
(fastf1 does the same for ``fastf1`` at import) and sets that logger to
``INFO``. fastf1 keeps its package logger at ``DEBUG`` and filters at the
handler; tif1 keeps the logger itself at the visible level so the many
``logger.debug`` calls on hot paths skip record construction unless debug
output was asked for. ``TIF1_DEBUG=1`` in the environment enables
:attr:`LoggingManager.debug` (fastf1's ``FASTF1_DEBUG``).

The module only imports the standard library and :mod:`tif1.exceptions`.
"""

from __future__ import annotations

import functools
import logging
import os
import sys
import warnings
from collections.abc import Callable
from typing import ClassVar, ParamSpec, TextIO, TypeVar

from .exceptions import FastF1CriticalError

__all__ = ["LoggingManager", "get_logger", "set_log_level", "soft_exceptions"]

P = ParamSpec("P")
R = TypeVar("R")


class _StderrHandler(logging.StreamHandler):
    """Console handler that writes to the *current* ``sys.stderr``.

    A plain ``StreamHandler`` binds the stream object alive at construction;
    pytest's capture and notebook kernels swap ``sys.stderr`` later, so the
    bound object can be a closed capture file. Resolving the stream at emit
    time (what ``logging.lastResort`` does) keeps the handler valid.
    """

    def __init__(self) -> None:
        logging.Handler.__init__(self)

    @property
    def stream(self) -> TextIO:
        return sys.stderr

    @stream.setter
    def stream(self, value: TextIO) -> None:
        """``StreamHandler.setStream`` assigns here; the stream is always ``sys.stderr``."""


class LoggingManager:
    """Interface for configuring tif1's logging (fastf1's ``LoggingManager``).

    All submodule loggers of tif1 are children of the ``tif1`` base logger.
    This class sets the level shown on the console and hands out child
    loggers; fastf1 code that touches ``LoggingManager`` works unchanged
    after swapping the import.
    """

    _console_formatter: ClassVar[logging.Formatter] = logging.Formatter(
        "{module: <8} {levelname: >10} \t{message}", style="{"
    )

    _console_handler: ClassVar[logging.StreamHandler] = _StderrHandler()
    _console_handler.setFormatter(_console_formatter)
    _console_handler.setLevel(logging.INFO)

    _root_logger: ClassVar[logging.Logger] = logging.getLogger("tif1")
    _root_logger.setLevel(logging.INFO)
    _root_logger.addHandler(_console_handler)

    debug: ClassVar[bool] = False
    """Flag for enabling debug mode. This disables the catch-all error
    handling of :func:`soft_exceptions`, so wrapped data-loading functions
    raise instead of logging."""

    @classmethod
    def get_child(cls, name: str) -> logging.Logger:
        """Return the child ``tif1.<name>`` of the base logger.

        Args:
            name: Name of the child logger (``"core"`` gives ``tif1.core``).
        """
        return cls._root_logger.getChild(name)

    @classmethod
    def set_level(cls, level: int) -> None:
        """Set the log level for tif1.

        Both the console handler and the ``tif1`` logger move to ``level``:
        the handler so fastf1's console output follows it, the logger so
        ``logging.getLogger("tif1").level`` reports it and lower records are
        never built.

        Args:
            level: Log level, for example ``logging.INFO``.
        """
        cls._console_handler.setLevel(level)
        cls._root_logger.setLevel(level)


if os.getenv("TIF1_DEBUG") == "1":
    warnings.warn("Debug Mode enabled for Logger!", UserWarning, stacklevel=2)
    LoggingManager.debug = True
else:
    LoggingManager.debug = False


def get_logger(name: str) -> logging.Logger:
    """Return the logger ``tif1.<name>``, a child of tif1's base logger.

    Args:
        name: Child logger name, usually the module name.
    """
    return LoggingManager.get_child(name)


def set_log_level(level: str | int = logging.WARNING) -> None:
    """Set the log level for all parts of tif1.

    Only messages at ``level`` or above are shown afterwards.

    Args:
        level: A level from the :mod:`logging` module (``logging.INFO``) or
            its name in any case (``"WARNING"``, ``"info"``).

    Raises:
        ValueError: ``level`` is a string that names no logging level.
    """
    if isinstance(level, str):
        resolved = logging.getLevelNamesMapping().get(level.upper())
        if resolved is None:
            known = ", ".join(sorted(logging.getLevelNamesMapping()))
            raise ValueError(f"Unknown log level {level!r}; expected one of {known}")
        level = resolved
    LoggingManager.set_level(level)


def soft_exceptions(
    descr_name: str, msg: str, logger: logging.Logger
) -> Callable[[Callable[P, R]], Callable[P, R | None]]:
    """Wrap a data-loading function in catch-all error handling.

    The wrapped function's unhandled exceptions are logged (``msg`` as a
    warning, the traceback at debug level) and the call returns ``None``, so
    loading degrades instead of failing outright. Two things break through:
    :class:`~tif1.exceptions.FastF1CriticalError` (rate limits and the like)
    is always re-raised, and when :attr:`LoggingManager.debug` is ``True``
    (``TIF1_DEBUG=1``) nothing is caught at all.

    Args:
        descr_name: Descriptive name of the data the wrapped function loads;
            it labels the traceback in the debug log.
        msg: Short message shown to users as the warning.
        logger: Logger that receives the messages (one returned by
            :func:`get_logger`, for example).
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R | None]:
        @functools.wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R | None:
            if LoggingManager.debug:
                return func(*args, **kwargs)
            try:
                return func(*args, **kwargs)
            except FastF1CriticalError:
                raise
            except Exception as exc:
                logger.warning(msg)
                logger.debug(f"Traceback for failure in {descr_name}", exc_info=exc)
                return None

        return wrapped

    return decorator
