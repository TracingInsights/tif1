"""The installed tif1 version without importing :mod:`importlib.metadata`.

``importlib.metadata.version("tif1")`` costs ~20-30 ms in a fresh process:
the module imports ``email``, ``zipfile``, ``csv`` and their dependencies,
then parses ``METADATA`` with the email parser. ``tif1 version``, the
User-Agent of every request (the first-wave threads of a cold session,
``tif1.ergast.interface`` at import) only need the ``Version`` header of one
``*.dist-info`` directory. :func:`tif1_version` finds it the way
importlib.metadata's path finder does (the first ``sys.path`` entry holding a
distribution of that name) and reads the header; any layout it does not
cover (an ``*.egg-info``, a zip on ``sys.path``, a custom finder that serves
distributions, a missing header) is answered by importlib.metadata itself.
Stdlib only; no tif1 module is imported.
"""

from __future__ import annotations

import os
import re
import sys

_DIST_NAME = "tif1"
_UNKNOWN_VERSION = "0.0.0"

_version_value: str | None = None


def _normalize(name: str) -> str:
    """importlib.metadata's ``Prepared.normalize``: PEP 503, ``-`` spelled ``_``."""
    return re.sub(r"[-_.]+", "-", name).lower().replace("-", "_")


def _metadata_version(dist_info: str) -> str | None:
    """The ``Version`` header of ``dist_info/METADATA`` (None when absent or unreadable)."""
    try:
        metadata = os.path.join(dist_info, "METADATA")  # noqa: PTH118
        with open(metadata, encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    return None  # end of the header block
                key, sep, value = line.partition(":")
                if sep and key.strip().lower() == "version":
                    return value.strip() or None
    except (OSError, UnicodeDecodeError):
        return None
    return None


def _find_dist_info(name: str) -> str | None:
    """The first ``*.dist-info`` of ``name`` on ``sys.path`` (None: ask importlib.metadata).

    importlib.metadata searches every ``sys.meta_path`` finder that serves
    distributions and, in the path finder, the ``sys.path`` entries in order
    (directories by their listing, zip files by their contents). Any part of
    that this lookup does not reproduce returns None.
    """
    from importlib.machinery import PathFinder

    for finder in sys.meta_path:
        if finder is not PathFinder and hasattr(finder, "find_distributions"):
            return None
    wanted = _normalize(name)
    for entry in sys.path:
        root = entry or "."
        try:
            # importlib.metadata lists the directory the same way (FastPath).
            children = os.listdir(root)  # noqa: PTH208
        except NotADirectoryError:
            return None  # a zip on sys.path: importlib reads its entries
        except OSError:
            continue  # missing entries hold no distributions
        for child in children:
            low = child.lower()
            if not low.endswith((".dist-info", ".egg-info")):
                continue
            if _normalize(low.rpartition(".")[0].partition("-")[0]) == wanted:
                # An egg-info (a file or a directory) is left to importlib.metadata.
                if low.endswith(".dist-info"):
                    return os.path.join(root, child)  # noqa: PTH118
                return None
        if root.lower().endswith(".egg"):
            return None  # importlib also reads an egg's EGG-INFO
    return None


def tif1_version() -> str:
    """The installed tif1 version, ``"0.0.0"`` when tif1 is not installed (memoized)."""
    global _version_value
    if _version_value is None:
        dist_info = _find_dist_info(_DIST_NAME)
        value = _metadata_version(dist_info) if dist_info is not None else None
        if value is None:
            from importlib.metadata import PackageNotFoundError, version

            try:
                value = version(_DIST_NAME)
            except PackageNotFoundError:
                value = _UNKNOWN_VERSION
        _version_value = value
    return _version_value


__all__ = ["tif1_version"]
