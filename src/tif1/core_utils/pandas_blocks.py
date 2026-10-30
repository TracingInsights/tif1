"""pandas block assembly for the telemetry frame builders (pandas sessions only).

The direct ``BlockManager`` route (Thebes TH2, Pergamon P1) and its
``_from_arrays`` fallback, moved out of :mod:`tif1.core_utils.helpers` so a
polars session never imports pandas: the import-time probes below need it.
:mod:`~tif1.core_utils.helpers` resolves these names lazily (its
``_frame_from_columns`` and the module attributes of the same names), and
:mod:`~tif1.core_utils.frame_codec` imports them from here.
"""

from __future__ import annotations

import importlib
from typing import Any, cast

import numpy as np
import pandas as pd


def _frame_from_arrays(arrays: list[Any], columns: list[str], n_rows: int) -> pd.DataFrame:
    """One-shot DataFrame from ready column arrays (no sanitize, consolidated).

    ``DataFrame._from_arrays`` is the constructor pandas' own readers use for
    pre-typed columns; it is private but stable across pandas 1.x-3.x. Should
    it ever disappear, the public constructor (slower, unconsolidated) is the
    fallback, so behaviour never depends on it.
    """
    from_arrays: Any = getattr(pd.DataFrame, "_from_arrays", None)
    if from_arrays is None:  # pragma: no cover - pandas internals changed
        return pd.DataFrame(dict(zip(columns, arrays)), copy=False)
    return cast(
        pd.DataFrame,
        from_arrays(arrays, columns=columns, index=pd.RangeIndex(n_rows), verify_integrity=False),
    )


# -- direct block assembly (Thebes TH2) ---------------------------------------
#
# ``DataFrame._from_arrays`` still runs pandas' generic column pipeline
# (sanitize every array, form blocks, consolidate, build a fresh column
# Index) - ~250 us per telemetry frame, i.e. ~40 % of the cold assembly.
# The typed builder already knows every column's dtype, so it can lay the
# blocks out itself: numpy columns of one dtype become one 2-D block (the
# same consolidated layout ``_from_arrays`` produces), extension arrays
# become one block each, and the column Index is shared between frames with
# identical columns. The private pandas API involved (``BlockManager``,
# ``new_block``, ``DataFrame._from_mgr``) is probed once at import with a
# round-trip against the public constructor; if anything drifted, or a
# build raises, ``_frame_from_arrays`` is used instead.
try:  # pragma: no cover - import-time probe of pandas internals
    # importlib keeps the static checker out of pandas' private modules (the
    # stubs do not describe them); any ImportError/AttributeError disables
    # the direct path and ``_from_arrays`` is used instead.
    _BlockPlacement = importlib.import_module("pandas._libs.internals").BlockPlacement
    _wrap_name = "ensure_wrapped_if_datetimelike"  # absent from the pandas stubs
    _ensure_wrapped_if_datetimelike = getattr(
        importlib.import_module("pandas.core.construction"), _wrap_name
    )
    _BlockManager = importlib.import_module("pandas.core.internals").BlockManager
    _new_block = importlib.import_module("pandas.core.internals.blocks").new_block
    _dataframe_from_mgr: Any = getattr(pd.DataFrame, "_from_mgr", None)
    _DIRECT_BLOCKS_AVAILABLE = callable(_dataframe_from_mgr)
except (ImportError, AttributeError):  # pragma: no cover - pandas internals changed
    _DIRECT_BLOCKS_AVAILABLE = False

# Column ``Index`` objects shared between frames with identical columns
# (an Index is immutable; building one costs ~100 us per frame otherwise).
_COLUMN_INDEX_CACHE: dict[tuple[str, ...], pd.Index] = {}
_COLUMN_INDEX_CACHE_MAX = 64


def _shared_column_index(columns: list[str]) -> pd.Index:
    key = tuple(columns)
    index = _COLUMN_INDEX_CACHE.get(key)
    if index is None:
        if len(_COLUMN_INDEX_CACHE) >= _COLUMN_INDEX_CACHE_MAX:
            _COLUMN_INDEX_CACHE.clear()
        index = pd.Index(list(columns))
        _COLUMN_INDEX_CACHE[key] = index
    return index


# Pergamon P1: the per-frame skeleton is built from parts pandas would
# otherwise re-derive on every call. ``RangeIndex`` and ``BlockPlacement``
# objects are immutable and shared between frames (pandas itself shares an
# index between a frame and its column slices); block classes are resolved
# once from ``new_block`` so construction skips the dtype dispatch, the ndim
# check and ``maybe_coerce_values`` — the values handed in are already the
# coerced kinds ``new_block`` would produce (float/bool/object ndarrays,
# ``TimedeltaArray``/``DatetimeArray`` for datetime-likes, extension arrays
# for the rest). ``_probe_direct_blocks`` verifies the whole route against
# the public constructor at import; any drift disables it.
_RANGE_INDEX_CACHE: dict[int, pd.RangeIndex] = {}
_RANGE_INDEX_CACHE_MAX = 4096
_PLACEMENT_CACHE: dict[Any, Any] = {}
_PLACEMENT_CACHE_MAX = 4096


def _range_index(n_rows: int) -> pd.RangeIndex:
    index = _RANGE_INDEX_CACHE.get(n_rows)
    if index is None:
        if len(_RANGE_INDEX_CACHE) >= _RANGE_INDEX_CACHE_MAX:
            _RANGE_INDEX_CACHE.clear()
        index = pd.RangeIndex(n_rows)
        _RANGE_INDEX_CACHE[n_rows] = index
    return index


def _placement_for(positions: Any) -> Any:
    """Shared ``BlockPlacement`` for an int position or a tuple of positions."""
    key = positions if isinstance(positions, int | tuple) else tuple(int(p) for p in positions)
    placement = _PLACEMENT_CACHE.get(key)
    if placement is None:
        if len(_PLACEMENT_CACHE) >= _PLACEMENT_CACHE_MAX:
            _PLACEMENT_CACHE.clear()
        placement = _BlockPlacement(key if isinstance(key, int) else np.asarray(key, dtype="int64"))
        _PLACEMENT_CACHE[key] = placement
    return placement


try:  # pragma: no cover - import-time resolution of pandas block classes
    _probe_placement = _BlockPlacement(0)
    _NUMPY_BLOCK_CLASS: Any = type(_new_block(np.zeros((1, 1)), placement=_probe_placement, ndim=2))
    _DATETIMELIKE_BLOCK_CLASS: Any = type(
        _new_block(
            _ensure_wrapped_if_datetimelike(np.zeros((1, 1), dtype="m8[ns]")),
            placement=_probe_placement,
            ndim=2,
        )
    )
    _EXTENSION_BLOCK_CLASS: Any = type(
        _new_block(
            pd.arrays.IntegerArray(np.zeros(1, dtype="int64"), np.zeros(1, dtype=bool)),
            placement=_probe_placement,
            ndim=2,
        )
    )
    _TimedeltaArray: Any = pd.arrays.TimedeltaArray
    _DatetimeArray: Any = pd.arrays.DatetimeArray
    _IntegerArray: Any = pd.arrays.IntegerArray
    _INTEGER_ARRAY_SIMPLE_NEW: Any = getattr(_IntegerArray, "_simple_new", None)
    _DIRECT_BLOCK_CLASSES_OK = (
        _NUMPY_BLOCK_CLASS is not _EXTENSION_BLOCK_CLASS
        and callable(getattr(_TimedeltaArray, "_simple_new", None))
        and callable(_INTEGER_ARRAY_SIMPLE_NEW)
    )
except Exception:  # pragma: no cover - pandas internals changed
    _DIRECT_BLOCK_CLASSES_OK = False
    _INTEGER_ARRAY_SIMPLE_NEW = None


def _frame_from_blocks(
    blocks: tuple[Any, ...], columns: list[str], n_rows: int, frame_class: Any = None
) -> pd.DataFrame:
    """DataFrame over ready 2-D blocks (``(ncols, n_rows)`` values + placements).

    ``frame_class`` (a DataFrame subclass such as ``Telemetry``) builds that
    class directly: wrapping a finished frame afterwards costs a shallow
    manager copy (~70 us per frame), building it here costs nothing. Its
    ``_metadata`` attributes start as None.
    """
    mgr = _BlockManager(
        blocks, [_shared_column_index(columns), _range_index(n_rows)], verify_integrity=False
    )
    if frame_class is None or frame_class is pd.DataFrame:
        return cast(pd.DataFrame, _dataframe_from_mgr(mgr, axes=mgr.axes))
    frame = frame_class._from_mgr(mgr, axes=mgr.axes)
    for name in getattr(frame_class, "_metadata", ()):
        object.__setattr__(frame, name, None)
    return cast(pd.DataFrame, frame)


def _wrap_datetimelike(values: np.ndarray) -> Any:
    """``TimedeltaArray``/``DatetimeArray`` over a datetime-like ndarray (no re-validation)."""
    kind = values.dtype.kind
    try:
        if kind == "m":
            return _TimedeltaArray._simple_new(values, dtype=values.dtype)
        if kind == "M":
            return _DatetimeArray._simple_new(values, dtype=values.dtype)
    except (AssertionError, AttributeError, TypeError, ValueError):
        pass
    return _ensure_wrapped_if_datetimelike(values)


def _numpy_block(values: np.ndarray, placement: Any) -> Any:
    """A 2-D numpy block; datetime-like values are wrapped the way pandas does."""
    if values.dtype.kind in "mM":
        wrapped = _wrap_datetimelike(values)
        if _DIRECT_BLOCK_CLASSES_OK:
            return _DATETIMELIKE_BLOCK_CLASS(wrapped, placement=_placement_for(placement), ndim=2)
        return _new_block(wrapped, placement=_BlockPlacement(placement), ndim=2)
    if _DIRECT_BLOCK_CLASSES_OK:
        return _NUMPY_BLOCK_CLASS(values, placement=_placement_for(placement), ndim=2)
    return _new_block(values, placement=_BlockPlacement(placement), ndim=2)


def _block_placement(placement: Any) -> Any:
    """A (shared) ``BlockPlacement`` the ``*_placed`` builders accept as-is."""
    if _DIRECT_BLOCK_CLASSES_OK:
        return _placement_for(placement)
    return _BlockPlacement(placement)


def _numpy_block_placed(values: np.ndarray, placement: Any) -> Any:
    """``_numpy_block`` over a ready ``BlockPlacement`` (hot loops resolve it once)."""
    if values.dtype.kind in "mM":
        wrapped = _wrap_datetimelike(values)
        if _DIRECT_BLOCK_CLASSES_OK:
            return _DATETIMELIKE_BLOCK_CLASS(wrapped, placement=placement, ndim=2)
        return _new_block(wrapped, placement=placement, ndim=2)
    if _DIRECT_BLOCK_CLASSES_OK:
        return _NUMPY_BLOCK_CLASS(values, placement=placement, ndim=2)
    return _new_block(values, placement=placement, ndim=2)


def _extension_block(values: Any, placement: Any) -> Any:
    """A 2-D block over a 1-D extension array at one column position."""
    if _DIRECT_BLOCK_CLASSES_OK:
        return _EXTENSION_BLOCK_CLASS(values, placement=_placement_for(placement), ndim=2)
    return _new_block(values, placement=_BlockPlacement(placement), ndim=2)


def _extension_block_placed(values: Any, placement: Any) -> Any:
    """``_extension_block`` over a ready ``BlockPlacement``."""
    if _DIRECT_BLOCK_CLASSES_OK:
        return _EXTENSION_BLOCK_CLASS(values, placement=placement, ndim=2)
    return _new_block(values, placement=placement, ndim=2)


def _int64_array(data: np.ndarray, mask: np.ndarray) -> Any:
    """``Int64`` extension array over ready int64 data + bool mask ndarrays."""
    if _INTEGER_ARRAY_SIMPLE_NEW is not None:
        return _INTEGER_ARRAY_SIMPLE_NEW(data, mask)
    return pd.arrays.IntegerArray(data, mask)


def _direct_frame_from_columns(arrays: list[Any], columns: list[str], n_rows: int) -> pd.DataFrame:
    """Assemble ready 1-D columns into a consolidated frame without sanitizing."""
    numpy_groups: dict[np.dtype, list[tuple[int, np.ndarray]]] = {}
    ea_columns: list[tuple[int, Any]] = []
    for position, column in enumerate(arrays):
        if isinstance(column, np.ndarray):
            arr = column
        elif type(column).__name__ == "NumpyExtensionArray":
            arr = np.asarray(column)
        else:
            if len(column) != n_rows:
                raise ValueError("column length mismatch")
            ea_columns.append((position, column))
            continue
        if arr.ndim != 1 or arr.shape[0] != n_rows:
            raise ValueError("column shape mismatch")
        numpy_groups.setdefault(arr.dtype, []).append((position, arr))
    blocks: list[Any] = []
    for dtype, items in numpy_groups.items():
        if len(items) == 1:
            position, arr = items[0]
            blocks.append(_numpy_block(arr.reshape(1, n_rows), position))
        else:
            values = np.vstack([arr for _, arr in items])
            blocks.append(_numpy_block(values, tuple(pos for pos, _ in items)))
        del dtype
    for position, column in ea_columns:
        blocks.append(_extension_block(column, position))
    return _frame_from_blocks(tuple(blocks), columns, n_rows)


def _probe_direct_blocks() -> bool:
    """Round-trip a small typed frame through the direct path and the public one."""
    if not _DIRECT_BLOCKS_AVAILABLE:
        return False
    try:
        columns = ["Time", "RPM", "Speed", "nGear", "Brake", "DriverAhead", "Driver", "LapNumber"]
        arrays: list[Any] = [
            np.array([0, 16_000_000, 256_000_000], dtype="m8[ns]"),
            np.array([10000.0, 10500.0, np.nan]),
            np.array([250.0, 251.5, 252.0]),
            pd.arrays.IntegerArray(np.array([7, 8, 8]), np.zeros(3, dtype=bool)),
            np.array([False, True, False]),
            pd.Series(["1", None, "44"]).array,
            np.full(3, "VER", dtype=object),
            pd.arrays.IntegerArray(np.full(3, 5), np.zeros(3, dtype=bool)),
        ]
        direct = _direct_frame_from_columns(arrays, columns, 3)
        public = _frame_from_arrays(arrays, columns, 3)
        # Block classes and value types must match too (a NumpyBlock over a
        # raw m8 ndarray would compare equal yet misbehave on arithmetic).
        direct_kinds = sorted(
            (type(b).__name__, type(b.values).__name__) for b in direct._mgr.blocks
        )
        public_kinds = sorted(
            (type(b).__name__, type(b.values).__name__) for b in public._mgr.blocks
        )
        mutated = direct.copy()
        mutated.iloc[0, 1] = 1.5
        return bool(
            list(direct.columns) == list(public.columns)
            and direct.dtypes.equals(public.dtypes)
            and direct.equals(public)
            and len(direct._mgr.blocks) == len(public._mgr.blocks)
            and direct_kinds == public_kinds
            and mutated.iloc[0, 1] == 1.5
            and direct.iloc[0, 1] == 10000.0
        )
    except Exception:
        return False


_DIRECT_BLOCKS_OK = _probe_direct_blocks()


def _frame_from_columns(arrays: list[Any], columns: list[str], n_rows: int) -> pd.DataFrame:
    """Consolidated DataFrame from ready 1-D columns (direct blocks, else ``_from_arrays``)."""
    if _DIRECT_BLOCKS_OK:
        try:
            return _direct_frame_from_columns(arrays, columns, n_rows)
        except (AttributeError, TypeError, ValueError):
            pass
    return _frame_from_arrays(arrays, columns, n_rows)
