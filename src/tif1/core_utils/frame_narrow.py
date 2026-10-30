"""Narrowed pandas driver rows (Olympia H1): small-integer clocks, channels and codes.

A driver blob (:func:`tif1.core_utils.frame_codec.encode_driver_frames`)
stores every column at its frame dtype. lz4 gets almost nothing out of the
three clocks (``Date``, ``SessionTime``, ``Time``: ~1.3x on 8-byte
nanosecond counts), and the ``Int64`` channels and dictionary codes are
mostly zero bytes. A narrowed row stores those columns as small integers:

- a clock is a whole number of milliseconds (or microseconds), so it is
  stored as ``int32`` milliseconds, or as the difference from the group's
  ``SessionTime`` (``Date`` is ``SessionTime`` plus one constant: zero bytes;
  ``Time`` is ``SessionTime`` minus the lap start: one value per lap, which
  lz4 folds into runs);
- an ``Int64`` channel (``nGear``, ``DRS``) is stored as ``int8``;
- dictionary codes (``DriverAhead``) are stored as ``int8``.

Floats are left alone: they are full-precision interpolations, no channel
narrows exactly (a survey of eleven cached sessions found ~1% of the
columns, all of them all-NaN or near-empty), and a numpy byte shuffle costs
more to undo (~3 ms per race driver row) than the ~5% of bytes it saves.
Delta coding is not used either: ``np.cumsum`` costs ~3 ns per value and
holds the GIL, where widening is one cast/multiply/add pass that does not.

Every encoding is checked by decoding it before it is used: a column that
does not come back bit for bit is stored as is.

Stored row::

    magic "TF1\\x03" | plan length (uint32 LE) | pickled plan | compressed content

``content`` is a driver blob whose body holds the narrowed regions last;
there they hold their narrow encodings back to back (the *tail*).
Decompressing ``content`` into a buffer of the full driver-blob length and
calling :func:`expand` turns the buffer into that driver blob in place, so
every reader of driver blobs works unchanged and the widening is paid once
per row, in the decompression worker.

The plan is ``(expanded length, tail offset, tail length, ops)``; offsets
are absolute in the expanded buffer (``src`` is relative to the tail):

- ``(OP_INT, dst, n, enc_dtype, src, unit, add, ref_dst)``: ``int64``
  ``enc * unit + add`` (plus the ``int64`` values at ``ref_dst``);
- ``(OP_REF, dst, n, add, ref_dst)``: ``int64`` values at ``ref_dst`` plus
  ``add``;
- ``(OP_CODES, dst, n, enc_dtype, src)``: ``int32`` ``enc``.

Ops run in plan order (a referenced column is widened before its users).
This module needs only numpy, so :mod:`tif1.cache` can expand rows without
importing pandas.
"""

from __future__ import annotations

import pickle
import struct
from typing import Any

import numpy as np

NARROW_MAGIC = b"TF1\x03"
_PLAN_LEN = struct.Struct("<I")
_INT64 = np.dtype("int64")
_INT32 = np.dtype("int32")
_NARROW_INTS = (np.dtype("int8"), np.dtype("int16"), np.dtype("int32"))
# Clock units tried, coarsest first (values are nanoseconds).
_CLOCK_UNITS = (1_000_000, 1_000)
_UNIT_PROBE = 64  # values checked before a whole-column divisibility test

OP_INT = 0
OP_REF = 1
OP_CODES = 2

# A recipe is what the encoder decided for one column, before layout:
# ("int", enc, unit, add, uses_ref) or ("ref", None, 1, add, True).
Recipe = tuple[str, Any, int, int, bool]


def _fitting_dtype(lo: int, hi: int, start: int = 0) -> np.dtype | None:
    for dtype in _NARROW_INTS[start:]:
        info = np.iinfo(dtype)
        if info.min <= lo and hi <= info.max:
            return dtype
    return None


def _unit_of(values: np.ndarray) -> int:
    for unit in _CLOCK_UNITS:
        if not np.any(values[:_UNIT_PROBE] % unit) and not np.any(values % unit):
            return unit
    return 1


def _widen_int(
    out: np.ndarray, enc: np.ndarray, unit: int, add: int, ref: np.ndarray | None
) -> None:
    """``out[:] = enc * unit + add (+ ref)`` in int64 (wrapping like the encoder's check)."""
    if unit != 1:
        np.multiply(enc, unit, out=out, dtype=_INT64)
    else:
        np.copyto(out, enc, casting="unsafe")
    if add:
        np.add(out, add, out=out)
    if ref is not None:
        np.add(out, ref, out=out)


def _offset_encoding(values: np.ndarray, unit: int) -> tuple[np.ndarray, int] | None:
    """``(enc, add)`` with ``values == enc * unit + add``, enc the narrowest int, or None."""
    if not len(values):
        return None
    scaled = values // unit if unit != 1 else values
    lo, hi = int(scaled.min()), int(scaled.max())
    dtype = _fitting_dtype(lo, hi)
    if dtype is not None:  # fits as is: no offset pass when widening
        return scaled.astype(dtype), 0
    dtype = _fitting_dtype(0, hi - lo)
    if dtype is None:
        return None
    return (scaled - lo).astype(dtype), lo * unit


def _changes(enc: np.ndarray) -> int:
    return int(np.count_nonzero(enc[1:] != enc[:-1]))


def int_recipe(values: np.ndarray, *, clock: bool, ref: np.ndarray | None = None) -> Recipe | None:
    """Narrowest exact encoding of an int64 (or datetime-like) column, or None.

    ``clock``: try millisecond/microsecond units (the values are nanoseconds).
    ``ref``: another column of the same rows the encoding may be relative to.
    Candidates are ranked by stored item size, then by how often the encoded
    value changes (runs are what lz4 folds), and the first one that decodes
    back bit for bit wins.
    """
    x = values.view(_INT64)
    if not len(x):
        return None
    candidates: list[tuple[tuple[int, int], Recipe]] = []
    unit = _unit_of(x) if clock else 1
    own = _offset_encoding(x, unit)
    if own is not None:
        enc, add = own
        candidates.append(((enc.dtype.itemsize, _changes(enc)), ("int", enc, unit, add, False)))
    if ref is not None:
        residual = np.subtract(x, ref.view(_INT64))
        first = int(residual[0])
        if not np.any(residual != first):
            candidates.append(((0, 0), ("ref", None, 1, first, True)))
        else:
            unit_r = _unit_of(residual) if clock else 1
            rel = _offset_encoding(residual, unit_r)
            if rel is not None:
                enc, add = rel
                key = (enc.dtype.itemsize, _changes(enc))
                candidates.append((key, ("int", enc, unit_r, add, True)))
    scratch = np.empty(len(x), dtype=_INT64)
    for _key, recipe in sorted(candidates, key=lambda item: item[0]):
        kind, enc, unit_c, add, uses_ref = recipe
        ref_values = ref.view(_INT64) if uses_ref and ref is not None else None
        try:
            if kind == "ref":
                np.add(ref_values, add, out=scratch)
            else:
                _widen_int(scratch, enc, unit_c, add, ref_values)
        except (OverflowError, TypeError, ValueError):
            continue
        if np.array_equal(scratch, x):
            return recipe
    return None


def codes_recipe(codes: np.ndarray) -> np.ndarray | None:
    """int32 dictionary codes as int8/int16 (None when they need all 32 bits)."""
    if not len(codes):
        return None
    dtype = _fitting_dtype(int(codes.min()), int(codes.max()))
    if dtype is None or dtype.itemsize >= _INT32.itemsize:
        return None
    return codes.astype(dtype)


def row_prefix(plan: tuple[Any, ...]) -> bytes:
    """The stored row's uncompressed head (magic + plan)."""
    blob = pickle.dumps(plan, protocol=5)
    return NARROW_MAGIC + _PLAN_LEN.pack(len(blob)) + blob


def split_row(blob: bytes | bytearray | memoryview) -> tuple[tuple[Any, ...], memoryview]:
    """``(plan, compressed content)`` of a stored narrowed row (``ValueError`` if malformed)."""
    view = memoryview(blob)
    if bytes(view[:4]) != NARROW_MAGIC or len(view) < 8:
        raise ValueError("not a narrowed driver row")
    start = 8 + _PLAN_LEN.unpack_from(view, 4)[0]
    try:
        plan = pickle.loads(view[8:start])
        expanded_len, tail_at, tail_len, _ops = plan
        valid = tail_at >= 0 and tail_at + tail_len <= expanded_len
    except (
        AttributeError,
        EOFError,
        ImportError,
        IndexError,
        KeyError,
        TypeError,
        ValueError,
        pickle.UnpicklingError,
    ) as e:
        raise ValueError(f"corrupt narrowed driver row plan: {e}") from e
    if not valid:
        raise ValueError("corrupt narrowed driver row plan: tail outside the row")
    return plan, view[start:]


def expand(plan: tuple[Any, ...], buffer: Any) -> None:
    """Widen the tail of ``buffer`` (the decompressed content plus slack) in place.

    ``buffer`` must be writable and exactly the plan's expanded length; on
    return it is the driver blob the row was encoded from. Raises
    ``ValueError`` on a malformed plan or buffer.
    """
    try:
        expanded_len, tail_at, tail_len, ops = plan
        data = np.frombuffer(buffer, dtype=np.uint8)
        if len(data) != expanded_len or not data.flags.writeable:
            raise ValueError("buffer does not match the narrowed row")
        # The widened columns overwrite the tail they are read from.
        tail = data[tail_at : tail_at + tail_len].copy()
        for op in ops:
            code = op[0]
            if code == OP_INT:
                _, dst, n, enc_dtype, src, unit, add, ref_dst = op
                enc_type = np.dtype(enc_dtype)
                enc = tail[src : src + n * enc_type.itemsize].view(enc_type)
                out = data[dst : dst + 8 * n].view(_INT64)
                ref = None if ref_dst is None else data[ref_dst : ref_dst + 8 * n].view(_INT64)
                if len(enc) != n or len(out) != n or (ref is not None and len(ref) != n):
                    raise ValueError("op outside the row")
                _widen_int(out, enc, unit, add, ref)
            elif code == OP_REF:
                _, dst, n, add, ref_dst = op
                out = data[dst : dst + 8 * n].view(_INT64)
                ref = data[ref_dst : ref_dst + 8 * n].view(_INT64)
                if len(out) != n or len(ref) != n:
                    raise ValueError("op outside the row")
                np.add(ref, add, out=out)
            elif code == OP_CODES:
                _, dst, n, enc_dtype, src = op
                enc_type = np.dtype(enc_dtype)
                enc = tail[src : src + n * enc_type.itemsize].view(enc_type)
                out = data[dst : dst + 4 * n].view(_INT32)
                if len(enc) != n or len(out) != n:
                    raise ValueError("op outside the row")
                np.copyto(out, enc, casting="unsafe")
            else:
                raise ValueError(f"unknown op {code!r}")
    except (IndexError, OverflowError, TypeError, ValueError) as e:
        raise ValueError(f"corrupt narrowed driver row: {e}") from e
