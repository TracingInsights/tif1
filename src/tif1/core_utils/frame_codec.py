"""Typed-columnar codec for materialized telemetry frames (Thebes TH1).

The materialized-frame cache tier used to pickle every per-(driver, lap)
DataFrame. Unpickling a frame costs ~127 us, ~80 us of which is pandas
rebuilding the frame skeleton (column ``Index``, ``BlockManager`` checks,
one ``__setstate__`` per block) rather than copying data. This codec stores
the frame's blocks as raw bytes behind a tiny header and rebuilds the frame
through the same direct block path the typed assembly uses
(:func:`tif1.core_utils.helpers._frame_from_blocks`): one ``np.frombuffer``
per block, no sanitize, a shared column ``Index``.

Encoded layout::

    magic (4 bytes) | header length (uint32 LE) | pickled header | body

The header is ``(columns, n_rows, block_specs)``; the body is the
concatenation of every block's raw bytes. Block kinds:

- ``np``      numpy block (float64/int64/bool/timedelta64...), ``(ncols, n)``
              row-major bytes;
- ``obj``     object block; each column is either a per-frame constant
              (``Driver``) or dictionary-encoded strings/None (``DriverAhead``);
- ``Int64``   nullable integer extension array (data + mask bytes), or a
              per-frame constant (``LapNumber``);
- ``str``     pandas ``str`` extension array, dictionary-encoded.

Any frame with a block the codec does not know is left to pickle (the
caller falls back), and legacy pickled rows stay readable because they
never start with the magic. Decoding returns writable frames (the body is
copied into a ``bytearray`` once), so cached frames behave exactly like
freshly assembled ones.
"""

from __future__ import annotations

import pickle
import struct
from itertools import accumulate
from typing import Any

import numpy as np
import pandas as pd

from . import helpers as _helpers
from .helpers import (
    _DIRECT_BLOCKS_OK,
    _block_placement,
    _extension_block,
    _extension_block_placed,
    _frame_from_blocks,
    _int64_array,
    _numpy_block,
    _numpy_block_placed,
)

FRAME_CODEC_MAGIC = b"TF1\x01"
_HEADER_LEN = struct.Struct("<I")
_STR_DTYPE = "str"
_OBJECT_DTYPE = np.dtype(object)


_STR_PANDAS_DTYPE: Any = None  # pd.StringDtype("pyarrow", na_value=nan), resolved lazily


def _pa():
    import pyarrow as pa

    return pa


def _codes_and_cats(pa_array: Any) -> tuple[np.ndarray, list[str]]:
    """Dictionary-encode a pyarrow string array into int32 codes (-1 = null) + categories."""
    encoded = pa_array.dictionary_encode()
    indices = encoded.indices
    if indices.null_count:
        codes = indices.fill_null(-1).to_numpy(zero_copy_only=False)
    else:
        codes = indices.to_numpy(zero_copy_only=False)
    return codes.astype("int32", copy=False), encoded.dictionary.to_pylist()


def _object_dictionary(values: np.ndarray) -> tuple[np.ndarray, list[str]]:
    """Dictionary-encode an object array of ``str``/None/NaN (anything else raises)."""
    pa = _pa()
    try:
        arr = pa.array(values, type=pa.large_string(), from_pandas=True)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        raise TypeError("non-string object column") from e
    return _codes_and_cats(arr)


def _str_dictionary(ea: Any) -> tuple[np.ndarray, list[str]]:
    """Dictionary-encode a pandas ``str`` extension array (pyarrow-backed)."""
    chunked = getattr(ea, "_pa_array", None)
    if chunked is None:
        return _object_dictionary(np.asarray(ea.astype(object)))
    return _codes_and_cats(chunked.combine_chunks())


def _decode_object(cats: list[str], codes: np.ndarray) -> np.ndarray:
    # A trailing None lets the -1 null code index the last slot directly.
    table = np.array([*cats, None], dtype=object)
    return table[codes]


def _decode_str(cats: list[str], codes: np.ndarray) -> Any:
    """Rebuild a ``str`` extension array from codes without re-inferring from objects."""
    global _STR_PANDAS_DTYPE
    pa = _pa()
    if (codes < 0).any():
        indices = pa.array(codes, type=pa.int32(), mask=codes < 0)
    else:
        indices = pa.array(codes, type=pa.int32())
    # ``take`` on the dictionary is the dictionary decode without the
    # DictionaryArray detour (Pergamon P1: ~23 -> ~15 us per column).
    strings = pa.array(cats, type=pa.large_string()).take(indices)
    if _STR_PANDAS_DTYPE is None:
        _STR_PANDAS_DTYPE = pd.StringDtype("pyarrow", na_value=np.nan)
    string_array: Any = pd.arrays.ArrowStringArray
    return string_array(strings, dtype=_STR_PANDAS_DTYPE)


def encode_frame(frame: pd.DataFrame) -> bytes | None:
    """Encode a telemetry DataFrame; ``None`` when a block is not supported."""
    if not _DIRECT_BLOCKS_OK or not isinstance(frame, pd.DataFrame):
        return None
    if (
        not isinstance(frame.index, pd.RangeIndex)
        or frame.index.start != 0
        or frame.index.step != 1
    ):
        return None
    n = len(frame)
    columns = [str(c) for c in frame.columns]
    if len(columns) != len(set(columns)):
        return None
    specs: list[tuple[Any, ...]] = []
    chunks: list[bytes] = []
    offset = 0
    try:
        for block in frame._mgr.blocks:
            placement = [int(p) for p in block.mgr_locs]
            values = block.values
            if isinstance(values, np.ndarray):
                if values.ndim != 2:
                    return None
                if values.dtype == _OBJECT_DTYPE:
                    col_specs: list[tuple[Any, ...]] = []
                    for row in range(values.shape[0]):
                        column = values[row]
                        if n and isinstance(column[0], str) and (column == column[0]).all():
                            col_specs.append(("const", column[0]))
                        else:
                            codes, cats = _object_dictionary(column)
                            raw = codes.tobytes()
                            col_specs.append(("dict", cats, offset, len(raw)))
                            chunks.append(raw)
                            offset += len(raw)
                    specs.append(("obj", placement, col_specs))
                    continue
                if values.dtype.hasobject or values.dtype.kind in "SUV":
                    return None
                raw = np.ascontiguousarray(values).tobytes()
                specs.append(("np", values.dtype.str, values.shape[0], placement, offset, len(raw)))
                chunks.append(raw)
                offset += len(raw)
                continue
            if isinstance(values, pd.arrays.TimedeltaArray | pd.arrays.DatetimeArray):
                # Every row of the block: a consolidated block holds several
                # columns (``SessionTime`` and ``Time`` share one).
                nd = values._ndarray
                if nd.ndim != 2:
                    nd = nd.reshape(1, n)
                if (
                    nd.dtype.kind not in "mM"
                    or nd.shape[0] != len(placement)
                    or getattr(values, "tz", None) is not None
                ):
                    return None
                raw = np.ascontiguousarray(nd).tobytes()
                specs.append(("np", nd.dtype.str, nd.shape[0], placement, offset, len(raw)))
                chunks.append(raw)
                offset += len(raw)
                continue
            # Extension block: values is the 1-D (or 2-D wrapped) array itself.
            ea = values[0] if getattr(values, "ndim", 1) == 2 else values
            if isinstance(ea, pd.arrays.IntegerArray) and str(ea.dtype) == "Int64":
                data = ea._data
                mask = ea._mask
                if n and not mask.any() and (data == data[0]).all():
                    specs.append(("Int64c", placement, int(data[0])))
                    continue
                raw_data = np.ascontiguousarray(data, dtype="int64").tobytes()
                raw_mask = np.ascontiguousarray(mask, dtype=bool).tobytes()
                specs.append(("Int64", placement, offset, offset + len(raw_data)))
                chunks.append(raw_data)
                chunks.append(raw_mask)
                offset += len(raw_data) + len(raw_mask)
                continue
            if str(ea.dtype) == _STR_DTYPE:
                codes, cats = _str_dictionary(ea)
                raw = codes.tobytes()
                specs.append(("str", placement, cats, offset, len(raw)))
                chunks.append(raw)
                offset += len(raw)
                continue
            return None
    except (AttributeError, TypeError, ValueError):
        return None
    header = pickle.dumps((columns, n, specs), protocol=5)
    return b"".join((FRAME_CODEC_MAGIC, _HEADER_LEN.pack(len(header)), header, *chunks))


def is_encoded_frame(blob: bytes | bytearray | memoryview) -> bool:
    return bytes(blob[:4]) == FRAME_CODEC_MAGIC


def decode_frame(blob: bytes | bytearray | memoryview, frame_class: Any = None) -> pd.DataFrame:
    """Rebuild a DataFrame written by :func:`encode_frame`.

    A writable buffer (``bytearray``, a mutable ``pyarrow.Buffer``, ...) is
    used in place: every block becomes a view into it, so the caller must
    hand over ownership. Read-only input is copied once.

    Raises ``ValueError`` on a malformed blob (callers treat it as a
    corrupt-row cache miss).
    """
    if not _DIRECT_BLOCKS_OK:
        raise ValueError("direct block assembly unavailable")
    view = memoryview(blob)
    if bytes(view[:4]) != FRAME_CODEC_MAGIC:
        raise ValueError("not a typed-columnar frame")
    header_len = _HEADER_LEN.unpack_from(view, 4)[0]
    body_start = 8 + header_len
    try:
        columns, n, specs = pickle.loads(view[8:body_start])
    except (pickle.UnpicklingError, EOFError, TypeError, ValueError) as e:
        raise ValueError(f"corrupt frame header: {e}") from e
    body = view[body_start:]
    if body.readonly:
        body = memoryview(bytearray(body))
    try:
        return _frame_from_blocks(_decode_blocks(body, n, specs), list(columns), n, frame_class)
    except (AttributeError, IndexError, TypeError, ValueError) as e:
        raise ValueError(f"corrupt frame body: {e}") from e


def _decode_blocks(body: memoryview, n: int, specs: list[tuple[Any, ...]]) -> tuple[Any, ...]:
    """Blocks over ``body`` for one frame's ``specs`` (views, no copies)."""
    frombuffer = np.frombuffer
    blocks: list[Any] = []
    for spec in specs:
        kind = spec[0]
        if kind == "np":
            _, dtype_str, ncols, placement, offset, _nbytes = spec
            if ncols != len(placement):  # rows of the 0.11 development cache
                raise ValueError("np block rows do not match its placement")
            arr = frombuffer(body, dtype=dtype_str, count=ncols * n, offset=offset)
            blocks.append(_numpy_block(arr.reshape(ncols, n), _placement(placement)))
        elif kind == "obj":
            _, placement, col_specs = spec
            values = np.empty((len(col_specs), n), dtype=object)
            for row, col_spec in enumerate(col_specs):
                if col_spec[0] == "const":
                    values[row, :] = col_spec[1]
                else:
                    _, cats, offset, _nbytes = col_spec
                    codes = frombuffer(body, dtype="int32", count=n, offset=offset)
                    values[row, :] = _decode_object(cats, codes)
            blocks.append(_numpy_block(values, _placement(placement)))
        elif kind == "Int64c":
            _, placement, value = spec
            ea = _int64_array(np.full(n, value, dtype="int64"), np.zeros(n, dtype=bool))
            blocks.append(_extension_block(ea, placement[0]))
        elif kind == "Int64":
            _, placement, data_offset, mask_offset = spec
            ea = _int64_array(
                frombuffer(body, dtype="int64", count=n, offset=data_offset),
                frombuffer(body, dtype=bool, count=n, offset=mask_offset),
            )
            blocks.append(_extension_block(ea, placement[0]))
        elif kind == "str":
            _, placement, cats, offset, _nbytes = spec
            codes = frombuffer(body, dtype="int32", count=n, offset=offset)
            blocks.append(_extension_block(_decode_str(cats, codes), placement[0]))
        else:
            raise ValueError(f"unknown block kind {kind!r}")
    return tuple(blocks)


def _placement(placement: list[int]) -> Any:
    if len(placement) == 1:
        return placement[0]
    return tuple(placement)


# -- per-driver columnar blobs (Pergamon P3) ----------------------------------
#
# One blob holds every materialized frame of a driver, column-concatenated:
# the frames are grouped by block layout (columns + block kinds + placements),
# each group stores one big array per block over the group's rows, and a
# frame is rebuilt from *views* into those arrays — ~8 slice + block
# constructions instead of one header, one buffer and ~25 Python calls per
# frame. Reading a session touches ~22 rows instead of ~1450.
#
# Encoded layout::
#
#     magic (4 bytes) | header length (uint32 LE) | pickled header | body
#
# The header is a list of groups ``(columns, laps, lengths, specs)``; a spec
# addresses the body like the per-frame codec's specs do, over the group's
# total row count:
#
# - ``np``     ``(dtype, ncols, placement, offset, nbytes)``, ``(ncols, total)``
#              row-major;
# - ``obj``    ``(placement, col_specs)`` with ``("const", value)`` or
#              ``("dict", cats, offset, nbytes)`` (int32 codes, -1 = null);
# - ``Int64``  ``(placement, data_offset, mask_offset)``;
# - ``Int64c`` ``(placement, values)`` — one constant per frame (LapNumber,
#              a DRS channel that never changes inside a lap);
# - ``str``    ``(placement, cats, offset, nbytes)`` (int32 codes).
#
# Frames whose blocks the codec does not know are handed back to the caller
# (they stay on the per-frame tier).

DRIVER_CODEC_MAGIC = b"TF1\x02"


def _frame_columns(
    frame: pd.DataFrame, seen: dict[int, tuple[Any, list[str] | None]] | None
) -> list[str] | None:
    """``[str(c) for c in frame.columns]`` (None on duplicates), memoized per Index object.

    Iterating a ``str``-dtype column ``Index`` walks its pyarrow array one
    scalar at a time (~15 us for 20 columns); ``tolist`` converts it in one
    call, and frames decoded from one layout group share their Index, so a
    driver's ~76 frames usually resolve it once (Pergamon Q13).
    """
    index = frame.columns
    if seen is not None:
        hit = seen.get(id(index))
        if hit is not None and hit[0] is index:
            return hit[1]
    columns: list[str] | None = [c if type(c) is str else str(c) for c in index.tolist()]
    if len(columns) != len(set(columns)):
        columns = None
    if seen is not None:
        seen[id(index)] = (index, columns)  # holding the Index keeps its id unique
    return columns


def _frame_parts(
    frame: pd.DataFrame, seen: dict[int, tuple[Any, list[str] | None]] | None = None
) -> tuple[list[str], int, list[tuple[Any, ...]]] | None:
    """Columns, row count and canonical block parts of a frame, or None if unsupported.

    Parts are ``(kind, placement_tuple, extra..., values...)`` sorted by
    placement so frames with the same blocks in a different order still
    share one layout. ``values`` are the live block arrays (no copies).
    ``seen`` memoizes the column list per column-Index object across calls.
    """
    if not _DIRECT_BLOCKS_OK or not isinstance(frame, pd.DataFrame):
        return None
    index = frame.index
    if not isinstance(index, pd.RangeIndex) or index.start != 0 or index.step != 1:
        return None
    n = len(frame)
    columns = _frame_columns(frame, seen)
    if columns is None:
        return None
    parts: list[tuple[Any, ...]] = []
    try:
        for block in frame._mgr.blocks:
            placement = tuple(block.mgr_locs.as_array.tolist())
            values = block.values
            if isinstance(values, np.ndarray):
                if values.ndim != 2:
                    return None
                if values.dtype == _OBJECT_DTYPE:
                    parts.append(("obj", placement, values))
                    continue
                if values.dtype.hasobject or values.dtype.kind in "SUV":
                    return None
                parts.append(("np", placement, values.dtype.str, values))
                continue
            if isinstance(values, pd.arrays.TimedeltaArray | pd.arrays.DatetimeArray):
                # The block's backing ndarray directly: ``values[0]`` on the
                # 2-D array goes through the datetimelike ``__getitem__``
                # (~10 us). Every row: a consolidated block holds several
                # columns (``SessionTime`` and ``Time`` share one).
                nd = values._ndarray
                if nd.ndim != 2:
                    nd = nd.reshape(1, n)
                if (
                    nd.dtype.kind not in "mM"
                    or nd.shape[0] != len(placement)
                    or getattr(values, "tz", None) is not None
                ):
                    return None
                parts.append(("np", placement, nd.dtype.str, nd))
                continue
            ea = values[0] if getattr(values, "ndim", 1) == 2 else values
            if isinstance(ea, pd.arrays.TimedeltaArray | pd.arrays.DatetimeArray):
                nd = np.asarray(ea)
                parts.append(("np", placement, nd.dtype.str, nd.reshape(1, n)))
                continue
            if isinstance(ea, pd.arrays.IntegerArray) and str(ea.dtype) == "Int64":
                parts.append(("Int64", placement, ea._data, ea._mask))
                continue
            if str(ea.dtype) == _STR_DTYPE:
                parts.append(("str", placement, ea))
                continue
            return None
    except (AttributeError, TypeError, ValueError):
        return None
    parts.sort(key=lambda part: part[1])
    return columns, n, parts


def _layout_key(columns: list[str], parts: list[tuple[Any, ...]]) -> tuple[Any, ...]:
    return (
        tuple(columns),
        tuple((p[0], p[1], p[2]) if p[0] == "np" else (p[0], p[1]) for p in parts),
    )


def _object_column_spec(
    values: list[np.ndarray], lengths: list[int], writes: list[tuple[Any, ...]], offset: int
) -> tuple[tuple[Any, ...], int]:
    """Spec for one object column over a group (constant or dictionary-encoded).

    One concatenation and one vectorized comparison for the whole group
    instead of one comparison per frame (Pergamon Q13); a group is constant
    when every frame is non-empty and every value equals the first one.
    """
    joined = np.concatenate(values) if len(values) > 1 else values[0]
    if len(joined) and isinstance(joined[0], str) and all(lengths):
        head = joined[0]
        if (joined == head).all():
            return ("const", head), offset
    codes, cats = _object_dictionary(joined)
    writes.append(("raw", offset, codes))
    nbytes = codes.nbytes
    return ("dict", cats, offset, nbytes), offset + nbytes


def _int64_group_constants(
    datas: list[np.ndarray], masks: list[np.ndarray], lengths: list[int]
) -> tuple[list[int] | None, np.ndarray, np.ndarray]:
    """Per-frame constants of an Int64 block (None if any frame varies) + the joined arrays."""
    data = np.concatenate(datas) if len(datas) > 1 else datas[0]
    mask = np.concatenate(masks) if len(masks) > 1 else masks[0]
    if not all(lengths) or mask.any():
        return None, data, mask
    sizes = np.asarray(lengths, dtype="int64")
    starts = np.zeros(len(sizes), dtype="int64")
    np.cumsum(sizes[:-1], out=starts[1:])
    firsts = data[starts]
    if not (data == np.repeat(firsts, sizes)).all():
        return None, data, mask
    return [int(v) for v in firsts.tolist()], data, mask


def _fill_body(out: np.ndarray, body_start: int, writes: list[tuple[Any, ...]]) -> None:
    """Copy every pending block write into ``out`` (one copy per source array)."""
    for write in writes:
        kind, offset = write[0], body_start + write[1]
        if kind == "raw":
            src = write[2]
            out[offset : offset + src.nbytes] = src.reshape(-1).view(np.uint8)
            continue
        # ("cat", offset, dtype, ncols, total, arrays): the (ncols, n_i) frame
        # arrays laid side by side as one row-major (ncols, total) block.
        _, _, dtype, ncols, total, arrays = write
        dest = out[offset : offset + dtype.itemsize * ncols * total].view(dtype)
        dest = dest.reshape(ncols, total)
        if len(arrays) == 1:
            np.copyto(dest, arrays[0])
        else:
            np.concatenate(arrays, axis=1, out=dest)


def encode_driver_frames(
    frames: list[tuple[int, pd.DataFrame]],
) -> tuple[memoryview | None, list[tuple[int, pd.DataFrame]]]:
    """Encode a driver's ``(lap, frame)`` list; returns ``(blob, unsupported frames)``.

    ``blob`` is None when no frame could be encoded. It is a writable
    ``memoryview`` over one buffer the encoder fills in place: the specs and
    header are computed first, then every block's bytes are written straight
    to their final offset (one copy per block instead of concatenate +
    ``tobytes`` + ``join``, and the fresh pages are touched once).
    """
    groups: dict[tuple[Any, ...], list[tuple[int, int, list[tuple[Any, ...]]]]] = {}
    group_columns: dict[tuple[Any, ...], list[str]] = {}
    leftovers: list[tuple[int, pd.DataFrame]] = []
    seen: dict[int, tuple[Any, list[str] | None]] = {}
    for lap, frame in sorted(frames, key=lambda item: item[0]):
        described = _frame_parts(frame, seen)
        if described is None:
            leftovers.append((lap, frame))
            continue
        columns, n, parts = described
        key = _layout_key(columns, parts)
        groups.setdefault(key, []).append((int(lap), n, parts))
        group_columns.setdefault(key, columns)
    if not groups:
        return None, leftovers
    header: list[tuple[Any, ...]] = []
    writes: list[tuple[Any, ...]] = []
    offset = 0
    try:
        for key, members in groups.items():
            laps = [lap for lap, _, _ in members]
            lengths = [n for _, n, _ in members]
            total = sum(lengths)
            specs: list[tuple[Any, ...]] = []
            first_parts = members[0][2]
            for index in range(len(first_parts)):
                kind = first_parts[index][0]
                placement = list(first_parts[index][1])
                if kind == "np":
                    dtype_str = first_parts[index][2]
                    arrays = [m[2][index][3] for m in members]
                    dtype = arrays[0].dtype
                    ncols = arrays[0].shape[0]
                    if any(a.dtype != dtype or a.shape[0] != ncols for a in arrays):
                        raise ValueError("np block shape/dtype differs inside a layout group")
                    nbytes = dtype.itemsize * ncols * total
                    specs.append(("np", dtype_str, ncols, placement, offset, nbytes))
                    writes.append(("cat", offset, dtype, ncols, total, arrays))
                    offset += nbytes
                elif kind == "obj":
                    col_specs: list[tuple[Any, ...]] = []
                    ncols = first_parts[index][2].shape[0]
                    for row in range(ncols):
                        col_spec, offset = _object_column_spec(
                            [m[2][index][2][row] for m in members], lengths, writes, offset
                        )
                        col_specs.append(col_spec)
                    specs.append(("obj", placement, col_specs))
                elif kind == "Int64":
                    constants, data, mask = _int64_group_constants(
                        [m[2][index][2] for m in members],
                        [m[2][index][3] for m in members],
                        lengths,
                    )
                    if constants is not None:
                        specs.append(("Int64c", placement, constants))
                        continue
                    data = np.ascontiguousarray(data, dtype="int64")
                    mask = np.ascontiguousarray(mask, dtype=bool)
                    specs.append(("Int64", placement, offset, offset + data.nbytes))
                    writes.append(("raw", offset, data))
                    writes.append(("raw", offset + data.nbytes, mask))
                    offset += data.nbytes + mask.nbytes
                elif kind == "str":
                    pa = _pa()
                    arrays = []
                    for m in members:
                        ea = m[2][index][2]
                        chunked = getattr(ea, "_pa_array", None)
                        if chunked is None:
                            arrays.append(
                                pa.array(
                                    np.asarray(ea.astype(object)),
                                    type=pa.large_string(),
                                    from_pandas=True,
                                )
                            )
                        else:
                            arrays.append(chunked.combine_chunks().cast(pa.large_string()))
                    joined_pa = pa.concat_arrays(arrays) if len(arrays) > 1 else arrays[0]
                    codes, cats = _codes_and_cats(joined_pa)
                    specs.append(("str", placement, cats, offset, codes.nbytes))
                    writes.append(("raw", offset, codes))
                    offset += codes.nbytes
                else:  # pragma: no cover - _frame_parts only emits the kinds above
                    raise TypeError(kind)
            header.append((group_columns[key], laps, lengths, specs))
        header_bytes = pickle.dumps(header, protocol=5)
        body_start = 8 + len(header_bytes)
        out = np.empty(body_start + offset, dtype=np.uint8)
        out[:4] = np.frombuffer(DRIVER_CODEC_MAGIC, dtype=np.uint8)
        out[4:8] = np.frombuffer(_HEADER_LEN.pack(len(header_bytes)), dtype=np.uint8)
        out[8:body_start] = np.frombuffer(header_bytes, dtype=np.uint8)
        _fill_body(out, body_start, writes)
    except (AttributeError, TypeError, ValueError):
        return None, leftovers + [
            (lap, frame) for lap, frame in frames if all(lap != lo for lo, _ in leftovers)
        ]
    return memoryview(out), leftovers


def is_driver_blob(blob: bytes | bytearray | memoryview) -> bool:
    return bytes(blob[:4]) == DRIVER_CODEC_MAGIC


def _driver_header(view: memoryview) -> tuple[list[tuple[Any, ...]], int]:
    if bytes(view[:4]) != DRIVER_CODEC_MAGIC:
        raise ValueError("not a driver frame blob")
    header_len = _HEADER_LEN.unpack_from(view, 4)[0]
    body_start = 8 + header_len
    try:
        header = pickle.loads(view[8:body_start])
    except (pickle.UnpicklingError, EOFError, TypeError, ValueError) as e:
        raise ValueError(f"corrupt driver blob header: {e}") from e
    return header, body_start


def driver_blob_laps(blob: bytes | bytearray | memoryview) -> list[int]:
    """Lap numbers stored in a driver blob (header only)."""
    header, _ = _driver_header(memoryview(blob))
    laps: list[int] = []
    for _columns, group_laps, _lengths, _specs in header:
        laps.extend(int(lap) for lap in group_laps)
    return laps


def _group_parts(
    body: memoryview, laps: list[int], total: int, specs: list[tuple[Any, ...]]
) -> list[tuple[Any, ...]]:
    """Group-level arrays (one per block spec) with their shared placements."""
    frombuffer = np.frombuffer
    parts: list[tuple[Any, ...]] = []
    for spec in specs:
        kind = spec[0]
        if kind == "np":
            _, dtype_str, ncols, placement, offset, _nbytes = spec
            if ncols != len(placement):  # rows of the 0.11 development cache
                raise ValueError("np block rows do not match its placement")
            arr = frombuffer(body, dtype=dtype_str, count=ncols * total, offset=offset)
            parts.append(("np", _block_placement(_placement(placement)), arr.reshape(ncols, total)))
        elif kind == "obj":
            _, placement, col_specs = spec
            values = np.empty((len(col_specs), total), dtype=object)
            for row, col_spec in enumerate(col_specs):
                if col_spec[0] == "const":
                    values[row, :] = col_spec[1]
                else:
                    _, cats, offset, _nbytes = col_spec
                    codes = frombuffer(body, dtype="int32", count=total, offset=offset)
                    values[row, :] = _decode_object(cats, codes)
            parts.append(("np", _block_placement(_placement(placement)), values))
        elif kind == "Int64c":
            _, placement, values = spec
            if len(values) != len(laps):
                raise ValueError("Int64c constants do not match the lap count")
            parts.append(("Int64c", _block_placement(placement[0]), values))
        elif kind == "Int64":
            _, placement, data_offset, mask_offset = spec
            parts.append(
                (
                    "Int64",
                    _block_placement(placement[0]),
                    frombuffer(body, dtype="int64", count=total, offset=data_offset),
                    frombuffer(body, dtype=bool, count=total, offset=mask_offset),
                )
            )
        elif kind == "str":
            _, placement, cats, offset, _nbytes = spec
            codes = frombuffer(body, dtype="int32", count=total, offset=offset)
            parts.append(("str", _block_placement(placement[0]), _decode_str(cats, codes)))
        else:
            raise ValueError(f"unknown block kind {kind!r}")
    return parts


def _writable_body(
    blob: bytes | bytearray | memoryview,
) -> tuple[list[tuple[Any, ...]], memoryview]:
    if not _DIRECT_BLOCKS_OK:
        raise ValueError("direct block assembly unavailable")
    view = memoryview(blob)
    header, body_start = _driver_header(view)
    body = view[body_start:]
    if body.readonly:
        body = memoryview(bytearray(body))
    return header, body


def decode_driver_merged(
    blob: bytes | bytearray | memoryview,
    laps: list[int],
    frame_class: Any = None,
    *,
    drop_repeats: bool = False,
) -> pd.DataFrame | None:
    """One frame holding ``laps`` back to back, straight from the group arrays.

    Pergamon P10: ``Laps.telemetry`` for a driver is the concatenation of the
    driver's lap frames in laps-row order. Every maximal run of laps that
    are stored consecutively in one layout group becomes a slice of that
    group's arrays (no per-lap frames); a single run is returned as is,
    several runs are concatenated (a handful of pieces instead of one per
    lap, with the same dtype promotion ``pd.concat`` of the lap frames
    gives). Returns None when a lap is not stored in the blob.

    ``drop_repeats`` leaves out a lap's first row when its ``Date`` equals
    the row before it (each lap frame repeats the sample at its boundary):
    the kept stretches are joined with one ``np.concatenate`` per block
    instead of a row gather over the whole frame afterwards.
    """
    if not laps:
        return None
    header, body = _writable_body(blob)
    try:
        where: dict[int, tuple[int, int]] = {}
        groups: list[tuple[Any, ...]] = []
        for g, (group_columns, raw_group_laps, lengths, specs) in enumerate(header):
            group_laps = [int(lap) for lap in raw_group_laps]
            starts = np.zeros(len(lengths) + 1, dtype="int64")
            np.cumsum(lengths, out=starts[1:])
            groups.append((list(group_columns), group_laps, list(lengths), specs, starts))
            for position, lap in enumerate(group_laps):
                where.setdefault(lap, (g, position))
        wanted = [int(lap) for lap in laps]
        if any(lap not in where for lap in wanted):
            return None
        # Maximal runs of (group, consecutive positions) in laps order.
        runs: list[tuple[int, int, int]] = []  # (group, first position, count)
        for lap in wanted:
            g, position = where[lap]
            if runs and runs[-1][0] == g and runs[-1][1] + runs[-1][2] == position:
                runs[-1] = (g, runs[-1][1], runs[-1][2] + 1)
            else:
                runs.append((g, position, 1))
        parts_by_group: dict[int, list[tuple[Any, ...]]] = {}
        pieces: list[pd.DataFrame] = []
        previous_clock: int | None = None  # Date of the last row taken so far
        for g, first, k in runs:
            columns, group_laps, lengths, specs, starts = groups[g]
            total = int(starts[-1])
            parts = parts_by_group.get(g)
            if parts is None:
                parts = parts_by_group[g] = _group_parts(body, group_laps, total, specs)
            start, end = int(starts[first]), int(starts[first + k])
            # Row stretches to keep and each lap's kept length.
            spans = [(start, end)]
            kept = [int(length) for length in lengths[first : first + k]]
            clock = _date_values(parts, columns) if drop_repeats else None
            if clock is not None and end > start:
                spans = []
                stretch = start
                for j in range(k):
                    row = int(starts[first + j])
                    before = previous_clock if j == 0 else int(clock[row - 1])
                    if (
                        kept[j]
                        and before is not None
                        and before != _NAT_NS
                        and clock[row] == before
                    ):
                        if row > stretch:
                            spans.append((stretch, row))
                        stretch = row + 1
                        kept[j] -= 1
                if end > stretch:
                    spans.append((stretch, end))
                previous_clock = int(clock[end - 1])
            n = sum(stop - begin for begin, stop in spans)
            blocks: list[Any] = []
            for part in parts:
                kind = part[0]
                if kind == "np":
                    blocks.append(_numpy_block_placed(_take_spans(part[2], spans), part[1]))
                elif kind == "Int64":
                    data = _take_spans(part[2], spans)
                    mask = _take_spans(part[3], spans)
                    blocks.append(_extension_block_placed(_int64_array(data, mask), part[1]))
                elif kind == "Int64c":
                    data = np.repeat(
                        np.asarray(part[2][first : first + k], dtype="int64"),
                        np.asarray(kept, dtype="int64"),
                    )
                    ea = _int64_array(data, np.zeros(n, dtype=bool))
                    blocks.append(_extension_block_placed(ea, part[1]))
                else:  # str
                    ea = part[2]
                    pieces_ea = [ea[begin:stop] for begin, stop in spans]
                    joined = (
                        pieces_ea[0]
                        if len(pieces_ea) == 1
                        else type(ea)._concat_same_type(pieces_ea)
                    )
                    blocks.append(_extension_block_placed(joined, part[1]))
            pieces.append(_frame_from_blocks(tuple(blocks), columns, n, frame_class))
    except (AttributeError, IndexError, TypeError, ValueError) as e:
        raise ValueError(f"corrupt driver blob body: {e}") from e
    if len(pieces) == 1:
        return pieces[0]
    return pd.concat(pieces, ignore_index=True)


_NAT_NS = np.iinfo("int64").min


def _take_spans(values: np.ndarray, spans: list[tuple[int, int]]) -> np.ndarray:
    """The ``spans`` of ``values`` (last axis) back to back, a view of ``values``.

    The stretches are moved down to the start of the first one in place:
    ``values`` is a view of this decode's own buffer and the spans of one run
    lie inside that run's rows, so nothing else reads them. Moving the data
    touches no fresh memory, where a new ~7 MB array costs its page faults
    (a race driver: 6 -> ~1.5 ms). A read-only array is concatenated instead.
    """
    begin = spans[0][0]
    if len(spans) == 1:
        return values[..., begin : spans[0][1]]
    if not values.flags.writeable:
        return np.concatenate([values[..., start:stop] for start, stop in spans], axis=-1)
    end = begin
    for start, stop in spans:
        length = stop - start
        if start != end:
            values[..., end : end + length] = values[..., start:stop]
        end += length
    return values[..., begin:end]


def _date_values(parts: list[tuple[Any, ...]], columns: list[str]) -> np.ndarray | None:
    """The group's ``Date`` column as int64 nanoseconds (None without one)."""
    if "Date" not in columns:
        return None
    position = columns.index("Date")
    for part in parts:
        if part[0] != "np":
            continue
        rows = list(part[1].as_array) if hasattr(part[1], "as_array") else list(part[1])
        if position in rows:
            values = part[2][rows.index(position)]
            return values.view("int64") if values.dtype.kind == "M" else None
    return None


class DriverFrameSource:
    """Decoded group arrays of one driver blob; lap frames are built on demand.

    Pergamon Q1: a warm ``Session.load()`` decodes every driver row (header,
    writable body, one ``np.frombuffer`` per block) but defers the ~11 us of
    per-lap block + ``BlockManager`` + frame construction until a lap is
    first touched; most sessions never touch more than a few laps. The
    frames are the same views of the same body the eager
    :func:`decode_driver_frames` builds, so parity is by construction.
    Raises ``ValueError`` on a malformed blob.
    """

    __slots__ = ("_builders", "_frame_class", "_groups", "_where", "laps")

    def __init__(self, blob: bytes | bytearray | memoryview, frame_class: Any = None) -> None:
        header, body = _writable_body(blob)
        self._frame_class = frame_class
        # (columns, laps, row offsets, parts) per layout group; a lap maps to
        # (group, position) — the last group storing it wins, as the eager
        # decode's dict overwrite did.
        groups: list[tuple[list[str], list[int], list[int], list[tuple[Any, ...]]]] = []
        where: dict[int, tuple[int, int]] = {}
        try:
            for g, (group_columns, raw_laps, lengths, specs) in enumerate(header):
                laps = [int(lap) for lap in raw_laps]
                total = int(sum(lengths))
                starts = np.zeros(len(lengths) + 1, dtype="int64")
                np.cumsum(lengths, out=starts[1:])
                parts = _group_parts(body, laps, total, specs)
                groups.append((list(group_columns), laps, starts.tolist(), parts))
                for position, lap in enumerate(laps):
                    where[lap] = (g, position)
        except (AttributeError, IndexError, TypeError, ValueError) as e:
            raise ValueError(f"corrupt driver blob body: {e}") from e
        self._groups = groups
        self._where = where
        self._builders: dict[int, _LapBuilder] = {}
        self.laps: list[int] = list(where)

    def _builder(self, g: int) -> _LapBuilder:
        builder = self._builders.get(g)
        if builder is None:
            columns, _laps, offsets, parts = self._groups[g]
            lengths = [offsets[i + 1] - offsets[i] for i in range(len(offsets) - 1)]
            builder = _LapBuilder(parts, columns, lengths, offsets[-1], self._frame_class)
            self._builders[g] = builder
        return builder

    def __contains__(self, lap: object) -> bool:
        return lap in self._where

    def frame(self, lap: int) -> pd.DataFrame:
        """Build the frame of ``lap`` (``KeyError`` when the blob does not hold it)."""
        g, i = self._where[lap]
        columns, _laps, offsets, parts = self._groups[g]
        start, end = offsets[i], offsets[i + 1]
        n = end - start
        try:
            if _FAST_LAP_FRAMES:
                return self._builder(g).build(start, end)
            blocks: list[Any] = []
            for part in parts:
                kind = part[0]
                if kind == "np":
                    blocks.append(_numpy_block_placed(part[2][:, start:end], part[1]))
                elif kind == "Int64":
                    blocks.append(
                        _extension_block_placed(
                            _int64_array(part[2][start:end], part[3][start:end]), part[1]
                        )
                    )
                elif kind == "Int64c":
                    ea = _int64_array(
                        np.full(n, part[2][i], dtype="int64"), np.zeros(n, dtype=bool)
                    )
                    blocks.append(_extension_block_placed(ea, part[1]))
                else:  # str
                    blocks.append(_extension_block_placed(part[2][start:end], part[1]))
            return _frame_from_blocks(tuple(blocks), columns, n, self._frame_class)
        except (AttributeError, IndexError, TypeError, ValueError) as e:
            raise ValueError(f"corrupt driver blob body: {e}") from e

    def frames(self, wanted: set[int] | None = None) -> dict[int, pd.DataFrame]:
        """Frames by lap (all laps, or only ``wanted``), in stored order."""
        out: dict[int, pd.DataFrame] = {}
        frame_class = self._frame_class
        numpy_block = _numpy_block_placed
        extension_block = _extension_block_placed
        int64_array = _int64_array
        full = np.full
        zeros = np.zeros
        try:
            if _FAST_LAP_FRAMES:
                for g, (_columns, laps, offsets, _parts) in enumerate(self._groups):
                    build = self._builder(g).build
                    for i, lap in enumerate(laps):
                        if wanted is None or lap in wanted:
                            out[lap] = build(offsets[i], offsets[i + 1])
                return out
            for columns, laps, offsets, parts in self._groups:
                for i, lap in enumerate(laps):
                    if wanted is not None and lap not in wanted:
                        continue
                    start, end = offsets[i], offsets[i + 1]
                    n = end - start
                    blocks: list[Any] = []
                    for part in parts:
                        kind = part[0]
                        if kind == "np":
                            blocks.append(numpy_block(part[2][:, start:end], part[1]))
                        elif kind == "Int64":
                            blocks.append(
                                extension_block(
                                    int64_array(part[2][start:end], part[3][start:end]), part[1]
                                )
                            )
                        elif kind == "Int64c":
                            ea = int64_array(
                                full(n, part[2][i], dtype="int64"), zeros(n, dtype=bool)
                            )
                            blocks.append(extension_block(ea, part[1]))
                        else:  # str
                            blocks.append(extension_block(part[2][start:end], part[1]))
                    out[lap] = _frame_from_blocks(tuple(blocks), columns, n, frame_class)
        except (AttributeError, IndexError, TypeError, ValueError) as e:
            raise ValueError(f"corrupt driver blob body: {e}") from e
        return out


def decode_driver_frames(
    blob: bytes | bytearray | memoryview,
    wanted: set[int] | None = None,
    frame_class: Any = None,
) -> dict[int, pd.DataFrame]:
    """Frames by lap from a driver blob (all laps, or only ``wanted``).

    The frames are views into the blob's body (copied once when the input
    is read-only), so the caller hands over ownership of a writable buffer.
    Raises ``ValueError`` on a malformed blob.
    """
    return DriverFrameSource(blob, frame_class).frames(wanted)


# -- per-lap frames over the group arrays (Pergamon Q18) ----------------------
#
# ``decode_driver_frames`` builds ~8 blocks per lap. The generic helpers
# re-derive per call what is fixed per group (datetime-like or not, the
# column ``Index``, the ``Int64`` constants), and slicing a ``str`` column
# goes through ``ArrowExtensionArray.__getitem__`` (indexer validation,
# ``getitem_returns_view``, ``ArrowStringArray.__init__`` with its type
# checks: ~5.8 us vs ~0.75 us per lap on the 2018-2025 layouts where
# ``DriverAhead`` is a ``str`` column). ``_lap_plan`` resolves everything
# once per group and the per-lap loop only slices and calls constructors:
#
# - block classes are called directly with positional arguments;
# - ``Int64c`` constants become one repeated array + one all-False mask per
#   group, sliced per lap (disjoint slices: frames stay mutation-isolated);
# - a ``str`` lap is an ``ArrowStringArray`` over ``ChunkedArray.slice``
#   carrying the attributes ``__getitem__`` sets;
# - a ``Time`` lap is ``NDArrayBacked._simple_new`` + ``_freq = None``,
#   which is ``TimedeltaArray._simple_new`` without its argument asserts
#   (the group's dtype is checked once instead).
#
# The two hand-built arrays are verified against the public route once per
# process (``_fast_arrays``); on any drift the route falls back to
# ``__getitem__`` / ``TimedeltaArray._simple_new``.

_P_NP, _P_TD, _P_WRAP, _P_INT, _P_STR, _P_EA = range(6)

_FAST_LAP_FRAMES = bool(_DIRECT_BLOCKS_OK and getattr(_helpers, "_DIRECT_BLOCK_CLASSES_OK", False))
_FAST_ARRAYS: tuple[bool, bool] | None = None  # (str slice, timedelta), resolved on first use


def _str_slice(cls: Any, chunked: Any, dtype: Any, start: int, n: int) -> Any:
    """``ea[start:start + n]`` for an ``ArrowStringArray`` without ``__getitem__``."""
    sliced = object.__new__(cls)
    sliced._pa_array = chunked.slice(start, n)
    sliced._dtype = dtype
    sliced._readonly = False
    return sliced


def _timedelta_new() -> Any:
    tda: Any = pd.arrays.TimedeltaArray
    return super(tda, tda)._simple_new


def _timedelta_array(base_new: Any, values: np.ndarray, dtype: np.dtype) -> Any:
    """``TimedeltaArray._simple_new(values, dtype=dtype)`` without the asserts."""
    arr = base_new(values, dtype)
    arr._freq = None
    return arr


def _probe_str_slice() -> bool:
    ea = _decode_str(["a", "bb"], np.array([0, -1, 1, 1, 0], dtype="int32"))
    cls = type(ea)
    if cls is not pd.arrays.ArrowStringArray:
        return False
    for start, stop in ((1, 4), (2, 2), (4, 5)):
        ref = ea[start:stop]
        got = _str_slice(cls, ea._pa_array, ea._dtype, start, stop - start)
        if (
            type(got) is not type(ref)
            or list(vars(got)) != list(vars(ref))
            or got._dtype is not ref._dtype
            or got._readonly != ref._readonly
            or got._pa_array.num_chunks != ref._pa_array.num_chunks
            or not got._pa_array.equals(ref._pa_array)
        ):
            return False
    return True


def _probe_timedelta() -> bool:
    values = np.array([[0, 16_000_000, 256_000_000]], dtype="m8[ns]")
    tda: Any = pd.arrays.TimedeltaArray
    ref = tda._simple_new(values[:, 1:], dtype=values.dtype)
    got = _timedelta_array(_timedelta_new(), values[:, 1:], values.dtype)
    return bool(
        type(got) is type(ref)
        and vars(got) == vars(ref)
        and got.dtype == ref.dtype
        and got.freq is None
        and got._ndarray.shape == ref._ndarray.shape
        and np.shares_memory(got._ndarray, values)
        and np.array_equal(got._ndarray, ref._ndarray)
    )


def _fast_arrays() -> tuple[bool, bool]:
    global _FAST_ARRAYS
    if _FAST_ARRAYS is None:
        flags: list[bool] = []
        for probe in (_probe_str_slice, _probe_timedelta):
            try:
                flags.append(probe())
            except Exception:  # pragma: no cover - pandas internals changed
                flags.append(False)
        _FAST_ARRAYS = (flags[0], flags[1])
    return _FAST_ARRAYS


def _lap_plan(
    parts: list[tuple[Any, ...]], lengths: list[int], total: int
) -> list[tuple[Any, ...]]:
    """``(code, values, extra, placement)`` per block, resolved once per group."""
    fast_str, fast_td = _fast_arrays()
    plan: list[tuple[Any, ...]] = []
    for part in parts:
        kind = part[0]
        if kind == "np":
            values = part[2]
            dtype = values.dtype
            if dtype.kind == "m":
                if fast_td and np.datetime_data(dtype)[0] != "generic":
                    plan.append((_P_TD, values, (_timedelta_new(), dtype), part[1]))
                else:
                    plan.append((_P_WRAP, values, _helpers._TimedeltaArray._simple_new, part[1]))
            elif dtype.kind == "M":
                plan.append((_P_WRAP, values, _helpers._DatetimeArray._simple_new, part[1]))
            else:
                plan.append((_P_NP, values, None, part[1]))
        elif kind == "Int64":
            plan.append((_P_INT, part[2], part[3], part[1]))
        elif kind == "Int64c":
            data = np.repeat(np.asarray(part[2], dtype="int64"), np.asarray(lengths, dtype="int64"))
            plan.append((_P_INT, data, np.zeros(total, dtype=bool), part[1]))
        else:  # str
            ea = part[2]
            if fast_str and type(ea) is pd.arrays.ArrowStringArray:
                plan.append((_P_STR, ea._pa_array, ea._dtype, part[1]))
            else:  # pragma: no cover - pandas internals changed
                plan.append((_P_EA, ea, None, part[1]))
    return plan


class _LapBuilder:
    """Per-group context of the ``_FAST_LAP_FRAMES`` route; ``build`` makes one lap frame.

    Pergamon Q18: everything fixed per layout group (the block plan, the
    column ``Index``, the frame constructor and its metadata) is resolved
    once; ``build`` only slices the group arrays and calls constructors.
    Used by the bulk decode and, cached per group, by the lazy
    :class:`DriverFrameSource` (Q1) single-lap path.
    """

    __slots__ = ("_column_index", "_from_mgr", "_metadata", "_plan")

    def __init__(
        self,
        parts: list[tuple[Any, ...]],
        columns: list[str],
        lengths: list[int],
        total: int,
        frame_class: Any,
    ) -> None:
        self._plan = _lap_plan(parts, lengths, total)
        self._column_index = _helpers._shared_column_index(columns)
        if frame_class is None or frame_class is pd.DataFrame:
            self._from_mgr = _helpers._dataframe_from_mgr
            self._metadata: tuple[str, ...] = ()
        else:
            self._from_mgr = frame_class._from_mgr
            self._metadata = tuple(getattr(frame_class, "_metadata", ()))

    def build(self, start: int, end: int) -> pd.DataFrame:
        numpy_block = _helpers._NUMPY_BLOCK_CLASS
        wrapped_block = _helpers._DATETIMELIKE_BLOCK_CLASS
        extension_block = _helpers._EXTENSION_BLOCK_CLASS
        int64_array: Any = _helpers._INTEGER_ARRAY_SIMPLE_NEW
        blocks: list[Any] = []
        for code, values, extra, placement in self._plan:
            if code == _P_NP:
                blocks.append(numpy_block(values[:, start:end], placement, 2))
            elif code == _P_INT:
                ea = int64_array(values[start:end], extra[start:end])
                blocks.append(extension_block(ea, placement, 2))
            elif code == _P_TD:
                wrapped = extra[0](values[:, start:end], extra[1])
                wrapped._freq = None
                blocks.append(wrapped_block(wrapped, placement, 2))
            elif code == _P_STR:
                sliced: Any = object.__new__(pd.arrays.ArrowStringArray)
                sliced._pa_array = values.slice(start, end - start)
                sliced._dtype = extra
                sliced._readonly = False
                blocks.append(extension_block(sliced, placement, 2))
            elif code == _P_WRAP:
                window = values[:, start:end]
                blocks.append(wrapped_block(extra(window, dtype=window.dtype), placement, 2))
            else:  # pragma: no cover - see _lap_plan
                blocks.append(extension_block(values[start:end], placement, 2))
        mgr = _helpers._BlockManager(
            tuple(blocks),
            [self._column_index, _helpers._range_index(end - start)],
            verify_integrity=False,
        )
        frame = self._from_mgr(mgr, axes=mgr.axes)
        for name in self._metadata:
            object.__setattr__(frame, name, None)
        return frame


def _lap_frames_into(
    out: dict[int, pd.DataFrame],
    parts: list[tuple[Any, ...]],
    columns: list[str],
    laps: list[int],
    lengths: list[int],
    total: int,
    wanted: set[int] | None,
    frame_class: Any,
) -> None:
    """One frame per lap of a group into ``out`` (the ``_FAST_LAP_FRAMES`` route)."""
    builder = _LapBuilder(parts, columns, lengths, total, frame_class)
    starts = [0, *accumulate(int(length) for length in lengths)]
    if starts[-1] != total:
        raise ValueError("group lengths do not match the row count")
    build = builder.build
    for i, raw_lap in enumerate(laps):
        lap = int(raw_lap)
        if wanted is not None and lap not in wanted:
            continue
        out[lap] = build(starts[i], starts[i + 1])
