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
from typing import Any

import numpy as np
import pandas as pd

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
            # Extension block: values is the 1-D (or 2-D wrapped) array itself.
            ea = values[0] if getattr(values, "ndim", 1) == 2 else values
            if isinstance(ea, pd.arrays.TimedeltaArray | pd.arrays.DatetimeArray):
                nd = np.asarray(ea)
                raw = np.ascontiguousarray(nd).tobytes()
                specs.append(("np", nd.dtype.str, 1, placement, offset, len(raw)))
                chunks.append(raw)
                offset += len(raw)
                continue
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


def _frame_parts(frame: pd.DataFrame) -> tuple[list[str], int, list[tuple[Any, ...]]] | None:
    """Columns, row count and canonical block parts of a frame, or None if unsupported.

    Parts are ``(kind, placement_tuple, extra..., values...)`` sorted by
    placement so frames with the same blocks in a different order still
    share one layout. ``values`` are the live block arrays (no copies).
    """
    if not _DIRECT_BLOCKS_OK or not isinstance(frame, pd.DataFrame):
        return None
    index = frame.index
    if not isinstance(index, pd.RangeIndex) or index.start != 0 or index.step != 1:
        return None
    n = len(frame)
    columns = [str(c) for c in frame.columns]
    if len(columns) != len(set(columns)):
        return None
    parts: list[tuple[Any, ...]] = []
    try:
        for block in frame._mgr.blocks:
            placement = tuple(int(p) for p in block.mgr_locs)
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
    values: list[np.ndarray], chunks: list[bytes], offset: int
) -> tuple[tuple[Any, ...], int]:
    """Spec for one object column over a group (constant or dictionary-encoded)."""
    first = values[0]
    if len(first) and isinstance(first[0], str):
        head = first[0]
        if all(len(v) and (v == head).all() for v in values):
            return ("const", head), offset
    joined = np.concatenate(values) if len(values) > 1 else values[0]
    codes, cats = _object_dictionary(joined)
    raw = codes.tobytes()
    chunks.append(raw)
    return ("dict", cats, offset, len(raw)), offset + len(raw)


def encode_driver_frames(
    frames: list[tuple[int, pd.DataFrame]],
) -> tuple[bytes | None, list[tuple[int, pd.DataFrame]]]:
    """Encode a driver's ``(lap, frame)`` list; returns ``(blob, unsupported frames)``.

    ``blob`` is None when no frame could be encoded.
    """
    groups: dict[tuple[Any, ...], list[tuple[int, int, list[tuple[Any, ...]]]]] = {}
    group_columns: dict[tuple[Any, ...], list[str]] = {}
    leftovers: list[tuple[int, pd.DataFrame]] = []
    for lap, frame in sorted(frames, key=lambda item: item[0]):
        described = _frame_parts(frame)
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
    chunks: list[bytes] = []
    offset = 0
    try:
        for key, members in groups.items():
            laps = [lap for lap, _, _ in members]
            lengths = [n for _, n, _ in members]
            specs: list[tuple[Any, ...]] = []
            n_parts = len(members[0][2])
            for index in range(n_parts):
                kind = members[0][2][index][0]
                placement = list(members[0][2][index][1])
                if kind == "np":
                    dtype_str = members[0][2][index][2]
                    arrays = [m[2][index][3] for m in members]
                    joined = np.concatenate(arrays, axis=1) if len(arrays) > 1 else arrays[0]
                    raw = np.ascontiguousarray(joined).tobytes()
                    specs.append(("np", dtype_str, joined.shape[0], placement, offset, len(raw)))
                    chunks.append(raw)
                    offset += len(raw)
                elif kind == "obj":
                    col_specs: list[tuple[Any, ...]] = []
                    ncols = members[0][2][index][2].shape[0]
                    for row in range(ncols):
                        col_spec, offset = _object_column_spec(
                            [m[2][index][2][row] for m in members], chunks, offset
                        )
                        col_specs.append(col_spec)
                    specs.append(("obj", placement, col_specs))
                elif kind == "Int64":
                    datas = [m[2][index][2] for m in members]
                    masks = [m[2][index][3] for m in members]
                    if all(
                        len(d) and not mk.any() and (d == d[0]).all() for d, mk in zip(datas, masks)
                    ):
                        specs.append(("Int64c", placement, [int(d[0]) for d in datas]))
                        continue
                    data = np.concatenate(datas) if len(datas) > 1 else datas[0]
                    mask = np.concatenate(masks) if len(masks) > 1 else masks[0]
                    raw_data = np.ascontiguousarray(data, dtype="int64").tobytes()
                    raw_mask = np.ascontiguousarray(mask, dtype=bool).tobytes()
                    specs.append(("Int64", placement, offset, offset + len(raw_data)))
                    chunks.append(raw_data)
                    chunks.append(raw_mask)
                    offset += len(raw_data) + len(raw_mask)
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
                    raw = codes.tobytes()
                    specs.append(("str", placement, cats, offset, len(raw)))
                    chunks.append(raw)
                    offset += len(raw)
                else:  # pragma: no cover - _frame_parts only emits the kinds above
                    raise TypeError(kind)
            header.append((group_columns[key], laps, lengths, specs))
    except (AttributeError, TypeError, ValueError):
        return None, leftovers + [
            (lap, frame) for lap, frame in frames if all(lap != lo for lo, _ in leftovers)
        ]
    header_bytes = pickle.dumps(header, protocol=5)
    blob = b"".join(
        (DRIVER_CODEC_MAGIC, _HEADER_LEN.pack(len(header_bytes)), header_bytes, *chunks)
    )
    return blob, leftovers


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
    blob: bytes | bytearray | memoryview, laps: list[int], frame_class: Any = None
) -> pd.DataFrame | None:
    """One frame holding ``laps`` back to back, straight from the group arrays.

    Pergamon P10: ``Laps.telemetry`` for a driver is the concatenation of the
    driver's lap frames in laps-row order. Every maximal run of laps that
    are stored consecutively in one layout group becomes a slice of that
    group's arrays (no per-lap frames); a single run is returned as is,
    several runs are concatenated (a handful of pieces instead of one per
    lap, with the same dtype promotion ``pd.concat`` of the lap frames
    gives). Returns None when a lap is not stored in the blob.
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
        for g, first, k in runs:
            columns, group_laps, lengths, specs, starts = groups[g]
            total = int(starts[-1])
            parts = parts_by_group.get(g)
            if parts is None:
                parts = parts_by_group[g] = _group_parts(body, group_laps, total, specs)
            start, end = int(starts[first]), int(starts[first + k])
            n = end - start
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
                    data = np.repeat(
                        np.asarray(part[2][first : first + k], dtype="int64"),
                        np.asarray(lengths[first : first + k], dtype="int64"),
                    )
                    ea = _int64_array(data, np.zeros(n, dtype=bool))
                    blocks.append(_extension_block_placed(ea, part[1]))
                else:  # str
                    blocks.append(_extension_block_placed(part[2][start:end], part[1]))
            pieces.append(_frame_from_blocks(tuple(blocks), columns, n, frame_class))
    except (AttributeError, IndexError, TypeError, ValueError) as e:
        raise ValueError(f"corrupt driver blob body: {e}") from e
    if len(pieces) == 1:
        return pieces[0]
    return pd.concat(pieces, ignore_index=True)


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
    header, body = _writable_body(blob)
    out: dict[int, pd.DataFrame] = {}
    try:
        for group_columns, laps, lengths, specs in header:
            columns = list(group_columns)
            total = int(sum(lengths))
            starts = np.zeros(len(lengths) + 1, dtype="int64")
            np.cumsum(lengths, out=starts[1:])
            parts = _group_parts(body, laps, total, specs)
            numpy_block = _numpy_block_placed
            extension_block = _extension_block_placed
            int64_array = _int64_array
            full = np.full
            zeros = np.zeros
            for i, raw_lap in enumerate(laps):
                lap = int(raw_lap)
                if wanted is not None and lap not in wanted:
                    continue
                start, end = int(starts[i]), int(starts[i + 1])
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
                        ea = int64_array(full(n, part[2][i], dtype="int64"), zeros(n, dtype=bool))
                        blocks.append(extension_block(ea, part[1]))
                    else:  # str
                        blocks.append(extension_block(part[2][start:end], part[1]))
                out[lap] = _frame_from_blocks(tuple(blocks), columns, n, frame_class)
    except (AttributeError, IndexError, TypeError, ValueError) as e:
        raise ValueError(f"corrupt driver blob body: {e}") from e
    return out
