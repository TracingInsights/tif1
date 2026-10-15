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

from .helpers import _DIRECT_BLOCKS_OK, _frame_from_blocks, _numpy_block

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
    dictionary = pa.array(cats, type=pa.large_string())
    strings = pa.DictionaryArray.from_arrays(indices, dictionary).dictionary_decode()
    if _STR_PANDAS_DTYPE is None:
        _STR_PANDAS_DTYPE = pd.StringDtype("pyarrow", na_value=np.nan)
    return pd.arrays.ArrowStringArray(strings, dtype=_STR_PANDAS_DTYPE)


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


def decode_frame(blob: bytes | bytearray | memoryview) -> pd.DataFrame:
    """Rebuild a DataFrame written by :func:`encode_frame`.

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
    # One copy into a writable buffer: every array below is a view into it.
    body = memoryview(bytearray(view[body_start:]))
    blocks: list[Any] = []
    from pandas._libs.internals import BlockPlacement
    from pandas.core.internals.blocks import new_block

    try:
        for spec in specs:
            kind = spec[0]
            if kind == "np":
                _, dtype_str, ncols, placement, offset, _nbytes = spec
                dtype = np.dtype(dtype_str)
                arr = np.frombuffer(body, dtype=dtype, count=ncols * n, offset=offset)
                blocks.append(_numpy_block(arr.reshape(ncols, n), _placement(placement)))
            elif kind == "obj":
                _, placement, col_specs = spec
                values = np.empty((len(col_specs), n), dtype=object)
                for row, col_spec in enumerate(col_specs):
                    if col_spec[0] == "const":
                        values[row, :] = col_spec[1]
                    else:
                        _, cats, offset, _nbytes = col_spec
                        codes = np.frombuffer(body, dtype="int32", count=n, offset=offset)
                        values[row, :] = _decode_object(cats, codes)
                blocks.append(_numpy_block(values, _placement(placement)))
            elif kind == "Int64c":
                _, placement, value = spec
                ea = pd.arrays.IntegerArray(
                    np.full(n, value, dtype="int64"), np.zeros(n, dtype=bool)
                )
                blocks.append(new_block(ea, placement=BlockPlacement(placement[0]), ndim=2))
            elif kind == "Int64":
                _, placement, data_offset, mask_offset = spec
                ea = pd.arrays.IntegerArray(
                    np.frombuffer(body, dtype="int64", count=n, offset=data_offset),
                    np.frombuffer(body, dtype=bool, count=n, offset=mask_offset),
                )
                blocks.append(new_block(ea, placement=BlockPlacement(placement[0]), ndim=2))
            elif kind == "str":
                _, placement, cats, offset, _nbytes = spec
                codes = np.frombuffer(body, dtype="int32", count=n, offset=offset)
                ea = _decode_str(cats, codes)
                blocks.append(new_block(ea, placement=BlockPlacement(placement[0]), ndim=2))
            else:
                raise ValueError(f"unknown block kind {kind!r}")
    except (AttributeError, IndexError, TypeError, ValueError) as e:
        raise ValueError(f"corrupt frame body: {e}") from e
    return _frame_from_blocks(tuple(blocks), list(columns), n)


def _placement(placement: list[int]) -> Any:
    if len(placement) == 1:
        return placement[0]
    return np.asarray(placement, dtype="int64")
