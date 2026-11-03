"""Per-driver columnar telemetry files (``{driver}/tel.v1.{first}.bin``): encoder and decoder.

One file holds ten consecutive laps of one driver in one session (laps
``first`` to ``first + 9``, ``first`` = 1, 11, 21...), as typed arrays in a
single zstd frame, instead of one ``{lap}_tel.json`` per lap (~61 KB gzipped
each). A cold telemetry wave for a race driver is then 6 requests instead of
57, for ~26% fewer bytes. :func:`decode_driver_file` returns, per lap, exactly the ``tel``
mapping :func:`tif1.core_utils.json_utils.decode_telemetry_payload` returns
for that lap's JSON with the native decoder: same keys in the same order,
int64/float64 numpy arrays with bit-identical values, ``DriverAhead`` as a
:class:`~tif1.core_utils.json_utils.DictionaryChannel` with the same
categories, and every other value (``dataKey``, any channel the decoder keeps
as a list) as the same Python object. Every frame builder downstream is
therefore unchanged. The layout and the rounding rules are documented in
``design/TELEMETRY_COLUMNAR_FORMAT.md``.

File layout (little-endian)::

    magic   8 bytes  b"TIF1TEL\\x01" (the last byte is the format version)
    raw     u64      size of the decompressed body
    body    zstd frame of:
        u32 header length | header (orjson) | padding to 8 | buffers (8-aligned)

The header lists the laps (number, sample count, key order, the values kept
as JSON, the ``DriverAhead`` categories) and one entry per (channel, dtype)
column: the encoding, the buffer slices and the per-lap lengths. Encodings:

- ``int``: an int64 channel narrowed to the smallest integer dtype holding it.
- ``dec``: a float64 channel whose every sample is ``n / 10**k`` for an
  integer ``n`` (lap ``time`` in milliseconds): ``n`` stored narrowed,
  optionally as deltas; ``n / 10**k`` (one IEEE division of two exact
  doubles) is the correctly rounded value of the decimal, exactly what the
  JSON parser produced.
- ``split``: a float64 channel where some samples are whole numbers (the car
  and position samples fastf1 merges; the rest are interpolated): a bit mask,
  the whole samples as narrowed integers and the others as raw float64.
- ``f64``: raw float64.

Multi-byte buffers are byte-shuffled (byte ``i`` of every value, then byte
``i + 1``...) before compression. The encoder decodes every column back and
compares the bits; a column that does not round-trip is stored as ``f64``.
"""

from __future__ import annotations

import struct
import sys
from typing import Any

import numpy as np
import orjson

from .constants import COLUMNAR_CHUNK_LAPS, columnar_chunk_first_lap, columnar_file_path
from .json_utils import DictionaryChannel

MAGIC = b"TIF1TEL\x01"
#: Laps per file (see :data:`tif1.core_utils.constants.COLUMNAR_CHUNK_LAPS`;
#: one file per driver rode a single connection and was slower).
CHUNK_LAPS = COLUMNAR_CHUNK_LAPS
chunk_first_lap = columnar_chunk_first_lap
driver_file_path = columnar_file_path
_RAW_SIZE = struct.Struct("<Q")
_HEADER_SIZE = struct.Struct("<I")
_INT_DTYPES = (np.int8, np.int16, np.int32, np.int64)
# |n| below 2**53: every such integer is an exact double, so ``n / 10**k``
# is one correctly rounded division.
_EXACT_INT_LIMIT = 2**53


class ColumnarFormatError(ValueError):
    """A driver file that is not a valid ``tel.v1.bin`` (bad magic, truncated, corrupt)."""


def encode_driver_files(laps: dict[int, dict[str, Any]], *, level: int = 19) -> dict[str, bytes]:
    """Every file of one driver: ``tel.v1.{first}.bin`` file name -> bytes.

    Args:
        laps: Lap number -> ``tel`` mapping (see :func:`encode_driver_file`).
        level: zstd compression level.
    """
    groups: dict[int, dict[int, dict[str, Any]]] = {}
    for lap, tel in laps.items():
        groups.setdefault(columnar_chunk_first_lap(lap), {})[int(lap)] = tel
    return {
        f"tel.v1.{first}.bin": encode_driver_file(group, level=level)
        for first, group in sorted(groups.items())
    }


# -- encoder -----------------------------------------------------------------------


def _narrow(values: np.ndarray) -> np.ndarray:
    """``values`` (integers) in the smallest signed dtype holding them."""
    if values.size == 0:
        return values.astype(np.int8)
    lo, hi = int(values.min()), int(values.max())
    for dtype in _INT_DTYPES:
        info = np.iinfo(dtype)
        if info.min <= lo and hi <= info.max:
            return values.astype(dtype)
    return values.astype(np.int64)  # pragma: no cover - int64 input always fits


def _shuffle(arr: np.ndarray) -> bytes:
    """Bytes of ``arr`` with its byte planes grouped (byte shuffle)."""
    width = arr.dtype.itemsize
    raw = np.ascontiguousarray(arr).view(np.uint8)
    if width == 1 or arr.size == 0:
        return raw.tobytes()
    return raw.reshape(-1, width).T.tobytes()


class _Body:
    """Collects the 8-aligned buffers of the body."""

    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self.size = 0

    def add(self, arr: np.ndarray) -> list[Any]:
        """Append ``arr`` (shuffled); returns its descriptor ``[dtype, offset, count]``."""
        data = _shuffle(arr)
        descriptor = [arr.dtype.str, self.size, int(arr.size)]
        self.chunks.append(data)
        self.size += len(data)
        pad = -self.size % 8
        if pad:
            self.chunks.append(b"\0" * pad)
            self.size += pad
        return descriptor


def _decimal_scale(values: np.ndarray) -> int | None:
    """The smallest ``k`` (0-6) with ``values == round(values * 10**k) / 10**k`` bitwise."""
    if values.size == 0 or not np.isfinite(values).all() or np.signbit(values[values == 0]).any():
        return None
    for k in range(7):
        scale = 10.0**k
        ints = np.round(values * scale)
        if np.abs(ints).max() >= _EXACT_INT_LIMIT:
            return None
        if np.array_equal((ints / scale).view(np.int64), values.view(np.int64)):
            return k
    return None


def _encode_float(values: np.ndarray, body: _Body) -> dict[str, Any]:
    """Column descriptor of a float64 channel (see the module docstring for the encodings)."""
    k = _decimal_scale(values)
    if k is not None:
        ints = np.round(values * 10.0**k).astype(np.int64)
        deltas = np.diff(ints, prepend=np.int64(0))
        plain, delta = _narrow(ints), _narrow(deltas)
        use_delta = delta.dtype.itemsize < plain.dtype.itemsize or bool(np.all(deltas[1:] >= 0))
        return {
            "enc": "dec",
            "k": k,
            "delta": use_delta,
            "buf": body.add(delta if use_delta else plain),
        }
    whole = np.isfinite(values) & (values == np.trunc(values)) & (np.abs(values) < 2**31)
    whole &= ~((values == 0) & np.signbit(values))
    if whole.any():
        return {
            "enc": "split",
            "mask": body.add(np.packbits(whole)),
            "ints": body.add(_narrow(values[whole].astype(np.int64))),
            "rest": body.add(values[~whole]),
        }
    return {"enc": "f64", "buf": body.add(values)}


def encode_driver_file(laps: dict[int, dict[str, Any]], *, level: int = 19) -> bytes:
    """Encode one driver's laps into a ``tel.v1.bin`` file.

    Args:
        laps: Lap number -> the lap's ``tel`` mapping as
            :func:`~tif1.core_utils.json_utils.decode_telemetry_payload`
            returns it (numpy arrays, ``DictionaryChannel``, lists, scalars).
        level: zstd compression level.

    Returns:
        The file bytes. Decoding them gives back ``laps`` exactly (verified per
        column; a column that would not round-trip is stored raw).
    """
    lap_entries: list[dict[str, Any]] = []
    # (channel, dtype) -> list of (lap index, array)
    columns: dict[tuple[str, str], list[np.ndarray]] = {}
    for lap_num in sorted(laps):
        tel = laps[lap_num]
        entry: dict[str, Any] = {"lap": int(lap_num), "keys": list(tel)}
        json_values: dict[str, Any] = {}
        categories: dict[str, list[str]] = {}
        arrays: dict[str, list[Any]] = {}
        for key, value in tel.items():
            if isinstance(value, np.ndarray) and value.ndim == 1 and value.dtype.kind in "if":
                kind = "i8" if value.dtype.kind == "i" else "f8"
                if value.dtype not in (np.dtype(np.int64), np.dtype(np.float64)):
                    json_values[key] = value.tolist()  # pragma: no cover - not a decoder dtype
                    continue
                columns.setdefault((key, kind), []).append(value)
                arrays[key] = [kind, int(value.size)]
            elif isinstance(value, DictionaryChannel):
                codes = np.asarray(value.codes)
                columns.setdefault((key, "dict"), []).append(codes.astype(np.int64))
                arrays[key] = ["dict", int(codes.size)]
                categories[key] = list(value.categories)
            else:
                json_values[key] = value
        entry["arrays"] = arrays
        if json_values:
            entry["json"] = json_values
        if categories:
            entry["cats"] = categories
        lap_entries.append(entry)

    body = _Body()
    column_entries: list[dict[str, Any]] = []
    for (key, kind), parts in columns.items():
        values = np.concatenate(parts) if parts else np.empty(0)
        if kind == "f8":
            descriptor = _encode_float(values, body)
            if not np.array_equal(
                _decode_column(descriptor, _view_of(body), values.size).view(np.int64),
                values.view(np.int64),
            ):  # pragma: no cover - the encodings are exact by construction
                descriptor = {"enc": "f64", "buf": body.add(values)}
        else:
            descriptor = {"enc": "int", "buf": body.add(_narrow(values))}
        descriptor["key"] = key
        descriptor["kind"] = kind
        column_entries.append(descriptor)

    header = orjson.dumps({"version": 1, "laps": lap_entries, "columns": column_entries})
    head = _HEADER_SIZE.pack(len(header)) + header
    head += b"\0" * (-len(head) % 8)
    raw = head + b"".join(body.chunks)
    compressed = _zstd_compress_level(raw, level)
    return MAGIC + _RAW_SIZE.pack(len(raw)) + compressed


def _view_of(body: _Body) -> memoryview:
    """The body written so far (the encoder's round-trip check)."""
    return memoryview(b"".join(body.chunks))


def _zstd_compress_level(raw: bytes, level: int) -> bytes:
    """``raw`` as one zstd frame at ``level`` (stdlib ``compression.zstd`` or ``zstandard``)."""
    if sys.version_info >= (3, 14):  # pragma: no cover - interpreter dependent
        try:
            from compression import zstd

            return zstd.compress(raw, level=level)
        except ImportError:  # interpreter built without libzstd
            pass
    try:
        import zstandard
    except ImportError as e:  # pragma: no cover - zstd is a dependency
        raise ColumnarFormatError("no zstd binding available") from e
    return zstandard.ZstdCompressor(level=level).compress(raw)


# -- decoder -----------------------------------------------------------------------


def _buffer(descriptor: list[Any], body: memoryview) -> np.ndarray:
    """The array a ``[dtype, offset, count]`` descriptor names (unshuffled, writable)."""
    dtype_str, offset, count = descriptor
    dtype = np.dtype(dtype_str)
    width = dtype.itemsize
    nbytes = count * width
    if offset < 0 or offset + nbytes > len(body):
        raise ColumnarFormatError("buffer outside the body")
    planes = np.frombuffer(body, dtype=np.uint8, count=nbytes, offset=offset)
    if width == 1:
        return planes.view(dtype).copy()
    return planes.reshape(width, count).T.copy().view(dtype).reshape(count)


def _decode_column(descriptor: dict[str, Any], body: memoryview, total: int) -> np.ndarray:
    """One (channel, dtype) column of every lap, concatenated (int64 or float64)."""
    enc = descriptor["enc"]
    if enc == "int":
        out = _buffer(descriptor["buf"], body).astype(np.int64)
    elif enc == "f64":
        out = _buffer(descriptor["buf"], body).astype(np.float64, copy=False)
    elif enc == "dec":
        ints = _buffer(descriptor["buf"], body).astype(np.int64)
        if descriptor.get("delta"):
            ints = np.cumsum(ints)
        out = ints / (10.0 ** int(descriptor["k"]))
    elif enc == "split":
        whole = np.unpackbits(_buffer(descriptor["mask"], body), count=total).view(bool)
        out = np.empty(total, dtype=np.float64)
        out[whole] = _buffer(descriptor["ints"], body)
        out[~whole] = _buffer(descriptor["rest"], body)
    else:
        raise ColumnarFormatError(f"unknown column encoding {enc!r}")
    if out.size != total:
        raise ColumnarFormatError("column length does not match its laps")
    return out


def _decompress(blob: bytes | bytearray | memoryview) -> memoryview:
    """The body of a driver file (checks the magic and the size)."""
    data = memoryview(blob)
    head = len(MAGIC) + _RAW_SIZE.size
    if len(data) < head or bytes(data[: len(MAGIC)]) != MAGIC:
        raise ColumnarFormatError("not a tif1 columnar telemetry file (bad magic)")
    (raw_size,) = _RAW_SIZE.unpack_from(data, len(MAGIC))
    from tif1 import cache as _cache  # the zstd binding the cache already selected

    # The per-thread decompressor: files are decoded in several fetch threads
    # at once, and a zstandard decompressor must not be shared (Pergamon Q15).
    decompress = _cache._zstd_decompress_mt
    if decompress is None:  # pragma: no cover - zstd is a dependency
        raise ColumnarFormatError("no zstd binding available")
    try:
        raw = decompress(bytes(data[head:]))
    except Exception as e:
        raise ColumnarFormatError(f"corrupt zstd frame: {e}") from e
    if len(raw) != raw_size:
        raise ColumnarFormatError("decompressed size does not match the header")
    return memoryview(raw)


def decode_driver_file(blob: bytes | bytearray | memoryview) -> dict[int, dict[str, Any]]:
    """Decode a ``tel.v1.bin`` file into lap number -> ``tel`` mapping.

    Each lap's mapping equals what the native telemetry decoder returns for
    that lap's ``_tel.json`` (see the module docstring). Arrays of one
    channel are slices of one per-driver array (disjoint per lap).

    Raises:
        ColumnarFormatError: The bytes are not a valid version-1 file.
    """
    body = _decompress(blob)
    try:
        (header_len,) = _HEADER_SIZE.unpack_from(body, 0)
        header = orjson.loads(bytes(body[_HEADER_SIZE.size : _HEADER_SIZE.size + header_len]))
        start = _HEADER_SIZE.size + header_len
        start += -start % 8
        buffers = body[start:]
        laps_meta: list[dict[str, Any]] = header["laps"]
        if header.get("version") != 1:
            raise ColumnarFormatError(f"unsupported version {header.get('version')!r}")

        # Concatenated columns, then per-lap slices in lap order.
        totals: dict[tuple[str, str], int] = {}
        for entry in laps_meta:
            for key, (kind, size) in entry["arrays"].items():
                totals[(key, kind)] = totals.get((key, kind), 0) + size
        columns: dict[tuple[str, str], np.ndarray] = {}
        for descriptor in header["columns"]:
            ref = (descriptor["key"], descriptor["kind"])
            columns[ref] = _decode_column(descriptor, buffers, totals.get(ref, 0))
        if columns.keys() != totals.keys():
            raise ColumnarFormatError("columns do not match the laps")

        cursors = dict.fromkeys(columns, 0)
        out: dict[int, dict[str, Any]] = {}
        for entry in laps_meta:
            arrays = entry["arrays"]
            json_values = entry.get("json") or {}
            categories = entry.get("cats") or {}
            tel: dict[str, Any] = {}
            for key in entry["keys"]:
                spec = arrays.get(key)
                if spec is None:
                    tel[key] = json_values[key]
                    continue
                kind, size = spec
                ref = (key, kind)
                pos = cursors[ref]
                cursors[ref] = pos + size
                values = columns[ref][pos : pos + size]
                if kind == "dict":
                    tel[key] = DictionaryChannel(values.astype(np.int32), categories[key])
                else:
                    tel[key] = values
            out[int(entry["lap"])] = tel
    except ColumnarFormatError:
        raise
    except (KeyError, TypeError, ValueError, IndexError, struct.error) as e:
        raise ColumnarFormatError(f"malformed driver file: {e}") from e
    return out
