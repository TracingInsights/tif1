"""Per-driver columnar telemetry files (``{driver}/tel.v2.{first}.bin``): encoder and decoder.

One file holds ten consecutive laps of one driver in one session (laps
``first`` to ``first + 9``, ``first`` = 1, 11, 21...), as typed arrays in a
single zstd frame, instead of one ``{lap}_tel.json`` per lap (~61 KB gzipped
each). A cold telemetry wave for a race driver is then 6 requests instead of
57, for ~85% fewer bytes. :func:`decode_driver_file` returns, per lap, exactly the ``tel``
mapping :func:`tif1.core_utils.json_utils.decode_telemetry_payload` returns
for that lap's JSON with the native decoder: same keys in the same order,
int64/float64 numpy arrays with bit-identical values, ``DriverAhead`` as a
:class:`~tif1.core_utils.json_utils.DictionaryChannel` with the same
categories, and every other value (``dataKey``, any channel the decoder keeps
as a list) as the same Python object. Every frame builder downstream is
therefore unchanged. The layout and the rounding rules are documented in
``design/TELEMETRY_COLUMNAR_FORMAT.md``.

File layout (little-endian)::

    magic   8 bytes  b"TIF1TEL\\x02" (the last byte is the format version)
    raw     u64      size of the decompressed body
    body    zstd frame of:
        u32 header length | header (orjson) | padding to 8 | buffers (8-aligned)

The header lists the laps (number, sample count, key order, the values kept
as JSON, the ``DriverAhead`` categories) and one entry per (channel, dtype)
column, in decoding order: the encoding, the buffer slices and the per-lap
lengths. Value encodings:

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

Predicted encodings (version 2): a float64 channel derived upstream from other
channels is stored as the difference, in units in the last place, between
each sample and a prediction computed from channels decoded before it, with
IEEE double arithmetic only (no libm, no fused multiply-add), so every
decoder computes the same prediction bit for bit and the stored difference
restores the exact sample, NaN payloads included:

- ``interp``: samples fastf1 interpolated linearly between two car samples
  (``rpm``, ``speed``, ``throttle``, ``DistanceToDriverAhead`` at the
  position rows), predicted with ``numpy.interp``'s formula over the rows'
  float64 nanosecond dates (millisecond ``time`` plus one start date per lap,
  the header's ``dates``, which the encoder finds modulo 512 ns);
- ``lerp``: the same samples without the dates, predicted with the
  interpolation fraction the reference channel (``rpm``) shows at that row;
- ``spline``: ``x``/``y``/``z`` at the car rows, predicted with the quadratic
  interpolating spline (scipy's ``make_interp_spline(k=2)`` knots) through
  the lap's whole samples over the row dates;
- ``integ``: ``distance`` at the car rows, predicted as the previous car row's
  distance plus ``speed / 3.6 * dt`` (fastf1's ``integrate_distance``);
  ``integs`` adds the position rows, predicted with the spline through the
  car rows;
- ``ratio``: ``rel_distance``, predicted as ``distance / D`` with one ``D`` per
  lap (fastf1's ``add_relative_distance``);
- ``accx``: ``acc_x``, predicted with the longitudinal acceleration formula of
  the data pipeline (gradient of ``speed / 3.6`` over the gradient of ``time``,
  three-sample moving average);
- ``lat``: ``acc_y``/``acc_z``, predicted with the pipeline's lateral and
  vertical formulas (heading from an fdlibm ``atan2`` written with ``+ - * /``,
  ``numpy.unwrap``, curvature over the distance gradient, nine-sample moving
  average).

The encoder keeps, per channel, the smallest of its candidate encodings (the
value encoding included). Multi-byte buffers are byte-shuffled (byte ``i`` of every value, then byte
``i + 1``...) before compression. The encoder decodes every column back and
compares the bits; a column that does not round-trip is stored as ``f64``.
"""

from __future__ import annotations

import math
import struct
import sys
from typing import Any

import numpy as np
import orjson

from . import json_utils as _json_utils
from .constants import (
    COLUMNAR_CHUNK_LAPS,
    COLUMNAR_FORMAT_VERSION,
    columnar_chunk_first_lap,
    columnar_file_name,
    columnar_file_path,
)
from .json_utils import DictionaryChannel

FORMAT_VERSION = COLUMNAR_FORMAT_VERSION
MAGIC = b"TIF1TEL" + bytes([FORMAT_VERSION])
#: Laps per file (see :data:`tif1.core_utils.constants.COLUMNAR_CHUNK_LAPS`;
#: one file per driver rode a single connection and was slower).
CHUNK_LAPS = COLUMNAR_CHUNK_LAPS
chunk_first_lap = columnar_chunk_first_lap
driver_file_path = columnar_file_path
_RAW_SIZE = struct.Struct("<Q")
_HEADER_SIZE = struct.Struct("<I")
_INT_DTYPES = (np.int8, np.int16, np.int32, np.int64)
_UINT_DTYPES = (np.uint8, np.uint16, np.uint32, np.uint64)
# |n| below 2**53: every such integer is an exact double, so ``n / 10**k``
# is one correctly rounded division.
_EXACT_INT_LIMIT = 2**53
_MAGNITUDE_BITS = np.int64(0x7FFF_FFFF_FFFF_FFFF)

# Predicted channels, in decoding order: channel -> candidate (encoding,
# references); the encoder keeps the smallest candidate, its value encoding
# included. A channel missing from a file, or one that does not round-trip,
# keeps its value encoding.
_INTERP = ("interp", {"time": "time"})
_SPLINE = ("spline", {"time": "time"})
_PREDICTED: dict[str, list[tuple[str, dict[str, str]]]] = {
    "rpm": [_INTERP],
    "speed": [_INTERP, ("lerp", {"ref": "rpm"})],
    "throttle": [_INTERP, ("lerp", {"ref": "rpm"})],
    "DistanceToDriverAhead": [_INTERP, ("lerp", {"ref": "rpm"})],
    "x": [_SPLINE],
    "y": [_SPLINE],
    "z": [_SPLINE],
    "distance": [
        ("integs", {"speed": "speed", "time": "time"}),
        ("integ", {"speed": "speed", "time": "time"}),
    ],
    "rel_distance": [("ratio", {"of": "distance"})],
    "acc_x": [("accx", {"speed": "speed", "time": "time"})],
    "acc_y": [("lat", {"speed": "speed", "x": "x", "other": "y", "distance": "distance"})],
    "acc_z": [("lat", {"speed": "speed", "x": "x", "other": "z", "distance": "distance"})],
}
# The channels whose whole samples mark the car rows (the ``anchors``).
_ANCHOR_CHANNELS = ("rpm", "speed", "throttle")
# The lap start dates are searched as ``_DATE_BASE + k``, ``0 <= k < 512``:
# float64 nanosecond dates of 2006-2043 are multiples of 256 and
# ``ms * 1e6`` advances by 64 mod 512, so ``T0 mod 512`` fixes every rounding.
_DATE_BASE = 2**60 + 2**59
_DATE_CANDIDATES = 512
_MS_LIMIT = 2**40


class ColumnarFormatError(ValueError):
    """A driver file that is not a valid ``tel.v2.bin`` (bad magic, truncated, corrupt)."""


def encode_driver_files(laps: dict[int, dict[str, Any]], *, level: int = 19) -> dict[str, bytes]:
    """Every file of one driver: ``tel.v2.{first}.bin`` file name -> bytes.

    Args:
        laps: Lap number -> ``tel`` mapping (see :func:`encode_driver_file`).
        level: zstd compression level.
    """
    groups: dict[int, dict[int, dict[str, Any]]] = {}
    for lap, tel in laps.items():
        groups.setdefault(columnar_chunk_first_lap(lap), {})[int(lap)] = tel
    return {
        columnar_file_name(first): encode_driver_file(group, level=level)
        for first, group in sorted(groups.items())
    }


# -- shared: units in the last place -----------------------------------------------


def _ordered(values: np.ndarray) -> np.ndarray:
    """float64 -> int64 keys in the order of the floats (an involution on the bits)."""
    bits = np.ascontiguousarray(values, dtype=np.float64).view(np.int64)
    return bits ^ ((bits >> 63) & _MAGNITUDE_BITS)


def _from_ordered(keys: np.ndarray) -> np.ndarray:
    """Inverse of :func:`_ordered`."""
    return (keys ^ ((keys >> 63) & _MAGNITUDE_BITS)).view(np.float64)


def _residuals(actual: np.ndarray, predicted: np.ndarray) -> np.ndarray:
    """Zigzag-coded distance in ULPs from ``predicted`` to ``actual`` (uint64, wrapping)."""
    diff = _ordered(actual) - _ordered(predicted)
    return ((diff << 1) ^ (diff >> 63)).view(np.uint64)


def _apply_residuals(predicted: np.ndarray, zigzag: np.ndarray) -> np.ndarray:
    """``actual`` from ``predicted`` and the zigzag ULP distances (inverse of :func:`_residuals`)."""
    native = _json_utils.NATIVE_TEL_APPLY
    if native is not None:
        out = np.array(predicted, dtype=np.float64)  # a copy: the kernel works in place
        native(out, np.ascontiguousarray(zigzag, dtype=np.uint64))
        return out
    zz = zigzag.astype(np.uint64)
    diff = (zz >> np.uint64(1)).view(np.int64) ^ -(zz & np.uint64(1)).view(np.int64)
    return _from_ordered(_ordered(predicted) + diff)


def _canonical(predicted: np.ndarray) -> np.ndarray:
    """A NaN prediction becomes 0.0 (NaN payloads of arithmetic differ across CPUs)."""
    return np.where(np.isnan(predicted), 0.0, predicted)


# -- shared: predictions (encoder and decoder run the same code) ------------------


class _Rows:
    """Row layout of a file: the laps' samples concatenated in lap order."""

    def __init__(self, sizes: list[int]) -> None:
        self.sizes = np.asarray(sizes, dtype=np.int64)
        self.total = int(self.sizes.sum())
        self.ends = np.cumsum(self.sizes)
        self.starts = self.ends - self.sizes
        self.lap_start = np.repeat(self.starts, self.sizes)
        self.lap_end = np.repeat(self.ends, self.sizes)


class _Anchors:
    """Car rows (whole ``rpm``/``speed``/``throttle``) and each row's neighbours in its lap."""

    def __init__(self, mask: np.ndarray, rows: _Rows) -> None:
        total = rows.total
        index = np.arange(total, dtype=np.int64)
        self.mask = mask
        prev = np.maximum.accumulate(np.where(mask, index, -1)) if total else index
        nxt = np.minimum.accumulate(np.where(mask, index, total)[::-1])[::-1] if total else index
        self.prev = prev  # nearest anchor at or before the row
        self.next = nxt  # nearest anchor at or after the row
        self.has_prev = prev >= rows.lap_start
        self.has_next = nxt < rows.lap_end
        # previous anchor strictly before the row, in the same lap
        before = np.concatenate([np.array([-1], dtype=np.int64), prev[:-1]]) if total else index
        self.before = before
        self.has_before = before >= rows.lap_start
        self.count = int(np.count_nonzero(mask))
        self._between: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None = None

    def between(self) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """``interp`` rows, their anchors, and the rows neither anchor nor predicted (cached).

        Read-only: every ``interp`` channel of the file shares them.
        """
        if self._between is None:
            rows = np.flatnonzero(~self.mask & self.has_prev & self.has_next)
            rest = ~self.mask
            rest[rows] = False
            self._between = (rows, self.prev[rows], self.next[rows], rest)
        return self._between


def _lerp_rows(ref: np.ndarray, anchors: _Anchors) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Rows predicted by ``lerp`` and their anchors: (rows, j0, j1)."""
    j0 = np.where(anchors.has_prev, anchors.prev, 0)
    j1 = np.where(anchors.has_next, anchors.next, 0)
    predicted = ~anchors.mask & anchors.has_prev & anchors.has_next
    predicted &= ref[j0] != ref[j1]
    rows = np.flatnonzero(predicted)
    return rows, j0[rows], j1[rows]


def _lerp_predict(
    ref: np.ndarray, values: np.ndarray, rows: np.ndarray, j0: np.ndarray, j1: np.ndarray
) -> np.ndarray:
    """``y0 + (y1 - y0) * f`` with ``f`` the reference's interpolation fraction."""
    r0 = ref[j0]
    fraction = (ref[rows] - r0) / (ref[j1] - r0)
    y0 = values[j0]
    return _canonical(y0 + (values[j1] - y0) * fraction)


def _interp_rows(anchors: _Anchors) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Rows predicted by ``interp`` (between two car rows of their lap) and those rows."""
    rows, j0, j1, _rest = anchors.between()
    return rows, j0, j1


def _row_dates(time: np.ndarray, starts: np.ndarray, rows: _Rows) -> np.ndarray:
    """Each row's date as fastf1 interpolates over it: float64 nanoseconds.

    ``float(T0 + round(time * 1000) * 10**6)`` with ``T0`` the lap's start
    date (``starts``, int64 ns, wrapping arithmetic); NaN where ``time`` is
    not a usable millisecond count.
    """
    with np.errstate(invalid="ignore"):
        ms = np.rint(time * 1000.0)
        usable = np.abs(ms) < _MS_LIMIT
    ns = np.repeat(starts.astype(np.int64), rows.sizes)
    ns = ns + np.where(usable, ms, 0.0).astype(np.int64) * np.int64(1_000_000)
    return np.where(usable, ns.astype(np.float64), np.nan)


def _interp_predict(
    dates: np.ndarray, values: np.ndarray, rows: np.ndarray, j0: np.ndarray, j1: np.ndarray
) -> np.ndarray:
    """``numpy.interp`` of ``values`` at ``dates[rows]`` between the car rows ``j0`` and ``j1``.

    numpy's formula spelled out (``slope * (x - x0) + y0``, retried from the
    right end when NaN, ``y0`` at ``x == x0``), two roundings each, never fused.
    """
    x, x0, x1 = dates[rows], dates[j0], dates[j1]
    y0, y1 = values[j0], values[j1]
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        slope = (y1 - y0) / (x1 - x0)
        out = slope * (x - x0) + y0
        retry = np.isnan(out)
        if retry.any():
            out = np.where(retry, slope * (x - x1) + y1, out)
            out = np.where(np.isnan(out) & (y0 == y1), y0, out)
    out = np.where(x0 == x, y0, out)
    return _canonical(out)


def _de_boor2(knots: np.ndarray, x: np.ndarray, interval: np.ndarray) -> list[np.ndarray]:
    """The three nonzero quadratic B-splines at ``x`` (scipy's ``_deBoor_D``, vectorized)."""
    h = [np.ones_like(x), np.zeros_like(x), np.zeros_like(x)]
    with np.errstate(divide="ignore", invalid="ignore"):
        for j in (1, 2):
            hh = h[:j]
            h = [np.zeros_like(x), *h[1:]]
            for n in range(1, j + 1):
                xb = knots[interval + n]
                xa = knots[interval + n - j]
                same = xb == xa
                w = hh[n - 1] / (xb - xa)
                h[n - 1] = np.where(same, h[n - 1], h[n - 1] + w * (xb - x))
                h[n] = np.where(same, 0.0, w * (x - xa))
    return h


def _spline_lap(xd: np.ndarray, yd: np.ndarray, xe: np.ndarray) -> np.ndarray:
    """Quadratic interpolating spline through ``(xd, yd)`` evaluated at ``xe``.

    scipy's ``make_interp_spline(k=2)`` knots (``xd[0]`` and ``xd[-1]`` three
    times, the midpoints of ``xd`` but the first and last), the collocation
    rows from :func:`_de_boor2`, a tridiagonal solve without pivoting (one
    Python float operation at a time, the order ``tif1_native.tel_spline``
    uses) and the evaluation ``((0 + c0*h0) + c1*h1) + c2*h2``. Fewer than
    three points, a NaN or a non-increasing ``xd`` predict 0.
    """
    n = xd.size
    if n < 3 or not np.all(xd[1:] > xd[:-1]) or np.isnan(xd).any():
        return np.zeros(xe.size)
    mid = (xd[2:-1] + xd[1:-2]) / 2.0
    knots = np.concatenate([np.repeat(xd[:1], 3), mid, np.repeat(xd[-1:], 3)])
    interval = np.clip(np.searchsorted(knots, xd, side="right") - 1, 2, n - 1)
    h = _de_boor2(knots, xd, interval)
    offset = interval - 2 - np.arange(n)  # column of h[0] relative to the diagonal
    sub, diag, sup = np.zeros(n), np.zeros(n), np.zeros(n)
    for c in range(3):
        position = offset + c
        for target, where in ((sub, -1), (diag, 0), (sup, 1)):
            hit = position == where
            target[hit] = h[c][hit]
        if np.any((np.abs(position) > 1) & (h[c] != 0)):
            return np.zeros(xe.size)
    d = diag.tolist()
    s = sub.tolist()
    u = sup.tolist()
    r = yd.tolist()
    for i in range(1, n):
        factor = s[i] / d[i - 1]
        d[i] = d[i] - factor * u[i - 1]
        r[i] = r[i] - factor * r[i - 1]
    coef = [0.0] * n
    coef[n - 1] = r[n - 1] / d[n - 1]
    for i in range(n - 2, -1, -1):
        coef[i] = (r[i] - u[i] * coef[i + 1]) / d[i]
    c_arr = np.asarray(coef)
    finite_x = ~np.isnan(xe)
    xs = np.where(finite_x, xe, xd[0])
    at = np.clip(np.searchsorted(knots, xs, side="right") - 1, 2, n - 1)
    he = _de_boor2(knots, xs, at)
    with np.errstate(invalid="ignore", over="ignore"):
        out = ((0.0 + c_arr[at - 2] * he[0]) + c_arr[at - 1] * he[1]) + c_arr[at] * he[2]
    return np.where(finite_x, out, np.nan)


def _spline_predict(
    dates: np.ndarray, values: np.ndarray, points: np.ndarray, targets: np.ndarray, rows: _Rows
) -> np.ndarray:
    """Per lap, the spline through the ``points`` rows evaluated at the ``targets`` rows."""
    native = _json_utils.NATIVE_TEL_SPLINE
    if native is not None:
        return native(
            _f64(dates),
            _f64(values),
            np.ascontiguousarray(points, dtype=bool),
            np.ascontiguousarray(targets, dtype=np.int64),
            _lap_sizes(rows),
        )
    out = np.zeros(targets.size, dtype=np.float64)
    lap_of_target = np.searchsorted(rows.ends, targets, side="right")
    for lap, (start, stop) in enumerate(zip(rows.starts.tolist(), rows.ends.tolist(), strict=True)):
        mine = np.flatnonzero(lap_of_target == lap)
        if mine.size == 0:
            continue
        data = start + np.flatnonzero(points[start:stop])
        out[mine] = _spline_lap(dates[data], values[data], dates[targets[mine]])
    return _canonical(out)


def _find_lap_starts(
    time: np.ndarray, rpm: np.ndarray, anchors: _Anchors, rows: _Rows
) -> np.ndarray:
    """Encoder: per lap, the start date (mod 512 ns) under which ``interp`` reproduces most of ``rpm``."""
    starts = np.full(rows.sizes.size, _DATE_BASE, dtype=np.int64)
    predicted, j0, j1 = _interp_rows(anchors)
    lap_of = np.repeat(np.arange(rows.sizes.size), rows.sizes)
    with np.errstate(invalid="ignore"):
        ms = np.rint(time * 1000.0)
        usable = np.abs(ms) < _MS_LIMIT
    ns = np.where(usable, ms, 0.0).astype(np.int64) * np.int64(1_000_000)
    offsets = np.arange(_DATE_CANDIDATES, dtype=np.int64)[:, None]

    def dates_at(index: np.ndarray) -> np.ndarray:
        return (_DATE_BASE + offsets + ns[index][None, :]).astype(np.float64)

    for lap in range(rows.sizes.size):
        mine = lap_of[predicted] == lap
        if not mine.any():
            continue
        at, left, right = predicted[mine], j0[mine], j1[mine]
        if not (usable[at].all() and usable[left].all() and usable[right].all()):
            continue
        x, x0, x1 = dates_at(at), dates_at(left), dates_at(right)
        y0, y1 = rpm[left], rpm[right]
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            guess = (y1 - y0) / (x1 - x0) * (x - x0) + y0
        hits = (guess.view(np.int64) == rpm[at].view(np.int64)).sum(axis=1)
        starts[lap] = _DATE_BASE + int(np.argmax(hits))
    return starts


def _integ_increments(
    speed: np.ndarray, time: np.ndarray, anchors: _Anchors
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Rows predicted by ``integ``, their previous anchors and ``speed / 3.6 * dt``."""
    rows = np.flatnonzero(anchors.mask & anchors.has_before)
    prev = anchors.before[rows]
    return rows, prev, speed[rows] / 3.6 * (time[rows] - time[prev])


def _integ_decode(
    values: np.ndarray,
    rows: np.ndarray,
    prev: np.ndarray,
    increments: np.ndarray,
    zigzag: np.ndarray,
) -> None:
    """Fill ``values[rows]`` in row order: ``values[prev] + increment``, then the residual.

    Each row depends on the decoded previous anchor. A run of zero residuals is
    one sequential sum (``np.add.accumulate`` adds left to right), so only the
    rows with a residual and the chain starts are handled one by one.
    """
    if rows.size == 0:
        return
    native = _json_utils.NATIVE_TEL_INTEG
    if native is not None:
        native(
            values,
            np.ascontiguousarray(rows, dtype=np.int64),
            np.ascontiguousarray(prev, dtype=np.int64),
            np.ascontiguousarray(increments, dtype=np.float64),
            np.ascontiguousarray(zigzag, dtype=np.uint64),
        )
        return
    nonzero = zigzag != 0
    # a row starts a run when its previous anchor is not the row before it in the
    # chain (the first predicted row of a lap) or when it carries a residual
    chain_break = np.ones(rows.size, dtype=bool)
    chain_break[1:] = prev[1:] != rows[:-1]
    starts = np.flatnonzero(chain_break | nonzero)
    ends = np.append(starts[1:], rows.size)
    for start, end in zip(starts.tolist(), ends.tolist(), strict=True):
        values[rows[start]] = _integ_step(values, rows, prev, increments, zigzag, start)
        if end - start > 1:
            run = np.empty(end - start, dtype=np.float64)
            run[0] = values[rows[start]]
            run[1:] = increments[start + 1 : end]
            sums = np.add.accumulate(run)
            if np.isnan(sums).any():  # a NaN sum predicts 0.0: one row at a time
                for k in range(start + 1, end):
                    values[rows[k]] = _integ_step(values, rows, prev, increments, zigzag, k)
            else:
                values[rows[start + 1 : end]] = sums[1:]


_F64 = struct.Struct("<d")
_I64 = struct.Struct("<q")


def _integ_step(
    values: np.ndarray,
    rows: np.ndarray,
    prev: np.ndarray,
    increments: np.ndarray,
    zigzag: np.ndarray,
    k: int,
) -> float:
    """Decoded value of the ``k``-th ``integ`` row (Python floats are IEEE doubles)."""
    predicted = float(values[prev[k]]) + float(increments[k])
    if math.isnan(predicted):  # NaN predicts 0.0
        predicted = 0.0
    zz = int(zigzag[k])
    if not zz:
        return predicted
    diff = (zz >> 1) ^ -(zz & 1)
    (bits,) = _I64.unpack(_F64.pack(predicted))
    key = bits ^ 0x7FFF_FFFF_FFFF_FFFF if bits < 0 else bits
    key = (key + diff + 2**63) % 2**64 - 2**63  # int64 wrapping
    bits = key ^ 0x7FFF_FFFF_FFFF_FFFF if key < 0 else key
    return _F64.unpack(_I64.pack(bits))[0]


def _integ_predict_sequential(
    values: np.ndarray, rows: np.ndarray, prev: np.ndarray, increments: np.ndarray
) -> np.ndarray:
    """Encoder side of ``integ``: the prediction of each row from the true previous value."""
    return _canonical(values[prev] + increments)


def _ratio_predict(numerator: np.ndarray, divisors: np.ndarray, rows: _Rows) -> np.ndarray:
    """``numerator / D`` with one divisor per lap."""
    return _canonical(numerator / np.repeat(divisors, rows.sizes))


def _accx_predict(speed: np.ndarray, time: np.ndarray, rows: _Rows) -> np.ndarray:
    """The pipeline's longitudinal acceleration of each lap from ``speed`` and ``time``.

    Per lap: ``a = gradient(speed / 3.6) / gradient(time)`` (central
    differences, one-sided at the ends), interior samples above 25 replaced by
    the previous sample as it was, then a three-sample moving average (two
    samples at the ends). A lap of one sample predicts 0.
    """
    out = np.zeros(rows.total, dtype=np.float64)
    third = 1.0 / 3.0
    for start, size in zip(rows.starts.tolist(), rows.sizes.tolist(), strict=True):
        if size < 2:
            continue
        v = speed[start : start + size] / 3.6
        t = time[start : start + size]
        gv = np.empty(size, dtype=np.float64)
        gt = np.empty(size, dtype=np.float64)
        gv[1:-1] = (v[2:] - v[:-2]) / 2.0
        gt[1:-1] = (t[2:] - t[:-2]) / 2.0
        gv[0] = v[1] - v[0]
        gt[0] = t[1] - t[0]
        gv[-1] = v[-1] - v[-2]
        gt[-1] = t[-1] - t[-2]
        a = gv / gt
        bad = np.zeros(size, dtype=bool)
        bad[1:-1] = a[1:-1] > 25
        if bad.any():
            held = a.copy()
            held[1:][bad[1:]] = a[:-1][bad[1:]]
            a = held
        w = a * third
        lap = out[start : start + size]
        if size == 2:
            lap[:] = w[0] + w[1]
            continue
        lap[1:-1] = (w[:-2] + w[1:-1]) + w[2:]
        lap[0] = w[0] + w[1]
        lap[-1] = w[-2] + w[-1]
    return _canonical(out)


# fdlibm's atan (s_atan.c): the reduction constants and the polynomial. Only
# + - * / on doubles, so numpy and the native decoder compute the same bits
# on every CPU (libm's atan2 differs between platforms and numpy's SIMD loops).
_ATAN_HI = (
    4.63647609000806093515e-01,
    7.85398163397448278999e-01,
    9.82793723247329054082e-01,
    1.57079632679489655800e00,
)
_ATAN_LO = (
    2.26987774529616870924e-17,
    3.06161699786838301793e-17,
    1.39033110312309984516e-17,
    6.12323399573676603587e-17,
)
_ATAN_T = (
    3.33333333333329318027e-01,
    -1.99999999998764832476e-01,
    1.42857142725034663711e-01,
    -1.11111104054623557880e-01,
    9.09088713343650656196e-02,
    -7.69187620504482999495e-02,
    6.66107313738753120669e-02,
    -5.83357013379057348645e-02,
    4.97687799461593236017e-02,
    -3.65315727442169155270e-02,
    1.62858201153657823623e-02,
)
_PI = 3.14159265358979311600e00
_PI_LO = 1.22464679914735317720e-16
_TWO_PI = 2.0 * _PI
_EPS = float(np.finfo(np.float64).eps)


def _atan_abs(t: np.ndarray) -> np.ndarray:
    """``atan(t)`` for ``t >= 0`` (fdlibm's reduction and polynomial; NaN stays NaN)."""
    a = _ATAN_T
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        x = np.where(
            t < 0.6875,
            (2.0 * t - 1.0) / (2.0 + t),
            np.where(
                t < 1.1875,
                (t - 1.0) / (t + 1.0),
                np.where(t < 2.4375, (t - 1.5) / (1.0 + 1.5 * t), -1.0 / t),
            ),
        )
        x = np.where(t < 0.4375, t, x)
        z = x * x
        w = z * z
        s1 = z * (a[0] + w * (a[2] + w * (a[4] + w * (a[6] + w * (a[8] + w * a[10])))))
        s2 = w * (a[1] + w * (a[3] + w * (a[5] + w * (a[7] + w * a[9]))))
        poly = x * (s1 + s2)
        band = np.where(t < 0.6875, 0, np.where(t < 1.1875, 1, np.where(t < 2.4375, 2, 3)))
        hi = np.asarray(_ATAN_HI)[band]
        lo = np.asarray(_ATAN_LO)[band]
        reduced = hi - ((poly - lo) - x)
        return np.where(t < 0.4375, x - poly, reduced)


def _atan2(y: np.ndarray, x: np.ndarray) -> np.ndarray:
    """``atan2(y, x)`` from :func:`_atan_abs` and fdlibm's quadrant rules."""
    with np.errstate(divide="ignore", invalid="ignore"):
        z = _atan_abs(np.abs(y / x))
    neg_y = np.signbit(y)
    out = np.where(neg_y, -z, z)
    left = np.signbit(x)
    out = np.where(left & ~neg_y, _PI - (z - _PI_LO), out)
    return np.where(left & neg_y, (z - _PI_LO) - _PI, out)


def _unwrap(theta: np.ndarray) -> np.ndarray:
    """``numpy.unwrap(theta)`` (period 2*pi) with the remainder spelled out (``fmod`` is exact)."""
    dd = theta[1:] - theta[:-1]
    shifted = dd + _PI
    mod = np.fmod(shifted, _TWO_PI)
    mod = np.where(mod < 0, mod + _TWO_PI, mod)
    mod = np.where(mod == 0, 0.0, mod)
    ddmod = mod - _PI
    ddmod = np.where((ddmod == -_PI) & (dd > 0), _PI, ddmod)
    correction = np.where(np.abs(dd) < _PI, 0.0, ddmod - dd)
    out = theta.copy()
    out[1:] = theta[1:] + np.cumsum(correction)
    return out


def _gradient(f: np.ndarray) -> np.ndarray:
    """``numpy.gradient(f)`` with unit spacing (``f`` has at least two samples)."""
    g = np.empty(f.size, dtype=np.float64)
    g[1:-1] = (f[2:] - f[:-2]) / 2.0
    g[0] = f[1] - f[0]
    g[-1] = f[-1] - f[-2]
    return g


def _hold_above(values: np.ndarray, limit: float) -> np.ndarray:
    """Interior samples with ``|v| > limit`` replaced by the previous sample as it was.

    The pipeline's data matches the previous input sample, not the previous
    replaced one (two jumps in a row keep the first jump).
    """
    bad = np.zeros(values.size, dtype=bool)
    bad[1:-1] = np.abs(values[1:-1]) > limit
    if not bad.any():
        return values
    out = values.copy()
    out[1:][bad[1:]] = values[:-1][bad[1:]]
    return out


def _moving_sum9(values: np.ndarray) -> np.ndarray:
    """Nine-sample moving average (``values * (1/9)`` summed left to right, clipped at the ends)."""
    w = values * (1.0 / 9.0)
    size = w.size
    out = np.zeros(size, dtype=np.float64)
    for offset in range(-4, 5):
        lo = max(0, -offset)
        hi = min(size, size - offset)
        if lo >= hi:
            continue
        out[lo:hi] = out[lo:hi] + w[lo + offset : hi + offset]
    return out


def _lateral_predict(
    speed: np.ndarray,
    x: np.ndarray,
    other: np.ndarray,
    distance: np.ndarray,
    rows: _Rows,
) -> np.ndarray:
    """The pipeline's lateral (``other`` = y) or vertical (``other`` = z) acceleration.

    Per lap: the heading ``atan2(d other, dx + eps)`` (first sample = second),
    unwrapped, its gradient with jumps above 0.5 held, divided by the
    distance gradient plus 1e-4, times ``(speed / 3.6)**2``; samples above
    150 in magnitude become 0, then a nine-sample moving average. A lap of one
    sample predicts 0.
    """
    out = np.zeros(rows.total, dtype=np.float64)
    for start, size in zip(rows.starts.tolist(), rows.sizes.tolist(), strict=True):
        if size < 2:
            continue
        stop = start + size
        v = speed[start:stop] / 3.6
        theta = _atan2(_gradient(other[start:stop]), _gradient(x[start:stop]) + _EPS)
        theta[0] = theta[1]
        dtheta = _hold_above(_gradient(_unwrap(theta)), 0.5)
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            curvature = dtheta / (_gradient(distance[start:stop]) + 0.0001)
            acc = (v * v) * curvature
        acc = np.where(np.abs(acc) > 150, 0.0, acc)
        out[start:stop] = _moving_sum9(acc)
    return _canonical(out)


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


def _narrow_unsigned(values: np.ndarray) -> np.ndarray:
    """``values`` (uint64) in the smallest unsigned dtype holding them."""
    if values.size == 0:
        return values.astype(np.uint8)
    hi = int(values.max())
    for dtype in _UINT_DTYPES:
        if hi <= np.iinfo(dtype).max:
            return values.astype(dtype)
    return values  # pragma: no cover - uint64 input always fits


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

    def view(self) -> memoryview:
        """The body written so far (the encoder's round-trip checks)."""
        return memoryview(b"".join(self.chunks))


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


def _whole(values: np.ndarray) -> np.ndarray:
    """Samples ``split`` stores as integers: finite, whole, ``|v| < 2**31``, not ``-0.0``."""
    whole = np.isfinite(values) & (values == np.trunc(values)) & (np.abs(values) < 2**31)
    whole &= ~((values == 0) & np.signbit(values))
    return whole


def _encode_float(values: np.ndarray, body: _Body) -> dict[str, Any]:
    """Value-encoding descriptor of a float64 array (``dec``, ``split`` or ``f64``)."""
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
    whole = _whole(values)
    if whole.any():
        return {
            "enc": "split",
            "mask": body.add(np.packbits(whole)),
            "ints": body.add(_narrow(values[whole].astype(np.int64))),
            "rest": body.add(values[~whole]),
        }
    return {"enc": "f64", "buf": body.add(values)}


def _encode_between_anchors(
    values: np.ndarray,
    anchors: _Anchors,
    predicted_rows: np.ndarray,
    predicted: np.ndarray,
    descriptor: dict[str, Any],
    body: _Body,
) -> None:
    """``lerp``/``interp`` parts: the anchor rows, the other unpredicted rows, the residuals."""
    rest = np.ones(values.size, dtype=bool)
    rest[anchors.mask] = False
    rest[predicted_rows] = False
    descriptor["anchor"] = _encode_float(values[anchors.mask], body)
    descriptor["rest"] = _encode_float(values[rest], body)
    descriptor["res"] = body.add(_narrow_unsigned(_residuals(values[predicted_rows], predicted)))


def _encode_predicted(
    enc: str,
    refs: dict[str, str],
    values: np.ndarray,
    context: _DecodeContext,
    body: _Body,
) -> dict[str, Any] | None:
    """Descriptor of a predicted column, or None when its references are missing."""
    decoded, rows, anchors = context.decoded, context.rows, context.anchors
    if rows is None or any(name not in decoded for name in refs.values()):
        return None
    descriptor: dict[str, Any] = {"enc": enc, **refs}
    if enc == "interp":
        if anchors is None or context.starts is None:
            return None
        dates = context.dates(decoded[refs["time"]])
        interp_rows, j0, j1 = _interp_rows(anchors)
        predicted = _interp_predict(dates, values, interp_rows, j0, j1)
        _encode_between_anchors(values, anchors, interp_rows, predicted, descriptor, body)
    elif enc == "lerp":
        if anchors is None:
            return None
        ref = decoded[refs["ref"]]
        lerp_rows, j0, j1 = _lerp_rows(ref, anchors)
        predicted = _lerp_predict(ref, values, lerp_rows, j0, j1)
        _encode_between_anchors(values, anchors, lerp_rows, predicted, descriptor, body)
    elif enc == "spline":
        if context.starts is None:
            return None
        dates = context.dates(decoded[refs["time"]])
        whole = _whole(values)
        targets = np.flatnonzero(~whole)
        predicted = _spline_predict(dates, values, whole, targets, rows)
        descriptor["mask"] = body.add(np.packbits(whole))
        descriptor["ints"] = body.add(_narrow(values[whole].astype(np.int64)))
        descriptor["res"] = body.add(_narrow_unsigned(_residuals(values[targets], predicted)))
    elif enc in ("integ", "integs"):
        if anchors is None or (enc == "integs" and context.starts is None):
            return None
        integ_rows, prev, increments = _integ_increments(
            decoded[refs["speed"]], decoded[refs["time"]], anchors
        )
        predicted = _integ_predict_sequential(values, integ_rows, prev, increments)
        rest = np.ones(rows.total, dtype=bool)
        rest[integ_rows] = False
        if enc == "integs":  # the position rows: fastf1's quadratic spline over the car rows
            rest[~anchors.mask] = False
            targets = np.flatnonzero(~anchors.mask)
            dates = context.dates(decoded[refs["time"]])
            between = _spline_predict(dates, values, anchors.mask, targets, rows)
            descriptor["spl"] = body.add(_narrow_unsigned(_residuals(values[targets], between)))
        descriptor["rest"] = _encode_float(values[rest], body)
        descriptor["res"] = body.add(_narrow_unsigned(_residuals(values[integ_rows], predicted)))
    elif enc == "ratio":
        numerator = decoded[refs["of"]]
        divisors = np.array(
            [
                _best_divisor(numerator[start : start + size], values[start : start + size])
                for start, size in zip(rows.starts.tolist(), rows.sizes.tolist(), strict=True)
            ],
            dtype=np.float64,
        )
        predicted = _ratio_predict(numerator, divisors, rows)
        descriptor["div"] = body.add(divisors)
        descriptor["res"] = body.add(_narrow_unsigned(_residuals(values, predicted)))
    elif enc == "accx":
        predicted = _accx_predict(decoded[refs["speed"]], decoded[refs["time"]], rows)
        descriptor["res"] = body.add(_narrow_unsigned(_residuals(values, predicted)))
    elif enc == "lat":
        predicted = _lateral_predict(
            decoded[refs["speed"]],
            decoded[refs["x"]],
            decoded[refs["other"]],
            decoded[refs["distance"]],
            rows,
        )
        descriptor["res"] = body.add(_narrow_unsigned(_residuals(values, predicted)))
    else:  # pragma: no cover - _PREDICTED names known encodings only
        return None
    return descriptor


def _encode_smallest(
    key: str, values: np.ndarray, context: _DecodeContext, body: _Body, predict: bool
) -> dict[str, Any]:
    """The smallest of the channel's candidate encodings (zstd -3 of each one's buffers)."""
    candidates: list[tuple[str, dict[str, str]] | None] = [None]
    if predict:
        candidates = [*_PREDICTED[key], None]
    best: tuple[int, tuple[str, dict[str, str]] | None] | None = None
    if len(candidates) > 1:
        for candidate in candidates:
            trial = _Body()
            if candidate is None:
                _encode_float(values, trial)
            elif _encode_predicted(*candidate, values, context, trial) is None:
                continue
            size = len(_zstd_compress_level(b"".join(trial.chunks), 3))
            if best is None or size < best[0]:
                best = (size, candidate)
    choice = None if best is None else best[1]
    if choice is not None:
        descriptor = _encode_predicted(*choice, values, context, body)
        if descriptor is not None:
            return descriptor
    return _encode_float(values, body)


def _best_divisor(numerator: np.ndarray, values: np.ndarray) -> float:
    """The ``D`` for which ``numerator / D`` lands on most samples of ``values`` exactly."""
    usable = np.isfinite(numerator) & np.isfinite(values) & (values != 0) & (numerator != 0)
    if not usable.any():
        return 1.0
    centre = float(np.median(numerator[usable] / values[usable]))
    if not np.isfinite(centre) or centre == 0:
        return 1.0
    candidates = [centre]
    lo = hi = centre
    for _ in range(4):
        lo = float(np.nextafter(lo, -np.inf))
        hi = float(np.nextafter(hi, np.inf))
        candidates += [lo, hi]
    best, best_score = centre, -1
    for candidate in candidates:
        score = int(np.count_nonzero((numerator / candidate) == values))
        if score > best_score:
            best, best_score = candidate, score
    return best


def _column_layout(
    lap_entries: list[dict[str, Any]], columns: dict[tuple[str, str], list[np.ndarray]]
) -> tuple[_Rows | None, set[str]]:
    """The row layout (every lap's ``time`` samples) and the float64 channels aligned to it."""
    sizes: list[int] = []
    for entry in lap_entries:
        spec = entry["arrays"].get("time")
        if spec is None or spec[0] != "f8":
            return None, set()
        sizes.append(spec[1])
    aligned = {
        key
        for (key, kind) in columns
        if kind == "f8"
        and all(entry["arrays"].get(key) == ["f8", size] for entry, size in zip(lap_entries, sizes))
    }
    return _Rows(sizes), aligned


def encode_driver_file(laps: dict[int, dict[str, Any]], *, level: int = 19) -> bytes:
    """Encode one driver's laps into a ``tel.v2.bin`` file.

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

    concatenated = {
        ref: np.concatenate(parts) if parts else np.empty(0) for ref, parts in columns.items()
    }
    rows, aligned = _column_layout(lap_entries, columns)
    body = _Body()
    header: dict[str, Any] = {"version": FORMAT_VERSION, "laps": lap_entries}
    anchors: _Anchors | None = None
    starts: np.ndarray | None = None
    if rows is not None and all(name in aligned for name in _ANCHOR_CHANNELS):
        mask = np.ones(rows.total, dtype=bool)
        for name in _ANCHOR_CHANNELS:
            mask &= _whole(concatenated[(name, "f8")])
        anchors = _Anchors(mask, rows)
        header["anchors"] = body.add(np.packbits(mask))
        if "time" in aligned:
            time, rpm = concatenated[("time", "f8")], concatenated[("rpm", "f8")]
            starts = _find_lap_starts(time, rpm, anchors, rows)
            header["dates"] = body.add(starts)

    predicted_keys = [
        key for key in _PREDICTED if rows is not None and key in aligned and key not in ("time",)
    ]
    order = [ref for ref in concatenated if not (ref[1] == "f8" and ref[0] in predicted_keys)]
    order += [(key, "f8") for key in predicted_keys]
    context = _DecodeContext(rows, anchors, {}, starts)
    column_entries: list[dict[str, Any]] = []
    for key, kind in order:
        values = concatenated[(key, kind)]
        descriptor: dict[str, Any]
        if kind == "f8":
            descriptor = _encode_smallest(key, values, context, body, key in predicted_keys)
            if not np.array_equal(
                _decode_column(descriptor, body.view(), values.size, context).view(np.int64),
                values.view(np.int64),
            ):  # pragma: no cover - the encodings are exact by construction
                descriptor = {"enc": "f64", "buf": body.add(values)}
            if key in aligned:
                context.decoded[key] = values
        else:
            descriptor = {"enc": "int", "buf": body.add(_narrow(values))}
        descriptor["key"] = key
        descriptor["kind"] = kind
        column_entries.append(descriptor)
    header["columns"] = column_entries

    head_bytes = orjson.dumps(header)
    head = _HEADER_SIZE.pack(len(head_bytes)) + head_bytes
    head += b"\0" * (-len(head) % 8)
    raw = head + b"".join(body.chunks)
    compressed = _zstd_compress_level(raw, level)
    return MAGIC + _RAW_SIZE.pack(len(raw)) + compressed


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


class _DecodeContext:
    """What a predicted column reads: the row layout, the anchors, the decoded channels.

    ``starts`` are the laps' start dates (int64 ns) of ``interp``; the rows'
    float64 dates are computed once from the decoded ``time``.
    """

    def __init__(
        self,
        rows: _Rows | None,
        anchors: _Anchors | None,
        decoded: dict[str, np.ndarray],
        starts: np.ndarray | None = None,
    ) -> None:
        self.rows = rows
        self.anchors = anchors
        self.decoded = decoded
        self.starts = starts
        self._dates: tuple[np.ndarray, np.ndarray] | None = None

    def dates(self, time: np.ndarray) -> np.ndarray:
        """float64 nanosecond date per row (see :func:`_row_dates`)."""
        if self.starts is None or self.rows is None:
            raise ColumnarFormatError("'interp' column without the lap start dates")
        if self._dates is None or self._dates[0] is not time:
            self._dates = (time, _row_dates(time, self.starts, self.rows))
        return self._dates[1]

    def channel(self, descriptor: dict[str, Any], role: str, total: int) -> np.ndarray:
        """The decoded reference channel ``descriptor[role]`` (row-aligned)."""
        name = descriptor.get(role)
        values = self.decoded.get(name) if isinstance(name, str) else None
        if values is None or values.size != total:
            raise ColumnarFormatError(f"predicted column without its reference {name!r}")
        return values


def _buffer(descriptor: list[Any], body: memoryview) -> np.ndarray:
    """The array a ``[dtype, offset, count]`` descriptor names (unshuffled, writable)."""
    dtype_str, offset, count = descriptor
    dtype = np.dtype(dtype_str)
    width = dtype.itemsize
    nbytes = count * width
    if offset < 0 or count < 0 or offset + nbytes > len(body):
        raise ColumnarFormatError("buffer outside the body")
    planes = np.frombuffer(body, dtype=np.uint8, count=nbytes, offset=offset)
    if width == 1:
        return planes.view(dtype).copy()
    return planes.reshape(width, count).T.copy().view(dtype).reshape(count)


def _decode_values(descriptor: dict[str, Any], body: memoryview, total: int) -> np.ndarray:
    """A value-encoded column (``int``, ``f64``, ``dec``, ``split``)."""
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


def _f64(values: np.ndarray) -> np.ndarray:
    """``values`` as a contiguous float64 array (what the native kernels take)."""
    return np.ascontiguousarray(values, dtype=np.float64)


def _lap_sizes(rows: _Rows) -> np.ndarray:
    return np.ascontiguousarray(rows.sizes, dtype=np.int64)


def _residual_buffer(descriptor: dict[str, Any], body: memoryview, count: int) -> np.ndarray:
    zigzag = _buffer(descriptor["res"], body)
    if zigzag.size != count or zigzag.dtype.kind != "u":
        raise ColumnarFormatError("residuals do not match their rows")
    return zigzag


def _decode_column(
    descriptor: dict[str, Any], body: memoryview, total: int, context: _DecodeContext
) -> np.ndarray:
    """One (channel, dtype) column of every lap, concatenated (int64 or float64)."""
    enc = descriptor["enc"]
    if enc in ("int", "f64", "dec", "split"):
        return _decode_values(descriptor, body, total)
    rows = context.rows
    if rows is None or rows.total != total:
        raise ColumnarFormatError(f"{enc!r} column without the row layout")
    anchors = context.anchors
    if enc in ("lerp", "interp"):
        if anchors is None:
            raise ColumnarFormatError(f"{enc!r} column without anchors")
        if enc == "lerp":
            ref = context.channel(descriptor, "ref", total)
            predicted_rows, j0, j1 = _lerp_rows(ref, anchors)
            rest = ~anchors.mask
            rest[predicted_rows] = False
        else:
            dates = context.dates(context.channel(descriptor, "time", total))
            predicted_rows, j0, j1, rest = anchors.between()
        out = np.empty(total, dtype=np.float64)
        out[anchors.mask] = _decode_values(descriptor["anchor"], body, anchors.count)
        out[rest] = _decode_values(descriptor["rest"], body, int(np.count_nonzero(rest)))
        zigzag = _residual_buffer(descriptor, body, predicted_rows.size)
        native = _json_utils.NATIVE_TEL_INTERP
        if enc == "interp" and native is not None:
            native(
                out,
                _f64(dates),
                np.ascontiguousarray(predicted_rows, dtype=np.int64),
                np.ascontiguousarray(j0, dtype=np.int64),
                np.ascontiguousarray(j1, dtype=np.int64),
                np.ascontiguousarray(zigzag, dtype=np.uint64),
            )
            return out
        if enc == "lerp":
            predicted = _lerp_predict(ref, out, predicted_rows, j0, j1)
        else:
            predicted = _interp_predict(dates, out, predicted_rows, j0, j1)
        out[predicted_rows] = _apply_residuals(predicted, zigzag)
        return out
    if enc == "spline":
        dates = context.dates(context.channel(descriptor, "time", total))
        whole = np.unpackbits(_buffer(descriptor["mask"], body), count=total).view(bool)
        out = np.empty(total, dtype=np.float64)
        out[whole] = _buffer(descriptor["ints"], body)
        targets = np.flatnonzero(~whole)
        zigzag = _residual_buffer(descriptor, body, targets.size)
        out[targets] = _apply_residuals(_spline_predict(dates, out, whole, targets, rows), zigzag)
        return out
    if enc in ("integ", "integs"):
        if anchors is None:
            raise ColumnarFormatError(f"{enc!r} column without anchors")
        speed = context.channel(descriptor, "speed", total)
        time = context.channel(descriptor, "time", total)
        integ_rows, prev, increments = _integ_increments(speed, time, anchors)
        rest = np.ones(total, dtype=bool)
        rest[integ_rows] = False
        if enc == "integs":
            rest[~anchors.mask] = False
        out = np.empty(total, dtype=np.float64)
        out[rest] = _decode_values(descriptor["rest"], body, int(np.count_nonzero(rest)))
        zigzag = _residual_buffer(descriptor, body, integ_rows.size)
        _integ_decode(out, integ_rows, prev, increments, zigzag)
        if enc == "integs":
            targets = np.flatnonzero(~anchors.mask)
            spl = _buffer(descriptor["spl"], body)
            if spl.size != targets.size or spl.dtype.kind != "u":
                raise ColumnarFormatError("residuals do not match their rows")
            between = _spline_predict(context.dates(time), out, anchors.mask, targets, rows)
            out[targets] = _apply_residuals(between, spl)
        return out
    if enc == "ratio":
        numerator = context.channel(descriptor, "of", total)
        divisors = _buffer(descriptor["div"], body)
        if divisors.dtype != np.float64 or divisors.size != rows.sizes.size:
            raise ColumnarFormatError("'ratio' divisors do not match the laps")
        zigzag = _residual_buffer(descriptor, body, total)
        return _apply_residuals(_ratio_predict(numerator, divisors, rows), zigzag)
    if enc == "accx":
        speed = context.channel(descriptor, "speed", total)
        time = context.channel(descriptor, "time", total)
        zigzag = _residual_buffer(descriptor, body, total)
        native = _json_utils.NATIVE_TEL_ACCX
        if native is not None:
            predicted = native(_f64(speed), _f64(time), _lap_sizes(rows))
        else:
            predicted = _accx_predict(speed, time, rows)
        return _apply_residuals(predicted, zigzag)
    if enc == "lat":
        speed, x, other, distance = (
            context.channel(descriptor, role, total) for role in ("speed", "x", "other", "distance")
        )
        native = _json_utils.NATIVE_TEL_LATERAL
        if native is not None:
            predicted = native(_f64(speed), _f64(x), _f64(other), _f64(distance), _lap_sizes(rows))
        else:
            predicted = _lateral_predict(speed, x, other, distance, rows)
        return _apply_residuals(predicted, _residual_buffer(descriptor, body, total))
    raise ColumnarFormatError(f"unknown column encoding {enc!r}")


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
    """Decode a ``tel.v2.bin`` file into lap number -> ``tel`` mapping.

    Each lap's mapping equals what the native telemetry decoder returns for
    that lap's ``_tel.json`` (see the module docstring). With
    ``tif1_native.tel_decode_file`` the whole body is decoded in one call
    with the GIL released, one array per lap and channel; it declines
    (``ValueError``) anything it does not take, and so does a build without
    it: the column-by-column decoder below then runs, where arrays of one
    channel are slices of one per-driver array (disjoint per lap). Both give
    the same values.

    Raises:
        ColumnarFormatError: The bytes are not a valid version-2 file.
    """
    body = _decompress(blob)
    native = _json_utils.NATIVE_TEL_DECODE_FILE
    if native is not None:
        try:
            return native(body, DictionaryChannel)
        except ValueError:
            pass  # a layout the native decoder does not take: the decoder below decides
    try:
        (header_len,) = _HEADER_SIZE.unpack_from(body, 0)
        header = orjson.loads(bytes(body[_HEADER_SIZE.size : _HEADER_SIZE.size + header_len]))
        start = _HEADER_SIZE.size + header_len
        start += -start % 8
        buffers = body[start:]
        laps_meta: list[dict[str, Any]] = header["laps"]
        if header.get("version") != FORMAT_VERSION:
            raise ColumnarFormatError(f"unsupported version {header.get('version')!r}")

        # Concatenated columns, then per-lap slices in lap order.
        totals: dict[tuple[str, str], int] = {}
        for entry in laps_meta:
            for key, (kind, size) in entry["arrays"].items():
                totals[(key, kind)] = totals.get((key, kind), 0) + size
        rows = _layout_of(laps_meta)
        anchors = None
        starts = None
        if rows is not None and header.get("anchors") is not None:
            packed = _buffer(header["anchors"], buffers)
            anchors = _Anchors(np.unpackbits(packed, count=rows.total).view(bool), rows)
        if rows is not None and header.get("dates") is not None:
            starts = _buffer(header["dates"], buffers)
            if starts.dtype != np.int64 or starts.size != rows.sizes.size:
                raise ColumnarFormatError("lap start dates do not match the laps")
        context = _DecodeContext(rows, anchors, {}, starts)
        columns: dict[tuple[str, str], np.ndarray] = {}
        for descriptor in header["columns"]:
            ref = (descriptor["key"], descriptor["kind"])
            values = _decode_column(descriptor, buffers, totals.get(ref, 0), context)
            columns[ref] = values
            if ref[1] == "f8" and rows is not None and values.size == rows.total:
                context.decoded[ref[0]] = values
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


def _layout_of(laps_meta: list[dict[str, Any]]) -> _Rows | None:
    """The row layout of a file (every lap's ``time`` samples), or None without ``time``."""
    sizes: list[int] = []
    for entry in laps_meta:
        spec = entry["arrays"].get("time")
        if spec is None or spec[0] != "f8":
            return None
        sizes.append(int(spec[1]))
    return _Rows(sizes)
