"""Polars-native model family for ``lib="polars"`` sessions.

:class:`PolarsLaps`, :class:`PolarsTelemetry` and :class:`PolarsSessionResults`
subclass :class:`polars.DataFrame` and carry a ``session`` (telemetry also a
``driver``) attribute through every tif1 method; :class:`PolarsLap`,
:class:`PolarsDriverResult` and :class:`PolarsDriver` wrap one row of those
frames with scalar access. Every method re-wraps its result, so fastf1-style
chains keep the class (``laps.pick_driver("VER").pick_fastest().get_telemetry()``).
The selection, telemetry and lookup paths run on Series kernels and numpy
rather than the polars query engine (see "Kernels off the query engine").

polars returns a *plain* ``DataFrame`` from ``filter``/``select``/``sort``/
``with_columns``; the ``df.f1`` namespace (:class:`F1Namespace`) exposes the
same methods on any plain frame and reads the session from the frame's
``_tif1_session`` attribute (the classes expose their ``session`` under that
name; a plain frame can be tagged by hand) or from an explicit ``session=``.

This module never imports pandas (or :mod:`tif1.core`): the only project
imports are the exception classes.
"""

import bisect
import datetime as dt
import math
import re
import sys
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any, ClassVar, NamedTuple, Self, cast

import numpy as np
import polars as pl

from .exceptions import (
    DataNotFoundError,
    DriverNotFoundError,
    InvalidDataError,
    LapNotFoundError,
    NetworkError,
)

__all__ = [
    "TELEMETRY_SCHEMA",
    "WEATHER_COLUMNS",
    "F1Namespace",
    "IterLapResult",
    "PolarsDriver",
    "PolarsDriverResult",
    "PolarsLap",
    "PolarsLaps",
    "PolarsSessionResults",
    "PolarsTelemetry",
]

_DURATION_NS = pl.Duration("ns")
#: Telemetry columns that are metadata, not channels: never dropped by
#: ``drop_unknown_channels`` (pandas ``Telemetry`` parity).
_TELEMETRY_META_COLUMNS = ("Driver", "LapNumber")
_DATETIME_NS = pl.Datetime("ns")
_NS_PER_SECOND = 1_000_000_000

#: Column contract of a tif1 polars telemetry frame (``Session`` wiring and
#: the empty frames the models hand out).
TELEMETRY_SCHEMA: dict[str, Any] = {
    "Date": _DATETIME_NS,
    "SessionTime": _DURATION_NS,
    "DriverAhead": pl.String,
    "DistanceToDriverAhead": pl.Float64,
    "Time": _DURATION_NS,
    "RPM": pl.Float64,
    "Speed": pl.Float64,
    "nGear": pl.Int64,
    "Throttle": pl.Float64,
    "Brake": pl.Boolean,
    "DRS": pl.Int64,
    "Source": pl.Categorical,
    "Distance": pl.Float64,
    "RelativeDistance": pl.Float64,
    "Status": pl.Categorical,
    "X": pl.Float64,
    "Y": pl.Float64,
    "Z": pl.Float64,
    "AccelerationX": pl.Float64,
    "AccelerationY": pl.Float64,
    "AccelerationZ": pl.Float64,
    "Driver": pl.String,
    "LapNumber": pl.Int64,
}

#: Weather channels a laps frame carries per lap (after ``WeatherTime``).
WEATHER_COLUMNS: tuple[str, ...] = (
    "AirTemp",
    "Humidity",
    "Pressure",
    "Rainfall",
    "TrackTemp",
    "WindDirection",
    "WindSpeed",
)

#: Errors a per-lap telemetry fetch may raise; they are recorded on the
#: session and the lap answers with an empty frame (pandas ``Lap`` parity).
_TELEMETRY_ERRORS = (DataNotFoundError, InvalidDataError, NetworkError, TypeError, ValueError)

#: Finishing statuses that count as classified finishes (``Finished``,
#: ``+1 Lap``, ``+2 Laps`` ..., ``Lapped``); everything else is a DNF.
_CLASSIFIED_STATUS_RE = re.compile(r"^(finished|lapped|\+\s*\d+\s*laps?)$", re.IGNORECASE)

#: Qualifying segments are separated by a break of several minutes during
#: which no car starts a lap; a gap between consecutive lap starts (any
#: driver) longer than this is a candidate break, the two largest are Q1/Q2
#: and Q2/Q3.
_QUALIFYING_BREAK_NS = 5 * 60 * _NS_PER_SECOND
#: Sessions ``split_qualifying_sessions`` accepts (fastf1's quali-like names).
_QUALI_LIKE_SESSION_NAMES = frozenset({"Qualifying", "Sprint Qualifying", "Sprint Shootout"})
#: Upper bound of the open-ended last qualifying segment (no lap starts after it).
_NO_UPPER_BOUND = 2**63 - 1
#: ``NaT`` as int64 nanoseconds (``pandas.NaT.value``).
_NAT_NS = -(2**63)

_RULE_RE = re.compile(r"^\s*(\d*\.?\d*)\s*([A-Za-z]+)\s*$")
#: pandas offset aliases (current lower-case and the legacy upper-case
#: spellings ``1S``/``500L``/``1T``/``1H``) in nanoseconds per unit.
_RULE_UNIT_NS: dict[str, int] = {
    "ns": 1,
    "n": 1,
    "us": 1_000,
    "u": 1_000,
    "ms": 1_000_000,
    "l": 1_000_000,
    "s": _NS_PER_SECOND,
    "min": 60 * _NS_PER_SECOND,
    "t": 60 * _NS_PER_SECOND,
    "h": 3_600 * _NS_PER_SECOND,
}
_TIME_STRING_RE = re.compile(
    r"^\s*(?P<sign>[-+]?)\s*(?:(?P<days>\d+)\s*days?\s*,?\s*)?"
    r"(?:(?P<h>\d+):)?(?:(?P<m>\d+):)?(?P<s>\d+(?:\.\d*)?)\s*$"
)


# --------------------------------------------------------------------------- #
# Scalar and expression helpers
# --------------------------------------------------------------------------- #


def _is_missing_number(value: Any) -> bool:
    """True for ``None`` and float NaN (a missing scalar read from a frame)."""
    return value is None or (isinstance(value, float) and math.isnan(value))


def _timedelta_ns(value: dt.timedelta) -> int:
    return (value.days * 86_400 + value.seconds) * _NS_PER_SECOND + value.microseconds * 1_000


def _to_ns(value: Any) -> int | None:
    """Scalar time-like value as integer nanoseconds (``None`` for missing).

    Numbers are seconds (fastf1 compatibility); strings follow the
    ``[-][D days ][H:]M:S[.f]`` clock forms; ``datetime.timedelta``,
    ``pandas.Timedelta`` (duck-typed through ``total_seconds``/``value``) and
    ``numpy.timedelta64`` (through ``astype``) are accepted without importing
    their libraries.
    """
    if value is None:
        return None
    if isinstance(value, dt.timedelta):
        return _timedelta_ns(value)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value * _NS_PER_SECOND
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value * _NS_PER_SECOND)
    if isinstance(value, str):
        match = _TIME_STRING_RE.match(value)
        if match is None:
            return None
        groups = match.groupdict()
        # "M:S" (two fields) binds minutes, "H:M:S" binds all three.
        hours = groups["h"]
        minutes = groups["m"]
        if hours is not None and minutes is None:
            hours, minutes = None, hours
        total = (
            int(groups["days"] or 0) * 86_400 + int(hours or 0) * 3_600 + int(minutes or 0) * 60
        ) * _NS_PER_SECOND + round(float(groups["s"]) * _NS_PER_SECOND)
        return -total if groups["sign"] == "-" else total
    raw = getattr(value, "value", None)
    if isinstance(raw, int) and hasattr(value, "total_seconds"):
        return raw  # pandas.Timedelta keeps its nanoseconds in ``value``
    total_seconds = getattr(value, "total_seconds", None)
    if callable(total_seconds):
        seconds = total_seconds()
        if _is_missing_number(seconds):  # NaT
            return None
        return round(float(seconds) * _NS_PER_SECOND)
    astype = getattr(value, "astype", None)
    if callable(astype):  # numpy.timedelta64
        try:
            ns = int(astype("timedelta64[ns]").astype("int64"))
        except (TypeError, ValueError):
            return None
        return None if ns == -(2**63) else ns
    return None


def _rule_to_ns(rule: Any) -> int:
    """Resampling rule (``"1s"``, ``"100ms"``, ``"1min"``, ``"1S"``) as nanoseconds."""
    if isinstance(rule, str):
        match = _RULE_RE.match(rule)
        if match is None:
            raise ValueError(f"Invalid resampling rule {rule!r}")
        number, unit = match.groups()
        unit_ns = _RULE_UNIT_NS.get(unit.lower())
        if unit_ns is None:
            raise ValueError(f"Unknown unit {unit!r} in resampling rule {rule!r}")
        step = round(float(number or 1) * unit_ns)
    else:
        step_ns = _to_ns(rule)
        if step_ns is None:
            raise ValueError(f"Invalid resampling rule {rule!r}")
        step = step_ns
    if step <= 0:
        raise ValueError(f"Resampling rule {rule!r} must be a positive duration")
    return step


def _ns_expr(frame: pl.DataFrame, column: str) -> pl.Expr:
    """``column`` as integer nanoseconds (durations, datetimes or numeric seconds)."""
    dtype = frame.schema[column]
    if isinstance(dtype, pl.Duration):
        return pl.col(column).dt.total_nanoseconds()
    if isinstance(dtype, pl.Datetime):
        return pl.col(column).dt.epoch("ns")
    if dtype.is_numeric():
        return (
            (pl.col(column).cast(pl.Float64) * _NS_PER_SECOND)
            .fill_nan(None)
            .round()
            .cast(pl.Int64, strict=False)
        )
    return pl.lit(None, dtype=pl.Int64)


def _ns_series(frame: pl.DataFrame, column: str) -> pl.Series:
    native = _ns_native(frame.get_column(column))
    if native is not None:
        return native.alias(column)
    return frame.select(_ns_expr(frame, column).alias(column)).to_series()


# --------------------------------------------------------------------------- #
# Kernels off the query engine
#
# Every ``DataFrame.filter``/``select``/``with_columns`` with expressions, and
# many Series methods (``fill_null``, ``is_null``, ``is_in``, ``shift``,
# ``diff``, ``gather``, ``arg_true``, ``str.*``, ``dt.*`` ...), build and run a
# query plan: ~0.5 ms each, ~5-7 ms for the first of a process while polars
# starts its thread pools. The helpers below use only kernels that do not
# (Series comparisons and arithmetic, ``cast``, ``filter``, ``zip_with``,
# indexing, ``to_numpy``) plus numpy, and give the same values, dtypes, null
# handling and row order as the expressions they replace.
# --------------------------------------------------------------------------- #


def _ns_native(series: pl.Series) -> pl.Series | None:
    """A ``Duration``/``Datetime("ns")`` clock as Int64 nanoseconds (``None``: no native route).

    ``dt.total_nanoseconds()``/``dt.epoch("ns")`` without the query engine:
    the physical value of a nanosecond clock is its nanosecond count (UTC
    for a zoned datetime).
    """
    dtype = series.dtype
    if isinstance(dtype, pl.Duration):
        if dtype.time_unit != "ns":
            series = series.cast(_DURATION_NS)
        return series.cast(pl.Int64)
    if isinstance(dtype, pl.Datetime) and dtype.time_unit == "ns":
        return series.cast(pl.Int64)
    return None


def _mask_values(mask: pl.Series, null: bool = False) -> np.ndarray:
    """A Boolean Series as a numpy ``bool`` array, nulls read as ``null`` (``fill_null``)."""
    if not mask.null_count():
        return mask.to_numpy()
    values = mask.cast(pl.Int8).to_numpy()  # nulls come out as NaN
    return values != 0 if null else values == 1


_NESTED_DTYPES = (pl.List, pl.Array, pl.Struct, pl.Object)


def _present_values(series: pl.Series) -> np.ndarray:
    """``series.is_not_null()`` as a numpy ``bool`` array."""
    n = series.len()
    nulls = series.null_count()
    if not nulls:
        return np.ones(n, dtype=bool)
    if nulls == n:
        return np.zeros(n, dtype=bool)
    if isinstance(series.dtype, _NESTED_DTYPES):
        return series.is_not_null().to_numpy()
    # ``x == x`` is null exactly where ``x`` is (polars: NaN equals NaN).
    return _mask_values(series.eq(series))


def _valid_values(series: pl.Series) -> np.ndarray:
    """Non-missing mask of ``series`` (not null, and not NaN for floats) as numpy."""
    if series.dtype.is_float():
        return ~np.isnan(series.to_numpy())  # nulls come out as NaN
    return _present_values(series)


def _filter_rows(frame: pl.DataFrame, keep: np.ndarray) -> pl.DataFrame:
    """``frame.filter(keep)`` as a plain frame, column by column (``keep``: numpy ``bool``)."""
    columns = frame.get_columns()
    if not columns:
        return frame.filter(pl.Series(keep, dtype=pl.Boolean))
    if keep.all():
        return pl.DataFrame._from_pydf(frame._df.clone())
    mask = pl.Series(keep, dtype=pl.Boolean)
    return pl.DataFrame([column.filter(mask) for column in columns])


def _take_rows(frame: pl.DataFrame, positions: np.ndarray | list[int]) -> pl.DataFrame:
    """``frame.select(pl.all().gather(positions))`` column by column (positions in range)."""
    columns = frame.get_columns()
    if not columns:
        return frame.select(pl.all().gather(positions))
    index = np.asarray(positions, dtype=np.int64)
    return pl.DataFrame([column[index] for column in columns])


def _with_columns(frame: pl.DataFrame, *columns: pl.Series) -> pl.DataFrame:
    """``frame.with_columns(columns)`` for full-height Series, as a plain frame.

    A column already in ``frame`` is replaced in place, a new one appended,
    as ``with_columns`` does.
    """
    if not frame.width or any(column.len() != frame.height for column in columns):
        return frame.with_columns(*columns)
    out = pl.DataFrame._from_pydf(frame._df.clone())
    names = frame.columns
    for column in columns:
        if column.name in names:
            out.replace_column(names.index(column.name), column)
        else:
            out.hstack([column], in_place=True)
            names.append(column.name)
    return out


def _full_series(name: str, value: Any, length: int, dtype: Any) -> pl.Series:
    """``pl.repeat(value, length, dtype=dtype, eager=True)`` named ``name``."""
    return pl.Series(name, [value], dtype=dtype).new_from_index(0, length)


def _shift_down(series: pl.Series) -> pl.Series:
    """``series.shift(1)``: a leading null, the last value dropped."""
    n = series.len()
    if n == 0:
        return series.clone()
    return _full_series(series.name, None, 1, series.dtype).append(series.head(n - 1))


def _isin_values(series: pl.Series, values: list[Any]) -> np.ndarray:
    """``series.is_in(values).fill_null(False)`` for a text Series, as numpy."""
    n = series.len()
    if not values or not n:
        return np.zeros(n, dtype=bool)
    if len(values) <= 4:
        hits = series == values[0]
        for value in values[1:]:
            hits = hits | (series == value)
        return _mask_values(hits)
    wanted = set(values)
    return np.fromiter((value in wanted for value in series.to_list()), dtype=bool, count=n)


def _text_test_values(series: pl.Series, test: Callable[[str], bool]) -> np.ndarray:
    """``test`` of each value of a text Series (null: False), once per distinct value."""
    values = series.to_list()
    outcome = {value: value is not None and test(value) for value in set(values)}
    return np.fromiter(map(outcome.__getitem__, values), dtype=bool, count=len(values))


def _distinct_present(series: pl.Series) -> tuple[int, Any]:
    """``(n, value)``: the number of distinct non-null values and the first of them.

    ``series.drop_nulls().unique()`` as a count, plus the value when it is
    the only one (``None`` otherwise or without values).
    """
    nulls = series.null_count()
    if nulls == series.len():
        return 0, None
    count = series.n_unique() - (1 if nulls else 0)
    if count != 1:
        return count, None
    if not nulls:
        return 1, series[0]
    return 1, series[int(np.flatnonzero(_present_values(series))[0])]


def _held(series: pl.Series, has_nan: bool) -> pl.Series:
    """``fill_nan(None)`` (with ``has_nan``), then ``forward_fill().backward_fill()``.

    Each missing sample takes the last value before it, else the first after
    it. A channel without any value is returned as it is, all NaN when it
    held NaN (``fill_null(nan)``).
    """
    valid = _valid_values(series)
    if not valid.any():
        return (
            _full_series(series.name, math.nan, series.len(), series.dtype) if has_nan else series
        )
    positions = np.where(valid, np.arange(valid.size), -1)
    np.maximum.accumulate(positions, out=positions)
    positions[positions < 0] = np.argmax(valid)
    return series[positions]


def _int_values(series: pl.Series) -> tuple[np.ndarray, np.ndarray]:
    """An Int64 Series as ``(int64 values, present mask)``; a null reads as 0."""
    present = _present_values(series)
    if present.all():
        return series.to_numpy(), present
    filled = series.zip_with(pl.Series(present), pl.Series(np.zeros(series.len(), np.int64)))
    return filled.to_numpy(), present


def _first_present(frame: pl.DataFrame, *columns: str) -> str | None:
    for column in columns:
        if column in frame.columns:
            return column
    return None


def _driver_identifier(identifier: Any) -> str:
    """Driver identifier (code, number, dict or driver-like object) as text."""
    if isinstance(identifier, str | int):
        return str(identifier)
    if isinstance(identifier, Mapping):
        for key in ("driver", "Driver", "Abbreviation", "abbreviation", "dn", "RacingNumber"):
            value = identifier.get(key)
            if value is not None and str(value).strip():
                return str(value)
    for attribute in ("driver", "Abbreviation"):
        value = getattr(identifier, attribute, None)
        if value is not None and str(value).strip():
            return str(value)
    return str(identifier)


def _identifier_items(identifiers: Any) -> list[Any]:
    """``identifiers`` as a list for the ``pick_*`` selectors.

    A string, number, mapping or driver-like object (``.driver``/
    ``.Abbreviation``, e.g. a results row) is one identifier; anything else
    iterable (list, tuple, set, polars/pandas Series, numpy array, generator)
    is consumed once.
    """
    if (
        isinstance(identifiers, str | bytes | int | Mapping)
        or hasattr(identifiers, "driver")
        or hasattr(identifiers, "Abbreviation")
    ):
        return [identifiers]
    try:
        return list(identifiers)
    except TypeError:
        return [identifiers]


def _unique_driver_labels(entries: list[tuple[str, str]]) -> list[str]:
    """Index labels for ``(DriverNumber, Abbreviation)`` rows: the number, made unique.

    The rule of ``tif1.core_utils.ergast_results.unique_driver_labels`` (that
    module imports pandas): a shared car keeps the plain number for its
    first driver and labels the later ones ``number/Abbreviation``.
    """
    labels: list[str] = []
    seen: set[str] = set()
    for number, code in entries:
        label = number
        if label in seen:
            label = f"{number}/{code or len(labels) + 1}"
            ordinal = 2
            while label in seen:
                label = f"{number}/{code or len(labels) + 1}-{ordinal}"
                ordinal += 1
        seen.add(label)
        labels.append(label)
    return labels


def _require_qualifying_session(session: Any) -> None:
    """fastf1's guard: only a qualifying-like session's laps split into Q1/Q2/Q3.

    A session object without a name (an in-memory stand-in) is not checked.

    Raises:
        ValueError: ``session``'s name is not Qualifying, Sprint Qualifying or
            Sprint Shootout (or not in its own ``_QUALI_LIKE_SESSIONS``).
    """
    if session is None:
        return
    name = getattr(session, "name", None)
    if not isinstance(name, str):
        name = getattr(session, "session_name", None)
    if not isinstance(name, str):
        return
    quali_like = getattr(session, "_QUALI_LIKE_SESSIONS", None)
    if not isinstance(quali_like, tuple | list | set | frozenset):
        quali_like = _QUALI_LIKE_SESSION_NAMES
    if name not in quali_like:
        raise ValueError("Session is not a qualifying session!")


def _scalar_ns(value: Any) -> int | None:
    """:func:`_to_ns` with ``NaT`` (whose ``value`` is the int64 minimum) as ``None``."""
    ns = _to_ns(value)
    return None if ns == _NAT_NS else ns


#: ASCII characters ``str.strip_chars()`` (Rust's ``trim``, Unicode White_Space) removes.
_ASCII_WHITESPACE = "\t\n\x0b\x0c\r "


def _segment_marks(labels: list[str | None]) -> list[str | None] | None:
    """Q1/Q2/Q3 digit of each ``QualifyingSession`` label (``None`` without one).

    ``str.to_uppercase().str.strip_chars().str.extract(r"([123])$")``, per
    distinct label; ``None`` when a label is not ASCII (polars' Unicode case
    mapping and trimming decide those).
    """
    marks: dict[str | None, str | None] = {None: None}
    for label in set(labels):
        if label is None:
            continue
        if not label.isascii():
            return None
        text = label.upper().strip(_ASCII_WHITESPACE)
        marks[label] = text[-1] if text and text[-1] in "123" else None
    return [marks[label] for label in labels]


def _session_status_rows(status: Any) -> list[tuple[int | None, str]]:
    """``(Time in ns or None, Status)`` per row of a session-status table (polars or pandas)."""
    columns = getattr(status, "columns", None)
    if columns is None or "Time" not in columns or "Status" not in columns or not len(status):
        return []
    if isinstance(status, pl.DataFrame):
        times = _ns_series(status, "Time").to_list()
        states = status.get_column("Status").cast(pl.String).to_list()
    else:  # a pandas table (a pandas session's), read without importing pandas
        times = [_scalar_ns(value) for value in status["Time"]]
        states = list(status["Status"])
    return [(time, str(state)) for time, state in zip(times, states, strict=True)]


def _qualifying_split_times(session: Any, starts: pl.Series | None) -> list[int | None] | None:
    """Qualifying segment bounds in ns (``None`` is NaT: no lap matches it).

    fastf1's algorithm on ``session.session_status``: every ``Started`` row
    opens a segment except the restart after an ``Aborted`` row (a red flag),
    which clears the suspension, as ``Finished`` does; a session's own
    ``_session_split_times`` take precedence; the table's last time closes the
    last segment. Without session status, the lap starts decide
    (:func:`_lap_start_gap_split_times`). ``None`` when there is nothing to
    split on. The pandas ``Laps`` runs the same algorithm.
    """
    status_rows = (
        _session_status_rows(getattr(session, "session_status", None))
        if session is not None
        else []
    )
    if not status_rows:
        return None if starts is None else _lap_start_gap_split_times(starts)
    preset = getattr(session, "_session_split_times", None)
    split_times: list[int | None] = []
    if isinstance(preset, list | tuple) and preset:
        split_times = [_scalar_ns(value) for value in preset]
    else:
        suspended = False
        for time, state in status_rows:
            if state == "Started":
                if suspended:
                    suspended = False
                else:
                    split_times.append(time)
            elif state == "Aborted":
                suspended = True
            elif state == "Finished":
                # A segment that is not resumed after its red flag.
                suspended = False
    split_times.append(status_rows[-1][0])
    return split_times


def _lap_start_gap_split_times(starts: pl.Series) -> list[int | None] | None:
    """Segment bounds from the lap starts (ns) alone: the last resort, no session status.

    The two longest gaps of more than five minutes between consecutive lap
    starts (any driver) are the breaks; a bound sits just before the first
    lap start after each break and the last segment is open-ended. ``None``
    without any lap start.
    """
    valid = starts.drop_nulls().sort()
    if valid.len() == 0:
        return None
    gaps = valid.diff().slice(1)
    breaks = (gaps > _QUALIFYING_BREAK_NS).arg_true()
    if breaks.len() > 2:
        # Longest first, ties to the earlier break; then back in time order.
        ranked = pl.DataFrame({"pos": breaks, "gap": gaps.gather(breaks)}).sort(
            ["gap", "pos"], descending=[True, False]
        )
        breaks = ranked.head(2).get_column("pos").sort()
    bounds = [int(valid[int(b) + 1]) - 1 for b in breaks]
    return [int(valid[0]) - 1, *bounds, _NO_UPPER_BOUND]


#: Columns and dtypes of a per-lap weather sample (``get_weather_data``).
_WEATHER_SAMPLE_SCHEMA: dict[str, Any] = {
    "Time": _DURATION_NS,
    "AirTemp": pl.Float64,
    "Humidity": pl.Float64,
    "Pressure": pl.Float64,
    "Rainfall": pl.Boolean,
    "TrackTemp": pl.Float64,
    "WindDirection": pl.Int64,
    "WindSpeed": pl.Float64,
}


def _session_weather_table(session: Any) -> pl.DataFrame | None:
    """``session.weather_data`` when it is a non-empty polars table with ``Time``."""
    if session is None:
        return None
    weather = getattr(session, "weather_data", None)
    if not isinstance(weather, pl.DataFrame) or weather.height == 0:
        return None
    return weather if "Time" in weather.columns else None


def _weather_sample_positions(
    weather: pl.DataFrame, starts: pl.Series, ends: pl.Series
) -> pl.Series:
    """Row of each lap's weather sample in ``weather`` (null when none).

    fastf1's ``Lap.get_weather_data`` rule with two sorted searches: the first
    sample within ``[start, end]``, else the last sample at or before ``end``;
    a lap without a start takes the latter, a lap without an end has none,
    and a sample without a time is never picked. ``starts``/``ends`` are ns.
    """
    values, present = _int_values(_ns_series(weather, "Time"))
    if present.all() and bool((values[1:] >= values[:-1]).all()):
        order: np.ndarray | None = None  # sorted position -> table row (identity: sorted)
        times = values
    else:  # samples without a time dropped, the rest in time order (stable)
        order = np.flatnonzero(present)
        order = order[np.argsort(values[order], kind="stable")]
        times = values[order]
    n = times.size
    if n == 0:
        return _full_series("rows", None, starts.len(), pl.Int64)
    start, has_start = _int_values(starts)
    end, has_end = _int_values(ends)
    first = np.searchsorted(times, start, side="left")
    after_end = np.searchsorted(times, end, side="right")
    take_first = has_start & has_end & (first < n) & (times[np.minimum(first, n - 1)] <= end)
    take_last = ~take_first & has_end & (after_end > 0)
    rows = np.where(take_first, first, np.where(take_last, after_end - 1, -1))
    if order is not None:
        rows = np.where(rows >= 0, order[np.maximum(rows, 0)], -1)
    # -1 (no sample) as null: through NaN, exact for row numbers.
    return pl.Series("rows", np.where(rows >= 0, rows, np.nan), nan_to_null=True).cast(pl.Int64)


def _weather_sample_row(times: list[int | None], start: int | None, end: int | None) -> int | None:
    """:func:`_weather_sample_positions` for one lap, on the table's times as a list (ns)."""
    if end is None:
        return None
    ordered = sorted((time, row) for row, time in enumerate(times) if time is not None)
    keys = [time for time, _ in ordered]
    if start is not None:
        first = bisect.bisect_left(keys, start)
        if first < len(keys) and keys[first] <= end:
            return ordered[first][1]
    last = bisect.bisect_right(keys, end) - 1
    return ordered[last][1] if last >= 0 else None


def _weather_sample_frame(weather: pl.DataFrame, positions: pl.Series) -> pl.DataFrame:
    """``weather``'s rows at ``positions`` (null: all-null row) as the sample columns.

    ``Time`` becomes ``Duration("ns")``; the channels keep the table's dtypes
    and a channel the table lacks is null.
    """
    columns: list[pl.Series] = []
    index = positions.cast(pl.get_index_type())
    for column, dtype in _WEATHER_SAMPLE_SCHEMA.items():
        if column in weather.columns:
            values = weather.get_column(column)[index]  # a null position: a null row
            if column == "Time":
                values = values.cast(_DURATION_NS)
        else:
            values = _full_series(column, None, positions.len(), dtype)
        columns.append(values)
    return pl.DataFrame(columns)


def _missing_weather_frame(height: int) -> pl.DataFrame:
    """``height`` all-null weather samples with the sample schema."""
    return pl.DataFrame(
        [
            _full_series(column, None, height, dtype)
            for column, dtype in _WEATHER_SAMPLE_SCHEMA.items()
        ]
    )


def _empty_telemetry_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=TELEMETRY_SCHEMA)


def _as_polars_frame(value: Any, what: str) -> pl.DataFrame:
    if isinstance(value, pl.DataFrame):
        return value
    raise TypeError(f"{what} must be a polars DataFrame, got {type(value).__name__}")


def _session_context(session: Any) -> dict[str, Any]:
    """Structured error context (year/event/session) from a session-like object."""
    context: dict[str, Any] = {}
    for key, attribute in (("year", "year"), ("event", "gp"), ("session", "session")):
        value = getattr(session, attribute, None)
        if value is not None and not callable(value):
            context[key] = value
    return context


def _frequency_rule(frequency: Any) -> str | None:
    """Resampling rule for a telemetry ``frequency`` (``None`` for ``"original"``)."""
    if frequency == "original":
        return None
    if isinstance(frequency, bool) or not isinstance(frequency, int | float) or frequency <= 0:
        raise ValueError(
            f"Invalid telemetry frequency {frequency!r}: expected 'original' or a number of Hz"
        )
    return f"{round(_NS_PER_SECOND / frequency)}ns"


# --------------------------------------------------------------------------- #
# Frame base
# --------------------------------------------------------------------------- #


class _TIF1Frame(pl.DataFrame):
    """A polars frame carrying tif1 metadata (``session`` ...) through its methods.

    ``head``/``tail``/``slice``/``limit``/``clone``/``reverse`` and row
    slicing keep the subclass in polars but not its attributes; they are
    overridden to carry them. Every other polars operation returns a plain
    ``DataFrame`` (use ``df.f1`` or ``_from_frame`` to come back).
    """

    #: Attributes copied onto every re-wrapped result.
    _tif1_attrs: ClassVar[tuple[str, ...]] = ("session",)
    session: Any = None

    def __init__(self, data: Any = None, *args: Any, session: Any = None, **kwargs: Any) -> None:
        super().__init__(data, *args, **kwargs)
        self.session = session

    @classmethod
    def _adopt(cls, df: pl.DataFrame) -> Self:
        """A new instance sharing ``df``'s columns (no data copy)."""
        obj = cls.__new__(cls)
        obj._df = df._df.clone()
        return obj

    def _rewrap(self, df: pl.DataFrame) -> Self:
        """``df`` as this class with this frame's metadata (no data copy)."""
        if df is self:
            return self
        cls = type(self)
        obj = cast(Self, df) if type(df) is cls else cls._adopt(df)
        for name in self._tif1_attrs:
            setattr(obj, name, getattr(self, name, None))
        return obj

    @property
    def _tif1_session(self) -> Any:
        """Session handle the ``df.f1`` namespace reads."""
        return self.session

    @property
    def base_class_view(self) -> pl.DataFrame:
        """The same data as a plain :class:`polars.DataFrame` (fastf1 parity: a property)."""
        return pl.DataFrame._from_pydf(self._df.clone())

    def head(self, n: int = 5) -> Self:
        return self._rewrap(super().head(n))

    def tail(self, n: int = 5) -> Self:
        return self._rewrap(super().tail(n))

    def limit(self, n: int = 5) -> Self:
        return self._rewrap(super().limit(n))

    def slice(self, offset: int, length: int | None = None) -> Self:
        return self._rewrap(super().slice(offset, length))

    def clone(self) -> Self:
        return self._rewrap(super().clone())

    def reverse(self) -> Self:
        return self._rewrap(super().reverse())

    def __getitem__(self, key: Any) -> Any:
        result = super().__getitem__(key)
        if isinstance(result, pl.DataFrame):
            return self._rewrap(result)
        return result


# --------------------------------------------------------------------------- #
# Telemetry
# --------------------------------------------------------------------------- #


class PolarsTelemetry(_TIF1Frame):
    """Multi-channel telemetry of one driver as a polars frame.

    Attributes:
        session: The session the telemetry belongs to (``None`` when detached).
        driver: Driver code of the telemetry (``None`` for mixed frames).
    """

    #: Sampling policy of merged/resampled telemetry (fastf1 parity).
    TELEMETRY_FREQUENCY: ClassVar[str | int] = "original"
    #: Channel registry (fastf1's table, plus tif1's acceleration channels;
    #: pandas ``Telemetry`` parity): name -> ``{"type": "continuous" |
    #: "discrete" | "excluded", "method": ...}``. Continuous channels are
    #: interpolated (linearly), discrete ones held, excluded ones left alone.
    _CHANNELS: ClassVar[dict[str, dict[str, str | None]]] = {
        "X": {"type": "continuous", "method": "quadratic"},
        "Y": {"type": "continuous", "method": "quadratic"},
        "Z": {"type": "continuous", "method": "quadratic"},
        "Status": {"type": "discrete"},
        "Speed": {"type": "continuous", "method": "index"},
        "RPM": {"type": "continuous", "method": "index"},
        "Throttle": {"type": "continuous", "method": "index"},
        "Brake": {"type": "discrete"},
        "DRS": {"type": "discrete"},
        "nGear": {"type": "discrete"},
        "Source": {"type": "excluded"},
        "Date": {"type": "excluded"},
        "Time": {"type": "excluded"},
        "SessionTime": {"type": "excluded"},
        "Distance": {"type": "continuous", "method": "quadratic"},
        "RelativeDistance": {"type": "continuous", "method": "quadratic"},
        "DifferentialDistance": {"type": "continuous", "method": "quadratic"},
        "DriverAhead": {"type": "discrete"},
        "DistanceToDriverAhead": {"type": "continuous", "method": "index"},
        "AccelerationX": {"type": "continuous", "method": "index"},
        "AccelerationY": {"type": "continuous", "method": "index"},
        "AccelerationZ": {"type": "continuous", "method": "index"},
    }

    _tif1_attrs: ClassVar[tuple[str, ...]] = ("session", "driver")
    driver: str | None = None

    def __init__(
        self,
        data: Any = None,
        *args: Any,
        session: Any = None,
        driver: str | None = None,
        drop_unknown_channels: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(data, *args, session=session, **kwargs)
        self.driver = driver
        if drop_unknown_channels:
            unknown = [
                column
                for column in self.columns
                if column not in self._CHANNELS and column not in _TELEMETRY_META_COLUMNS
            ]
            if unknown:
                self._df = pl.DataFrame.drop(self, unknown)._df

    @classmethod
    def register_new_channel(
        cls, name: str, signal_type: str, interpolation_method: str | None = None
    ) -> None:
        """Register a custom telemetry channel (fastf1 parity).

        Args:
            name: Channel (column) name.
            signal_type: ``"continuous"`` (Speed, RPM, Distance, ...),
                ``"discrete"`` (DRS, nGear, status values, ...) or
                ``"excluded"`` (left alone when filling and resampling).
            interpolation_method: The interpolation method; required with
                ``signal_type="continuous"``. tif1 interpolates continuous
                channels linearly whatever the method; it is stored for
                fastf1 compatibility.

        Raises:
            ValueError: ``signal_type`` is unknown, or a continuous channel
                has no interpolation method.
        """
        if signal_type not in ("discrete", "continuous", "excluded"):
            raise ValueError(f"Unknown signal type {signal_type}.")
        if signal_type == "continuous" and interpolation_method is None:
            raise ValueError(
                "signal_type='continuous' requires interpolation_method to be specified."
            )
        cls._CHANNELS[name] = {"type": signal_type, "method": interpolation_method}

    @classmethod
    def _from_frame(cls, df: pl.DataFrame, session: Any = None, driver: str | None = None) -> Self:
        """Wrap ``df`` (no data copy) with ``session`` and ``driver``."""
        obj = cls._adopt(df)
        obj.session = session
        obj.driver = driver
        return obj

    @classmethod
    def _from_owned_frame(
        cls, df: pl.DataFrame, session: Any = None, driver: str | None = None
    ) -> Self:
        """:meth:`_from_frame` for a fresh ``df`` nobody else holds (a frame-tier slice).

        Its column handle is taken over instead of cloned: the caller drops
        ``df``, so nothing can observe the sharing.
        """
        obj = cls.__new__(cls)
        obj._df = df._df
        obj.session = session
        obj.driver = driver
        return obj

    @classmethod
    def _empty(cls, session: Any = None, driver: str | None = None) -> Self:
        return cls._from_frame(_empty_telemetry_frame(), session, driver)

    # -- introspection ------------------------------------------------------

    def _session_time_as_time(self) -> Self:
        """This frame on fastf1's session-long clock: ``Time`` equal to ``SessionTime``.

        fastf1's ``session.car_data[n]``/``pos_data[n]`` carry
        ``Time = Date - t0_date``, which is ``SessionTime`` (pandas class
        parity). Frames without both columns come back unchanged.
        """
        if "SessionTime" not in self.columns or "Time" not in self.columns:
            return self
        time = self.get_column("SessionTime").cast(self.schema["Time"], strict=False)
        return self._rewrap(_with_columns(self, time.alias("Time")))

    def _resolve_driver_code(self) -> str | None:
        if isinstance(self.driver, str) and self.driver:
            return self.driver
        if "Driver" not in self.columns:
            return None
        count, driver = _distinct_present(self.get_column("Driver"))
        if count != 1:
            return None
        return str(driver) if driver else None

    def _get_lap_numbers(self) -> list[int]:
        if "LapNumber" not in self.columns:
            return []
        numbers, present = _int_values(self.get_column("LapNumber").cast(pl.Int64, strict=False))
        return [int(value) for value in np.unique(numbers[present])]

    def _time_reference_column(self, other: pl.DataFrame | None = None) -> str | None:
        for column in ("SessionTime", "Time"):
            if column in self.columns and (other is None or column in other.columns):
                return column
        return None

    def get_first_non_zero_time_index(self) -> int:
        """Position of the first sample whose ``Time`` is above zero (``0`` if none)."""
        if "Time" not in self.columns or self.height == 0:
            return 0
        positions = np.flatnonzero(_mask_values(_ns_series(self, "Time") > 0))
        return int(positions[0]) if positions.size else 0

    # -- missing values -----------------------------------------------------

    def fill_missing(self) -> Self:
        """Fill missing samples the way fastf1's ``fill_missing`` does (pandas class parity).

        Discrete channels (``nGear``, ``DRS``, ``Brake``, ``Status``,
        ``DriverAhead``, registered discrete channels) and ``Driver``/
        ``LapNumber`` take the value in force (forward then backward fill)
        and keep their dtype; ``Source`` gaps become ``"interpolation"``;
        excluded channels (the clocks) are left alone. Other float columns
        are interpolated linearly in both directions (NaN counts as
        missing), other integer and boolean columns are held like discrete
        ones, anything else is kept. Only columns with gaps are touched.
        """
        null_counts = self.null_count().row(0)
        exprs: list[pl.Expr] = []
        filled: list[pl.Series] = []
        for column, dtype, nulls in zip(self.columns, self.dtypes, null_counts, strict=True):
            rule = _fill_rule(column, dtype)
            if rule is None:
                continue
            series = self.get_column(column)
            # ``is_nan().any()``: polars orders NaN above +inf (nulls are skipped).
            has_nan = bool(dtype.is_float() and self.height and (series > math.inf).any())
            if not nulls and not has_nan:
                continue
            if rule == "interpolate":
                expr = pl.col(column)
                if has_nan:
                    expr = expr.fill_nan(None)
                expr = expr.interpolate().forward_fill().backward_fill()
                if has_nan:  # a channel that was all NaN stays NaN, not null
                    expr = expr.fill_null(math.nan)
                exprs.append(expr.alias(column))
            elif rule == "source":
                if dtype not in (pl.String, pl.Categorical):
                    exprs.append(pl.col(column).fill_null(pl.lit("interpolation").cast(dtype)))
                    continue
                gaps = _full_series(column, "interpolation", self.height, pl.String).cast(dtype)
                filled.append(series.zip_with(pl.Series(_present_values(series)), gaps))
            else:
                filled.append(_held(series, has_nan))
        if not exprs and not filled:
            return self._rewrap(self.clone())
        frame: pl.DataFrame = _with_columns(self, *filled) if filled else self
        return self._rewrap(frame.with_columns(exprs) if exprs else frame)

    # -- distance -----------------------------------------------------------

    def _differential_distance_values(self) -> np.ndarray:
        """``Speed / 3.6 * dt`` per sample, the pandas class's arithmetic bit for bit.

        numpy arrays: element-wise division (polars evaluates a division by a
        scalar as a multiplication by its reciprocal, 1 ulp from numpy).
        """
        speed = self.get_column("Speed").cast(pl.Float64, strict=False).to_numpy()
        speed_ms = np.where(np.isnan(speed), 0.0, speed) / 3.6  # NaN and null count as 0
        if "Time" not in self.columns:
            return speed_ms * 0.0
        nanoseconds = _series_ns(self.get_column("Time")).cast(pl.Float64).to_numpy()
        seconds = nanoseconds / float(_NS_PER_SECOND)
        seconds[np.isnan(seconds)] = 0.0  # a missing clock counts as 0
        delta = np.zeros(seconds.size)
        np.maximum(np.diff(seconds), 0.0, out=delta[1:])
        return speed_ms * delta

    def calculate_differential_distance(self) -> pl.Series:
        """Distance driven between subsequent samples in metres (``Speed * dt``)."""
        if self.height == 0 or "Speed" not in self.columns:
            return pl.Series("DifferentialDistance", [], dtype=pl.Float64)
        return pl.Series("DifferentialDistance", self._differential_distance_values())

    def integrate_distance(self) -> pl.Series:
        """Distance driven since the first sample in metres (cumulative sum)."""
        if self.height == 0 or "Speed" not in self.columns:
            return pl.Series("Distance", [], dtype=pl.Float64)
        return pl.Series("Distance", np.cumsum(self._differential_distance_values()))

    def _with_float_column(self, name: str, values: pl.Series | None) -> Self:
        """A copy with ``name`` set to ``values`` (all null when None), in place or appended."""
        if values is None:
            values = _full_series(name, None, self.height, pl.Float64)
        return self._rewrap(_with_columns(self, values.alias(name)))

    def add_differential_distance(self, drop_existing: bool = True) -> Self:
        """Add ``DifferentialDistance`` (recomputed unless ``drop_existing`` is False)."""
        if "DifferentialDistance" in self.columns and not drop_existing:
            return self
        if self.height == 0 or "Speed" not in self.columns:
            return self._with_float_column("DifferentialDistance", None)
        return self._with_float_column(
            "DifferentialDistance", self.calculate_differential_distance()
        )

    def add_distance(self, drop_existing: bool = True) -> Self:
        """Add ``Distance``: metres driven since the first sample (fastf1 parity).

        Integrated from ``Speed`` over the whole frame
        (:meth:`integrate_distance`). An existing ``Distance`` (tif1 frames
        carry the source's per-lap channel) is replaced in place with
        ``drop_existing=True`` and kept, the frame returned unchanged, with
        ``False`` (pandas class parity).
        """
        if "Distance" in self.columns and not drop_existing:
            return self
        if self.height == 0 or "Speed" not in self.columns:
            return self._with_float_column("Distance", None)
        return self._with_float_column("Distance", self.integrate_distance())

    def add_relative_distance(self, drop_existing: bool = True) -> Self:
        """Add ``RelativeDistance``: ``Distance`` over its last value (fastf1 parity).

        The frame's ``Distance`` is used, integrated from ``Speed`` when the
        column is missing (no ``Distance`` column is added). An existing
        ``RelativeDistance`` is recomputed with ``drop_existing=True`` and
        kept, the frame returned unchanged, with ``False``.
        """
        if "RelativeDistance" in self.columns and not drop_existing:
            return self
        if "Distance" in self.columns:
            distance = self.get_column("Distance").cast(pl.Float64, strict=False)
        elif self.height and "Speed" in self.columns:
            distance = self.integrate_distance()
        else:
            distance = _full_series("Distance", math.nan, self.height, pl.Float64)
        # Divided element by element by a Series: polars turns a division by
        # a scalar into a multiplication by its reciprocal (1 ulp off numpy).
        last = distance[-1] if distance.len() else None
        divisor = np.full(distance.len(), math.nan if last is None else last, dtype=np.float64)
        relative = distance / pl.Series(divisor)
        return self._rewrap(_with_columns(self, relative.alias("RelativeDistance")))

    # -- driver ahead -------------------------------------------------------

    def calculate_driver_ahead(self, return_reference: bool = False) -> tuple[Any, ...]:
        """``(DriverAhead, DistanceToDriverAhead[, reference])`` series.

        tif1 telemetry ships both channels pre-computed; without them the
        result is all-null/NaN (no position data to integrate).
        """
        if "DriverAhead" in self.columns and "DistanceToDriverAhead" in self.columns:
            driver_ahead = self.get_column("DriverAhead").clone()
            distance = self.get_column("DistanceToDriverAhead")
            if distance.dtype != pl.Float64:
                distance = distance.cast(pl.Float64, strict=False)
            else:
                distance = distance.clone()
        else:
            driver_ahead = _full_series("DriverAhead", None, self.height, pl.String)
            distance = _full_series("DistanceToDriverAhead", math.nan, self.height, pl.Float64)
        if return_reference:
            return driver_ahead, distance, self
        return driver_ahead, distance

    def add_driver_ahead(self, drop_existing: bool = True) -> Self:  # noqa: ARG002
        """Add ``DriverAhead``/``DistanceToDriverAhead`` (kept when already present)."""
        if "DriverAhead" in self.columns and "DistanceToDriverAhead" in self.columns:
            return self
        driver_ahead, distance = self.calculate_driver_ahead(return_reference=False)
        return self._rewrap(
            _with_columns(
                self, driver_ahead.alias("DriverAhead"), distance.alias("DistanceToDriverAhead")
            )
        )

    # -- track status -------------------------------------------------------

    def _track_status_series(self) -> pl.Series:
        """Track status per sample from ``session.track_status`` (``"1"`` without one)."""
        table = getattr(self.session, "track_status", None) if self.session is not None else None
        reference = _first_present(self, "SessionTime", "Time")
        usable = (
            isinstance(table, pl.DataFrame)
            and table.height > 0
            and reference is not None
            and "Time" in table.columns
            and "Status" in table.columns
        )
        if not usable:
            return _full_series("TrackStatus", "1", self.height, pl.String)
        assert isinstance(table, pl.DataFrame)
        assert reference is not None
        times = _series_ns(table.get_column("Time"))
        present = _present_values(times)
        if not present.any():
            return _full_series("TrackStatus", "1", self.height, pl.String)
        statuses = table.get_column("Status").cast(pl.String)
        if not present.all():
            keep = pl.Series(present)
            times, statuses = times.filter(keep), statuses.filter(keep)
        event_ns = times.to_numpy()
        if bool((event_ns[1:] > event_ns[:-1]).all()):
            # Strictly increasing event times (the order any sort gives): the
            # last event at or before each sample, floored at the first event
            # for samples preceding every event.
            samples, known = _int_values(_ns_series(self, reference))
            samples = np.where(known, samples, event_ns[0])
            index = np.maximum(np.searchsorted(event_ns, samples, side="right") - 1, 0)
            return statuses[index].alias("TrackStatus")
        # Tied or unordered event times: the sort decides which event wins.
        events = (
            table.select(
                _ns_expr(table, "Time").alias("t"), pl.col("Status").cast(pl.String).alias("s")
            )
            .drop_nulls("t")
            .sort("t")
        )
        if events.height == 0:
            return pl.repeat("1", self.height, dtype=pl.String, eager=True).alias("TrackStatus")
        event_times = events.get_column("t")
        samples = _ns_series(self, reference).fill_null(event_times[0])
        # Last event at or before each sample: searchsorted(right) - 1, floored
        # at the first event for samples preceding every event.
        index = (event_times.search_sorted(samples, side="right").cast(pl.Int64) - 1).clip(
            lower_bound=0
        )
        return events.get_column("s").gather(index).alias("TrackStatus")

    def add_track_status(self, drop_existing: bool = True) -> Self:
        """Add ``TrackStatus`` mapped from the session's track-status table.

        ``SessionTime`` (or ``Time``) is matched against ``session.track_status``
        (``Time``/``Status``) with a sorted search; without a session or a
        table every sample is ``"1"``. ``drop_existing=False`` keeps an
        existing column.
        """
        if "TrackStatus" in self.columns and not drop_existing:
            return self
        return self._rewrap(_with_columns(self, self._track_status_series()))

    # -- slicing ------------------------------------------------------------

    def slice_by_mask(self, mask: Any, pad: int | float = 0, pad_side: str = "both") -> Self:
        """Rows where ``mask`` is True, optionally padded by ``pad`` samples."""
        series = mask if isinstance(mask, pl.Series) else pl.Series(mask)
        if series.dtype != pl.Boolean:
            series = series.cast(pl.Boolean)
        if series.len() != self.height:
            raise ValueError("Mask length must match telemetry length.")
        keep = _mask_values(series)
        if pad and keep.any():
            positions = np.flatnonzero(keep)
            first = int(positions[0])
            last = int(positions[-1])
            if pad_side in ("both", "before"):
                first = max(0, first - int(pad))
            if pad_side in ("both", "after"):
                last = min(self.height - 1, last + int(pad))
            if first <= last:
                keep = keep.copy()
                keep[first : last + 1] = True
        return self._rewrap(_filter_rows(self, keep))

    def _slice_with_edges(
        self, reference: str, start_ns: int, end_ns: int, pad: int, pad_side: str
    ) -> pl.DataFrame:
        """``slice_by_time(interpolate_edges=True)`` before ``Time`` is re-based.

        The frame is read in ``reference`` order (sorted only when it is
        not), the window and its padding are sliced, and the two edge
        samples, computed from their neighbours, are inserted: no other
        sample changes (pandas ``Telemetry`` parity).
        """
        if start_ns > end_ns:
            return self.clear()
        times = _ns_series(self, reference)
        frame: pl.DataFrame = self
        if times.null_count() or not times.is_sorted():
            order = _stable_order(times)
            frame = _take_rows(self, order)
            times = times[order]
        clock = times.head(times.len() - times.null_count()).to_numpy()
        n_valid = clock.size
        lo = int(np.searchsorted(clock, start_ns, side="left"))
        hi = int(np.searchsorted(clock, end_ns, side="right"))
        first, last = lo, hi
        if pad and pad_side in ("both", "before"):
            first = max(0, lo - pad)
        if pad and pad_side in ("both", "after"):
            last = min(n_valid, hi + pad)
        need_start = n_valid > 0 and not (lo < n_valid and clock[lo] == start_ns)
        need_end = n_valid > 0 and end_ns != start_ns and not (hi > 0 and clock[hi - 1] == end_ns)
        edges = [(lo, start_ns)] if need_start else []
        if need_end:
            edges.append((hi, end_ns))
        if not edges:
            return frame.slice(first, last - first)
        samples = _edge_samples(frame, clock, reference, [t for _, t in edges])
        pieces = []
        cursor = first
        for k, (position, _) in enumerate(edges):
            pieces.extend((frame.slice(cursor, position - cursor), samples.slice(k, 1)))
            cursor = position
        pieces.append(frame.slice(cursor, last - cursor))
        return _stack_rows([piece for piece in pieces if piece.height])

    def _slice_by_time_ns(
        self,
        start_ns: int | None,
        end_ns: int | None,
        pad: int | float,
        pad_side: str,
        interpolate_edges: bool,
    ) -> Self:
        reference = _first_present(self, "SessionTime", "Time")
        if reference is None:
            return self._rewrap(self.clone())
        if start_ns is None or end_ns is None:
            return self._rewrap(self.clear())
        if interpolate_edges and self.height:
            data = self._rewrap(
                self._slice_with_edges(reference, start_ns, end_ns, int(pad), pad_side)
            )
        else:
            times = _ns_series(self, reference)
            mask = pl.Series(_mask_values((times >= start_ns) & (times <= end_ns)))
            data = self.slice_by_mask(mask, pad, pad_side)
        if data.height:
            # Time restarts at the slice start so laps can overlap (fastf1).
            time = (_ns_series(data, reference) - start_ns).cast(_DURATION_NS)
            data = data._rewrap(_with_columns(data, time.alias("Time")))
        return data

    def slice_by_time(
        self,
        start_time: Any,
        end_time: Any,
        pad: int | float = 0,
        pad_side: str = "both",
        interpolate_edges: bool = False,
    ) -> Self:
        """Samples between ``start_time`` and ``end_time`` (inclusive; fastf1 parity).

        ``SessionTime`` is the reference when present, else ``Time``; the
        result's ``Time`` restarts at ``start_time``. Numbers are seconds.
        ``pad`` adds that many samples on ``pad_side``. ``interpolate_edges``
        adds one sample exactly at ``start_time`` and one at ``end_time``
        (unless a sample is already there): continuous channels interpolated
        linearly between the neighbouring samples, the clocks advanced to the
        edge, discrete channels and text the value in force, ``Source``
        ``"interpolation"``. The rest of the frame is unchanged.
        """
        return self._slice_by_time_ns(
            _to_ns(start_time), _to_ns(end_time), pad, pad_side, interpolate_edges
        )

    def slice_by_lap(
        self,
        ref_laps: Any,
        pad: int | float = 0,
        pad_side: str = "both",
        interpolate_edges: bool = False,
    ) -> Self:
        """Samples of ``ref_laps`` (a :class:`PolarsLap`, laps frame, lap number or mapping).

        Without ``SessionTime`` the samples are matched on ``LapNumber`` (and
        ``Driver`` when both sides carry it); with it the lap's session-time
        window (``LapStartTime`` .. ``Time``) is sliced.
        """
        frame: pl.DataFrame | None = None
        if isinstance(ref_laps, PolarsLap):
            frame = ref_laps.frame
        elif isinstance(ref_laps, pl.DataFrame):
            frame = ref_laps
        elif isinstance(ref_laps, bool) or not isinstance(ref_laps, int | Mapping):
            raise TypeError(
                "'ref_laps' needs to be a PolarsLap, a laps frame, a lap number or a mapping"
            )
        if (
            frame is not None
            and frame.height > 1
            and "DriverNumber" in frame.columns
            and frame.get_column("DriverNumber").n_unique() > 1
        ):
            raise ValueError(
                "Cannot slice telemetry because 'ref_laps' contains Laps of multiple drivers!"
            )
        lap_numbers = _ref_lap_numbers(ref_laps)
        if "SessionTime" not in self.columns and "LapNumber" in self.columns and lap_numbers:
            keep = self._lap_number_values(lap_numbers)
            drivers = _ref_lap_drivers(ref_laps)
            if drivers and "Driver" in self.columns:
                keep = keep & _isin_values(self.get_column("Driver").cast(pl.String), drivers)
            return self.slice_by_mask(pl.Series(keep), pad=pad, pad_side=pad_side)
        start_ns, end_ns = _ref_lap_window(ref_laps)
        if start_ns is not None and end_ns is not None:
            return self._slice_by_time_ns(start_ns, end_ns, pad, pad_side, interpolate_edges)
        if "LapNumber" not in self.columns:
            return self._rewrap(self.clone())
        if not lap_numbers:
            return self._rewrap(self.clear())
        keep = self._lap_number_values(lap_numbers)
        return self.slice_by_mask(pl.Series(keep), pad=pad, pad_side=pad_side)

    def _lap_number_values(self, lap_numbers: list[int]) -> np.ndarray:
        """``LapNumber.is_in(lap_numbers).fill_null(False)`` as numpy."""
        column = self.get_column("LapNumber")
        if column.dtype.is_integer() or column.dtype.is_float():
            return np.isin(column.to_numpy(), lap_numbers)  # a null reads as NaN: no match
        hits = self.select(pl.col("LapNumber").is_in(lap_numbers).fill_null(False)).to_series()
        return hits.to_numpy()

    # -- merging and resampling --------------------------------------------

    def merge_channels(self, other: pl.DataFrame, frequency: int | str | None = None) -> Self:
        """Merge ``other``'s channels into this frame, nearest sample in time.

        The frames are matched on the session clock: ``Date`` when both
        carry it, else ``SessionTime``, else ``Time`` (numbers are seconds).
        Every row of this frame is kept and takes ``other``'s sample nearest
        in time (the earlier one on a tie, the last of equal timestamps on an
        exact match); a row whose clock is missing gets nulls for ``other``'s
        columns. The result is sorted by that clock (missing last). Columns
        in both frames keep this frame's values; ``other``'s copies get an
        ``_other`` suffix. Without a shared clock the frames are put side by
        side. ``frequency`` (Hz) resamples the result; ``None`` means
        :attr:`TELEMETRY_FREQUENCY`. The pandas class matches the same rows.
        """
        other = _as_polars_frame(other, "other")
        key = next(
            (c for c in _TIME_AXIS_COLUMNS if c in self.columns and c in other.columns), None
        )
        if key is not None:
            merged = _merge_nearest(self.base_class_view, other, key)
        else:
            clashes = [column for column in other.columns if column in self.columns]
            if other.height == self.height:
                merged = self.base_class_view.hstack(
                    [
                        column.alias(f"{column.name}_other") if column.name in clashes else column
                        for column in other.get_columns()
                    ]
                )
            else:  # the shorter frame is padded with nulls
                if clashes:
                    other = other.rename({column: f"{column}_other" for column in clashes})
                merged = pl.concat([self.base_class_view, other], how="horizontal")
        result = self._rewrap(merged)
        if frequency is None:
            frequency = type(self).TELEMETRY_FREQUENCY
        rule = _frequency_rule(frequency)
        return result if rule is None else result.resample_channels(rule=rule)

    def resample_channels(self, rule: Any = None, new_date_ref: Any = None, **kwargs: Any) -> Self:
        """Resample onto a fixed ``rule`` or onto ``new_date_ref`` timestamps.

        At most one of ``rule``/``new_date_ref``; neither means ``rule="1s"``
        (the pandas class's default). ``rule`` is a
        pandas-style offset (``"1s"``, ``"100ms"``, ``"1min"``; the legacy
        ``"1S"``/``"500L"``/``"1T"``/``"1H"`` spellings too) or a timedelta:
        the time axis (``SessionTime``, else ``Date``, else ``Time``) is
        bucketed, numeric channels are bucket means interpolated across
        empty buckets, booleans/strings take the first value of each bucket
        and are filled, the other time columns follow the axis and ``Time``
        restarts at zero (fastf1 parity). ``new_date_ref`` (datetimes)
        interpolates every channel at those ``Date`` values. ``**kwargs``
        are accepted for pandas compatibility and ignored.
        """
        _ = kwargs
        if rule is not None and new_date_ref is not None:
            raise ValueError("You can only specify one of 'rule' or 'new_date_ref'")
        if new_date_ref is not None:
            return self._resample_onto_dates(new_date_ref)
        return self._rewrap(_resample_by_rule(self, _rule_to_ns("1s" if rule is None else rule)))

    def _resample_onto_dates(self, new_date_ref: Any) -> Self:
        """One row per unique ``new_date_ref`` timestamp (sorted), as the pandas class.

        Continuous channels and the other clock columns are interpolated on
        ``Date`` (constant beyond the first/last sample); everything else
        takes the value in force.
        """
        if "Date" not in self.columns:
            raise ValueError("'Date' is required to resample onto new reference dates")
        dates = new_date_ref if isinstance(new_date_ref, pl.Series) else pl.Series(new_date_ref)
        if isinstance(dates.dtype, pl.Datetime):
            dates_ns = dates.dt.replace_time_zone(None).dt.epoch("ns")
        elif dates.dtype.is_integer():
            dates_ns = dates.cast(pl.Int64)
        else:
            dates_ns = dates.cast(_DATETIME_NS).dt.epoch("ns")
        targets = pl.DataFrame({"__x": dates_ns.drop_nulls().unique().sort()})
        base = (
            self.with_columns(_ns_expr(self, "Date").alias("__x"))
            .drop_nulls("__x")
            .sort("__x", maintain_order=True)
        )
        if base.height == 0 or targets.height == 0:
            return self._rewrap(self.clear())
        schema = self.schema
        clocks = [c for c in _TIME_AXIS_COLUMNS if c in schema and c != "Date"]
        continuous = [c for c, t in schema.items() if c not in clocks and _is_continuous(c, t)]
        continuous = [c for c in continuous if c != "Date"]
        held = [c for c in schema if c not in (*clocks, *continuous, "Date")]
        x_first = int(base.get_column("__x")[0])
        result = targets
        offsets: list[pl.Expr] = []
        for column in clocks:
            anchor = base.select(_ns_expr(base, column).min()).item()
            if anchor is None:
                continue
            offsets.append((_ns_expr(base, column) - anchor).cast(pl.Float64).alias(column))
        offsets.extend(
            pl.col(column).cast(pl.Float64).fill_nan(None).alias(column) for column in continuous
        )
        if offsets:
            # Interpolate on offsets from the first sample: epoch nanoseconds
            # exceed float64's exact integer range.
            numeric = base.select((pl.col("__x") - x_first).cast(pl.Float64).alias("__t"), *offsets)
            numeric_columns = [c for c in numeric.columns if c != "__t"]
            stacked = pl.concat(
                [
                    numeric,
                    targets.select((pl.col("__x") - x_first).cast(pl.Float64).alias("__t")),
                ],
                how="diagonal_relaxed",
            ).sort("__t", maintain_order=True)
            filled = (
                stacked.with_columns(
                    pl.col(column).interpolate_by("__t").forward_fill().backward_fill()
                    for column in numeric_columns
                )
                .unique("__t", keep="last", maintain_order=True)
                .with_columns((pl.col("__t").round().cast(pl.Int64) + x_first).alias("__x"))
            )
            result = result.join(filled.drop("__t"), on="__x", how="left", maintain_order="left")
        if held:
            in_force = base.select("__x", *(pl.col(column).forward_fill() for column in held))
            result = result.join_asof(in_force, on="__x", strategy="backward")
            # Before the first sample: the column's first value (pandas class rule).
            leading = {column: in_force.get_column(column).drop_nulls().first() for column in held}
            result = result.with_columns(
                pl.col(column).fill_null(value).alias(column)
                for column, value in leading.items()
                if value is not None
            )
        exprs: list[pl.Expr] = [_clock_from_ns(pl.col("__x"), schema["Date"]).alias("Date")]
        for column in clocks:
            anchor = base.select(_ns_expr(base, column).min()).item()
            if anchor is None or column not in result.columns:
                exprs.append(pl.lit(None, dtype=schema[column]).alias(column))
                continue
            value = pl.col(column).round().cast(pl.Int64) + int(anchor)
            exprs.append(_clock_from_ns(value, schema[column]).alias(column))
        exprs.extend(
            pl.col(column).round().cast(schema[column]).alias(column)
            for column in continuous
            if schema[column].is_integer()
        )
        return self._rewrap(result.with_columns(exprs).select(self.columns))


def _first_lap_start_ns(laps: pl.DataFrame) -> int | None:
    """Earliest ``LapStartTime`` of a laps frame as integer ns (None when unknown)."""
    if laps.height == 0 or "LapStartTime" not in laps.columns:
        return None
    start = _ns_series(laps, "LapStartTime").min()
    return None if start is None else int(cast(int, start))


def _join_lap_frames(
    frame: pl.DataFrame, lap_start_ns: int | None, *, multi_driver: bool = False
) -> pl.DataFrame:
    """Lap frames concatenated into one frame, made a multi-lap slice as fastf1 has it.

    pandas ``models._join_lap_telemetry`` parity: the sample each lap frame
    repeats at its lap boundary is dropped (the first kept), one driver's
    lap blocks are put in session-clock order, and ``Time`` becomes
    ``SessionTime`` minus the first lap's ``LapStartTime`` (``lap_start_ns``,
    else the earliest lap start the frame implies; per driver with
    ``multi_driver``). A frame of one lap, or without a session clock, is
    returned unchanged.
    """
    # Series kernels and numpy, no query plan: on a frame of ~60 concatenated
    # chunks each expression-engine pass costs ~1.5 ms.
    if frame.height < 2 or "LapNumber" not in frame.columns:
        return frame
    clock_col = _first_present(frame, "Date", "SessionTime")
    if clock_col is None:
        return frame
    not_first = np.arange(frame.height) > 0
    lap = frame.get_column("LapNumber").cast(pl.Float64, strict=False)
    if not (_mask_values(lap != _shift_down(lap), null=True) & not_first).any():
        return frame
    clock = _series_ns(frame.get_column(clock_col))
    if not multi_driver and not clock.null_count() and not clock.is_sorted():
        # By clock, then lap number (nulls last), stable; numpy sorts NaN last.
        order = np.lexsort((lap.to_numpy(), ~_present_values(lap), clock.to_numpy()))
        frame = _take_rows(frame, order)
        clock = clock[order]
        lap = lap[order]
    same = (
        _mask_values(lap != _shift_down(lap), null=True)
        & not_first
        & _mask_values(clock == _shift_down(clock))  # False where the clock is null
    )
    multi = multi_driver and "Driver" in frame.columns
    if multi:
        drivers = frame.get_column("Driver")
        previous = _shift_down(drivers)
        # ``eq_missing``: equal, or both null.
        same = same & (
            _mask_values(drivers == previous)
            | (~_present_values(drivers) & ~_present_values(previous))
        )
    if "SessionTime" in frame.columns and "Time" in frame.columns:
        session_time = _series_ns(frame.get_column("SessionTime"))
        implied = session_time - _series_ns(frame.get_column("Time"))  # each row's LapStartTime
        origin: pl.Series | int | None = None
        if multi:
            origin = (
                pl.DataFrame({"d": frame.get_column("Driver"), "i": implied})
                .select(pl.col("i").min().over("d"))
                .to_series()
            )
        elif lap_start_ns is not None:
            origin = lap_start_ns
        elif implied.null_count() < implied.len():
            origin = int(cast(int, implied.min()))
        if origin is not None:
            time_dtype = frame.schema["Time"]
            time = (session_time - origin).cast(_DURATION_NS)
            if isinstance(time_dtype, pl.Duration):
                time = time.cast(time_dtype)
            frame = _with_columns(frame, time.alias("Time"))
    return _filter_rows(frame, ~same) if same.any() else frame


def _series_ns(series: pl.Series) -> pl.Series:
    """A clock Series as integer nanoseconds (``_ns_expr`` on a Series; numbers are seconds)."""
    native = _ns_native(series)
    if native is not None:
        return native
    dtype = series.dtype
    if isinstance(dtype, pl.Duration):
        return series.dt.total_nanoseconds()
    if isinstance(dtype, pl.Datetime):
        return series.dt.epoch("ns")
    if dtype.is_numeric():
        scaled = (series.cast(pl.Float64) * _NS_PER_SECOND).fill_nan(None).round()
        return scaled.cast(pl.Int64, strict=False)
    return _full_series(series.name, None, series.len(), pl.Int64)


def _stable_order(values: pl.Series) -> np.ndarray:
    """Row positions sorting Int64 ``values`` ascending, stable, nulls last."""
    numbers, present = _int_values(values)
    # lexsort: the last key is the primary one (present rows first).
    return np.lexsort((numbers, ~present))


def _stack_rows(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """``pl.concat(frames, how="vertical")`` of same-schema frames, as a plain frame."""
    if len(frames) == 1:
        return frames[0]
    out = pl.DataFrame._from_pydf(frames[0]._df.clone())
    for frame in frames[1:]:
        out.vstack(frame, in_place=True)
    return out


def _merge_nearest(left: pl.DataFrame, right: pl.DataFrame, key: str) -> pl.DataFrame:
    """``left`` (sorted on ``key``, nulls last) with ``right``'s nearest sample's columns.

    The pandas ``Telemetry._merge_channels`` rule: a tie goes to the earlier
    sample, an exact match to the last of equal timestamps; a null ``left``
    clock (or an empty ``right``) gets nulls. ``right``'s columns already in
    ``left`` get an ``_other`` suffix.
    """
    # numpy searches and column-by-column gathers: no query plan (an
    # expression pass over a frame of ~60 concatenated chunks costs ~1.5 ms).
    left_ns = _series_ns(left.get_column(key))
    if left_ns.null_count() or not left_ns.is_sorted():
        order = _stable_order(left_ns)
        left = _take_rows(left, order)
        left_ns = left_ns[order]
    right_ns = _series_ns(right.get_column(key))
    if right_ns.null_count():
        present = _present_values(right_ns)
        right = _filter_rows(right, present)
        right_ns = right_ns.filter(pl.Series(present))
    if not right_ns.is_sorted():
        order = _stable_order(right_ns)
        right = _take_rows(right, order)
        right_ns = right_ns[order]
    renamed = {column: f"{column}_other" for column in right.columns if column in left.columns}
    part = [
        right.get_column(column).alias(renamed.get(column, column))
        for column in right.columns
        if column != key
    ]
    n = right_ns.len()
    height = left.height
    known = height - left_ns.null_count()  # null clocks sort last
    if n == 0 or known == 0:
        return left.hstack([_full_series(c.name, None, height, c.dtype) for c in part])
    x = left_ns.head(known).to_numpy()
    keys = right_ns.to_numpy()
    # The first sample after each clock, and the last one at or before it.
    after = np.searchsorted(keys, x, side="right")
    before = after - 1
    gap_before = x - keys[np.maximum(before, 0)]
    gap_after = keys[np.minimum(after, n - 1)] - x
    take_after = (after < n) & ((before < 0) | (gap_after < gap_before))
    index = np.where(take_after, after, before)  # never -1: n > 0
    gathered = [column[index] for column in part]
    if known < height:  # rows without a clock get nulls
        gathered = [
            column.append(_full_series(column.name, None, height - known, column.dtype))
            for column in gathered
        ]
    return left.hstack(gathered)


def _edge_linear(
    t: int, prev_ref: int | None, prev_value: float, next_ref: int | None, next_value: float
) -> float:
    """A continuous channel at ``t`` ns: linear between the samples around it.

    The pandas twin (``tif1.models._edge_linear``) runs the same Python-float
    operations in the same order, so both classes compute equal edge samples.
    """
    if prev_ref is None or math.isnan(prev_value):
        return math.nan if next_ref is None else next_value
    if next_ref is None or math.isnan(next_value):
        return prev_value
    return prev_value + (next_value - prev_value) * ((t - prev_ref) / (next_ref - prev_ref))


def _as_float(value: Any) -> float:
    return math.nan if value is None else float(value)


def _edge_samples(
    frame: pl.DataFrame, clock: np.ndarray, reference: str, times: list[int]
) -> pl.DataFrame:
    """Samples of ``frame`` at ``times`` ns on ``reference`` (none a sampled instant).

    Continuous channels: linear between the samples before and after (the
    one present beyond the data). Other clocks: the held sample's value
    advanced by the edge minus its ``reference``. ``Source``:
    ``"interpolation"``. Everything else: the held sample's value (the
    sample before the edge, else the one after). pandas
    ``Telemetry._edge_samples`` parity.
    """
    n = clock.size
    after = [int(a) for a in np.searchsorted(clock, times, side="right")]
    prevs = [a - 1 if a > 0 else -1 for a in after]
    nexts = [a if a < n else -1 for a in after]
    # One gather of every neighbour: rows [prev..., next...], -1 read as row 0.
    rows = [max(p, 0) for p in (*prevs, *nexts)]
    neighbours = _take_rows(frame, rows)
    k = len(times)
    refs = clock[rows].tolist()
    held_at = [i if prevs[i] >= 0 else k + i for i in range(k)]
    series: list[pl.Series] = []
    for column, dtype in neighbours.schema.items():
        if column == reference and not isinstance(dtype, pl.Duration):  # numeric seconds
            series.append(pl.Series(column, [t / _NS_PER_SECOND for t in times]).cast(dtype))
        elif isinstance(dtype, pl.Datetime | pl.Duration):
            if column == reference:
                ns: list[int | None] = list(times)
            else:
                base = _series_ns(neighbours.get_column(column)).to_list()
                ns = [
                    None if base[h] is None else base[h] + t - refs[h]
                    for h, t in zip(held_at, times, strict=True)
                ]
            unit = _DATETIME_NS if isinstance(dtype, pl.Datetime) else _DURATION_NS
            series.append(pl.Series(column, ns, dtype=pl.Int64).cast(unit).cast(dtype))
        elif column == "Source" and dtype in (pl.String, pl.Categorical):
            series.append(pl.Series(column, ["interpolation"] * k, dtype=pl.String).cast(dtype))
        elif _is_continuous(column, dtype):
            values = neighbours.get_column(column).cast(pl.Float64, strict=False).to_list()
            interpolated = [
                _edge_linear(
                    t,
                    refs[i] if prevs[i] >= 0 else None,
                    _as_float(values[i]) if prevs[i] >= 0 else math.nan,
                    refs[k + i] if nexts[i] >= 0 else None,
                    _as_float(values[k + i]) if nexts[i] >= 0 else math.nan,
                )
                for i, t in enumerate(times)
            ]
            if dtype.is_integer():
                rounded = [None if math.isnan(v) else round(v) for v in interpolated]
                series.append(pl.Series(column, rounded, dtype=dtype))
            else:
                series.append(pl.Series(column, interpolated, dtype=pl.Float64).cast(dtype))
        else:
            series.append(neighbours.get_column(column)[np.asarray(held_at, dtype=np.int64)])
    return pl.DataFrame(series)


def _fill_rule(column: str, dtype: Any) -> str | None:
    """How ``fill_missing`` fills ``column`` (pandas ``models._fill_rule`` parity).

    ``"interpolate"`` (unregistered or continuous floats), ``"hold"``
    (discrete channels, ``Driver``/``LapNumber``, integers, booleans),
    ``"source"`` (``Source`` gaps become ``"interpolation"``) or None
    (excluded channels and anything else).
    """
    if column == "Source":
        return "source"
    kind = PolarsTelemetry._CHANNELS.get(column, {}).get("type")
    if kind == "excluded":
        return None
    if kind == "discrete" or column in _TELEMETRY_META_COLUMNS:
        return "hold"
    if dtype == pl.Boolean or dtype.is_integer():
        return "hold"
    if dtype.is_float():
        return "interpolate"
    return None


#: Clock columns in the order the resampler picks its axis from (fastf1
#: resamples on ``Date``; ``SessionTime`` and ``Time`` stand in without it).
_TIME_AXIS_COLUMNS = ("Date", "SessionTime", "Time")


def _is_continuous(column: str, dtype: Any) -> bool:
    """Numeric, not a discrete/excluded channel, not ``LapNumber`` (pandas class rule)."""
    if column in _TELEMETRY_META_COLUMNS:
        return False
    if PolarsTelemetry._CHANNELS.get(column, {}).get("type") in ("discrete", "excluded"):
        return False
    return dtype.is_float() or dtype.is_integer()


def _clock_from_ns(value: pl.Expr, dtype: Any) -> pl.Expr:
    """Integer nanoseconds back to a clock column's dtype (numeric clocks are seconds)."""
    if isinstance(dtype, pl.Duration):
        return value.cast(_DURATION_NS).cast(dtype)
    if isinstance(dtype, pl.Datetime):
        return value.cast(_DATETIME_NS).cast(dtype)
    return (value.cast(pl.Float64) / _NS_PER_SECOND).cast(dtype)


def _resample_by_rule(frame: pl.DataFrame, step: int) -> pl.DataFrame:
    """Bin ``frame`` every ``step`` ns from its first sample (see ``resample_channels``).

    The axis is ``Date``, else ``SessionTime``, else ``Time``. Continuous
    channels are bin means interpolated across empty bins; everything else
    takes the value in force at each bin start; ``Date``/``SessionTime``
    advance with the bins and ``Time`` restarts at zero. The pandas class
    computes the same rows.
    """
    axis = _first_present(frame, *_TIME_AXIS_COLUMNS)
    if axis is None or frame.height == 0:
        return frame.clone()
    base = (
        frame.with_columns(_ns_expr(frame, axis).alias("__x"))
        .drop_nulls("__x")
        .sort("__x", maintain_order=True)
    )
    if base.height == 0:
        return frame.clear()
    x_first = int(base.get_column("__x")[0])
    x_last = int(base.get_column("__x")[-1])
    n_bins = (x_last - x_first) // step + 1
    base = base.with_columns(((pl.col("__x") - x_first) // step).alias("__bin"))
    grid = pl.DataFrame({"__bin": pl.int_range(0, n_bins, dtype=pl.Int64, eager=True)})
    grid = grid.with_columns((pl.col("__bin") * step + x_first).alias("__x"))

    schema = frame.schema
    clocks = [column for column in _TIME_AXIS_COLUMNS if column in schema]
    continuous = [c for c, t in schema.items() if c not in clocks and _is_continuous(c, t)]
    held = [c for c in schema if c not in clocks and c not in continuous]

    result = grid
    if continuous:
        means = base.group_by("__bin").agg(
            pl.col(column).cast(pl.Float64).fill_nan(None).mean().alias(column)
            for column in continuous
        )
        result = result.join(means, on="__bin", how="left", maintain_order="left").with_columns(
            pl.col(column).interpolate().forward_fill().backward_fill() for column in continuous
        )
    if held:
        in_force = base.select("__x", *(pl.col(column).forward_fill() for column in held))
        result = result.join_asof(in_force, on="__x", strategy="backward").with_columns(
            pl.col(column).backward_fill() for column in held
        )
    first = base.head(1)
    clock_exprs: list[pl.Expr] = []
    for column in clocks:
        if column == "Time":
            value = pl.col("__bin") * step
        else:
            origin = _ns_series(first, column)[0]
            if origin is None:
                clock_exprs.append(pl.lit(None, dtype=schema[column]).alias(column))
                continue
            value = pl.col("__bin") * step + int(origin)
        clock_exprs.append(_clock_from_ns(value, schema[column]).alias(column))
    return result.with_columns(clock_exprs).select(frame.columns)


def _ref_lap_numbers(ref_laps: Any) -> list[int]:
    if isinstance(ref_laps, PolarsLap):
        number = ref_laps.lap_number
        return [] if number is None else [number]
    if isinstance(ref_laps, pl.DataFrame):
        column = _first_present(ref_laps, "LapNumber", "lap")
        if column is None:
            return []
        values = ref_laps.get_column(column).cast(pl.Int64, strict=False).to_list()
        return [int(value) for value in values if value is not None]
    if isinstance(ref_laps, Mapping):
        for key in ("LapNumber", "lap"):
            value = ref_laps.get(key)
            if not _is_missing_number(value):
                return [int(value)]
        return []
    if isinstance(ref_laps, int) and not isinstance(ref_laps, bool):
        return [ref_laps]
    return []


def _ref_lap_drivers(ref_laps: Any) -> list[str]:
    if isinstance(ref_laps, PolarsLap):
        driver = ref_laps.driver
        return [] if driver is None else [str(driver)]
    if isinstance(ref_laps, pl.DataFrame):
        if "Driver" not in ref_laps.columns:
            return []
        drivers = set(ref_laps.get_column("Driver").to_list())
        drivers.discard(None)
        return [str(value) for value in drivers]
    if isinstance(ref_laps, Mapping):
        value = ref_laps.get("Driver")
        return [] if value is None else [str(value)]
    return []


def _ref_lap_window(ref_laps: Any) -> tuple[int | None, int | None]:
    """Session-time window ``(LapStartTime, Time)`` of ``ref_laps`` in nanoseconds."""
    if isinstance(ref_laps, PolarsLap):
        ref_laps = ref_laps.to_dict()
    if isinstance(ref_laps, pl.DataFrame):
        if ref_laps.height == 0:
            return None, None
        start = end = None
        if "LapStartTime" in ref_laps.columns:
            start = _ns_series(ref_laps, "LapStartTime").min()
        if "Time" in ref_laps.columns:
            end = _ns_series(ref_laps, "Time").max()
        if end is None and "LapStartTime" in ref_laps.columns and "LapTime" in ref_laps.columns:
            end = (_ns_series(ref_laps, "LapStartTime") + _ns_series(ref_laps, "LapTime")).max()
        return cast("int | None", start), cast("int | None", end)
    if isinstance(ref_laps, Mapping):
        start = _to_ns(ref_laps.get("LapStartTime"))
        end = _to_ns(ref_laps.get("Time"))
        if end is None and start is not None:
            lap_time = _to_ns(ref_laps.get("LapTime"))
            end = None if lap_time is None else start + lap_time
        return start, end
    return None, None


# --------------------------------------------------------------------------- #
# Laps
# --------------------------------------------------------------------------- #


class IterLapResult(NamedTuple):
    """``(index, lap)`` pair yielded by :meth:`PolarsLaps.iterlaps`."""

    index: int
    lap: "PolarsLap"


class PolarsLaps(_TIF1Frame):
    """Lap timing data of many laps as a polars frame (fastf1 ``Laps`` parity).

    Attributes:
        session: The session the laps belong to (``None`` when detached).
    """

    #: Laps slower than this multiple of the fastest lap are not "quick".
    QUICKLAP_THRESHOLD: ClassVar[float] = 1.07

    _tif1_attrs: ClassVar[tuple[str, ...]] = ("session", "_detached")
    #: Set on the laps ``df.f1`` builds from a frame without a session (and
    #: carried to every frame and lap derived from them): their telemetry
    #: calls raise instead of answering with an empty frame.
    _detached: Any = None

    @classmethod
    def _from_frame(cls, df: pl.DataFrame, session: Any = None) -> Self:
        """Wrap ``df`` (no data copy) with ``session``."""
        obj = cls._adopt(df)
        obj.session = session
        return obj

    def _require_session(self, member: str) -> None:
        """Raise when these laps came from ``df.f1`` on a frame without a session.

        Raises:
            ValueError: ``member`` needs the session and the laps carry none.
        """
        if self._detached and self.session is None and self.height:
            raise ValueError(
                f"{member} needs the session, and these laps carry none: they come "
                "from df.f1 on a plain frame (polars filter/select/sort/with_columns "
                "return plain frames). Pass session= to the df.f1 call, e.g. "
                "df.f1.pick_fastest(session=session)"
            )

    # -- selection ----------------------------------------------------------

    def pick_driver(self, identifier: Any) -> Self:
        """Laps of one driver: fastf1's (deprecated) exact match, no case folding.

        A number (int or digit string) matches ``DriverNumber``; anything else
        must equal ``Driver`` as given (``pick_driver("ver")`` matches
        nothing, as in fastf1; :meth:`pick_drivers` upper-cases codes).
        """
        code = _driver_identifier(identifier)
        if code.isdigit():
            return self._rewrap(_filter_rows(self, self._driver_number_values([code])))
        keep = _mask_values(self.get_column("Driver").cast(pl.String) == code)
        return self._rewrap(_filter_rows(self, keep))

    def pick_drivers(self, identifiers: Any) -> Self:
        """Laps of the given drivers (fastf1 parity).

        Args:
            identifiers: One identifier or any iterable of them (list, tuple,
                set, polars/pandas Series, numpy array, generator). An
                identifier is a code (upper-cased, as fastf1 does), a car
                number (int or digit string), a mapping or a driver-like
                object (``.driver``/``.Abbreviation``).
        """
        codes = [_driver_identifier(identifier) for identifier in _identifier_items(identifiers)]
        names = [code.upper() for code in codes if not code.isdigit()]
        numbers = [code for code in codes if code.isdigit()]
        keep = _isin_values(self.get_column("Driver").cast(pl.String), names)
        if numbers:
            keep = keep | self._driver_number_values(numbers)
        return self._rewrap(_filter_rows(self, keep))

    def _driver_number_values(self, numbers: list[str]) -> np.ndarray:
        """``DriverNumber`` (text or integer column) is one of ``numbers`` (digit strings)."""
        if "DriverNumber" not in self.columns:
            return np.zeros(self.height, dtype=bool)
        return _isin_values(self.get_column("DriverNumber").cast(pl.String), numbers)

    def pick_lap(self, lap_number: int) -> Self:
        """Laps with ``LapNumber == lap_number``."""
        column = self.get_column("LapNumber")
        if type(lap_number) is int and column.dtype.is_integer():
            return self._rewrap(_filter_rows(self, _mask_values(column == lap_number)))
        return self._rewrap(self.filter(pl.col("LapNumber") == lap_number))

    def pick_laps(self, lap_numbers: Any = None, *, laps: Any = None) -> Self:
        """Laps whose ``LapNumber`` is in ``lap_numbers`` (int, iterable or slice).

        ``laps=`` is the historical tif1 keyword and an alias of
        ``lap_numbers``. A slice selects the inclusive range ``start..stop``
        (``start`` defaults to 1).
        """
        if lap_numbers is None:
            lap_numbers = laps
        if lap_numbers is None:
            raise TypeError("pick_laps() missing required argument: 'lap_numbers'")
        column = pl.col("LapNumber")
        if isinstance(lap_numbers, slice):
            start = 1 if lap_numbers.start is None else lap_numbers.start
            stop = lap_numbers.stop
            values = self.get_column("LapNumber")
            if (
                values.dtype.is_integer()
                and type(start) is int
                and (stop is None or type(stop) is int)
            ):
                hits = values >= start
                if stop is not None:
                    hits = hits & (values <= stop)
                return self._rewrap(_filter_rows(self, _mask_values(hits)))
            expr = column >= start
            if stop is not None:
                expr = expr & (column <= stop)
            return self._rewrap(self.filter(expr))
        if isinstance(lap_numbers, int | float):
            lap_numbers = [lap_numbers]
        numbers: list[int] = []
        for value in lap_numbers:
            if isinstance(value, float) and not value.is_integer():
                raise ValueError(f"Invalid value {value} in `lap_numbers`")
            numbers.append(int(value))
        dtype = self.schema["LapNumber"]
        if dtype.is_integer() or dtype.is_float():
            # A null reads as NaN, which matches no number.
            values = self.get_column("LapNumber").to_numpy()
            return self._rewrap(_filter_rows(self, np.isin(values, numbers)))
        return self._rewrap(self.filter(column.is_in(numbers)))

    def pick_team(self, name: str) -> Self:
        """Laps of one team (fastf1's exact ``Team == name``)."""
        if isinstance(name, str):
            keep = _mask_values(self.get_column("Team").cast(pl.String) == name)
            return self._rewrap(_filter_rows(self, keep))
        return self._rewrap(self.filter(pl.col("Team").cast(pl.String) == name))

    def pick_teams(self, names: Any) -> Self:
        """Laps of the given teams: a name or any iterable of names, matched exactly."""
        if isinstance(names, str):
            return self.pick_team(names)
        wanted = [str(name) for name in names]
        keep = _isin_values(self.get_column("Team").cast(pl.String), wanted)
        return self._rewrap(_filter_rows(self, keep))

    def pick_fastest(self, only_by_time: bool = False) -> "PolarsLap | None":
        """The lap with the lowest ``LapTime`` (fastf1 parity), or ``None``.

        By default only laps marked ``IsPersonalBest`` are candidates (a lap
        that is not a personal best did not count, e.g. a deleted time);
        ``only_by_time=True`` takes every lap. A frame without an
        ``IsPersonalBest`` column falls back to the laps not ``Deleted``. Laps
        without a lap time never are candidates; ties go to the first lap.
        """
        if only_by_time:
            return self._fastest_lap("time")
        return self._fastest_lap(
            "personal_best" if "IsPersonalBest" in self.columns else "not_deleted"
        )

    def _fastest_lap(self, rule: str) -> "PolarsLap | None":
        """The first lap with the lowest ``LapTime`` among the ``rule`` candidates.

        Args:
            rule: ``"personal_best"`` (``IsPersonalBest`` set), ``"not_deleted"``
                (``Deleted`` not set; tif1's ``Driver.fastest_lap``) or
                ``"time"`` (every lap with a lap time).
        """
        if self.height == 0 or "LapTime" not in self.columns:
            return None
        lap_time = self.get_column("LapTime")
        valid = _valid_values(lap_time)
        if rule == "personal_best" and "IsPersonalBest" in self.columns:
            flag = self.get_column("IsPersonalBest").cast(pl.Boolean, strict=False)
            valid = valid & _mask_values(flag)
        elif rule == "not_deleted" and "Deleted" in self.columns:
            flag = self.get_column("Deleted").cast(pl.Boolean, strict=False)
            valid = valid & ~_mask_values(flag)
        if valid.all():
            position = int(cast(int, lap_time.arg_min()))
        else:
            positions = np.flatnonzero(valid)
            if positions.size == 0:
                return None
            times = lap_time.filter(pl.Series(valid))
            position = int(positions[int(cast(int, times.arg_min()))])
        return PolarsLap(self.slice(position, 1), session=self.session, name=position)

    def pick_quicklaps(self, threshold: float | None = None) -> Self:
        """Laps within ``threshold`` (default :attr:`QUICKLAP_THRESHOLD`) of the best."""
        if threshold is None:
            threshold = self.QUICKLAP_THRESHOLD
        if self.height == 0 or "LapTime" not in self.columns:
            return self
        lap_time_ns = _ns_series(self, "LapTime")
        best = lap_time_ns.min()
        if best is None:
            return self
        # Int64 against a number: compared as float64, as polars does; a null
        # (NaN here) is never quick.
        keep = lap_time_ns.to_numpy() < cast(int, best) * threshold
        return self._rewrap(_filter_rows(self, keep))

    def pick_tyre(self, compound: str) -> Self:
        """Laps on one compound (upper-cased, as fastf1 does)."""
        keep = _mask_values(self.get_column("Compound").cast(pl.String) == str(compound).upper())
        return self._rewrap(_filter_rows(self, keep))

    def pick_compounds(self, compounds: Any) -> Self:
        """Laps on the given compounds: a name or any iterable of names, upper-cased (fastf1)."""
        if isinstance(compounds, str):
            return self.pick_tyre(compounds)
        wanted = [str(compound).upper() for compound in compounds]
        keep = _isin_values(self.get_column("Compound").cast(pl.String), wanted)
        return self._rewrap(_filter_rows(self, keep))

    def pick_track_status(self, status: Any, how: str = "equals") -> Self:
        """Laps by ``TrackStatus`` code string (fastf1 semantics).

        Args:
            status: Status code(s), e.g. ``"2"`` or ``"26"``.
            how: ``"equals"`` (exact), ``"contains"`` (substring),
                ``"excludes"`` (no substring), ``"any"`` (any character of
                ``status``) or ``"none"`` (no character of ``status``).
        """
        status = str(status)
        if how in ("equals", "contains", "excludes") or (
            how in ("any", "none") and status.isalnum() and status.isascii()
        ):
            values = self.get_column("TrackStatus").cast(pl.String)
            if how == "equals":
                keep = _mask_values(values == status)
            elif how in ("contains", "excludes"):
                keep = _text_test_values(values, lambda value: status in value)
            else:  # any character of ``status`` (a regex class of literal characters)
                keep = _text_test_values(values, lambda value: any(ch in value for ch in status))
            if how in ("excludes", "none"):
                keep = ~keep  # a null status is kept, as ~fill_null(False) keeps it
            return self._rewrap(_filter_rows(self, keep))
        column = pl.col("TrackStatus").cast(pl.String)
        if how == "equals":
            expr = column == status
        elif how == "contains":
            expr = column.str.contains(status, literal=True).fill_null(False)
        elif how == "excludes":
            expr = ~column.str.contains(status, literal=True).fill_null(False)
        elif how == "any":
            expr = column.str.contains("|".join(re.escape(ch) for ch in status)).fill_null(False)
        elif how == "none":
            expr = ~column.str.contains("|".join(re.escape(ch) for ch in status)).fill_null(False)
        else:
            raise ValueError(f"Invalid value '{how}' for kwarg 'how'")
        return self._rewrap(self.filter(expr))

    def pick_wo_box(self) -> Self:
        """Laps that are neither in-laps nor out-laps."""
        if "PitInTime" not in self.columns or "PitOutTime" not in self.columns:
            return self
        keep = ~_present_values(self.get_column("PitInTime")) & ~_present_values(
            self.get_column("PitOutTime")
        )
        return self._rewrap(_filter_rows(self, keep))

    def pick_box_laps(self, which: str = "both") -> Self:
        """In-laps (``"in"``), out-laps (``"out"``) or either (``"both"``)."""
        if "PitInTime" not in self.columns or "PitOutTime" not in self.columns:
            return self
        if which == "in":
            keep = _present_values(self.get_column("PitInTime"))
        elif which == "out":
            keep = _present_values(self.get_column("PitOutTime"))
        elif which == "both":
            keep = _present_values(self.get_column("PitInTime")) | _present_values(
                self.get_column("PitOutTime")
            )
        else:
            raise ValueError(f"Invalid value '{which}' for kwarg 'which'")
        return self._rewrap(_filter_rows(self, keep))

    def pick_not_deleted(self) -> Self:
        """Laps whose time was not deleted (an unknown flag counts as not deleted)."""
        if "Deleted" not in self.columns:
            return self
        deleted = self.get_column("Deleted").cast(pl.Boolean, strict=False)
        return self._rewrap(_filter_rows(self, ~_mask_values(deleted)))

    def pick_accurate(self) -> Self:
        """Laps flagged ``IsAccurate``."""
        if "IsAccurate" not in self.columns:
            return self
        accurate = self.get_column("IsAccurate")
        if accurate.dtype == pl.Boolean:
            return self._rewrap(_filter_rows(self, _mask_values(accurate)))
        return self._rewrap(self.filter(pl.col("IsAccurate").fill_null(False)))

    # -- qualifying ---------------------------------------------------------

    def _segment(self, keep: np.ndarray) -> "PolarsLaps | None":
        if not keep.any():
            return None
        return self._rewrap(_filter_rows(self, keep))

    def split_qualifying_sessions(self) -> "list[PolarsLaps | None]":
        """``[Q1, Q2, Q3]`` laps (fastf1 parity; ``None`` for a segment without laps).

        The ``QualifyingSession`` column (``"Q1"``.. or any value ending in
        1/2/3) decides when present. Otherwise fastf1's algorithm runs on
        ``session.session_status``: each ``Started`` row opens a segment,
        except the restart after an ``Aborted`` (red flag) row, which
        continues it (``Finished`` also clears the suspension); a lap belongs
        to the segment its ``LapStartTime`` falls in, and an out-lap that
        starts before a split and ends after it belongs to the next segment.
        Without session status the two longest gaps of more than five minutes
        between lap starts split the laps.

        Raises:
            ValueError: The laps belong to a session that is not
                qualifying-like (Qualifying, Sprint Qualifying, Sprint
                Shootout).
        """
        _require_qualifying_session(self.session)
        if "QualifyingSession" in self.columns:
            labels = self.get_column("QualifyingSession").cast(pl.String)
            marks = _segment_marks(labels.to_list())
            if marks is None:  # non-ASCII labels: polars' own case mapping and trimming
                marks = labels.to_frame().select(
                    pl.col("QualifyingSession")
                    .str.to_uppercase()
                    .str.strip_chars()
                    .str.extract(r"([123])$", 1)
                )
                marks = marks.to_series().to_list()
            if any(mark is not None for mark in marks):
                return [
                    self._segment(np.fromiter((m == str(i) for m in marks), bool, len(marks)))
                    for i in (1, 2, 3)
                ]
        has_start = "LapStartTime" in self.columns
        starts = _ns_series(self, "LapStartTime") if has_start else None
        split_times = _qualifying_split_times(self.session, starts)
        if split_times is None:
            return [self if self.height else None, None, None]
        none = np.zeros(self.height, dtype=bool)
        # Each bound as (int64 ns, present); a comparison with a missing
        # bound is never true (``fill_null(False)``).
        start, has_start_ns = self._lap_ns("LapStartTime" if has_start else None)
        end, has_end = self._lap_ns("Time" if "Time" in self.columns else None)
        pit_out = (
            _present_values(self.get_column("PitOutTime")) if "PitOutTime" in self.columns else none
        )
        segments: list[PolarsLaps | None] = [None, None, None]
        previous_early: np.ndarray | None = None
        for i in range(min(3, len(split_times) - 1)):
            low, high = split_times[i], split_times[i + 1]
            if low is None or high is None:
                cond = none
            else:
                cond = has_start_ns & (start > low) & (start < high)
            if previous_early is not None:
                cond = cond | previous_early
            if high is None:
                is_early = none
            else:
                # An out-lap that crossed the pit timing line before the split
                # and ends after it belongs to the next segment (fastf1).
                is_early = has_start_ns & has_end & (start < high) & (end > high) & pit_out
            segments[i] = self._segment(cond & ~is_early)
            previous_early = is_early
        return segments

    def _lap_ns(self, column: str | None) -> tuple[np.ndarray, np.ndarray]:
        """``column`` as ``(int64 ns, present)`` (all missing for ``None`` or a text column)."""
        if column is not None:
            values = _ns_series(self, column)
            if values.len() == self.height:
                return _int_values(values)
        return np.zeros(self.height, dtype=np.int64), np.zeros(self.height, dtype=bool)

    # -- iteration ----------------------------------------------------------

    def iterlaps(self, require: Iterable[str] | None = None) -> Iterator[IterLapResult]:
        """Yield ``(index, PolarsLap)`` for the laps in this frame (fastf1 parity).

        Args:
            require: Columns that must exist and hold a value (not null/NaN)
                for a lap to be yielded. ``None`` or empty yields every lap; a
                column the frame lacks yields none (fastf1).

        Yields:
            ``(index, lap)`` with ``index`` the row position in this frame.
        """
        required = [] if require is None else list(require)
        if any(column not in self.columns for column in required):
            return
        session = self.session
        positions: Iterable[int] = range(self.height)
        if required and self.height:
            keep = _valid_values(self.get_column(required[0]))
            for column in required[1:]:
                keep = keep & _valid_values(self.get_column(column))
            if not keep.all():
                positions = np.flatnonzero(keep).tolist()
        rows = self.rows(named=True)
        for position in positions:
            yield IterLapResult(
                position,
                PolarsLap(
                    self.slice(position, 1), session=session, name=position, row=rows[position]
                ),
            )

    # -- telemetry ----------------------------------------------------------

    def _telemetry_refs(self) -> list[tuple[str, int]]:
        drivers = self.get_column("Driver").cast(pl.String).to_list()
        numbers = self.get_column("LapNumber").cast(pl.Int64, strict=False).to_list()
        return [
            (str(driver), int(number))
            for driver, number in zip(drivers, numbers, strict=True)
            if driver is not None and number is not None
        ]

    def _lap_telemetry_frames(self, refs: list[tuple[str, int]]) -> list[pl.DataFrame]:
        """The non-empty telemetry frames of ``refs``, in ``refs`` order.

        A tif1 session answers the whole list in one batch (memo, frame tier,
        payload tier, then one parallel network wave for the rest); other
        session objects are asked lap by lap.
        """
        session = self.session
        assert session is not None
        batch = getattr(session, "_telemetry_frames_for_refs", None)
        if callable(batch):
            by_ref = batch(refs)
            batched: list[pl.DataFrame] = []
            for ref in refs:
                tel = by_ref.get(ref)
                if tel is None:
                    continue
                tel = _as_polars_frame(tel, "telemetry of a polars session")
                if tel.height:
                    batched.append(tel)
            return batched
        ultra_cold = session._resolve_telemetry_ultra_cold_mode(None)
        frames: list[pl.DataFrame] = []
        for driver, lap_number in refs:
            try:
                tel = session._get_telemetry_df_for_ref(
                    driver, lap_number, ultra_cold=ultra_cold, allow_prefetch=False
                )
            except _TELEMETRY_ERRORS as error:
                session._record_telemetry_failure(driver, lap_number, error)
                continue
            if tel is None:
                continue
            tel = _as_polars_frame(tel, "telemetry of a polars session")
            if tel.height:
                frames.append(tel)
        return frames

    def _concat_telemetry(
        self, frames: list[pl.DataFrame], driver: str | None, *, multi_driver: bool = False
    ) -> PolarsTelemetry:
        """The lap frames as one multi-lap slice (``_join_lap_frames``, pandas parity)."""
        if not frames:
            return PolarsTelemetry._empty(self.session, driver)
        if len(frames) == 1:
            return PolarsTelemetry._from_frame(frames[0], self.session, driver)
        try:
            # vstack accepts only the same names, order and dtypes, where the
            # relaxed concat is a plain stack (comparing the schemas in
            # Python costs ~0.25 ms per frame).
            merged = _stack_rows(frames)
        except pl.exceptions.PolarsError:
            merged = pl.concat(frames, how="diagonal_relaxed")
        start = None if multi_driver else _first_lap_start_ns(self)
        merged = _join_lap_frames(merged, start, multi_driver=multi_driver)
        return PolarsTelemetry._from_frame(merged, self.session, driver)

    @property
    def telemetry(self) -> PolarsTelemetry:
        """Telemetry of every lap in this frame (one driver), laps concatenated.

        Raises:
            ValueError: The frame holds laps of more than one driver, or the
                laps come from ``df.f1`` on a frame without a session.
        """
        self._require_session("telemetry")
        if self.height == 0 or self.session is None:
            return PolarsTelemetry._empty(self.session, None)
        count, code = _distinct_present(self.get_column("Driver"))
        if count > 1:
            raise ValueError("Cannot retrieve telemetry for multiple drivers.")
        driver = str(code) if count else None
        refs = self._telemetry_refs()
        cached = getattr(self, "_telemetry_cache", None)
        if cached is not None and cached[0] == refs:
            return cached[1]
        tel = self._concat_telemetry(self._lap_telemetry_frames(refs), driver)
        self._telemetry_cache = (refs, tel)
        return tel

    def get_telemetry(self, *, frequency: int | str | None = None) -> PolarsTelemetry:
        """:attr:`telemetry` with the driver-ahead channels, optionally resampled.

        Args:
            frequency: ``None`` (:attr:`PolarsTelemetry.TELEMETRY_FREQUENCY`),
                ``"original"`` (as built) or a sampling rate in Hz.
        """
        tel = self.telemetry.add_driver_ahead()
        if frequency is None:
            frequency = PolarsTelemetry.TELEMETRY_FREQUENCY
        rule = _frequency_rule(frequency)
        return tel if rule is None else tel.resample_channels(rule=rule)

    def _padded_car_data(self, kwargs: Mapping[str, Any]) -> PolarsTelemetry | None:
        """The driver's session-long telemetry sliced to these laps for ``pad`` or edges.

        fastf1 slices ``session.car_data[driver]``; tif1 keeps one frame per
        lap, so a pad reaching into the neighbouring laps, or an interpolated
        edge sample, needs the driver's whole timeline. ``None`` when neither
        is requested or the session has no ``car_data`` (the per-lap frames
        answer).
        """
        if (
            not (kwargs.get("pad") or kwargs.get("interpolate_edges"))
            or self.session is None
            or self.height == 0
        ):
            return None
        car_data = getattr(self.session, "car_data", None)
        if car_data is None:
            return None
        count, code = _distinct_present(self.get_column("Driver"))
        if count != 1:
            raise ValueError("Cannot retrieve telemetry for multiple drivers.")
        driver = str(code)
        try:
            full = car_data[driver]
        except KeyError:
            return None
        if not isinstance(full, pl.DataFrame) or full.height == 0:
            return None
        if not isinstance(full, PolarsTelemetry):
            full = PolarsTelemetry._from_frame(full, self.session, driver)
        return full.slice_by_lap(
            self,
            pad=kwargs.get("pad", 0),
            pad_side=kwargs.get("pad_side", "both"),
            interpolate_edges=bool(kwargs.get("interpolate_edges", False)),
        )

    def get_car_data(self, **kwargs: Any) -> PolarsTelemetry:
        """Telemetry of the laps (tif1 frames are merged car+position data).

        Laps of several drivers are concatenated driver by driver. ``pad``,
        ``pad_side`` and ``interpolate_edges`` slice the driver's session-long
        ``car_data`` around the laps (fastf1 parity; one driver only).
        """
        self._require_session("get_car_data")
        if self.height == 0:
            return PolarsTelemetry._empty(self.session, None)
        padded = self._padded_car_data(kwargs)
        if padded is not None:
            return padded
        try:
            return self.telemetry
        except ValueError:
            if self.session is None:
                return PolarsTelemetry._empty(None, None)
            frames = self._lap_telemetry_frames(self._telemetry_refs())
            return self._concat_telemetry(frames, None, multi_driver=True)

    def get_pos_data(self, **kwargs: Any) -> PolarsTelemetry:
        """Position data of the laps (the same merged frame as :meth:`get_car_data`)."""
        return self.get_car_data(**kwargs)

    def get_weather_data(self) -> pl.DataFrame:
        """One weather sample per lap, in this frame's row order (fastf1 parity).

        fastf1's rule on ``session.weather_data``: the first sample within
        ``[LapStartTime, Time]``, else the last sample at or before the lap's
        ``Time``, else all-null (a lap without ``LapStartTime`` takes the last
        sample before its end). Columns are ``Time`` (the sample's session
        time, ``Duration("ns")``) then the seven channels. Without a session
        weather table the laps' own ``WeatherTime`` and channel columns
        answer; without those every sample is null.
        """
        weather = _session_weather_table(self.session)
        if weather is not None:
            starts, ends = self._weather_bounds()
            return _weather_sample_frame(weather, _weather_sample_positions(weather, starts, ends))
        if all(column in self.columns for column in ("WeatherTime", *WEATHER_COLUMNS)):
            return pl.DataFrame(
                [
                    self.get_column("WeatherTime").cast(_DURATION_NS).alias("Time"),
                    *(self.get_column(column) for column in WEATHER_COLUMNS),
                ]
            )
        return _missing_weather_frame(self.height)

    def _weather_bounds(self) -> tuple[pl.Series, pl.Series]:
        """Each lap's ``(LapStartTime, Time)`` in ns (null when missing).

        A frame without a ``Time`` column ends its laps at
        ``LapStartTime + LapTime``.
        """
        height = self.height

        def clock(column: str) -> pl.Series:
            values = _ns_series(self, column) if column in self.columns else None
            if values is None or values.len() != height:  # missing, or not a clock
                return _full_series(column, None, height, pl.Int64)
            return values

        start = clock("LapStartTime")
        if "Time" in self.columns:
            end = clock("Time")
        elif "LapStartTime" in self.columns and "LapTime" in self.columns:
            end = start + clock("LapTime")
        else:
            end = _full_series("end", None, height, pl.Int64)
        return start.alias("start"), end.alias("end")


# --------------------------------------------------------------------------- #
# Row wrappers
# --------------------------------------------------------------------------- #


class _PolarsRow:
    """One row of a tif1 polars frame with scalar access.

    ``row["LapTime"]``, ``row.LapTime`` and ``row.get("LapTime")`` read the
    row's values (read once with ``row(0, named=True)``); ``row.frame`` is
    the one-row frame of the owning class. ``name`` is the row's position in
    the frame it was taken from (``None`` when unknown).
    """

    _frame_class: ClassVar[type[Any]]

    def __init__(
        self,
        frame: pl.DataFrame,
        session: Any = None,
        name: int | None = None,
        *,
        row: Mapping[str, Any] | None = None,
    ) -> None:
        frame = _as_polars_frame(frame, f"{type(self).__name__} frame")
        if frame.height != 1:
            raise ValueError(f"{type(self).__name__} wraps exactly one row, got {frame.height}")
        frame_class = self._frame_class
        if not isinstance(frame, frame_class) or frame.session is not session:
            frame = frame_class._from_frame(frame, session)
        self._frame = frame
        self._row: dict[str, Any] = dict(row) if row is not None else frame.row(0, named=True)
        self.session = session
        self.name = name

    @property
    def frame(self) -> Any:
        """The one-row frame (``PolarsLaps``/``PolarsSessionResults``)."""
        return self._frame

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            return self._row[key]
        if isinstance(key, list | tuple):
            return {name: self._row[name] for name in key}
        raise KeyError(key)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        row = self.__dict__.get("_row")
        if row is not None and name in row:
            return row[name]
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    def __contains__(self, key: object) -> bool:
        return key in self._row

    def __iter__(self) -> Iterator[str]:
        return iter(self._row)

    def __len__(self) -> int:
        return len(self._row)

    def get(self, key: str, default: Any = None) -> Any:
        """Value of column ``key`` or ``default``."""
        return self._row.get(key, default)

    def keys(self) -> Any:
        return self._row.keys()

    def values(self) -> Any:
        return self._row.values()

    def items(self) -> Any:
        return self._row.items()

    def to_dict(self) -> dict[str, Any]:
        """The row as a new ``dict`` (column -> value)."""
        return dict(self._row)

    def __repr__(self) -> str:
        values = ", ".join(f"{key}={value!r}" for key, value in self._row.items())
        return f"{type(self).__name__}(name={self.name!r}, {values})"


class PolarsLap(_PolarsRow):
    """One lap (a one-row :class:`PolarsLaps`) with telemetry access."""

    _frame_class = PolarsLaps

    @property
    def driver(self) -> str | None:
        """Driver code of the lap."""
        value = self._row.get("Driver")
        return None if value is None else str(value)

    @property
    def lap_number(self) -> int | None:
        """Lap number (``None`` when missing)."""
        value = self._row.get("LapNumber")
        if _is_missing_number(value):
            return None
        return int(cast(Any, value))

    @property
    def telemetry(self) -> PolarsTelemetry:
        """Telemetry of this lap from the session (empty frame without one or on failure).

        Raises:
            ValueError: The lap comes from ``df.f1`` on a frame without a session.
        """
        self._frame._require_session("telemetry")
        driver = self.driver
        lap_number = self.lap_number
        session = self.session
        if not driver or lap_number is None or session is None:
            return PolarsTelemetry._empty(session, driver)
        try:
            tel = session._get_telemetry_df_for_ref(
                driver,
                lap_number,
                ultra_cold=session._resolve_telemetry_ultra_cold_mode(None),
                allow_prefetch=False,
            )
        except _TELEMETRY_ERRORS as error:
            session._record_telemetry_failure(driver, lap_number, error)
            return PolarsTelemetry._empty(session, driver)
        if tel is None:
            return PolarsTelemetry._empty(session, driver)
        tel = _as_polars_frame(tel, "telemetry of a polars session")
        if isinstance(tel, PolarsTelemetry) and tel.session is session and tel.driver == driver:
            return tel
        return PolarsTelemetry._from_frame(tel, session, driver)

    def get_telemetry(self, *, frequency: int | str | None = None) -> PolarsTelemetry:
        """:attr:`telemetry` with the driver-ahead channels, optionally resampled (Hz)."""
        tel = self.telemetry.add_driver_ahead()
        if frequency is None:
            frequency = PolarsTelemetry.TELEMETRY_FREQUENCY
        rule = _frequency_rule(frequency)
        return tel if rule is None else tel.resample_channels(rule=rule)

    def get_car_data(self, **kwargs: Any) -> PolarsTelemetry:
        """Telemetry of the lap (merged car+position data).

        ``pad``, ``pad_side`` and ``interpolate_edges`` slice the driver's
        session-long ``car_data`` around the lap (see
        :meth:`PolarsLaps.get_car_data`).
        """
        padded = self._frame._padded_car_data(kwargs) if kwargs.get("pad") else None
        return self.telemetry if padded is None else padded

    def get_pos_data(self, **kwargs: Any) -> PolarsTelemetry:
        """Position data of the lap (the same merged frame as :meth:`get_car_data`)."""
        return self.get_car_data(**kwargs)

    def get_weather_data(self) -> dict[str, Any]:
        """The lap's weather sample as a dict: ``Time`` plus the seven channels (fastf1 parity).

        The rule of :meth:`PolarsLaps.get_weather_data` (``session.weather_data``
        first, else the lap's own ``WeatherTime``/channel values); every value
        is ``None`` when there is no sample.
        """
        weather = _session_weather_table(self.session)
        if weather is None:
            return self._frame.get_weather_data().row(0, named=True)
        row = self._row
        start = _scalar_ns(row.get("LapStartTime"))
        if "Time" in row or "LapTime" not in row:
            end = _scalar_ns(row.get("Time"))
        else:
            lap_time = _scalar_ns(row.get("LapTime"))
            end = None if start is None or lap_time is None else start + lap_time
        position = _weather_sample_row(_ns_series(weather, "Time").to_list(), start, end)
        if position is None:
            return dict.fromkeys(_WEATHER_SAMPLE_SCHEMA)
        sample = weather.row(position, named=True)
        return {key: sample.get(key) for key in _WEATHER_SAMPLE_SCHEMA}


# --------------------------------------------------------------------------- #
# Results
# --------------------------------------------------------------------------- #


class PolarsSessionResults(_TIF1Frame):
    """Session classification (fastf1's 22 columns first, tif1 extras last).

    ``DriverNumber`` is a column (polars has no index); :meth:`row_for`
    answers the ``results.loc[identifier]`` lookups.
    """

    _IDENTIFIER_COLUMNS: ClassVar[tuple[str, ...]] = (
        "DriverNumber",
        "Abbreviation",
        "FullName",
        "BroadcastName",
    )

    @classmethod
    def _from_frame(cls, df: pl.DataFrame, session: Any = None) -> Self:
        """Wrap ``df`` (no data copy) with ``session``."""
        obj = cls._adopt(df)
        obj.session = session
        return obj

    def _position_for(self, identifier: Any) -> int | None:
        key = _driver_identifier(identifier).strip()
        if not key:
            # A blank identifier names no driver (it would match blank names).
            return None
        matches: np.ndarray | None = None
        upper, lower = key.upper(), key.lower()
        for column in self._IDENTIFIER_COLUMNS:
            if column not in self.columns:
                continue
            text = self.get_column(column).cast(pl.String)
            if column == "Abbreviation":
                hits = _text_test_values(text, lambda value: value.upper() == upper)
            elif column == "DriverNumber":
                hits = _mask_values(text == key)
            else:
                hits = _text_test_values(text, lambda value: value.lower() == lower)
            matches = hits if matches is None else matches | hits
        if matches is None:
            return None
        positions = np.flatnonzero(matches)
        if positions.size:
            return int(positions[0])
        number, separator, code = key.partition("/")
        if (
            separator
            and number.isdigit()
            and code.strip()
            and {"DriverNumber", "Abbreviation"} <= set(self.columns)
        ):
            # A shared car's later driver ("number/Abbreviation", the pandas
            # index label) against the raw car number polars keeps.
            wanted = code.strip().upper()
            label = _mask_values(self.get_column("DriverNumber").cast(pl.String) == number)
            label = label & _text_test_values(
                self.get_column("Abbreviation").cast(pl.String),
                lambda value: value.upper() == wanted,
            )
            positions = np.flatnonzero(label)
            if positions.size:
                return int(positions[0])
        return None

    def row_for(self, identifier: Any) -> "PolarsDriverResult":
        """The result row of a driver.

        Args:
            identifier: Car number (int or str), abbreviation, a shared-car
                label (``"2/MOS"``) or the full/broadcast name.

        Raises:
            DriverNotFoundError: No row matches.
        """
        position = self._position_for(identifier)
        if position is None:
            raise DriverNotFoundError(
                _driver_identifier(identifier), **_session_context(self.session)
            )
        return PolarsDriverResult(
            self.slice(position, 1), session=self.session, name=self._label_for(position)
        )

    def _label_for(self, position: int) -> str | None:
        """Row ``position``'s label in the pandas ``results`` index (``None`` without a number).

        The car number, made unique as the pandas index is: a shared car's
        later drivers are ``number/Abbreviation`` (``unique_driver_labels``).
        """
        if "DriverNumber" not in self.columns or not 0 <= position < self.height:
            return None
        numbers = self.get_column("DriverNumber").cast(pl.String)
        number = numbers[position]
        if number is None:
            return None
        if not (numbers.head(position) == number).any():
            return number
        codes = (
            self.get_column("Abbreviation").cast(pl.String).head(position + 1).to_list()
            if "Abbreviation" in self.columns
            else [None] * (position + 1)
        )
        entries = zip(numbers.head(position + 1).to_list(), codes, strict=True)
        return _unique_driver_labels([(n or "", c or "") for n, c in entries])[-1]

    def get_driver(self, identifier: Any) -> "PolarsDriverResult":
        """Alias of :meth:`row_for`."""
        return self.row_for(identifier)


class PolarsDriverResult(_PolarsRow):
    """One driver's classification row with scalar access.

    ``name`` is the row's label in the pandas ``results`` index, as fastf1's
    ``DriverResult.name``: the car number (``"1"``), ``number/Abbreviation``
    for a shared car's later driver.
    """

    _frame_class = PolarsSessionResults

    def __init__(
        self,
        frame: pl.DataFrame,
        session: Any = None,
        name: int | str | None = None,
        *,
        row: Mapping[str, Any] | None = None,
    ) -> None:
        """Wrap a one-row results frame.

        Args:
            frame: The row.
            session: The session.
            name: The row's label; a row position in ``session.results`` (or
                ``None``) is turned into the label of that row.
            row: The row's values when already read.
        """
        super().__init__(frame, session, None, row=row)
        self.name = name if isinstance(name, str) else self._result_label(name)

    def _result_label(self, position: int | None) -> str | int | None:
        """The pandas index label of this row (``position`` when it has no car number)."""
        number = self._row.get("DriverNumber")
        if number is None or str(number) == "":
            return position
        number = str(number)
        if position is None or self.session is None:
            return number
        try:
            results = getattr(self.session, "results", None)
        except (DataNotFoundError, InvalidDataError, NetworkError, KeyError, TypeError):
            return number
        if not isinstance(results, pl.DataFrame) or not 0 <= position < results.height:
            return number
        table = (
            results
            if isinstance(results, PolarsSessionResults)
            else PolarsSessionResults._from_frame(results, self.session)
        )
        label = table._label_for(position)
        # The position must name this row (same car number) to borrow its label.
        return label if label is not None and label.partition("/")[0] == number else number

    @property
    def dnf(self) -> bool:
        """True when the driver did not finish (status other than ``Finished``/``+N Lap(s)``).

        ``Lapped`` counts as a finish, ``Not classified`` as a DNF; an empty
        or non-string status is not a DNF.
        """
        status = self._row.get("Status", "")
        if not isinstance(status, str) or not status.strip():
            return False
        return _CLASSIFIED_STATUS_RE.match(status.strip()) is None


def _empty_laps_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "Driver": pl.String,
            "DriverNumber": pl.String,
            "LapNumber": pl.Int64,
            "LapTime": _DURATION_NS,
        }
    )


class PolarsDriver(PolarsDriverResult):
    """A driver of a polars session: the result row plus laps and telemetry.

    Attributes:
        driver: Driver code (abbreviation).
        session: The session.
    """

    def __init__(
        self,
        frame: pl.DataFrame,
        session: Any = None,
        name: int | str | None = None,
        *,
        driver: str | None = None,
        row: Mapping[str, Any] | None = None,
        prefetched_lap_data: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(frame, session, name, row=row)
        code = driver if driver else self._row.get("Abbreviation")
        self.driver: str = "" if code is None else str(code)
        self._laps: PolarsLaps | None = None
        # The driver's laptime payload when get_driver already fetched it.
        self._prefetched_lap_data = prefetched_lap_data

    @classmethod
    def from_session(cls, session: Any, driver: str) -> Self:
        """Build the driver from ``session.results`` (or ``_get_driver_info`` without a row)."""
        results = getattr(session, "results", None)
        if (
            isinstance(results, pl.DataFrame)
            and results.height
            and "Abbreviation" in results.columns
        ):
            table = (
                results
                if isinstance(results, PolarsSessionResults)
                else PolarsSessionResults._from_frame(results, session)
            )
            position = table._position_for(driver)
            if position is not None:
                row = table.slice(position, 1)
                code = row.get_column("Abbreviation")[0]
                label = table._label_for(position)
                return cls(row, session=session, name=label, driver=str(code or driver))
        info = session._get_driver_info(driver)
        first_name = str(info.get("fn", "") or "")
        last_name = str(info.get("ln", "") or "")
        data: dict[str, Any] = {
            "DriverNumber": str(info.get("dn", "") or ""),
            "Abbreviation": driver,
            "TeamName": str(info.get("team", "") or ""),
            "TeamColor": str(info.get("tc", "") or ""),
            "FirstName": first_name,
            "LastName": last_name,
            "FullName": f"{first_name} {last_name}".strip(),
            "HeadshotUrl": str(info.get("headshot_url", info.get("url", "")) or ""),
        }
        ergast_identity = getattr(session, "_ergast_identity", None)
        if callable(ergast_identity):
            data.update(ergast_identity(data["DriverNumber"], driver))
        frame = pl.DataFrame({key: [value] for key, value in data.items()})
        return cls(frame, session=session, driver=driver)

    @property
    def driver_number(self) -> str:
        """Car number label (``"1"``, or ``"2/MOS"`` for a shared car)."""
        value = self._row.get("DriverNumber")
        return "" if value is None else str(value)

    @property
    def laps(self) -> PolarsLaps:
        """The driver's laps, memoised.

        A tif1 session loads only this driver's laps when the session table
        is not loaded yet (``Session._driver_laps_frame``); other session
        objects answer with ``session.laps.pick_driver(code)``.
        """
        laps = self._laps
        loader = getattr(self.session, "_driver_laps_frame", None) if self.session else None
        if laps is None and callable(loader):
            prefetched, self._prefetched_lap_data = self._prefetched_lap_data, None
            laps = loader(self.driver, prefetched)
            if not isinstance(laps, PolarsLaps):
                laps = PolarsLaps._from_frame(_as_polars_frame(laps, "driver laps"), self.session)
            self._laps = laps
        if laps is None:
            source = getattr(self.session, "laps", None) if self.session is not None else None
            if not isinstance(source, pl.DataFrame) or "Driver" not in source.columns:
                source = _empty_laps_frame()
            table = (
                source
                if isinstance(source, PolarsLaps) and source.session is self.session
                else PolarsLaps._from_frame(source, self.session)
            )
            laps = table.pick_driver(self.driver)
            self._laps = laps
        return laps

    def get_lap(self, lap_number: int) -> PolarsLap:
        """The driver's lap ``lap_number``.

        Raises:
            LapNotFoundError: The driver has no such lap.
        """
        if isinstance(lap_number, bool) or not isinstance(lap_number, int) or lap_number < 1:
            raise ValueError(f"lap_number must be a positive integer, got {lap_number!r}")
        laps = self.laps
        positions = (
            np.flatnonzero(_mask_values(laps.get_column("LapNumber") == lap_number))
            if "LapNumber" in laps.columns
            else np.zeros(0, dtype=np.int64)
        )
        if positions.size == 0:
            raise LapNotFoundError(
                lap_number=lap_number, driver=self.driver, **_session_context(self.session)
            )
        position = int(positions[0])
        return PolarsLap(laps.slice(position, 1), session=self.session, name=position)

    def get_fastest_lap(self) -> PolarsLaps:
        """The driver's fastest non-deleted lap as a one-row frame (empty when none).

        The pandas ``Driver.get_fastest_lap`` rule (``Deleted`` laps never
        count), not ``pick_fastest``'s personal-best rule.
        """
        laps = self.laps
        fastest = laps._fastest_lap("not_deleted")
        if fastest is None:
            return laps._rewrap(laps.clear())
        return fastest.frame

    @property
    def fastest_lap(self) -> PolarsLap | None:
        """The driver's fastest non-deleted lap (``None`` when there is none)."""
        return self.laps._fastest_lap("not_deleted")

    @property
    def telemetry(self) -> PolarsTelemetry:
        """Telemetry of every lap of the driver."""
        return self.laps.telemetry

    def get_telemetry(self, *, frequency: int | str | None = None) -> PolarsTelemetry:
        """:meth:`PolarsLaps.get_telemetry` of the driver's laps."""
        return self.laps.get_telemetry(frequency=frequency)

    def get_fastest_lap_tel(self) -> PolarsTelemetry:
        """Telemetry of the fastest lap (empty frame when there is none).

        Before the laps frame is built, a tif1 session names the fastest lap
        straight from the raw laptime payload (no laps frame is built), as the
        pandas ``Driver`` does.
        """
        session = self.session
        extract = getattr(session, "_extract_fastest_lap_candidate", None) if session else None
        if self._laps is None and callable(extract):
            payload = self._prefetched_lap_data
            if not isinstance(payload, Mapping):
                payload = session._get_or_derive_driver_laptime_payload(self.driver)
            candidate = extract(self.driver, payload)
            if candidate is not None:
                tel = session._get_telemetry_df_for_ref(
                    self.driver,
                    int(candidate[1]),
                    ultra_cold=session._resolve_ultra_cold_mode(None),
                )
            else:
                # No lap named by the payload: the session's fastest-lap batch
                # (as the pandas Driver; it answers at once for Ergast-only seasons).
                tel = session.get_fastest_laps_tels(by_driver=True, drivers=[self.driver])
            tel = _as_polars_frame(tel, "telemetry of a polars session")
            if isinstance(tel, PolarsTelemetry):
                return tel
            return PolarsTelemetry._from_frame(tel, session, self.driver)
        fastest = self.fastest_lap
        if fastest is None:
            return PolarsTelemetry._empty(self.session, self.driver or None)
        return fastest.telemetry


# --------------------------------------------------------------------------- #
# ``df.f1`` namespace
# --------------------------------------------------------------------------- #


def _own_public_names(cls: type) -> frozenset[str]:
    """Public methods and properties ``cls`` defines itself (not inherited)."""
    return frozenset(
        name
        for name, attribute in vars(cls).items()
        if not name.startswith("_")
        and (callable(attribute) or isinstance(attribute, property | classmethod | staticmethod))
    )


@pl.api.register_dataframe_namespace("f1")
class F1Namespace:
    """``df.f1``: tif1's laps/telemetry/results methods on any polars frame.

    Methods are dispatched to :class:`PolarsLaps`, :class:`PolarsTelemetry`
    or :class:`PolarsSessionResults` (in that order of lookup) after wrapping
    the frame; the session comes from the frame's ``_tif1_session`` attribute
    (set on every tif1 class instance, or by hand on a plain frame) or from
    a ``session=`` keyword (``driver=`` for telemetry) on the call. Explicit
    wrappers: :meth:`as_laps`, :meth:`as_telemetry`, :meth:`as_results`.
    """

    _DISPATCH: ClassVar[tuple[tuple[type[Any], frozenset[str], bool], ...]] = ()

    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    @property
    def session(self) -> Any:
        """The session attached to the frame (``None`` for an untagged plain frame)."""
        return getattr(self._df, "_tif1_session", None)

    def _resolve_session(self, session: Any) -> Any:
        return session if session is not None else self.session

    def as_laps(self, session: Any = None) -> PolarsLaps:
        """The frame as :class:`PolarsLaps`."""
        session = self._resolve_session(session)
        df = self._df
        if isinstance(df, PolarsLaps) and df.session is session:
            return df
        return PolarsLaps._from_frame(df, session)

    def as_telemetry(self, session: Any = None, driver: str | None = None) -> PolarsTelemetry:
        """The frame as :class:`PolarsTelemetry`."""
        session = self._resolve_session(session)
        df = self._df
        if isinstance(df, PolarsTelemetry):
            if driver is None:
                driver = df.driver
            if df.session is session and df.driver == driver:
                return df
        return PolarsTelemetry._from_frame(df, session, driver)

    def as_results(self, session: Any = None) -> PolarsSessionResults:
        """The frame as :class:`PolarsSessionResults`."""
        session = self._resolve_session(session)
        df = self._df
        if isinstance(df, PolarsSessionResults) and df.session is session:
            return df
        return PolarsSessionResults._from_frame(df, session)

    def _bind(self, name: str, wrap: Callable[..., Any], takes_driver: bool) -> Callable[..., Any]:
        def call(*args: Any, session: Any = None, **kwargs: Any) -> Any:
            if takes_driver:
                target = wrap(session=session, driver=kwargs.pop("driver", None))
            else:
                target = _mark_detached(wrap(session=session))
            _require_session_for_telemetry(target, name, args, kwargs)
            return getattr(target, name)(*args, **kwargs)

        call.__name__ = name
        return call

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        for cls, names, takes_driver in self._DISPATCH:
            if name not in names:
                continue
            attribute = vars(cls)[name]
            if takes_driver:
                wrap: Callable[..., Any] = self.as_telemetry
            elif cls is PolarsSessionResults:
                wrap = self.as_results
            else:
                wrap = self.as_laps
            if isinstance(attribute, property):
                target = wrap() if takes_driver else _mark_detached(wrap())
                _require_session_for_telemetry(target, name)
                return getattr(target, name)
            if isinstance(attribute, classmethod | staticmethod):
                return getattr(cls, name)
            return self._bind(name, wrap, takes_driver)
        raise AttributeError(f"'f1' namespace has no attribute {name!r}")


#: ``PolarsLaps`` members that fetch telemetry through the session.
_LAPS_TELEMETRY_MEMBERS = frozenset({"telemetry", "get_telemetry", "get_car_data", "get_pos_data"})


def _mark_detached(target: Any) -> Any:
    """``target`` flagged as session-less laps from ``df.f1`` (a new wrapper, no data copy).

    Laps and laps derived from them (``pick_*``, ``iterlaps``,
    ``split_qualifying_sessions``) then refuse telemetry calls, which could
    only answer with an empty frame (:meth:`PolarsLaps._require_session`).
    """
    if (
        isinstance(target, PolarsLaps)
        and target.session is None
        and target.height
        and not target._detached
    ):
        target = PolarsLaps._from_frame(target, None)
        target._detached = True
    return target


def _require_session_for_telemetry(
    target: Any, name: str, args: tuple[Any, ...] = (), kwargs: Mapping[str, Any] | None = None
) -> None:
    """Refuse a ``df.f1`` call that needs the session on a frame that carries none.

    A plain frame from ``filter``/``select``/``sort``/``with_columns`` has no
    session. Laps without one can only produce an empty telemetry frame,
    which would look like a session without telemetry, and telemetry without
    one can only report green-flag track status. Empty frames pass.

    Raises:
        ValueError: ``target`` is non-empty ``PolarsLaps`` without a session
            asked for telemetry, or non-empty ``PolarsTelemetry`` without a
            session asked for ``add_track_status``.
    """
    if target.session is not None or not target.height:
        return
    if name in _LAPS_TELEMETRY_MEMBERS and isinstance(target, PolarsLaps):
        hint = (
            "df.f1.as_laps(session).telemetry"
            if name == "telemetry"
            else f"df.f1.{name}(session=session)"
        )
    elif name == "add_track_status" and isinstance(target, PolarsTelemetry):
        drop_existing = (kwargs or {}).get("drop_existing", args[0] if args else True)
        if not drop_existing and "TrackStatus" in target.columns:
            return  # the existing column is kept: no session needed
        hint = f"df.f1.{name}(session=session)"
    else:
        return
    raise ValueError(
        f"df.f1.{name} needs the session, and this frame carries none (polars "
        f"filter/select/sort/with_columns return plain frames): use {hint}"
    )


_POLARS_OWN_METHODS = frozenset({"head", "tail", "limit", "slice", "clone", "reverse"})
F1Namespace._DISPATCH = (
    (PolarsLaps, _own_public_names(PolarsLaps), False),
    (PolarsTelemetry, _own_public_names(PolarsTelemetry), True),
    (PolarsSessionResults, _own_public_names(PolarsSessionResults), False),
    (_TIF1Frame, _own_public_names(_TIF1Frame) - _POLARS_OWN_METHODS, False),
)

# Notebook cards for PolarsDriver/PolarsLap when the display was enabled before
# this module was imported. tif1.jupyter is only looked up, never imported, so
# this module stays free of pandas.
_jupyter = sys.modules.get("tif1.jupyter")
if _jupyter is not None and getattr(_jupyter, "_display_enabled", False):
    _jupyter.register_polars_display(sys.modules[__name__])
del _jupyter
