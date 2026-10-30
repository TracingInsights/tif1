"""Fast exact laps frame builds: one driver (Pergamon Q19), a whole session (Olympia H8).

``Driver.laps`` on a session whose laps are not loaded builds the driver's
frame from its laptime payload with
``_process_lap_df(_create_lap_df(payload, driver, team, "pandas"), "pandas")``:
~25-40 ms for a 66-lap driver, almost all of it per-column pandas overhead
(46 ``__getitem__``/``isin``/``mask``/``astype`` round trips on 66 rows).

:func:`build_driver_laps_frame` produces the same frame — columns, order,
dtypes, values, missing-value representation, block layout — from one Python
pass per column over the payload lists, converting each column the way the
pipeline would: ``_replace_null_like_strings`` (exact-token probe, then the
strip/lower match), the ``_apply_laps_dtypes`` contract (the shared
``_seconds_to_td64ns`` cast for timedeltas, float64, Int64, bool, boolean,
``object``), pandas' ``str`` inference for string columns and the same
``astype("category")`` call for the categoricals. Whatever the fast rules do
not cover — values of an unexpected type, numeric strings, ``LapStartDate``
spellings other than canonical nanosecond ISO-8601 (``pd.to_datetime``
infers resolution and timezone from the strings) and non-string columns
outside the contract — goes through the real ``_apply_laps_dtypes`` (or
``pd.to_datetime``) on just those columns, or (for contract columns with
unexpected values) makes the function return None so the caller runs the
full pipeline.

:func:`build_laps_frame` applies the same rules to the merged dict-of-lists
of a session's first laps load (every driver, ``Driver``/``Team`` per row).
The column rules read each list's value types once at C level
(``set(map(type, values))``), so a clean float/int/bool column converts in
one numpy call.
"""

from __future__ import annotations

import warnings
from typing import Any, cast

import numpy as np
import pandas as pd

from tif1.exceptions import _NULL_LIKE_STRINGS

from .constants import (
    CATEGORICAL_COLS,
    COL_DRIVER,
    COL_LAP_TIME,
    COL_LAP_TIME_SECONDS,
    COL_TEAM,
    LAP_RENAME_MAP,
)
from .helpers import (
    _NULL_LIKE_TOKEN_PROBE,
    _apply_laps_dtypes,
    _fastf1_laps_column_order,
    _normalize_lap_payload,
    _rename_plan,
    _seconds_to_td64ns,
    _td_cast_is_safe,
)

_NAN = float("nan")
_NONE_TYPE = type(None)
# Value-type sets of the C-level fast paths (``set(map(type, values))``).
_FLOAT_KIND = frozenset((float,))
_INT_KIND = frozenset((int,))
_SECONDS_KINDS = frozenset((float, int, _NONE_TYPE, str))
_FLAG_KINDS = frozenset((bool, _NONE_TYPE))
_STRING_KINDS = frozenset((str, _NONE_TYPE))
_NULL_LIKE = frozenset(_NULL_LIKE_STRINGS)
_EXACT_TOKENS = frozenset(_NULL_LIKE_TOKEN_PROBE)
_INT64_BOUND = 2**63
_INT64_MIN = -(2**63)
_ISO_NS_LENGTH = len("2025-04-13T15:03:49.174000000")
_STR = pd.StringDtype(na_value=np.nan)

# ``_numeric_seconds_to_timedelta`` columns. LapTime/Time/WeatherTime are
# converted before the null-like masking (pd.to_numeric(errors="coerce"),
# LapTime then pd.to_timedelta for what stayed NaN); every null-like string
# lands on NaN/NaT either way, so all of them share one rule.
_TIMEDELTA_COLS = frozenset(
    (
        COL_LAP_TIME,
        "Time",
        "WeatherTime",
        "PitOutTime",
        "PitInTime",
        "Sector1Time",
        "Sector2Time",
        "Sector3Time",
        "Sector1SessionTime",
        "Sector2SessionTime",
        "Sector3SessionTime",
        "LapStartTime",
    )
)
_FLOAT_COLS = frozenset(
    (
        "LapNumber",
        "Stint",
        "TyreLife",
        "Position",
        "SpeedI1",
        "SpeedI2",
        "SpeedFL",
        "SpeedST",
        "AirTemp",
        "Humidity",
        "Pressure",
        "TrackTemp",
        "WindSpeed",
    )
)
_BOOL_COLS = frozenset(("IsPersonalBest", "FreshTyre", "FastF1Generated", "IsAccurate", "Rainfall"))
_INT64_COL = "WindDirection"
_BOOLEAN_COL = "Deleted"
_CATEGORICAL = frozenset(CATEGORICAL_COLS)
# ``_apply_laps_dtypes`` ``_STR_COLS`` that are not categorical: astype(object).
_OBJECT_STR_COLS = frozenset(("DriverNumber", "DeletedReason", "QualifyingSession"))
# Contract columns always converted by the real pipeline.
_PIPELINE_COLS = frozenset(("LapStartDate",))
_CONTRACT_COLS = (
    _TIMEDELTA_COLS
    | _FLOAT_COLS
    | _BOOL_COLS
    | _CATEGORICAL
    | _OBJECT_STR_COLS
    | {_INT64_COL, _BOOLEAN_COL, COL_LAP_TIME_SECONDS}
)

# Column-name plans keyed by the payload's key tuple (one per session layout).
_PLAN_CACHE: dict[tuple[str, ...], tuple[list[str], list[str]] | None] = {}
_PLAN_CACHE_MAX = 64


def _column_plan(keys: tuple[str, ...]) -> tuple[list[str], list[str]] | None:
    """(renamed payload columns, final column order) of the pipeline build, or None.

    Applies the real ``_rename_columns`` plan (``_rename_plan``: its drops
    and renames, computed from the labels) to the pipeline's pre-rename
    columns (payload keys, then ``Driver``/``Team``), then adds
    ``LapTimeSeconds`` and ``index`` and applies the FastF1 order exactly as
    ``_process_lap_df`` does.
    """
    if keys in _PLAN_CACHE:
        return _PLAN_CACHE[keys]
    plan: tuple[list[str], list[str]] | None = None
    if "index" not in keys and "level_0" not in keys:
        names = [*keys, COL_DRIVER, COL_TEAM]
        cols_to_drop, final_rename = _rename_plan(names, LAP_RENAME_MAP)
        dropped = set(cols_to_drop)
        renamed = [str(final_rename.get(c, c)) for c in names if c not in dropped]
        if (
            len(renamed) == len(names)
            and len(set(renamed)) == len(renamed)
            and "index" not in renamed
            and "level_0" not in renamed
            and renamed[-2:] == [COL_DRIVER, COL_TEAM]
        ):
            current = list(renamed)
            if COL_LAP_TIME in current and COL_LAP_TIME_SECONDS not in current:
                current.append(COL_LAP_TIME_SECONDS)
            plan = (renamed[:-2], _fastf1_laps_column_order(["index", *current]))
    if len(_PLAN_CACHE) >= _PLAN_CACHE_MAX:
        _PLAN_CACHE.clear()
    _PLAN_CACHE[keys] = plan
    return plan


def _ints_in_bounds(values: Any) -> bool:
    """Whether every int of ``values`` fits int64 (the pipeline's numeric range)."""
    ints = [value for value in values if type(value) is int]
    return not ints or (min(ints) > -_INT64_BOUND and max(ints) < _INT64_BOUND)


def _seconds(values: Any) -> np.ndarray | None:
    """float64 seconds as ``pd.to_numeric(errors="coerce")`` gives them, or None.

    Accepts ints (not bools), floats, None and null-like strings (NaN either
    way); any other value is left to the pipeline. The value types are read
    once at C level (Olympia H8): a float column converts in one call, and
    only columns holding missing values build a replaced list.
    """
    kinds = set(map(type, values))
    if kinds <= _FLOAT_KIND:
        return np.array(values, dtype=np.float64)
    if kinds == _INT_KIND:
        try:
            ints = np.array(values, dtype=np.int64)
        except OverflowError:
            return None
        if (ints == _INT64_MIN).any():
            return None  # outside the open int64 bound the pipeline accepts
        return ints.astype(np.float64)  # the same rounding as float(value)
    if not kinds <= _SECONDS_KINDS:
        return None
    if int in kinds and not _ints_in_bounds(values):
        return None
    if str in kinds:
        strings = {value for value in values if type(value) is str}
        if not all(value.strip().lower() in _NULL_LIKE for value in strings):
            return None
    if str in kinds or _NONE_TYPE in kinds:
        values = [_NAN if value is None or type(value) is str else value for value in values]
    return np.array(values, dtype=np.float64)


def _has_exact_token(values: Any) -> bool:
    """The ``series.isin(_NULL_LIKE_TOKEN_PROBE).any()`` probe of the masking step."""
    return any(type(value) is str and value in _EXACT_TOKENS for value in values)


def _masked(value: Any) -> bool:
    return type(value) is str and value.strip().lower() in _NULL_LIKE


def _flags(values: Any) -> list[Any] | None:
    """Bool/None values with masked null-like strings as None, or None for anything else."""
    if set(map(type, values)) <= _FLAG_KINDS:
        return list(values)  # nothing to mask
    exact = _has_exact_token(values)
    out: list[Any] = []
    append = out.append
    for value in values:
        if value is True or value is False or value is None:
            append(value)
        elif exact and _masked(value):
            append(None)
        else:
            return None
    return out


def _ints(values: Any) -> list[Any] | None:
    """Int/None values (null-like strings as None) for ``to_numeric().astype("Int64")``."""
    if set(map(type, values)) <= _INT_KIND and _ints_in_bounds(values):
        return list(values)
    out: list[Any] = []
    append = out.append
    for value in values:
        kind = type(value)
        if kind is int and -_INT64_BOUND < value < _INT64_BOUND:
            append(value)
        elif value is None or (kind is str and value.strip().lower() in _NULL_LIKE):
            append(None)
        else:
            return None
    return out


def _strings(values: Any) -> tuple[list[Any], bool] | None:
    """(masked values, str-inferred) for a str/None column, or None for other values.

    pandas infers the ``str`` dtype when at least one value is a string (the
    masking then turns null-like strings into NaN); an all-None column stays
    ``object`` holding None.
    """
    kinds = set(map(type, values))
    if not kinds <= _STRING_KINDS:
        return None
    if str not in kinds:
        return list(values), False
    if _EXACT_TOKENS.isdisjoint(values):
        return list(values), True
    masked = {value for value in set(values) if _masked(value)}
    return [None if value in masked else value for value in values], True


def _iso_ns_dates(values: list[Any]) -> np.ndarray | None:
    """``datetime64[ns]`` of canonical nanosecond ISO-8601 strings (None as NaT), or None.

    ``pd.to_datetime(format="ISO8601")`` infers the nanosecond unit from
    ``YYYY-MM-DDTHH:MM:SS.fffffffff`` strings and parses them to the instants
    numpy's C parser gives (~1.5 ms vs ~0.1 ms for a race, Olympia H8). A
    column is taken only when numpy renders every value back to the exact
    input string; any other spelling (an offset, fewer digits, an invalid
    date) is left to pandas.
    """
    present = [value for value in values if value is not None]
    if not present or set(map(len, present)) != {_ISO_NS_LENGTH}:
        return None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # numpy warns on a timezone suffix
            parsed = np.array(present, dtype="datetime64[ns]")
    except (OverflowError, TypeError, ValueError):
        return None
    if np.datetime_as_string(parsed, unit="ns").tolist() != present:
        return None
    if len(present) == len(values):
        return parsed
    dates = np.full(len(values), np.datetime64("NaT", "ns"))
    dates[[i for i, value in enumerate(values) if value is not None]] = parsed
    return dates


def _object_column(values: list[Any], str_inferred: bool) -> np.ndarray:
    """``astype(object)`` of the column: NaN for missing when str-inferred, else None."""
    if str_inferred:
        values = [_NAN if value is None else value for value in values]
    array = np.empty(len(values), dtype=object)
    array[:] = values
    return array


def _categorical(values: list[Any], str_inferred: bool) -> Any:
    """``astype("category")`` of the ``astype(object)`` column.

    pandas categorizes an object column of strings into its sorted unique
    values (``str`` categories) with -1 codes for the missing ones, which is
    built directly here; an all-missing column (empty categories, whose dtype
    pandas infers from the missing values) still goes through ``astype``.
    """
    present = set(values)
    present.discard(None)
    if not present:
        return pd.Series(_object_column(values, str_inferred), dtype=object).astype("category")
    categories = sorted(present)
    lookup: dict[Any, int] = {category: code for code, category in enumerate(categories)}
    lookup[None] = -1
    codes = np.fromiter(map(lookup.__getitem__, values), dtype=np.int64, count=len(values))
    return pd.Categorical.from_codes(
        codes, dtype=pd.CategoricalDtype(pd.Index(categories, dtype=_STR)), validate=False
    )


def build_driver_laps_frame(lap_data: dict, driver: str, team: str) -> pd.DataFrame | None:
    """Return ``_process_lap_df(_create_lap_df(lap_data, driver, team, "pandas"), "pandas")``.

    See the module docstring. Returns None when a column holds values the
    fast rules cannot convert exactly; the caller then runs the pipeline.

    Args:
        lap_data: The driver's raw laptime payload.
        driver: Driver code (the ``Driver`` column).
        team: Team name (the ``Team`` column).

    Returns:
        The processed laps frame (plain DataFrame), or None.
    """
    if not isinstance(lap_data, dict) or not lap_data or not isinstance(team, str):
        return None
    normalized = _normalize_lap_payload(lap_data)
    if not normalized:
        return None
    keys = tuple(normalized)
    plan = _column_plan(keys)
    if plan is None:
        return None
    renamed, order = plan
    raw_by_name: dict[str, Any] = {}
    for name, key in zip(renamed, keys, strict=True):
        values = normalized[key]
        if not isinstance(values, list | tuple):
            return None
        raw_by_name[name] = values
    n_rows = len(raw_by_name[renamed[0]])
    if n_rows == 0:
        return None
    raw_by_name[COL_DRIVER] = [driver] * n_rows
    raw_by_name[COL_TEAM] = [team] * n_rows
    return _build_frame(raw_by_name, order, n_rows)


def build_laps_frame(merged: dict[str, Any]) -> pd.DataFrame | None:
    """Return ``_process_lap_df(pd.DataFrame(merged, copy=False), "pandas")``.

    The session-wide twin of :func:`build_driver_laps_frame` (Olympia H8):
    ``merged`` is the dict-of-lists :func:`tif1.core._merge_lap_payloads`
    builds from every driver's normalized payload (payload columns, then
    ``Driver`` and ``Team``), so the first laps load of a session skips the
    frame-wide pipeline (~35-45 ms on a 1,128-lap race). Returns None when a
    column holds values the fast rules cannot convert exactly; the caller then
    runs the pipeline.

    Args:
        merged: Equal-length column lists, ``Driver`` and ``Team`` last.

    Returns:
        The processed laps frame (plain DataFrame), or None.
    """
    keys = tuple(merged)
    if len(keys) < 3 or keys[-2:] != (COL_DRIVER, COL_TEAM):
        return None
    plan = _column_plan(keys[:-2])
    if plan is None:
        return None
    renamed, order = plan
    raw_by_name: dict[str, Any] = {}
    for name, key in zip([*renamed, COL_DRIVER, COL_TEAM], keys, strict=True):
        values = merged[key]
        if not isinstance(values, list | tuple):
            return None
        raw_by_name[name] = values
    lengths = {len(values) for values in raw_by_name.values()}
    if len(lengths) != 1:
        return None  # the DataFrame constructor raises: leave that to the pipeline
    n_rows = lengths.pop()
    if n_rows == 0:
        return None
    return _build_frame(raw_by_name, order, n_rows)


def _build_frame(raw_by_name: dict[str, Any], order: list[str], n_rows: int) -> pd.DataFrame | None:
    """The processed frame from the renamed raw columns (see the module docstring), or None."""
    columns: dict[str, Any] = {}
    pipeline: dict[str, Any] = {}
    for name in order:
        if name == "index":
            columns[name] = np.arange(n_rows, dtype=np.int64)
            continue
        if name == COL_LAP_TIME_SECONDS and COL_LAP_TIME in raw_by_name:
            columns[name] = None  # derived from LapTime below
            continue
        raw = raw_by_name[name]
        if name in _TIMEDELTA_COLS:
            seconds = _seconds(raw)
            if seconds is None or not _td_cast_is_safe(seconds):
                return None
            columns[name] = _seconds_to_td64ns(seconds)
        elif name in _FLOAT_COLS:
            seconds = _seconds(raw)
            if seconds is None:
                return None
            columns[name] = seconds
        elif name in _BOOL_COLS:
            flags = _flags(raw)
            if flags is None:
                return None
            # True/False/None only: None reads False, as fillna(False) does.
            columns[name] = np.array(flags, dtype=bool)
        elif name == _BOOLEAN_COL:
            flags = _flags(raw)
            if flags is None:
                return None
            columns[name] = pd.array(flags, dtype="boolean")
        elif name == _INT64_COL:
            ints = _ints(raw)
            if ints is None:
                return None
            columns[name] = pd.array(ints, dtype="Int64")
        elif name in _CATEGORICAL or name in _OBJECT_STR_COLS:
            strings = _strings(raw)
            if strings is None:
                return None
            columns[name] = (
                _categorical(*strings) if name in _CATEGORICAL else _object_column(*strings)
            )
        elif name in _PIPELINE_COLS:
            # Masked here (the pipeline's probe then finds no token and skips its
            # strip/lower pass); an all-missing str column is left to the
            # pipeline, as masking it to None would change the inferred dtype.
            strings = _strings(raw)
            if strings is not None and strings[1] and any(v is not None for v in strings[0]):
                # The pipeline's conversion of a masked str column (its probe
                # finds no token): the same pd.to_datetime call, without the
                # one-column frame and its probe.
                dates = _iso_ns_dates(strings[0])
                columns[name] = (
                    dates
                    if dates is not None
                    else pd.to_datetime(
                        pd.Series(strings[0]), errors="coerce", utc=False, format="ISO8601"
                    )._values
                )
                continue
            pipeline[name] = raw
            columns[name] = None
        elif name in _CONTRACT_COLS:
            pipeline[name] = raw
            columns[name] = None
        else:
            # Outside the dtype contract: inference + null-like masking only.
            strings = _strings(raw)
            if strings is not None and strings[1]:
                columns[name] = pd.array(strings[0], dtype=_STR)
            elif strings is not None:
                columns[name] = _object_column(*strings)
            else:
                pipeline[name] = raw
                columns[name] = None

    if COL_LAP_TIME in raw_by_name:
        lap_time = pd.Series(columns[COL_LAP_TIME], copy=False)
        columns[COL_LAP_TIME_SECONDS] = lap_time.dt.total_seconds().to_numpy(copy=False)
    if pipeline:
        converted = _apply_laps_dtypes(pd.DataFrame(pipeline, copy=False))
        for name in pipeline:
            columns[name] = converted[name]._values

    # Every column already has its final dtype: skip the constructor's
    # per-column sanitize/inference (an object column must stay object).
    frame: pd.DataFrame = cast(Any, pd.DataFrame)._from_arrays(
        list(columns.values()),
        columns=pd.Index(list(columns)),
        index=pd.RangeIndex(n_rows),
        verify_integrity=False,
    )
    cast(Any, frame)._consolidate_inplace()
    return frame
