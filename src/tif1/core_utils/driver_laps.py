"""Fast exact per-driver laps frame build (Pergamon Q19).

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
(``pd.to_datetime`` infers resolution and timezone from the strings) and
non-string columns outside the contract — goes through the real
``_apply_laps_dtypes`` on just those columns, or (for contract columns with
unexpected values) makes the function return None so the caller runs the
full pipeline.
"""

from __future__ import annotations

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
    _rename_columns,
    _seconds_to_td64ns,
    _td_cast_is_safe,
)

_NAN = float("nan")
_NULL_LIKE = frozenset(_NULL_LIKE_STRINGS)
_EXACT_TOKENS = frozenset(_NULL_LIKE_TOKEN_PROBE)
_INT64_BOUND = 2**63
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

    Runs the real ``_rename_columns`` on an empty frame holding the pipeline's
    pre-rename columns (payload keys, then ``Driver``/``Team``), then adds
    ``LapTimeSeconds`` and ``index`` and applies the FastF1 order exactly as
    ``_process_lap_df`` does.
    """
    if keys in _PLAN_CACHE:
        return _PLAN_CACHE[keys]
    plan: tuple[list[str], list[str]] | None = None
    if "index" not in keys and "level_0" not in keys:
        names = [*keys, COL_DRIVER, COL_TEAM]
        renamed = [
            str(c)
            for c in _rename_columns(pd.DataFrame(columns=names), LAP_RENAME_MAP, "pandas").columns
        ]
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


def _seconds(values: Any) -> np.ndarray | None:
    """float64 seconds as ``pd.to_numeric(errors="coerce")`` gives them, or None.

    Accepts ints (not bools), floats, None and null-like strings (NaN either
    way); any other value is left to the pipeline.
    """
    out: list[float] = []
    append = out.append
    for value in values:
        kind = type(value)
        if kind is float:
            append(value)
        elif kind is int:
            if not -_INT64_BOUND < value < _INT64_BOUND:
                return None
            append(float(value))
        elif value is None or (kind is str and value.strip().lower() in _NULL_LIKE):
            append(_NAN)
        else:
            return None
    return np.array(out, dtype=np.float64)


def _has_exact_token(values: Any) -> bool:
    """The ``series.isin(_NULL_LIKE_TOKEN_PROBE).any()`` probe of the masking step."""
    return any(type(value) is str and value in _EXACT_TOKENS for value in values)


def _masked(value: Any) -> bool:
    return type(value) is str and value.strip().lower() in _NULL_LIKE


def _flags(values: Any) -> list[Any] | None:
    """Bool/None values with masked null-like strings as None, or None for anything else."""
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
    has_str = False
    exact = False
    for value in values:
        if type(value) is str:
            has_str = True
            if value in _EXACT_TOKENS:
                exact = True
        elif value is not None:
            return None
    if not has_str:
        return list(values), False
    if exact:
        return [None if _masked(value) else value for value in values], True
    return list(values), True


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
    present = {value for value in values if value is not None}
    if not present:
        return pd.Series(_object_column(values, str_inferred), dtype=object).astype("category")
    categories = sorted(present)
    lookup = {category: code for code, category in enumerate(categories)}
    codes = np.array([-1 if value is None else lookup[value] for value in values], dtype=np.int64)
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
            columns[name] = np.array([value is True for value in flags], dtype=bool)
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
                raw = strings[0]
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
