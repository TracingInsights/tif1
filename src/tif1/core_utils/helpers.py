"""Helper functions for tif1 core.

pandas is bound lazily (:func:`~tif1.core_utils.lazy_import.lazy_module`): a
polars session runs these helpers without importing it. The pandas block
assembly lives in :mod:`tif1.core_utils.pandas_blocks`; its names stay
readable here (``helpers._frame_from_columns`` and friends) through the module
``__getattr__`` at the end.
"""

from __future__ import annotations

import array
import importlib
import logging
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Union, cast
from urllib.parse import quote

import numpy as np

from tif1.config import get_config
from tif1.core_utils.lazy_import import is_pandas_frame, lazy_module
from tif1.core_utils.runtime import import_supported_polars
from tif1.exceptions import _NULL_LIKE_STRINGS, _coerce_null_like_string_list

from .constants import (
    CATEGORICAL_COLS,
    COL_DELETED,
    COL_DRIVER,
    COL_LAP_NUMBER,
    COL_LAP_NUMBER_ALT,
    COL_LAP_TIME,
    COL_LAP_TIME_SECONDS,
    COL_TEAM,
    FASTF1_LAPS_COLUMN_ORDER,
    LAP_RENAME_MAP,
    NUMERIC_TELEMETRY_CHANNELS,
    TELEMETRY_RENAME_MAP,
    TELEMETRY_SOURCE_VALUE,
    TELEMETRY_STATUS_VALUE,
    ordered_telemetry_columns,
)
from .json_utils import DictionaryChannel

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

    from . import pandas_blocks as _pb
else:
    # Polars is optional: importing it eagerly costs ~260 ms on every cold
    # start for pandas-only users. It loads lazily via
    # _ensure_polars_available() on the first polars backend use.
    pl = None
    # pandas (~250 ms) loads on the first ``pd.X`` a pandas frame needs; the
    # global is then the module itself (no per-access indirection).
    pd = lazy_module("pandas", globals(), "pd")
    _pb = lazy_module("tif1.core_utils.pandas_blocks", globals(), "_pb")

POLARS_AVAILABLE = False


logger = logging.getLogger(__name__)

# Shared global configuration singleton (same instance as tif1.core.config).
config = get_config()

# Type alias for DataFrame (both libraries resolved lazily; string refs keep
# the annotation meaningful without importing either at module load).
DataFrame = Union["pd.DataFrame", "pl.DataFrame"]


def _ensure_polars_available() -> bool:
    """Lazy-load Polars and refresh stale module state."""
    global pl, POLARS_AVAILABLE
    if POLARS_AVAILABLE and pl is not None:
        return True

    pl = import_supported_polars()  # type: ignore[ty:invalid-assignment]
    POLARS_AVAILABLE = pl is not None
    return POLARS_AVAILABLE


def _validate_year(year: int, min_year: int, max_year: int) -> None:
    """Validate year is in supported range."""
    if not min_year <= year <= max_year:
        raise ValueError(f"Year must be between {min_year} and {max_year}, got {year}")


def _validate_drivers_list(drivers: list[str] | None) -> None:
    """Validate drivers list parameter."""
    if drivers is not None:
        if not isinstance(drivers, list):
            raise TypeError(f"drivers must be a list, got {type(drivers).__name__}")
        if not drivers:
            raise ValueError("drivers list cannot be empty")
        if not all(isinstance(d, str) and d for d in drivers):
            raise ValueError("drivers must be a list of non-empty strings")


def _validate_lap_number(lap_number: int) -> None:
    """Validate lap number parameter."""
    if not isinstance(lap_number, int):
        raise TypeError(f"lap_number must be an integer, got {type(lap_number).__name__}")
    if lap_number <= 0:
        raise ValueError(f"lap_number must be positive, got {lap_number}")


def _validate_string_param(param: str, param_name: str) -> None:
    """Validate string parameter is not empty."""
    if not isinstance(param, str):
        raise TypeError(f"{param_name} must be a string, got {type(param).__name__}")
    if not param or not param.strip():
        raise ValueError(f"{param_name} cannot be empty")


@lru_cache(maxsize=1024)
def _encode_url_component(component: str) -> str:
    """Properly encode URL component with memoization."""
    return quote(component, safe="")


def _is_empty_df(df, lib: str) -> bool:
    """Check if DataFrame-like object is empty.

    Prefer the concrete object type over the lib flag because some code paths
    can surface pandas DataFrames even when the configured lib is "polars".
    """
    if is_pandas_frame(df):
        return df.empty

    if _ensure_polars_available() and isinstance(df, pl.DataFrame):
        return df.is_empty()

    if lib == "polars":
        is_empty = getattr(df, "is_empty", None)
        if callable(is_empty):
            return bool(is_empty())
        try:
            return bool(df.empty)
        except AttributeError:
            pass

    empty = getattr(df, "empty", None)
    if empty is not None:
        return bool(empty)

    is_empty = getattr(df, "is_empty", None)
    if callable(is_empty):
        return bool(is_empty())

    return len(df) == 0


def _create_empty_df(lib: str):
    """Create empty DataFrame for given lib."""
    if lib == "polars" and _ensure_polars_available():
        return pl.DataFrame()
    return pd.DataFrame()


def _not_deleted_mask(laps: pd.DataFrame) -> np.ndarray | None:
    """Boolean mask of laps not marked ``Deleted`` (``None`` without the column)."""
    if COL_DELETED not in laps.columns:
        return None
    deleted = laps[COL_DELETED]
    if deleted.dtype == bool:
        return ~deleted.to_numpy()
    # Nullable/object flags: a missing value means "not deleted".
    return ~deleted.astype("boolean").fillna(False).to_numpy(dtype=bool)


def _filter_valid_laptimes(laps, lib: str, *, exclude_deleted: bool = False):
    """Filter laps with valid lap times (optimized to minimize copies).

    Args:
        laps: Laps frame of either backend.
        lib: Backend of ``laps``.
        exclude_deleted: Also drop laps whose ``Deleted`` flag is set (the
            fastest-lap selections: a deleted lap time does not count).
    """
    if COL_LAP_TIME not in laps.columns:
        return laps

    if lib == "polars" and _ensure_polars_available() and isinstance(laps, pl.DataFrame):
        # Keep original LapTime representation and provide a numeric helper column.
        keep = pl.col(COL_LAP_TIME_SECONDS).is_not_null()
        if exclude_deleted and COL_DELETED in laps.columns:
            keep = keep & ~pl.col(COL_DELETED).cast(pl.Boolean, strict=False).fill_null(False)
        lap_time = pl.col(COL_LAP_TIME)
        if isinstance(laps.schema[COL_LAP_TIME], pl.Duration):
            # A Duration cast to Float64 is nanoseconds, not seconds.
            seconds = lap_time.dt.total_nanoseconds().cast(pl.Float64) / 1_000_000_000
        else:
            seconds = lap_time.cast(pl.Float64, strict=False)
        return laps.with_columns(seconds.alias(COL_LAP_TIME_SECONDS)).filter(keep)

    # For pandas: check if already timedeltas (any resolution, e.g. timedelta64[us]
    # with pandas 3.0 unit inference)
    # Argos A5: a boolean take already yields a fresh frame under pandas 3's
    # copy-on-write, so the explicit ``.copy()`` that followed it doubled the
    # per-block copies on every pick_fastest/get_fastest_lap call. A shallow
    # copy keeps the all-valid case free of a block copy too.
    lap_time = cast("pd.Series", laps[COL_LAP_TIME])
    not_deleted = _not_deleted_mask(laps) if exclude_deleted else None
    if pd.api.types.is_timedelta64_dtype(lap_time):
        mask = lap_time.notna().to_numpy()
        if not_deleted is not None:
            mask = mask & not_deleted
        valid = laps[mask] if not mask.all() else laps.copy(deep=False)
        valid[COL_LAP_TIME_SECONDS] = (
            cast("pd.Series", valid[COL_LAP_TIME]).dt.total_seconds().to_numpy(copy=False)
        )
        return valid

    lap_time_numeric = pd.to_numeric(lap_time, errors="coerce")
    valid_mask = lap_time_numeric.notna().to_numpy()
    if not_deleted is not None:
        valid_mask = valid_mask & not_deleted
    valid = laps[valid_mask]
    if valid.empty:
        return valid

    # Canonical pandas contract: Timedelta LapTime + numeric LapTimeSeconds.
    lap_time_seconds = lap_time_numeric.to_numpy()[valid_mask]
    valid[COL_LAP_TIME] = pd.to_timedelta(lap_time_seconds, unit="s")
    valid[COL_LAP_TIME_SECONDS] = lap_time_seconds
    return valid


def _rename_plan(columns: Any, rename_map: dict) -> tuple[list[str], dict[str, str]]:
    """(columns to drop, renames) of :func:`_rename_columns`, from the labels alone."""
    # One set of the labels: polars builds a new list on every ``df.columns``.
    present = set(columns)
    cols_to_rename = {k: v for k, v in rename_map.items() if k in present and v is not None}
    cols_to_drop = [k for k, v in rename_map.items() if k in present and v is None]

    # Check for duplicate target names and skip them to avoid column name conflicts
    target_names = {}
    final_rename = {}
    existing_columns = present.difference(cols_to_drop)
    for source, target in cols_to_rename.items():
        if source == target:
            continue
        # If target already exists independently, renaming would create duplicates (polars error).
        if target in existing_columns and target not in cols_to_rename:
            continue
        if target not in target_names:
            target_names[target] = source
            final_rename[source] = target
        else:
            # Skip this rename to avoid duplicate column names
            # Keep the first occurrence
            pass
    return cols_to_drop, final_rename


def _rename_columns(df, rename_map: dict, lib: str):
    """Rename DataFrame columns based on lib, avoiding duplicates."""
    cols_to_drop, final_rename = _rename_plan(df.columns, rename_map)

    # Drop columns marked with None
    if cols_to_drop:
        if lib == "polars" and _ensure_polars_available() and isinstance(df, pl.DataFrame):
            df = df.drop(cols_to_drop)
        else:
            df = df.drop(columns=cols_to_drop)

    if lib == "polars" and _ensure_polars_available() and isinstance(df, pl.DataFrame):
        return df.rename(final_rename) if final_rename else df
    return df.rename(columns=final_rename) if final_rename else df


def _apply_categorical(df, cols: list, lib: str):
    """Apply categorical dtype to columns."""
    if lib == "polars" and _ensure_polars_available() and isinstance(df, pl.DataFrame):
        existing_cols = [c for c in cols if c in df.columns]
        if existing_cols:
            return df.with_columns([pl.col(c).cast(pl.Categorical) for c in existing_cols])
    else:
        # Deduplicate columns first if needed (safety check)
        if df.columns.duplicated().any():
            df = df.loc[:, ~df.columns.duplicated()].copy()

        for col in cols:
            if col in df.columns:
                df[col] = df[col].astype("category")
    return df


def _get_lap_number(row: dict | pd.Series) -> int:
    """Safely extract lap number from row (dict or pandas Series)."""
    lap_num = row.get("LapNumber")
    if lap_num is None:
        # Only fall through on a missing/None LapNumber — a falsy value like
        # 0 is still a real (if unusual) lap number, not a missing key.
        lap_num = row.get("lap")
    if lap_num is None:
        raise ValueError("No lap number found in row")
    try:
        return int(lap_num)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid lap number: {lap_num}") from e


def _create_telemetry_df(
    tel_data: dict | None,
    driver: str,
    lap_num: int,
    lib: str,
    clock: tuple[int, int] | None = None,
) -> DataFrame | None:
    """Create telemetry DataFrame with driver and lap metadata (zero-copy optimized).

    Args:
        tel_data: The ``tel`` part of a ``{driver}/{lap}_tel.json`` payload.
        driver: Driver code.
        lap_num: Lap number.
        lib: ``"pandas"`` or ``"polars"``.
        clock: ``(LapStartTime, LapStartDate)`` of the lap as int64 nanoseconds
            (``LapStartDate`` may be the int64 minimum when unknown). When
            given, ``SessionTime`` and ``Date`` are built with the frame
            (fastf1's first two columns) instead of being inserted later.

    Returns:
        DataFrame or None if data is empty or invalid
    """
    if not tel_data:
        return None

    # Build column dict with pre-renamed keys, skipping non-list scalars (e.g. dataKey).
    # This avoids: list copies, scalar expansion, and the expensive df.rename() copy.
    col_data: dict[str, Any] = {}
    expected_len: int | None = None

    for k, v in tel_data.items():
        # Native-decoded payloads (Thebes TH4) carry numpy arrays for the
        # numeric channels and a DictionaryChannel for DriverAhead (Olympia
        # H6); the typed builders consume them directly.
        if not isinstance(v, _TELEMETRY_CHANNEL_TYPES):
            continue
        renamed_key = TELEMETRY_RENAME_MAP.get(k, k)
        col_data[renamed_key] = v
        if expected_len is None:
            expected_len = len(v)

    if expected_len is None or expected_len == 0:
        return None

    try:
        if lib == "polars" and _ensure_polars_available():
            ahead = col_data.get(_DRIVER_AHEAD_CHANNEL)
            if ahead is not None and not isinstance(ahead, DictionaryChannel):
                col_data[_DRIVER_AHEAD_CHANNEL] = _without_null_tokens(ahead)
            typed = _polars_typed_telemetry_frame(col_data, driver, lap_num, clock)
            if typed is not None:
                return typed
            telemetry_df = pl.DataFrame(
                _decoded_channels(col_data, clean_driver_ahead=True), strict=False
            )
            if telemetry_df.is_empty():
                return None
            return _polars_telemetry_frame(telemetry_df, driver, lap_num, clock)

        # Normalize mismatched lengths by padding with NaN
        max_len = max(len(v) for v in col_data.values())
        normalized_data = {}
        for k, v in col_data.items():
            if len(v) < max_len:
                # Pad with None (becomes NaN in pandas); arrays go back to
                # lists for this (never seen in real payloads) path.
                values = v.tolist() if isinstance(v, np.ndarray | DictionaryChannel) else v
                normalized_data[k] = values + [None] * (max_len - len(values))
            else:
                normalized_data[k] = v

        telemetry_df = _typed_telemetry_frame(
            normalized_data, max_len, driver, lap_num, clock=clock
        )
        if telemetry_df is not None:
            if telemetry_df.empty:
                return None
            return telemetry_df

        # Canonical conversion failed on this payload: build from raw lists
        # and run the legacy per-column conversion (bad payloads still drop
        # to None via the surrounding error contract).
        telemetry_df = pd.DataFrame(_decoded_channels(normalized_data), copy=False)
        if telemetry_df.empty:
            return None

        telemetry_df["Driver"] = driver
        telemetry_df["LapNumber"] = lap_num
        return _apply_telemetry_dtypes(telemetry_df)
    except Exception as e:
        logger.warning(f"Failed to create telemetry DataFrame: {e}")
        return None


def _polars_typed_channel(name: str, values: Any) -> Any:
    """One channel as a ready polars Series, or None when only the generic path types it.

    Mirrors polars' inference plus the contract casts for the clean cases:
    ints stay ``Int64``, floats are ``Float64`` with NaN as null (a ``"None"``
    sentinel too), ``Brake`` is ``Boolean``, ``nGear``/``DRS`` ``Int64``.
    """
    if isinstance(values, np.ndarray):
        arr: np.ndarray | None = values
    elif isinstance(values, DictionaryChannel):
        if name != _DRIVER_AHEAD_CHANNEL:
            return None
        codes, categories = _without_null_categories(values)
        if not categories:  # no driver ahead on any sample
            return pl.Series(name, [None] * len(codes), dtype=pl.String)
        return pl.Series(name, _arrow_strings_from_codes(categories, codes))
    elif name == "Brake":
        arr = np.asarray(values)
    elif name == _DRIVER_AHEAD_CHANNEL:
        cleaned = _without_null_tokens(values)
        if not all(x is None or type(x) is str for x in cleaned):
            return None
        return pl.Series(name, cleaned, dtype=pl.String)
    else:
        arr = _numeric_channel_array(values)
        if arr is None and name in _NUMERIC_TELEMETRY_CHANNELS:
            if name not in ("nGear", "DRS") and all(x is None for x in values):
                # No sample at all (a race leader has nobody ahead).
                return pl.Series(name, [None] * len(values), dtype=pl.Float64)
            arr = _numeric_channel_with_sentinels(values)
    if arr is None:
        return None
    kind = arr.dtype.kind
    if name == "Brake":
        return pl.Series(name, arr.astype(bool)) if kind in "biu" else None
    if name in ("nGear", "DRS"):
        return pl.Series(name, arr.astype("int64", copy=False)) if kind in "iu" else None
    if name not in _NUMERIC_TELEMETRY_CHANNELS:
        return None
    if kind in "iu":
        return pl.Series(name, arr.astype("int64", copy=False))
    if kind == "f":
        return pl.Series(name, arr, nan_to_null=True)
    return None


def _polars_constant_categorical(name: str, value: str, n: int) -> Any:
    """``pl.Series(name, [value] * n, dtype=pl.Categorical)`` without the ``n``-item list.

    A one-row Series repeated: the same Series (values, chunks, flags) for
    ~9 instead of ~50 us on a race lap. Not for ``String`` columns: there
    ``new_from_index`` flags the result sorted, and the Arrow route costs
    a ~0.2 s first call in a process that has not converted Arrow yet.
    """
    return pl.Series(name, [value], dtype=pl.Categorical).new_from_index(0, n)


def _polars_typed_telemetry_frame(
    col_data: dict[str, Any], driver: str, lap_num: int, clock: tuple[int, int] | None
):
    """The polars telemetry frame built from ready numpy arrays (no query plan).

    The result is the generic path's (:func:`_polars_telemetry_frame`) for
    clean payloads; every column becomes a ``Series`` directly and the frame
    is assembled once, because polars 2 runs each frame expression or
    ``Series`` operation as a lazy plan (~0.1-0.3 ms each, more than the whole
    build). Returns None for anything it does not type exactly (non-finite
    times, float gears, unexpected strings, unknown channels, ragged
    lengths), and the caller takes the generic path.
    """
    lengths = {len(values) for values in col_data.values()}
    if len(lengths) != 1:
        return None
    n = lengths.pop()
    series: dict[str, Any] = {}
    time_ns: np.ndarray | None = None
    for name, values in col_data.items():
        if name == "Time":
            if isinstance(values, np.ndarray):
                seconds = values.astype("float64", copy=False)
            else:
                seconds = _float_channel_array(values)
            if seconds is None or not np.isfinite(seconds).all():
                return None
            time_ns = np.round(seconds * 1_000_000_000).astype("int64")
            series[name] = pl.Series(name, time_ns.view("m8[ns]"))
            continue
        column = _polars_typed_channel(name, values)
        if column is None:
            return None
        series[name] = column
    for name in ("Speed", "nGear", "X", "Y", "Z"):
        if name not in series:
            dtype = pl.Int64 if name == "nGear" else pl.Float64
            series[name] = pl.Series(name, [None] * n, dtype=dtype)
    if time_ns is None:
        series["Time"] = pl.Series("Time", [None] * n, dtype=pl.Duration("ns"))
    series["Driver"] = pl.Series("Driver", [driver] * n, dtype=pl.String)
    series["LapNumber"] = pl.Series("LapNumber", np.full(n, lap_num, dtype="int64"))
    for name, value in (("Source", TELEMETRY_SOURCE_VALUE), ("Status", TELEMETRY_STATUS_VALUE)):
        if name not in series:
            series[name] = _polars_constant_categorical(name, value, n)
    if clock is not None and time_ns is not None:
        start_ns, date_ns = clock
        series["SessionTime"] = pl.Series("SessionTime", (time_ns + start_ns).view("m8[ns]"))
        if date_ns == _NS_MIN:
            series["Date"] = pl.Series("Date", [None] * n, dtype=pl.Datetime("ns"))
        else:
            series["Date"] = pl.Series("Date", (time_ns + date_ns).view("M8[ns]"))
    return pl.DataFrame([series[name] for name in ordered_telemetry_columns(list(series))])


def _polars_telemetry_frame(frame, driver: str, lap_num: int, clock: tuple[int, int] | None):
    """A freshly built polars telemetry frame in tif1's contract, in one ``select``.

    The same result as :func:`_coerce_polars_telemetry_sentinels`, the
    missing-channel/``Driver``/``LapNumber`` additions,
    :func:`_polars_telemetry_contract` and the session clock applied in
    turn. polars 2 runs every eager frame or ``Series`` operation as a lazy
    plan (~0.1-0.2 ms each); one ``select`` holding every expression is one
    plan per lap instead of a dozen.
    """
    schema = frame.schema
    exprs: dict[str, Any] = {}
    for name, dtype in schema.items():
        expr = pl.col(name)
        if name in _NUMERIC_TELEMETRY_CHANNELS and dtype in (pl.String, pl.Null):
            # A channel that is a sentinel on every sample ("None" or null).
            expr = expr.cast(pl.Float64, strict=False)
        elif name == _DRIVER_AHEAD_CHANNEL and dtype == pl.Null:
            expr = expr.cast(pl.String)
        elif dtype == pl.Float64:
            # A natively decoded sentinel is NaN in a float array.
            expr = expr.fill_nan(None)
        exprs[name] = expr
    for name in ("Speed", "nGear", "X", "Y", "Z"):
        if name not in exprs:
            exprs[name] = pl.lit(None, dtype=pl.Int64 if name == "nGear" else pl.Float64)
    exprs["Driver"] = pl.lit(driver, dtype=pl.String)
    exprs["LapNumber"] = pl.lit(lap_num, dtype=pl.Int64)
    time_dtype = schema.get("Time")
    if time_dtype is None:
        exprs["Time"] = pl.lit(None, dtype=pl.Duration("ns"))
    elif time_dtype not in (pl.Duration, pl.Null):
        exprs["Time"] = (
            (exprs["Time"].cast(pl.Float64, strict=False) * 1_000_000_000)
            .round()
            .cast(pl.Int64)
            .cast(pl.Duration("ns"))
        )
    brake_dtype = schema.get("Brake")
    if brake_dtype is not None and brake_dtype != pl.Boolean:
        exprs["Brake"] = exprs["Brake"].cast(pl.Int64, strict=False).cast(pl.Boolean)
    for name in ("nGear", "DRS"):
        dtype = schema.get(name)
        if dtype is not None and dtype != pl.Int64:
            exprs[name] = exprs[name].cast(pl.Int64, strict=False)
    for name, value in (("Source", TELEMETRY_SOURCE_VALUE), ("Status", TELEMETRY_STATUS_VALUE)):
        if name not in exprs:
            exprs[name] = pl.lit(value, dtype=pl.String).cast(pl.Categorical)
    time_is_ns = time_dtype is not None and (
        time_dtype == pl.Duration("ns") or time_dtype not in (pl.Duration, pl.Null)
    )
    if clock is not None and time_is_ns:
        start_ns, date_ns = clock
        time_ns = exprs["Time"].cast(pl.Int64)
        exprs["SessionTime"] = (time_ns + start_ns).cast(pl.Duration("ns"))
        exprs["Date"] = (
            pl.lit(None, dtype=pl.Datetime("ns"))
            if date_ns == _NS_MIN
            else (time_ns + date_ns).cast(pl.Datetime("ns"))
        )
    return frame.select(
        [exprs[name].alias(name) for name in ordered_telemetry_columns(list(exprs))]
    )


def _polars_telemetry_contract(frame):
    """fastf1's dtypes, constant channels and column order on a polars telemetry frame.

    ``Time`` (float seconds) becomes ``Duration("ns")``, ``Brake`` ``Boolean``,
    ``nGear``/``DRS``/``LapNumber`` ``Int64``; ``Source``/``Status`` are the
    constant categorical channels; columns follow fastf1's order.
    """
    schema = frame.schema
    exprs = []
    time_dtype = schema.get("Time")
    if time_dtype is not None and time_dtype not in (pl.Duration, pl.Null):
        exprs.append(
            (pl.col("Time").cast(pl.Float64, strict=False) * 1_000_000_000)
            .round()
            .cast(pl.Int64)
            .cast(pl.Duration("ns"))
            .alias("Time")
        )
    brake_dtype = schema.get("Brake")
    if brake_dtype is not None and brake_dtype != pl.Boolean:
        exprs.append(pl.col("Brake").cast(pl.Int64, strict=False).cast(pl.Boolean).alias("Brake"))
    for col in ("nGear", "DRS", "LapNumber"):
        dtype = schema.get(col)
        if dtype is not None and dtype != pl.Int64:
            exprs.append(pl.col(col).cast(pl.Int64, strict=False).alias(col))
    if "Source" not in schema:
        exprs.append(pl.lit(TELEMETRY_SOURCE_VALUE).cast(pl.Categorical).alias("Source"))
    if "Status" not in schema:
        exprs.append(pl.lit(TELEMETRY_STATUS_VALUE).cast(pl.Categorical).alias("Status"))
    if exprs:
        frame = frame.with_columns(exprs)
    ordered = ordered_telemetry_columns(list(frame.columns))
    if ordered != list(frame.columns):
        frame = frame.select(ordered)
    return frame


#: NaT as int64 (the session-clock arithmetic works on int64 nanoseconds).
_NS_MIN = np.iinfo("int64").min


def _typed_telemetry_frame(
    normalized_data: dict[str, Any],
    max_len: int,
    driver: str,
    lap_num: int,
    clock: tuple[int, int] | None = None,
) -> pd.DataFrame | None:
    """Build a telemetry DataFrame with canonical dtypes applied in the constructor.

    Fast path for :func:`_create_telemetry_df`: pre-types Time/Brake/nGear/DRS/
    Driver/LapNumber exactly as :func:`_apply_telemetry_dtypes` would, so the
    frame is constructed once instead of built raw and converted column by
    column (~1.7x faster per frame, verified dtype- and value-identical on the
    full 2026 Monaco race telemetry set). Returns None when a canonical
    conversion fails; the caller then uses the legacy build+convert path.
    """
    # Argos A1: every column is handed to the frame as a ready array. Thebes
    # TH2: the frame is then assembled block by block (``_frame_from_columns``)
    # instead of through ``DataFrame._from_arrays``, which re-sanitizes and
    # re-forms every column (~250 us of the ~640 us per frame). Column dtypes
    # are exactly the ones the dict constructor inferred (verified
    # frame-by-frame on the 2026 Monaco set and 8883 payloads of 2018-2025);
    # on any conversion error the caller falls back to the legacy
    # build+convert path.
    try:
        arrays: list[Any] = []
        columns: list[str] = []
        for k, v in normalized_data.items():
            if isinstance(v, np.ndarray):
                column = _typed_channel_from_array(k, v)
                if column is None:
                    column = pd.Series(v).array
                arrays.append(column)
                columns.append(k)
                continue
            if isinstance(v, DictionaryChannel):
                # Olympia H6: the string column straight from the codes (no
                # per-sample str objects); the same array the list branch
                # below infers.
                arrays.append(
                    _driver_ahead_from_codes(v)
                    if k == _DRIVER_AHEAD_CHANNEL
                    else pd.Series(v.tolist()).array
                )
                columns.append(k)
                continue
            if k == "Time":
                # Bit-exact vectorized replica of pandas' base/frac cast
                # (A2 / KR-2); the exact pd.to_timedelta call stays the path
                # for NaN/inf/huge values and non-float lists.
                seconds = _float_channel_array(v)
                if seconds is None:
                    try:
                        seconds = np.asarray(v, dtype="float64")
                    except (TypeError, ValueError):
                        seconds = None
                if seconds is not None and _td_cast_is_safe(seconds):
                    column: Any = _seconds_to_td64ns(seconds)
                else:
                    column = np.asarray(
                        pd.to_timedelta(seconds if seconds is not None else v, unit="s")
                    )
            elif k == "Brake" and None not in v:
                ints = _numeric_channel_array(v)
                column = (
                    ints.astype(bool)
                    if ints is not None and ints.dtype.kind == "i"
                    else np.asarray(v, dtype=bool)
                )
            elif k in ("nGear", "DRS"):
                # Integer lists (the common case) build the IntegerArray
                # directly: one C conversion + zero mask beats pd.array's
                # generic mask inference (~23 us/frame on Monaco-size rows).
                arr = _numeric_channel_array(v)
                if arr is None:
                    arr = np.asarray(v)
                if arr.dtype.kind in "iu":
                    column = pd.arrays.IntegerArray(
                        arr.astype("int64", copy=False), np.zeros(len(arr), dtype=bool)
                    )
                else:
                    column = pd.array(v, dtype="Int64")
            elif k in _NUMERIC_TELEMETRY_CHANNELS:
                # A "None" sentinel in the list would make numpy build a str
                # array; that column is coerced to float64 NaN instead of
                # being stringified.
                arr = _numeric_channel_array(v)
                if arr is None:
                    arr = _numeric_channel_with_sentinels(v)
                if arr is None:
                    arr = np.asarray(v)
                    column = arr if arr.dtype.kind in "if" else _coerce_numeric_channel(v, arr)
                else:
                    column = arr
            elif k == _DRIVER_AHEAD_CHANNEL:
                # Driver numbers as strings; "None" means nobody ahead. Series
                # inference keeps the dict constructor's dtype (str).
                column = pd.Series(_without_null_tokens(v)).array
            else:
                # Unknown channels keep pandas' list inference.
                column = pd.Series(v).array
            arrays.append(column)
            columns.append(k)
        # pandas 3 infers str dtype from object arrays at construction; the
        # FastF1-compatible Driver contract is object (no inference here).
        # Source/Status are fastf1's stream channels as constants (one
        # Python object each, like Driver).
        if "Source" not in columns:
            arrays.append(np.full(max_len, TELEMETRY_SOURCE_VALUE, dtype=object))
            columns.append("Source")
        if "Status" not in columns:
            arrays.append(np.full(max_len, TELEMETRY_STATUS_VALUE, dtype=object))
            columns.append("Status")
        arrays.append(np.full(max_len, driver, dtype=object))
        columns.append("Driver")
        arrays.append(
            pd.arrays.IntegerArray(
                np.full(max_len, lap_num, dtype="int64"), np.zeros(max_len, dtype=bool)
            )
        )
        columns.append("LapNumber")
        if clock is not None and "Time" in columns:
            time_column = arrays[columns.index("Time")]
            if isinstance(time_column, np.ndarray) and time_column.dtype == "m8[ns]":
                # The session clock as two more ready arrays (an insert after
                # construction cost ~0.45 ms per frame).
                time_ns = time_column.view("i8")
                known = time_ns != _NS_MIN
                start_ns, date_ns = clock
                arrays.append(np.where(known, time_ns + start_ns, _NS_MIN).view("m8[ns]"))
                columns.append("SessionTime")
                dates = np.where(known & (date_ns != _NS_MIN), time_ns + date_ns, _NS_MIN)
                arrays.append(dates.view("M8[ns]"))
                columns.append("Date")
        ordered = ordered_telemetry_columns(columns)
        if ordered != columns:
            by_name = dict(zip(columns, arrays, strict=True))
            columns = ordered
            arrays = [by_name[name] for name in ordered]
        frame = _pb._frame_from_columns(arrays, columns, max_len)
    except (AttributeError, TypeError, ValueError):
        return None
    return frame


def _typed_channel_from_array(name: str, values: np.ndarray) -> Any | None:
    """Canonical column for a natively decoded channel (int64/float64 ndarray).

    Mirrors the list branches of :func:`_typed_telemetry_frame`: an int64
    array is what ``array.array("q")`` produced for an all-int list, a
    float64 array (NaN for sentinels) what the sentinel coercion produced.
    Returns None for shapes the list path would hand to pandas' inference.
    """
    kind = values.dtype.kind
    if kind not in "iuf":
        return None
    if name == "Time":
        seconds = values if kind == "f" else values.astype("float64")
        if _td_cast_is_safe(seconds):
            return _seconds_to_td64ns(seconds)
        return np.asarray(pd.to_timedelta(seconds, unit="s"))
    if name == "Brake":
        return values.astype(bool) if kind in "iu" else None
    if name in ("nGear", "DRS"):
        if kind in "iu":
            return pd.arrays.IntegerArray(
                values.astype("int64", copy=False), np.zeros(len(values), dtype=bool)
            )
        return pd.array(values, dtype="Int64")
    if name in _NUMERIC_TELEMETRY_CHANNELS:
        return values
    return None


def _apply_telemetry_dtypes(telemetry_df: DataFrame) -> DataFrame:
    """Apply the canonical telemetry dtype conversions in a single pass.

    Shared by :func:`_create_telemetry_df` (per-driver frames) and the
    merged-dict telemetry assembly so the dtype rules cannot drift between
    the two construction paths.

    - ``Time``: float seconds → ``timedelta64[ns]``
    - ``Brake``: int (0/1) → ``bool`` (skipped when missing values are present;
      ``astype(bool)`` would coerce NaN → ``True``)
    - ``nGear``/``DRS``: int → nullable ``Int64``
    - Numeric channels stringified by a ``"None"`` sentinel → ``float64`` (NaN)
    - ``DriverAhead``: ``"None"`` sentinel → missing
    - Missing ``Time``/``Speed``/``nGear``/``X``/``Y``/``Z`` → ``pd.NA``
    - ``Driver`` stays ``object`` (FastF1 compatibility), ``LapNumber`` → ``Int64``

    This helper is pandas-only: it is called from the pandas construction paths
    in :func:`_create_telemetry_df` and the merged-dict assembly.
    """
    telemetry_df = cast("pd.DataFrame", telemetry_df)

    # Time: float seconds → timedelta64[ns]
    if "Time" in telemetry_df.columns:
        telemetry_df["Time"] = pd.to_timedelta(telemetry_df["Time"], unit="s")

    # Brake: int (0/1) → bool. Guarded: on a NaN-padded column astype(bool)
    # would turn NaN → True, so leave the column untouched when nulls exist.
    if "Brake" in telemetry_df.columns and not telemetry_df["Brake"].isna().any():
        telemetry_df["Brake"] = telemetry_df["Brake"].astype(bool)

    # nGear, DRS: int → Int64 (nullable)
    if "nGear" in telemetry_df.columns:
        telemetry_df["nGear"] = telemetry_df["nGear"].astype("Int64")
    if "DRS" in telemetry_df.columns:
        telemetry_df["DRS"] = telemetry_df["DRS"].astype("Int64")

    # A "None" sentinel (a race leader's DistanceToDriverAhead) turns a numeric
    # channel into object/str; the merged-dict build sees it from any lap.
    for col in _NUMERIC_TELEMETRY_CHANNELS:
        if col in telemetry_df.columns and not pd.api.types.is_numeric_dtype(telemetry_df[col]):
            telemetry_df[col] = pd.to_numeric(telemetry_df[col], errors="coerce").astype("float64")
    if _DRIVER_AHEAD_CHANNEL in telemetry_df.columns:
        ahead = telemetry_df[_DRIVER_AHEAD_CHANNEL]
        if pd.api.types.is_object_dtype(ahead) or pd.api.types.is_string_dtype(ahead):
            sentinel = ahead.isin(_NULL_LIKE_TOKEN_PROBE)
            if sentinel.any():
                telemetry_df[_DRIVER_AHEAD_CHANNEL] = ahead.mask(sentinel, None)

    # Keep object dtype for FastF1 compatibility (pandas 3.0 infers str dtype
    # by default for string columns; the laps contract uses object).
    if "Driver" in telemetry_df.columns:
        telemetry_df["Driver"] = telemetry_df["Driver"].astype(object)
    if "LapNumber" in telemetry_df.columns:
        telemetry_df["LapNumber"] = telemetry_df["LapNumber"].astype("Int64")

    for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
        if col not in telemetry_df.columns:
            telemetry_df[col] = pd.NA
    n_rows = len(telemetry_df)
    if "Source" not in telemetry_df.columns:
        telemetry_df["Source"] = np.full(n_rows, TELEMETRY_SOURCE_VALUE, dtype=object)
    if "Status" not in telemetry_df.columns:
        telemetry_df["Status"] = np.full(n_rows, TELEMETRY_STATUS_VALUE, dtype=object)
    ordered = ordered_telemetry_columns(list(telemetry_df.columns))
    if ordered != list(telemetry_df.columns):
        telemetry_df = telemetry_df[ordered]
    return telemetry_df


def _check_cached_telemetry(
    cache, year: int, gp: str, session: str, driver: str, lap_num: int, lib: str
) -> DataFrame | None:
    """Check cache for telemetry and return DataFrame if found."""
    cached_tel = cache.get_telemetry(year, gp, session, driver, lap_num)
    if cached_tel:
        return _create_telemetry_df(cached_tel, driver, lap_num, lib)
    return None


def _normalize_row_iteration(df, lib: str):
    """Normalize row iteration across backends."""
    if lib == "polars" and _ensure_polars_available() and isinstance(df, pl.DataFrame):
        return df.iter_rows(named=True)
    return (row for _, row in df.iterrows())


def _fastf1_laps_column_order(current_cols: list) -> list:
    """FastF1 laps columns present in ``current_cols`` first (FastF1 order), then the extras."""
    present = set(current_cols)
    ordered_cols = [col for col in FASTF1_LAPS_COLUMN_ORDER if col in present]
    seen = set(ordered_cols)
    ordered_cols.extend(col for col in current_cols if col not in seen)
    return ordered_cols


def _reorder_laps_columns(df, lib: str):
    """Reorder DataFrame columns to match FastF1 column order.

    Places columns in the same order as FastF1, with any extra tif1-specific
    columns at the end. Also adds an 'index' column if not present.
    """
    if _is_empty_df(df, lib):
        return df

    # Add index column if not present (FastF1 compatibility)
    if lib == "pandas":
        # Drop level_0 if it exists (artifact from reset_index)
        if "level_0" in df.columns:
            df = df.drop(columns=["level_0"])

        if "index" not in df.columns:
            df.insert(0, "index", range(len(df)))

    ordered_cols = _fastf1_laps_column_order(list(df.columns))

    # Reorder the DataFrame
    if lib == "polars" and _ensure_polars_available() and isinstance(df, pl.DataFrame):
        return df.select(ordered_cols)
    return df[ordered_cols]


# --- Lap/telemetry frame assembly (shared by Session and the model family) ---
def _get_lap_column(df, lib: str) -> str:
    """Get lap number column name."""
    return COL_LAP_NUMBER if COL_LAP_NUMBER in df.columns else COL_LAP_NUMBER_ALT


def _coerce_lap_number(lap_value: Any) -> int:
    """Coerce lap value to int with a stable error contract."""
    if lap_value is None:
        raise ValueError("No lap number found in row")
    try:
        return int(lap_value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid lap number: {lap_value}") from e


def _normalize_lap_payload(lap_data: dict) -> dict:
    """Normalize a raw lap payload into equal-length lists.

    Removes any existing Driver/Team columns (avoiding duplicates), pads short
    arrays with ``None`` to the payload's max column length, and replicates
    scalars. Shared by :func:`_create_lap_df` and the merged-dict lap assembly
    so both backends keep identical normalization semantics.
    """
    if not lap_data:
        return {}

    # Remove any existing Driver/Team columns to avoid duplicates
    lap_data = {k: v for k, v in lap_data.items() if k not in (COL_DRIVER, COL_TEAM)}

    # Calculate lengths for all values
    lengths = []
    for v in lap_data.values():
        if isinstance(v, list | tuple):
            lengths.append(len(v))
        elif hasattr(v, "__len__") and not isinstance(v, str | bytes):
            # Handle numpy arrays and other array-like objects
            lengths.append(len(v))
        else:
            # Scalar value
            lengths.append(1)

    max_len = max(lengths) if lengths else 0

    # Pad arrays that are too short
    normalized_data = {}
    for k, v in lap_data.items():
        if isinstance(v, list | tuple):
            current_len = len(v)
            if current_len < max_len:
                normalized_data[k] = list(v) + [None] * (max_len - current_len)
            else:
                normalized_data[k] = v
        elif hasattr(v, "__len__") and not isinstance(v, str | bytes):
            # Handle numpy arrays and other array-like objects
            current_len = len(v)
            if current_len < max_len:
                # Convert to list and pad
                normalized_data[k] = list(v) + [None] * (max_len - current_len)
            else:
                normalized_data[k] = v
        else:
            # Scalar value - replicate to match max_len
            normalized_data[k] = [v] * max_len if max_len > 0 else [v]
    return normalized_data


def _numeric_seconds_to_timedelta(values: pd.Series) -> pd.Series:
    """Convert numeric seconds to timedelta64[ns] without NaN cast warnings.

    Input that is already a timedelta (any resolution, e.g. ``timedelta64[us]``
    from pandas 3.0 unit inference) is returned unchanged - its integer values
    are never reinterpreted as seconds.
    """
    if pd.api.types.is_timedelta64_dtype(values):
        return values
    numeric_values = (
        values if pd.api.types.is_numeric_dtype(values) else pd.to_numeric(values, errors="coerce")
    )
    # Argos A2: one vectorized cast (NaN -> NaT inside) replaces the NaT
    # template + masked ``pd.to_timedelta`` assignment; NaN never reaches a
    # pandas conversion and the values are bit-identical to that path.
    seconds = numeric_values.to_numpy(dtype="float64", na_value=np.nan)
    if not _td_cast_is_safe(seconds):
        # inf / values beyond the nanosecond range: pandas decides (it widens
        # the resolution or raises), exactly as the constructor path would.
        return pd.Series(
            pd.to_timedelta(seconds, unit="s"), index=numeric_values.index, name=values.name
        )
    return pd.Series(_seconds_to_td64ns(seconds), index=numeric_values.index, name=values.name)


def _seconds_array_to_td64ns(seconds: np.ndarray) -> np.ndarray:
    """:func:`_numeric_seconds_to_timedelta` of a float64 array, as an array (no pandas).

    pandas is only imported for what the vectorized cast cannot take (inf,
    values beyond the nanosecond range, an empty array): the same
    ``pd.to_timedelta`` the Series path falls back to.
    """
    if not _td_cast_is_safe(seconds):
        return np.asarray(pd.to_timedelta(seconds, unit="s"))
    return _seconds_to_td64ns(seconds)


_NULL_LIKE_TOKEN_PROBE = _NULL_LIKE_STRINGS | {"None"}
_NULL_LIKE_TOKEN_ARRAY = np.array(sorted(_NULL_LIKE_TOKEN_PROBE))

# Telemetry channels that are numeric by contract. Upstream encodes a missing
# value as the literal string "None" (e.g. ``DistanceToDriverAhead`` on a race
# leader's lap, where nobody is ahead); left alone, one sentinel stringifies
# the whole column on both backends. The frame builders coerce it to NaN/null.
_NUMERIC_TELEMETRY_CHANNELS = NUMERIC_TELEMETRY_CHANNELS
# String channel (driver number ahead) whose missing values arrive as "None".
_DRIVER_AHEAD_CHANNEL = "DriverAhead"


_NS_PER_SECOND = 1_000_000_000
_NAT_INT64 = np.iinfo("int64").min


def _seconds_to_td64ns(seconds: np.ndarray) -> np.ndarray:
    """float64 seconds -> ``timedelta64[ns]`` exactly as ``pd.to_timedelta(unit="s")``.

    Vectorized replica of pandas' ``cast_from_unit_vectorized``: truncate to
    whole seconds, round the fraction to 9 decimals, scale both to
    nanoseconds and truncate; NaN becomes NaT. Bit-identical to the pandas
    path on every Monaco 2026 telemetry frame (Argos A2), at ~8 us instead of
    ~90 us per 400-sample channel (pandas converts the sequence twice:
    ``_convert_listlike`` and again inside ``TimedeltaIndex.__new__``).
    """
    with np.errstate(invalid="ignore"):
        whole = seconds.astype("int64")
        fraction = np.round(seconds - whole, 9)
        nanos = whole * _NS_PER_SECOND + (fraction * _NS_PER_SECOND).astype("int64")
    missing = np.isnan(seconds)
    if missing.any():
        nanos[missing] = _NAT_INT64
    return nanos.view("m8[ns]")


def _td_cast_is_safe(seconds: np.ndarray) -> bool:
    """Whether every value is finite and far from the int64-nanosecond range.

    The vectorized cast truncates silently where pandas would raise
    ``OutOfBoundsTimedelta``; NaN is handled inside the cast, so only inf and
    |x| >= 9e9 s (~285 years) are routed back to ``pd.to_timedelta``.
    """
    if seconds.size == 0:
        return False
    finite = np.isfinite(seconds)
    if finite.all():
        return bool(np.abs(seconds).max() < 9.0e9)
    if not (finite | np.isnan(seconds)).all():
        return False  # inf present
    return bool(np.abs(seconds[finite]).max() < 9.0e9) if finite.any() else True


def _float_channel_array(values: list[Any]) -> np.ndarray | None:
    """float64 ndarray for a list of Python numbers, or None if anything else is in it."""
    try:
        return np.frombuffer(array.array("d", values), dtype="float64")
    except TypeError:
        return None


def _numeric_channel_array(values: list[Any]) -> np.ndarray | None:
    """int64 for an all-int list, float64 for an int/float list, None otherwise.

    ``array.array`` converts a list of Python numbers in one C loop with no
    per-element dtype inference (~2x faster than ``np.asarray`` on 400-sample
    channels); the dtype outcome matches ``np.asarray``'s for these inputs
    (ints -> int64, any float -> float64). Bools and non-numbers return None so
    the caller keeps numpy's inference for them.
    """
    if values and isinstance(values[0], bool):
        return None
    if values and type(values[0]) is not float:
        try:
            return np.frombuffer(array.array("q", values), dtype="int64")
        except (TypeError, OverflowError):
            pass
    try:
        return np.frombuffer(array.array("d", values), dtype="float64")
    except TypeError:
        return None


def _numeric_channel_with_sentinels(values: list[Any]) -> np.ndarray | None:
    """float64 array for a numeric channel whose only non-numbers are null tokens.

    Upstream writes the literal string ``"None"`` into numeric channels (five
    channels on ~90% of 2026 Monaco frames); numpy would build a str array
    for the whole channel and the generic coercion then masks and re-parses
    it (~48 us per 675-sample channel). Swapping the known tokens for NaN in
    one Python pass and converting with ``array.array`` halves that. Any
    other non-number (an unexpected string, ``None``) returns None so the
    caller keeps the generic coercion, which also parses numeric strings.
    """
    try:
        return np.frombuffer(
            array.array(
                "d",
                [np.nan if (type(x) is str and x in _NULL_LIKE_TOKEN_PROBE) else x for x in values],
            ),
            dtype="float64",
        )
    except TypeError:
        return None


def _coerce_numeric_channel(values: list[Any], arr: np.ndarray | None = None) -> np.ndarray:
    """float64 array for a numeric channel list; sentinels/None become NaN.

    ``arr`` is the ``np.asarray(values)`` the caller already built. A str
    array (the sentinel case) is masked with a vectorized ``isin`` and parsed
    by numpy, several times cheaper than ``pd.to_numeric`` on an object
    Series; that pandas path stays the fallback for anything numpy cannot
    parse.
    """
    if arr is not None and arr.dtype.kind == "U":
        sentinel = np.isin(arr, _NULL_LIKE_TOKEN_ARRAY)
        out = np.full(arr.shape[0], np.nan, dtype="float64")
        try:
            out[~sentinel] = arr[~sentinel].astype("float64")
        except ValueError:
            pass
        else:
            return out
    return pd.to_numeric(pd.Series(values, dtype=object), errors="coerce").to_numpy(
        dtype="float64", na_value=np.nan
    )


def _without_null_tokens(values: list[Any]) -> list[Any]:
    """``values`` with exact null-like tokens (``"None"``, ``"null"``, ...) as None.

    The probe is ``set.isdisjoint`` (C-level hashing, no Python iteration), so
    a clean list is returned as the same object for a few microseconds.
    """
    try:
        clean = _NULL_LIKE_TOKEN_PROBE.isdisjoint(values)
    except TypeError:  # unhashable element: not a string channel
        return values
    if clean:
        return values
    return [None if isinstance(x, str) and x in _NULL_LIKE_TOKEN_PROBE else x for x in values]


# Payload channel shapes: orjson lists, native numpy arrays (Thebes TH4) and
# native dictionary-encoded string channels (Olympia H6).
_TELEMETRY_CHANNEL_TYPES = (list, np.ndarray, DictionaryChannel)


def _channel_list(values: Any) -> Any:
    """A payload channel as the list a generic JSON parser returns (lists pass through)."""
    return values.tolist() if isinstance(values, np.ndarray | DictionaryChannel) else values


def _decoded_channels(
    col_data: dict[str, Any], *, clean_driver_ahead: bool = False
) -> dict[str, Any]:
    """``col_data`` with dictionary-encoded channels as lists (for the generic builders).

    ``clean_driver_ahead`` also swaps a decoded ``DriverAhead``'s null-like
    tokens for None, as the polars builder does to its list before inference.
    """
    if not any(isinstance(v, DictionaryChannel) for v in col_data.values()):
        return col_data
    decoded: dict[str, Any] = {}
    for name, channel in col_data.items():
        values = channel
        if isinstance(channel, DictionaryChannel):
            values = channel.tolist()
            if clean_driver_ahead and name == _DRIVER_AHEAD_CHANNEL:
                values = _without_null_tokens(values)
        decoded[name] = values
    return decoded


def _without_null_categories(channel: DictionaryChannel) -> tuple[np.ndarray, list[str]]:
    """The codes and categories of ``channel`` with null-like tokens as missing (-1).

    The dictionary twin of :func:`_without_null_tokens`: the check runs over
    the few categories, and a token category (``"None"``: nobody ahead) is
    dropped by remapping the codes once.
    """
    categories = channel.categories
    codes = channel.codes
    if _NULL_LIKE_TOKEN_PROBE.isdisjoint(categories):
        return codes, categories
    remap = np.full(len(categories) + 1, -1, dtype=np.int32)  # last slot: code -1
    kept: list[str] = []
    for i, category in enumerate(categories):
        if category not in _NULL_LIKE_TOKEN_PROBE:
            remap[i] = len(kept)
            kept.append(category)
    return remap[codes], kept


_ARROW_STR_DTYPE: Any = None  # pd.StringDtype("pyarrow", na_value=nan), resolved lazily


def _arrow_strings_from_codes(categories: list[str], codes: np.ndarray) -> Any:
    """A pyarrow ``large_string`` array of ``categories[codes]`` (code -1: null)."""
    import pyarrow as pa

    missing = codes < 0
    indices = (
        pa.array(codes, type=pa.int32(), mask=missing)
        if missing.any()
        else pa.array(codes, type=pa.int32())
    )
    return pa.array(categories, type=pa.large_string()).take(indices)


def _driver_ahead_from_codes(channel: DictionaryChannel) -> Any:
    """The pandas ``DriverAhead`` column of a dictionary-encoded channel.

    Identical to ``pd.Series(_without_null_tokens(channel.tolist())).array``:
    the ``str`` (pyarrow) array with NaN for missing samples, or the object
    array of None that pandas infers when no sample has a driver ahead, built
    with one ``take`` instead of ~770 ``str`` objects and pandas' inference.
    """
    codes, categories = _without_null_categories(channel)
    if not categories:
        return pd.Series([None] * len(codes)).array
    return _arrow_string_column(categories, codes)


def _arrow_string_column(categories: list[str], codes: np.ndarray) -> Any:
    """pandas' default ``str`` array (pyarrow, NaN missing) of ``categories[codes]``."""
    global _ARROW_STR_DTYPE
    if _ARROW_STR_DTYPE is None:
        _ARROW_STR_DTYPE = pd.StringDtype("pyarrow", na_value=np.nan)
    string_array: Any = pd.arrays.ArrowStringArray
    return string_array(_arrow_strings_from_codes(categories, codes), dtype=_ARROW_STR_DTYPE)


def _coerce_polars_telemetry_sentinels(frame):
    """Cast numeric telemetry channels polars stringified because of a sentinel.

    Schema lookups only on a clean frame; the non-strict Float64 cast
    (sentinel -> null) runs just for a channel that came out as String.
    ``DriverAhead`` is cleaned on the Python list before construction, so
    polars infers a String column with nulls directly.
    """
    schema = frame.schema
    # A channel that is a sentinel on every sample arrives as String (the
    # "None" spelling) or as Null (the null spelling, Thebes TH9); both
    # become the Float64 column the contract promises.
    exprs = [
        pl.col(col).cast(pl.Float64, strict=False)
        for col in _NUMERIC_TELEMETRY_CHANNELS
        if schema.get(col) in (pl.String, pl.Null)
    ]
    if schema.get(_DRIVER_AHEAD_CHANNEL) == pl.Null:
        # Every sample was a sentinel (a race leader's lap): keep the String
        # contract rather than polars' Null dtype for an all-null list.
        exprs.append(pl.col(_DRIVER_AHEAD_CHANNEL).cast(pl.String))
    return frame.with_columns(exprs) if exprs else frame


def _replace_null_like_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize null-like string sentinels (e.g. ``"None"``) to None in string columns.

    Upstream encodes missing values as the literal string ``"None"`` in
    some lap columns (e.g. ``Deleted``). Normalize before dtype coercion so
    ``.astype("boolean")`` does not raise and bool columns do not silently
    convert missing values to ``True``. No-op for clean data.
    """
    for col in df.columns:
        series = df[col]
        if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
            continue
        # Cheap exact-token probe first (C-level, no string allocation).
        if not series.isin(_NULL_LIKE_TOKEN_PROBE).any():
            continue
        lowered = series.astype("string").str.strip().str.lower()
        df[col] = series.mask(lowered.isin(_NULL_LIKE_STRINGS), None)
    return df


def _replace_null_like_strings_pl(lap_df):
    """Polars equivalent of :func:`_replace_null_like_strings` (String columns only).

    Normalizes null-like string sentinels (e.g. ``"None"``) to null in String
    columns. No-op for clean data. Columns already stringified by polars (e.g.
    bools coerced to Utf8 when mixed with strings) cannot be re-typed here.
    """
    if not _ensure_polars_available():
        return lap_df
    lap_df_pl = cast(Any, lap_df)
    string_cols = [c for c, t in zip(lap_df_pl.columns, lap_df_pl.dtypes) if t == pl.String]
    if not string_cols:
        return lap_df
    hits = lap_df_pl.select(
        [pl.col(c).is_in(_NULL_LIKE_TOKEN_PROBE).any().alias(c) for c in string_cols]
    ).row(0)
    dirty_cols = [c for c, hit in zip(string_cols, hits) if hit]
    if not dirty_cols:
        return lap_df
    exprs = [
        pl.when(pl.col(c).str.strip_chars().str.to_lowercase().is_in(_NULL_LIKE_STRINGS))
        .then(None)
        .otherwise(pl.col(c))
        .alias(c)
        for c in dirty_cols
    ]
    return lap_df_pl.with_columns(exprs)


def _apply_laps_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce _COLUMNS dtype contract on a pandas laps DataFrame.

    Columns already handled upstream (LapTime, Time, WeatherTime, LapTimeSeconds)
    are skipped here to avoid double-conversion. All others from _COLUMNS are
    coerced to their canonical dtype. Missing columns are silently ignored.
    Null-like string sentinels (e.g. ``"None"``) are normalized first.
    """
    df = _replace_null_like_strings(df)

    # ------------------------------------------------------------------
    # Timedelta columns: raw values are floats (seconds since session start)
    # ------------------------------------------------------------------
    _TD_SECONDS_COLS = (
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
    for col in _TD_SECONDS_COLS:
        if col in df.columns and not pd.api.types.is_timedelta64_dtype(df[col]):
            df[col] = _numeric_seconds_to_timedelta(df[col])

    # ------------------------------------------------------------------
    # Datetime column: LapStartDate arrives as ISO-8601 strings
    # ------------------------------------------------------------------
    if "LapStartDate" in df.columns and not pd.api.types.is_datetime64_any_dtype(
        df["LapStartDate"]
    ):
        df["LapStartDate"] = pd.to_datetime(
            df["LapStartDate"], errors="coerce", utc=False, format="ISO8601"
        )

    # ------------------------------------------------------------------
    # Float64 columns (may arrive as int or object with None)
    # ------------------------------------------------------------------
    _FLOAT64_COLS = (
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
    for col in _FLOAT64_COLS:
        if col in df.columns and df[col].dtype != "float64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # ------------------------------------------------------------------
    # Int64 nullable column: WindDirection (int in JSON, but may be None)
    # ------------------------------------------------------------------
    if "WindDirection" in df.columns and df["WindDirection"].dtype.name != "Int64":
        df["WindDirection"] = pd.to_numeric(df["WindDirection"], errors="coerce").astype("Int64")

    # ------------------------------------------------------------------
    # Bool columns (JSON booleans, but None is possible for Deleted)
    # ------------------------------------------------------------------
    for col in _LAPS_BOOL_COLS:
        if col in df.columns and df[col].dtype != bool:
            df[col] = df[col].fillna(False).astype(bool)

    # Deleted is nullable bool (bool | None)
    if "Deleted" in df.columns and df["Deleted"].dtype.name != "boolean":
        df["Deleted"] = df["Deleted"].astype("boolean")

    # String columns — ensure object/str dtype (fillna with empty string for
    # non-nullable ones to preserve FastF1 compatibility)
    _STR_COLS = (
        "Driver",
        "DriverNumber",
        "Compound",
        "Team",
        "TrackStatus",
        "DeletedReason",
        "QualifyingSession",
    )
    for col in _STR_COLS:
        if col in df.columns:
            col_series = df[col]
            if not pd.api.types.is_object_dtype(col_series):
                df[col] = col_series.astype(object)

    return df


def _normalize_lap_payload_polars(lap_data: dict) -> dict:
    """Normalize a lap payload for polars construction.

    Applies :func:`_normalize_lap_payload` plus the null-like sentinel
    coercion polars needs to infer proper dtypes (Boolean/Float) instead of
    stringifying mixed columns. Shared by :func:`_create_lap_df` and the
    merged-dict polars lap assembly so both paths keep identical semantics.
    """
    return _coerce_null_like_payload(_normalize_lap_payload(lap_data))


def _coerce_null_like_payload(data: dict) -> dict:
    """Replace null-like string sentinels with ``None`` in a payload's list fields.

    polars infers a String column from a list that mixes real values with
    sentinels such as ``"None"`` (2026 data), and no later cast can recover
    the Boolean/Float/Int dtype; coercing before construction lets polars
    infer it directly. Mirrors validate_lap_data. Exact-token probe first so
    clean payloads skip the strip/lowercase pass entirely (no string
    allocation on the hot path) and are returned as the same object.
    """
    if not data:
        return data
    try:
        # Olympia H8: C-level hashing probe (a set holds only strings, so a
        # non-string value never matches); a list holding an unhashable value
        # takes the per-item probe below.
        dirty = any(
            isinstance(v, list | tuple) and not _NULL_LIKE_TOKEN_PROBE.isdisjoint(v)
            for v in data.values()
        )
    except TypeError:
        dirty = any(
            isinstance(v, list | tuple)
            and any(isinstance(x, str) and x in _NULL_LIKE_TOKEN_PROBE for x in v)
            for v in data.values()
        )
    if dirty:
        return {
            k: _coerce_null_like_values(v) if isinstance(v, list | tuple) else v
            for k, v in data.items()
        }
    return data


def _coerce_null_like_values(values: list[Any] | tuple[Any, ...]) -> list[Any]:
    """``_coerce_null_like_string_list(list(values))``, skipping lists without strings.

    The value types are read at C level; a list holding strings matches the
    strip/lower rule once per distinct string, then swaps the matching items.
    """
    if not any(issubclass(kind, str) for kind in set(map(type, values))):
        return list(values)
    try:
        distinct = set(values)
    except TypeError:  # unhashable item: the per-item rule
        return _coerce_null_like_string_list(list(values))
    null_like = {
        value
        for value in distinct
        if isinstance(value, str) and value.strip().lower() in _NULL_LIKE_STRINGS
    }
    if not null_like:
        return list(values)
    return [None if value in null_like and isinstance(value, str) else value for value in values]


def _create_lap_df(lap_data: dict, driver: str, team: str, lib: str) -> DataFrame:
    """Create lap DataFrame with driver and team info (zero-copy optimized)."""
    if lib == "polars":
        _ensure_polars_available()
        lap_df = pl.DataFrame(_normalize_lap_payload_polars(lap_data), strict=False)
        lap_df = lap_df.with_columns(
            [pl.lit(driver).alias(COL_DRIVER), pl.lit(team).alias(COL_TEAM)]
        )
        return lap_df

    # Normalize data to handle mismatched column heights
    # This is required in Python 3.12+ where both Pandas and Polars are stricter
    normalized_data = _normalize_lap_payload(lap_data)
    lap_df = pd.DataFrame(normalized_data, copy=False)
    # Deduplicate columns immediately after creation (safety check)
    if lap_df.columns.duplicated().any():
        lap_df = lap_df.loc[:, ~lap_df.columns.duplicated()]
    # Remove any existing Driver/Team columns before adding them (safety check)
    if COL_DRIVER in lap_df.columns:
        lap_df = lap_df.drop(columns=[COL_DRIVER])
    if COL_TEAM in lap_df.columns:
        lap_df = lap_df.drop(columns=[COL_TEAM])
    lap_df[COL_DRIVER] = driver
    lap_df[COL_TEAM] = team
    return lap_df


#: Laps columns that arrive as float seconds and become ``Duration("ns")`` on polars
#: (fastf1's ``timedelta64`` columns).
_POLARS_LAPS_DURATION_COLS = (
    "Time",
    "LapTime",
    "PitOutTime",
    "PitInTime",
    "Sector1Time",
    "Sector2Time",
    "Sector3Time",
    "Sector1SessionTime",
    "Sector2SessionTime",
    "Sector3SessionTime",
    "LapStartTime",
    "WeatherTime",
)


#: polars dtypes of the laps columns a session can leave entirely empty (all
#: null): pre-2018 Ergast laps have no speeds, tyres or track status.
_POLARS_LAPS_NULL_DTYPES: dict[str, str] = {
    **dict.fromkeys(
        (
            "Driver",
            "DriverNumber",
            "Compound",
            "Team",
            "TrackStatus",
            "DeletedReason",
            "QualifyingSession",
            "ms1",
            "ms2",
            "ms3",
            "drv",
            "team",
        ),
        "String",
    ),
    **dict.fromkeys(("LapNumber", "Stint", "TyreLife", "Position", "WindDirection"), "Int64"),
    **dict.fromkeys(
        (
            "SpeedI1",
            "SpeedI2",
            "SpeedFL",
            "SpeedST",
            "AirTemp",
            "Humidity",
            "Pressure",
            "TrackTemp",
            "WindSpeed",
            "LapTimeSeconds",
        ),
        "Float64",
    ),
}

#: Lap flags fastf1 types as bool (a missing flag reads False on both backends).
_LAPS_BOOL_COLS = ("IsPersonalBest", "FreshTyre", "FastF1Generated", "IsAccurate", "Rainfall")


def _polars_laps_contract(lap_df):
    """fastf1's dtype families on a polars laps frame.

    Time columns become ``Duration("ns")``, ``LapStartDate`` ``Datetime("ns")``;
    ``LapTimeSeconds`` (already computed from the float seconds) stays Float64.
    The flag columns follow the pandas contract: ``Boolean`` with a missing
    flag read as False (``Deleted`` stays nullable).
    """
    schema = lap_df.schema
    # A column no lap fills arrives as polars' Null dtype: give it the dtype a
    # populated session has (pandas keeps float64/object there).
    exprs = [
        pl.col(col).cast(getattr(pl, dtype)).alias(col)
        for col, dtype in _POLARS_LAPS_NULL_DTYPES.items()
        if schema.get(col) == pl.Null
    ]
    exprs += [
        pl.col(col).cast(pl.Boolean, strict=False).fill_null(False).alias(col)
        for col in _LAPS_BOOL_COLS
        if col in schema and (schema[col] != pl.Boolean or lap_df[col].null_count())
    ]
    if "Deleted" in schema and schema["Deleted"] != pl.Boolean:
        exprs.append(pl.col("Deleted").cast(pl.Boolean, strict=False).alias("Deleted"))
    for col in _POLARS_LAPS_DURATION_COLS:
        dtype = schema.get(col)
        if dtype is None or dtype == pl.Duration:
            continue
        exprs.append(
            (pl.col(col).cast(pl.Float64, strict=False) * 1_000_000_000)
            .round()
            .cast(pl.Int64)
            .cast(pl.Duration("ns"))
            .alias(col)
        )
    date_dtype = schema.get("LapStartDate")
    if date_dtype is not None and date_dtype != pl.Datetime:
        exprs.append(
            pl.col("LapStartDate")
            .cast(pl.String, strict=False)
            .str.to_datetime(strict=False, time_unit="ns")
            .alias("LapStartDate")
        )
    return lap_df.with_columns(exprs) if exprs else lap_df


_STR_OR_NONE_KINDS = frozenset((str, type(None)))


def _polars_contract_expr(name: str, expr: Any, dtype: Any, null_count: int) -> Any:
    """``expr`` (laps column ``name`` of ``dtype``) under :func:`_polars_laps_contract`."""
    if dtype == pl.Null and name in _POLARS_LAPS_NULL_DTYPES:
        return expr.cast(getattr(pl, _POLARS_LAPS_NULL_DTYPES[name]))
    if name in _LAPS_BOOL_COLS:
        if dtype != pl.Boolean or null_count:
            return expr.cast(pl.Boolean, strict=False).fill_null(False)
        return expr
    if name == "Deleted":
        return expr.cast(pl.Boolean, strict=False) if dtype != pl.Boolean else expr
    if name in _POLARS_LAPS_DURATION_COLS:
        if dtype == pl.Duration:
            return expr
        return (
            (expr.cast(pl.Float64, strict=False) * 1_000_000_000)
            .round()
            .cast(pl.Int64)
            .cast(pl.Duration("ns"))
        )
    if name == "LapStartDate" and dtype != pl.Datetime:
        return expr.cast(pl.String, strict=False).str.to_datetime(strict=False, time_unit="ns")
    return expr


def _process_polars_laps(lap_df, source_lists: dict[str, Any] | None = None):
    """The polars branch of :func:`_process_lap_df` as one ``select``.

    Olympia H8: rename, the null-like masking, ``LapTimeSeconds``, the dtype
    contract, the optional categoricals and the FastF1 column order ran as
    five query plans (~0.5 ms each on a race). Every step is column-wise, so
    each output column is the composition of its steps on the input column,
    computed in one plan; only the masking probe (which columns hold an exact
    null-like token) still reads the data first, exactly as
    :func:`_replace_null_like_strings_pl` does. ``source_lists`` (the lists
    the frame was built from, by input column) lets that probe read a
    string column's list instead of running a query plan.
    """
    _ensure_polars_available()
    frame = cast(Any, lap_df)
    columns = frame.columns
    schema = frame.schema
    cols_to_drop, final_rename = _rename_plan(columns, LAP_RENAME_MAP)
    dropped = set(cols_to_drop)
    source = {final_rename.get(c, c): c for c in columns if c not in dropped}
    if len(source) != len(columns) - len(dropped):
        # Two columns would share a name: the stepwise path raises as before.
        return _process_lap_df_stepwise(lap_df, "polars")

    string_cols = [c for c in source.values() if schema[c] == pl.String]
    dirty: set[str] = set()
    probed: list[str] = []
    for column in string_cols:
        values = None if source_lists is None else source_lists.get(column)
        if (
            values is not None
            and len(values) == frame.height
            and set(map(type, values)) <= _STR_OR_NONE_KINDS
        ):
            # The probe on the list the column was built from (str/None only:
            # the column holds exactly these strings), with no query plan.
            if not _NULL_LIKE_TOKEN_PROBE.isdisjoint(values):
                dirty.add(column)
        else:
            probed.append(column)
    if probed:
        hits = frame.select(
            [pl.col(c).is_in(_NULL_LIKE_TOKEN_PROBE).any().alias(c) for c in probed]
        ).row(0)
        dirty.update(c for c, hit in zip(probed, hits) if hit)

    def masked(name: str) -> Any:
        column = source[name]
        expr = pl.col(column)
        if column in dirty:
            expr = (
                pl.when(expr.str.strip_chars().str.to_lowercase().is_in(_NULL_LIKE_STRINGS))
                .then(None)
                .otherwise(expr)
            )
        return expr

    exprs: dict[str, Any] = {name: masked(name) for name in source}
    dtypes = {name: schema[column] for name, column in source.items()}
    if COL_LAP_TIME in exprs:
        exprs[COL_LAP_TIME_SECONDS] = exprs[COL_LAP_TIME].cast(pl.Float64, strict=False)
        dtypes[COL_LAP_TIME_SECONDS] = pl.Float64
    categorical = bool(config.get("polars_lap_categorical", False))
    for name, column_expr in list(exprs.items()):
        null_count = (
            frame[source[name]].null_count()
            if name in _LAPS_BOOL_COLS and dtypes[name] == pl.Boolean
            else 0
        )
        expr = _polars_contract_expr(name, column_expr, dtypes[name], null_count)
        if categorical and name in CATEGORICAL_COLS:
            expr = expr.cast(pl.Categorical)
        exprs[name] = expr.alias(name)
    order = list(exprs) if frame.height == 0 else _fastf1_laps_column_order(list(exprs))
    return frame.select([exprs[name] for name in order])


def _process_lap_df(lap_df, lib: str) -> DataFrame:
    """Apply column renaming, dtype coercions, and categorical types."""
    if lib == "polars":
        _ensure_polars_available()
        if isinstance(lap_df, pl.DataFrame):
            return _process_polars_laps(lap_df)
    return _process_lap_df_stepwise(lap_df, lib)


def _process_lap_df_stepwise(lap_df, lib: str) -> DataFrame:
    """:func:`_process_lap_df` one step at a time (pandas; polars fallback)."""
    if lib == "polars":
        _ensure_polars_available()
    # Remove duplicate columns if they exist (pandas only) - must be done FIRST
    if lib == "pandas" and isinstance(lap_df.columns, pd.Index):
        if lap_df.columns.duplicated().any():
            lap_df = lap_df.loc[:, ~lap_df.columns.duplicated()]

    lap_df = _rename_columns(lap_df, LAP_RENAME_MAP, lib)
    if lib == "pandas" and COL_LAP_TIME in lap_df.columns:
        lap_time_series = cast("pd.Series", lap_df[COL_LAP_TIME])
        if not pd.api.types.is_timedelta64_dtype(lap_time_series):
            numeric_lap_times = pd.to_numeric(lap_time_series, errors="coerce")
            numeric_lap_timedeltas = _numeric_seconds_to_timedelta(numeric_lap_times)
            missing_mask = numeric_lap_times.isna()
            if bool(missing_mask.any()):
                # Only non-numeric entries (deleted laps, string times) pay for
                # the string parse, and only on that subset.
                parsed_subset = pd.to_timedelta(lap_time_series[missing_mask], errors="coerce")
                numeric_lap_timedeltas = numeric_lap_timedeltas.copy()
                numeric_lap_timedeltas[missing_mask] = parsed_subset
            lap_df[COL_LAP_TIME] = numeric_lap_timedeltas
        lap_df[COL_LAP_TIME_SECONDS] = (
            cast("pd.Series", lap_df[COL_LAP_TIME]).dt.total_seconds().to_numpy(copy=False)
        )
    if lib == "pandas" and "Time" in lap_df.columns:
        time_series = cast("pd.Series", lap_df["Time"])
        if not pd.api.types.is_timedelta64_dtype(time_series):
            # Only convert if it's actually a Series (not already converted)
            if isinstance(time_series, pd.Series):
                lap_df["Time"] = _numeric_seconds_to_timedelta(time_series)
    if lib == "pandas" and "WeatherTime" in lap_df.columns:
        weather_time_series = cast("pd.Series", lap_df["WeatherTime"])
        if not pd.api.types.is_timedelta64_dtype(weather_time_series):
            if isinstance(weather_time_series, pd.Series):
                lap_df["WeatherTime"] = _numeric_seconds_to_timedelta(weather_time_series)
    # Apply full _COLUMNS dtype contract for all remaining pandas columns
    if lib == "pandas":
        lap_df = _apply_laps_dtypes(lap_df)
    if lib == "polars":
        lap_df = _replace_null_like_strings_pl(lap_df)
    if lib == "polars" and COL_LAP_TIME in lap_df.columns:
        lap_df_pl = cast(Any, lap_df)
        lap_df = lap_df_pl.with_columns(
            pl.col(COL_LAP_TIME).cast(pl.Float64, strict=False).alias(COL_LAP_TIME_SECONDS)
        )
    if lib == "polars":
        lap_df = _polars_laps_contract(lap_df)
    if lib == "polars" and not bool(config.get("polars_lap_categorical", False)):
        lap_df = _reorder_laps_columns(lap_df, lib)
        return lap_df
    lap_df = _apply_categorical(lap_df, CATEGORICAL_COLS, lib)
    lap_df = _reorder_laps_columns(lap_df, lib)
    if lib == "pandas":
        # Argos A4: the per-column dtype assignments above leave one block per
        # column (46 blocks on a race laps frame); every later row selection
        # (pick_*, Driver.laps, iterrows) then pays a per-block take. One
        # consolidation here (~0.1 ms) halves pick_driver on the same frame.
        cast(Any, lap_df)._consolidate_inplace()
    return lap_df


def _merge_telemetry_payloads(
    tel_entries: list[tuple[str, int, dict]],
) -> dict:
    """Merge raw telemetry payloads into a single dict-of-lists.

    Produces the same rows/columns that ``pd.concat([_create_telemetry_df(p)
    for p in payloads])`` would, including None-padding for columns absent from
    some drivers. Building one DataFrame from a single dict and applying the
    dtype conversions once is ~2-3x faster than constructing a per-driver
    DataFrame (which repeats ``pd.to_timedelta``/``astype`` machinery per
    frame) and concatenating them.

    Args:
        tel_entries: Iterable of ``(driver, lap_num, tel_payload)`` tuples where
            ``tel_payload`` is the raw telemetry channel dict (list-valued
            channels are kept; scalars such as ``dataKey`` are ignored, exactly
            like :func:`_create_telemetry_df`).

    Returns:
        A dict mapping renamed column names to concatenated values, with
        ``Driver`` and ``LapNumber`` columns appended in payload order. A
        column is a list, except where every payload carries it natively at
        full length (Olympia H6): numeric arrays are concatenated
        (``int64``/``float64``, the dtype pandas infers from the lists), and
        dictionary-encoded ``DriverAhead`` channels become the string array
        pandas infers from the lists, null-like tokens already missing.
    """
    # Collect column order by first appearance (dict preserves insertion order).
    all_columns: dict[str, None] = {}
    normalized_entries: list[tuple[dict, int, str, int]] = []
    for driver, lap_num, tel_payload in tel_entries:
        if not isinstance(tel_payload, dict) or not tel_payload:
            continue
        col_data = {
            TELEMETRY_RENAME_MAP.get(k, k): v
            for k, v in tel_payload.items()
            if isinstance(v, _TELEMETRY_CHANNEL_TYPES)
        }
        if not col_data:
            continue
        max_len = max(len(v) for v in col_data.values())
        if max_len == 0:
            continue
        normalized = {
            k: (_channel_list(v) + [None] * (max_len - len(v))) if len(v) < max_len else v
            for k, v in col_data.items()
        }
        for col in normalized:
            all_columns.setdefault(col, None)
        normalized_entries.append((normalized, max_len, driver, lap_num))

    merged: dict[str, Any] = {}
    for col in all_columns:
        parts = [normalized.get(col) for normalized, _, _, _ in normalized_entries]
        typed = _merged_native_column(col, parts)
        if typed is not None:
            merged[col] = typed
            continue
        values: list[Any] = []
        for part, (_, n_rows, _, _) in zip(parts, normalized_entries, strict=True):
            if part is None:
                values.extend([None] * n_rows)
            else:
                values.extend(_channel_list(part))
        merged[col] = values
    driver_col: list[str] = []
    lap_col: list[int] = []
    for _, n_rows, driver, lap_num in normalized_entries:
        driver_col.extend([driver] * n_rows)
        lap_col.extend([lap_num] * n_rows)
    merged[COL_DRIVER] = driver_col
    merged[COL_LAP_NUMBER] = lap_col
    return merged


def _dictionary_from_list(values: Any) -> DictionaryChannel | None:
    """A list of ``str``/None as a DictionaryChannel; None for anything else."""
    if not isinstance(values, list):
        return None
    positions: dict[str, int] = {}
    codes: list[int] = []
    for value in values:
        if value is None:
            codes.append(-1)
        elif type(value) is str:
            codes.append(positions.setdefault(value, len(positions)))
        else:
            return None
    return DictionaryChannel(np.array(codes, dtype=np.int32), list(positions))


def _merged_native_column(name: str, parts: list[Any]) -> Any | None:
    """One merged column built from native channels, or None for the list path.

    Only for a column some payload carries natively (an orjson-only merge
    keeps the list path) and every payload carries at full length: int64/
    float64 arrays concatenate to the dtype pandas infers from the
    equivalent int/float lists (a NaN sentinel is the float the list path
    coerces ``"None"`` to; a clean int/float list joins as its array);
    ``DriverAhead`` codes are remapped onto one category list and built as
    the string array pandas infers from the string list, with the null-like
    tokens :func:`_apply_telemetry_dtypes` would mask already missing.
    Padding, sentinel lists and other dtypes keep the list path.
    """
    if not any(isinstance(p, np.ndarray | DictionaryChannel) for p in parts):
        return None
    if name != _DRIVER_AHEAD_CHANNEL:
        arrays = [
            p
            if isinstance(p, np.ndarray)
            else (_numeric_channel_array(p) if isinstance(p, list) else None)
            for p in parts
        ]
        if any(a is None or a.dtype.kind not in "if" for a in arrays):
            return None
        return np.concatenate(cast(list[np.ndarray], arrays))
    channels = [p if isinstance(p, DictionaryChannel) else _dictionary_from_list(p) for p in parts]
    if any(channel is None for channel in channels):
        return None
    categories: list[str] = []
    positions: dict[str, int] = {}
    code_parts: list[np.ndarray] = []
    for part in cast(list[DictionaryChannel], channels):
        remap = np.full(len(part.categories) + 1, -1, dtype=np.int32)  # last slot: code -1
        for i, category in enumerate(part.categories):
            position = positions.get(category)
            if position is None:
                position = positions[category] = len(categories)
                categories.append(category)
            remap[i] = position
        code_parts.append(remap[part.codes])
    if not categories:
        return None  # JSON nulls only: pandas infers object from the list
    codes, kept = _without_null_categories(
        DictionaryChannel(np.concatenate(code_parts), categories)
    )
    return _arrow_string_column(kept, codes)


def _telemetry_frame_from_merged(merged: dict) -> pd.DataFrame:
    """Build a telemetry DataFrame from a merged dict, applying dtypes once.

    Applies the same dtype conversions that :func:`_create_telemetry_df`
    performs per driver frame (via the shared :func:`_apply_telemetry_dtypes`),
    but a single pass on the merged frame — which is what makes merged-dict
    assembly ~2-3x faster than per-frame construction + concat on pandas 3.x.
    """
    if not merged:
        return pd.DataFrame()
    return cast("pd.DataFrame", _apply_telemetry_dtypes(pd.DataFrame(merged, copy=False)))


#: Names that moved to :mod:`tif1.core_utils.pandas_blocks` (imported on first read).
_PANDAS_BLOCK_NAMES = frozenset(
    {
        "_BlockManager",
        "_BlockPlacement",
        "_COLUMN_INDEX_CACHE",
        "_DATETIMELIKE_BLOCK_CLASS",
        "_DIRECT_BLOCKS_AVAILABLE",
        "_DIRECT_BLOCKS_OK",
        "_DIRECT_BLOCK_CLASSES_OK",
        "_DatetimeArray",
        "_EXTENSION_BLOCK_CLASS",
        "_INTEGER_ARRAY_SIMPLE_NEW",
        "_IntegerArray",
        "_NUMPY_BLOCK_CLASS",
        "_PLACEMENT_CACHE",
        "_RANGE_INDEX_CACHE",
        "_TimedeltaArray",
        "_block_placement",
        "_dataframe_from_mgr",
        "_direct_frame_from_columns",
        "_ensure_wrapped_if_datetimelike",
        "_extension_block",
        "_extension_block_placed",
        "_frame_from_arrays",
        "_frame_from_blocks",
        "_frame_from_columns",
        "_int64_array",
        "_new_block",
        "_numpy_block",
        "_numpy_block_placed",
        "_placement_for",
        "_probe_direct_blocks",
        "_range_index",
        "_shared_column_index",
        "_wrap_datetimelike",
    }
)


def _load_pandas_blocks() -> None:
    """Import (and probe) :mod:`tif1.core_utils.pandas_blocks` now; a pandas session does."""
    _pb._DIRECT_BLOCKS_OK  # noqa: B018 - the attribute read imports and rebinds ``_pb``


def __getattr__(name: str) -> Any:
    """The pandas block-assembly names (:mod:`tif1.core_utils.pandas_blocks`), imported lazily."""
    if name in _PANDAS_BLOCK_NAMES:
        return getattr(importlib.import_module("tif1.core_utils.pandas_blocks"), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
