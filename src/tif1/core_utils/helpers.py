"""Helper functions for tif1 core."""

import logging
from functools import lru_cache
from typing import Any, Union, cast
from urllib.parse import quote

import pandas as pd

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    pl = None  # type: ignore
    POLARS_AVAILABLE = False

from .constants import (
    COL_LAP_TIME,
    COL_LAP_TIME_SECONDS,
    FASTF1_LAPS_COLUMN_ORDER,
    TELEMETRY_RENAME_MAP,
)

logger = logging.getLogger(__name__)

# Type alias for DataFrame
if POLARS_AVAILABLE:
    DataFrame = Union[pd.DataFrame, pl.DataFrame]
else:
    DataFrame = pd.DataFrame


def _ensure_polars_available() -> bool:
    """Lazy-load Polars and refresh stale module state."""
    global pl, POLARS_AVAILABLE
    if POLARS_AVAILABLE and pl is not None:
        return True

    try:
        import polars as pl

        POLARS_AVAILABLE = True
    except ImportError:
        pl = None  # type: ignore[assignment]
        POLARS_AVAILABLE = False
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
    if isinstance(df, pd.DataFrame):
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


def _filter_valid_laptimes(laps, lib: str):
    """Filter laps with valid lap times (optimized to minimize copies)."""
    if COL_LAP_TIME not in laps.columns:
        return laps

    if lib == "polars" and _ensure_polars_available() and isinstance(laps, pl.DataFrame):
        # Keep original LapTime representation and provide a numeric helper column.
        return laps.with_columns(
            pl.col(COL_LAP_TIME).cast(pl.Float64, strict=False).alias(COL_LAP_TIME_SECONDS)
        ).filter(pl.col(COL_LAP_TIME_SECONDS).is_not_null())

    # For pandas: check if already timedeltas
    if pd.api.types.is_timedelta64_ns_dtype(laps[COL_LAP_TIME]):
        valid = laps[laps[COL_LAP_TIME].notna()].copy()
        valid[COL_LAP_TIME_SECONDS] = (
            cast(pd.Series, valid[COL_LAP_TIME]).dt.total_seconds().to_numpy(copy=False)
        )
        return valid

    # For pandas: minimize copies by filtering before copying
    lap_time_numeric = pd.to_numeric(laps[COL_LAP_TIME], errors="coerce")
    valid_mask = lap_time_numeric.notna()

    # Filter first (view operation), then copy only the filtered result
    valid = laps[valid_mask].copy()
    if valid.empty:
        return valid

    # Canonical pandas contract: Timedelta LapTime + numeric LapTimeSeconds.
    lap_time_seconds = lap_time_numeric[valid_mask].to_numpy(copy=False)
    valid[COL_LAP_TIME] = pd.to_timedelta(lap_time_seconds, unit="s")
    valid[COL_LAP_TIME_SECONDS] = lap_time_seconds
    return valid


def _rename_columns(df, rename_map: dict, lib: str):
    """Rename DataFrame columns based on lib, avoiding duplicates."""
    cols_to_rename = {k: v for k, v in rename_map.items() if k in df.columns and v is not None}
    cols_to_drop = [k for k, v in rename_map.items() if k in df.columns and v is None]

    # Drop columns marked with None
    if cols_to_drop:
        if lib == "polars" and _ensure_polars_available() and isinstance(df, pl.DataFrame):
            df = df.drop(cols_to_drop)
        else:
            df = df.drop(columns=cols_to_drop)

    # Check for duplicate target names and skip them to avoid column name conflicts
    target_names = {}
    final_rename = {}
    existing_columns = set(df.columns)
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


def _get_lap_number(row: dict) -> int:
    """Safely extract lap number from row."""
    lap_num = row.get("LapNumber") or row.get("lap")
    if lap_num is None:
        raise ValueError("No lap number found in row")
    try:
        return int(lap_num)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid lap number: {lap_num}") from e


def _create_telemetry_df(tel_data: dict, driver: str, lap_num: int, lib: str) -> DataFrame | None:
    """Create telemetry DataFrame with driver and lap metadata (zero-copy optimized).

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
        if not isinstance(v, list):
            continue
        renamed_key = TELEMETRY_RENAME_MAP.get(k, k)
        col_data[renamed_key] = v
        if expected_len is None:
            expected_len = len(v)

    if expected_len is None or expected_len == 0:
        return None

    try:
        if lib == "polars" and _ensure_polars_available():
            telemetry_df = pl.DataFrame(col_data, strict=False)
            if telemetry_df.is_empty():
                return None
            for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
                if col not in telemetry_df.columns:
                    telemetry_df = telemetry_df.with_columns(pl.lit(None).alias(col))
            telemetry_df = telemetry_df.with_columns(
                [
                    pl.lit(driver).alias("Driver"),
                    pl.lit(lap_num).alias("LapNumber"),
                ]
            )
            return telemetry_df

        # Normalize mismatched lengths by padding with NaN
        max_len = max(len(v) for v in col_data.values())
        normalized_data = {}
        for k, v in col_data.items():
            if len(v) < max_len:
                # Pad with None (becomes NaN in pandas)
                normalized_data[k] = v + [None] * (max_len - len(v))
            else:
                normalized_data[k] = v

        telemetry_df = pd.DataFrame(normalized_data, copy=False)
        if telemetry_df.empty:
            return None

        # Apply dtype conversions for telemetry data
        # Time: float seconds → timedelta64[ns]
        if "Time" in telemetry_df.columns:
            telemetry_df["Time"] = pd.to_timedelta(telemetry_df["Time"], unit="s")

        # Brake: int (0/1) → bool
        if "Brake" in telemetry_df.columns:
            telemetry_df["Brake"] = telemetry_df["Brake"].astype(bool)

        # nGear, DRS: int → Int64 (nullable)
        if "nGear" in telemetry_df.columns:
            telemetry_df["nGear"] = telemetry_df["nGear"].astype("Int64")
        if "DRS" in telemetry_df.columns:
            telemetry_df["DRS"] = telemetry_df["DRS"].astype("Int64")

        telemetry_df["Driver"] = driver
        telemetry_df["LapNumber"] = lap_num
        telemetry_df["LapNumber"] = telemetry_df["LapNumber"].astype("Int64")

        for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
            if col not in telemetry_df.columns:
                telemetry_df[col] = pd.NA
        return telemetry_df
    except Exception as e:
        logger.warning(f"Failed to create telemetry DataFrame: {e}")
        return None


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

    current_cols = list(df.columns)

    # Build ordered column list: fastf1 columns first (if present), then extras
    ordered_cols = []
    seen = set()

    # Add columns in FastF1 order if they exist
    for col in FASTF1_LAPS_COLUMN_ORDER:
        if col in current_cols:
            ordered_cols.append(col)
            seen.add(col)

    # Add any remaining columns not in the FastF1 order
    ordered_cols.extend(col for col in current_cols if col not in seen)

    # Reorder the DataFrame
    if lib == "polars" and _ensure_polars_available() and isinstance(df, pl.DataFrame):
        return df.select(ordered_cols)
    return df[ordered_cols]
