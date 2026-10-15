"""Helper functions for tif1 core."""

import array
import logging
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Union, cast
from urllib.parse import quote

import numpy as np
import pandas as pd

from tif1.config import get_config
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
    TELEMETRY_RENAME_MAP,
)

if TYPE_CHECKING:
    import polars as pl
else:
    # Polars is optional: importing it eagerly costs ~260 ms on every cold
    # start for pandas-only users. It loads lazily via
    # _ensure_polars_available() on the first polars backend use.
    pl = None

POLARS_AVAILABLE = False

logger = logging.getLogger(__name__)

# Shared global configuration singleton (same instance as tif1.core.config).
config = get_config()

# Type alias for DataFrame (polars resolved lazily; string ref keeps the
# annotation meaningful without importing polars at module load).
DataFrame = Union[pd.DataFrame, "pl.DataFrame"]


def _ensure_polars_available() -> bool:
    """Lazy-load Polars and refresh stale module state."""
    global pl, POLARS_AVAILABLE
    if POLARS_AVAILABLE and pl is not None:
        return True

    try:
        import polars as pl

        POLARS_AVAILABLE = True
    except ImportError:
        pl = None  # type: ignore[ty:invalid-assignment]
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
        return laps.with_columns(
            pl.col(COL_LAP_TIME).cast(pl.Float64, strict=False).alias(COL_LAP_TIME_SECONDS)
        ).filter(keep)

    # For pandas: check if already timedeltas (any resolution, e.g. timedelta64[us]
    # with pandas 3.0 unit inference)
    # Argos A5: a boolean take already yields a fresh frame under pandas 3's
    # copy-on-write, so the explicit ``.copy()`` that followed it doubled the
    # per-block copies on every pick_fastest/get_fastest_lap call. A shallow
    # copy keeps the all-valid case free of a block copy too.
    lap_time = cast(pd.Series, laps[COL_LAP_TIME])
    not_deleted = _not_deleted_mask(laps) if exclude_deleted else None
    if pd.api.types.is_timedelta64_dtype(lap_time):
        mask = lap_time.notna().to_numpy()
        if not_deleted is not None:
            mask = mask & not_deleted
        valid = laps[mask] if not mask.all() else laps.copy(deep=False)
        valid[COL_LAP_TIME_SECONDS] = (
            cast(pd.Series, valid[COL_LAP_TIME]).dt.total_seconds().to_numpy(copy=False)
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
    tel_data: dict | None, driver: str, lap_num: int, lib: str
) -> DataFrame | None:
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
            if _DRIVER_AHEAD_CHANNEL in col_data:
                col_data[_DRIVER_AHEAD_CHANNEL] = _without_null_tokens(
                    col_data[_DRIVER_AHEAD_CHANNEL]
                )
            telemetry_df = pl.DataFrame(col_data, strict=False)
            if telemetry_df.is_empty():
                return None
            telemetry_df = _coerce_polars_telemetry_sentinels(telemetry_df)
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

        telemetry_df = _typed_telemetry_frame(normalized_data, max_len, driver, lap_num)
        if telemetry_df is not None:
            if telemetry_df.empty:
                return None
            return telemetry_df

        # Canonical conversion failed on this payload: build from raw lists
        # and run the legacy per-column conversion (bad payloads still drop
        # to None via the surrounding error contract).
        telemetry_df = pd.DataFrame(normalized_data, copy=False)
        if telemetry_df.empty:
            return None

        telemetry_df["Driver"] = driver
        telemetry_df["LapNumber"] = lap_num
        return _apply_telemetry_dtypes(telemetry_df)
    except Exception as e:
        logger.warning(f"Failed to create telemetry DataFrame: {e}")
        return None


def _typed_telemetry_frame(
    normalized_data: dict[str, Any], max_len: int, driver: str, lap_num: int
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
        arrays.append(np.full(max_len, driver, dtype=object))
        columns.append("Driver")
        arrays.append(
            pd.arrays.IntegerArray(
                np.full(max_len, lap_num, dtype="int64"), np.zeros(max_len, dtype=bool)
            )
        )
        columns.append("LapNumber")
        frame = _frame_from_columns(arrays, columns, max_len)
    except (AttributeError, TypeError, ValueError):
        return None
    return frame


def _frame_from_arrays(arrays: list[Any], columns: list[str], n_rows: int) -> pd.DataFrame:
    """One-shot DataFrame from ready column arrays (no sanitize, consolidated).

    ``DataFrame._from_arrays`` is the constructor pandas' own readers use for
    pre-typed columns; it is private but stable across pandas 1.x-3.x. Should
    it ever disappear, the public constructor (slower, unconsolidated) is the
    fallback, so behaviour never depends on it.
    """
    from_arrays: Any = getattr(pd.DataFrame, "_from_arrays", None)
    if from_arrays is None:  # pragma: no cover - pandas internals changed
        return pd.DataFrame(dict(zip(columns, arrays)), copy=False)
    return cast(
        pd.DataFrame,
        from_arrays(arrays, columns=columns, index=pd.RangeIndex(n_rows), verify_integrity=False),
    )


# -- direct block assembly (Thebes TH2) ---------------------------------------
#
# ``DataFrame._from_arrays`` still runs pandas' generic column pipeline
# (sanitize every array, form blocks, consolidate, build a fresh column
# Index) - ~250 us per telemetry frame, i.e. ~40 % of the cold assembly.
# The typed builder already knows every column's dtype, so it can lay the
# blocks out itself: numpy columns of one dtype become one 2-D block (the
# same consolidated layout ``_from_arrays`` produces), extension arrays
# become one block each, and the column Index is shared between frames with
# identical columns. The private pandas API involved (``BlockManager``,
# ``new_block``, ``DataFrame._from_mgr``) is probed once at import with a
# round-trip against the public constructor; if anything drifted, or a
# build raises, ``_frame_from_arrays`` is used instead.
try:  # pragma: no cover - import-time probe of pandas internals
    from pandas._libs.internals import BlockPlacement as _BlockPlacement
    from pandas.core.construction import (
        ensure_wrapped_if_datetimelike as _ensure_wrapped_if_datetimelike,
    )
    from pandas.core.internals import BlockManager as _BlockManager
    from pandas.core.internals.blocks import new_block as _new_block

    _DIRECT_BLOCKS_AVAILABLE = callable(getattr(pd.DataFrame, "_from_mgr", None))
except (ImportError, AttributeError):  # pragma: no cover - pandas internals changed
    _DIRECT_BLOCKS_AVAILABLE = False

# Column ``Index`` objects shared between frames with identical columns
# (an Index is immutable; building one costs ~100 us per frame otherwise).
_COLUMN_INDEX_CACHE: dict[tuple[str, ...], pd.Index] = {}
_COLUMN_INDEX_CACHE_MAX = 64


def _shared_column_index(columns: list[str]) -> pd.Index:
    key = tuple(columns)
    index = _COLUMN_INDEX_CACHE.get(key)
    if index is None:
        if len(_COLUMN_INDEX_CACHE) >= _COLUMN_INDEX_CACHE_MAX:
            _COLUMN_INDEX_CACHE.clear()
        index = pd.Index(list(columns))
        _COLUMN_INDEX_CACHE[key] = index
    return index


def _frame_from_blocks(blocks: tuple[Any, ...], columns: list[str], n_rows: int) -> pd.DataFrame:
    """DataFrame over ready 2-D blocks (``(ncols, n_rows)`` values + placements)."""
    mgr = _BlockManager(
        blocks, [_shared_column_index(columns), pd.RangeIndex(n_rows)], verify_integrity=False
    )
    return cast(pd.DataFrame, pd.DataFrame._from_mgr(mgr, axes=mgr.axes))


def _numpy_block(values: np.ndarray, placement: Any) -> Any:
    """A 2-D numpy block; datetime-like values are wrapped the way pandas does."""
    if values.dtype.kind in "mM":
        values = _ensure_wrapped_if_datetimelike(values)
    return _new_block(values, placement=_BlockPlacement(placement), ndim=2)


def _direct_frame_from_columns(arrays: list[Any], columns: list[str], n_rows: int) -> pd.DataFrame:
    """Assemble ready 1-D columns into a consolidated frame without sanitizing."""
    numpy_groups: dict[np.dtype, list[tuple[int, np.ndarray]]] = {}
    ea_columns: list[tuple[int, Any]] = []
    for position, column in enumerate(arrays):
        if isinstance(column, np.ndarray):
            arr = column
        elif type(column).__name__ == "NumpyExtensionArray":
            arr = np.asarray(column)
        else:
            if len(column) != n_rows:
                raise ValueError("column length mismatch")
            ea_columns.append((position, column))
            continue
        if arr.ndim != 1 or arr.shape[0] != n_rows:
            raise ValueError("column shape mismatch")
        numpy_groups.setdefault(arr.dtype, []).append((position, arr))
    blocks: list[Any] = []
    for dtype, items in numpy_groups.items():
        if len(items) == 1:
            position, arr = items[0]
            blocks.append(_numpy_block(arr.reshape(1, n_rows), position))
        else:
            values = np.vstack([arr for _, arr in items])
            placement = np.fromiter((pos for pos, _ in items), dtype="int64", count=len(items))
            blocks.append(_numpy_block(values, placement))
        del dtype
    for position, column in ea_columns:
        blocks.append(_new_block(column, placement=_BlockPlacement(position), ndim=2))
    return _frame_from_blocks(tuple(blocks), columns, n_rows)


def _probe_direct_blocks() -> bool:
    """Round-trip a small typed frame through the direct path and the public one."""
    if not _DIRECT_BLOCKS_AVAILABLE:
        return False
    try:
        columns = ["Time", "RPM", "Speed", "nGear", "Brake", "DriverAhead", "Driver", "LapNumber"]
        arrays: list[Any] = [
            np.array([0, 16_000_000, 256_000_000], dtype="m8[ns]"),
            np.array([10000.0, 10500.0, np.nan]),
            np.array([250.0, 251.5, 252.0]),
            pd.arrays.IntegerArray(np.array([7, 8, 8]), np.zeros(3, dtype=bool)),
            np.array([False, True, False]),
            pd.Series(["1", None, "44"]).array,
            np.full(3, "VER", dtype=object),
            pd.arrays.IntegerArray(np.full(3, 5), np.zeros(3, dtype=bool)),
        ]
        direct = _direct_frame_from_columns(arrays, columns, 3)
        public = _frame_from_arrays(arrays, columns, 3)
        return bool(
            list(direct.columns) == list(public.columns)
            and direct.dtypes.equals(public.dtypes)
            and direct.equals(public)
            and len(direct._mgr.blocks) == len(public._mgr.blocks)
        )
    except Exception:  # noqa: BLE001 - any drift disables the fast path
        return False


_DIRECT_BLOCKS_OK = _probe_direct_blocks()


def _frame_from_columns(arrays: list[Any], columns: list[str], n_rows: int) -> pd.DataFrame:
    """Consolidated DataFrame from ready 1-D columns (direct blocks, else ``_from_arrays``)."""
    if _DIRECT_BLOCKS_OK:
        try:
            return _direct_frame_from_columns(arrays, columns, n_rows)
        except (AttributeError, TypeError, ValueError):
            pass
    return _frame_from_arrays(arrays, columns, n_rows)


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
    telemetry_df = cast(pd.DataFrame, telemetry_df)

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


_NULL_LIKE_TOKEN_PROBE = _NULL_LIKE_STRINGS | {"None"}
_NULL_LIKE_TOKEN_ARRAY = np.array(sorted(_NULL_LIKE_TOKEN_PROBE))

# Telemetry channels that are numeric by contract. Upstream encodes a missing
# value as the literal string "None" (e.g. ``DistanceToDriverAhead`` on a race
# leader's lap, where nobody is ahead); left alone, one sentinel stringifies
# the whole column on both backends. The frame builders coerce it to NaN/null.
_NUMERIC_TELEMETRY_CHANNELS = frozenset(
    {
        "RPM",
        "Speed",
        "Throttle",
        "Distance",
        "RelativeDistance",
        "DistanceToDriverAhead",
        "AccelerationX",
        "AccelerationY",
        "AccelerationZ",
        "X",
        "Y",
        "Z",
    }
)
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


def _coerce_polars_telemetry_sentinels(frame):
    """Cast numeric telemetry channels polars stringified because of a sentinel.

    Schema lookups only on a clean frame; the non-strict Float64 cast
    (sentinel -> null) runs just for a channel that came out as String.
    ``DriverAhead`` is cleaned on the Python list before construction, so
    polars infers a String column with nulls directly.
    """
    schema = frame.schema
    exprs = [
        pl.col(col).cast(pl.Float64, strict=False)
        for col in _NUMERIC_TELEMETRY_CHANNELS
        if schema.get(col) == pl.String
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
    _BOOL_COLS = ("IsPersonalBest", "FreshTyre", "FastF1Generated", "IsAccurate", "Rainfall")
    for col in _BOOL_COLS:
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
    if data and any(
        isinstance(v, list | tuple)
        and any(isinstance(x, str) and x in _NULL_LIKE_TOKEN_PROBE for x in v)
        for v in data.values()
    ):
        return {
            k: _coerce_null_like_string_list(list(v)) if isinstance(v, list | tuple) else v
            for k, v in data.items()
        }
    return data


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


def _process_lap_df(lap_df, lib: str) -> DataFrame:
    """Apply column renaming, dtype coercions, and categorical types."""
    if lib == "polars":
        _ensure_polars_available()
    # Remove duplicate columns if they exist (pandas only) - must be done FIRST
    if lib == "pandas" and isinstance(lap_df.columns, pd.Index):
        if lap_df.columns.duplicated().any():
            lap_df = lap_df.loc[:, ~lap_df.columns.duplicated()]

    lap_df = _rename_columns(lap_df, LAP_RENAME_MAP, lib)
    if lib == "pandas" and COL_LAP_TIME in lap_df.columns:
        lap_time_series = cast(pd.Series, lap_df[COL_LAP_TIME])
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
            cast(pd.Series, lap_df[COL_LAP_TIME]).dt.total_seconds().to_numpy(copy=False)
        )
    if lib == "pandas" and "Time" in lap_df.columns:
        time_series = cast(pd.Series, lap_df["Time"])
        if not pd.api.types.is_timedelta64_dtype(time_series):
            # Only convert if it's actually a Series (not already converted)
            if isinstance(time_series, pd.Series):
                lap_df["Time"] = _numeric_seconds_to_timedelta(time_series)
    if lib == "pandas" and "WeatherTime" in lap_df.columns:
        weather_time_series = cast(pd.Series, lap_df["WeatherTime"])
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
        A dict mapping renamed column names to concatenated lists, with
        ``Driver`` and ``LapNumber`` columns appended in payload order.
    """
    # Collect column order by first appearance (dict preserves insertion order).
    all_columns: dict[str, None] = {}
    normalized_entries: list[tuple[dict, int, str, int]] = []
    for driver, lap_num, tel_payload in tel_entries:
        if not isinstance(tel_payload, dict) or not tel_payload:
            continue
        col_data = {
            TELEMETRY_RENAME_MAP.get(k, k): v for k, v in tel_payload.items() if isinstance(v, list)
        }
        if not col_data:
            continue
        max_len = max(len(v) for v in col_data.values())
        if max_len == 0:
            continue
        normalized = {
            k: (v + [None] * (max_len - len(v))) if len(v) < max_len else v
            for k, v in col_data.items()
        }
        for col in normalized:
            all_columns.setdefault(col, None)
        normalized_entries.append((normalized, max_len, driver, lap_num))

    merged: dict[str, list] = {}
    driver_col: list[str] = []
    lap_col: list[int] = []
    for normalized, n_rows, driver, lap_num in normalized_entries:
        driver_col.extend([driver] * n_rows)
        lap_col.extend([lap_num] * n_rows)
        for col in all_columns:
            values = normalized.get(col)
            if values is None:
                merged.setdefault(col, []).extend([None] * n_rows)
            else:
                merged.setdefault(col, []).extend(values)
    merged[COL_DRIVER] = driver_col
    merged[COL_LAP_NUMBER] = lap_col
    return merged


def _telemetry_frame_from_merged(merged: dict) -> pd.DataFrame:
    """Build a telemetry DataFrame from a merged dict, applying dtypes once.

    Applies the same dtype conversions that :func:`_create_telemetry_df`
    performs per driver frame (via the shared :func:`_apply_telemetry_dtypes`),
    but a single pass on the merged frame — which is what makes merged-dict
    assembly ~2-3x faster than per-frame construction + concat on pandas 3.x.
    """
    if not merged:
        return pd.DataFrame()
    return cast(pd.DataFrame, _apply_telemetry_dtypes(pd.DataFrame(merged, copy=False)))
