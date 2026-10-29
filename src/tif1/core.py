"""Core functionality for tif1."""

import logging
import math
import numbers
import re
import threading
import warnings
from collections.abc import Iterable
from typing import Any, Literal, cast
from urllib.parse import unquote

import numpy as np
import pandas as pd

from .async_fetch import _validate_json_payload as _validate_json_impl
from .async_fetch import fetch_multiple_async
from .cache import (
    FASTEST_LAP_REF_MISS,
    LRUCache,  # noqa: F401  # re-exported for backward compatibility
    SessionMemo,
    _suspend_gc,
    clear_lap_cache,  # noqa: F401  # re-exported alias (canonical home: tif1.cache)
    get_backend_lap_cache,
    get_cache,
    register_frame_writer,
)
from .cdn import get_cdn_manager
from .config import get_config
from .core_utils import ergast_results as _er
from .core_utils import session_state as _ss

# Import from core utilities modules
from .core_utils.constants import (
    COL_DELETED,
    COL_DRIVER,
    COL_LAP_NUMBER,
    COL_LAP_NUMBER_ALT,
    COL_LAP_TIME,
    COL_LAP_TIME_SECONDS,
    COL_TEAM,
    ERGAST_LOADABLE_SESSIONS,
    EVENT_NAME_ALIASES,
    F1_API_FIRST_YEAR,
    MAX_YEAR,
    MIN_YEAR,
    RACE_CONTROL_RENAME_MAP,
    TELEMETRY_RENAME_MAP,
    WEATHER_RENAME_MAP,
    telemetry_ref_from_path,
)
from .core_utils.helpers import (
    DataFrame,
    _coerce_lap_number,
    _coerce_null_like_payload,
    _create_empty_df,
    _create_lap_df,
    _create_telemetry_df,
    _encode_url_component,
    _filter_valid_laptimes,
    _get_lap_column,
    _is_empty_df,
    _merge_telemetry_payloads,
    _normalize_lap_payload,
    _normalize_lap_payload_polars,
    _not_deleted_mask,
    _numeric_seconds_to_timedelta,
    _process_lap_df,
    _rename_columns,
    _replace_null_like_strings,
    _telemetry_frame_from_merged,
    _validate_drivers_list,
    _validate_string_param,
    _validate_year,
)
from .core_utils.runtime import (
    IS_EMSCRIPTEN,
    THREADS_AVAILABLE,
    import_supported_polars,
    run_sync_in_browser,
)
from .exceptions import (
    AsyncRequiredError,
    DataNotFoundError,
    DriverNotFoundError,
    EventNotFoundError,
    InvalidDataError,
    LapNotFoundError,  # noqa: F401  # re-exported for backward compatibility
    NetworkError,
    TIF1Error,
)
from .models import (
    CircuitInfo,
    Driver,
    DriverResult,  # noqa: F401  # re-exported for backward compatibility
    Lap,  # noqa: F401  # re-exported for backward compatibility
    Laps,
    LazyTelemetryDict,
    SessionResults,
    Telemetry,
    _empty_driver_laps,
    _empty_telemetry,
    _IterLapResult,  # noqa: F401  # re-exported for backward compatibility
    _LapInternal,  # noqa: F401  # re-exported for backward compatibility
    _load_driver_lap_payload,
    _timedelta_ns,
)
from .payload_loader import PayloadLoader
from .types import ScheduleBackend

pl: Any = None
POLARS_AVAILABLE: bool | None = None

logger = logging.getLogger(__name__)


def _ensure_polars_available() -> bool:
    """Lazy-load Polars only when explicitly requested.

    This re-checks imports when a prior probe failed so notebook users can
    install polars mid-session and immediately use the polars lib.
    """
    global pl, POLARS_AVAILABLE
    if POLARS_AVAILABLE is True and pl is not None:
        return True

    pl = import_supported_polars()
    POLARS_AVAILABLE = pl is not None
    return POLARS_AVAILABLE


def _polars_models() -> Any:
    """:mod:`tif1.models_polars` (imports polars; only polars sessions call it)."""
    from . import models_polars

    return models_polars


def _polars_error_types() -> tuple[type[Exception], ...]:
    """polars' exception base, or ``()`` when polars is not loaded.

    polars raises ``PolarsError`` subclasses (not ``ValueError``/``TypeError``)
    for dtype failures, so the optional-table loaders list it next to the
    pandas equivalents. Never imports polars: pandas sessions must not pay
    the ~260 ms import for it.
    """
    return (pl.exceptions.PolarsError,) if pl is not None else ()


# Get configuration
config = get_config()
SESSION_LAPTIMES_PATH = "session_laptimes.json"


def _validate_json_payload(path: str, data: dict[str, Any]) -> dict[str, Any]:
    """Validate JSON payload using global config (wrapper for async_fetch version)."""
    return _validate_json_impl(path, data, config)


# Applied on-demand only when a blocking sync API is called from an async loop.
_NEST_ASYNCIO2_APPLIED = False


# Helper functions
def _ensure_nested_loop_support(operation: str) -> None:
    """Apply nest_asyncio2 on demand for sync APIs invoked inside an async loop."""
    import asyncio  # lazy: only async/network paths need it

    global _NEST_ASYNCIO2_APPLIED

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return

    if _NEST_ASYNCIO2_APPLIED:
        return

    try:
        import nest_asyncio2

        nest_asyncio2.apply()
        _NEST_ASYNCIO2_APPLIED = True
        logger.info(f"Applied nest_asyncio2 dynamically for {operation}")
    except ImportError as e:
        raise RuntimeError(
            f"{operation} cannot run inside an active event loop without nest_asyncio2. "
            "Use async APIs (e.g. laps_async) or install nest-asyncio2."
        ) from e


def _async_alternative(operation: str) -> str:
    """The ``await``-able call that replaces the synchronous ``operation``."""
    candidate = f"{operation}_async"
    if callable(getattr(Session, candidate, None)):
        return f"session.{candidate}()"
    return "session.laps_async()"


def _run_coroutine_sync(factory: Any, operation: str, *, alternative: str | None = None) -> Any:
    """Run the coroutine ``factory`` builds to completion from synchronous code.

    The one bridge every synchronous entry point uses. Natively it applies the
    nested-loop shim and ``asyncio.run``s the coroutine. Pyodide cannot block
    on its browser event loop: there the coroutine runs through JavaScript
    Promise Integration when the runtime offers it, otherwise
    :class:`~tif1.exceptions.AsyncRequiredError` names ``alternative``, the
    async call to await instead.
    """
    if IS_EMSCRIPTEN:
        return run_sync_in_browser(
            factory, operation, alternative=alternative or _async_alternative(operation)
        )
    import asyncio  # lazy: only async/network paths need it

    _ensure_nested_loop_support(operation)
    return asyncio.run(factory())


def _is_nan(value: Any) -> bool:
    """True for a float NaN (never raises for non-numeric values)."""
    return isinstance(value, float) and math.isnan(value)


def _float_marker_distances(frame: Any) -> np.ndarray:
    """A circuit marker frame's ``Distance`` as float64 (NaN where unknown), either backend."""
    column = frame["Distance"]
    if isinstance(column, pd.Series):
        return column.to_numpy(dtype=float, na_value=np.nan)
    return column.cast(float).to_numpy().astype(float, copy=False)


def _pandas_ns_values(column: pd.Series, mask: np.ndarray, *, timedelta: bool) -> np.ndarray:
    """A pandas clock column's ``mask`` rows as int64 ns (int64 min where unknown).

    ``timedelta64``/naive ``datetime64`` of any resolution convert as arrays
    (pandas 3 keeps ``ms``/``us`` units); other dtypes go value by value.
    """
    kind = column.dtype.kind if isinstance(column.dtype, np.dtype) else ""
    if timedelta and kind == "m":
        return column.to_numpy()[mask].astype("timedelta64[ns]").view("int64")
    if not timedelta and kind == "M":
        return column.to_numpy()[mask].astype("datetime64[ns]").view("int64")
    values = column.to_numpy(dtype=object)[mask].tolist()
    return _ss._timedelta_ns(values) if timedelta else _ss._datetime_ns(values)


def _polars_ns_values(column: Any, mask: np.ndarray, *, timedelta: bool) -> np.ndarray:
    """A polars clock column's ``mask`` rows as int64 ns (int64 min where unknown).

    ``Duration("ns")`` / naive ``Datetime("ns")`` convert straight from
    ``to_numpy()`` (no query plan); any other dtype goes through the
    value-by-value session-state helpers.
    """
    dtype = column.dtype
    unit_ns = getattr(dtype, "time_unit", None) == "ns"
    if timedelta and isinstance(dtype, pl.Duration) and unit_ns:
        return column.to_numpy()[mask].view("int64")
    if (
        not timedelta
        and isinstance(dtype, pl.Datetime)
        and unit_ns
        and getattr(dtype, "time_zone", None) is None
    ):
        return column.to_numpy()[mask].view("int64")
    values = column.to_numpy()[mask].tolist()
    return _ss._timedelta_ns(values) if timedelta else _ss._datetime_ns(values)


def _fastest_laps_polars(laps: Any, *, by_driver: bool, drivers: list[str] | None) -> Any:
    """``Session._select_fastest_laps`` for a polars frame, on column ``to_numpy()`` arrays.

    The expression path ran ~5 query plans for a ~1,100-row table (the
    process's first plan alone costs ~7 ms while polars starts its thread
    pool). Same rows: laps of ``drivers`` with a time that are not deleted;
    per driver the fastest (the first on ties), ordered by time with ties in
    the drivers' first-appearance order; or the single fastest lap.
    ``LapTimeSeconds`` is set from ``LapTime`` as ``_filter_valid_laptimes``
    does. Returns None for a frame it does not handle (a non-``Duration``
    ``LapTime``, a non-boolean ``Deleted``, a null driver code): the caller
    keeps the expression path.
    """
    columns = laps.columns
    if COL_LAP_TIME not in columns or COL_DRIVER not in columns:
        return None
    lap_time = laps[COL_LAP_TIME]
    if not isinstance(lap_time.dtype, pl.Duration):
        return None
    raw = lap_time.to_numpy().astype("timedelta64[ns]").view("int64")
    keep = raw != _ss._NS_MIN
    if COL_DELETED in columns:
        deleted = laps[COL_DELETED]
        if deleted.dtype != pl.Boolean:
            return None
        keep &= ~(deleted.to_numpy() == True)  # noqa: E712 - object array with None
    codes = laps[COL_DRIVER].to_numpy()
    if any(code is None for code in codes):
        return None
    if drivers:
        keep &= np.isin(codes, list(drivers))
    rows = np.flatnonzero(keep)
    seconds = raw.astype("float64") / 1_000_000_000
    if not len(rows):
        picked: list[int] = []
    elif not by_driver:
        picked = [int(rows[np.argmin(seconds[rows])])]
    else:
        first_seen: dict[Any, int] = {}
        for position, code in enumerate(codes[rows].tolist()):
            first_seen.setdefault(code, position)
        best: dict[Any, int] = {}
        for row in rows[np.argsort(seconds[rows], kind="stable")].tolist():
            best.setdefault(codes[row], row)
        picked = sorted(best.values(), key=lambda row: (seconds[row], first_seen[codes[row]]))
    base = pl.DataFrame._from_pydf(laps._df.clone())
    result = base[np.asarray(picked, dtype="int64")]
    lap_seconds = pl.Series(COL_LAP_TIME_SECONDS, seconds[picked], dtype=pl.Float64)
    if COL_LAP_TIME_SECONDS in result.columns:
        result.replace_column(result.get_column_index(COL_LAP_TIME_SECONDS), lap_seconds)
    else:
        result.hstack([lap_seconds], in_place=True)
    return result


def _identifier_car_number(identifier: Any) -> str | None:
    """The car number a ``get_driver`` identifier names (``1``, ``"8"``, ``"8/FAG"``), else None."""
    if isinstance(identifier, bool) or not isinstance(identifier, str | int):
        return None
    number = str(identifier).strip().partition("/")[0].strip()
    return number if number.isdigit() else None


def _polars_result_position(results: Any, driver: str, number: str) -> int | None:
    """Row of ``driver`` in a polars results table (pandas ``Driver`` rule).

    The abbreviation names the driver; the car number decides between two
    rows with one abbreviation and stands in when the code is unknown.
    """
    codes = results["Abbreviation"].cast(pl.String).to_list()
    numbers = results["DriverNumber"].cast(pl.String).to_list()
    matches = [i for i, code in enumerate(codes) if code == driver]
    if len(matches) > 1:
        matches = [i for i in matches if numbers[i] == number] or matches[:1]
    if not matches and number:
        matches = [i for i, value in enumerate(numbers) if value == number]
    return matches[0] if matches else None


def _extract_driver_codes(drivers: list[dict] | None) -> set[str]:
    """Extract valid driver codes from drivers payload."""
    if not drivers:
        return set()

    codes: set[str] = set()
    for driver_info in drivers:
        if isinstance(driver_info, dict):
            code = driver_info.get("driver")
            if isinstance(code, str):
                codes.add(code)
    return codes


def _extract_driver_info_map(drivers: list[dict] | None) -> dict[str, dict]:
    """Build driver-code lookup map from drivers payload."""
    if not drivers:
        return {}

    driver_info_map: dict[str, dict] = {}
    for driver_info in drivers:
        if not isinstance(driver_info, dict):
            continue
        code = driver_info.get("driver")
        if isinstance(code, str):
            driver_info_map[code] = driver_info
    return driver_info_map


_RACE_LIKE_SESSION_NAMES = frozenset({"Race", "Sprint"})
_UNSET: Any = object()
_ERGAST_TABLE_ENDPOINTS = ("results", "qualifying", "sprint", "laps", "pitstops")
# Serializes background frame-tier materialization writes (Pergamon Q15).
_FRAME_MATERIALIZATION_LOCK = threading.Lock()


def _is_number_sorted(numbers: list[str]) -> bool:
    """True when a driver-number list is merely sorted (as strings or as ints), not classified."""
    if len(numbers) < 2:
        return False
    if numbers == sorted(numbers):
        return True
    if all(number.isdigit() for number in numbers):
        as_int = [int(number) for number in numbers]
        return as_int == sorted(as_int)
    return False


def _clean_scalar(value: Any) -> Any:
    """``None`` for missing/sentinel values (None, NaN, NA, "", "None"), else the value."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        return None if value in ("", "None", "nan", "NaN") else value
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _session_driver_rows(drivers: Any) -> list[Any]:
    """The session's own copies of the ``drivers.json`` rows.

    The payload is shared through the JSON memo; car numbers and names are
    backfilled into these dicts in place, which must not reach other sessions.
    """
    if not isinstance(drivers, list):
        return []
    return [dict(row) if isinstance(row, dict) else row for row in drivers]


_PRACTICE_SESSIONS = frozenset({"Practice 1", "Practice 2", "Practice 3"})


def _best_lap_timedelta(info: dict[str, Any] | None) -> Any:
    """A lap summary's best lap (seconds) as a ``Timedelta``, ``NaT`` without one."""
    best = info.get("best") if info is not None else None
    if best is None:
        return pd.NaT
    # LapTimeSeconds is total_seconds() of the LapTime: rounding to the
    # nanosecond recovers the exact Timedelta.
    return pd.Timedelta(round(best * 1e9), unit="ns")


def _summarize_laps_for_classification(laps: Any, lib: str) -> dict[str, dict[str, Any]]:
    """One pass over the laps table: per driver number, laps completed, last-lap position, best lap.

    Works on either backend by pulling the few needed columns out as Python
    lists (about 1,000 rows per session), so it costs ~1 ms and needs no
    backend conversion.
    """
    wanted = ["Driver", "DriverNumber", "LapNumber", "Position", "LapTimeSeconds", "Deleted"]
    present = [column for column in wanted if column in laps.columns]
    if "Driver" not in present:
        return {}
    if lib != "polars":
        fast = _summarize_pandas_laps_vectorized(laps)
        if fast is not None:
            return fast
    if lib == "polars":
        columns = laps.select(present).to_dict(as_series=False)
    else:
        columns = {column: laps[column].tolist() for column in present}

    drivers = columns["Driver"]
    numbers = columns.get("DriverNumber")
    lap_numbers = columns.get("LapNumber")
    positions = columns.get("Position")
    times = columns.get("LapTimeSeconds")
    deleted = columns.get("Deleted")

    summary: dict[str, dict[str, Any]] = {}
    for i, raw_code in enumerate(drivers):
        code = _clean_scalar(raw_code)
        if code is None:
            continue
        key = str(code)
        entry: dict[str, Any] | None = summary.get(key)
        if entry is None:
            entry = {"number": None, "laps": 0, "position": None, "best": None}
            summary[key] = entry
        if numbers is not None and entry["number"] is None:
            number = _clean_scalar(numbers[i])
            if number is not None:
                entry["number"] = str(int(number)) if isinstance(number, float) else str(number)
        if lap_numbers is not None:
            lap_number = _clean_scalar(lap_numbers[i])
            if lap_number is not None and int(lap_number) >= entry["laps"]:
                entry["laps"] = int(lap_number)
                position = _clean_scalar(positions[i]) if positions is not None else None
                entry["position"] = float(position) if position is not None else None
        if times is not None:
            lap_time = _clean_scalar(times[i])
            if lap_time is not None and not (deleted is not None and deleted[i] is True):
                lap_time = float(lap_time)
                if entry["best"] is None or lap_time < entry["best"]:
                    entry["best"] = lap_time
    return summary


def _summarize_pandas_laps_vectorized(laps: pd.DataFrame) -> dict[str, dict[str, Any]] | None:
    """Vectorized :func:`_summarize_laps_for_classification` for a pandas laps frame (Q19).

    Same result as the row loop (which ran ``_clean_scalar`` four times per
    row, ~3.5 ms on a 1,455-lap race): per driver, the first usable car
    number, the highest truncated lap number with the position of the last
    row reaching it, and the best non-deleted lap time. The per-row helpers
    still decide the few per-driver scalars. Returns None (row loop) when the
    frame lacks the float64 ``LapNumber``/``LapTimeSeconds`` columns or two
    driver values clean to the same key.
    """
    columns = laps.columns
    if not columns.is_unique or "LapNumber" not in columns or "LapTimeSeconds" not in columns:
        return None
    lap_numbers = laps["LapNumber"].to_numpy()
    times = laps["LapTimeSeconds"].to_numpy()
    if lap_numbers.dtype != np.float64 or times.dtype != np.float64:
        return None
    codes, uniques = pd.factorize(laps["Driver"], use_na_sentinel=True)
    keys: list[str | None] = []
    for raw_code in uniques.tolist():
        code = _clean_scalar(raw_code)
        keys.append(None if code is None else str(code))
    clean_keys = [key for key in keys if key is not None]
    if len(set(clean_keys)) != len(clean_keys):
        return None
    n_groups = len(keys)
    n_rows = len(codes)
    rows = np.arange(n_rows)
    grouped = codes >= 0

    # laps / position: highest int(LapNumber) (truncation) >= 0, last row reaching it.
    truncated = np.trunc(lap_numbers)
    lap_ok = grouped & ~np.isnan(lap_numbers)
    max_lap = np.full(n_groups, -np.inf)
    np.maximum.at(max_lap, codes[lap_ok], truncated[lap_ok])
    at_max = lap_ok.copy()
    at_max[lap_ok] = truncated[lap_ok] == max_lap[codes[lap_ok]]
    last_at_max = np.full(n_groups, -1, dtype=np.int64)
    np.maximum.at(last_at_max, codes[at_max], rows[at_max])

    # best: minimum non-NaN LapTimeSeconds over rows whose Deleted is not True.
    time_ok = grouped & ~np.isnan(times)
    if "Deleted" in columns:
        deleted = laps["Deleted"].tolist()
        time_ok &= ~np.fromiter((value is True for value in deleted), dtype=bool, count=n_rows)
    best = np.full(n_groups, np.inf)
    np.minimum.at(best, codes[time_ok], times[time_ok])
    has_best = np.bincount(codes[time_ok], minlength=n_groups) > 0

    numbers = laps["DriverNumber"].tolist() if "DriverNumber" in columns else None
    positions = laps["Position"].tolist() if "Position" in columns else None
    group_rows: list[np.ndarray] = []
    if numbers is not None:
        order = np.argsort(codes, kind="stable")
        bounds = np.searchsorted(codes[order], np.arange(n_groups + 1))
        group_rows = [order[bounds[g] : bounds[g + 1]] for g in range(n_groups)]

    summary: dict[str, dict[str, Any]] = {}
    for group, key in enumerate(keys):
        if key is None:
            continue
        entry: dict[str, Any] = {"number": None, "laps": 0, "position": None, "best": None}
        if numbers is not None:
            for i in group_rows[group].tolist():
                number = _clean_scalar(numbers[i])
                if number is not None:
                    entry["number"] = str(int(number)) if isinstance(number, float) else str(number)
                    break
        if last_at_max[group] >= 0 and max_lap[group] >= 0:
            entry["laps"] = int(max_lap[group])
            position = (
                _clean_scalar(positions[int(last_at_max[group])]) if positions is not None else None
            )
            entry["position"] = float(position) if position is not None else None
        if has_best[group]:
            entry["best"] = float(best[group])
        summary[key] = entry
    return summary


_QUALI_LIKE_SESSION_NAMES = frozenset({"Qualifying", "Sprint Qualifying", "Sprint Shootout"})


def _segment_lap_times(segment: Any, lib: str) -> list[tuple[str, int]]:
    """``(driver code, lap time ns)`` of a qualifying segment's timed, not-deleted laps."""
    if "Driver" not in segment.columns or "LapTime" not in segment.columns:
        return []
    if lib == "polars":
        codes = segment["Driver"].cast(pl.String).to_list()
        times = segment["LapTime"].dt.total_nanoseconds().to_list()
        deleted = (
            segment["Deleted"].cast(pl.Boolean, strict=False).fill_null(False).to_list()
            if "Deleted" in segment.columns
            else [False] * len(codes)
        )
    else:
        codes = segment["Driver"].astype(object).tolist()
        raw = _ss._timedelta_ns(segment["LapTime"].tolist())
        times = [None if value == _ss._NS_MIN else int(value) for value in raw]
        deleted = (
            [value is True for value in segment["Deleted"].tolist()]
            if "Deleted" in segment.columns
            else [False] * len(codes)
        )
    return [
        (str(code), lap_ns)
        for code, lap_ns, gone in zip(codes, times, deleted, strict=True)
        if code is not None and lap_ns is not None and not gone
    ]


def _apply_quali_knockout(bests: dict[str, list[int | None]], entrants: int) -> None:
    """Void the Q2/Q3 times of drivers the knockout eliminated (in place).

    fastf1's lap-based classification keeps every segment time it finds, so a
    driver whose Q1 lap the stewards deleted after Q2 (2025 Bahrain: HUL, Q1
    lap 1:31.998 deleted 16 minutes after Q2, 15th -> 16th) kept a Q2 time the
    rules void. The Q1 cut is ``10 + ceil((entrants - 10) / 2)`` (15 of 20
    cars, 16 of 22), the Q2 cut 10; ties keep the drivers' order. A driver
    without a Q1 time is left alone (the segment split may have missed it).
    """
    cuts = ((0, 10 + math.ceil(max(entrants - 10, 0) / 2)), (1, 10))
    for slot, advancing in cuts:
        ranked = sorted(
            (times[slot], position, code)
            for position, (code, times) in enumerate(bests.items())
            if times[slot] is not None
        )
        for _time, _position, code in ranked[advancing:]:
            for later in range(slot + 1, 3):
                bests[code][later] = None


def _quali_sort(rows: list[dict], bests: dict[str, list[int | None]]) -> list[dict]:
    """Driver rows in fastf1's qualifying order: by Q3, then Q2, then Q1 (missing times last)."""

    def key(item: tuple[int, dict]) -> tuple[Any, ...]:
        index, row = item
        times = bests.get(str(row.get("driver", "")), [None, None, None])
        parts: list[Any] = []
        for value in (times[2], times[1], times[0]):
            parts.extend((value is None, value if value is not None else 0))
        return (*parts, index)

    return [row for _, row in sorted(enumerate(rows), key=key)]


def _quali_q1_floor(bests: dict[str, list[int | None]]) -> int | None:
    """107% of the fastest Q1 lap in ns (fastf1's not-classified rule), or None."""
    q1 = [times[0] for times in bests.values() if times[0] is not None]
    return int(min(q1) * 1.07) if q1 else None


def _quali_row_fields(times: list[int | None] | None, q1_floor: int | None) -> dict[str, Any]:
    """``Q1``-``Q3`` of a driver's results row, plus ``"N"`` for a Q1 exit outside 107%."""
    if times is None:
        return {}
    fields: dict[str, Any] = {
        name: pd.Timedelta(value, unit="ns") if value is not None else pd.NaT
        for name, value in zip(("Q1", "Q2", "Q3"), times, strict=True)
    }
    if q1_floor is not None and times[1] is None and times[0] is not None and times[0] > q1_floor:
        fields["ClassifiedPosition"] = "N"
    return fields


def _classification_sort(
    rows: list[dict], summary: dict[str, dict[str, Any]], *, race: bool
) -> list[dict]:
    """Order driver rows like a classification; drivers without laps keep their place at the back."""

    def key(item: tuple[int, dict]) -> tuple[int, int, float, int]:
        index, row = item
        info = summary.get(str(row.get("driver", "")))
        if info is None or info["laps"] == 0:
            return (1, 0, 0.0, index)
        if race:
            position = info["position"] if info["position"] is not None else math.inf
            return (0, -info["laps"], position, index)
        best = info["best"] if info["best"] is not None else math.inf
        return (0, 0, best, index)

    return [row for _, row in sorted(enumerate(rows), key=key)]


def _coerce_lap_time(lap_time_value: Any) -> float:
    """Coerce lap time to float and reject NaN-like values."""
    if lap_time_value is None:
        raise ValueError("No lap time found in row")
    try:
        lap_time = float(lap_time_value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid lap time: {lap_time_value}") from e

    if math.isnan(lap_time):
        raise ValueError(f"Invalid lap time: {lap_time_value}")
    return lap_time


def _extract_lap_numbers(laps, lib: str) -> set[int]:
    """Extract valid lap numbers for fast membership checks."""
    if _is_empty_df(laps, lib):
        return set()

    lap_col = _get_lap_column(laps, lib)
    if lap_col not in laps.columns:
        return set()

    if lib == "polars":
        laps_pl = cast(Any, laps)
        lap_values = laps_pl.get_column(lap_col).to_list()
    else:
        laps_pd = cast(pd.DataFrame, laps)
        lap_values = laps_pd[lap_col].to_numpy(copy=False)

    lap_numbers: set[int] = set()
    for lap_value in lap_values:
        try:
            lap_numbers.add(_coerce_lap_number(lap_value))
        except ValueError:
            continue
    return lap_numbers


def _cache_telemetry_payload(
    cache, year: int, gp: str, session: str, path: str, data: dict[str, Any]
) -> bool:
    """Persist a telemetry payload to the telemetry tier (single-tier write).

    Returns True when the payload was routed to the telemetry table; callers
    then skip the JSON-tier write. Payloads without a dict ``tel`` member fall
    back to the JSON tier so odd shapes keep the legacy storage contract.
    """
    ref = telemetry_ref_from_path(path)
    if ref is None:
        return False
    tel = data.get("tel") if isinstance(data, dict) else None
    if not isinstance(tel, dict) or not tel:
        return False
    set_telemetry = getattr(cache, "set_telemetry", None)
    if not callable(set_telemetry):
        return False
    driver, lap = ref
    set_telemetry(year, gp, session, driver, lap, tel)
    return True


def _merge_lap_payloads(
    payloads: Iterable[tuple[dict, str, str]],
) -> dict:
    """Merge normalized per-driver lap payloads into a single dict-of-lists.

    Produces exactly the rows that ``pd.concat([per-driver frames])`` would,
    including None-padding for columns absent from some drivers (concat fills
    those with NaN; downstream ``_process_lap_df`` coerces canonical dtypes
    either way). Building one DataFrame from a single dict is much faster than
    concatenating many small frames, which regressed ~2x in pandas 3.0.

    Args:
        payloads: Iterable of ``(normalized_lap_data, driver, team)`` tuples,
            where each ``normalized_lap_data`` is the output of
            :func:`_normalize_lap_payload`.

    Returns:
        A dict mapping column names to concatenated lists, with ``Driver`` and
        ``Team`` columns appended in payload order.
    """
    # Collect column order by first appearance (dict preserves insertion order).
    all_columns: dict[str, None] = {}
    normalized_entries: list[tuple[dict, int, str, str]] = []
    for normalized, driver, team in payloads:
        n_rows = len(next(iter(normalized.values()))) if normalized else 0
        for col in normalized:
            all_columns.setdefault(col, None)
        normalized_entries.append((normalized, n_rows, driver, team))

    merged: dict[str, list] = {}
    driver_col: list[str] = []
    team_col: list[str] = []
    for normalized, n_rows, driver, team in normalized_entries:
        driver_col.extend([driver] * n_rows)
        team_col.extend([team] * n_rows)
        for col in all_columns:
            values = normalized.get(col)
            if values is None:
                merged.setdefault(col, []).extend([None] * n_rows)
            else:
                merged.setdefault(col, []).extend(values)
    merged[COL_DRIVER] = driver_col
    merged[COL_TEAM] = team_col
    return merged


def _telemetry_payload_has_lists(tel_payload: dict | None) -> bool:
    """True if a telemetry payload carries at least one non-empty list channel.

    Mirrors the validity check inside :func:`_create_telemetry_df` (which
    returns ``None`` for payloads without list-valued channels).
    """
    if not isinstance(tel_payload, dict) or not tel_payload:
        return False
    expected_len: int | None = None
    for value in tel_payload.values():
        if isinstance(value, list | np.ndarray):
            expected_len = len(value)
            break
    return expected_len is not None and expected_len > 0


def _create_session_df(data: dict[str, Any], rename_map: dict[str, str], lib: str) -> DataFrame:
    """Create a session-level DataFrame from a payload dict and rename columns (zero-copy optimized)."""
    if lib == "polars":
        # Coerce "None"-style sentinels before construction: polars would
        # otherwise infer String for a mixed column, which no cast can undo.
        # pandas keeps its per-table passes instead (coercing here would turn
        # an all-sentinel string column into float64 NaN rather than None).
        frame = pl.DataFrame(_coerce_null_like_payload(data), strict=False)
    else:
        # Use copy=False to avoid unnecessary data duplication
        frame = pd.DataFrame(data, copy=False)

    if _is_empty_df(frame, lib):
        return _create_empty_df(lib)
    return _rename_columns(frame, rename_map, lib)


def _resolve_session_options(
    enable_cache: bool | None,
    lib: Literal["pandas", "polars"] | None,
    *,
    log_warnings: bool = True,
) -> tuple[bool, Literal["pandas", "polars"]]:
    """Resolve and normalize session options."""
    resolved_enable_cache = (
        config.get("enable_cache", True) if enable_cache is None else enable_cache
    )
    if not isinstance(resolved_enable_cache, bool):
        if log_warnings:
            logger.warning("Invalid enable_cache=%s, falling back to True", resolved_enable_cache)
        resolved_enable_cache = True
    # CI mode never reads or writes the persistent cache; turning the session's
    # cache off gates every tier at once (payloads, laps/telemetry frames).
    if config.get("ci_mode", False):
        resolved_enable_cache = False

    resolved_lib = config.get("lib", "pandas") if lib is None else lib
    if resolved_lib not in {"pandas", "polars"}:
        if log_warnings:
            logger.warning("Invalid lib=%s, falling back to pandas", resolved_lib)
        resolved_lib = "pandas"

    normalized_lib = cast(Literal["pandas", "polars"], resolved_lib)
    if normalized_lib == "polars" and not _ensure_polars_available():
        if log_warnings:
            logger.warning("Polars not available, falling back to pandas")
        normalized_lib = "pandas"

    return resolved_enable_cache, normalized_lib


class Session:
    """
    Represents an F1 session with lap and telemetry data.

    Args:
        year: Season year (2018-current)
        gp: Grand Prix name or round number (e.g., "Abu Dhabi Grand Prix" or 1)
        session: Session name
        enable_cache: Enable caching. If None, uses config value.
        lib: Data lib choice ('pandas' or 'polars'). If None, uses config value.

    Attributes:
        year: Season year
        gp: Grand Prix name (URL encoded; every CDN path is built from it)
        session: Session name (URL encoded)
        event_name: Grand Prix name as the schedule spells it
        session_name: Session name as the schedule spells it
        drivers_df: DataFrame with driver information
        laps: DataFrame with all laps
        ergast: Merge the official Ergast classification into ``results`` and
            ``drivers`` (default: the ``ergast_results`` setting, on).
            Seasons before 2018 are always served from Ergast data.
        backend: Schedule backend the event name belongs to (``"fastf1"``,
            ``"f1timing"``, ``"ergast"`` or ``None`` for the default).

    Raises:
        ValueError: If year is out of range
    """

    def __init__(
        self,
        year: int,
        gp: str | int,
        session: str,
        enable_cache: bool | None = None,
        lib: Literal["pandas", "polars"] | None = None,
        ergast: bool | None = None,
        backend: ScheduleBackend | None = None,
        *,
        _event_resolved: bool = False,
    ):
        _validate_year(year, MIN_YEAR, MAX_YEAR)
        # The schedule backend the event was resolved with; every later
        # lookup (event, sessions) must use the same calendar.
        self._schedule_backend: ScheduleBackend | None = backend
        # get_session passes the event name it already resolved
        # (``_event_resolved``); a direct construction resolves it here.
        if _event_resolved:
            gp_name = cast(str, gp)
        elif backend is None:
            gp_name = _resolve_gp_name(year, gp)
        else:
            gp_name = _resolve_gp_name(year, gp, backend=backend)
        _validate_string_param(gp_name, "gp")
        _validate_string_param(session, "session")

        resolved_enable_cache, resolved_lib = _resolve_session_options(enable_cache, lib)

        self.year = year
        self.gp = _encode_url_component(gp_name)
        self.session = _encode_url_component(session)
        self.enable_cache = resolved_enable_cache
        self.lib = resolved_lib
        self._laps = None
        self._drivers = None
        self._driver_codes = None
        self._driver_info_by_code = None
        self._driver_index_source_id = None
        # Classification-ordered driver rows and the per-driver lap facts they
        # are built from, keyed on the identity of the payload they came from.
        self._driver_rows_cache: tuple[tuple[int, int | None], list[dict], bool] | None = None
        self._lap_summary_cache: tuple[int, dict[str, dict[str, Any]]] | None = None
        # Single session-tier cache: JSON payloads, telemetry payloads/frames,
        # fastest-lap-reference memo, cache probe result, and failure tracking
        # all live in tif1.cache.SessionMemo.
        self._memo = SessionMemo()
        # Frames the memo builds lazily from decoded driver rows (Pergamon Q1)
        # get this session attached exactly like eagerly built ones.
        self._memo.frame_adopter = self._adopt_telemetry_frame
        self._fastest_lap_tel_ref: tuple[str, int] | None = None
        self._fastest_lap_tel_df: DataFrame | None = None
        self._race_control_messages: DataFrame | None = None
        self._weather: DataFrame | None = None
        self._car_data: LazyTelemetryDict | None = None
        self._results = None
        self._circuit_info: CircuitInfo | None = None
        # Derived session state (track_status, session_status, session_info,
        # t0_date, session_start_time), memoised per attribute.
        self._session_state: dict[str, Any] = {}
        # Per-driver lap start lookup (LapStartTime/LapStartDate by lap
        # number) used to give telemetry its SessionTime/Date columns.
        self._lap_start_cache: dict[str, tuple[np.ndarray, np.ndarray] | None] = {}
        self._lap_start_cache_source: int | None = None
        # (laps table, per-driver Q1/Q2/Q3 best ns): fastf1's quali results from laps.
        self._quali_bests_cache: tuple[Any, dict[str, list[int | None]]] | None = None
        # Ergast classification (tif1.ergast): the Stats repository file of
        # this round (Jolpica fallback) merged into results/drivers. Seasons
        # before the timing data (F1_API_FIRST_YEAR) are Ergast-only.
        self._ergast_only = year < F1_API_FIRST_YEAR
        self._ergast_enabled = (
            True
            if self._ergast_only
            else (bool(config.get("ergast_results", True)) if ergast is None else bool(ergast))
        )
        self._ergast_payload: dict[str, Any] | None = None
        self._ergast_loaded = False
        self._ergast_error: BaseException | None = None
        self._ergast_prefetch_started = False
        self._ergast_lock = threading.Lock()
        self._ergast_round: Any = _UNSET
        self._pit_stops: DataFrame | None = None
        self._driver_standings: DataFrame | None = None
        self._constructor_standings: DataFrame | None = None
        self._telemetry_bulk_prefetch_lock = threading.Lock()
        self._telemetry_bulk_prefetch_attempted = False
        self._telemetry_bulk_prefetch_done = False
        self._telemetry_background_prefetch_started = False
        self._session_tables_prefetched = False
        # Session-level tables (weather.json/rcm.json) are only fetched when
        # explicitly requested (load() flags or a property access); laps-only
        # flows must not pay for payloads they never read.
        self._requested_session_tables: set[str] = set()
        # Single owner of the payload pipeline (memo -> cache -> CDN fallback
        # -> retry -> validate -> parse). The fetch step routes back through
        # the overridable _fetch_from_cdn/_fetch_from_cdn_fast delegates.
        self._payload_loader = PayloadLoader(
            self.year,
            self.gp,
            self.session,
            memo=self._memo,
            enable_cache=self.enable_cache,
            fetch=self._delegate_loader_fetch,
            cache_get=self._get_from_cache,
            cache_set=self._cache_result,
        )
        logger.debug(
            "Session initialized: year=%s, gp=%s, session=%s, lib=%s",
            year,
            gp,
            session,
            self.lib,
        )

    def _session_cache_available(self) -> bool:
        """Check once whether persistent cache already has data for this session."""
        if not self.enable_cache:
            return False
        if self._memo.has_session_data is not None:
            return self._memo.has_session_data

        try:
            cache = get_cache()
            has_session_data = getattr(cache, "has_session_data", None)
            if callable(has_session_data):
                self._memo.has_session_data = bool(
                    has_session_data(self.year, self.gp, self.session)
                )
            else:
                logger.debug(
                    "Cache probe unavailable for %s/%s/%s; assuming cold session",
                    self.year,
                    self.gp,
                    self.session,
                )
                self._memo.has_session_data = False
        except (AttributeError, RuntimeError, TypeError, ValueError) as e:
            logger.debug(
                "Session cache probe failed for %s/%s/%s: %s",
                self.year,
                self.gp,
                self.session,
                e,
            )
            # Fall back to conservative behavior (attempt cache reads).
            self._memo.has_session_data = True

        return self._memo.has_session_data

    def _mark_session_cache_populated(self) -> None:
        """Mark that this session now has cacheable data persisted or scheduled.

        Only upgrades an unresolved probe: a session that started cold keeps
        its cold-start semantics for the rest of the process (reads were
        already skipped), while everything it fetches persists for the next
        process. Overriding a resolved-cold probe would silently flip later
        waves into cache-read mode mid-session.
        """
        if self.enable_cache and self._memo.has_session_data is None:
            self._memo.has_session_data = True

    def _get_from_cache(self, cache_key: str):
        """Get data from cache if enabled."""
        if not self.enable_cache or not self._session_cache_available():
            return None
        return get_cache().get(cache_key)

    def _cache_result(self, cache_key: str, data: dict) -> None:
        """Cache data if caching is enabled.

        Telemetry payloads persist to the telemetry tier (read first by every
        batch/single ref reader); other payloads keep the JSON tier.
        """
        if self.enable_cache:
            cache = get_cache()
            parts = cache_key.split("/", 3)
            if (
                len(parts) == 4
                and parts[0].isdigit()
                and _cache_telemetry_payload(
                    cache, int(parts[0]), parts[1], parts[2], parts[3], data
                )
            ):
                self._mark_session_cache_populated()
                return
            cache.set(cache_key, data)
            self._mark_session_cache_populated()

    @staticmethod
    def _reject_livedata(livedata: Any) -> None:
        """fastf1's ``livedata`` keyword names a live-timing recording tif1 cannot read."""
        if livedata is not None:
            raise NotImplementedError(
                "tif1 serves historical data only; live timing recordings "
                "(fastf1's livedata=) are not supported"
            )

    def load(self, laps=True, telemetry=True, weather=True, messages=True, *, livedata=None):
        """Load session data based on requested data types.

        This method fetches only the data that is required based on the boolean parameters.
        By default, all data types are fetched (laps, telemetry, weather, and messages).

        Args:
            laps: If True, fetch laps data. Required for telemetry - if telemetry=True
                  and laps=False, laps will be automatically set to True.
            telemetry: If True, fetch telemetry data for all laps
            weather: If True, fetch weather data
            messages: If True, fetch race control messages
            livedata: fastf1's live-timing recording; tif1 serves historical
                data only, so any value other than ``None`` raises
                ``NotImplementedError``.

        Returns:
            self: The Session object for method chaining

        Example:
            >>> session = get_session(2025, "Silverstone Grand Prix", "Race")
            >>> # Fetch only laps and telemetry
            >>> session.load(laps=True, telemetry=True, weather=False, messages=False)
            >>> # Fetch everything (default)
            >>> session.load()
        """
        self._reject_livedata(livedata)
        # Telemetry requires laps data, so ensure laps is True if telemetry is requested
        if telemetry and not laps:
            laps = True

        # Mark table intent before touching properties so the batch prefetch
        # fetches exactly what was requested.
        if weather:
            self._requested_session_tables.add("weather.json")
        if messages:
            self._requested_session_tables.add("rcm.json")

        if laps:
            _ = self.laps
        if weather:
            _ = self.weather
        if messages:
            _ = self.race_control_messages
        if telemetry:
            # Fetch telemetry for all laps if telemetry is requested
            self._load_all_laps_telemetry()
        return self

    async def load_async(
        self, laps=True, telemetry=True, weather=True, messages=True, *, livedata=None
    ):
        """Load session data without blocking: the async twin of :meth:`load`.

        Fetches the same tables as :meth:`load`, through the async pipeline
        only (one concurrent wave per phase), so it never has to block on the
        event loop. It is the entry point for runtimes whose synchronous API
        cannot wait on the network, such as Pyodide in a browser without
        JavaScript Promise Integration, and for callers already inside an
        event loop.

        Args:
            laps: Fetch laps data (forced on when ``telemetry`` is requested).
            telemetry: Fetch telemetry for all laps.
            weather: Fetch weather data.
            messages: Fetch race control messages.
            livedata: Not supported (see :meth:`load`).

        Returns:
            self: The Session object for method chaining.

        Example:
            >>> session = get_session(2025, "Silverstone Grand Prix", "Race")
            >>> await session.load_async(telemetry=False)
            >>> session.laps.head()
        """
        self._reject_livedata(livedata)
        if telemetry and not laps:
            laps = True

        if weather:
            self._requested_session_tables.add("weather.json")
        if messages:
            self._requested_session_tables.add("rcm.json")

        if laps:
            await self.laps_async()
        if weather or messages:
            # Tables the laps wave did not bring (laps served from the frame
            # tier, or laps=False) arrive in one more wave; the properties
            # below are then memo hits.
            await self._prefetch_session_tables_async()
        if weather:
            _ = self.weather
        if messages:
            _ = self.race_control_messages
        if telemetry:
            await self._load_all_laps_telemetry_async()
        return self

    async def _prefetch_session_tables_async(self) -> None:
        """Fetch the requested session tables still missing from the memo (``load_async``).

        The async twin of :meth:`_prefetch_session_tables`: weather, race
        control and drivers go out in one concurrent wave through
        :func:`fetch_multiple_async`, so the table properties that follow are
        memo hits. Nothing is fetched when the laps wave already brought them.
        """
        if self._ergast_only:
            return
        paths: list[str] = []
        for table, attr in (("weather.json", "_weather"), ("rcm.json", "_race_control_messages")):
            if (
                table in self._requested_session_tables
                and self._get_local_payload(table) is None
                and getattr(self, attr) is None
            ):
                paths.append(table)
        if self._get_local_payload("drivers.json") is None and self._drivers is None:
            paths.append("drivers.json")
        if not paths or config.get("offline_mode", False):
            return

        ultra_cold = self._resolve_ultra_cold_mode(None)
        use_cache = self.enable_cache and not ultra_cold and self._session_cache_available()
        requests = [(self.year, self.gp, self.session, path) for path in paths]
        try:
            try:
                results = await fetch_multiple_async(
                    requests,
                    use_cache=use_cache,
                    write_cache=self.enable_cache,
                    validate_payload=not ultra_cold,
                )
            except TypeError as e:
                if "unexpected keyword argument" not in str(e):
                    raise
                results = await fetch_multiple_async(requests)
        except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
            logger.debug("Session table prefetch skipped: %s", e)
            return

        for path, result in zip(paths, results):
            if not isinstance(result, dict) or not result:
                continue
            if path == "drivers.json" and not isinstance(result.get("drivers"), list):
                continue  # not a driver list: leave it to the validated loader
            self._remember_local_payload(path, result)
        self._session_tables_prefetched = True

    def _load_all_laps_telemetry(self) -> None:
        """Make every lap's telemetry available without the network (``load()``)."""
        if self._telemetry_load_pending():
            self.fetch_all_laps_telemetry()

    async def _load_all_laps_telemetry_async(self) -> None:
        """Async twin of :meth:`_load_all_laps_telemetry` (``load_async()``)."""
        if self._telemetry_load_pending():
            await self.fetch_all_laps_telemetry_async()

    def _telemetry_load_pending(self) -> bool:
        """Whether ``load()`` still needs the eager telemetry batch.

        Pergamon Q1: a warm pandas load reads and decodes the driver rows of
        the frame tier but leaves the per-lap frame construction (~11 us
        each, ~16 ms for a race) to the first access of each lap through
        the session memo; :meth:`fetch_all_laps_telemetry` builds them all.
        Laps the tier does not hold (legacy rows, payload tier, network)
        take the eager batch path as before.
        """
        ultra_cold = self._resolve_telemetry_ultra_cold_mode(None)
        if (
            self.lib == "pandas"
            and not ultra_cold
            and self.enable_cache
            and self._session_cache_available()
        ):
            laps = self.laps
            if _is_empty_df(laps, self.lib):
                return False
            lap_refs = self._telemetry_lap_refs(laps)
            if not lap_refs:
                return False
            memo = self._memo
            pending = [ref for ref in lap_refs if not memo.contains("telemetry_df", ref)]
            if pending:
                read_sources = getattr(get_cache(), "get_telemetry_frame_sources", None)
                if callable(read_sources):
                    sources = read_sources(
                        self.year,
                        self.gp,
                        self.session,
                        list(dict.fromkeys(driver for driver, _ in pending)),
                        frame_class=self._telemetry_frame_class(),
                    )
                    for driver, source in sources.items():
                        memo.add_telemetry_source(driver, source)
                    pending = [ref for ref in pending if not memo.contains("telemetry_df", ref)]
            if not pending or self._all_known_missing(pending):
                return False
        return True

    def _get_local_payload(self, path: str) -> dict[str, Any] | None:
        """Get in-memory payload previously fetched during this session."""
        payload = self._memo.get("json", path)
        if isinstance(payload, dict):
            return payload
        return None

    def _remember_local_payload(self, path: str, data: Any) -> None:
        """Store JSON payload in memory for subsequent access during this session."""
        # SessionMemo only stores known payload paths (or nested "DRIVER/..."
        # paths) to avoid memory bloat.
        self._memo.set("json", path, data)

    def _remember_telemetry_payload(
        self, driver: str, lap_num: int, tel_payload: dict[str, Any] | None
    ) -> None:
        """Memoize telemetry payloads fetched in this session."""
        self._memo.set("telemetry_payload", (driver, lap_num), tel_payload)

    def _get_telemetry_payload(self, driver: str, lap_num: int) -> dict[str, Any] | None:
        """Get memoized telemetry payload for (driver, lap)."""
        payload = self._memo.get("telemetry_payload", (driver, lap_num))
        if isinstance(payload, dict):
            return payload
        return None

    def _prefetch_session_tables(self) -> None:
        """Batch-fetch session-level tables (weather, rcm, drivers) in parallel.

        Uses a dedicated HTTP session and thread pool to avoid corrupting the
        shared session state used by subsequent asyncio.run() calls.
        """
        if self._ergast_only:
            return
        if self._session_tables_prefetched:
            return
        self._session_tables_prefetched = True

        paths_to_fetch: list[str] = []
        if (
            "weather.json" in self._requested_session_tables
            and self._get_local_payload("weather.json") is None
            and self._weather is None
        ):
            paths_to_fetch.append("weather.json")
        if (
            "rcm.json" in self._requested_session_tables
            and self._get_local_payload("rcm.json") is None
            and self._race_control_messages is None
        ):
            paths_to_fetch.append("rcm.json")
        if self._get_local_payload("drivers.json") is None and self._drivers is None:
            paths_to_fetch.append("drivers.json")

        if len(paths_to_fetch) < 2:
            return
        # Offline mode: the per-table loaders serve cache hits and raise on a
        # miss, so this network shortcut has nothing to add.
        if config.get("offline_mode", False):
            return
        # CI mode keeps the parallel fetch but never touches the persistent cache.
        use_persistent_cache = not config.get("ci_mode", False)

        # Persistent-cache hits satisfy the prefetch without the network;
        # network results are persisted below so a cold load() warms the
        # cache for the *next* process (previously these tables were
        # memo-only and re-downloaded by every fresh process).
        if use_persistent_cache:
            still_to_fetch: list[str] = []
            for path in paths_to_fetch:
                cached = None
                try:
                    cached = self._payload_loader._cache_get(self._payload_loader.cache_key(path))
                except Exception:
                    cached = None
                if isinstance(cached, dict):
                    self._remember_local_payload(path, cached)
                else:
                    still_to_fetch.append(path)
            paths_to_fetch = still_to_fetch

        if not paths_to_fetch:
            return
        if not THREADS_AVAILABLE:
            # Pyodide: no thread pool and no niquests. load_async() fetches
            # these tables in its wave; a table property reached without it
            # fetches its own file through the browser transport.
            return

        import niquests as _nq

        from .core_utils.json_utils import parse_response_json

        cdn_manager = get_cdn_manager()
        timeout = config.get("timeout", 10)

        def _fetch_one(path: str, http: _nq.Session) -> dict[str, Any] | None:
            try:
                sources = cdn_manager.get_sources()
                for src in sources:
                    url = src.format_url(self.year, self.gp, self.session, path)
                    try:
                        resp = http.get(url, timeout=timeout)
                        if resp.status_code == 404:
                            # Mirror may be stale; the next CDN may have it.
                            continue
                        resp.raise_for_status()
                        data = parse_response_json(resp)
                        return data if isinstance(data, dict) else None
                    except Exception:
                        continue
                return None
            except Exception:
                return None

        from concurrent.futures import ThreadPoolExecutor, as_completed

        try:
            with _nq.Session() as http:
                with ThreadPoolExecutor(max_workers=len(paths_to_fetch)) as pool:
                    future_to_path = {pool.submit(_fetch_one, p, http): p for p in paths_to_fetch}
                    for future in as_completed(future_to_path):
                        path = future_to_path[future]
                        try:
                            result = future.result()
                            if isinstance(result, dict):
                                self._remember_local_payload(path, result)
                                if use_persistent_cache:
                                    try:
                                        self._payload_loader._cache_set(
                                            self._payload_loader.cache_key(path), result
                                        )
                                    except Exception:
                                        pass
                        except Exception:
                            pass
        except Exception as e:
            logger.debug("Session table prefetch failed: %s", e)

    async def _prefetch_laps_wave_async(self, ultra_cold: bool) -> None:
        """Fetch the laps-path session tables in one concurrent wave (Thebes TH6).

        Collects the tables still missing from the session memo - ``drivers.json``,
        ``session_laptimes.json`` and, when requested, ``weather.json``/``rcm.json`` -
        and fetches them together through :func:`fetch_multiple_async` (cache
        reads, write-back, validation and CDN fallback as on every other async
        path). Results land in the memo, so the driver lookup, the laptime wave
        and the table loaders that follow find them without another request.
        Failures stay graceful: a table that did not arrive is fetched by its
        own loader as before.
        """
        if self._ergast_only:
            return
        paths: list[str] = []
        if self._drivers is None and self._get_local_payload("drivers.json") is None:
            paths.append("drivers.json")
        if self._get_local_payload(SESSION_LAPTIMES_PATH) is None:
            paths.append(SESSION_LAPTIMES_PATH)
        for table, attr in (("weather.json", "_weather"), ("rcm.json", "_race_control_messages")):
            if (
                table in self._requested_session_tables
                and self._get_local_payload(table) is None
                and getattr(self, attr) is None
            ):
                paths.append(table)
        if len(paths) < 2:
            return
        if config.get("offline_mode", False):
            return

        use_cache = self.enable_cache and not ultra_cold and self._session_cache_available()
        write_cache = self.enable_cache
        validate_payload = not ultra_cold
        max_retries = 1 if (ultra_cold and config.get("ultra_cold_skip_retries", True)) else None
        requests = [(self.year, self.gp, self.session, path) for path in paths]
        try:
            try:
                results = await fetch_multiple_async(
                    requests,
                    use_cache=use_cache,
                    write_cache=write_cache,
                    validate_payload=validate_payload,
                    max_retries=max_retries,
                )
            except TypeError as e:
                if "unexpected keyword argument" not in str(e):
                    raise
                results = await fetch_multiple_async(requests)
        except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
            logger.debug("Laps-wave prefetch skipped: %s", e)
            return

        fetched_any = False
        for path, result in zip(paths, results):
            if not isinstance(result, dict) or not result:
                continue
            if path == "drivers.json" and not isinstance(result.get("drivers"), list):
                continue  # not a driver list: leave it to the validated loader
            self._remember_local_payload(path, result)
            fetched_any = True
        if fetched_any:
            # The sync per-table prefetch has nothing left to add.
            self._session_tables_prefetched = True
            if write_cache:
                self._mark_session_cache_populated()

    def _resolve_telemetry_ultra_cold_mode(self, ultra_cold: bool | None) -> bool:
        """Resolve telemetry-specific ultra-cold mode.

        For telemetry-heavy cold starts, enable the low-latency path automatically
        when no persistent session cache is available.
        """
        requested = self._resolve_ultra_cold_mode(ultra_cold)
        if requested:
            return True
        return self.enable_cache and not self._session_cache_available()

    def _record_telemetry_failure(self, driver: str, lap_num: int, error: Exception) -> None:
        """Log telemetry failures with per-driver throttling."""
        fail_count = self._memo.record_telemetry_failure(driver)

        if fail_count <= 3:
            logger.warning("Failed telemetry load for %s lap %s: %s", driver, lap_num, error)
            return

        if not self._memo.is_failure_suppressed(driver):
            logger.warning(
                "Further telemetry load failures for %s are suppressed after 3 occurrences",
                driver,
            )
            self._memo.suppress_failure_warnings(driver)
            return

        logger.debug("Suppressed telemetry load failure for %s lap %s: %s", driver, lap_num, error)

    def _should_skip_telemetry_fetch(self, driver: str) -> bool:
        """Return True when telemetry fetches should be short-circuited for a driver."""
        return self._memo.is_telemetry_unavailable(driver)

    def _resolve_ultra_cold_mode(self, ultra_cold: bool | None) -> bool:
        """Resolve whether ultra-cold mode should be enabled."""
        if ultra_cold is not None:
            return ultra_cold
        if not bool(config.get("ultra_cold_start", False)):
            return False
        # ultra_cold_start=True is the cold-START fast path: skip cache reads
        # only when the session isn't already cached. A warm cache reads
        # faster than the network, so never re-download over it.
        return not self._session_cache_available()

    def _is_fastest_lap_tel_cold_start(self) -> bool:
        """Detect whether fastest-lap telemetry is being requested on a brand-new session."""
        return self._laps is None and self._memo.fastest_lap_ref is None and self._drivers is None

    def _should_backfill_ultra_cold_cache(self, ultra_cold_enabled: bool) -> bool:
        """Determine whether ultra-cold fetches should backfill cache in background."""
        if not ultra_cold_enabled or not self.enable_cache:
            return False
        return bool(config.get("ultra_cold_background_cache_fill", False))

    def _load_drivers_for_fastest_lap_reference(
        self, *, ultra_cold: bool
    ) -> tuple[list[dict], list[tuple[str, dict[str, Any]]]]:
        """Load drivers for fastest-lap lookup, optionally bypassing cache/validation."""
        if self._drivers is not None:
            self._refresh_driver_indices()
            return self._drivers, []

        if not ultra_cold:
            # Force load drivers through property, then return internal list
            _ = self._drivers_data
            return self._drivers if self._drivers is not None else [], []

        try:
            drivers_payload = self._fetch_json_unvalidated("drivers.json")
        except (DataNotFoundError, InvalidDataError, NetworkError, TypeError, ValueError) as e:
            logger.debug("Ultra-cold driver fetch failed, falling back to validated path: %s", e)
            _ = self._drivers_data
            return self._drivers if self._drivers is not None else [], []

        drivers = drivers_payload.get("drivers") if isinstance(drivers_payload, dict) else None
        self._drivers = _session_driver_rows(drivers)
        self._refresh_driver_indices()
        if not self._drivers:
            logger.info(f"No drivers in session: {self.year}/{self.event_name}/{self.session_name}")

        cacheable_payloads: list[tuple[str, dict[str, Any]]] = []
        if isinstance(drivers_payload, dict):
            cacheable_payloads.append(("drivers.json", drivers_payload))
        return self._drivers, cacheable_payloads

    def _schedule_background_cache_fill(
        self,
        *,
        json_payloads: list[tuple[str, dict[str, Any]]] | None = None,
        telemetry_payload: tuple[str, int, dict[str, Any]] | None = None,
        telemetry_payloads: list[tuple[str, int, dict[str, Any]]] | None = None,
    ) -> None:
        """Backfill JSON/telemetry cache entries on a daemon thread."""
        if not self.enable_cache:
            return
        telemetry_items = [] if telemetry_payloads is None else list(telemetry_payloads)
        if telemetry_payload is not None:
            telemetry_items.append(telemetry_payload)

        if not json_payloads and not telemetry_items:
            return

        year = self.year
        gp = self.gp
        session = self.session
        self._mark_session_cache_populated()

        def _worker() -> None:
            cache = get_cache()
            try:
                if json_payloads:
                    for path, payload in json_payloads:
                        if not isinstance(payload, dict):
                            continue
                        cache_key = f"{year}/{gp}/{session}/{path}"
                        cache.set(cache_key, payload)

                # Telemetry payloads persist to the telemetry tier only: every
                # reader consults it first, so a JSON-tier copy is a duplicate
                # dumps/compress/insert per payload.
                for driver, lap_num, tel_payload in telemetry_items:
                    cache.set_telemetry(year, gp, session, driver, lap_num, tel_payload)
            except (AttributeError, RuntimeError, TypeError, ValueError) as e:
                logger.debug("Background cache fill skipped: %s", e)

        if not THREADS_AVAILABLE:
            _worker()
            return
        threading.Thread(target=_worker, name="tif1-ultra-cold-cache-fill", daemon=True).start()

    def _delegate_loader_fetch(self, path: str, *, fast: bool) -> dict[str, Any]:
        """Route PayloadLoader fetches through the overridable CDN fetch methods.

        Keeps ``Session._fetch_from_cdn`` / ``Session._fetch_from_cdn_fast``
        as the test override seam while the pipeline itself lives in
        :class:`tif1.payload_loader.PayloadLoader`.
        """
        if fast:
            return self._fetch_from_cdn_fast(path)
        return self._fetch_from_cdn(path)

    def _fetch_json_unvalidated(self, path: str) -> dict[str, Any]:
        """Fetch JSON payload without validation for ultra-cold paths.

        Skips cache reads (the ultra-cold fast path) but still persists the
        fetched payload, so a cold session becomes a warm cache for the next
        process instead of re-downloading the full session every time.
        """
        if self._ergast_only:
            raise DataNotFoundError(
                year=self.year,
                event=self.gp,
                session=self.session,
                path=path,
                reason=f"timing data is not available before {F1_API_FIRST_YEAR}",
            )
        return self._payload_loader.get(
            path,
            validate=False,
            use_cache=False,
            write_cache=True,
            fast=bool(config.get("ultra_cold_skip_retries", True)),
        )

    def _fetch_from_cdn_fast(self, path: str) -> dict:
        """Fetch data from CDN without per-source retry/backoff delays."""
        return self._payload_loader.fetch_from_cdn(path, fast=True)

    def _fetch_from_cdn(self, path: str) -> dict:
        """Fetch data from CDN with retry logic."""
        return self._payload_loader.fetch_from_cdn(path, fast=False)

    def _fetch_json(self, path: str) -> dict:
        """Fetch JSON data with caching, retry logic, and CDN fallback."""
        if self._ergast_only:
            raise DataNotFoundError(
                year=self.year,
                event=self.gp,
                session=self.session,
                path=path,
                reason=f"timing data is not available before {F1_API_FIRST_YEAR}",
            )
        return self._payload_loader.get(
            path,
            validate=True,
            use_cache=True,
            write_cache=True,
            fast=False,
        )

    @property
    def drivers(self) -> list[str]:
        """Driver numbers as strings, in classification order (FastF1 ``Session.drivers``).

        The upstream ``drivers.json`` lists the official classification for most
        sessions. Where it is only sorted by car number (several seasons before
        2025) the order is rebuilt from the laps table: laps completed, then
        position on the last lap for races and sprints; best non-deleted lap
        time for every other session. Post-session penalties are not visible
        in lap data, so a rebuilt order can differ from the official result by
        a place or two. Numbers missing from ``drivers.json`` are filled in
        from the laps table as well.

        A car two drivers shared (classifications before the 1960s) appears
        once per driver: the first keeps the car number, the others are
        labelled ``number/Abbreviation`` so that every entry is a unique
        identifier for :meth:`get_driver` and a unique label of the
        ``results`` index.

        Returns:
            List of driver numbers as strings (e.g., ['81', '63', '4', ...])
        """
        return _er.unique_driver_labels(
            [(str(d["dn"]), str(d.get("driver") or "")) for d in self._driver_rows() if d.get("dn")]
        )

    def _driver_rows(self) -> list[dict]:
        """Driver payload rows in classification order; see :attr:`drivers`."""
        return self._driver_rows_info()[0]

    def _driver_rows_info(self) -> tuple[list[dict], bool]:
        """``(rows, classified)``: ordered driver rows and whether that order is a classification.

        Cheap for payloads that are already a classification (one pass over the
        numbers, no laps access). Only a number-sorted or number-less payload
        reads the laps table, through :meth:`_lap_summary`.
        """
        if self._ergast_enabled and self._drivers is None:
            # The classification file rides alongside drivers.json (D4).
            self._start_ergast_prefetch()
        rows = [d for d in self._drivers_data if isinstance(d, dict)]
        if not rows and not self._ergast_enabled:
            return rows, False
        cache = self._driver_rows_cache
        if cache is not None and cache[0] == self._driver_rows_token():
            return cache[1], cache[2]
        if self._ergast_enabled and _er.session_kind(self.session_name) != "identity":
            classification = _er.ergast_rows(self._ergast_race(), self._ergast_endpoint())
            if classification:
                # Official classification: no laps table needed, drivers only
                # Ergast knows (did not start) are added in their place.
                ordered = _er.order_driver_rows(
                    rows, classification, _er.team_color_map(rows, self.year)
                )
                self._driver_rows_cache = (self._driver_rows_token(), ordered, True)
                return ordered, True
        if not rows:
            return rows, False

        numbers = [str(d.get("dn") or "") for d in rows]
        needs_numbers = not all(numbers)
        number_sorted = not needs_numbers and _is_number_sorted(numbers)
        classified = not number_sorted and not needs_numbers
        # A qualifying without official results is classified from its laps as
        # fastf1 does, whatever order drivers.json lists (2024 Chinese Sprint
        # Qualifying lists STR, PIA, VER; the result is NOR, HAM, ALO).
        quali_like = self._is_quali_like()
        if needs_numbers or number_sorted or quali_like:
            summary = self._lap_summary()
            if summary:
                if needs_numbers:
                    for d in rows:
                        if not d.get("dn"):
                            info = summary.get(str(d.get("driver", "")))
                            if info is not None and info["number"]:
                                d["dn"] = info["number"]  # same dict the lookup indices hold
                    numbers = [str(d.get("dn") or "") for d in rows]
                    number_sorted = all(numbers) and _is_number_sorted(numbers)
                bests = self._quali_segment_bests() if quali_like else {}
                if bests:
                    rows = _quali_sort(rows, bests)
                    classified = True
                elif number_sorted or needs_numbers:
                    rows = _classification_sort(rows, summary, race=self._is_race_like())
                    classified = True
        self._driver_rows_cache = (self._driver_rows_token(), rows, classified)
        return rows, classified

    def _driver_rows_token(self) -> tuple[int, int | None]:
        """Identity of the payloads the ordered driver rows were built from."""
        return (
            id(self._drivers),
            id(self._ergast_payload) if self._ergast_loaded else None,
        )

    def _is_race_like(self) -> bool:
        return self.session_name in _RACE_LIKE_SESSION_NAMES

    def _is_quali_like(self) -> bool:
        return self.session_name in _QUALI_LIKE_SESSION_NAMES

    def _quali_segment_bests(self) -> dict[str, list[int | None]]:
        """Per driver code, the best Q1/Q2/Q3 lap in ns (``None`` where none), fastf1's rule.

        ``laps.pick_accurate().split_qualifying_sessions()``, then each
        segment's fastest lap with a time that is not deleted, then the
        knockout (:func:`_apply_quali_knockout`). Memoized on the laps table;
        ``{}`` without laps or when the split fails.
        """
        laps = self._loaded_laps_or_none()
        if laps is None:
            return {}
        cached = self._quali_bests_cache
        if cached is not None and cached[0] is laps:
            return cached[1]
        bests: dict[str, list[int | None]] = {}
        try:
            segments = cast(Any, laps).pick_accurate().split_qualifying_sessions()
        except (KeyError, TypeError, ValueError) as e:
            logger.info("Qualifying segments unavailable for %s: %s", self.api_path, e)
            segments = []
        for slot, segment in enumerate(segments[:3]):
            if segment is None or not len(segment):
                continue
            for code, lap_ns in _segment_lap_times(segment, self.lib):
                entry = bests.setdefault(code, [None, None, None])
                current = entry[slot]
                if current is None or lap_ns < current:
                    entry[slot] = lap_ns
        entrants = sum(1 for row in self._drivers_data if isinstance(row, dict))
        _apply_quali_knockout(bests, entrants)
        self._quali_bests_cache = (laps, bests)
        return bests

    def _lap_summary(self) -> dict[str, dict[str, Any]]:
        """Per-driver lap facts: number, laps completed, last-lap position, best clean lap.

        One pass over the loaded laps table (either backend), memoized on the
        table's identity. A session without a laps payload yields ``{}`` (logged
        at info level) so ``drivers``/``results`` still return the entrants.
        """
        try:
            laps = self.laps
        except DataNotFoundError as exc:
            logger.info(
                "No laps to classify %s/%s/%s: %s",
                self.year,
                self.event_name,
                self.session_name,
                exc,
            )
            return {}
        except NetworkError as exc:
            # The entrant list itself loaded; only the classification is
            # degraded, so keep the upstream order and say so.
            logger.warning(
                "Laps unavailable, driver order/results left unclassified for %s/%s/%s: %s",
                self.year,
                self.event_name,
                self.session_name,
                exc,
            )
            return {}
        cache = self._lap_summary_cache
        if cache is not None and cache[0] == id(laps):
            return cache[1]
        summary = (
            {}
            if _is_empty_df(laps, self.lib)
            else _summarize_laps_for_classification(laps, self.lib)
        )
        self._lap_summary_cache = (id(laps), summary)
        return summary

    @property
    def _drivers_data(self) -> list[dict]:
        """Get list of driver dictionaries (internal use)."""
        if self._drivers is None and self._ergast_only:
            self._drivers = self._ergast_driver_payload()
        if self._drivers is None:
            self._prefetch_session_tables()
            ultra_cold_enabled = self._resolve_ultra_cold_mode(None)
            if ultra_cold_enabled:
                data = self._fetch_json_unvalidated("drivers.json")
                if isinstance(data, dict) and self._should_backfill_ultra_cold_cache(
                    ultra_cold_enabled
                ):
                    self._schedule_background_cache_fill(json_payloads=[("drivers.json", data)])
            else:
                data = self._fetch_json("drivers.json")
            self._drivers = _session_driver_rows(data.get("drivers"))
            if not self._drivers:
                logger.info(
                    f"No drivers in session: {self.year}/{self.event_name}/{self.session_name}"
                )
        self._refresh_driver_indices()
        return self._drivers

    @property
    def driver_list(self) -> list[str]:
        """Alias of :attr:`drivers` (fastf1 compatibility)."""
        return self.drivers

    @property
    def drivers_df(self) -> DataFrame:
        """Get drivers as a DataFrame, one row per driver in classification order.

        A pandas frame, or a polars frame (every column ``String``) on a
        polars session.

        Returns:
            DataFrame with columns: Driver, Team, DriverNumber, FirstName, LastName, TeamColor, HeadshotUrl
        """
        drivers_list = self._driver_rows()
        if self.lib == "polars" and _ensure_polars_available():
            return self._drivers_df_polars(drivers_list)
        if not drivers_list:
            return pd.DataFrame(columns=list(_DRIVERS_DF_COLUMNS))

        rows = [
            {
                "Driver": d.get("driver", ""),
                "Team": d.get("team", ""),
                "DriverNumber": str(d.get("dn", "")),
                "FirstName": d.get("fn", ""),
                "LastName": d.get("ln", ""),
                "TeamColor": d.get("tc", ""),
                "HeadshotUrl": d.get("url", ""),
            }
            for d in drivers_list
            if isinstance(d, dict)
        ]

        return pd.DataFrame(rows)

    @staticmethod
    def _drivers_df_polars(drivers_list: list[dict]) -> DataFrame:
        """:attr:`drivers_df` built natively in polars (same rows and values as pandas)."""
        rows = [d for d in drivers_list if isinstance(d, dict)]
        data = {
            "Driver": [d.get("driver", "") for d in rows],
            "Team": [d.get("team", "") for d in rows],
            "DriverNumber": [str(d.get("dn", "")) for d in rows],
            "FirstName": [d.get("fn", "") for d in rows],
            "LastName": [d.get("ln", "") for d in rows],
            "TeamColor": [d.get("tc", "") for d in rows],
            "HeadshotUrl": [d.get("url", "") for d in rows],
        }
        schema = dict.fromkeys(_DRIVERS_DF_COLUMNS, pl.String)
        return pl.DataFrame(data, schema=schema, strict=False)

    def _load_session_table(self, path: str, rename_map: dict[str, str]) -> DataFrame:
        """Load and normalize a session-level table payload."""
        ultra_cold_enabled = self._resolve_ultra_cold_mode(None)
        data = self._fetch_json_unvalidated(path) if ultra_cold_enabled else self._fetch_json(path)

        if (
            ultra_cold_enabled
            and isinstance(data, dict)
            and self._should_backfill_ultra_cold_cache(ultra_cold_enabled)
        ):
            self._schedule_background_cache_fill(json_payloads=[(path, data)])

        if not isinstance(data, dict) or not data:
            return _create_empty_df(self.lib)

        return _create_session_df(data, rename_map, self.lib)

    @property
    def car_data(self) -> "LazyTelemetryDict":
        """Per-driver session-long telemetry, keyed by driver number (fastf1 parity).

        A lazy dict: nothing is fetched until a key is read; the value is the
        driver's laps concatenated in session order as one :class:`Telemetry`
        (``PolarsTelemetry`` on a polars session), memoised per driver.
        Abbreviations are accepted as keys too. ``car_data[n]`` and
        ``pos_data[n]`` hold the same merged frame: tif1's telemetry already
        carries the car channels (``RPM``, ``Speed``, ``nGear``, ``Throttle``,
        ``Brake``, ``DRS``) and the position channels (``X``, ``Y``, ``Z``).

        Example:
            >>> session = get_session(2024, "Monaco", "Race")
            >>> ver = session.car_data["1"]
            >>> ver[["SessionTime", "Speed", "X", "Y"]].head()
        """
        if self._car_data is None:
            self._car_data = LazyTelemetryDict(self)
        return self._car_data

    @property
    def pos_data(self) -> "LazyTelemetryDict":
        """Per-driver session-long telemetry keyed by driver number (see :attr:`car_data`)."""
        return self.car_data

    @property
    def total_laps(self) -> int | None:
        """Laps of the race distance: the leader's lap count from the Ergast classification.

        None for sessions without an official race classification.
        """
        if self.session_name not in _RACE_LIKE_SESSION_NAMES or not self._ergast_enabled:
            return None
        return _er.total_laps_from_rows(
            _er.ergast_rows(self._ergast_race(), self._ergast_endpoint())
        )

    @property
    def results(self) -> "SessionResults":
        """Session classification with driver information, one row per driver.

        Rows are in classification order (see :attr:`drivers`). ``Position`` and
        ``ClassifiedPosition`` number that order; ``Laps`` is the laps completed
        from the laps table; ``Status`` is derived from lap counts for races and
        sprints (``"Finished"`` on the leader's lap count, ``"+N Lap(s)"``
        otherwise, ``"Did not start"`` with no laps) and ``"Finished"`` for
        other sessions. In practice sessions ``Time`` is the driver's best
        non-deleted lap, the order fastf1 ranks them by. The data source
        publishes no official classification, so ``GridPosition`` and
        ``Points`` are NaN, and a session without a laps payload has NaN
        ``Position``/``Laps`` and ``None`` ``Status``.
        """
        if self._results is None:
            driver_rows, classified = self._driver_rows_info()
            if not driver_rows:
                return self._finalize_results([])
            kind = _er.session_kind(self.session_name)
            race = self._ergast_race() if self._ergast_enabled else None
            classification = _er.ergast_rows(race, self._ergast_endpoint())
            official = bool(classification) and kind != "identity"
            # With the official classification the laps table is not needed
            # for Position/Status/Laps; practice keeps the lap-derived view.
            summary = {} if official else self._lap_summary()
            race_like = self._is_race_like()
            practice = self.session_name in _PRACTICE_SESSIONS
            # fastf1 classifies a qualifying without results from the laps:
            # best accurate lap per segment, "N" outside 107% of the best Q1.
            bests = self._quali_segment_bests() if not official and self._is_quali_like() else {}
            q1_floor = _quali_q1_floor(bests)
            leader_laps = max((info["laps"] for info in summary.values()), default=0)

            rows = []
            for index, d in enumerate(driver_rows, start=1):
                info = summary.get(str(d.get("driver", "")))
                laps_done = info["laps"] if info is not None else 0
                if official:
                    # Race/sprint: Ergast fills Status; qualifying has none.
                    status: str | None = None if kind == "race" else "Finished"
                elif not summary:
                    status = None
                elif not race_like:
                    status = "Finished"
                elif laps_done == 0:
                    status = "Did not start"
                elif laps_done == leader_laps:
                    status = "Finished"
                else:
                    gap = leader_laps - laps_done
                    status = f"+{gap} Lap" if gap == 1 else f"+{gap} Laps"
                rows.append(
                    {
                        "DriverNumber": str(d.get("dn", "")),
                        "Abbreviation": d.get("driver", ""),
                        "TeamName": d.get("team", ""),
                        "TeamColor": d.get("tc", ""),
                        "FirstName": d.get("fn", ""),
                        "LastName": d.get("ln", ""),
                        "FullName": f"{d.get('fn', '')} {d.get('ln', '')}".strip(),
                        "HeadshotUrl": d.get("url", ""),
                        "Position": float(index) if classified and not official else math.nan,
                        "ClassifiedPosition": str(index) if classified and not official else "",
                        "GridPosition": math.nan,
                        "Status": status,
                        "Points": math.nan,
                        "Laps": float(laps_done) if summary else math.nan,
                        # fastf1: a practice ``Time`` is the best lap.
                        "Time": _best_lap_timedelta(info) if practice else pd.NaT,
                        **_quali_row_fields(bests.get(str(d.get("driver", ""))), q1_floor),
                        # Did-not-start rows (only Ergast knows the driver) keep
                        # NaN positions, except in Ergast-only seasons where
                        # every driver comes from the classification.
                        "_ergast_only": bool(d.get("_ergast_only")) and not self._ergast_only,
                    }
                )
            if classification:
                rows = _er.merge_ergast_rows(rows, classification, kind)
            else:
                for row in rows:
                    row.pop("_ergast_only", None)

            self._results = self._finalize_results(rows)
        return self._results

    def _finalize_results(self, rows: list[dict[str, Any]]) -> Any:
        """The results table of ``rows``: ``SessionResults`` or ``PolarsSessionResults``.

        The polars table is built natively (no pandas frame); polars has no
        index, so ``DriverNumber`` stays a column.
        """
        if self.lib == "polars" and _ensure_polars_available():
            frame = _er.finalize_results_frame_polars(rows)
            return _polars_models().PolarsSessionResults._from_frame(frame, self)
        results = SessionResults(_er.finalize_results_frame(rows))
        results.session = self
        return results

    @property
    def race_control_messages(self) -> DataFrame:
        """Get session race control messages."""
        if self._race_control_messages is None:
            # Accessing messages IS the request for the rcm table.
            self._requested_session_tables.add("rcm.json")
            self._prefetch_session_tables()
            try:
                self._race_control_messages = self._load_session_table(
                    "rcm.json", RACE_CONTROL_RENAME_MAP
                )
                if not _is_empty_df(self._race_control_messages, self.lib):
                    if self.lib == "polars":
                        import polars as pl

                        rcm_pl = cast(Any, self._race_control_messages)
                        cols = rcm_pl.columns
                        exprs = []
                        # Time: ISO string → Datetime (ns)
                        if "Time" in cols:
                            exprs.append(
                                pl.col("Time")
                                .cast(pl.Utf8)
                                .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                                .cast(pl.Datetime("ns"))
                            )
                        # String columns: replace "None" sentinel with null
                        exprs.extend(
                            pl.when(pl.col(_col).cast(pl.Utf8) == "None")
                            .then(None)
                            .otherwise(pl.col(_col).cast(pl.Utf8))
                            .alias(_col)
                            for _col in (
                                "Category",
                                "Message",
                                "Status",
                                "Flag",
                                "Scope",
                                "RacingNumber",
                            )
                            if _col in cols
                        )
                        # Sector: float64, "None" → null
                        if "Sector" in cols:
                            exprs.append(
                                pl.when(pl.col("Sector").cast(pl.Utf8) == "None")
                                .then(None)
                                .otherwise(pl.col("Sector"))
                                .cast(pl.Float64, strict=False)
                                .alias("Sector")
                            )
                        # Lap: int64; non-race sessions send an all-"None" Lap
                        # column, so coerce like pandas' to_numeric(errors="coerce").
                        if "Lap" in cols:
                            exprs.append(pl.col("Lap").cast(pl.Int64, strict=False))
                        if exprs:
                            self._race_control_messages = rcm_pl.with_columns(exprs)
                    else:
                        rcm_pd = cast(pd.DataFrame, self._race_control_messages)
                        # Time: ISO string → datetime64[ns]
                        if "Time" in rcm_pd.columns:
                            rcm_pd["Time"] = pd.to_datetime(
                                rcm_pd["Time"], errors="coerce", utc=False, format="ISO8601"
                            ).astype("datetime64[ns]")
                        # String columns: replace "None" sentinel with None → object dtype
                        for _col in (
                            "Category",
                            "Message",
                            "Status",
                            "Flag",
                            "Scope",
                            "RacingNumber",
                        ):
                            if _col in rcm_pd.columns:
                                rcm_series = rcm_pd[_col].astype(object)
                                rcm_series.loc[rcm_series == "None"] = None
                                rcm_pd[_col] = rcm_series
                        # Sector: float64, "None" → NaN
                        if "Sector" in rcm_pd.columns:
                            _sector = rcm_pd["Sector"].astype(object)
                            _sector = _sector.mask(_sector == "None")
                            rcm_pd["Sector"] = pd.to_numeric(_sector, errors="coerce").astype(
                                "float64"
                            )
                        # Lap: plain int64 like FastF1; messages without a lap
                        # (sentinels, non-race sessions) become 0, not NA
                        if "Lap" in rcm_pd.columns:
                            rcm_pd["Lap"] = (
                                pd.to_numeric(rcm_pd["Lap"], errors="coerce")
                                .fillna(0)
                                .astype("int64")
                            )
                        self._race_control_messages = rcm_pd
            except DataNotFoundError:
                logger.info(
                    "No race control messages in session: %s/%s/%s",
                    self.year,
                    self.event_name,
                    self.session_name,
                )
                self._race_control_messages = _create_empty_df(self.lib)
            except (
                InvalidDataError,
                NetworkError,
                RuntimeError,
                TypeError,
                ValueError,
                *_polars_error_types(),
            ) as e:
                logger.warning(
                    "Failed to load race control messages for %s/%s/%s: %s",
                    self.year,
                    self.event_name,
                    self.session_name,
                    e,
                )
                self._race_control_messages = _create_empty_df(self.lib)
        return self._race_control_messages

    # -- derived session state (fastf1's live-timing stream surfaces) ------------

    def _state(self, key: str, build: Any) -> Any:
        """Memoised derived session state (``None`` results are memoised too).

        The memo is keyed on the identity of the laps table: every derivation
        starts from ``t0_date`` (and the laps' codes), so swapping the table
        rebuilds it.
        """
        laps_id = id(self._laps) if self._laps is not None else None
        entry = self._session_state.get(key)
        if entry is None or entry[0] != laps_id:
            entry = (laps_id, build())
            self._session_state[key] = entry
        return entry[1]

    def _t0_date_ns(self) -> int | None:
        return self._state("t0_ns", lambda: _ss.t0_date_ns(self._loaded_laps_or_none()))

    def _loaded_laps_or_none(self) -> DataFrame | None:
        """The laps table, loaded on demand (``None`` when the session has none).

        ``None`` before 2018 too: laps built from Ergast timings carry no
        ``LapStartDate`` or ``TrackStatus``, so loading them answers nothing.
        """
        if self._ergast_only:
            return None
        try:
            laps = self.laps
        except (DataNotFoundError, NetworkError, InvalidDataError):
            return None
        return None if _is_empty_df(laps, self.lib) else laps

    def _scheduled_start_utc_ns(self) -> int | None:
        try:
            scheduled = self.event.get_session_date(self.session_name, utc=True)
        except (ValueError, KeyError, TypeError, EventNotFoundError):
            return None
        if scheduled is None or pd.isna(scheduled):
            return None
        return int(pd.Timestamp(scheduled).tz_localize(None).value)

    def _session_start_time_ns(self) -> int | None:
        return self._state(
            "start_ns",
            lambda: _ss.session_start_time_ns(
                self.race_control_messages,
                self._t0_date_ns(),
                self._scheduled_start_utc_ns(),
                race_like=self._is_race_like(),
            ),
        )

    @property
    def t0_date(self) -> Any:
        """The date at ``SessionTime`` zero (fastf1 parity).

        ``LapStartDate - LapStartTime`` of the laps table, constant to the
        millisecond across every lap; ``NaT`` for a session without laps.
        """
        t0_ns = self._t0_date_ns()
        return pd.NaT if t0_ns is None else pd.Timestamp(t0_ns)

    @property
    def session_start_time(self) -> Any:
        """``SessionTime`` of the session start (fastf1 parity).

        Races and sprints: the scheduled UTC start (the formation lap) minus
        :attr:`t0_date`; other sessions: the first ``GREEN LIGHT - PIT EXIT
        OPEN`` race-control message. ``NaT`` when neither is known.
        """
        start_ns = self._session_start_time_ns()
        return pd.NaT if start_ns is None else pd.Timedelta(start_ns)

    @property
    def track_status(self) -> DataFrame:
        """Track status changes: ``Time`` (session time), ``Status`` code, ``Message``.

        fastf1's shape and vocabulary (``1`` AllClear, ``2`` Yellow, ``4``
        SCDeployed, ``5`` Red, ``6`` VSCDeployed, ``7`` VSCEnding) derived
        from the race-control messages (exact timestamps, inferred codes:
        sector yellows are tracked as a set so the status returns to ``1``
        only when every sector is clear). Without race-control messages the
        laps' ``TrackStatus`` codes at each lap start are used instead. The
        first row is ``AllClear`` at time zero as in the live stream.
        """

        def build() -> DataFrame:
            t0_ns = self._t0_date_ns()
            rows: list[tuple[int, str]] = [(0, "1")]
            if t0_ns is not None:
                rcm = self.race_control_messages
                if not _is_empty_df(rcm, self.lib):
                    rows = _ss.track_status_rows(rcm, t0_ns)
            if len(rows) <= 1:
                laps = self._loaded_laps_or_none()
                if laps is not None:
                    rows = _ss.track_status_rows_from_laps(laps)
            return _ss.status_frame(rows, self.lib, with_message=True)

        return self._state("track_status", build)

    @property
    def session_status(self) -> DataFrame:
        """Session status changes: ``Time`` (session time) and ``Status`` (fastf1 parity).

        ``Started`` at :attr:`session_start_time`, ``Aborted`` at each red
        flag, ``Started`` again on the following green light and
        ``Finished`` at the chequered flag, from the race-control messages.
        ``Finalised``/``Ends`` are not derivable and are not emitted.
        """

        def build() -> DataFrame:
            t0_ns = self._t0_date_ns()
            rows: list[tuple[int, str]] = []
            if t0_ns is not None:
                rows = _ss.session_status_rows(
                    self.race_control_messages, t0_ns, self._session_start_time_ns()
                )
            return _ss.status_frame(rows, self.lib, with_message=False)

        return self._state("session_status", build)

    @property
    def session_info(self) -> dict[str, Any]:
        """fastf1's ``session_info`` dict derived from the schedule row.

        ``Meeting`` (name, official name, location, country, circuit),
        ``Type``, ``Name``, ``StartDate``, ``EndDate`` (nominal duration) and
        ``GmtOffset`` are filled; ``Key``, ``Meeting.Key``, ``Path`` and
        ``ArchiveStatus`` are ``None`` (not derivable). The circuit and
        country keys come from the vendored circuit table or, when it was
        already built, :meth:`get_circuit_info`; reading this dict never
        starts a request beyond the schedule.
        """

        def build() -> dict[str, Any]:
            try:
                event = self.event
            except (EventNotFoundError, ValueError):
                event = pd.Series(dtype=object)
            from .events import _parse_gmt_offset

            gmt_raw = event.get("GmtOffset") if hasattr(event, "get") else None
            parsed_offset = _parse_gmt_offset(gmt_raw) if gmt_raw is not None else None
            gmt_offset: Any = None if parsed_offset is None else pd.Timedelta(parsed_offset)
            start = self.date
            start_date = None if start is None or pd.isna(start) else pd.Timestamp(start)
            keys = self._circuit_keys()
            return _ss.session_info_dict(
                event,
                self.session_name,
                start_date=start_date,
                gmt_offset=gmt_offset,
                circuit_key=keys.get("circuit_key"),
                country_key=keys.get("country_key"),
                country_code=keys.get("country_code") or "",
                circuit_short_name=keys.get("circuit_name") or "",
            )

        return self._state("session_info", build)

    def _circuit_keys(self) -> dict[str, Any]:
        """Circuit/country keys without a request: memoised circuit info, else the vendored table."""
        info = self._circuit_info
        if info is not None and info.circuit_key is not None:
            return {
                "circuit_key": info.circuit_key,
                "country_key": info.country_key,
                "country_code": info.country_code,
                "circuit_name": info.circuit_name,
            }
        try:
            from .core_utils.circuit_info import load_vendored_circuits

            (circuit_id,) = self._event_texts("CircuitId")
            entry = load_vendored_circuits().get(circuit_id) if circuit_id else None
        except (ImportError, ValueError, KeyError, TypeError, OSError):
            entry = None
        if not entry:
            return {}
        return {
            "circuit_key": entry.get("circuitKey"),
            "country_key": entry.get("countryKey"),
            "country_code": entry.get("iocCountryCode") or "",
            "circuit_name": entry.get("name") or "",
        }

    @property
    def f1_api_support(self) -> bool:
        """Whether the session has timing/telemetry data (seasons from 2018)."""
        return self.year >= F1_API_FIRST_YEAR

    @property
    def api_path(self) -> str:
        """The session's path on the data repositories (``"{year}/{gp}/{session}/"``)."""
        return f"{self.year}/{self.gp}/{self.session}/"

    @property
    def name(self) -> str:
        """Get session name (decoded from URL encoding).

        Returns:
            Session name (e.g., "Practice 1", "Qualifying", "Race")
        """
        return self.session_name

    @property
    def event_name(self) -> str:
        """Grand Prix name as the schedule spells it (``gp`` is its URL-encoded form)."""
        return unquote(self.gp)

    @property
    def session_name(self) -> str:
        """Session name as the schedule spells it (``session`` is its URL-encoded form)."""
        return unquote(self.session)

    @property
    def date(self):
        """Get session date.

        Returns:
            Session date as pandas Timestamp, or pd.NaT if not available
        """
        event = self.event
        session_name = self.name

        for i in range(1, 6):
            session_key = f"Session{i}"
            if session_key in event.index and event[session_key] == session_name:
                date_key = f"Session{i}Date"
                if date_key in event.index and not pd.isna(event[date_key]):
                    ts = pd.Timestamp(event[date_key])
                    return ts.tz_localize(None) if ts.tz else ts
                # Ergast-backed events carry UTC times only.
                utc_key = f"Session{i}DateUtc"
                if utc_key in event.index and not pd.isna(event[utc_key]):
                    return pd.Timestamp(event[utc_key])

        return pd.NaT

    @property
    def event(self):
        """Placeholder for Event object."""
        # We will implement Event properly in events.py
        from .events import get_event_by_name

        backend = getattr(self, "_schedule_backend", None)
        if backend is None:
            return get_event_by_name(self.year, self.event_name)
        return get_event_by_name(self.year, self.event_name, backend=backend)

    @property
    def weather(self) -> DataFrame:
        """Get session weather data."""
        if self._weather is None:
            # Accessing weather IS the request for the weather table.
            self._requested_session_tables.add("weather.json")
            self._prefetch_session_tables()
            try:
                self._weather = self._load_session_table("weather.json", WEATHER_RENAME_MAP)
                if not _is_empty_df(self._weather, self.lib):
                    if self.lib == "polars":
                        import polars as pl

                        weather_pl = cast(Any, self._weather)
                        cols = weather_pl.columns
                        exprs = []
                        # Casts are non-strict like the pandas branch's
                        # to_numeric(errors="coerce"): unparseable → null.
                        # Time: seconds float → Duration("ns") through the pandas
                        # branch's conversion (a ms cast truncated .335 to .334).
                        if "Time" in cols:
                            seconds = weather_pl["Time"].cast(pl.Float64, strict=False)
                            exprs.append(
                                pl.Series(
                                    "Time",
                                    _numeric_seconds_to_timedelta(
                                        pd.Series(seconds.to_numpy())
                                    ).to_numpy(),
                                )
                            )
                        # Float columns
                        float_cols = [
                            _col
                            for _col in (
                                "AirTemp",
                                "Humidity",
                                "Pressure",
                                "TrackTemp",
                                "WindSpeed",
                            )
                            if _col in cols
                        ]
                        exprs.extend(
                            [pl.col(_col).cast(pl.Float64, strict=False) for _col in float_cols]
                        )
                        # Int column: round like the pandas branch, not truncate
                        if "WindDirection" in cols:
                            exprs.append(
                                pl.col("WindDirection")
                                .cast(pl.Float64, strict=False)
                                .round()
                                .cast(pl.Int64, strict=False)
                            )
                        # Bool column
                        if "Rainfall" in cols:
                            exprs.append(pl.col("Rainfall").cast(pl.Boolean, strict=False))
                        if exprs:
                            self._weather = weather_pl.with_columns(exprs)
                    else:
                        # "None" sentinels first: astype("boolean") below rejects
                        # the string, which used to empty the whole table.
                        weather_pd = _replace_null_like_strings(cast(pd.DataFrame, self._weather))
                        # Time: seconds float → timedelta
                        if "Time" in weather_pd.columns:
                            weather_pd["Time"] = _numeric_seconds_to_timedelta(weather_pd["Time"])
                        # Float columns
                        for _col in ("AirTemp", "Humidity", "Pressure", "TrackTemp", "WindSpeed"):
                            if _col in weather_pd.columns:
                                weather_pd[_col] = pd.to_numeric(
                                    weather_pd[_col], errors="coerce"
                                ).astype("float64")
                        # WindDirection: nullable integer (Int64 keeps NaN as pd.NA)
                        if "WindDirection" in weather_pd.columns:
                            weather_pd["WindDirection"] = (
                                pd.to_numeric(weather_pd["WindDirection"], errors="coerce")
                                .round()
                                .astype("Int64")
                            )
                        # Rainfall: nullable boolean
                        if "Rainfall" in weather_pd.columns:
                            weather_pd["Rainfall"] = weather_pd["Rainfall"].astype("boolean")
                        self._weather = weather_pd
            except DataNotFoundError:
                logger.info(
                    "No weather in session: %s/%s/%s", self.year, self.event_name, self.session_name
                )
                self._weather = _create_empty_df(self.lib)
            except (
                InvalidDataError,
                NetworkError,
                RuntimeError,
                TypeError,
                ValueError,
                *_polars_error_types(),
            ) as e:
                logger.warning(
                    "Failed to load weather for %s/%s/%s: %s",
                    self.year,
                    self.event_name,
                    self.session_name,
                    e,
                )
                self._weather = _create_empty_df(self.lib)
        return self._weather

    @property
    def weather_data(self) -> DataFrame:
        """Get session weather data (FastF1 compatibility alias).

        This is an alias for the `weather` property to match FastF1's API.
        Returns the same DataFrame as `session.weather`.
        """
        return self.weather

    def get_circuit_info(self) -> "CircuitInfo":
        """Circuit markers and layout (fastf1 parity plus tif1 extras).

        Built from the session's ``corners.json`` (both the old schema, corners
        only, and the new one with marshal lights/sectors, letters, pit loss,
        track outline and sections). When the file is missing or old-schema the
        MultiViewer API fills the marshal data and letters (cached permanently);
        corners and rotation from the file always win. Marker distances that
        MultiViewer left empty are fitted on the fastest lap's telemetry as
        fastf1 does. The result is memoised on the session.

        Returns:
            :class:`CircuitInfo` instance.
        """
        if self._circuit_info is None:
            info = self._build_circuit_info()
            self._circuit_info = info
            self._fit_marker_distances(info)
        return self._circuit_info

    def _fit_marker_distances(self, info: "CircuitInfo") -> None:
        """Fit the markers without a ``Distance`` on the fastest lap (either backend).

        Distances the circuit file gives are kept (fastf1's
        ``add_marker_distance`` overwrites every marker; tif1 only fills the
        gaps). Seasons before 2018 have no telemetry to fit on.
        """
        if self._ergast_only:
            return
        frames = (info.corners, info.marshal_lights, info.marshal_sectors)
        if not any(
            len(frame) and bool(np.isnan(_float_marker_distances(frame)).any()) for frame in frames
        ):
            return
        try:
            laps = cast(Any, self.laps)
            fastest = laps.pick_fastest() if len(laps) else None
        except (DataNotFoundError, NetworkError, InvalidDataError):
            fastest = None
        if fastest is None:
            return
        try:
            info._fit_marker_distances(fastest, only_missing=True)
        except (DataNotFoundError, NetworkError, InvalidDataError, TypeError, ValueError) as e:
            logger.info("Marker distances not fitted for %s: %s", self.api_path, e)

    def _event_texts(self, *keys: str) -> list[str]:
        """Text fields of the schedule row (``""`` when unknown), read from one lookup."""
        try:
            event = self.event
        except (EventNotFoundError, ValueError):
            return [""] * len(keys)
        if not hasattr(event, "get"):
            return [""] * len(keys)
        texts = []
        for key in keys:
            value = event.get(key)
            texts.append("" if value is None or _is_nan(value) else str(value))
        return texts

    def _build_circuit_info(self) -> "CircuitInfo":
        """Fetch ``corners.json`` and build the :class:`CircuitInfo` (MultiViewer backup)."""
        from .core_utils.circuit_info import build_circuit_info

        payload: dict[str, Any] | None
        try:
            data = self._fetch_json("corners.json")
            payload = data if isinstance(data, dict) else None
        except DataNotFoundError:
            logger.info("No corners.json for %s/%s/%s", self.year, self.gp, self.session)
            payload = None
        except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
            logger.warning(
                "Failed to load circuit info for %s/%s/%s: %s",
                self.year,
                self.gp,
                self.session,
                e,
            )
            payload = None
        circuit_id, circuit_name, country, location = self._event_texts(
            "CircuitId", "CircuitName", "Country", "Location"
        )
        return build_circuit_info(
            payload,
            year=self.year,
            circuit_id=circuit_id or None,
            circuit_name=circuit_name,
            country=country,
            location=location,
            lib=self.lib,
            allow_multiviewer=not bool(config.get("offline_mode", False)),
        )

    def _refresh_driver_indices(self) -> None:
        """Rebuild driver lookup indices when the underlying payload changes."""
        if self._drivers is None:
            self._driver_codes = set()
            self._driver_info_by_code = {}
            self._driver_index_source_id = None
            return

        driver_index_source_id = id(self._drivers)
        if self._driver_index_source_id == driver_index_source_id:
            return

        self._driver_codes = _extract_driver_codes(self._drivers)
        self._driver_info_by_code = _extract_driver_info_map(self._drivers)
        self._driver_index_source_id = driver_index_source_id

    def _prefetch_driver_lookup_and_laps(self, driver: str) -> dict[str, Any] | None:
        """Fetch drivers and target driver laps concurrently on first driver lookup.

        Nothing to fetch in a season served from Ergast alone: it has no
        timing files, and the drivers come from the classification.
        """
        if self._drivers is not None or self._ergast_only:
            return None
        if not bool(config.get("prefetch_driver_laps_on_get_driver", True)):
            return None

        requests = [
            (self.year, self.gp, self.session, "drivers.json"),
            (self.year, self.gp, self.session, SESSION_LAPTIMES_PATH),
        ]
        ultra_cold_enabled = self._resolve_ultra_cold_mode(None)
        use_cache = self.enable_cache and not ultra_cold_enabled and self._session_cache_available()
        # Cold-start speed comes from skipping cache READS; writes always
        # persist so the session is warm for the next process.
        write_cache = self.enable_cache
        validate_payload = not ultra_cold_enabled
        max_retries = (
            1 if (ultra_cold_enabled and config.get("ultra_cold_skip_retries", True)) else None
        )

        cached_results = (
            self._cached_payloads_sync([request[3] for request in requests]) if use_cache else None
        )
        try:
            results = (
                cached_results
                if cached_results is not None
                else self._run_fetch_multiple(
                    "get_driver",
                    requests,
                    use_cache=use_cache,
                    write_cache=write_cache,
                    validate_payload=validate_payload,
                    max_retries=max_retries,
                )
            )
        except (
            InvalidDataError,
            NetworkError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as e:
            logger.debug("Driver/laps prefetch skipped: %s", e)
            return None

        if write_cache and any(isinstance(payload, dict) for payload in results):
            self._mark_session_cache_populated()

        if len(results) > 0 and isinstance(results[0], dict):
            self._remember_local_payload("drivers.json", results[0])
            drivers_payload = results[0].get("drivers")
            if isinstance(drivers_payload, list):
                self._drivers = _session_driver_rows(drivers_payload)
                self._refresh_driver_indices()

        if len(results) > 1 and isinstance(results[1], dict):
            source_drivers = results[1].get("source_driver")
            if not isinstance(source_drivers, list | tuple):
                source_drivers = results[1].get("drv")

            if isinstance(source_drivers, list | tuple):
                self._remember_local_payload(SESSION_LAPTIMES_PATH, results[1])
                self._derive_driver_laptime_payloads_from_session(
                    results[1], drivers_filter=[driver]
                )
            else:
                # Compatibility path for tests or older mocks that still return a
                # single-driver laptime payload in the second slot.
                self._remember_local_payload(f"{driver}/laptimes.json", results[1])

        prefetched_payload = self._get_or_derive_driver_laptime_payload(driver)
        if prefetched_payload is not None:
            return prefetched_payload

        fallback_payloads, _ = self._fetch_laptime_payloads(
            [(self._get_driver_info(driver), f"{driver}/laptimes.json")],
            operation="get_driver",
            ultra_cold=ultra_cold_enabled,
        )
        return fallback_payloads[0] if fallback_payloads else None

    def _has_driver_code(self, driver: str) -> bool:
        """Check whether a driver code exists for this session."""
        _ = self._drivers_data
        if self._driver_codes is not None and driver in self._driver_codes:
            return True
        return any(row.get("driver") == driver for row in self._ergast_extra_rows())

    def _get_driver_info(self, driver: str) -> dict:
        """Get cached driver metadata by code."""
        _ = self._drivers_data
        info = None if self._driver_info_by_code is None else self._driver_info_by_code.get(driver)
        if info is not None:
            return info
        for row in self._ergast_extra_rows():
            if row.get("driver") == driver:
                return row
        return {"driver": driver, "team": ""}

    def _ergast_identity(self, driver_number: str, abbreviation: str) -> dict[str, Any]:
        """Ergast identity columns of one driver for :class:`Driver` (empty when unknown)."""
        if not self._ergast_enabled:
            return {}
        classification = _er.ergast_rows(self._ergast_race(), self._ergast_endpoint())
        # The code names the driver; the car number is only a fallback, since
        # a shared car (pre-1960s) lists two drivers under one number.
        matches = [
            row for row in classification if abbreviation and _er.driver_code(row) == abbreviation
        ]
        if not matches:
            matches = [
                row
                for row in classification
                if str(row.get("number", "")).strip() == str(driver_number)
            ]
        for row in matches[:1]:
            driver = row.get("Driver", {}) if isinstance(row.get("Driver"), dict) else {}
            constructor = (
                row.get("Constructor", {}) if isinstance(row.get("Constructor"), dict) else {}
            )
            return {
                "DriverId": str(driver.get("driverId", "")),
                "TeamId": str(constructor.get("constructorId", "")),
                "PermanentNumber": str(driver.get("permanentNumber", "")),
                "DriverNationality": str(driver.get("nationality", "")),
                "DateOfBirth": str(driver.get("dateOfBirth", "")),
            }
        return {}

    def _driver_has_timing_data(self, driver: str) -> bool:
        """False for drivers only the Ergast classification lists (no laps or telemetry).

        Decided from the ordered driver rows already built (never a fetch):
        a driver the rows do not mark as Ergast-only is assumed to have
        timing data, which keeps sessions with stubbed driver lists working.
        In a season served from Ergast alone every row is synthesized from
        the classification, so the flag says nothing there: the laps table
        (built from the Ergast lap timings) decides per driver instead.
        """
        if self._ergast_only:
            return True
        cache = self._driver_rows_cache
        if cache is None:
            return True
        return not any(row.get("driver") == driver and row.get("_ergast_only") for row in cache[1])

    def _ergast_extra_rows(self) -> list[dict]:
        """Driver rows only the Ergast classification knows (did not start)."""
        if self._driver_rows_cache is None and self._ergast_enabled and not self._ergast_only:
            self._driver_rows_info()
        cache = self._driver_rows_cache
        if cache is None:
            return []
        return [row for row in cache[1] if row.get("_ergast_only")]

    # -- Ergast classification (tif1.ergast) ---------------------------------

    def _ergast_endpoint(self) -> str:
        """Ergast endpoint whose rows classify this session (race results otherwise)."""
        return _er.ergast_endpoint(self.session_name)

    def _ergast_round_number(self) -> int | None:
        """This event's championship round (None for testing or unresolved events)."""
        if self._ergast_round is not _UNSET:
            return self._ergast_round
        number = 0
        try:
            event = self.event
            if "RoundNumber" in event.index:
                number = int(event["RoundNumber"])
        except (EventNotFoundError, ValueError, TypeError, KeyError) as exc:
            logger.debug("No round number for %s/%s: %s", self.year, self.event_name, exc)
        self._ergast_round = number if number > 0 else None
        return self._ergast_round

    def _ergast_table_race(self, endpoint: str) -> dict[str, Any] | None:
        """``Races[0]`` of an Ergast table for this round (Stats first, Jolpica fallback)."""
        if endpoint not in _ERGAST_TABLE_ENDPOINTS:
            raise ValueError(f"Unknown Ergast table {endpoint!r}")
        round_number = self._ergast_round_number()
        if round_number is None:
            return None
        from .ergast import structure
        from .ergast.router import QueryPlan, execute
        from .ergast.stats import StatsSource

        categories = {
            "results": (structure.Races_RaceResults, structure.RaceResults),
            "qualifying": (structure.Races_QualifyingResults, structure.QualifyingResults),
            "sprint": (structure.Races_SprintResults, structure.SprintResults),
            "laps": (structure.Races_Laps, structure.Laps),
            "pitstops": (structure.Races_PitStops, structure.PitStops),
        }
        category, subcategory = categories[endpoint]
        try:
            executed = execute(
                QueryPlan(
                    endpoint=endpoint,
                    table="RaceTable",
                    category=category,
                    subcategory=subcategory,
                    selectors={"season": self.year, "round": round_number},
                )
            )
        except TIF1Error as exc:
            if self._ergast_only:
                # Ergast is the only source of this season: a failure is the
                # caller's to see, never an empty session.
                raise
            logger.warning(
                "Ergast %s unavailable for %s/%s/%s: %s",
                endpoint,
                self.year,
                self.event_name,
                self.session_name,
                exc,
            )
            return None
        return StatsSource.first_race(executed.payload)

    def _load_ergast_payload(self) -> dict[str, Any] | None:
        """Fetch (once) the Ergast race record that classifies this session.

        The outcome is memoized either way: an Ergast-only season stores the
        error of a failed fetch and re-raises it on every later call instead
        of walking the mirrors again (the background prefetch fails exactly
        as the main thread would); a merge session keeps ``None`` because
        :meth:`_ergast_table_race` already degraded and logged.
        """
        with self._ergast_lock:
            if self._ergast_loaded:
                if self._ergast_error is not None:
                    raise self._ergast_error
                return self._ergast_payload
            payload = None
            if self._ergast_enabled:
                try:
                    payload = self._ergast_table_race(self._ergast_endpoint())
                except Exception as exc:
                    if self._ergast_only:
                        self._ergast_error = exc
                        self._ergast_loaded = True
                        raise
                    # A bug in the enrichment must not break a timing load.
                    logger.warning(
                        "Ergast classification skipped for %s/%s/%s: %s",
                        self.year,
                        self.event_name,
                        self.session_name,
                        exc,
                    )
            self._ergast_payload = payload
            self._ergast_loaded = True
            return payload

    def _ergast_race(self) -> dict[str, Any] | None:
        """The Ergast race record of this session (None when unavailable or disabled)."""
        if not self._ergast_enabled:
            return None
        return self._load_ergast_payload()

    def _start_ergast_prefetch(self) -> None:
        """Fetch the Ergast classification in the background while drivers.json loads.

        A daemon thread rather than a pool: ``ThreadPoolExecutor`` workers are
        joined when the interpreter exits, so a fetch still retrying against
        slow mirrors would hold up a script that is already done. The thread
        only fills the memo; :meth:`_load_ergast_payload` hands the outcome
        (or the stored error) to whoever asks next.
        """
        if not self._ergast_enabled or self._ergast_loaded or self._ergast_prefetch_started:
            return
        self._ergast_prefetch_started = True
        if not THREADS_AVAILABLE:
            return
        try:
            threading.Thread(
                target=self._prefetch_ergast_payload, name="tif1-ergast", daemon=True
            ).start()
        except RuntimeError:  # interpreter shutting down
            self._ergast_prefetch_started = False

    def _prefetch_ergast_payload(self) -> None:
        """Thread target of :meth:`_start_ergast_prefetch`: never lets an error escape."""
        try:
            self._load_ergast_payload()
        except Exception as exc:  # memoized; the main thread re-raises or degrades
            logger.debug(
                "Ergast prefetch failed for %s/%s/%s: %s",
                self.year,
                self.event_name,
                self.session_name,
                exc,
            )

    def _ergast_driver_payload(self) -> list[dict]:
        """Driver dicts synthesized from the Ergast classification (seasons before 2018)."""
        classification = _er.ergast_rows(self._ergast_race(), self._ergast_endpoint())
        if not classification:
            logger.info(
                "No Ergast classification for %s/%s/%s",
                self.year,
                self.event_name,
                self.session_name,
            )
            return []
        return _er.order_driver_rows([], classification, _er.team_color_map([], self.year))

    def _ergast_laps(self) -> DataFrame:
        """Laps built from Ergast lap timings (races from 1996; empty otherwise)."""
        if self.session_name != "Race":
            return _create_empty_df(self.lib)
        classification = _er.ergast_rows(self._ergast_race(), "results")
        laps_race = self._ergast_table_race("laps")
        stops_race = self._ergast_table_race("pitstops") if laps_race is not None else None
        payloads = _er.build_laps_payloads(laps_race, classification, stops_race)
        if not payloads:
            logger.info("No Ergast lap timings for %s/%s", self.year, self.event_name)
            return _create_empty_df(self.lib)
        if self.lib == "polars":
            merged = [
                (_normalize_lap_payload_polars(payload), driver, team)
                for payload, driver, team in payloads
            ]
            frame: DataFrame = pl.DataFrame(_merge_lap_payloads(merged), strict=False)
        else:
            merged = [
                (_normalize_lap_payload(payload), driver, team)
                for payload, driver, team in payloads
            ]
            frame = pd.DataFrame(_merge_lap_payloads(merged), copy=False)
        return _process_lap_df(frame, self.lib)

    def _as_session_lib(self, frame: pd.DataFrame) -> DataFrame:
        """A pandas frame in the session's backend (Arrow conversion for polars)."""
        if self.lib == "polars":
            from .core_utils.backend_conversion import pandas_to_polars

            return pandas_to_polars(frame)
        return frame

    @property
    def pit_stops(self) -> DataFrame:
        """Pit stops of the race from Ergast (2011 onward), one row per stop.

        Columns: ``DriverNumber``, ``Driver``, ``DriverId``, ``Stop``, ``Lap``,
        ``TimeOfDay`` (local time of the stop since midnight) and ``Duration``
        (pit-lane time). Empty for sessions other than the race and when no
        data exists; follows the session's ``lib``.
        """
        if self._pit_stops is None:
            race = self._ergast_table_race("pitstops") if self.session_name == "Race" else None
            # Driver numbers and codes come from the race classification; an
            # explicit pit-stop request fetches it even with the merge off.
            results = self._ergast_race() if self._ergast_enabled else None
            if race and results is None:
                results = self._ergast_table_race("results")
            classification = _er.ergast_rows(results, "results") if race else []
            self._pit_stops = self._as_session_lib(_er.build_pit_stops_frame(race, classification))
        return self._pit_stops

    def _ergast_standings(self, kind: str) -> DataFrame:
        from .ergast import structure
        from .ergast.router import QueryPlan, execute

        round_number = self._ergast_round_number()
        payload = None
        if round_number is not None:
            endpoint = "driverStandings" if kind == "driver" else "constructorStandings"
            category = (
                structure.StandingsLists_Driver
                if kind == "driver"
                else structure.StandingsLists_Constructor
            )
            subcategory = (
                structure.DriverStandings if kind == "driver" else structure.ConstructorStandings
            )
            try:
                payload = execute(
                    QueryPlan(
                        endpoint=endpoint,
                        table="StandingsTable",
                        category=category,
                        subcategory=subcategory,
                        selectors={"season": self.year, "round": round_number},
                    )
                ).payload
            except TIF1Error as exc:
                logger.warning(
                    "Ergast %s standings unavailable for %s round %s: %s",
                    kind,
                    self.year,
                    round_number,
                    exc,
                )
        return self._as_session_lib(_er.build_standings_frame(payload, kind))

    @property
    def driver_standings(self) -> DataFrame:
        """Drivers' championship standings after this event's round (Ergast)."""
        if self._driver_standings is None:
            self._driver_standings = self._ergast_standings("driver")
        return self._driver_standings

    @property
    def constructor_standings(self) -> DataFrame:
        """Constructors' championship standings after this event's round (Ergast)."""
        if self._constructor_standings is None:
            self._constructor_standings = self._ergast_standings("constructor")
        return self._constructor_standings

    def _derive_driver_laptime_payloads_from_session(
        self,
        session_payload: dict[str, Any],
        *,
        drivers_filter: list[str] | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Derive per-driver laptime payloads from a session-wide laptime table."""
        if not isinstance(session_payload, dict):
            return []

        source_drivers = session_payload.get("source_driver")
        if not isinstance(source_drivers, list | tuple):
            source_drivers = session_payload.get("drv")
        if not isinstance(source_drivers, list | tuple):
            return []

        allowed_drivers = set(drivers_filter) if drivers_filter else None
        positions_by_driver: dict[str, list[int]] = {}
        for pos, driver_code in enumerate(source_drivers):
            if not isinstance(driver_code, str) or not driver_code:
                continue
            if allowed_drivers is not None and driver_code not in allowed_drivers:
                continue
            positions_by_driver.setdefault(driver_code, []).append(pos)

        derived_payloads: list[tuple[str, dict[str, Any]]] = []
        for driver_code, positions in positions_by_driver.items():
            payload: dict[str, Any] = {}
            # Q10: a driver's rows are one contiguous run in the session table,
            # so each column is a single slice instead of a per-row gather
            # (the gather re-evaluated len() for every cell).
            first, last = positions[0], positions[-1]
            run = slice(first, last + 1) if last - first + 1 == len(positions) else None
            for key, value in session_payload.items():
                if isinstance(value, list | tuple):
                    size = len(value)
                    if run is not None and last < size:
                        payload[key] = list(value[run]) if isinstance(value, tuple) else value[run]
                    else:
                        payload[key] = [value[idx] if idx < size else None for idx in positions]
                    continue
                if hasattr(value, "__len__") and not isinstance(value, str | bytes | dict):
                    value_list = list(value)
                    size = len(value_list)
                    payload[key] = [value_list[idx] if idx < size else None for idx in positions]
                    continue
                payload[key] = value

            path = f"{driver_code}/laptimes.json"
            self._remember_local_payload(path, payload)
            derived_payloads.append((path, payload))

        return derived_payloads

    def _get_or_derive_driver_laptime_payload(self, driver: str) -> dict[str, Any] | None:
        """Return a memoized per-driver laptime payload, deriving it from session data if needed."""
        path = f"{driver}/laptimes.json"
        local_payload = self._get_local_payload(path)
        if local_payload is not None:
            return local_payload

        session_payload = self._get_local_payload(SESSION_LAPTIMES_PATH)
        if session_payload is None:
            return None

        self._derive_driver_laptime_payloads_from_session(
            session_payload,
            drivers_filter=[driver],
        )
        return self._get_local_payload(path)

    def _build_session_laptime_payload_from_driver_payloads(
        self,
        driver_requests: list[tuple[dict[str, Any], str]],
        payloads: list[dict[str, Any] | None],
    ) -> dict[str, Any] | None:
        """Combine per-driver laptime payloads into a synthetic session-wide payload."""
        normalized_payloads: list[tuple[dict[str, Any], dict[str, Any], int, set[str]]] = []
        list_keys: set[str] = set()

        for (driver_info, _path), payload in zip(driver_requests, payloads):
            if not isinstance(payload, dict) or not payload:
                continue

            driver_code = driver_info.get("driver")
            if not isinstance(driver_code, str) or not driver_code:
                continue

            payload_keys = {
                key for key, value in payload.items() if isinstance(value, list | tuple)
            }
            lap_count = max((len(payload[key]) for key in payload_keys), default=0)
            if lap_count <= 0:
                continue

            normalized_payloads.append((driver_info, payload, lap_count, payload_keys))
            list_keys.update(payload_keys)

        if not normalized_payloads:
            return None

        if "drv" not in list_keys and "source_driver" not in list_keys:
            list_keys.add("drv")
        if "team" not in list_keys and any(
            isinstance(driver_info.get("team"), str) and driver_info.get("team")
            for driver_info, _payload, _lap_count, _payload_keys in normalized_payloads
        ):
            list_keys.add("team")

        combined_payload: dict[str, Any] = {key: [] for key in list_keys}

        for driver_info, payload, lap_count, payload_keys in normalized_payloads:
            driver_code = cast(str, driver_info.get("driver"))
            team_name = driver_info.get("team", "")

            for key in list_keys:
                if key == "drv" and key not in payload_keys:
                    combined_payload[key].extend([driver_code] * lap_count)
                    continue

                if key == "source_driver" and key not in payload_keys:
                    combined_payload[key].extend([driver_code] * lap_count)
                    continue

                if key == "team" and key not in payload_keys:
                    combined_payload[key].extend([team_name] * lap_count)
                    continue

                value = payload.get(key)
                if isinstance(value, list | tuple):
                    values = list(value)
                    if len(values) < lap_count:
                        values = values + [None] * (lap_count - len(values))
                    combined_payload[key].extend(values[:lap_count])
                    continue

                combined_payload[key].extend([value] * lap_count)

        return combined_payload

    def _build_driver_laptime_requests(
        self,
        driver_pool: list[dict] | None = None,
        drivers_filter: list[str] | None = None,
    ) -> list[tuple[dict[str, Any], str]]:
        """Build per-driver laptime request tuples.

        Args:
            driver_pool: Optional explicit driver payload list. Defaults to session drivers.
            drivers_filter: Optional driver-code filter.

        Returns:
            List of (driver_info, laptime_path) tuples.
        """
        drivers_source = self._drivers_data if driver_pool is None else driver_pool
        if not drivers_source:
            return []

        allowed_drivers = set(drivers_filter) if drivers_filter else None
        requests: list[tuple[dict[str, Any], str]] = []

        for driver_info in drivers_source:
            if not isinstance(driver_info, dict):
                continue

            driver_code = driver_info.get("driver")
            if not isinstance(driver_code, str) or not driver_code:
                continue

            if allowed_drivers is not None and driver_code not in allowed_drivers:
                continue

            requests.append((driver_info, f"{driver_code}/laptimes.json"))

        return requests

    def _cached_payloads_sync(self, paths: list[str]) -> list[Any] | None:
        """Read session payloads from the persistent cache without an event loop.

        The warm counterpart of ``fetch_multiple_async(..., use_cache=True)``
        for requests that are all cache hits (Q10): the same tier reads
        (memory, then SQLite) and the same returned objects, without importing
        asyncio or starting a loop. Returns None when the cache is cold or
        bypassed, or when any path misses, so the caller keeps the async
        fetch for the whole request list.
        """
        if not self.enable_cache or not self._session_cache_available():
            return None
        if config.get("ci_mode", False):
            return None
        cache = get_cache()
        prefix = f"{self.year}/{self.gp}/{self.session}/"
        results: list[Any] = []
        for path in paths:
            cached = cache.get(prefix + path)  # memory tier first, then SQLite
            if cached is None:
                return None
            results.append(cached)
        return results

    @staticmethod
    def _run_fetch_multiple(
        operation: str, requests: list[tuple[int, str, str, str]], **fetch_kwargs: Any
    ) -> list[Any]:
        """Run ``fetch_multiple_async`` to completion from synchronous code."""
        try:
            return _run_coroutine_sync(
                lambda: fetch_multiple_async(requests, **fetch_kwargs), operation
            )
        except TypeError as e:
            # Some tests monkeypatch fetch_multiple_async with a simplified signature.
            if "unexpected keyword argument" not in str(e):
                raise
            return _run_coroutine_sync(lambda: fetch_multiple_async(requests), operation)

    def _fetch_laptime_payloads(
        self,
        driver_requests: list[tuple[dict[str, Any], str]],
        *,
        operation: str,
        ultra_cold: bool = False,
        prefer_session_payload: bool = True,
    ) -> tuple[list[dict[str, Any] | None], list[tuple[str, dict[str, Any]]]]:
        """Fetch laptime payloads using session-wide data first, with per-driver fallback."""
        if not driver_requests:
            return [], []

        use_cache = self.enable_cache and not ultra_cold and self._session_cache_available()
        # Cold-start speed comes from skipping cache READS; writes always
        # persist so the session is warm for the next process.
        write_cache = self.enable_cache
        validate_payload = not ultra_cold
        max_retries = 1 if (ultra_cold and config.get("ultra_cold_skip_retries", True)) else None

        normalized_results: list[dict[str, Any] | None] = [None] * len(driver_requests)
        cacheable_payloads: list[tuple[str, dict[str, Any]]] = []
        fetched_any_payload = False

        missing_drivers: list[str] = []
        for idx, (_driver_info, path) in enumerate(driver_requests):
            driver_code = path.split("/", 1)[0]
            local_payload = self._get_or_derive_driver_laptime_payload(driver_code)
            if local_payload is not None:
                normalized_results[idx] = local_payload
                continue
            missing_drivers.append(driver_code)

        session_payload = self._get_local_payload(SESSION_LAPTIMES_PATH)
        if prefer_session_payload and session_payload is None and missing_drivers:
            session_result: dict[str, Any] | None = None
            session_results = (
                self._cached_payloads_sync([SESSION_LAPTIMES_PATH]) if use_cache else None
            )
            try:
                if session_results is None:
                    session_results = self._run_fetch_multiple(
                        operation,
                        [(self.year, self.gp, self.session, SESSION_LAPTIMES_PATH)],
                        use_cache=use_cache,
                        write_cache=write_cache,
                        validate_payload=validate_payload,
                        max_retries=max_retries,
                    )
                if session_results and isinstance(session_results[0], dict):
                    session_result = session_results[0]
                    fetched_any_payload = True
            except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
                logger.debug(
                    "Session laptime fetch failed during %s, falling back: %s", operation, e
                )

            processed_session_payload, session_cacheable = self._process_laptime_payload(
                session_result,
                SESSION_LAPTIMES_PATH,
                ultra_cold=ultra_cold,
            )
            session_payload = processed_session_payload
            if session_cacheable is not None:
                cacheable_payloads.append(session_cacheable)

        if isinstance(session_payload, dict):
            cacheable_payloads.extend(
                self._derive_driver_laptime_payloads_from_session(
                    session_payload,
                    drivers_filter=missing_drivers or None,
                )
            )
            for idx, (_driver_info, path) in enumerate(driver_requests):
                if normalized_results[idx] is not None:
                    continue
                driver_code = path.split("/", 1)[0]
                normalized_results[idx] = self._get_or_derive_driver_laptime_payload(driver_code)

        pending_requests: list[tuple[int, str, str, str]] = []
        pending_indexes: list[int] = []

        for idx, (_driver_info, path) in enumerate(driver_requests):
            local_payload = normalized_results[idx]
            if local_payload is not None:
                normalized_results[idx] = local_payload
                continue
            pending_indexes.append(idx)
            pending_requests.append((self.year, self.gp, self.session, path))

        pending_results: list[dict[str, Any] | None] = []
        if pending_requests:
            cached_pending = (
                self._cached_payloads_sync([request[3] for request in pending_requests])
                if use_cache
                else None
            )
            try:
                pending_results = (
                    cached_pending
                    if cached_pending is not None
                    else self._run_fetch_multiple(
                        operation,
                        pending_requests,
                        use_cache=use_cache,
                        write_cache=write_cache,
                        validate_payload=validate_payload,
                        max_retries=max_retries,
                    )
                )
            except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
                logger.warning(f"Async laptime fetch failed, falling back to sequential: {e}")
                pending_results = [None] * len(pending_requests)

            if len(pending_results) < len(pending_requests):
                pending_results = pending_results + [None] * (
                    len(pending_requests) - len(pending_results)
                )
            elif len(pending_results) > len(pending_requests):
                pending_results = pending_results[: len(pending_requests)]

            for idx, payload in zip(pending_indexes, pending_results):
                normalized_results[idx] = payload
            if any(isinstance(item, dict) for item in pending_results):
                fetched_any_payload = True

        for idx, (_driver_info, path) in enumerate(driver_requests):
            data = normalized_results[idx]
            processed_payload, cacheable_entry = self._process_laptime_payload(
                data, path, ultra_cold=ultra_cold
            )
            normalized_results[idx] = processed_payload
            if cacheable_entry is not None:
                cacheable_payloads.append(cacheable_entry)

        if session_payload is None and prefer_session_payload:
            synthetic_session_payload = self._build_session_laptime_payload_from_driver_payloads(
                driver_requests,
                normalized_results,
            )
            if synthetic_session_payload is not None:
                self._remember_local_payload(SESSION_LAPTIMES_PATH, synthetic_session_payload)
                if ultra_cold:
                    cacheable_payloads.insert(
                        0,
                        (SESSION_LAPTIMES_PATH, synthetic_session_payload),
                    )

        if write_cache and fetched_any_payload:
            self._mark_session_cache_populated()

        return normalized_results, cacheable_payloads

    def _process_laptime_payload(
        self,
        data: dict[str, Any] | None,
        path: str,
        *,
        ultra_cold: bool = False,
    ) -> tuple[dict[str, Any] | None, tuple[str, dict[str, Any]] | None]:
        """Process a single laptime payload with validation and metadata.

        Shared logic for both sync and async laptime payload processing.

        Args:
            data: Raw payload data (may be None if fetch failed)
            path: API path for the payload
            ultra_cold: Whether to skip validation and prepare for caching

        Returns:
            Tuple of (processed_payload, cacheable_entry)
            - processed_payload: Validated and memoized payload, or None if invalid
            - cacheable_entry: (path, data) tuple for ultra_cold caching, or None
        """
        cacheable_entry = None

        # In ultra-cold mode, stay on the async bulk critical path and avoid
        # a second synchronous refetch when a payload comes back empty.
        if data is None:
            if ultra_cold:
                return None, None
            try:
                data = self._fetch_json(path)
            except (DataNotFoundError, InvalidDataError, NetworkError, TypeError, ValueError):
                data = None

        # Process valid payloads
        if isinstance(data, dict):
            # Remember locally for subsequent access
            self._remember_local_payload(path, data)

            # Prepare for caching if in ultra_cold mode
            if ultra_cold:
                cacheable_entry = (path, data)

            return data, cacheable_entry

        return None, None

    async def _fetch_laptime_payloads_async(
        self,
        driver_requests: list[tuple[dict[str, Any], str]],
        *,
        operation: str,
        ultra_cold: bool = False,
        prefer_session_payload: bool = True,
    ) -> tuple[list[dict[str, Any] | None], list[tuple[str, dict[str, Any]]]]:
        """Async laptime fetch using session-wide data first, with per-driver fallback."""
        if not driver_requests:
            return [], []

        use_cache = self.enable_cache and not ultra_cold and self._session_cache_available()
        # Cold-start speed comes from skipping cache READS; writes always
        # persist so the session is warm for the next process.
        write_cache = self.enable_cache
        validate_payload = not ultra_cold
        max_retries = 1 if (ultra_cold and config.get("ultra_cold_skip_retries", True)) else None

        normalized_results: list[dict[str, Any] | None] = [None] * len(driver_requests)
        cacheable_payloads: list[tuple[str, dict[str, Any]]] = []
        fetched_any_payload = False

        missing_drivers: list[str] = []
        for idx, (_driver_info, path) in enumerate(driver_requests):
            driver_code = path.split("/", 1)[0]
            local_payload = self._get_or_derive_driver_laptime_payload(driver_code)
            if local_payload is not None:
                normalized_results[idx] = local_payload
                continue
            missing_drivers.append(driver_code)

        session_payload = self._get_local_payload(SESSION_LAPTIMES_PATH)
        if prefer_session_payload and session_payload is None and missing_drivers:
            session_result: dict[str, Any] | None = None
            try:
                try:
                    session_results = await fetch_multiple_async(
                        [(self.year, self.gp, self.session, SESSION_LAPTIMES_PATH)],
                        use_cache=use_cache,
                        write_cache=write_cache,
                        validate_payload=validate_payload,
                        max_retries=max_retries,
                    )
                except TypeError as e:
                    if "unexpected keyword argument" not in str(e):
                        raise
                    session_results = await fetch_multiple_async(
                        [(self.year, self.gp, self.session, SESSION_LAPTIMES_PATH)]
                    )
                if session_results and isinstance(session_results[0], dict):
                    session_result = session_results[0]
                    fetched_any_payload = True
            except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
                logger.debug(
                    "Async session laptime fetch failed during %s, falling back: %s", operation, e
                )

            processed_session_payload, session_cacheable = self._process_laptime_payload(
                session_result,
                SESSION_LAPTIMES_PATH,
                ultra_cold=ultra_cold,
            )
            session_payload = processed_session_payload
            if session_cacheable is not None:
                cacheable_payloads.append(session_cacheable)

        if isinstance(session_payload, dict):
            cacheable_payloads.extend(
                self._derive_driver_laptime_payloads_from_session(
                    session_payload,
                    drivers_filter=missing_drivers or None,
                )
            )
            for idx, (_driver_info, path) in enumerate(driver_requests):
                if normalized_results[idx] is not None:
                    continue
                driver_code = path.split("/", 1)[0]
                normalized_results[idx] = self._get_or_derive_driver_laptime_payload(driver_code)

        pending_requests: list[tuple[int, str, str, str]] = []
        pending_indexes: list[int] = []

        for idx, (_driver_info, path) in enumerate(driver_requests):
            local_payload = normalized_results[idx]
            if local_payload is not None:
                normalized_results[idx] = local_payload
                continue
            pending_indexes.append(idx)
            pending_requests.append((self.year, self.gp, self.session, path))

        pending_results: list[dict[str, Any] | None] = []
        if pending_requests:
            try:
                try:
                    pending_results = await fetch_multiple_async(
                        pending_requests,
                        use_cache=use_cache,
                        write_cache=write_cache,
                        validate_payload=validate_payload,
                        max_retries=max_retries,
                    )
                except TypeError as e:
                    # Some tests monkeypatch fetch_multiple_async with a simplified signature.
                    if "unexpected keyword argument" not in str(e):
                        raise
                    pending_results = await fetch_multiple_async(pending_requests)
            except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
                logger.warning(f"Async laptime fetch failed during {operation}, falling back: {e}")
                pending_results = [None] * len(pending_requests)

            if len(pending_results) < len(pending_requests):
                pending_results = pending_results + [None] * (
                    len(pending_requests) - len(pending_results)
                )
            elif len(pending_results) > len(pending_requests):
                pending_results = pending_results[: len(pending_requests)]

            for idx, payload in zip(pending_indexes, pending_results):
                normalized_results[idx] = payload
            if any(isinstance(item, dict) for item in pending_results):
                fetched_any_payload = True

        for idx, (_driver_info, path) in enumerate(driver_requests):
            data = normalized_results[idx]
            processed_payload, cacheable_entry = self._process_laptime_payload(
                data, path, ultra_cold=ultra_cold
            )
            normalized_results[idx] = processed_payload
            if cacheable_entry is not None:
                cacheable_payloads.append(cacheable_entry)

        if session_payload is None and prefer_session_payload:
            synthetic_session_payload = self._build_session_laptime_payload_from_driver_payloads(
                driver_requests,
                normalized_results,
            )
            if synthetic_session_payload is not None:
                self._remember_local_payload(SESSION_LAPTIMES_PATH, synthetic_session_payload)
                if ultra_cold:
                    cacheable_payloads.insert(
                        0,
                        (SESSION_LAPTIMES_PATH, synthetic_session_payload),
                    )

        if write_cache and fetched_any_payload:
            self._mark_session_cache_populated()

        return normalized_results, cacheable_payloads

    def _extract_valid_lap_times(self, lap_data: dict[str, Any]) -> list[tuple[int, int, float]]:
        """Extract valid lap times from raw lap payload.

        Pure function that extracts all valid (index, lap_number, lap_time) tuples
        from a lap data payload. This is shared logic used by both sync and async paths.

        Args:
            lap_data: Raw lap data dictionary containing 'lap' and 'time' arrays

        Returns:
            List of (index, lap_number, lap_time) tuples for all valid laps,
            sorted by lap time (fastest first). Deleted laps (``del`` flag) are
            not valid: a deleted time never counts as the fastest lap.
        """
        if not isinstance(lap_data, dict):
            return []

        lap_values = lap_data.get("lap")
        if lap_values is None:
            lap_values = lap_data.get(COL_LAP_NUMBER, lap_data.get(COL_LAP_NUMBER_ALT))

        time_values = lap_data.get("time")
        if time_values is None:
            time_values = lap_data.get(COL_LAP_TIME)

        if not isinstance(lap_values, list | tuple) or not isinstance(time_values, list | tuple):
            return []

        deleted_values = lap_data.get("del")
        if deleted_values is None:
            deleted_values = lap_data.get("deleted", lap_data.get(COL_DELETED))
        if not isinstance(deleted_values, list | tuple):
            deleted_values = ()

        valid_laps: list[tuple[int, int, float]] = []
        for idx, (lap_value, lap_time_value) in enumerate(zip(lap_values, time_values)):
            if idx < len(deleted_values) and deleted_values[idx] is True:
                continue
            try:
                lap_num = _coerce_lap_number(lap_value)
                lap_time = _coerce_lap_time(lap_time_value)
                valid_laps.append((idx, lap_num, lap_time))
            except ValueError:
                continue

        # Sort by lap time (fastest first)
        valid_laps.sort(key=lambda x: x[2])
        return valid_laps

    def _find_fastest_lap(
        self, valid_laps: list[tuple[int, int, float]]
    ) -> tuple[int, int, float] | None:
        """Find the fastest lap from a list of valid laps.

        Pure function that returns the fastest lap from a pre-sorted list.

        Args:
            valid_laps: List of (index, lap_number, lap_time) tuples, sorted by time

        Returns:
            The fastest (index, lap_number, lap_time) tuple, or None if list is empty
        """
        if not valid_laps:
            return None
        return valid_laps[0]

    def _format_lap_result(
        self,
        lap_data: dict[str, Any],
        driver_info: dict[str, Any],
        best_idx: int,
        best_lap_num: int,
        best_lap_time: float,
    ) -> dict[str, Any]:
        """Format a lap result row from raw data and metadata.

        Pure function that constructs a result dictionary with all lap data fields
        for the specified lap index, plus driver and team metadata.

        Args:
            lap_data: Raw lap data dictionary with array fields
            driver_info: Driver metadata dict with 'driver' and 'team' keys
            best_idx: Index of the lap in the arrays
            best_lap_num: Lap number
            best_lap_time: Lap time in seconds

        Returns:
            Dictionary with all lap fields plus driver and team
        """
        driver_code = driver_info.get("driver", "")
        team = driver_info.get("team", "")

        row: dict[str, Any] = {}
        for key, values in lap_data.items():
            if isinstance(values, list | tuple) and best_idx < len(values):
                row[key] = values[best_idx]

        # Ensure required fields are present
        if "time" not in row and COL_LAP_TIME not in row:
            row["time"] = best_lap_time
        if "lap" not in row and COL_LAP_NUMBER not in row and COL_LAP_NUMBER_ALT not in row:
            row["lap"] = best_lap_num

        row[COL_DRIVER] = driver_code if isinstance(driver_code, str) else str(driver_code)
        row[COL_TEAM] = team if isinstance(team, str) else str(team)

        return row

    def _extract_fastest_lap_candidate(
        self, driver: str, lap_data: Any
    ) -> tuple[str, int, float] | None:
        """Extract fastest lap tuple (driver, lap, lap_time) from raw lap payload.

        Uses shared helper _extract_valid_lap_times and _find_fastest_lap.
        """
        if not isinstance(lap_data, dict):
            return None

        valid_laps = self._extract_valid_lap_times(lap_data)
        fastest = self._find_fastest_lap(valid_laps)

        if fastest is None:
            return None

        _idx, lap_num, lap_time = fastest
        return (driver, lap_num, lap_time)

    def _extract_fastest_lap_row(
        self, driver_info: dict[str, Any], lap_data: Any
    ) -> dict[str, Any] | None:
        """Extract fastest lap row from a raw laptime payload.

        Uses shared helpers _extract_valid_lap_times, _find_fastest_lap, and
        _format_lap_result.
        """
        if not isinstance(lap_data, dict):
            return None

        driver_code = driver_info.get("driver")
        if not isinstance(driver_code, str) or not driver_code:
            return None

        valid_laps = self._extract_valid_lap_times(lap_data)
        fastest = self._find_fastest_lap(valid_laps)

        if fastest is None:
            return None

        best_idx, best_lap_num, best_lap_time = fastest
        return self._format_lap_result(lap_data, driver_info, best_idx, best_lap_num, best_lap_time)

    def _find_overall_fastest_lap(
        self, driver_requests: list[tuple[dict[str, Any], str]], payloads: list[Any]
    ) -> dict[str, Any] | None:
        """Find the single fastest lap across all drivers.

        Shared helper that extracts the fastest lap row from multiple driver payloads.

        Args:
            driver_requests: List of (driver_info, path) tuples
            payloads: List of lap data payloads corresponding to driver_requests

        Returns:
            Dictionary with the fastest lap row, or None if no valid laps found
        """
        best_row = None
        best_time = None

        for (driver_info, _path), payload in zip(driver_requests, payloads):
            row = self._extract_fastest_lap_row(driver_info, payload)
            if row is None:
                continue

            lap_time_value = row.get(COL_LAP_TIME, row.get("time"))
            try:
                lap_time = _coerce_lap_time(lap_time_value)
            except ValueError:
                continue

            if best_time is None or lap_time < best_time:
                best_row = row
                best_time = lap_time

        return best_row

    def _collect_fastest_laps_by_driver(
        self, driver_requests: list[tuple[dict[str, Any], str]], payloads: list[Any]
    ) -> list[dict[str, Any]]:
        """Collect fastest lap for each driver.

        Shared helper that extracts fastest lap rows for all drivers.

        Args:
            driver_requests: List of (driver_info, path) tuples
            payloads: List of lap data payloads corresponding to driver_requests

        Returns:
            List of fastest lap row dictionaries, one per driver with valid laps
        """
        fastest_rows: list[dict[str, Any]] = []
        for (driver_info, _path), payload in zip(driver_requests, payloads):
            row = self._extract_fastest_lap_row(driver_info, payload)
            if row is not None:
                fastest_rows.append(row)
        return fastest_rows

    def _get_fastest_laps_from_raw(
        self,
        *,
        by_driver: bool,
        drivers: list[str] | None = None,
        ultra_cold: bool = False,
    ) -> DataFrame:
        """Compute fastest lap rows directly from raw per-driver payloads."""
        driver_pool: list[dict[str, Any]] | None = None
        if ultra_cold:
            driver_pool, _ = self._load_drivers_for_fastest_lap_reference(ultra_cold=True)

        driver_requests = self._build_driver_laptime_requests(
            driver_pool=driver_pool,
            drivers_filter=drivers,
        )
        if not driver_requests:
            return _create_empty_df(self.lib)

        payloads, cacheable_payloads = self._fetch_laptime_payloads(
            driver_requests,
            operation="get_fastest_laps",
            ultra_cold=ultra_cold,
        )

        # Use shared helpers instead of duplicated logic
        if by_driver:
            fastest_rows = self._collect_fastest_laps_by_driver(driver_requests, payloads)
        else:
            best_row = self._find_overall_fastest_lap(driver_requests, payloads)
            fastest_rows = [best_row] if best_row is not None else []

        if cacheable_payloads and self._should_backfill_ultra_cold_cache(ultra_cold):
            self._schedule_background_cache_fill(json_payloads=cacheable_payloads)

        if not fastest_rows:
            return _create_empty_df(self.lib)

        fastest_df = (
            pl.DataFrame(fastest_rows, strict=False)
            if self.lib == "polars"
            else pd.DataFrame(fastest_rows, copy=False)
        )
        fastest_df = _process_lap_df(fastest_df, self.lib)
        return self._select_fastest_laps(fastest_df, by_driver=by_driver)

    async def _get_fastest_laps_from_raw_async(
        self,
        *,
        by_driver: bool,
        drivers: list[str] | None = None,
        ultra_cold: bool = False,
    ) -> DataFrame:
        """Async variant of fastest-lap scan directly from raw per-driver payloads."""
        driver_pool: list[dict[str, Any]] | None = None
        if ultra_cold:
            driver_pool, _ = self._load_drivers_for_fastest_lap_reference(ultra_cold=True)

        driver_requests = self._build_driver_laptime_requests(
            driver_pool=driver_pool,
            drivers_filter=drivers,
        )
        if not driver_requests:
            return _create_empty_df(self.lib)

        payloads, cacheable_payloads = await self._fetch_laptime_payloads_async(
            driver_requests,
            operation="get_fastest_laps_async",
            ultra_cold=ultra_cold,
        )

        # Use shared helpers instead of duplicated logic
        if by_driver:
            fastest_rows = self._collect_fastest_laps_by_driver(driver_requests, payloads)
        else:
            best_row = self._find_overall_fastest_lap(driver_requests, payloads)
            fastest_rows = [best_row] if best_row is not None else []

        if cacheable_payloads and self._should_backfill_ultra_cold_cache(ultra_cold):
            self._schedule_background_cache_fill(json_payloads=cacheable_payloads)

        if not fastest_rows:
            return _create_empty_df(self.lib)

        fastest_df = (
            pl.DataFrame(fastest_rows, strict=False)
            if self.lib == "polars"
            else pd.DataFrame(fastest_rows, copy=False)
        )
        fastest_df = _process_lap_df(fastest_df, self.lib)
        return self._select_fastest_laps(fastest_df, by_driver=by_driver)

    def _fastest_lap_candidates_from_laps_tier(
        self, driver_requests: list[tuple[dict[str, Any], str]]
    ) -> list[tuple[str, int, float]] | None:
        """Per-driver fastest ``(driver, lap, seconds)`` from the laps frame tier.

        Q10: on a warm session the laps frame tier answers the fastest-lap
        lookup without reading and splitting the session laptime payload.
        The selection is the raw-payload scan's: deleted laps and laps without
        a time or lap number never count, the first lap of a driver's fastest
        time wins, and candidates follow ``driver_requests`` (drivers.json)
        order. Returns None (callers keep the payload scan) when the backend
        is not pandas, the session is not cached, or the tier has no usable
        frame.
        """
        if self.lib != "pandas" or not self.enable_cache or not self._session_cache_available():
            return None
        frame = get_cache().get_laps_frame(self.year, self.gp, self.session, lib="pandas")
        if frame is None:
            return None
        if self._laps is None:
            # The frame ``laps`` would load from the same tier: keep it, so the
            # telemetry built next gets its session clock (``_lap_start_lookup``).
            self._laps = self._as_session_laps(frame)
        try:
            lap_time = frame[COL_LAP_TIME]
            if not pd.api.types.is_timedelta64_dtype(lap_time.dtype):
                return None
            seconds = lap_time.dt.total_seconds().to_numpy(dtype=float, na_value=np.nan)
            lap_numbers = pd.to_numeric(frame[COL_LAP_NUMBER], errors="coerce").to_numpy(
                dtype=float, na_value=np.nan
            )
            codes = frame[COL_DRIVER].to_numpy(dtype=object)
            valid = ~(np.isnan(seconds) | np.isnan(lap_numbers))
            not_deleted = _not_deleted_mask(frame)
            if not_deleted is not None:
                valid &= not_deleted
        except (KeyError, TypeError, ValueError) as e:
            logger.debug("Laps frame tier unusable for fastest-lap lookup: %s", e)
            return None

        best: dict[Any, tuple[float, int]] = {}
        seconds_list = seconds.tolist()
        for idx in np.flatnonzero(valid).tolist():
            code = codes[idx]
            lap_seconds = seconds_list[idx]
            current = best.get(code)
            if current is None or lap_seconds < current[0]:
                best[code] = (lap_seconds, idx)

        candidates: list[tuple[str, int, float]] = []
        for driver_info, _path in driver_requests:
            driver_code = driver_info.get("driver")
            hit = best.get(driver_code)
            if isinstance(driver_code, str) and hit is not None:
                candidates.append((driver_code, int(lap_numbers[hit[1]]), hit[0]))
        return candidates

    def _get_fastest_lap_refs_from_raw(
        self,
        *,
        drivers: list[str] | None = None,
        ultra_cold: bool = False,
    ) -> list[tuple[str, int]]:
        """Resolve per-driver fastest lap references directly from raw payloads."""
        drivers_pool, driver_payloads = self._load_drivers_for_fastest_lap_reference(
            ultra_cold=ultra_cold
        )
        driver_requests = self._build_driver_laptime_requests(
            driver_pool=drivers_pool,
            drivers_filter=drivers,
        )
        if not driver_requests:
            return []

        tier_candidates = (
            None if ultra_cold else self._fastest_lap_candidates_from_laps_tier(driver_requests)
        )
        if tier_candidates is not None:
            tier_candidates.sort(key=lambda item: item[2])
            return [(driver, lap_num) for driver, lap_num, _lap_time in tier_candidates]

        try:
            payloads, laptime_payloads = self._fetch_laptime_payloads(
                driver_requests,
                operation="get_fastest_laps_tels",
                ultra_cold=ultra_cold,
                prefer_session_payload=not ultra_cold,
            )
        except TypeError as e:
            if "unexpected keyword argument 'prefer_session_payload'" not in str(e):
                raise
            payloads, laptime_payloads = self._fetch_laptime_payloads(
                driver_requests,
                operation="get_fastest_laps_tels",
                ultra_cold=ultra_cold,
            )

        return self._process_fastest_lap_refs_from_payloads(
            driver_requests,
            payloads,
            driver_payloads,
            laptime_payloads,
            ultra_cold=ultra_cold,
        )

    def _process_fastest_lap_refs_from_payloads(
        self,
        driver_requests: list[tuple[dict[str, Any], str]],
        payloads: list[dict[str, Any] | None],
        driver_payloads: list[tuple[str, dict[str, Any]]],
        laptime_payloads: list[tuple[str, dict[str, Any]]],
        *,
        ultra_cold: bool = False,
    ) -> list[tuple[str, int]]:
        """Process laptime payloads to extract fastest lap references.

        Shared logic for extracting and sorting fastest lap candidates from raw payloads.
        Used by both sync and async variants.

        Args:
            driver_requests: List of (driver_info, path) tuples
            payloads: List of fetched laptime payloads
            driver_payloads: Driver payloads for cache backfill
            laptime_payloads: Laptime payloads for cache backfill
            ultra_cold: Whether ultra-cold mode is enabled

        Returns:
            List of (driver_code, lap_number) tuples sorted by lap time
        """
        should_backfill = self._should_backfill_ultra_cold_cache(ultra_cold)
        backfill_payloads: list[tuple[str, dict[str, Any]]] = []
        if should_backfill:
            backfill_payloads.extend(driver_payloads)
            backfill_payloads.extend(laptime_payloads)
            if backfill_payloads:
                self._schedule_background_cache_fill(json_payloads=backfill_payloads)

        fastest_candidates: list[tuple[str, int, float]] = []
        for (driver_info, _path), lap_data in zip(driver_requests, payloads):
            driver_code = driver_info.get("driver")
            if not isinstance(driver_code, str):
                continue
            candidate = self._extract_fastest_lap_candidate(driver_code, lap_data)
            if candidate is not None:
                fastest_candidates.append(candidate)

        if not fastest_candidates:
            return []

        fastest_candidates.sort(key=lambda item: item[2])
        return [(driver, lap_num) for driver, lap_num, _lap_time in fastest_candidates]

    async def _get_fastest_lap_refs_from_raw_async(
        self,
        *,
        drivers: list[str] | None = None,
        ultra_cold: bool = False,
    ) -> list[tuple[str, int]]:
        """Async variant of per-driver fastest lap reference resolution."""
        drivers_pool, driver_payloads = self._load_drivers_for_fastest_lap_reference(
            ultra_cold=ultra_cold
        )
        driver_requests = self._build_driver_laptime_requests(
            driver_pool=drivers_pool,
            drivers_filter=drivers,
        )
        if not driver_requests:
            return []

        try:
            payloads, laptime_payloads = await self._fetch_laptime_payloads_async(
                driver_requests,
                operation="get_fastest_laps_tels_async",
                ultra_cold=ultra_cold,
                prefer_session_payload=not ultra_cold,
            )
        except TypeError as e:
            if "unexpected keyword argument 'prefer_session_payload'" not in str(e):
                raise
            payloads, laptime_payloads = await self._fetch_laptime_payloads_async(
                driver_requests,
                operation="get_fastest_laps_tels_async",
                ultra_cold=ultra_cold,
            )

        return self._process_fastest_lap_refs_from_payloads(
            driver_requests,
            payloads,
            driver_payloads,
            laptime_payloads,
            ultra_cold=ultra_cold,
        )

    def _extract_fastest_lap_from_loaded_laps(self) -> tuple[str, int] | None:
        """Resolve fastest lap metadata from an already loaded laps DataFrame."""
        fastest_laps = self.get_fastest_laps(by_driver=False)
        if _is_empty_df(fastest_laps, self.lib):
            return None

        if self.lib == "polars":
            fastest_laps_pl = cast(Any, fastest_laps)
            lap_col = _get_lap_column(fastest_laps_pl, self.lib)
            if COL_DRIVER not in fastest_laps_pl.columns or lap_col not in fastest_laps_pl.columns:
                return None
            driver, lap_value = fastest_laps_pl.select([COL_DRIVER, lap_col]).row(0)
        else:
            fastest_laps_pd = cast(pd.DataFrame, fastest_laps)
            lap_col = _get_lap_column(fastest_laps_pd, self.lib)
            if COL_DRIVER not in fastest_laps_pd.columns or lap_col not in fastest_laps_pd.columns:
                return None
            row = fastest_laps_pd.iloc[0]
            driver = row[COL_DRIVER]
            lap_value = row[lap_col]

        if not isinstance(driver, str):
            return None
        try:
            lap_num = _coerce_lap_number(lap_value)
        except ValueError:
            return None
        return (driver, lap_num)

    def _find_fastest_lap_reference_from_raw(
        self, drivers: list[dict], *, ultra_cold: bool = False
    ) -> tuple[str, int] | None:
        """Resolve fastest lap metadata from raw lap payloads without building a full laps DataFrame."""
        if not drivers:
            return None

        driver_requests = self._build_driver_laptime_requests(driver_pool=drivers)
        if not driver_requests:
            return None

        tier_candidates = (
            None if ultra_cold else self._fastest_lap_candidates_from_laps_tier(driver_requests)
        )
        if tier_candidates is not None:
            if not tier_candidates:
                return None
            # min() keeps the first of equal times: drivers.json order, as below.
            driver_code, lap_num, _lap_time = min(tier_candidates, key=lambda item: item[2])
            return (driver_code, lap_num)

        results, cacheable_payloads = self._fetch_laptime_payloads(
            driver_requests,
            operation="get_fastest_lap_tel",
            ultra_cold=ultra_cold,
        )
        best_candidate = None
        for (driver_info, _path), lap_data in zip(driver_requests, results):
            driver_code = driver_info.get("driver")
            if not isinstance(driver_code, str):
                continue

            candidate = self._extract_fastest_lap_candidate(driver_code, lap_data)
            if candidate is None:
                continue

            if best_candidate is None or candidate[2] < best_candidate[2]:
                best_candidate = candidate

        if cacheable_payloads and self._should_backfill_ultra_cold_cache(ultra_cold):
            self._schedule_background_cache_fill(json_payloads=cacheable_payloads)

        if best_candidate is None:
            return None
        return (best_candidate[0], best_candidate[1])

    async def _find_fastest_lap_reference_from_raw_async(
        self, drivers: list[dict], *, ultra_cold: bool = False
    ) -> tuple[str, int] | None:
        """Async variant of fastest-lap reference resolution from raw payloads."""
        if not drivers:
            return None

        driver_requests = self._build_driver_laptime_requests(driver_pool=drivers)
        if not driver_requests:
            return None

        results, cacheable_payloads = await self._fetch_laptime_payloads_async(
            driver_requests,
            operation="get_fastest_lap_tel_async",
            ultra_cold=ultra_cold,
        )
        best_candidate = None
        for (driver_info, _path), lap_data in zip(driver_requests, results):
            driver_code = driver_info.get("driver")
            if not isinstance(driver_code, str):
                continue

            candidate = self._extract_fastest_lap_candidate(driver_code, lap_data)
            if candidate is None:
                continue

            if best_candidate is None or candidate[2] < best_candidate[2]:
                best_candidate = candidate

        if cacheable_payloads and self._should_backfill_ultra_cold_cache(ultra_cold):
            self._schedule_background_cache_fill(json_payloads=cacheable_payloads)

        if best_candidate is None:
            return None
        return (best_candidate[0], best_candidate[1])

    def _get_fastest_lap_reference(self, *, ultra_cold: bool = False) -> tuple[str, int] | None:
        """Get overall fastest lap metadata with source-aware memoization."""
        if self._laps is not None:
            laps_source_id = id(self._laps)
            cached_ref = self._memo.get_fastest_lap_ref_if_current("laps", laps_source_id)
            if cached_ref is not FASTEST_LAP_REF_MISS:
                return cached_ref

            fastest_lap_ref = self._extract_fastest_lap_from_loaded_laps()
            self._memo.set_fastest_lap_ref(
                fastest_lap_ref, source_kind="laps", source_id=laps_source_id
            )
            return fastest_lap_ref

        drivers, driver_payloads = self._load_drivers_for_fastest_lap_reference(
            ultra_cold=ultra_cold
        )
        if driver_payloads and self._should_backfill_ultra_cold_cache(ultra_cold):
            self._schedule_background_cache_fill(json_payloads=driver_payloads)

        driver_source_id = id(drivers)
        cached_ref = self._memo.get_fastest_lap_ref_if_current("drivers", driver_source_id)
        if cached_ref is not FASTEST_LAP_REF_MISS:
            return cached_ref

        fastest_lap_ref = self._find_fastest_lap_reference_from_raw(drivers, ultra_cold=ultra_cold)
        self._memo.set_fastest_lap_ref(
            fastest_lap_ref, source_kind="drivers", source_id=driver_source_id
        )
        return fastest_lap_ref

    async def _get_fastest_lap_reference_async(
        self, *, ultra_cold: bool = False
    ) -> tuple[str, int] | None:
        """Async variant of overall fastest-lap metadata resolution."""
        if self._laps is not None:
            laps_source_id = id(self._laps)
            cached_ref = self._memo.get_fastest_lap_ref_if_current("laps", laps_source_id)
            if cached_ref is not FASTEST_LAP_REF_MISS:
                return cached_ref

            fastest_lap_ref = self._extract_fastest_lap_from_loaded_laps()
            self._memo.set_fastest_lap_ref(
                fastest_lap_ref, source_kind="laps", source_id=laps_source_id
            )
            return fastest_lap_ref

        drivers, driver_payloads = self._load_drivers_for_fastest_lap_reference(
            ultra_cold=ultra_cold
        )
        if driver_payloads and self._should_backfill_ultra_cold_cache(ultra_cold):
            self._schedule_background_cache_fill(json_payloads=driver_payloads)

        driver_source_id = id(drivers)
        cached_ref = self._memo.get_fastest_lap_ref_if_current("drivers", driver_source_id)
        if cached_ref is not FASTEST_LAP_REF_MISS:
            return cached_ref

        fastest_lap_ref = await self._find_fastest_lap_reference_from_raw_async(
            drivers,
            ultra_cold=ultra_cold,
        )
        self._memo.set_fastest_lap_ref(
            fastest_lap_ref, source_kind="drivers", source_id=driver_source_id
        )
        return fastest_lap_ref

    @property
    def laps(self) -> DataFrame:
        """Get all laps data for the session (auto-async for 4-5x faster loading).

        This property automatically loads lap data for all drivers in parallel using
        async requests. Data is cached globally to avoid redundant loading.

        Performance:
            - 4-5x faster than sequential loading (cold cache)
            - Global in-memory cache for instant access on subsequent calls
            - SQLite cache for persistence across sessions

        Note:
            This property uses asyncio internally and may block. If called from an
            async context, consider using laps_async() directly for better control.

        Returns:
            DataFrame with all laps from all drivers. Columns include:
            - LapNumber, LapTime, Driver, Team
            - Sector1Time, Sector2Time, Sector3Time
            - Compound, TyreLife, Stint
            - Position, TrackStatus, IsPersonalBest
            Returns empty DataFrame if no laps found.

        Example:
            >>> session = get_session(2025, "Silverstone Grand Prix", "Race")
            >>> laps = session.laps  # Loads all laps asynchronously
            >>> laps[["Driver", "LapNumber", "LapTime"]].head()
            Driver  LapNumber LapTime
            VER         1   78.123
            HAM         1   79.456
            ...
            >>> # Filter for specific driver
            >>> ver_laps = laps[laps["Driver"] == "VER"]
        """

        if self._laps is None and self._ergast_only:
            self._laps = self._as_session_laps(self._ergast_laps())
            return self._laps
        if self._laps is None:
            cache_key = f"{self.year}_{self.gp}_{self.session}_laps"
            lap_cache = get_backend_lap_cache(self.lib) if self.enable_cache else None
            if lap_cache is not None:
                cached_laps = lap_cache.get(cache_key)
                if cached_laps is not None:
                    logger.info(f"Lap cache hit ({self.lib}): {cache_key}")
                    self._laps = self._bind_cached_laps(cached_laps)
                    return self._laps

            # Pergamon P5: a laps frame-tier hit needs no event loop (and no
            # asyncio import) — only loads that must fetch go async.
            tier_frame = self._laps_from_frame_tier()
            if tier_frame is not None:
                self._laps = tier_frame
            else:
                logger.info(f"Loading laps async ({self.lib}): {cache_key}")
                self._laps = self._as_session_laps(
                    _run_coroutine_sync(
                        self.laps_async, "laps property", alternative="session.laps_async()"
                    )
                )
            if lap_cache is not None:
                lap_cache.set(cache_key, self._unbound_laps(self._laps))
            self._maybe_start_background_telemetry_prefetch()

        return self._laps

    @staticmethod
    def _unbound_laps(laps: DataFrame) -> DataFrame:
        """Session-free view of ``laps`` for the process-global lap cache (Q9).

        A cached :class:`Laps` bound to its session pinned that session (and
        its whole telemetry memo, ~100-150 MB per race) for up to
        ``MAX_CACHE_SIZE`` sessions after the caller dropped it. A shallow
        copy (copy-on-write: no data copied) without the back-reference keeps
        the laps reusable and lets dropped sessions be freed.
        """
        if isinstance(laps, Laps) and laps.session is not None:
            unbound = laps.copy(deep=False)
            unbound.session = None
            return unbound
        if getattr(laps, "session", None) is not None and hasattr(laps, "base_class_view"):
            return cast(Any, laps).base_class_view  # PolarsLaps: a plain polars frame
        return laps

    def _bind_cached_laps(self, cached: DataFrame) -> DataFrame:
        """Bind an unbound global-cache :class:`Laps` to this session (shallow copy)."""
        if isinstance(cached, Laps) and cached.session is None:
            bound = cached.copy(deep=False)
            bound.session = self
            return bound
        if self.lib == "polars":
            return self._as_session_laps(cached)
        return cached

    def _as_session_laps(self, frame: DataFrame) -> DataFrame:
        """Laps bound to this session: :class:`Laps` (pandas) or ``PolarsLaps`` (polars)."""
        if self.lib == "pandas" and isinstance(frame, pd.DataFrame):
            if not isinstance(frame, Laps):
                frame = Laps(frame)
            frame.session = self
        elif self.lib == "polars" and frame is not None and hasattr(frame, "height"):
            polars_laps = _polars_models().PolarsLaps
            if not (isinstance(frame, polars_laps) and frame.session is self):
                frame = polars_laps._from_frame(frame, self)
        return frame

    def _driver_laps_frame(
        self, driver: str, prefetched_lap_data: dict[str, Any] | None = None
    ) -> DataFrame:
        """One driver's session-bound laps without loading every driver (polars ``Driver.laps``).

        The same sources, in the same order, as the pandas :attr:`Driver.laps`:
        nothing for a driver only the classification lists, the loaded
        session table, else the driver's own laptime payload (prefetched by
        :meth:`get_driver`, memoised, cached, fetched). Never raises for a
        missing or broken payload: the laps are empty, with their columns.
        """
        if not self._driver_has_timing_data(driver):
            return self._as_session_laps(_empty_driver_laps(self))
        if self._ergast_only and self._laps is None:
            _ = self.laps
        if self._laps is not None and not _is_empty_df(self._laps, self.lib):
            driver_laps = cast(Any, self._as_session_laps(self._laps)).pick_driver(driver)
            if not _is_empty_df(driver_laps, self.lib):
                return driver_laps
        if self._ergast_only:
            return self._as_session_laps(_empty_driver_laps(self))
        try:
            lap_data = _load_driver_lap_payload(self, driver, prefetched_lap_data)
            team = self._get_driver_info(driver)["team"]
            frame = _process_lap_df(_create_lap_df(lap_data, driver, team, self.lib), self.lib)
        except DataNotFoundError:
            logger.info(f"No lap data: {driver}")
            return self._as_session_laps(_empty_driver_laps(self))
        except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
            logger.warning(f"Failed to load laps for {driver}: {e}")
            return self._as_session_laps(_empty_driver_laps(self))
        return self._as_session_laps(frame)

    def _laps_from_frame_tier(self, ultra_cold_enabled: bool | None = None) -> DataFrame | None:
        """Session-bound laps from the laps frame tier, or None on a miss.

        Laps frame tier (KR-1/KR-10): the processed frame materializes once
        per backend; warm loads unpickle it instead of re-running the
        payload read + merge + dtype-processing pipeline (~66 ms pandas /
        ~29 ms polars on the Monaco corpus).
        """
        if ultra_cold_enabled is None:
            ultra_cold_enabled = self._resolve_ultra_cold_mode(None)
        if not self.enable_cache or ultra_cold_enabled:
            return None
        cached_frame = get_cache().get_laps_frame(self.year, self.gp, self.session, lib=self.lib)
        if cached_frame is None:
            return None
        logger.info("Laps frame tier hit: %s/%s/%s", self.year, self.gp, self.session)
        return self._as_session_laps(cached_frame)

    async def laps_async(self) -> DataFrame:
        """Get all laps data asynchronously (faster for multiple drivers).

        Async version of the laps property. Use this when calling from async context
        to avoid blocking and nested event loop issues.

        Returns:
            DataFrame with all laps from all drivers

        Example:
            >>> import asyncio
            >>> async def load_data():
            ...     session = get_session(2025, "Spa Grand Prix", "Race")
            ...     laps = await session.laps_async()
            ...     return laps
            >>> laps = asyncio.run(load_data())
        """
        if self._laps is not None:
            return self._laps
        if self._ergast_only:
            self._laps = self._as_session_laps(self._ergast_laps())
            return self._laps

        ultra_cold_enabled = self._resolve_ultra_cold_mode(None)
        tier_frame = self._laps_from_frame_tier(ultra_cold_enabled)
        if tier_frame is not None:
            self._laps = tier_frame
            return self._laps

        # Thebes TH6: every session table the laps build needs (drivers,
        # session laptimes, and weather/rcm when requested) is fetched in ONE
        # concurrent wave through the async pipeline. The laptimes file does
        # not depend on the driver list, so the cold laps phase costs one CDN
        # round trip instead of two (drivers first, laptimes after).
        await self._prefetch_laps_wave_async(ultra_cold_enabled)

        drivers = self._drivers_data
        if not drivers:
            logger.info(f"No drivers, returning empty: {self.year}/{self.event_name}")
            return _create_empty_df(self.lib)

        driver_requests = self._build_driver_laptime_requests(driver_pool=drivers)
        if not driver_requests:
            logger.info(f"No valid driver requests, returning empty: {self.year}/{self.event_name}")
            return _create_empty_df(self.lib)

        cdn_failures_before = dict(get_cdn_manager()._failure_counts)
        payloads, ultra_cold_payloads = await self._fetch_laptime_payloads_async(
            driver_requests,
            operation="laps_async",
            ultra_cold=ultra_cold_enabled,
        )

        if self.lib == "polars":
            # Ithome U6: build ONE polars frame from a merged dict-of-lists
            # instead of a per-driver frame + pl.concat (the pandas single-shot
            # win; concat of ~20 small frames repeats schema inference per
            # driver). Failed drivers are skipped exactly like before.
            merged_laps: list[tuple[dict, str, str]] = []
            for (driver_info, _path), lap_data in zip(driver_requests, payloads):
                if not isinstance(lap_data, dict) or not lap_data:
                    continue
                driver_code = driver_info.get("driver", "")
                try:
                    normalized_pl = _normalize_lap_payload_polars(lap_data)
                except (KeyError, TypeError, ValueError, InvalidDataError) as e:
                    logger.warning(f"Failed to process lap data for {driver_code}: {e}")
                    continue
                if not normalized_pl:
                    continue
                merged_laps.append((normalized_pl, driver_code, driver_info.get("team", "")))

            if not merged_laps:
                self._raise_if_cdns_failed(cdn_failures_before)
                logger.info(f"No valid lap data: {self.year}/{self.event_name}")
                return _create_empty_df(self.lib)

            self._laps = pl.DataFrame(_merge_lap_payloads(merged_laps), strict=False)
        else:
            # Build one DataFrame from a single merged dict-of-lists instead of
            # pd.concat of per-driver frames: concat regressed ~2x in pandas 3.0
            # for many small frames, while the merged-dict path is much faster
            # and produces identical rows (None-padding replaces concat's NaN
            # fill for columns absent from some drivers; _process_lap_df below
            # coerces canonical dtypes either way).
            merged_payloads: list[tuple[dict, str, str]] = []
            for (driver_info, _path), lap_data in zip(driver_requests, payloads):
                if not isinstance(lap_data, dict) or not lap_data:
                    continue
                driver_code = driver_info.get("driver", "")
                try:
                    normalized = _normalize_lap_payload(lap_data)
                except (KeyError, TypeError, ValueError, InvalidDataError) as e:
                    logger.warning(f"Failed to process lap data for {driver_code}: {e}")
                    continue
                merged_payloads.append((normalized, driver_code, driver_info.get("team", "")))

            if not merged_payloads:
                self._raise_if_cdns_failed(cdn_failures_before)
                logger.info(f"No valid lap data: {self.year}/{self.event_name}")
                return _create_empty_df(self.lib)
            self._laps = pd.DataFrame(_merge_lap_payloads(merged_payloads), copy=False)
            # Remove duplicate columns if they exist (can happen if upstream data has Driver/Team)
            if isinstance(self._laps.columns, pd.Index) and self._laps.columns.duplicated().any():
                # Keep only the first occurrence of each column name
                self._laps = self._laps.loc[:, ~self._laps.columns.duplicated()]

        self._laps = _process_lap_df(self._laps, self.lib)

        if self.enable_cache and not _is_empty_df(self._laps, self.lib):
            if self.lib == "pandas" and isinstance(self._laps, pd.DataFrame):
                get_cache().set_laps_frame(self.year, self.gp, self.session, self._laps)
            elif self.lib == "polars" and hasattr(self._laps, "height"):
                get_cache().set_laps_frame(
                    self.year, self.gp, self.session, self._laps, lib="polars"
                )
        # Wrapped only after the frame-tier write: a pickled Laps would carry its session.
        self._laps = self._as_session_laps(self._laps)

        if ultra_cold_payloads and self._should_backfill_ultra_cold_cache(ultra_cold_enabled):
            self._schedule_background_cache_fill(json_payloads=ultra_cold_payloads)
        self._maybe_start_background_telemetry_prefetch()

        return self._laps

    async def fetch_driver_laps_parallel(self, drivers: list[str]) -> dict[str, DataFrame]:
        """Fetch laps for multiple drivers in parallel.

        This method fetches lap data for specified drivers concurrently using
        asyncio.gather(), providing optimal performance when you need data for
        a subset of drivers rather than all drivers in the session.

        Args:
            drivers: List of driver codes (e.g., ["VER", "HAM", "LEC"])

        Returns:
            Dictionary mapping driver codes to their lap DataFrames.
            Drivers with no data or errors will have empty DataFrames.

        Example:
            >>> import asyncio
            >>> async def get_top_drivers():
            ...     session = get_session(2025, "Monaco Grand Prix", "Race")
            ...     laps = await session.fetch_driver_laps_parallel(["VER", "HAM", "LEC"])
            ...     return laps
            >>> laps_dict = asyncio.run(get_top_drivers())
            >>> laps_dict["VER"]  # DataFrame with VER's laps
        """
        if not drivers:
            return {}

        ultra_cold_enabled = self._resolve_ultra_cold_mode(None)
        all_drivers = self._drivers_data
        if not all_drivers:
            logger.info(f"No drivers found: {self.year}/{self.event_name}")
            return {driver: _create_empty_df(self.lib) for driver in drivers}

        # Build driver info map
        driver_info_map = _extract_driver_info_map(all_drivers)

        # Filter to requested drivers that exist
        valid_drivers = [d for d in drivers if d in driver_info_map]
        if not valid_drivers:
            logger.warning(f"None of the requested drivers found: {drivers}")
            return {driver: _create_empty_df(self.lib) for driver in drivers}

        # Build requests for valid drivers
        driver_requests = []
        for driver_code in valid_drivers:
            driver_info = driver_info_map[driver_code]
            path = f"{driver_code}/laptimes.json"
            driver_requests.append((driver_info, path))

        # Fetch all driver lap data in parallel
        payloads, ultra_cold_payloads = await self._fetch_laptime_payloads_async(
            driver_requests,
            operation="fetch_driver_laps_parallel",
            ultra_cold=ultra_cold_enabled,
        )

        # Process results into DataFrames
        result = {}
        for driver_code in drivers:
            if driver_code not in driver_info_map:
                result[driver_code] = _create_empty_df(self.lib)
                continue

            # Find the payload for this driver
            driver_idx = valid_drivers.index(driver_code)
            lap_data = payloads[driver_idx]

            if not isinstance(lap_data, dict) or not lap_data:
                result[driver_code] = _create_empty_df(self.lib)
                continue

            try:
                driver_info = driver_info_map[driver_code]
                lap_df = _create_lap_df(
                    lap_data,
                    driver_code,
                    driver_info.get("team", ""),
                    self.lib,
                )
                lap_df = _process_lap_df(lap_df, self.lib)
                result[driver_code] = lap_df
            except (KeyError, TypeError, ValueError, InvalidDataError) as e:
                logger.warning(f"Failed to process lap data for {driver_code}: {e}")
                result[driver_code] = _create_empty_df(self.lib)

        # Handle ultra cold cache backfill
        if ultra_cold_payloads and self._should_backfill_ultra_cold_cache(ultra_cold_enabled):
            self._schedule_background_cache_fill(json_payloads=ultra_cold_payloads)

        return result

    def _coerce_driver_code(self, driver: Any) -> str:
        """Resolve FastF1-style driver identifiers to a driver code."""
        if isinstance(driver, dict):
            for key in ("driver", "Driver", "Abbreviation", "abbreviation", "code", "Code"):
                value = driver.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            for key in ("number", "RacingNumber"):
                value = driver.get(key)
                if value is not None:
                    driver = value

        if isinstance(driver, (str, int)):
            value = str(driver).strip()
            _validate_string_param(value, "driver")
            number, separator, code = value.partition("/")
            if separator and number.isdigit() and code.strip():
                # "number/Abbreviation": the label of a shared car's later
                # driver (see ``drivers``); the code names the driver.
                return code.strip()
            # If it looks like a driver number, resolve to driver code first
            if value.isdigit():
                for info in self._driver_rows():
                    number = info.get("dn")
                    code = info.get("driver")
                    if str(number) == value and isinstance(code, str) and code.strip():
                        return code.strip()
            elif " " in value:
                # A full name ("Max Verstappen"), matched without regard to case.
                wanted = " ".join(value.split()).casefold()
                for info in self._driver_rows():
                    full_name = f"{info.get('fn', '')} {info.get('ln', '')}".strip()
                    code = info.get("driver")
                    if full_name.casefold() == wanted and isinstance(code, str) and code.strip():
                        return code.strip()
            return value

        if hasattr(driver, "driver") and isinstance(driver.driver, str):
            value = str(driver.driver).strip()
            _validate_string_param(value, "driver")
            return value

        if hasattr(driver, "Abbreviation") and isinstance(driver.Abbreviation, str):
            value = str(driver.Abbreviation).strip()
            _validate_string_param(value, "driver")
            return value

        raise TypeError(
            f"driver must be a string or FastF1 driver-like object, got {type(driver).__name__}"
        )

    def get_driver(self, identifier: Any = None, *, driver: Any = None) -> "Driver":
        """
        Get a driver of the session: the driver's ``results`` row plus tif1's helpers.

        Args:
            identifier: Abbreviation (``"VER"``), car number (``1`` or ``"1"``),
                a shared car's ``"number/Abbreviation"`` label or the full name
                (fastf1's keyword).
            driver: tif1's earlier name of ``identifier`` (an alias).

        Returns:
            :class:`Driver` (a :class:`DriverResult`), or ``PolarsDriver`` on a
            polars session.

        Raises:
            TypeError: If the identifier is not a string, a number or driver-like
            ValueError: If the identifier is empty
            DriverNotFoundError: If driver not found in session
        """
        if identifier is not None and driver is not None:
            raise ValueError("Cannot pass both 'identifier' and 'driver'")
        if identifier is None:
            identifier = driver
        if identifier is None:
            raise TypeError("get_driver() missing 1 required argument: 'identifier'")
        driver_code = self._coerce_driver_code(identifier)
        car_number = _identifier_car_number(identifier)
        prefetched_laps = self._prefetch_driver_lookup_and_laps(driver_code)
        # Enforce API contract by validating against the session driver list.
        # This may trigger one network fetch when drivers are not loaded yet.
        if not self._has_driver_code(driver_code):
            raise DriverNotFoundError(
                driver=driver_code,
                year=self.year,
                event=self.event_name,
                session=self.session_name,
            )

        if self.lib == "polars" and _ensure_polars_available():
            return self._polars_driver(driver_code, prefetched_laps, car_number)
        return Driver(
            self, driver_code, prefetched_lap_data=prefetched_laps, _car_number=car_number
        )

    def _polars_driver(
        self,
        driver: str,
        prefetched_lap_data: dict[str, Any] | None,
        car_number: str | None = None,
    ) -> Any:
        """``PolarsDriver``: the driver's results row plus the identity, as on pandas.

        The same rules as :class:`Driver`: the ``results`` row, identity
        values from ``drivers.json`` and Ergast filling whatever the row leaves
        empty, fastf1's columns with empty values when there is no row.
        """
        info = self._get_driver_info(driver)
        first_name = info.get("fn", "")
        last_name = info.get("ln", "")
        number = str(car_number or info.get("dn", ""))
        identity: dict[str, Any] = {
            "DriverNumber": number,
            "Abbreviation": driver,
            "TeamName": info.get("team", ""),
            "TeamColor": info.get("tc", ""),
            "FirstName": first_name,
            "LastName": last_name,
            "FullName": f"{first_name} {last_name}".strip(),
            "HeadshotUrl": info.get("headshot_url", info.get("url", "")),
        }
        identity.update(self._ergast_identity(number, driver))
        row: dict[str, Any] = {}
        label: str | int | None = None
        try:
            results: Any = self.results
        except (TIF1Error, AttributeError, KeyError, TypeError, ValueError):
            results = None
        if results is not None and getattr(results, "height", 0):
            position = _polars_result_position(results, driver, number)
            if position is not None:
                row = results.row(position, named=True)
                # The pandas index label ("1", "8/FAG"), as fastf1 names a row.
                label_for = getattr(results, "_label_for", None)
                label = label_for(position) if callable(label_for) else None
                if label is None:
                    label = position
        for key, value in identity.items():
            current = row.get(key)
            if current is None or (isinstance(current, str) and not current):
                row[key] = value
        frame = _er.finalize_results_frame_polars([row])
        return _polars_models().PolarsDriver(
            frame,
            session=self,
            name=label,
            driver=driver,
            prefetched_lap_data=prefetched_lap_data,
        )

    def _raise_if_cdns_failed(self, failures_before: dict[str, int]) -> None:
        """Raise ``NetworkError`` when a load produced no laps because the CDNs failed.

        404s never count against a source, so a failure count that grew during
        the load means a real transport error, not a session without laps. An
        empty table in that case would silently hide the outage.
        """
        failures_after = get_cdn_manager()._failure_counts
        if any(failures_after.get(name, 0) > count for name, count in failures_before.items()):
            raise NetworkError(
                url=f"{self.year}/{self.gp}/{self.session}/{SESSION_LAPTIMES_PATH}",
                status_code=None,
            )

    def _lap_time_sort_column(self, laps) -> str:
        """Resolve the numeric lap-time sort column for fastest-lap operations."""
        if COL_LAP_TIME_SECONDS in laps.columns:
            return COL_LAP_TIME_SECONDS
        return COL_LAP_TIME

    def _select_fastest_laps(
        self, laps, *, by_driver: bool, drivers: list[str] | None = None
    ) -> DataFrame:
        """Filter, validate, and select fastest laps for both sync and async code paths."""
        if _is_empty_df(laps, self.lib):
            return _create_empty_df(self.lib)

        # Remove duplicate columns if they exist (pandas only) - safety check
        if self.lib == "pandas" and isinstance(laps.columns, pd.Index):
            if laps.columns.duplicated().any():
                laps = laps.loc[:, ~laps.columns.duplicated()]

        if self.lib == "polars":
            picked = _fastest_laps_polars(laps, by_driver=by_driver, drivers=drivers)
            if picked is not None:
                return picked if picked.height else _create_empty_df(self.lib)

        # Filter by drivers first to reduce data processing.
        if drivers:
            if self.lib == "polars":
                laps_pl = cast(Any, laps)
                laps = laps_pl.filter(pl.col(COL_DRIVER).is_in(drivers))
            else:
                laps_pd = cast(pd.DataFrame, laps)
                driver_list_str = ", ".join(f"'{d}'" for d in drivers)
                laps = laps_pd.query(f"{COL_DRIVER} in [{driver_list_str}]", engine="python")
            if _is_empty_df(laps, self.lib):
                return _create_empty_df(self.lib)

        valid = _filter_valid_laptimes(laps, self.lib, exclude_deleted=True)
        if _is_empty_df(valid, self.lib):
            return _create_empty_df(self.lib)

        sort_col = self._lap_time_sort_column(valid)
        if self.lib == "polars":
            # Ordered group and stable sort: ties (equal lap times) keep the
            # laps' driver order, as the pandas stable sort does.
            return (
                valid.group_by(COL_DRIVER, maintain_order=True)
                .agg(pl.all().sort_by(sort_col, maintain_order=True).first())
                .sort(sort_col, maintain_order=True)
                .select(valid.columns)
                if by_driver
                else valid.sort(sort_col, maintain_order=True).head(1)
            )

        valid_pd = cast(pd.DataFrame, valid)
        if by_driver:
            # sort + drop_duplicates is measurably faster than
            # groupby(...).idxmin() on both pandas 2.x and 3.x and selects the
            # identical rows (first occurrence per driver after ascending sort;
            # kind="stable" preserves idxmin's tie semantics).
            result = valid_pd.sort_values(sort_col, kind="stable").drop_duplicates(
                subset=COL_DRIVER, keep="first"
            )
            return result.reset_index(drop=True)
        return valid_pd.nsmallest(1, sort_col).reset_index(drop=True)

    def get_fastest_laps(
        self, by_driver: bool = True, drivers: list[str] | None = None
    ) -> DataFrame:
        """
        Get fastest laps sorted by LapTime.

        Cold start behavior is optimized to avoid materializing full session laps when
        possible. If full laps are already loaded, this method uses the in-memory table.

        Args:
            by_driver: If True, return fastest lap per driver. If False, return overall fastest.
            drivers: Optional list of driver codes to filter (e.g., ["VER", "HAM"])

        Returns:
            DataFrame with fastest lap(s) sorted by LapTime with reset index.
            Returns empty DataFrame if no valid laps found.

        Raises:
            TypeError: If drivers is not a list
            ValueError: If drivers list is empty or contains invalid values

        Example:
            >>> session = get_session(2025, "Monaco Grand Prix", "Race")
            >>> # Get fastest lap per driver
            >>> fastest = session.get_fastest_laps(by_driver=True)
            >>> # Get overall fastest lap
            >>> overall = session.get_fastest_laps(by_driver=False)
            >>> # Get fastest laps for specific drivers
            >>> top3 = session.get_fastest_laps(by_driver=True, drivers=["VER", "HAM", "LEC"])
        """
        _validate_drivers_list(drivers)

        if self._laps is None:
            # Cold-start fast path: scan raw laptime payloads directly.
            ultra_cold_enabled = self._resolve_ultra_cold_mode(None)
            fastest_from_raw = self._get_fastest_laps_from_raw(
                by_driver=by_driver,
                drivers=drivers,
                ultra_cold=ultra_cold_enabled,
            )
            if not _is_empty_df(fastest_from_raw, self.lib):
                return self._fastest_laps_result(fastest_from_raw)

        laps = self.laps
        return self._fastest_laps_result(
            self._select_fastest_laps(laps, by_driver=by_driver, drivers=drivers)
        )

    async def get_fastest_laps_async(
        self, by_driver: bool = True, drivers: list[str] | None = None
    ) -> DataFrame:
        """Get fastest laps asynchronously.

        Args:
            by_driver: If True, return fastest lap per driver. If False, return overall fastest.
            drivers: Optional list of driver codes to filter (e.g., ["VER", "HAM"]).

        Returns:
            DataFrame with fastest lap(s) sorted by lap time.
        """
        _validate_drivers_list(drivers)

        if self._laps is None:
            # Cold-start fast path: scan raw laptime payloads directly.
            ultra_cold_enabled = self._resolve_ultra_cold_mode(None)
            fastest_from_raw = await self._get_fastest_laps_from_raw_async(
                by_driver=by_driver,
                drivers=drivers,
                ultra_cold=ultra_cold_enabled,
            )
            if not _is_empty_df(fastest_from_raw, self.lib):
                return self._fastest_laps_result(fastest_from_raw)

        laps = await self.laps_async()
        return self._fastest_laps_result(
            self._select_fastest_laps(laps, by_driver=by_driver, drivers=drivers)
        )

    def _fastest_laps_result(self, frame: DataFrame) -> DataFrame:
        """A ``get_fastest_laps`` result as session-bound laps (``Laps``/``PolarsLaps``).

        The cold path scans raw payloads into a plain frame; fastf1 code chains
        ``pick_*``/``get_telemetry`` on it, as on the warm path's ``Laps``.
        """
        return self._as_session_laps(frame)

    def _fetch_telemetry_batch_from_refs(
        self,
        fastest_refs: list[tuple[str, int]],
        *,
        skip_cache: bool = False,
    ):
        """Build telemetry fetch plan from fastest lap references (batch-optimized cache lookups)."""
        cache = None
        if not skip_cache and self.enable_cache and self._session_cache_available():
            cache = get_cache()
        requests, lap_info, tels = [], [], []

        # First check in-memory memoized payloads
        remaining_refs = []
        for driver, lap_num in fastest_refs:
            memoized_tel = self._get_telemetry_payload(driver, lap_num)
            if _telemetry_payload_has_lists(memoized_tel):
                tels.append((driver, lap_num, memoized_tel))
                continue
            remaining_refs.append((driver, lap_num))

        if not remaining_refs:
            return requests, lap_info, tels

        # Batch check SQLite cache
        cached_batch = {}
        if cache is not None:
            cached_batch = cache.get_telemetry_batch(
                self.year, self.gp, self.session, remaining_refs
            )

        for driver, lap_num in remaining_refs:
            cached_tel = cached_batch.get((driver, lap_num))
            if _telemetry_payload_has_lists(cached_tel):
                self._remember_telemetry_payload(driver, lap_num, cached_tel)
                tels.append((driver, lap_num, cached_tel))
                continue

            requests.append((self.year, self.gp, self.session, f"{driver}/{lap_num}_tel.json"))
            lap_info.append((driver, lap_num))

        return requests, lap_info, tels

    async def _fetch_telemetry_batch_from_refs_async(
        self,
        fastest_refs: list[tuple[str, int]],
        *,
        skip_cache: bool = False,
    ):
        """Build telemetry fetch plan from fastest lap references (async with concurrent cache ops).

        This async version uses asyncio.gather() to check cache for multiple telemetry
        payloads concurrently, significantly improving performance when checking cache
        for many drivers.

        Args:
            fastest_refs: List of (driver, lap_num) tuples
            skip_cache: If True, bypass cache checks

        Returns:
            Tuple of (requests, lap_info, tels) where:
            - requests: List of fetch requests for cache misses
            - lap_info: List of (driver, lap_num) for requests
            - tels: List of (driver, lap_num, tel_payload) tuples for cache hits
        """
        cache = None
        if not skip_cache and self.enable_cache and self._session_cache_available():
            cache = get_cache()

        requests, lap_info, tels = [], [], []

        # First check in-memory memoized payloads (synchronous, fast)
        cache_check_refs = []
        for driver, lap_num in fastest_refs:
            memoized_tel = self._get_telemetry_payload(driver, lap_num)
            if _telemetry_payload_has_lists(memoized_tel):
                tels.append((driver, lap_num, memoized_tel))
                continue
            cache_check_refs.append((driver, lap_num))

        if not cache_check_refs:
            return requests, lap_info, tels

        # Batch check SQLite cache concurrently
        cached_batch = {}
        if cache is not None:
            try:
                cached_batch = await cache.get_telemetry_batch_async(
                    self.year, self.gp, self.session, cache_check_refs
                )
            except (RuntimeError, TypeError, ValueError) as e:
                logger.warning(f"Telemetry batch cache read error: {e}")

        # Process cache results
        for driver, lap_num in cache_check_refs:
            telemetry_data = cached_batch.get((driver, lap_num))

            if _telemetry_payload_has_lists(telemetry_data):
                self._remember_telemetry_payload(driver, lap_num, telemetry_data)
                tels.append((driver, lap_num, telemetry_data))
                continue

            # Cache miss - add to fetch requests
            requests.append((self.year, self.gp, self.session, f"{driver}/{lap_num}_tel.json"))
            lap_info.append((driver, lap_num))

        return requests, lap_info, tels

    def _collect_lap_refs_from_loaded_laps(self) -> list[tuple[str, int]]:
        """Collect unique (driver, lap) refs from loaded laps."""
        if self._laps is None or _is_empty_df(self._laps, self.lib):
            return []

        refs: list[tuple[str, int]] = []
        seen: set[tuple[str, int]] = set()

        if self.lib == "polars":
            laps_pl = cast(Any, self._laps)
            if COL_DRIVER not in laps_pl.columns or COL_LAP_NUMBER not in laps_pl.columns:
                return []
            rows = laps_pl.select([COL_DRIVER, COL_LAP_NUMBER]).iter_rows()
        else:
            laps_pd = cast(pd.DataFrame, self._laps)
            if COL_DRIVER not in laps_pd.columns or COL_LAP_NUMBER not in laps_pd.columns:
                return []
            rows = zip(
                laps_pd[COL_DRIVER].to_numpy(copy=False),
                laps_pd[COL_LAP_NUMBER].to_numpy(copy=False),
            )

        for driver, lap_value in rows:
            if not isinstance(driver, str):
                continue
            try:
                ref = (driver, _coerce_lap_number(lap_value))
            except ValueError:
                continue
            if ref in seen:
                continue
            seen.add(ref)
            refs.append(ref)

        return refs

    def _memoize_prefetched_telemetry_payloads(
        self,
        lap_info: list[tuple[str, int]],
        results: list[dict[str, Any] | None],
        *,
        ultra_cold: bool,
    ) -> None:
        """Persist telemetry payloads fetched via bulk prefetch."""
        cache = get_cache() if self.enable_cache and not ultra_cold else None
        should_backfill = self._should_backfill_ultra_cold_cache(ultra_cold)
        telemetry_backfill_payloads: list[tuple[str, int, dict[str, Any]]] = []

        for (driver, lap_num), tel_data in zip(lap_info, results):
            if not isinstance(tel_data, dict):
                continue
            tel_payload = tel_data.get("tel")
            if not isinstance(tel_payload, dict) or not tel_payload:
                continue

            self._remember_telemetry_payload(driver, lap_num, tel_payload)
            if cache is not None:
                cache.set_telemetry(self.year, self.gp, self.session, driver, lap_num, tel_payload)
                self._mark_session_cache_populated()
            elif should_backfill:
                telemetry_backfill_payloads.append((driver, lap_num, tel_payload))

        if telemetry_backfill_payloads:
            self._schedule_background_cache_fill(telemetry_payloads=telemetry_backfill_payloads)

    def _prefetch_all_loaded_laps_telemetry(self, *, ultra_cold: bool) -> None:
        """Bulk-fetch telemetry for all loaded laps on first per-lap telemetry request."""
        if self._ergast_only:
            return
        if not bool(config.get("prefetch_all_telemetry_on_first_lap_request", True)):
            return
        if self._telemetry_bulk_prefetch_done:
            return
        if self._laps is None or _is_empty_df(self._laps, self.lib):
            return

        with self._telemetry_bulk_prefetch_lock:
            if self._telemetry_bulk_prefetch_done or self._telemetry_bulk_prefetch_attempted:
                return
            self._telemetry_bulk_prefetch_attempted = True

        refs = self._collect_lap_refs_from_loaded_laps()
        if not refs:
            self._telemetry_bulk_prefetch_done = True
            return

        requests, lap_info, _ = self._fetch_telemetry_batch_from_refs(refs, skip_cache=ultra_cold)
        if requests:
            max_retries = (
                1 if (ultra_cold and config.get("ultra_cold_skip_retries", True)) else None
            )
            max_concurrent = max(
                1,
                config.get("telemetry_prefetch_max_concurrent_requests", 128),
            )
            try:
                results = _run_coroutine_sync(
                    lambda: fetch_multiple_async(
                        requests,
                        use_cache=not ultra_cold,
                        write_cache=False,
                        validate_payload=not ultra_cold,
                        max_retries=max_retries,
                        max_concurrent_requests=max_concurrent,
                    ),
                    "bulk telemetry prefetch",
                    alternative="session.fetch_all_laps_telemetry_async()",
                )
                self._memoize_prefetched_telemetry_payloads(
                    lap_info,
                    results,
                    ultra_cold=ultra_cold,
                )
                self._precompute_telemetry_dfs()
            except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
                logger.debug("Bulk telemetry prefetch failed: %s", e)

        self._telemetry_bulk_prefetch_done = True

    def _precompute_telemetry_dfs(self) -> None:
        """Pre-create all telemetry DataFrames from memoized payloads in a single batch.

        pandas only: the batch is one pandas frame sliced per lap. A polars
        session builds each lap's frame from its memoised payload on access.
        """
        if self.lib != "pandas" or self._memo.kind_size("telemetry_payload") == 0:
            return

        merged_cols: dict[str, list] = {}
        driver_col: list[str] = []
        lap_col: list[int] = []
        key_ranges: list[tuple[tuple[str, int], int, int]] = []
        offset = 0

        for (driver, lap_num), tel_payload in self._memo.items("telemetry_payload"):
            if self._memo.contains("telemetry_df", (driver, lap_num)):
                continue
            if not tel_payload:
                continue

            list_lengths = [
                len(values) for values in tel_payload.values() if isinstance(values, list)
            ]
            n_rows = max(list_lengths) if list_lengths else 0
            if n_rows == 0:
                continue

            payload_list_keys: set[str] = set()
            for k, v in tel_payload.items():
                if isinstance(v, list):
                    mapped_key = TELEMETRY_RENAME_MAP.get(k, k)
                    payload_list_keys.add(mapped_key)
                    if mapped_key not in merged_cols:
                        merged_cols[mapped_key] = [None] * offset
                    if len(v) >= n_rows:
                        merged_cols[mapped_key].extend(v[:n_rows])
                    else:
                        merged_cols[mapped_key].extend(v)
                        merged_cols[mapped_key].extend([None] * (n_rows - len(v)))

            for col_name, col_values in merged_cols.items():
                if col_name not in payload_list_keys:
                    col_values.extend([None] * n_rows)

            driver_col.extend([driver] * n_rows)
            lap_col.extend([lap_num] * n_rows)
            key_ranges.append(((driver, lap_num), offset, offset + n_rows))
            offset += n_rows

        if not offset:
            return

        merged_cols["Driver"] = driver_col
        merged_cols["LapNumber"] = lap_col

        big_df = pd.DataFrame(merged_cols, copy=False)
        for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
            if col not in big_df.columns:
                big_df[col] = pd.NA

        for key, start, end in key_ranges:
            frame = big_df.iloc[start:end]
            telemetry = Telemetry(frame, copy=False)
            telemetry.session = self
            telemetry.driver = key[0]
            self._memo.set("telemetry_df", key, self._attach_session_clock(telemetry, key[0]))

    def _maybe_start_background_telemetry_prefetch(self) -> None:
        """Kick off all-laps telemetry prefetch after laps are available."""
        if self._telemetry_background_prefetch_started:
            return
        if not bool(config.get("prefetch_all_telemetry_on_first_lap_request", True)):
            return
        if not bool(config.get("prefetch_all_telemetry_after_laps_load", True)):
            return
        if self._laps is None or _is_empty_df(self._laps, self.lib):
            return
        if self._telemetry_bulk_prefetch_done or self._telemetry_bulk_prefetch_attempted:
            return
        if not THREADS_AVAILABLE:
            logger.debug("Background telemetry prefetch skipped: no threads in this runtime")
            return

        self._telemetry_background_prefetch_started = True
        ultra_cold_enabled = self._resolve_telemetry_ultra_cold_mode(None)
        threading.Thread(
            target=self._prefetch_all_loaded_laps_telemetry,
            kwargs={"ultra_cold": ultra_cold_enabled},
            name="tif1-telemetry-prefetch",
            daemon=True,
        ).start()

    def _fetch_telemetry_batch(self, fastest_laps, *, skip_cache: bool = False):
        """Helper to fetch telemetry for fastest laps."""
        fastest_refs: list[tuple[str, int]] = []

        if self.lib == "polars":
            fastest_laps_pl = cast(Any, fastest_laps)
            lap_col = _get_lap_column(fastest_laps_pl, self.lib)
            rows = fastest_laps_pl.select([COL_DRIVER, lap_col]).iter_rows()
        else:
            fastest_laps_pd = cast(pd.DataFrame, fastest_laps)
            lap_col = _get_lap_column(fastest_laps_pd, self.lib)
            rows = zip(
                fastest_laps_pd[COL_DRIVER].to_numpy(copy=False),
                fastest_laps_pd[lap_col].to_numpy(copy=False),
            )

        for driver, lap_value in rows:
            try:
                lap_num = _coerce_lap_number(lap_value)
            except ValueError as e:
                logger.warning(f"Invalid lap number: {e}")
                continue
            fastest_refs.append((driver, lap_num))

        return self._fetch_telemetry_batch_from_refs(fastest_refs, skip_cache=skip_cache)

    async def _fetch_telemetry_batch_async(self, fastest_laps, *, skip_cache: bool = False):
        """Helper to fetch telemetry for fastest laps (async with concurrent cache ops)."""
        fastest_refs: list[tuple[str, int]] = []

        if self.lib == "polars":
            fastest_laps_pl = cast(Any, fastest_laps)
            lap_col = _get_lap_column(fastest_laps_pl, self.lib)
            rows = fastest_laps_pl.select([COL_DRIVER, lap_col]).iter_rows()
        else:
            fastest_laps_pd = cast(pd.DataFrame, fastest_laps)
            lap_col = _get_lap_column(fastest_laps_pd, self.lib)
            rows = zip(
                fastest_laps_pd[COL_DRIVER].to_numpy(copy=False),
                fastest_laps_pd[lap_col].to_numpy(copy=False),
            )

        for driver, lap_value in rows:
            try:
                lap_num = _coerce_lap_number(lap_value)
            except ValueError as e:
                logger.warning(f"Invalid lap number: {e}")
                continue
            fastest_refs.append((driver, lap_num))

        return await self._fetch_telemetry_batch_from_refs_async(
            fastest_refs, skip_cache=skip_cache
        )

    def _extract_fastest_ref_from_fastest_laps(self, fastest_laps) -> tuple[str, int] | None:
        """Extract the overall fastest (driver, lap) from a fastest-laps DataFrame."""
        if _is_empty_df(fastest_laps, self.lib):
            return None

        if self.lib == "polars":
            fastest_laps_pl = cast(Any, fastest_laps)
            lap_col = _get_lap_column(fastest_laps_pl, self.lib)
            if COL_DRIVER not in fastest_laps_pl.columns or lap_col not in fastest_laps_pl.columns:
                return None
            driver, lap_value = fastest_laps_pl.select([COL_DRIVER, lap_col]).row(0)
        else:
            fastest_laps_pd = cast(pd.DataFrame, fastest_laps)
            lap_col = _get_lap_column(fastest_laps_pd, self.lib)
            if COL_DRIVER not in fastest_laps_pd.columns or lap_col not in fastest_laps_pd.columns:
                return None
            row = fastest_laps_pd.iloc[0]
            driver = row[COL_DRIVER]
            lap_value = row[lap_col]

        if not isinstance(driver, str):
            return None
        try:
            return (driver, _coerce_lap_number(lap_value))
        except ValueError:
            return None

    def _find_telemetry_df_for_ref(
        self,
        telemetry_entries: list[tuple[str, int, dict]],
        fastest_ref: tuple[str, int],
    ) -> DataFrame | None:
        """Find a telemetry frame for the given fastest-lap reference.

        ``telemetry_entries`` is a list of ``(driver, lap_num, tel_payload)``
        tuples (the raw payloads flowing through the batch telemetry pipeline);
        the matching entry's frame is built on demand.
        """
        driver, lap_num = fastest_ref
        for entry_driver, entry_lap, tel_payload in telemetry_entries:
            if entry_driver != driver:
                continue
            try:
                if _coerce_lap_number(entry_lap) == lap_num:
                    frame = _create_telemetry_df(
                        tel_payload,
                        driver,
                        lap_num,
                        self.lib,
                        clock=self._lap_clock(driver, lap_num),
                    )
                    return None if frame is None else self._session_telemetry(frame, driver)
            except ValueError:
                continue
        return None

    def _hydrate_fastest_lap_tel_from_batch(
        self,
        telemetry_entries: list[tuple[str, int, dict]],
        fastest_ref: tuple[str, int] | None,
    ) -> None:
        """Hydrate overall fastest-lap telemetry cache from already-fetched batch telemetry."""
        if fastest_ref is None:
            return

        fastest_tel_df = self._find_telemetry_df_for_ref(telemetry_entries, fastest_ref)
        if fastest_tel_df is None:
            return

        self._fastest_lap_tel_ref = fastest_ref
        self._fastest_lap_tel_df = fastest_tel_df

    def _process_telemetry_results(self, results, lap_info, tels, *, ultra_cold: bool = False):
        """Helper to process fetched telemetry results."""
        cache = get_cache() if self.enable_cache and not ultra_cold else None
        telemetry_backfill_payloads: list[tuple[str, int, dict[str, Any]]] = []
        should_backfill = self._should_backfill_ultra_cold_cache(ultra_cold)

        for (driver, lap_num), tel_data in zip(lap_info, results):
            if not isinstance(tel_data, dict):
                continue
            tel_payload = tel_data.get("tel")
            if not isinstance(tel_payload, dict) or not tel_payload:
                continue

            try:
                self._remember_telemetry_payload(driver, lap_num, tel_payload)
                if cache is not None:
                    cache.set_telemetry(
                        self.year, self.gp, self.session, driver, lap_num, tel_payload
                    )
                    self._mark_session_cache_populated()
                elif should_backfill:
                    telemetry_backfill_payloads.append((driver, lap_num, tel_payload))

                if _telemetry_payload_has_lists(tel_payload):
                    tels.append((driver, lap_num, tel_payload))
            except (InvalidDataError, NetworkError, TypeError, ValueError) as e:
                logger.warning(f"Failed to process telemetry for {driver} lap {lap_num}: {e}")
                continue

        if telemetry_backfill_payloads:
            self._schedule_background_cache_fill(telemetry_payloads=telemetry_backfill_payloads)

        return tels

    def _assemble_telemetry_batch(self, tels: list[tuple[str, int, dict]]) -> DataFrame:
        """Assemble raw telemetry payload tuples into a single DataFrame.

        For the pandas backend, builds one merged dict-of-lists and constructs a
        single DataFrame with a single dtype-conversion pass (much faster than
        per-driver frame construction + ``pd.concat``, which pandas 3.0
        regressed). The polars branch keeps the per-driver ``_create_telemetry_df``
        + ``pl.concat`` path byte-for-byte unchanged.

        If a malformed payload breaks the merged-dict construction, falls back
        to the defensive per-driver path (which isolates failures to the
        offending driver, matching the pre-merge behavior).
        """
        if self.lib == "polars":
            frames = []
            with _suspend_gc():
                for driver, lap_num, tel_payload in tels:
                    frame = _create_telemetry_df(tel_payload, driver, lap_num, self.lib)
                    if frame is not None:
                        frames.append(frame)
            if not frames:
                return _create_empty_df(self.lib)
            return self._bind_telemetry(
                pl.concat(frames, how="vertical_relaxed", rechunk=False), None
            )

        try:
            with _suspend_gc():
                merged_df = _telemetry_frame_from_merged(_merge_telemetry_payloads(tels))
        except (InvalidDataError, NetworkError, TypeError, ValueError):
            # Defensive fallback: per-driver construction isolates malformed
            # payloads instead of failing the whole batch.
            logger.warning("Merged telemetry assembly failed; falling back to per-driver frames")
            frames = []
            with _suspend_gc():
                for driver, lap_num, tel_payload in tels:
                    frame = _create_telemetry_df(tel_payload, driver, lap_num, "pandas")
                    if frame is not None:
                        frames.append(frame)
            if not frames:
                return _create_empty_df(self.lib)
            frames_pd = [cast(pd.DataFrame, frame) for frame in frames]
            return self._bind_telemetry(pd.concat(frames_pd, ignore_index=True), None)
        if merged_df.empty:
            return _create_empty_df(self.lib)
        return self._bind_telemetry(merged_df, None)

    def get_fastest_laps_tels(
        self, by_driver: bool = True, drivers: list[str] | None = None
    ) -> DataFrame:
        """
        Get telemetry from fastest laps (cached + async parallel fetch).

        This method first identifies fastest laps, checks cache for telemetry,
        then fetches missing data in parallel using async requests. This is
        significantly faster than fetching telemetry sequentially.

        Performance:
            - Uses SQLite cache to avoid redundant network requests
            - Parallel async fetching: ~28x faster than sequential (11.2s → 0.4s for 19 drivers)
            - Automatic retry logic with circuit breaker and CDN fallback

        Args:
            by_driver: If True, return telemetry per driver. If False, return overall fastest.
            drivers: Optional list of driver codes to filter (e.g., ["VER", "HAM"])

        Returns:
            DataFrame with telemetry from fastest lap(s). Columns include:
            - Time, Speed, Throttle, Brake, RPM, nGear, DRS
            - Distance, RelativeDistance
            - X, Y, Z (position coordinates)
            - AccelerationX, AccelerationY, AccelerationZ
            - Driver, LapNumber (metadata)
            Returns empty DataFrame if no telemetry found.

        Raises:
            TypeError: If drivers is not a list
            ValueError: If drivers list is empty or contains invalid values
            NetworkError: If network requests fail after retries
            InvalidDataError: If telemetry data is corrupted

        Example:
            >>> session = get_session(2025, "Las Vegas Grand Prix", "Race")
            >>> # Get telemetry for all drivers' fastest laps (parallel)
            >>> all_tels = session.get_fastest_laps_tels(by_driver=True)  # ~0.4s for 19 drivers
            >>> # Get telemetry for specific drivers only
            >>> top3_tels = session.get_fastest_laps_tels(by_driver=True, drivers=["VER", "HAM", "LEC"])  # ~0.13s
            >>> # Get overall fastest lap telemetry
            >>> fastest_tel = session.get_fastest_laps_tels(by_driver=False)  # ~0.08s
            >>> fastest_tel[["Time", "Speed", "Throttle"]].head()
            Time     Speed  Throttle
            0  0.123s   298.5      0.85
            1  0.124s   299.1      0.87
            ...
        """
        if self._ergast_only:  # no telemetry exists before F1_API_FIRST_YEAR
            return self._empty_session_telemetry()
        _validate_drivers_list(drivers)
        requested_ultra_cold = self._resolve_ultra_cold_mode(None)
        auto_ultra_cold = (
            not requested_ultra_cold
            and self.enable_cache
            and not self._session_cache_available()
            and self._is_fastest_lap_tel_cold_start()
        )
        ultra_cold_enabled = requested_ultra_cold or auto_ultra_cold

        # Reuse the single-lap optimized path for the common overall lookup.
        if not by_driver and drivers is None:
            return self.get_fastest_lap_tel(ultra_cold=ultra_cold_enabled)

        overall_fastest_ref: tuple[str, int] | None = None
        requests = []
        lap_info = []
        tels = []
        if by_driver and self._laps is None:
            fastest_refs = self._get_fastest_lap_refs_from_raw(
                drivers=drivers,
                ultra_cold=ultra_cold_enabled,
            )
            if fastest_refs:
                if drivers is None:
                    overall_fastest_ref = fastest_refs[0]
                requests, lap_info, tels = self._fetch_telemetry_batch_from_refs(
                    fastest_refs,
                    skip_cache=ultra_cold_enabled,
                )

        if not requests and not lap_info and not tels:
            # Fallback path for warm sessions or when raw fetch returns no rows.
            fastest_laps = self.get_fastest_laps(by_driver=by_driver, drivers=drivers)
            if _is_empty_df(fastest_laps, self.lib):
                return _create_empty_df(self.lib)
            if by_driver and drivers is None:
                overall_fastest_ref = self._extract_fastest_ref_from_fastest_laps(fastest_laps)

            requests, lap_info, tels = self._fetch_telemetry_batch(
                fastest_laps,
                skip_cache=ultra_cold_enabled,
            )

        if requests:
            try:
                if ultra_cold_enabled:
                    max_retries = 1 if config.get("ultra_cold_skip_retries", True) else None
                    try:
                        results = _run_coroutine_sync(
                            lambda: fetch_multiple_async(
                                requests,
                                use_cache=False,
                                write_cache=self.enable_cache,
                                validate_payload=False,
                                max_retries=max_retries,
                            ),
                            "get_fastest_laps_tels",
                        )
                    except TypeError as e:
                        if "unexpected keyword argument" not in str(e):
                            raise
                        results = _run_coroutine_sync(
                            lambda: fetch_multiple_async(requests), "get_fastest_laps_tels"
                        )
                else:
                    results = _run_coroutine_sync(
                        lambda: fetch_multiple_async(requests), "get_fastest_laps_tels"
                    )
                tels = self._process_telemetry_results(
                    results,
                    lap_info,
                    tels,
                    ultra_cold=ultra_cold_enabled,
                )
            except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
                logger.error(f"Failed to fetch telemetry: {e}")
                if not tels:
                    raise

        if not tels:
            return _create_empty_df(self.lib)

        if by_driver and drivers is None:
            self._hydrate_fastest_lap_tel_from_batch(tels, overall_fastest_ref)

        return self._assemble_telemetry_batch(tels)

    async def get_fastest_laps_tels_async(
        self, by_driver: bool = True, drivers: list[str] | None = None
    ) -> DataFrame:
        """Get telemetry from fastest laps asynchronously.

        Args:
            by_driver: If True, return telemetry per driver. If False, return overall fastest.
            drivers: Optional list of driver codes to filter (e.g., ["VER", "HAM"]).

        Returns:
            DataFrame with telemetry from fastest lap(s).
        """
        if self._ergast_only:  # no telemetry exists before F1_API_FIRST_YEAR
            return self._empty_session_telemetry()
        _validate_drivers_list(drivers)
        requested_ultra_cold = self._resolve_ultra_cold_mode(None)
        auto_ultra_cold = (
            not requested_ultra_cold
            and self.enable_cache
            and not self._session_cache_available()
            and self._is_fastest_lap_tel_cold_start()
        )
        ultra_cold_enabled = requested_ultra_cold or auto_ultra_cold

        # Reuse the single-lap optimized path for the common overall lookup.
        if not by_driver and drivers is None:
            return await self.get_fastest_lap_tel_async(ultra_cold=ultra_cold_enabled)

        overall_fastest_ref: tuple[str, int] | None = None
        requests = []
        lap_info = []
        tels = []
        if by_driver and self._laps is None:
            fastest_refs = await self._get_fastest_lap_refs_from_raw_async(
                drivers=drivers,
                ultra_cold=ultra_cold_enabled,
            )
            if fastest_refs:
                if drivers is None:
                    overall_fastest_ref = fastest_refs[0]
                requests, lap_info, tels = await self._fetch_telemetry_batch_from_refs_async(
                    fastest_refs,
                    skip_cache=ultra_cold_enabled,
                )

        if not requests and not lap_info and not tels:
            # Fallback path for warm sessions or when raw fetch returns no rows.
            fastest_laps = await self.get_fastest_laps_async(by_driver=by_driver, drivers=drivers)
            if _is_empty_df(fastest_laps, self.lib):
                return _create_empty_df(self.lib)
            if by_driver and drivers is None:
                overall_fastest_ref = self._extract_fastest_ref_from_fastest_laps(fastest_laps)

            requests, lap_info, tels = await self._fetch_telemetry_batch_async(
                fastest_laps,
                skip_cache=ultra_cold_enabled,
            )

        if requests:
            try:
                if ultra_cold_enabled:
                    max_retries = 1 if config.get("ultra_cold_skip_retries", True) else None
                    try:
                        results = await fetch_multiple_async(
                            requests,
                            use_cache=False,
                            write_cache=self.enable_cache,
                            validate_payload=False,
                            max_retries=max_retries,
                        )
                    except TypeError as e:
                        if "unexpected keyword argument" not in str(e):
                            raise
                        results = await fetch_multiple_async(requests)
                else:
                    results = await fetch_multiple_async(requests)
                tels = self._process_telemetry_results(
                    results,
                    lap_info,
                    tels,
                    ultra_cold=ultra_cold_enabled,
                )
            except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
                logger.error(f"Failed to fetch telemetry: {e}")
                if not tels:
                    raise

        if not tels:
            return _create_empty_df(self.lib)

        if by_driver and drivers is None:
            self._hydrate_fastest_lap_tel_from_batch(tels, overall_fastest_ref)

        return self._assemble_telemetry_batch(tels)

    def _get_telemetry_payload_for_ref(
        self, driver: str, lap_num: int, *, ultra_cold: bool, allow_prefetch: bool = True
    ) -> dict[str, Any] | None:
        """Fetch the raw telemetry payload for a (driver, lap) reference.

        Mirrors the source chain of :meth:`_get_telemetry_df_for_ref` (memoized
        payload -> bulk prefetch -> SQLite cache -> network) but returns the raw
        payload dict instead of a DataFrame, so callers can merge many payloads
        into a single DataFrame build (see ``_merge_telemetry_payloads``).
        Returns None when no telemetry is available and raises the same
        exceptions as the DataFrame path on network failure.
        """
        if self._ergast_only:  # no telemetry exists before F1_API_FIRST_YEAR
            return None
        memoized_tel = self._get_telemetry_payload(driver, lap_num)
        if memoized_tel is not None:
            return memoized_tel

        if allow_prefetch:
            self._prefetch_all_loaded_laps_telemetry(ultra_cold=ultra_cold)
            memoized_tel = self._get_telemetry_payload(driver, lap_num)
            if memoized_tel is not None:
                return memoized_tel

        cache = None
        if self.enable_cache and not ultra_cold and self._session_cache_available():
            cache = get_cache()
            cached_tel = cache.get_telemetry(self.year, self.gp, self.session, driver, lap_num)
            if isinstance(cached_tel, dict) and cached_tel:
                self._remember_telemetry_payload(driver, lap_num, cached_tel)
                return cached_tel

        if self._should_skip_telemetry_fetch(driver):
            return None

        try:
            if ultra_cold:
                tel_data = self._fetch_json_unvalidated(f"{driver}/{lap_num}_tel.json")
            else:
                tel_data = self._fetch_json(f"{driver}/{lap_num}_tel.json")
        except (DataNotFoundError, InvalidDataError, NetworkError, TypeError, ValueError) as e:
            logger.debug("Telemetry fetch failed for %s lap %s: %s", driver, lap_num, e)
            raise

        if not tel_data or not isinstance(tel_data, dict):
            return None

        tel_payload = tel_data.get("tel")
        if not isinstance(tel_payload, dict) or not tel_payload:
            return None

        self._remember_telemetry_payload(driver, lap_num, tel_payload)
        # The fetch itself persists to the telemetry tier (write-back is no
        # longer ultra-cold-gated), so no explicit cache write here.
        return tel_payload

    def _telemetry_frame_class(self) -> Any:
        """DataFrame subclass the frame tier builds for this backend (pandas: Telemetry)."""
        return Telemetry if self.lib == "pandas" else None

    def _adopt_telemetry_frame(self, frame: Any, driver: str) -> Any:
        """Attach this session (and the session clock) to a tier-built telemetry frame.

        pandas frames are adopted in place and returned; a polars frame comes
        back as a new ``PolarsTelemetry`` (polars never mutates), which the
        caller stores.
        """
        if isinstance(frame, Telemetry):
            object.__setattr__(frame, "session", self)
            object.__setattr__(frame, "driver", driver)
        return self._bind_telemetry(frame, driver)

    def _empty_session_telemetry(self, driver: str | None = None) -> Any:
        """No samples, fastf1's telemetry columns and dtypes, bound to this session."""
        if self.lib == "polars" and _ensure_polars_available():
            return _polars_models().PolarsTelemetry._empty(self, driver)
        return _empty_telemetry(self, driver)

    def _session_telemetry(self, frame: Any, driver: str) -> Any:
        """One lap's frame as this session's telemetry: ``Telemetry``/``PolarsTelemetry``.

        Session, driver and the session clock are attached; pandas frames are
        wrapped without copying their data.
        """
        if self.lib != "pandas":
            return self._bind_telemetry(frame, driver)
        # object.__setattr__: both names are Telemetry._metadata, and
        # pandas' __setattr__ costs ~15 us per assignment on this path.
        if not isinstance(frame, Telemetry):
            frame = Telemetry(cast(pd.DataFrame, frame), copy=False)
        object.__setattr__(frame, "session", self)
        object.__setattr__(frame, "driver", driver)
        return self._attach_session_clock(frame, driver)

    def _bind_telemetry(self, frame: Any, driver: str | None) -> Any:
        """A built telemetry frame with the session clock; ``PolarsTelemetry`` on polars."""
        frame = self._attach_session_clock(frame, driver)
        if self.lib != "polars" or frame is None or not hasattr(frame, "height"):
            return frame
        polars_telemetry = _polars_models().PolarsTelemetry
        if (
            isinstance(frame, polars_telemetry)
            and frame.session is self
            and (driver is None or frame.driver == driver)
        ):
            return frame
        return polars_telemetry._from_frame(frame, self, driver)

    def _lap_start_lookup(self, driver: str) -> tuple[np.ndarray, np.ndarray] | None:
        """``(LapStartTime, LapStartDate)`` as int64-ns arrays indexed by lap number.

        From the loaded laps table, else from the driver's memoised laptime
        payload (``lap``/``lST``/``lSD``); never fetches. Int64 min marks an
        unknown lap; ``None`` when nothing is known for the driver.
        """
        laps = self._laps
        source_id = id(laps) if laps is not None else None
        if source_id != self._lap_start_cache_source:
            self._lap_start_cache.clear()
            self._lap_start_cache_source = source_id
        if driver in self._lap_start_cache:
            return self._lap_start_cache[driver]
        numbers: Any = None
        starts: np.ndarray | None = None
        dates: np.ndarray | None = None
        if laps is not None and not _is_empty_df(laps, self.lib):
            columns = list(laps.columns)
            if {"Driver", "LapNumber", "LapStartTime", "LapStartDate"}.issubset(columns):
                if self.lib == "polars":
                    # Column ``to_numpy()`` and a numpy mask: a ``filter`` runs a
                    # query plan (~7 ms for the process's first one).
                    frame = cast(Any, laps)
                    mask = frame["Driver"].to_numpy() == driver
                    numbers = frame["LapNumber"].to_numpy()[mask].tolist()
                    starts = _polars_ns_values(frame["LapStartTime"], mask, timedelta=True)
                    dates = _polars_ns_values(frame["LapStartDate"], mask, timedelta=False)
                else:
                    laps_pd = cast(pd.DataFrame, laps)
                    mask = laps_pd["Driver"].to_numpy(dtype=object) == driver
                    numbers = laps_pd["LapNumber"].to_numpy()[mask].tolist()
                    starts = _pandas_ns_values(laps_pd["LapStartTime"], mask, timedelta=True)
                    dates = _pandas_ns_values(laps_pd["LapStartDate"], mask, timedelta=False)
        else:
            payload = self._get_or_derive_driver_laptime_payload(driver)
            if isinstance(payload, dict) and isinstance(payload.get("lap"), list):
                numbers = payload.get("lap")
                starts = _ss._timedelta_ns(payload.get("lST"))
                dates = _ss._datetime_ns(payload.get("lSD"))
        lookup: tuple[np.ndarray, np.ndarray] | None = None
        if numbers is not None and starts is not None and dates is not None:
            laps_known: list[int] = []
            for value in numbers:
                try:
                    laps_known.append(-1 if value is None or _is_nan(value) else int(value))
                except (TypeError, ValueError):
                    laps_known.append(-1)
            size = max(laps_known, default=-1) + 1
            if size > 0 and len(laps_known) == len(starts) == len(dates):
                start_arr = np.full(size, _ss._NS_MIN, dtype="int64")
                date_arr = np.full(size, _ss._NS_MIN, dtype="int64")
                for position, lap in enumerate(laps_known):
                    if lap >= 0 and start_arr[lap] == _ss._NS_MIN:
                        start_arr[lap] = starts[position]
                        date_arr[lap] = dates[position]
                lookup = (start_arr, date_arr)
        self._lap_start_cache[driver] = lookup
        return lookup

    def _lap_clock(self, driver: str, lap_num: Any) -> tuple[int, int] | None:
        """``(LapStartTime, LapStartDate)`` of one lap as int64 ns (None when unknown)."""
        lookup = self._lap_start_lookup(str(driver))
        if lookup is None:
            return None
        starts, dates = lookup
        try:
            lap = int(lap_num)
        except (TypeError, ValueError):
            return None
        if not 0 <= lap < len(starts) or starts[lap] == _ss._NS_MIN:
            return None
        return int(starts[lap]), int(dates[lap])

    def _attach_session_clock(self, frame: Any, driver: str | None = None) -> Any:
        """Add ``Date`` and ``SessionTime`` (lap start plus ``Time``) to a telemetry frame.

        The two columns lead the frame as in fastf1. pandas frames gain them
        in place (the same object is returned); polars frames come back as a
        new frame. No-op when the columns exist, the frame is empty or the
        lap starts are unknown (no laps loaded for the driver).
        """
        if frame is None:
            return frame
        columns = list(frame.columns)
        if "SessionTime" in columns or "Time" not in columns or "LapNumber" not in columns:
            return frame
        is_polars = not isinstance(frame, pd.DataFrame)
        n = frame.height if is_polars else len(frame)
        if n == 0:
            return frame
        if is_polars:
            drivers = (
                [driver]
                if driver
                else [d for d in frame["Driver"].unique().to_list() if d is not None]
                if "Driver" in columns
                else []
            )
            lap_numbers = frame["LapNumber"].fill_null(-1).cast(pl.Int64).to_numpy()
            time_col = frame["Time"]
            if time_col.dtype == pl.Duration:
                time_ns = time_col.cast(pl.Int64).fill_null(_ss._NS_MIN).to_numpy()
            else:
                time_ns = _ss._timedelta_ns(time_col.to_list())
            driver_col = frame["Driver"].to_list() if "Driver" in columns else None
        else:
            if driver:
                drivers = [driver]
            elif "Driver" in columns:
                drivers = list(frame["Driver"].dropna().unique().tolist())
            else:
                drivers = []
            lap_numbers = pd.to_numeric(frame["LapNumber"], errors="coerce").to_numpy(
                dtype="float64", na_value=-1.0
            )
            lap_numbers = lap_numbers.astype("int64")
            time_ns = _timedelta_ns(cast(pd.Series, frame["Time"]))
            driver_col = frame["Driver"].to_numpy() if "Driver" in columns else None
        if not drivers:
            return frame
        starts = np.full(n, _ss._NS_MIN, dtype="int64")
        dates = np.full(n, _ss._NS_MIN, dtype="int64")
        for code in drivers:
            lookup = self._lap_start_lookup(str(code))
            if lookup is None:
                continue
            start_arr, date_arr = lookup
            if len(drivers) == 1 or driver_col is None:
                rows = np.arange(n)
            else:
                rows = np.flatnonzero(np.asarray(driver_col, dtype=object) == code)
            laps_of_rows = lap_numbers[rows]
            valid = (laps_of_rows >= 0) & (laps_of_rows < len(start_arr))
            rows = rows[valid]
            laps_of_rows = laps_of_rows[valid]
            starts[rows] = start_arr[laps_of_rows]
            dates[rows] = date_arr[laps_of_rows]
        known = (starts != _ss._NS_MIN) & (time_ns != _ss._NS_MIN)
        if not known.any():
            return frame
        session_time = np.where(known, time_ns + starts, _ss._NS_MIN)
        date = np.where(known & (dates != _ss._NS_MIN), time_ns + dates, _ss._NS_MIN)
        if is_polars:
            new = frame.with_columns(
                pl.Series("Date", date, dtype=pl.Int64).cast(pl.Datetime("ns")),
                pl.Series("SessionTime", session_time, dtype=pl.Int64).cast(pl.Duration("ns")),
            ).select(["Date", "SessionTime", *columns])
            from_frame = getattr(type(frame), "_from_frame", None)
            if callable(from_frame):
                return from_frame(
                    new,
                    session=getattr(frame, "session", self),
                    driver=getattr(frame, "driver", driver),
                )
            return new
        frame.insert(0, "Date", date.view("M8[ns]"))
        frame.insert(1, "SessionTime", session_time.view("m8[ns]"))
        return frame

    def _get_telemetry_df_for_ref(
        self, driver: str, lap_num: int, *, ultra_cold: bool, allow_prefetch: bool = True
    ) -> DataFrame:
        """Get telemetry DataFrame for a specific (driver, lap) reference."""
        if self._ergast_only:  # no telemetry exists before F1_API_FIRST_YEAR
            return self._empty_session_telemetry(driver)

        def _as_pandas_telemetry(frame: DataFrame) -> DataFrame:
            return self._session_telemetry(frame, driver)

        cached_df = self._memo.get("telemetry_df", (driver, lap_num))
        if cached_df is not None:
            wrapped = _as_pandas_telemetry(cached_df)
            if wrapped is not cached_df:
                # Wrapping costs a shallow manager copy; keep the wrapped
                # object so repeat accesses of this lap are a memo hit.
                self._memo.set("telemetry_df", (driver, lap_num), wrapped)
            return wrapped

        # Pergamon P6: a single lap is served from the materialized frame
        # tier before the payload tier (0.8 ms of SQL + orjson + assembly
        # per lap otherwise). The driver's row is decoded as a whole, so
        # every other lap of that driver is memoized in the same read.
        tier_read = (
            getattr(get_cache(), "get_telemetry_frames_batch", None)
            if not ultra_cold and self.enable_cache and self._session_cache_available()
            else None
        )
        whole_driver = True
        if callable(tier_read) and driver not in self._memo.single_lap_drivers:
            # A driver's first lone lap is built from its cached payload: a
            # race driver row is ~6 MB to decode for one ~100 KB lap (pandas 5
            # vs 14 ms in a fresh process; polars reads the whole IPC row). A
            # second lap of the driver decodes the row below, so a loop over
            # its laps still pays it once.
            self._memo.single_lap_drivers.add(driver)
            tel_df = self._cached_payload_telemetry_df(driver, lap_num)
            if tel_df is not None:
                return tel_df
        if callable(tier_read) and self.lib == "pandas":
            # Pergamon Q1: the driver's row is decoded once and held; only
            # the touched lap's frame is built now, the others on access.
            read_sources = getattr(get_cache(), "get_telemetry_frame_sources", None)
            if callable(read_sources):
                source = read_sources(
                    self.year,
                    self.gp,
                    self.session,
                    [driver],
                    frame_class=self._telemetry_frame_class(),
                ).get(driver)
                if source is not None:
                    self._memo.add_telemetry_source(driver, source)
                    # The row is held now: a lap it lacks (legacy per-frame
                    # row) must not decode it again below.
                    whole_driver = False
                    if lap_num in source:
                        cached_df = self._memo.get("telemetry_df", (driver, lap_num))
                        if cached_df is not None:
                            return _as_pandas_telemetry(cached_df)
        if callable(tier_read):
            tier_hits = tier_read(
                self.year,
                self.gp,
                self.session,
                [(driver, lap_num)],
                lib=self.lib,
                whole_driver=whole_driver,
                frame_class=self._telemetry_frame_class(),
            )
            for ref, frame in tier_hits.items():
                if self._memo.get("telemetry_df", ref) is None:
                    tier_hits[ref] = self._adopt_telemetry_frame(frame, ref[0])
                    self._memo.set("telemetry_df", ref, tier_hits[ref])
            tier_frame = tier_hits.get((driver, lap_num))
            if tier_frame is not None:
                return _as_pandas_telemetry(tier_frame)

        tel_payload = self._get_telemetry_payload_for_ref(
            driver, lap_num, ultra_cold=ultra_cold, allow_prefetch=allow_prefetch
        )
        if tel_payload is None:
            # Mirror the original empty-path semantics exactly: fetches
            # short-circuited by _should_skip_telemetry_fetch return a
            # session-wrapped empty frame, missing data a plain empty frame.
            if self._should_skip_telemetry_fetch(driver):
                return _as_pandas_telemetry(_create_empty_df(self.lib))
            return _create_empty_df(self.lib)

        tel_df = _create_telemetry_df(
            tel_payload, driver, lap_num, self.lib, clock=self._lap_clock(driver, lap_num)
        )
        if tel_df is not None:
            wrapped_df = _as_pandas_telemetry(tel_df)
            self._memo.set("telemetry_df", (driver, lap_num), wrapped_df)
            return wrapped_df
        return _create_empty_df(self.lib)

    def _cached_payload_telemetry_df(self, driver: str, lap_num: int) -> DataFrame | None:
        """The lap's frame built from a memoized or SQLite-cached payload (no request)."""
        payload = self._get_telemetry_payload(driver, lap_num)
        if payload is None:
            cached = get_cache().get_telemetry(self.year, self.gp, self.session, driver, lap_num)
            if not isinstance(cached, dict) or not cached:
                return None
            payload = cached
        tel_df = _create_telemetry_df(
            payload, driver, lap_num, self.lib, clock=self._lap_clock(driver, lap_num)
        )
        if tel_df is None:
            return None
        wrapped = self._session_telemetry(tel_df, driver)
        self._memo.set("telemetry_df", (driver, lap_num), wrapped)
        return wrapped

    def get_fastest_lap_tel(self, ultra_cold: bool | None = None) -> DataFrame:
        """Get telemetry from overall fastest lap.

        This cold-start optimized path resolves the fastest lap directly from raw
        lap payloads, then fetches only the matching telemetry file.

        Args:
            ultra_cold: If True, minimize first-load latency by skipping sync cache
                reads/writes and validation on the critical path. If None, uses
                config key `ultra_cold_start`.

        Returns:
            DataFrame with telemetry from the overall fastest lap

        Example:
            >>> session = get_session(2025, "Monaco Grand Prix", "Qualifying")
            >>> fastest_tel = session.get_fastest_lap_tel()
            >>> f"Max speed: {fastest_tel['Speed'].max()} km/h"
            'Max speed: 298.5 km/h'
        """
        if self._ergast_only:  # no telemetry exists before F1_API_FIRST_YEAR
            return self._empty_session_telemetry()
        requested_ultra_cold = self._resolve_ultra_cold_mode(ultra_cold)
        auto_ultra_cold = (
            ultra_cold is None
            and not requested_ultra_cold
            and self.enable_cache
            and not self._session_cache_available()
            and self._is_fastest_lap_tel_cold_start()
        )
        ultra_cold_enabled = requested_ultra_cold or auto_ultra_cold
        fastest_lap_ref = self._get_fastest_lap_reference(ultra_cold=ultra_cold_enabled)
        if fastest_lap_ref is None:
            self._fastest_lap_tel_ref = None
            self._fastest_lap_tel_df = None
            return _create_empty_df(self.lib)

        if self._fastest_lap_tel_ref == fastest_lap_ref and self._fastest_lap_tel_df is not None:
            return self._fastest_lap_tel_df

        driver, lap_num = fastest_lap_ref
        tel_df = self._get_telemetry_df_for_ref(driver, lap_num, ultra_cold=ultra_cold_enabled)
        if _is_empty_df(tel_df, self.lib):
            self._fastest_lap_tel_ref = None
            self._fastest_lap_tel_df = None
            return tel_df

        self._fastest_lap_tel_ref = fastest_lap_ref
        self._fastest_lap_tel_df = tel_df
        return tel_df

    async def get_fastest_lap_tel_async(self, ultra_cold: bool | None = None) -> DataFrame:
        """Get telemetry from overall fastest lap asynchronously.

        Args:
            ultra_cold: If True, skip synchronous cache reads/writes and validation on
                the critical path. If None, uses config key `ultra_cold_start`.

        Returns:
            DataFrame with telemetry from the overall fastest lap.
        """
        if self._ergast_only:  # no telemetry exists before F1_API_FIRST_YEAR
            return self._empty_session_telemetry()
        requested_ultra_cold = self._resolve_ultra_cold_mode(ultra_cold)
        auto_ultra_cold = (
            ultra_cold is None
            and not requested_ultra_cold
            and self.enable_cache
            and not self._session_cache_available()
            and self._is_fastest_lap_tel_cold_start()
        )
        ultra_cold_enabled = requested_ultra_cold or auto_ultra_cold
        fastest_lap_ref = await self._get_fastest_lap_reference_async(ultra_cold=ultra_cold_enabled)
        if fastest_lap_ref is None:
            self._fastest_lap_tel_ref = None
            self._fastest_lap_tel_df = None
            return _create_empty_df(self.lib)

        if self._fastest_lap_tel_ref == fastest_lap_ref and self._fastest_lap_tel_df is not None:
            return self._fastest_lap_tel_df

        requests, lap_info, tels = self._fetch_telemetry_batch_from_refs(
            [fastest_lap_ref],
            skip_cache=ultra_cold_enabled,
        )

        if requests:
            try:
                if ultra_cold_enabled:
                    max_retries = 1 if config.get("ultra_cold_skip_retries", True) else None
                    try:
                        results = await fetch_multiple_async(
                            requests,
                            use_cache=False,
                            write_cache=self.enable_cache,
                            validate_payload=False,
                            max_retries=max_retries,
                        )
                    except TypeError as e:
                        if "unexpected keyword argument" not in str(e):
                            raise
                        results = await fetch_multiple_async(requests)
                else:
                    results = await fetch_multiple_async(requests)
                tels = self._process_telemetry_results(
                    results,
                    lap_info,
                    tels,
                    ultra_cold=ultra_cold_enabled,
                )
            except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
                logger.error(f"Failed to fetch telemetry for fastest lap: {e}")
                if not tels:
                    raise

        if not tels:
            self._fastest_lap_tel_ref = None
            self._fastest_lap_tel_df = None
            return _create_empty_df(self.lib)

        tel_df = self._assemble_telemetry_batch(tels[:1])
        if _is_empty_df(tel_df, self.lib):
            self._fastest_lap_tel_ref = None
            self._fastest_lap_tel_df = None
            return tel_df
        self._fastest_lap_tel_ref = fastest_lap_ref
        self._fastest_lap_tel_df = tel_df
        return tel_df

    def _telemetry_lap_refs(self, laps: DataFrame) -> list[tuple[str, int]]:
        """``(driver, lap_number)`` references of every lap row, in row order."""
        lap_refs: list[tuple[str, int]] = []
        if self.lib == "polars":
            laps_pl = cast(Any, laps)
            for row in laps_pl.iter_rows(named=True):
                driver = row.get("Driver")
                lap_num = row.get("LapNumber")
                if driver and lap_num is not None:
                    lap_refs.append((str(driver), int(lap_num)))
        else:
            # Vectorized extraction: iterrows over ~1500 rows costs ~200 ms.
            refs_frame = cast(pd.DataFrame, laps)[[COL_DRIVER, COL_LAP_NUMBER]].dropna()
            lap_refs.extend(
                zip(
                    refs_frame[COL_DRIVER].astype(str),
                    refs_frame[COL_LAP_NUMBER].astype(int),
                )
            )
        return lap_refs

    def _memoized_telemetry(
        self, lap_refs: list[tuple[str, int]], ultra_cold_enabled: bool
    ) -> tuple[dict[tuple[str, int], DataFrame], list[tuple[str, int]]]:
        """Frames already available without the network: ``(frames, pending refs)``.

        Frames memoized earlier in this Session (built by a previous
        fetch_all call or the per-lap telemetry path) are reused as-is, then
        the materialized-frame tier serves the rest of a warm load: those
        refs skip the payload read, decompress, parse and frame
        construction. Frames are only written from payload-tier hits, so
        cold fetches never pay for this tier. Both backends keep their own
        frame table (Rhodes R4); a pandas session never reads polars rows
        and vice versa.
        """
        telemetry_map: dict[tuple[str, int], DataFrame] = {}
        pending_refs: list[tuple[str, int]] = []
        # Frames a lazy load() still holds as sources are built in one pass
        # here (cheaper than one memo miss per ref below).
        self._memo.materialize_telemetry()
        for ref in lap_refs:
            memoized_df = self._memo.get("telemetry_df", ref)
            if memoized_df is not None:
                telemetry_map[ref] = memoized_df
            else:
                pending_refs.append(ref)
        if (
            pending_refs
            and not ultra_cold_enabled
            and self.enable_cache
            and self._session_cache_available()
        ):
            frame_hits = get_cache().get_telemetry_frames_batch(
                self.year,
                self.gp,
                self.session,
                pending_refs,
                lib=self.lib,
                frame_class=self._telemetry_frame_class(),
            )
            for ref, tier_frame in frame_hits.items():
                # polars frames come back as new objects: keep the adopted one.
                adopted = self._adopt_telemetry_frame(tier_frame, ref[0])
                self._memo.set("telemetry_df", ref, adopted)
                telemetry_map[ref] = adopted
            pending_refs = [ref for ref in pending_refs if ref not in frame_hits]
        return telemetry_map, pending_refs

    async def fetch_all_laps_telemetry_async(
        self, *, ultra_cold: bool | None = None
    ) -> dict[tuple[str, int], DataFrame]:
        """Fetch telemetry for all laps in the session (async batch operation).

        This method efficiently fetches telemetry for all laps across all drivers
        using batch operations and parallel requests.

        Args:
            ultra_cold: If True, skip validation and cache writes for faster cold starts.
                       If None, uses config value.

        Returns:
            Dictionary mapping (driver, lap_number) tuples to telemetry DataFrames.
            Missing or failed telemetry will not be included in the result.

        Example:
            >>> session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Race")
            >>> telemetry_map = await session.fetch_all_laps_telemetry_async()
            >>> ver_lap_1 = telemetry_map.get(("VER", 1))
        """
        if self._ergast_only:  # no telemetry exists before F1_API_FIRST_YEAR
            return {}
        ultra_cold_enabled = self._resolve_telemetry_ultra_cold_mode(ultra_cold)

        # Get all laps first
        laps = await self.laps_async()
        if _is_empty_df(laps, self.lib):
            return {}

        lap_refs = self._telemetry_lap_refs(laps)
        if not lap_refs:
            return {}
        telemetry_map, pending_refs = self._memoized_telemetry(lap_refs, ultra_cold_enabled)

        # Fetch telemetry in batch
        requests, lap_info, cached_tels = await self._fetch_telemetry_batch_from_refs_async(
            pending_refs, skip_cache=ultra_cold_enabled
        )

        materialized_frames = self._frames_from_payloads(cached_tels, telemetry_map)

        # Fetch remaining telemetry from network. Writes persist even in
        # ultra-cold mode (single telemetry-tier write per payload) so the
        # next process loads this session from a warm cache; the writes are
        # deferred out of the fetch slots into one serial post-gather flush.
        # (An overlapped flush/assembly variant was measured and rejected:
        # interleaved A/B showed +23%/+36% cold regressions on 1-core
        # vantages; see .agents/perf-sestos/RESULTS.md L8.)
        if requests:
            from .async_fetch import fetch_multiple_async

            results = await fetch_multiple_async(
                requests,
                use_cache=not ultra_cold_enabled,
                write_cache=self.enable_cache,
                validate_payload=not ultra_cold_enabled,
                defer_telemetry_writes=True,
            )

            with _suspend_gc():
                for (driver, lap_num), result in zip(lap_info, results):
                    if result is not None and isinstance(result, dict):
                        tel_data = result.get("tel", {})
                        if tel_data:
                            self._remember_telemetry_payload(driver, lap_num, tel_data)
                            tel_df = _create_telemetry_df(
                                tel_data,
                                driver,
                                lap_num,
                                self.lib,
                                clock=self._lap_clock(driver, lap_num),
                            )
                            if tel_df is not None and not _is_empty_df(tel_df, self.lib):
                                tel_df = self._bind_telemetry(tel_df, driver)
                                self._memo.set("telemetry_df", (driver, lap_num), tel_df)
                                telemetry_map[(driver, lap_num)] = tel_df
                                if self.enable_cache:
                                    # N1: frames from the cold network path are
                                    # materialized too, so the *next* load starts
                                    # at the frame tier instead of re-parsing the
                                    # payload tier first.
                                    materialized_frames.append((driver, lap_num, tel_df))

        # One bulk write materializes the assembled telemetry frames (payload
        # tier hits and network fetches alike); the next warm load reads them
        # instead of re-assembling.
        self._schedule_frame_materialization(materialized_frames)

        return telemetry_map

    def _frames_from_payloads(
        self,
        cached_tels: list[tuple[str, int, Any]],
        telemetry_map: dict[tuple[str, int], DataFrame],
    ) -> list[tuple[str, int, DataFrame]]:
        """Build, memoize and map frames for cached payloads; returns them for materialization.

        The per-frame build loop allocates heavily (Rhodes R3: GC suspension
        cuts the 1452-frame build ~20%); the scope stays inside this
        synchronous loop, so an event loop never observes GC off across an
        await.
        """
        materialized_frames: list[tuple[str, int, DataFrame]] = []
        with _suspend_gc():
            for driver, lap_num, tel_payload in cached_tels:
                tel_df = _create_telemetry_df(
                    tel_payload, driver, lap_num, self.lib, clock=self._lap_clock(driver, lap_num)
                )
                if tel_df is not None and not _is_empty_df(tel_df, self.lib):
                    tel_df = self._bind_telemetry(tel_df, driver)
                    self._memo.set("telemetry_df", (driver, lap_num), tel_df)
                    telemetry_map[(str(driver), int(lap_num))] = tel_df
                    materialized_frames.append((driver, lap_num, tel_df))
        return materialized_frames

    def _schedule_frame_materialization(
        self, materialized_frames: list[tuple[str, int, DataFrame]]
    ) -> None:
        """Hand assembled frames to the background frame-tier writer.

        Ithome U5: the write runs in a background daemon thread so the caller
        receives its frames ~0.4-0.5 s earlier on cold loads; close() and
        invalidate() join the writer, and the atexit cache close guarantees
        the tier is complete before process exit.
        """
        if not materialized_frames:
            return
        frames_to_write = list(materialized_frames)
        year, gp, session_name, lib = self.year, self.gp, self.session, self.lib

        def _write_frames() -> None:
            # Serialized: a load served partly from the payload tier and
            # partly from the network schedules two writes; the per-driver
            # rows are read-merge-written, so overlapping writers of one
            # driver would drop each other's laps.
            with _FRAME_MATERIALIZATION_LOCK:
                try:
                    get_cache().set_telemetry_frames_batch(
                        year, gp, session_name, frames_to_write, lib=lib
                    )
                except (AttributeError, RuntimeError, TypeError, ValueError) as e:
                    logger.debug("Telemetry frame materialization skipped: %s", e)

        register_frame_writer(_write_frames)

    def fetch_all_laps_telemetry(
        self, *, ultra_cold: bool | None = None
    ) -> dict[tuple[str, int], DataFrame]:
        """Fetch telemetry for all laps in the session (synchronous wrapper).

        This is a synchronous wrapper around fetch_all_laps_telemetry_async().
        For better performance in async contexts, use the async version directly.

        Args:
            ultra_cold: If True, skip validation and cache writes for faster cold starts.
                       If None, uses config value.

        Returns:
            Dictionary mapping (driver, lap_number) tuples to telemetry DataFrames.
            Missing or failed telemetry will not be included in the result.

        Example:
            >>> session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Race")
            >>> telemetry_map = session.fetch_all_laps_telemetry()
            >>> ver_lap_1 = telemetry_map.get(("VER", 1))
        """
        # Pergamon P5: a warm load (every frame memoized or in the frame
        # tier) is served synchronously — no event loop, no asyncio import.
        if self._ergast_only:  # no telemetry exists before F1_API_FIRST_YEAR
            return {}
        ultra_cold_enabled = self._resolve_telemetry_ultra_cold_mode(ultra_cold)
        laps = self.laps
        if _is_empty_df(laps, self.lib):
            return {}
        lap_refs = self._telemetry_lap_refs(laps)
        if not lap_refs:
            return {}
        telemetry_map, pending_refs = self._memoized_telemetry(lap_refs, ultra_cold_enabled)
        if not pending_refs or self._all_known_missing(pending_refs):
            return telemetry_map
        # Pergamon Q15: payload-tier hits (frame tiers empty or stale) are
        # served synchronously too; the batch read decodes the rows on worker
        # threads. Only refs still missing afterwards start the async fetch
        # pipeline, which then finds the frames built here in the memo.
        _, missing_refs, cached_tels = self._fetch_telemetry_batch_from_refs(
            pending_refs, skip_cache=ultra_cold_enabled
        )
        if cached_tels:
            self._schedule_frame_materialization(
                self._frames_from_payloads(cached_tels, telemetry_map)
            )
        if not missing_refs or self._all_known_missing(missing_refs):
            return telemetry_map

        # Q16: since laps can come from the frame tier without an event loop,
        # nothing has applied nest_asyncio2 yet when this runs in a Jupyter
        # cell (a running loop) — the bridge applies it before ``asyncio.run``.
        return _run_coroutine_sync(
            lambda: self.fetch_all_laps_telemetry_async(ultra_cold=ultra_cold),
            "fetch_all_laps_telemetry",
        )

    def _prefetch_telemetry_payloads(
        self, refs: list[tuple[str, int]], *, ultra_cold: bool
    ) -> list[tuple[str, int, Any]]:
        """Memoise the raw telemetry payloads of ``refs`` in one batch.

        Memoised payloads first, then one SQLite payload-tier read, then one
        parallel network wave for the rest (the per-lap path would fetch them
        one after another). Returns the ``(driver, lap, payload)`` entries
        found; a lap missing from every source is simply absent, and a
        runtime that cannot block on the event loop (Pyodide without JSPI)
        skips the network wave, leaving those laps to the per-lap path.
        """
        if self._ergast_only:  # no telemetry exists before F1_API_FIRST_YEAR
            return []
        requests, lap_info, found = self._fetch_telemetry_batch_from_refs(
            refs, skip_cache=ultra_cold
        )
        pending = [
            (request, ref)
            for request, ref in zip(requests, lap_info, strict=True)
            if not self._should_skip_telemetry_fetch(ref[0])
        ]
        if not pending or self._all_known_missing([ref for _, ref in pending]):
            return found
        try:
            results = _run_coroutine_sync(
                lambda: fetch_multiple_async(
                    [request for request, _ in pending],
                    use_cache=not ultra_cold,
                    write_cache=self.enable_cache,
                    validate_payload=not ultra_cold,
                    defer_telemetry_writes=True,
                ),
                "telemetry batch",
            )
        except AsyncRequiredError:
            return found
        for (_, (driver, lap_num)), result in zip(pending, results, strict=True):
            tel_data = result.get("tel") if isinstance(result, dict) else None
            if isinstance(tel_data, dict) and tel_data:
                self._remember_telemetry_payload(driver, lap_num, tel_data)
                found.append((driver, lap_num, tel_data))
        return found

    def _telemetry_frames_for_refs(
        self, refs: list[tuple[str, int]]
    ) -> dict[tuple[str, int], DataFrame]:
        """Session-bound telemetry frames of ``refs`` (laps without telemetry are absent).

        The batch twin of :meth:`_get_telemetry_df_for_ref`: memoised and
        frame-tier frames, then payloads (memo, SQLite, one parallel network
        wave), built and memoised once; a lap still missing afterwards goes
        through the per-lap path, which records its failure.
        """
        if not refs or self._ergast_only:
            return {}
        ultra_cold = self._resolve_telemetry_ultra_cold_mode(None)
        frames, pending = self._memoized_telemetry(refs, ultra_cold)
        frames = {ref: self._session_telemetry(frame, ref[0]) for ref, frame in frames.items()}
        if not pending:
            return frames
        found = self._prefetch_telemetry_payloads(pending, ultra_cold=ultra_cold)
        self._schedule_frame_materialization(self._frames_from_payloads(found, frames))
        for driver, lap_num in pending:
            if (driver, lap_num) in frames or self._should_skip_telemetry_fetch(driver):
                continue
            try:
                frame = self._get_telemetry_df_for_ref(
                    driver, lap_num, ultra_cold=ultra_cold, allow_prefetch=False
                )
            except (DataNotFoundError, InvalidDataError, NetworkError, TypeError, ValueError) as e:
                self._record_telemetry_failure(driver, lap_num, e)
                continue
            if frame is not None and not _is_empty_df(frame, self.lib):
                frames[(driver, lap_num)] = frame
        return frames

    def _all_known_missing(self, refs: list[tuple[str, int]]) -> bool:
        """True when every ref is a payload the negative-result cache knows is missing.

        Sessions routinely list a few laps whose telemetry file exists on no
        CDN (recorded by the L2 verdict tier); a warm load must not spin up
        the async fetch pipeline just to re-confirm them.
        """
        if not self.enable_cache or len(refs) > 64:
            return False
        try:
            cache = get_cache()
            loader = self._payload_loader
            return all(
                cache.is_known_missing(loader.cache_key(f"{driver}/{int(lap)}_tel.json"))
                for driver, lap in refs
            )
        except Exception:
            return False


#: Columns of :attr:`Session.drivers_df`, in order.
_DRIVERS_DF_COLUMNS = (
    "Driver",
    "Team",
    "DriverNumber",
    "FirstName",
    "LastName",
    "TeamColor",
    "HeadshotUrl",
)

SESSION_MAPPING = {
    "FP1": "Practice 1",
    "FP2": "Practice 2",
    "FP3": "Practice 3",
    "Q": "Qualifying",
    "S": "Sprint",
    "SS": "Sprint Shootout",
    "SQ": "Sprint Qualifying",
    "R": "Race",
}

# Backward-compatible view of the alias table that event resolution uses
# (``tif1.events`` applies it, with per-season fallbacks).
GP_ALIAS_MAP = {alias: names[0] for alias, names in EVENT_NAME_ALIASES.items()}


def _normalize_event_key(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9 ]+", " ", name.lower())
    normalized = normalized.replace("grand prix", " ")
    normalized = normalized.replace("gp", " ")
    return " ".join(normalized.split())


# Canonical session names as the schedule stores them, keyed case-insensitively
# so "race", "qualifying" and "SPRINT" resolve like "Race" and "R" do.
_CANONICAL_SESSION_NAMES = {
    name.lower(): name
    for name in (
        "Practice 1",
        "Practice 2",
        "Practice 3",
        "Qualifying",
        "Sprint",
        "Sprint Shootout",
        "Sprint Qualifying",
        "Race",
    )
}


def _normalize_session_name(session: str, year: int | None = None) -> str:
    """Canonical session name for an abbreviation or a full name in any case/spacing.

    ``year`` applies fastf1's 2021/22 correction: the sprint was called
    "Sprint Qualifying" then, so "SQ" and "Sprint Qualifying" mean "Sprint".
    Unknown names come back unchanged (``get_session`` reports them).
    """
    key = session.strip()
    mapped = SESSION_MAPPING.get(key.upper())
    if mapped is None:
        mapped = _CANONICAL_SESSION_NAMES.get(" ".join(key.lower().split()))
    if mapped is None:
        return session
    if mapped == "Sprint Qualifying" and year in (2021, 2022):
        return "Sprint"
    return mapped


def _session_number(session: object) -> int | None:
    """Session number an identifier stands for (``None`` for a session name).

    fastf1 parity: anything ``float()`` accepts is a number, so ``"1"``,
    ``1.0`` and numpy integers mean session 1; a fractional or non-finite
    number is invalid.
    """
    if isinstance(session, str):
        try:
            value = float(session)
        except ValueError:
            return None
    elif isinstance(session, numbers.Real):
        value = float(session)
    else:
        return None
    if not value.is_integer():
        raise ValueError(f"Invalid session type '{session}'")
    return int(value)


def _resolve_gp_name(
    year: int, gp: str | int, *, backend: ScheduleBackend | None = None, exact_match: bool = False
) -> str:
    """Season event name for a round number or an event name (pandas-free).

    Any real number is a round (numpy integers and whole floats included, as
    in fastf1); a string goes through exact name, alias and fuzzy matching
    (:func:`tif1.events._resolve_event_name`, memoized per season).
    """
    from .events import _resolve_event_name, _resolve_event_round

    if isinstance(gp, numbers.Real):
        return _resolve_event_round(year, gp, backend=backend)
    gp_str = str(gp).strip()
    if not gp_str:
        return gp_str
    try:
        return _resolve_event_name(year, gp_str, exact_match=exact_match, backend=backend)
    except EventNotFoundError:
        # "Suzuka" in 2021 (alias understood, race not held) or "Invalid GP"
        # (resembles no event). Loading a different race or failing later with
        # DataNotFoundError would hide that, so fail here with the message.
        raise
    except (ValueError, AttributeError):
        # If event resolution fails, return the original string
        return gp_str


def _resolve_session_name(
    year: int, gp_name: str, session: str | int, *, backend: ScheduleBackend | None = None
) -> str:
    number = _session_number(session)
    if number is not None:
        from .events import get_sessions

        sessions = (
            get_sessions(year, gp_name)
            if backend is None
            else get_sessions(year, gp_name, backend=backend)
        )
        if 1 <= number <= len(sessions):
            return sessions[number - 1]
        raise ValueError(f"session index out of range: {session}")
    if not isinstance(session, str):
        raise ValueError(f"Invalid session type '{session}'")
    return _normalize_session_name(session, year)


def get_session(
    year: int,
    gp: str | int,
    session: str | int | None = None,
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] | None = None,
    *,
    identifier: str | int | None = None,
    exact_match: bool = False,
    ergast: bool | None = None,
    backend: ScheduleBackend | None = None,
    force_ergast: bool = False,
) -> Session:
    """
    Get a session object.

    Args:
        year: Year (2018-current)
        gp: Grand Prix name or round number (e.g., "Abu Dhabi Grand Prix" or 1)
        session: Session name (e.g., "Practice 1", "Qualifying", "Race") or
            number (``1``-``5``).
        enable_cache: Enable caching. If None, uses configured default.
        lib: Data lib choice - 'pandas' or 'polars'. If None, uses configured default.
        ergast: Merge the official Ergast classification into ``results`` and
            ``drivers`` (default: the ``ergast_results`` setting).
        backend: Schedule backend used to resolve the event name
            (``"fastf1"``/``"f1timing"`` read the f1schedule calendar,
            ``"ergast"`` the Ergast race table). Default: f1schedule from
            2018, Ergast before.
        force_ergast: Deprecated alias of ``backend="ergast"``.
        identifier: fastf1's name of ``session`` (pass one of the two).
        exact_match: Match ``gp`` against the event names exactly (no alias
            table, no fuzzy matching), as fastf1's ``exact_match``.

    Returns:
        Session object with lap and telemetry data

    Raises:
        ValueError: If year is out of supported range or session doesn't exist for the event
        DataNotFoundError: A practice session of a season before 2018 (the
            schedule lists it, as fastf1's Ergast backend does, but Ergast
            carries no practice data)

    Example:
        >>> session = get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        >>> session = get_session(2025, 1, "Practice 1")  # Using round number
        >>> session_polars = get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", lib="polars")
        >>> session.drivers  # car numbers, classification order
        ['81', '63', '4', ...]
        >>> laps = session.laps
    """
    if force_ergast:
        warnings.warn(
            "Option ``force_ergast`` has been deprecated, use ``backend='ergast'`` instead",
            DeprecationWarning,
            stacklevel=2,
        )
        backend = "ergast"
    if session is not None and identifier is not None and session != identifier:
        raise ValueError("Cannot pass both 'session' and 'identifier'")
    if session is None:
        session = identifier
    if session is None:
        raise TypeError("get_session() missing 1 required argument: 'identifier'")
    # The backend/exact_match keywords are only passed when set, so test
    # doubles of the event helpers with the original two-argument signature
    # keep working.
    resolve_kwargs: dict[str, Any] = {}
    if backend is not None:
        resolve_kwargs["backend"] = backend
    if exact_match:
        resolve_kwargs["exact_match"] = True
    gp_name = _resolve_gp_name(year, gp, **resolve_kwargs)
    session_name = (
        _resolve_session_name(year, gp_name, session)
        if backend is None
        else _resolve_session_name(year, gp_name, session, backend=backend)
    )

    # Validate that the session exists for this event
    from .events import get_sessions

    available_sessions = (
        get_sessions(year, gp_name)
        if backend is None
        else get_sessions(year, gp_name, backend=backend)
    )
    if session_name not in available_sessions:
        raise ValueError(
            f"Session '{session_name}' does not exist for {year} {gp_name}.\n"
            f"Available sessions: {', '.join(available_sessions)}"
        )
    if year < F1_API_FIRST_YEAR and session_name not in ERGAST_LOADABLE_SESSIONS:
        # Listed in the schedule (fastf1's Ergast backend lists practice with
        # estimated dates) but Ergast holds no practice data: fail here.
        raise DataNotFoundError(
            year=year,
            event=gp_name,
            session=session_name,
            reason=(
                f"Ergast has no {session_name} data; seasons before {F1_API_FIRST_YEAR} "
                f"load {', '.join(ERGAST_LOADABLE_SESSIONS)} only"
            ),
        )

    # gp_name is resolved already: the constructor must not resolve it again.
    new_session = Session(
        year, gp_name, session_name, enable_cache, lib, ergast, backend, _event_resolved=True
    )
    _preconnect_cdn(new_session)
    return new_session


_preconnect_started = False
_preconnect_lock = threading.Lock()


def _preconnect_cdn(session: Session) -> None:
    """Warm the shared CDN connection in the background (Argos A8).

    The first request of a cold load pays DNS + TLS (160-800 ms measured
    against jsDelivr); opening the connection from a daemon thread while the
    caller is still between ``get_session()`` and its first ``laps`` access
    hides most of it. One attempt per process; any failure is swallowed (the
    real fetch path reports its own errors) and the hook is skipped in
    offline/CI mode or when ``cdn_preconnect`` is off.
    """
    global _preconnect_started
    if not THREADS_AVAILABLE:
        return
    if (
        _preconnect_started
        or not bool(config.get("cdn_preconnect", True))
        or bool(config.get("offline_mode", False))
        or bool(config.get("ci_mode", False))
    ):
        return
    try:
        # A cached session loads without the network; the warm-up thread
        # would only steal GIL time from the cache reads (measured +0.1-0.2 s
        # on warm loads).
        if session.enable_cache and session._session_cache_available():
            return
    except Exception:  # cache probe failures never block the hook
        return
    with _preconnect_lock:
        if _preconnect_started:
            return
        _preconnect_started = True
    year, gp, session_name = session.year, session.gp, session.session

    def _warm() -> None:
        try:
            from .http_session import get_session as get_http_session

            sources = get_cdn_manager().get_sources()
            if not sources:
                return
            url = sources[0].format_url(year, gp, session_name, "drivers.json")
            http = get_http_session()
            http.head(url, timeout=max(1, config.get("timeout", 10)))
        except Exception as e:  # best effort, never surfaces
            logger.debug("CDN pre-connect skipped: %s", e)

    threading.Thread(target=_warm, name="tif1-cdn-preconnect", daemon=True).start()
