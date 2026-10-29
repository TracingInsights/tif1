"""Canonical core data models.

The DataFrame model family (:class:`Laps`, :class:`Lap`, :class:`Telemetry`,
:class:`Driver`, :class:`SessionResults`, :class:`DriverResult`,
:class:`CircuitInfo`) is pure in-process pandas code. Anything the models need
from a session goes through the narrow :class:`TelemetryProvider` protocol;
:class:`tif1.core.Session` satisfies it structurally as the production adapter,
and tests can satisfy it with in-memory implementations.

This module must never import :mod:`tif1.core` at runtime — ``core`` re-exports
these models, so importing it here would create an import cycle.
"""

import importlib
import logging
import math
import re
from collections.abc import Generator, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, Self, cast, runtime_checkable

import numpy as np
import pandas as pd

from .cache import get_cache
from .core_utils.constants import COL_DRIVER, COL_LAP_NUMBER, LAP_PAYLOAD_KEYS
from .core_utils.driver_laps import build_driver_laps_frame
from .core_utils.ergast_results import unique_driver_labels
from .core_utils.helpers import (
    DataFrame,
    _coerce_lap_number,
    _create_empty_df,
    _create_lap_df,
    _create_telemetry_df,
    _filter_valid_laptimes,
    _get_lap_column,
    _is_empty_df,
    _merge_telemetry_payloads,
    _not_deleted_mask,
    _numeric_seconds_to_timedelta,
    _process_lap_df,
    _telemetry_frame_from_merged,
    _validate_lap_number,
)
from .core_utils.runtime import import_supported_polars
from .exceptions import (
    DataNotFoundError,
    InvalidDataError,
    LapNotFoundError,
    NetworkError,
    TIF1Error,
)

if TYPE_CHECKING:
    import polars as pl
else:
    # Polars is optional; _ensure_polars_available() (below) imports it on
    # first polars-backend use instead of paying ~260 ms at module load.
    pl = None

POLARS_AVAILABLE = False

logger = logging.getLogger(__name__)


def _ensure_polars_available() -> bool:
    """Lazy-load Polars and refresh stale module state."""
    global pl, POLARS_AVAILABLE
    if POLARS_AVAILABLE and pl is not None:
        return True

    pl = import_supported_polars()  # type: ignore[ty:invalid-assignment]
    POLARS_AVAILABLE = pl is not None
    return POLARS_AVAILABLE


@runtime_checkable
class TelemetryProvider(Protocol):
    """Narrow session interface consumed by the model family.

    The DataFrame models are pure in-process pandas code, but their lazy
    loading paths resolve payloads, cache state, and failure tracking through
    the owning session. :class:`tif1.core.Session` satisfies this protocol
    structurally as the production adapter; tests can provide lightweight
    in-memory implementations instead. Members intentionally mirror the exact
    (mostly private) session surface the models already used when they lived
    in ``core.py`` — no runtime behavior changes.

    Decorated with :func:`typing.runtime_checkable` so tests (and session
    adapters) can assert structural conformance with ``isinstance`` — on
    Python 3.11 this checks method members only, on 3.12+ attribute members
    are checked as well.
    """

    year: int
    gp: str
    session: str
    enable_cache: bool
    lib: Literal["pandas", "polars"]
    _laps: Any
    _memo: Any

    @property
    def _drivers_data(self) -> list[dict]: ...
    @property
    def laps(self) -> DataFrame: ...
    @property
    def weather_data(self) -> DataFrame: ...
    def _resolve_ultra_cold_mode(self, ultra_cold: bool | None) -> bool: ...
    def _resolve_telemetry_ultra_cold_mode(self, ultra_cold: bool | None) -> bool: ...
    def _should_backfill_ultra_cold_cache(self, ultra_cold_enabled: bool) -> bool: ...
    def _session_cache_available(self) -> bool: ...
    def _mark_session_cache_populated(self) -> None: ...
    def _fetch_json(self, path: str) -> dict: ...
    def _fetch_json_unvalidated(self, path: str) -> dict[str, Any]: ...
    def _remember_local_payload(self, path: str, data: Any) -> None: ...
    def _remember_telemetry_payload(
        self, driver: str, lap_num: int, tel_payload: dict[str, Any] | None
    ) -> None: ...
    def _get_telemetry_payload(self, driver: str, lap_num: int) -> dict[str, Any] | None: ...
    def _get_telemetry_payload_for_ref(
        self, driver: str, lap_num: int, *, ultra_cold: bool, allow_prefetch: bool = True
    ) -> dict[str, Any] | None: ...
    def _get_telemetry_df_for_ref(
        self, driver: str, lap_num: int, *, ultra_cold: bool, allow_prefetch: bool = True
    ) -> DataFrame: ...
    def _record_telemetry_failure(self, driver: str, lap_num: int, error: Exception) -> None: ...
    def _should_skip_telemetry_fetch(self, driver: str) -> bool: ...
    def _schedule_background_cache_fill(
        self,
        *,
        json_payloads: list[tuple[str, dict[str, Any]]] | None = None,
        telemetry_payload: tuple[str, int, dict[str, Any]] | None = None,
        telemetry_payloads: list[tuple[str, int, dict[str, Any]]] | None = None,
    ) -> None: ...
    def _get_driver_info(self, driver: str) -> dict: ...
    def _get_or_derive_driver_laptime_payload(self, driver: str) -> dict[str, Any] | None: ...
    def _fetch_laptime_payloads(
        self,
        driver_requests: list[tuple[dict[str, Any], str]],
        *,
        operation: str,
        ultra_cold: bool = False,
        prefer_session_payload: bool = True,
    ) -> tuple[list[dict[str, Any] | None], list[tuple[str, dict[str, Any]]]]: ...
    def _extract_fastest_lap_candidate(
        self, driver: str, lap_data: Any
    ) -> tuple[str, int, float] | None: ...
    def _lap_time_sort_column(self, laps: Any) -> str: ...
    def get_fastest_laps_tels(
        self, by_driver: bool = True, drivers: list[str] | None = None
    ) -> DataFrame: ...


# --- FastF1 Compatibility Classes ---

#: Columns used in every corners / marshal-marker DataFrame.
_CORNERS_DF_COLUMNS = ["X", "Y", "Number", "Letter", "Angle", "Distance"]
#: tif1 extras of :class:`CircuitInfo`: the track outline and the track sections.
_TRACK_DF_COLUMNS = ["X", "Y", "Time"]
_SECTIONS_DF_COLUMNS = [
    "Number",
    "Section",
    "SectionType",
    "StartPercent",
    "EndPercent",
    "StartDistance",
    "EndDistance",
]


def _empty_marker_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_CORNERS_DF_COLUMNS)


#: The per-lap weather sample the laps table carries (``WeatherTime`` first,
#: then fastf1's seven weather channels in fastf1's order).
_LAP_WEATHER_COLUMNS = (
    "WeatherTime",
    "AirTemp",
    "Humidity",
    "Pressure",
    "Rainfall",
    "TrackTemp",
    "WindDirection",
    "WindSpeed",
)
#: Columns of a per-lap weather sample (``get_weather_data``): ``Time`` (the
#: sample's session time) then the seven channels.
_WEATHER_SAMPLE_COLUMNS = ("Time", *_LAP_WEATHER_COLUMNS[1:])
#: dtypes of ``session.weather_data`` (the all-missing sample frame uses them).
_WEATHER_SAMPLE_DTYPES = {
    "Time": "timedelta64[ns]",
    "AirTemp": "float64",
    "Humidity": "float64",
    "Pressure": "float64",
    "Rainfall": "boolean",
    "TrackTemp": "float64",
    "WindDirection": "Int64",
    "WindSpeed": "float64",
}
#: Sessions ``split_qualifying_sessions`` accepts (fastf1's quali-like names).
_QUALI_LIKE_SESSION_NAMES = frozenset({"Qualifying", "Sprint Qualifying", "Sprint Shootout"})
#: Gap between consecutive lap starts that separates two qualifying segments
#: when neither a ``QualifyingSession`` marker nor session status exists (5 min).
_QUALI_SEGMENT_GAP = 5 * 60 * 10**9
_NAT_NS = np.iinfo("int64").min
#: Upper bound of the open-ended last qualifying segment (no lap starts after it).
_NO_UPPER_BOUND = np.iinfo("int64").max


def _timedelta_ns(values: pd.Series) -> np.ndarray:
    """A time-like series as int64 nanoseconds, whatever its resolution (NaT: int64 min).

    Numbers are seconds (fastf1 compatibility). pandas 3 keeps ``ms``/``us``
    resolutions, so a bare ``.view("i8")`` of a timedelta column is not
    nanoseconds in general.
    """
    coerced = Telemetry._coerce_timedelta_series(values)
    return coerced.astype("timedelta64[ns]").to_numpy().view("i8")


def _weather_rows_for_laps(
    weather_times: np.ndarray, starts: np.ndarray, ends: np.ndarray
) -> np.ndarray:
    """Position of each lap's weather sample in ``weather_times`` (``-1`` when none).

    fastf1's ``Lap.get_weather_data`` rule: the first sample within
    ``[start, end]``, else the last sample at or before ``end``. All inputs
    are int64 nanoseconds with ``NaT`` as the int64 minimum: a lap without a
    start takes the last sample before its end, a lap without an end has no
    sample, and a sample without a time is never picked (fastf1's NaT
    comparisons are all False).
    """
    valid = np.flatnonzero(weather_times != _NAT_NS)
    if not len(valid):
        return np.full(len(starts), -1, dtype=np.intp)
    order = valid[np.argsort(weather_times[valid], kind="stable")]
    sorted_times = weather_times[order]
    n = len(sorted_times)
    has_end = ends != _NAT_NS
    first = np.searchsorted(sorted_times, starts, side="left")
    first_time = sorted_times[np.minimum(first, n - 1)]
    within = (starts != _NAT_NS) & has_end & (first < n) & (first_time <= ends)
    last_before = np.where(has_end, np.searchsorted(sorted_times, ends, side="right") - 1, -1)
    rows = np.where(within, first, last_before)
    return np.where(rows < 0, -1, order[np.maximum(rows, 0)])


def _weather_sample_frame(weather: pd.DataFrame, rows: np.ndarray, index: pd.Index) -> pd.DataFrame:
    """``weather``'s rows at ``rows`` (``-1``: all missing) as the sample columns on ``index``.

    The session table's dtypes are kept (fastf1 restores them); a channel the
    table lacks is all-NaN.
    """
    fill = bool((rows < 0).any())
    data: dict[str, Any] = {}
    for column in _WEATHER_SAMPLE_COLUMNS:
        if column in weather.columns:
            data[column] = pd.api.extensions.take(weather[column].array, rows, allow_fill=fill)
        else:
            data[column] = np.full(len(rows), np.nan)
    return pd.DataFrame(data, index=index, copy=False)


def _missing_weather_frame(index: pd.Index) -> pd.DataFrame:
    """All-missing weather samples on ``index`` with ``session.weather_data``'s dtypes."""
    return pd.DataFrame(
        {
            column: pd.Series(index=index, dtype=dtype)
            for column, dtype in _WEATHER_SAMPLE_DTYPES.items()
        },
        index=index,
    )


def _session_weather_table(session: Any) -> pd.DataFrame | None:
    """``session.weather_data`` when it is a non-empty pandas table with ``Time``."""
    if session is None:
        return None
    weather = getattr(session, "weather_data", None)
    if not isinstance(weather, pd.DataFrame) or weather.empty or "Time" not in weather.columns:
        return None
    return weather


def _identifier_items(identifiers: Any) -> list[Any]:
    """``identifiers`` as a list for the ``pick_*`` selectors.

    A string, number, mapping or driver-like object (``.driver``/
    ``.Abbreviation``, e.g. a results row) is one identifier; anything else
    iterable (list, tuple, set, pandas/polars Series, numpy array, generator)
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


def _true_flags(values: pd.Series) -> np.ndarray:
    """``values == True`` as a numpy mask, a missing flag counting as False (fastf1's ``== True``)."""
    if values.dtype == bool:
        return values.to_numpy()
    return values.eq(True).fillna(False).to_numpy(dtype=bool)


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


def _session_status_rows(status: Any) -> list[tuple[int | None, str]]:
    """``(Time in ns or None, Status)`` per row of a session-status table (pandas or polars)."""
    columns = getattr(status, "columns", None)
    if columns is None or "Time" not in columns or "Status" not in columns or not len(status):
        return []
    if isinstance(status, pd.DataFrame):
        times = _timedelta_ns(cast(pd.Series, status["Time"]))
    else:
        times = _timedelta_ns(pd.Series(list(status["Time"]), dtype=object))
    return [
        (None if time == _NAT_NS else int(time), str(state))
        for time, state in zip(times.tolist(), list(status["Status"]), strict=True)
    ]


def _qualifying_split_times(session: Any, starts: np.ndarray) -> list[int | None] | None:
    """Qualifying segment bounds in int64 ns (``None`` is NaT: no lap matches it).

    fastf1's algorithm on ``session.session_status``: every ``Started`` row
    opens a segment except the restart after an ``Aborted`` row (a red flag),
    which clears the suspension, as ``Finished`` does; a session's own
    ``_session_split_times`` take precedence; the table's last time closes the
    last segment. Without session status, :func:`_lap_start_gap_split_times`.
    ``None`` when there is nothing to split on.
    """
    status_rows = (
        _session_status_rows(getattr(session, "session_status", None))
        if session is not None
        else []
    )
    if not status_rows:
        return _lap_start_gap_split_times(starts)
    preset = getattr(session, "_session_split_times", None)
    split_times: list[int | None] = []
    if isinstance(preset, list | tuple) and preset:
        split_times = [
            None if time == _NAT_NS else int(time)
            for time in _timedelta_ns(pd.Series(list(preset), dtype=object)).tolist()
        ]
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


def _lap_start_gap_split_times(starts: np.ndarray) -> list[int | None] | None:
    """Segment bounds from the lap starts alone (the last resort, no session status).

    The two longest gaps of more than :data:`_QUALI_SEGMENT_GAP` between
    consecutive lap starts (any driver) are the breaks; a bound sits just
    before the first lap start after each break and the last segment is
    open-ended. ``None`` without any lap start.
    """
    valid = np.sort(starts[starts != _NAT_NS])
    if not len(valid):
        return None
    gaps = np.diff(valid)
    breaks = np.flatnonzero(gaps > _QUALI_SEGMENT_GAP)
    if len(breaks) > 2:
        # Longest first, ties to the earlier break; then back in time order.
        breaks = np.sort(breaks[np.lexsort((breaks, -gaps[breaks]))][:2])
    return [int(valid[0]) - 1, *(int(valid[b + 1]) - 1 for b in breaks), _NO_UPPER_BOUND]


def _qualifying_segment_masks(
    split_times: list[int | None], starts: np.ndarray, ends: np.ndarray, pit_out: np.ndarray
) -> list[np.ndarray | None]:
    """fastf1's lap assignment between consecutive ``split_times`` (three segments at most).

    A lap belongs to the segment its start falls in (strictly between the
    bounds); an out-lap that starts before a bound and ends after it belongs
    to the next segment instead. ``starts``/``ends`` are int64 ns (``NaT``:
    the int64 minimum, never matching); ``pit_out`` flags laps with a
    ``PitOutTime``.
    """
    has_start = starts != _NAT_NS
    has_end = ends != _NAT_NS
    nothing = np.zeros(len(starts), dtype=bool)
    masks: list[np.ndarray | None] = [None, None, None]
    previous_early: np.ndarray | None = None
    for i in range(min(3, len(split_times) - 1)):
        low, high = split_times[i], split_times[i + 1]
        if low is None or high is None:
            cond = nothing
        else:
            cond = has_start & (starts > low) & (starts < high)
        if previous_early is not None:
            cond = cond | previous_early
        if high is None:
            is_early = nothing
        else:
            is_early = has_start & (starts < high) & has_end & (ends > high) & pit_out
        masks[i] = cond & ~is_early
        previous_early = is_early
    return masks


@dataclass
class CircuitInfo:
    """Holds information about the circuit layout.

    This is a drop-in replacement for :class:`fastf1.mvapi.CircuitInfo`.
    Columns for all marker DataFrames: ``X <float>, Y <float>,
    Number <int>, Letter <str>, Angle <float>, Distance <float>``.

    ``corners``, ``marshal_lights``, ``marshal_sectors`` and ``rotation`` are
    fastf1's attributes. The remaining attributes are tif1 extras read from
    the new-schema ``corners.json`` or from the MultiViewer API; they are
    empty (never ``None`` for the frames and strings) when the source does
    not carry them. On a polars session every frame is a polars frame.
    """

    corners: pd.DataFrame = field(default_factory=_empty_marker_frame)
    """Location of corners (FastF1-compatible DataFrame)."""

    marshal_lights: pd.DataFrame = field(default_factory=_empty_marker_frame)
    """Location of marshal lights (new-schema ``corners.json`` or MultiViewer)."""

    marshal_sectors: pd.DataFrame = field(default_factory=_empty_marker_frame)
    """Location of marshal sectors (new-schema ``corners.json`` or MultiViewer)."""

    rotation: float = 0.0
    """Rotation of the circuit in degrees."""

    circuit_key: int | None = None
    """The F1 live-timing circuit key (the MultiViewer API's ``circuitKey``)."""

    circuit_name: str = ""
    """Short circuit name (``"Sakhir"``)."""

    location: str = ""
    """Circuit location (``"Sakhir"``)."""

    country: str = ""
    """Country name (``"Bahrain"``)."""

    country_code: str = ""
    """IOC country code (``"BRN"``)."""

    country_key: int | None = None
    """The F1 live-timing country key."""

    pit_loss: dict[str, float] = field(default_factory=dict)
    """Pit-lane time loss in seconds: ``normal``, ``sc``, ``vsc`` (empty when unknown)."""

    track: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=_TRACK_DF_COLUMNS))
    """Track outline: ``X``, ``Y`` and the session ``Time`` of each outline sample."""

    sections: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=_SECTIONS_DF_COLUMNS)
    )
    """Track sections per corner: ``Number, Section, SectionType, StartPercent,
    EndPercent, StartDistance, EndDistance``."""

    mini_sector_index: list[int] = field(default_factory=list)
    """Mini-sector boundaries as indices into the track outline."""

    source: str = ""
    """Where the data came from: ``"corners.json"``, ``"multiviewer"`` or both joined by ``+``."""

    def add_marker_distance(self, reference_lap: "Lap") -> None:
        """Compute the ``Distance`` value for each track marker.

        This is a FastF1-compatible method.  It populates the ``Distance``
        column of :attr:`corners`, :attr:`marshal_lights`, and
        :attr:`marshal_sectors` using the XY position data from the
        telemetry of *reference_lap*.

        The distance is selected via a *best-fit* approach: for each marker
        the telemetry sample whose squared XY error relative to the marker
        position is smallest is chosen, and its ``Distance`` value is
        assigned to the marker. The frames may be pandas or polars (a polars
        frame is replaced by its updated copy).

        Args:
            reference_lap: A :class:`Lap` whose telemetry contains
                ``X``, ``Y``, and ``Distance`` columns.
        """
        self._fit_marker_distances(reference_lap, only_missing=False)

    def _fit_marker_distances(self, reference_lap: Any, *, only_missing: bool) -> None:
        """:meth:`add_marker_distance`; ``only_missing`` keeps the distances already known."""
        _log = logging.getLogger(__name__)

        try:
            tel = reference_lap.telemetry
        except Exception as exc:  # pragma: no cover
            _log.warning(
                "Failed to generate marker distance information: could not retrieve telemetry (%s)",
                exc,
            )
            return

        if tel is None or len(tel) == 0:
            _log.warning("Failed to generate marker distance information: telemetry data is empty")
            return

        # Keep only rows that have valid X/Y position data.
        # tif1 does not use a 'Source' column like FastF1's merged
        # telemetry; instead we simply drop rows where X or Y is NaN.
        required = {"X", "Y", "Distance"}
        if not required.issubset(tel.columns):
            _log.warning(
                "Failed to generate marker distance information: "
                "telemetry is missing required columns %s",
                required - set(tel.columns),
            )
            return

        samples = _float_columns(tel, ["X", "Y", "Distance"])
        samples = samples[~np.isnan(samples).any(axis=1)]
        if not len(samples):
            _log.warning(
                "Failed to generate marker distance information: "
                "no valid position samples found in telemetry"
            )
            return
        xy_ref = samples[:, :2]
        sample_distance = samples[:, 2]

        for name in ("corners", "marshal_sectors", "marshal_lights"):
            df = getattr(self, name)
            if not len(df):
                continue
            marker_xy = _float_columns(df, ["X", "Y"])
            current = _float_columns(df, ["Distance"])[:, 0]
            target = ~np.isnan(marker_xy).any(axis=1)
            if only_missing:
                target &= np.isnan(current)
            if not target.any():
                continue
            # Squared XY error of every marker against every track sample
            # (n_markers x n_samples); the closest sample gives the distance.
            diff = xy_ref[None, :, :] - marker_xy[target][:, None, :]
            nearest = np.argmin(diff[..., 0] ** 2 + diff[..., 1] ** 2, axis=1)
            distances = current.copy()
            distances[target] = sample_distance[nearest]
            if isinstance(df, pd.DataFrame):
                df["Distance"] = distances
            else:
                import polars as pl

                setattr(self, name, df.with_columns(pl.Series("Distance", distances)))


def _float_columns(frame: Any, columns: list[str]) -> np.ndarray:
    """``columns`` of a pandas or polars frame as one float64 array (NaN for missing)."""
    if isinstance(frame, pd.DataFrame):
        return frame[columns].to_numpy(dtype=float, na_value=np.nan)
    return np.column_stack(
        [frame[column].cast(float).to_numpy().astype(float, copy=False) for column in columns]
    )


class LazyTelemetryDict(dict):
    """``session.car_data`` / ``session.pos_data``: per-driver telemetry, fetched on first read.

    Keyed by driver number like fastf1 (abbreviations are accepted too); the
    value is the driver's laps concatenated in session order as one
    :class:`Telemetry` (a ``PolarsTelemetry`` on a polars session), memoised
    under the driver number. ``len()``, iteration and ``in`` answer from the
    session's driver list without fetching anything.
    """

    def __init__(self, session: TelemetryProvider):
        super().__init__()
        self.session = session

    def _entries(self) -> list[tuple[str, str, str]]:
        """``(key, car number, abbreviation)`` per driver of the session.

        Car numbers a ``drivers.json`` lacks are resolved first (the session
        backfills them from Ergast or the laps), so the keys never depend on
        what was read before. A car two drivers shared is keyed
        ``number/Abbreviation`` for every driver but its first, as
        ``session.results`` labels it.
        """
        rows = [d for d in self.session._drivers_data if isinstance(d, dict)]
        if any(not str(d.get("dn") or "").strip() for d in rows):
            resolve = getattr(self.session, "_driver_rows_info", None)
            if callable(resolve):
                resolve()
        pairs = [(str(d.get("dn") or "").strip(), str(d.get("driver") or "").strip()) for d in rows]
        labels = unique_driver_labels([(number or code, code) for number, code in pairs])
        return [(label, number, code) for label, (number, code) in zip(labels, pairs, strict=True)]

    def _driver_entry(self, key: Any) -> tuple[str, str] | None:
        """``(key, abbreviation)`` of ``key`` (car number, code or shared-car label)."""
        wanted = str(key).strip()
        code_hint = wanted.partition("/")[2].strip() if "/" in wanted else ""
        for label, number, code in self._entries():
            if wanted in (label, code) or (code_hint and code_hint == code):
                return label, code
            if wanted == number and label == number:  # a shared car's first driver
                return label, code
        return None

    def __getitem__(self, key: Any):
        entry = self._driver_entry(key)
        if entry is None:
            raise KeyError(key)
        number, code = entry
        if not dict.__contains__(self, number) and getattr(self.session, "_ergast_only", False):
            # No telemetry exists before F1_API_FIRST_YEAR (nor, before 1996,
            # any laps table to slice): fastf1's empty frame, nothing loaded.
            empty = cast(Any, self.session)._empty_session_telemetry(code)
            dict.__setitem__(self, number, empty)
        if not dict.__contains__(self, number):
            laps = cast(Any, self.session.laps)
            if hasattr(laps, "pick_driver"):
                driver_laps = laps.pick_driver(code)
            elif isinstance(laps, pd.DataFrame):
                driver_laps = laps[laps["Driver"] == code]
            else:
                import polars as pl

                driver_laps = laps.filter(pl.col("Driver") == code)
            # fastf1's session-long ``car_data[n]``: ``Time`` is ``Date - t0_date``,
            # i.e. ``SessionTime`` (the laps' frame runs from its first lap start).
            telemetry = driver_laps.telemetry
            session_clock = getattr(telemetry, "_session_time_as_time", None)
            if callable(session_clock):
                telemetry = session_clock()
            dict.__setitem__(self, number, telemetry)
        return dict.__getitem__(self, number)

    def get(self, key: Any, default: Any = None):
        try:
            return self[key]
        except KeyError:
            return default

    def _numbers(self) -> list[str]:
        return [label for label, _number, _code in self._entries()]

    def __contains__(self, key: object) -> bool:
        return self._driver_entry(key) is not None

    def __iter__(self):
        return iter(self._numbers())

    def __len__(self) -> int:
        return len(self._numbers())

    def keys(self):
        return self._numbers()

    def values(self):
        return [self[number] for number in self._numbers()]

    def items(self):
        return [(number, self[number]) for number in self._numbers()]

    def __repr__(self) -> str:
        return f"LazyTelemetryDict(drivers={self._numbers()!r})"


# -- per-row Lap construction (Pergamon Q7) -----------------------------------
#
# ``Lap(values, index=..., name=...)`` runs the generic Series constructor
# (~8 us) and ``Laps.iloc[i]`` resolves the interleaved dtype of every block
# through ``find_common_type`` on each call (~33 us for the 17-block laps
# frame) before wrapping the row twice (``Series._from_mgr`` then
# ``Lap(series)``). Both paths below run the same pandas steps (the
# constructor's ``sanitize_array`` + ``SingleBlockManager.from_array``, and
# ``fast_xs``'s per-block ``iget`` loop) without the generic overhead. The
# private API is probed once at import; if it drifted the public paths are
# used unchanged.
try:  # pragma: no cover - import-time probe of pandas internals
    # Modules typed Any: the pandas stubs do not describe these internals.
    _construction: Any = importlib.import_module("pandas.core.construction")
    _sanitize_array: Any = _construction.sanitize_array
    _internals: Any = importlib.import_module("pandas.core.internals")
    _SingleBlockManager: Any = _internals.SingleBlockManager
    _blocks: Any = importlib.import_module("pandas.core.internals.blocks")
    _new_block: Any = _blocks.new_block
    _ExtensionBlock: Any = _blocks.ExtensionBlock
    _libs_lib: Any = importlib.import_module("pandas._libs.lib")
    _maybe_convert_objects: Any = _libs_lib.maybe_convert_objects
    _interleaved_dtype: Any = importlib.import_module(
        "pandas.core.internals.managers"
    ).interleaved_dtype
    _BlockPlacement: Any = importlib.import_module("pandas._libs.internals").BlockPlacement
    _FAST_LAP_ROWS = callable(getattr(pd.Series, "_from_mgr", None))
    _DTYPE_IF_ALL_NAT = np.dtype("M8[s]")
except (ImportError, AttributeError):  # pragma: no cover - pandas internals changed
    _FAST_LAP_ROWS = False

# interleaved dtype per tuple of block dtype objects, keyed by their ids. The
# entry keeps the dtype objects alive, so an id in a live key cannot be
# reused by another object: a key hit is the same dtypes, and
# ``interleaved_dtype`` is a pure function of them.
_ROW_DTYPE_CACHE: dict[tuple[int, ...], tuple[tuple[Any, ...], Any]] = {}
_ROW_DTYPE_CACHE_MAX = 256


def _row_interleaved_dtype(dtypes: tuple[Any, ...]) -> Any:
    key = tuple(map(id, dtypes))
    entry = _ROW_DTYPE_CACHE.get(key)
    if entry is None:
        if len(_ROW_DTYPE_CACHE) >= _ROW_DTYPE_CACHE_MAX:
            _ROW_DTYPE_CACHE.clear()
        entry = (dtypes, _interleaved_dtype(list(dtypes)))
        _ROW_DTYPE_CACHE[key] = entry
    return entry[1]


def _lap_from_row(values: np.ndarray, index: pd.Index, name: Any, session: Any) -> "Lap":
    """``Lap(values, index=index, name=name)`` with ``session`` set, for a fresh array.

    ``values`` must be a 1-D object array nobody else holds (the constructor
    would copy it first). The steps are the Series constructor's own:
    ``sanitize_array`` (object-dtype inference included) then
    ``SingleBlockManager.from_array``.
    """
    if not _FAST_LAP_ROWS:
        lap = Lap(values, index=index, name=name)
        lap.session = session
        return lap
    # ``sanitize_array`` on a 1-D object ndarray of the index's length is this
    # inference call plus no-op ndim/str checks; any conversion (a row whose
    # values are all timedeltas, all strings, ...) takes the full function.
    array = _maybe_convert_objects(
        values,
        convert_numeric=False,
        convert_non_numeric=True,
        convert_to_nullable_dtype=False,
        dtype_if_all_nat=_DTYPE_IF_ALL_NAT,
    )
    if array is not values:
        array = _sanitize_array(values, index, None, False)
    mgr = _SingleBlockManager.from_array(array, index)
    lap = cast(Any, Lap)._from_mgr(mgr, axes=mgr.axes)
    object.__setattr__(lap, "_name", name)
    object.__setattr__(lap, "session", session)
    return lap


class _IterLapResult(tuple):
    """Tuple-like result item for ``Laps.iterlaps`` with row-style string access."""

    __slots__ = ()

    def __new__(cls, index: Any, lap: Any):
        return tuple.__new__(cls, (index, lap))

    @property
    def index(self) -> Any:
        return tuple.__getitem__(self, 0)

    @property
    def lap(self) -> Any:
        return tuple.__getitem__(self, 1)

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            return self.lap[key]
        return tuple.__getitem__(self, key)


class Laps(pd.DataFrame):
    """Laps object for accessing lap (timing) data of multiple laps."""

    #: Laps slower than this multiple of the fastest lap are not "quick"
    #: (fastf1's ``pick_quicklaps`` default threshold).
    QUICKLAP_THRESHOLD: ClassVar[float] = 1.07

    _metadata: ClassVar[list[str]] = ["session"]

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow DataFrame-style subclass construction without re-implementing __new__."""
        return super().__new__(cls)

    def __init__(self, data=None, *args, session: TelemetryProvider | None = None, **kwargs):
        cast(Any, super()).__init__(data, *args, **kwargs)
        self.session = session

    @property
    def _constructor(self):
        return Laps

    @property
    def _constructor_sliced(self):
        return Lap

    # Pergamon Q4: pandas hands these hooks a manager freshly built by the
    # operation (take, column get, xs, ...) and wraps it with ``_from_mgr``
    # for a plain DataFrame. The default subclass path instead calls
    # ``Laps(df)`` / ``Lap(ser)``, whose ``__init__`` shallow-copies that
    # manager (~65 us per row selection on a race laps frame, a quarter of
    # every ``pick_*``). Wrapping it directly gives the plain-DataFrame
    # semantics; ``session`` is reset as ``__init__`` would and
    # ``__finalize__`` propagates it from the source as before.
    def _constructor_from_mgr(self, mgr, axes):
        obj = cast(Any, Laps)._from_mgr(mgr, axes=axes)
        object.__setattr__(obj, "session", None)
        return obj

    def _constructor_sliced_from_mgr(self, mgr, axes):
        ser = cast(Any, Lap)._from_mgr(mgr, axes=axes)
        ser._name = None  # caller sets the real name (pandas contract)
        object.__setattr__(ser, "session", None)
        return ser

    def _ixs(self, i: int, axis: int = 0) -> Any:
        """Row ``i`` as a ``Lap`` (``iloc[int]``), same result as pandas' ``_ixs``.

        Pergamon Q7: for a multi-block frame whose rows interleave to object
        dtype (every laps frame) this is ``BlockManager.fast_xs`` with the
        interleaved dtype memoized per block-dtype tuple and the row wrapped
        once (pandas wraps it in a Series, then in a ``Lap``). Every other
        case goes to pandas.
        """
        if axis != 0 or not _FAST_LAP_ROWS:
            return cast(Any, super())._ixs(i, axis)
        mgr = self._mgr
        blocks = mgr.blocks
        if len(blocks) < 2:
            return cast(Any, super())._ixs(i, axis)
        dtype = _row_interleaved_dtype(tuple(blk.dtype for blk in blocks))
        if not (isinstance(dtype, np.dtype) and dtype.kind == "O"):
            return cast(Any, super())._ixs(i, axis)
        columns = mgr.axes[0]
        result = np.empty(len(columns), dtype=object)
        for blk in blocks:
            # ``Block.iget((j, i))`` inlined: ``values[j, i]`` for 2-D blocks,
            # ``values[i]`` for an ExtensionBlock (one column, 1-D values).
            values = blk.values
            if isinstance(blk, _ExtensionBlock):
                for rl in blk.mgr_locs:
                    result[rl] = values[i]
            else:
                for j, rl in enumerate(blk.mgr_locs):
                    result[rl] = values[j, i]
        block = _new_block(result, placement=_BlockPlacement(slice(0, len(result))), ndim=1)
        row_mgr = _SingleBlockManager(block, columns.view())
        lap = cast(Any, Lap)._from_mgr(row_mgr, axes=row_mgr.axes)
        object.__setattr__(lap, "_name", self.index[i])
        object.__setattr__(lap, "session", None)
        return lap.__finalize__(self)

    def pick_driver(self, identifier):
        """Laps of one driver: fastf1's (deprecated) exact match, no case folding.

        A number (int or digit string) matches ``DriverNumber``; anything else
        must equal ``Driver`` as given (``pick_driver("ver")`` matches
        nothing, as in fastf1; :meth:`pick_drivers` upper-cases codes).
        """
        code = self._normalize_driver_identifier(identifier)
        if code.isdigit():
            return self[self._driver_number_mask([code])]
        return self[self["Driver"] == code]

    @staticmethod
    def _normalize_driver_identifier(identifier: Any) -> str:
        if isinstance(identifier, str | int):
            return str(identifier)
        if isinstance(identifier, dict):
            for key in (
                "driver",
                "Driver",
                "Abbreviation",
                "abbreviation",
                "dn",
                "RacingNumber",
            ):
                value = identifier.get(key)
                if value is not None and str(value).strip():
                    return str(value)
        if hasattr(identifier, "driver"):
            value = identifier.driver
            if value is not None and str(value).strip():
                return str(value)
        if hasattr(identifier, "Abbreviation"):
            value = identifier.Abbreviation
            if value is not None and str(value).strip():
                return str(value)
        return str(identifier)

    def pick_drivers(self, identifiers):
        """Laps of the given drivers (fastf1 parity).

        Args:
            identifiers: One identifier or any iterable of them (list, tuple,
                set, pandas/polars Series, numpy array, generator). An
                identifier is a code (upper-cased, as fastf1 does), a car
                number (int or digit string), a mapping or a driver-like
                object (``.driver``/``.Abbreviation``).
        """
        codes = [self._normalize_driver_identifier(i) for i in _identifier_items(identifiers)]
        names = [code.upper() for code in codes if not code.isdigit()]
        numbers = [code for code in codes if code.isdigit()]
        mask = self["Driver"].isin(names).to_numpy(dtype=bool)
        if numbers:
            mask = mask | self._driver_number_mask(numbers)
        return self[mask]

    def _driver_number_mask(self, numbers: list[str]) -> np.ndarray:
        """Rows whose ``DriverNumber`` is one of ``numbers`` (digit strings)."""
        if "DriverNumber" not in self.columns:
            return np.zeros(len(self), dtype=bool)
        column = self["DriverNumber"]
        if pd.api.types.is_numeric_dtype(column):
            column = column.astype("Int64").astype("string")
        elif not column.isin(numbers).any():
            # object column holding ints (hand-built frames): compare as text
            column = column.astype("string")
        return column.isin(numbers).fillna(False).to_numpy(dtype=bool)

    def pick_lap(self, lap_number):
        return self[self["LapNumber"] == lap_number]

    def pick_laps(self, lap_numbers=None, *, laps=None):
        """Laps with the given lap number(s) (fastf1's ``lap_numbers``; ``laps=`` is tif1's alias).

        An int or float, any iterable of them, or a slice (inclusive
        ``start..stop``, ``start`` defaulting to 1).

        Raises:
            ValueError: A lap number is a float that is not a whole number.
        """
        if lap_numbers is None:
            lap_numbers = laps
        if lap_numbers is None:
            raise TypeError("pick_laps() missing required argument: 'lap_numbers'")
        laps = lap_numbers
        if isinstance(laps, slice):
            start = 1 if laps.start is None else laps.start
            stop = laps.stop
            if stop is None:
                return self[self["LapNumber"] >= start]
            return self[(self["LapNumber"] >= start) & (self["LapNumber"] <= stop)]
        numbers = [laps] if isinstance(laps, int | float) else list(laps)
        for number in numbers:
            if isinstance(number, float) and not number.is_integer():
                raise ValueError(f"Invalid value {number} in `lap_numbers`")
        return self[self["LapNumber"].isin(numbers)]

    def pick_team(self, name):
        """Laps of one team (fastf1's exact ``Team == name``)."""
        return self[self["Team"] == name]

    def pick_teams(self, names):
        """Laps of the given teams: a name or any iterable of names, matched exactly."""
        if isinstance(names, str):
            return self[self["Team"] == names]
        return self[self["Team"].isin(list(names))]

    def pick_fastest(self, only_by_time=False):
        """The lap with the lowest ``LapTime`` (fastf1 parity), or ``None``.

        By default only laps marked ``IsPersonalBest`` are candidates (a lap
        that is not a personal best did not count, e.g. a deleted time);
        ``only_by_time=True`` takes every lap. A frame without an
        ``IsPersonalBest`` column falls back to the laps not ``Deleted``. Ties
        go to the first lap by position.
        """
        if self.empty or "LapTime" not in self.columns:
            return None
        lap_times = _timedelta_ns(self["LapTime"])
        usable = lap_times != _NAT_NS
        if not only_by_time:
            if "IsPersonalBest" in self.columns:
                usable &= _true_flags(self["IsPersonalBest"])
            else:
                not_deleted = _not_deleted_mask(self)
                if not_deleted is not None:
                    usable &= not_deleted
        candidates = np.flatnonzero(usable)
        if not len(candidates):
            return None
        fastest = self.iloc[int(candidates[np.argmin(lap_times[candidates])])]
        if isinstance(fastest, Lap):
            fastest.session = self.session
        return fastest

    def pick_quicklaps(self, threshold=None):
        """Laps faster than ``threshold`` times the fastest lap (``None``: ``QUICKLAP_THRESHOLD``)."""
        if threshold is None:
            threshold = self.QUICKLAP_THRESHOLD
        if self.empty:
            return self
        best_time = self["LapTime"].min()
        if pd.isna(best_time):
            return self
        return self[self["LapTime"] < best_time * threshold]

    def pick_tyre(self, compound):
        """Laps on one compound (upper-cased, as fastf1 does)."""
        return self[self["Compound"] == str(compound).upper()]

    def pick_compounds(self, compounds):
        """Laps on the given compounds: a name or any iterable of names, upper-cased (fastf1)."""
        if isinstance(compounds, str):
            return self[self["Compound"] == compounds.upper()]
        return self[self["Compound"].isin([str(compound).upper() for compound in compounds])]

    def pick_track_status(self, status, how="equals"):
        """Laps set under a track status (fastf1's ``how``: equals/contains/excludes/any/none).

        The match runs on the laps' ``TrackStatus`` code string (``"2"`` matches
        ``"267"`` with ``how="contains"``), never on the session's time-based
        ``track_status`` table.
        """
        status = str(status)
        codes = self["TrackStatus"].astype(str)
        if how == "equals":
            return self[codes == status]
        if how == "contains":
            return self[codes.str.contains(status, regex=False, na=False)]
        if how == "excludes":
            return self[~codes.str.contains(status, regex=False, na=False)]
        if how == "any":
            return self[codes.str.contains("|".join(status), regex=True, na=False)]
        if how == "none":
            return self[~codes.str.contains("|".join(status), regex=True, na=False)]
        raise ValueError(f"Invalid value '{how}' for kwarg 'how'")

    def pick_wo_box(self):
        if "PitInTime" not in self.columns or "PitOutTime" not in self.columns:
            return self
        return self[self["PitInTime"].isna() & self["PitOutTime"].isna()]

    def pick_box_laps(self, which="both"):
        if "PitInTime" not in self.columns or "PitOutTime" not in self.columns:
            return self
        if which == "in":
            return self[self["PitInTime"].notna()]
        if which == "out":
            return self[self["PitOutTime"].notna()]
        return self[self["PitInTime"].notna() | self["PitOutTime"].notna()]

    def pick_not_deleted(self):
        if "Deleted" in self.columns:
            return self[~self["Deleted"]]
        return self

    def pick_accurate(self):
        if "IsAccurate" in self.columns:
            return self[self["IsAccurate"]]
        return self

    def get_telemetry(self, *, frequency: int | Literal["original"] | None = None):
        """Merged telemetry of these laps (fastf1 parity).

        Args:
            frequency: ``None`` (``Telemetry.TELEMETRY_FREQUENCY``),
                ``"original"`` (the samples as published) or a sampling rate
                in Hz to resample to.
        """
        return _apply_telemetry_frequency(self.telemetry.add_driver_ahead(), frequency)

    def _padded_car_data(self, kwargs: dict[str, Any]) -> "Telemetry | None":
        """Driver's session-long telemetry sliced to these laps when ``pad`` or edges are asked for.

        ``get_car_data(pad=.., interpolate_edges=..)`` in fastf1 slices
        ``session.car_data[driver]``; tif1 keeps one frame per lap, so a pad
        reaching into the neighbouring laps, or an interpolated edge sample,
        needs the driver's whole timeline. Returns None when neither is
        requested (the per-lap fast path answers) or the session is unknown.
        """
        if (
            not kwargs
            or not (kwargs.get("pad") or kwargs.get("interpolate_edges"))
            or self.session is None
        ):
            return None
        drivers = self["Driver"].dropna().unique()
        if len(drivers) != 1:
            raise ValueError("Cannot retrieve telemetry for multiple drivers.")
        car_data = getattr(self.session, "car_data", None)
        if car_data is None:
            return None
        full = car_data[str(drivers[0])]
        if not isinstance(full, Telemetry) or full.empty:
            return None
        sliced = full.slice_by_lap(
            self,
            pad=kwargs.get("pad", 0),
            pad_side=kwargs.get("pad_side", "both"),
            interpolate_edges=kwargs.get("interpolate_edges", False),
        )
        # fastf1's get_car_data ends with ``reset_index(drop=True)``.
        sliced.index = pd.RangeIndex(len(sliced))
        return sliced

    def get_car_data(self, **kwargs):
        if self.empty:
            return _empty_telemetry(self.session)
        padded = self._padded_car_data(kwargs)
        if padded is not None:
            return padded
        try:
            return self.telemetry
        except ValueError:
            # Multi-driver laps: assemble per-lap payloads with a single merged
            # DataFrame build when possible (mirrors the single-driver path).
            assert self.session is not None  # guaranteed by _telemetry_merged_available
            if self._telemetry_merged_available():
                tel = self._telemetry_merged()
                if tel is not None:
                    return tel
            tels = [lap.telemetry for _, lap in self.iterrows() if hasattr(lap, "telemetry")]
            if not tels:
                return _empty_telemetry(self.session)
            tel = Telemetry(pd.concat(tels, ignore_index=True))
            tel.session = self.session
            return _join_lap_telemetry(_with_session_clock(self.session, tel), multi_driver=True)

    def get_pos_data(self, **kwargs):
        return self.get_car_data(**kwargs)

    def get_weather_data(self):
        """One weather sample per lap, aligned with this frame's index (fastf1 parity).

        fastf1's rule on ``session.weather_data``: the first sample within
        ``[LapStartTime, Time]``, else the last sample at or before the lap's
        ``Time``, else all-missing (a lap without ``LapStartTime`` takes the
        last sample before its end). Columns are ``Time`` (the sample's session
        time) then the seven channels, with the session table's dtypes. Rows
        keep this frame's index and order (fastf1 indexes them by weather
        sample). Without a session weather table the laps' own
        ``WeatherTime`` and channel columns answer; without those every
        sample is missing.
        """
        session_weather = _session_weather_table(self.session)
        if session_weather is not None:
            starts, ends = self._weather_bounds()
            rows = _weather_rows_for_laps(_timedelta_ns(session_weather["Time"]), starts, ends)
            return _weather_sample_frame(session_weather, rows, self.index)
        if all(col in self.columns for col in _LAP_WEATHER_COLUMNS):
            weather = pd.DataFrame(self)[list(_LAP_WEATHER_COLUMNS)]
            weather.columns = pd.Index(_WEATHER_SAMPLE_COLUMNS)
            return weather
        return _missing_weather_frame(self.index)

    def _weather_bounds(self) -> tuple[np.ndarray, np.ndarray]:
        """Each lap's ``(LapStartTime, Time)`` as int64 ns (``NaT``: the int64 minimum).

        A frame without a ``Time`` column ends its laps at
        ``LapStartTime + LapTime``.
        """
        n = len(self)
        if "LapStartTime" in self.columns:
            starts = _timedelta_ns(self["LapStartTime"])
        else:
            starts = np.full(n, _NAT_NS, dtype="int64")
        if "Time" in self.columns:
            ends = _timedelta_ns(self["Time"])
        elif {"LapStartTime", "LapTime"} <= set(self.columns):
            ends = _timedelta_ns(
                Telemetry._coerce_timedelta_series(self["LapStartTime"])
                + Telemetry._coerce_timedelta_series(self["LapTime"])
            )
        else:
            ends = np.full(n, _NAT_NS, dtype="int64")
        return starts, ends

    def split_qualifying_sessions(self):
        """Split qualifying laps into ``[Q1, Q2, Q3]`` (fastf1 parity; ``None`` for an empty one).

        The laps table's ``QualifyingSession`` marker decides when present.
        Otherwise fastf1's algorithm runs on ``session.session_status``: each
        ``Started`` row opens a segment, except the restart after an
        ``Aborted`` (red flag) row, which continues it (``Finished`` also
        clears the suspension); a lap belongs to the segment its
        ``LapStartTime`` falls in, and an out-lap that starts before a split
        and ends after it belongs to the next segment. Without session status
        the two longest gaps of more than five minutes between lap starts
        split the laps.

        Raises:
            ValueError: The laps belong to a session that is not
                qualifying-like (Qualifying, Sprint Qualifying, Sprint
                Shootout).
        """
        _require_qualifying_session(self.session)
        qualifying_sessions = self.get("QualifyingSession")
        if qualifying_sessions is not None:
            normalized = qualifying_sessions.astype("string").str.upper().str.strip()
            session_suffix = normalized.str.extract(r"([123])$", expand=False)
            if bool(session_suffix.notna().any()):
                return [self._bound_subset(session_suffix == suffix) for suffix in ("1", "2", "3")]
        n = len(self)
        if "LapStartTime" in self.columns:
            starts = _timedelta_ns(self["LapStartTime"])
        else:
            starts = np.full(n, _NAT_NS, dtype="int64")
        split_times = _qualifying_split_times(self.session, starts)
        if split_times is None:
            return [self._bound_subset(np.ones(n, dtype=bool)), None, None]
        if "Time" in self.columns:
            ends = _timedelta_ns(self["Time"])
        else:
            ends = np.full(n, _NAT_NS, dtype="int64")
        if "PitOutTime" in self.columns:
            pit_out = self["PitOutTime"].notna().to_numpy(dtype=bool)
        else:
            pit_out = np.zeros(n, dtype=bool)
        return [
            None if mask is None else self._bound_subset(mask)
            for mask in _qualifying_segment_masks(split_times, starts, ends, pit_out)
        ]

    def _bound_subset(self, mask: Any) -> "Laps | None":
        subset = self.loc[np.asarray(mask, dtype=bool)]
        if subset.empty:
            return None
        if isinstance(subset, Laps):
            object.__setattr__(subset, "session", self.session)
        return subset

    @property
    def base_class_view(self):
        """The same data as a plain :class:`pandas.DataFrame` (fastf1 parity)."""
        return pd.DataFrame(self)

    def join(self, *args, **kwargs):
        return cast(Any, super()).join(*args, **kwargs)

    def merge(self, *args, **kwargs):
        return cast(Any, super()).merge(*args, **kwargs)

    @property
    def telemetry(self):
        if self.empty:
            return _empty_telemetry(self.session)
        drivers = self["Driver"].unique()
        if len(drivers) > 1:
            raise ValueError("Cannot retrieve telemetry for multiple drivers.")
        if self._telemetry_merged_available():
            tel = self._telemetry_merged()
            if tel is not None:
                return tel
        tels = []
        for _, lap in self.iterrows():
            tels.append(lap.telemetry)
        if not tels:
            return _empty_telemetry(self.session)
        tel = Telemetry(pd.concat(tels, ignore_index=True))
        tel.session = self.session
        return self._joined_laps(_with_session_clock(self.session, tel), multi_driver=False)

    def _joined_laps(self, tel: "Telemetry", *, multi_driver: bool) -> "Telemetry":
        """These laps' frames back to back as one multi-lap slice (``_join_lap_telemetry``)."""
        start = None if multi_driver else _first_lap_start_ns(self)
        return _join_lap_telemetry(tel, start, multi_driver=multi_driver)

    def _telemetry_merged_available(self) -> bool:
        """Whether the merged-dict telemetry fast path is usable.

        Requires a real Session on the pandas backend so raw payloads can be
        collected (via ``_get_telemetry_payload_for_ref``) without building a
        per-lap DataFrame. Returns False for stand-in objects (e.g. mocks) so
        the legacy per-lap assembly remains the fallback.
        """
        session = self.session
        return (
            session is not None
            and getattr(session, "lib", "pandas") == "pandas"
            and hasattr(session, "_get_telemetry_payload_for_ref")
        )

    def _telemetry_merged(self) -> "Telemetry | None":
        """Assemble all laps' telemetry with a single merged-dict DataFrame build.

        Collects the raw telemetry payload for every row through the session's
        payload getter (mirroring per-lap failure handling) and builds one
        DataFrame instead of one per lap followed by a concat. Returns None when
        the session is unsuitable (e.g. missing a required method), letting
        callers fall back to the legacy per-lap path.  Returns an empty
        ``Telemetry`` when all laps were processed and all failed — failures
        are already counted per-lap inside this method, so returning None here
        would cause double-counting when the caller re-iterates.

        Rhodes R7: references come from vectorized column ops (no
        ``iterrows``) and warm-cache payloads come from ONE batched SQLite
        read instead of one query per lap; still-missing refs keep the
        original per-ref chain (skip-verdicts, network fallback, failure
        recording).
        """
        session = self.session
        assert session is not None  # guaranteed by _telemetry_merged_available
        entries: list[tuple[str, int, dict]] = []
        try:
            ultra_cold = session._resolve_telemetry_ultra_cold_mode(None)
            # Vectorized refs in row order (same extraction the batch fetch
            # path uses); iterrows built one Series per row (~30 ms per
            # 78-lap driver).
            refs_frame = self[["Driver", "LapNumber"]].dropna()
            refs: list[tuple[str, int]] = [
                (str(d), int(n)) for d, n in zip(refs_frame["Driver"], refs_frame["LapNumber"])
            ]

            # Ithome U8: merged result memoization. Slices of Laps do not run
            # __init__, so the cache is read via getattr and stored lazily; the
            # (row-count, refs) key invalidates when the laps frame changes.
            cached = getattr(self, "_merged_telemetry_cache", None)
            if cached is not None and cached[0] == len(refs) and cached[1] == refs:
                return cached[2]

            # Pergamon P10: the driver's row in the frame tier holds the laps
            # back to back, so the merged frame is a slice of it — no per-lap
            # frames, no concat (14 ms for a 76-lap driver otherwise).
            if refs and not ultra_cold and session.enable_cache:
                drivers = {driver for driver, _ in refs}
                if len(drivers) == 1 and session._session_cache_available():
                    merged_driver = next(iter(drivers))
                    merged_frame = get_cache().get_driver_merged_frame(
                        session.year,
                        session.gp,
                        session.session,
                        merged_driver,
                        [lap for _, lap in refs],
                        frame_class=Telemetry,
                        drop_repeats=True,
                    )
                    if merged_frame is not None:
                        tel = (
                            merged_frame
                            if isinstance(merged_frame, Telemetry)
                            else Telemetry(merged_frame)
                        )
                        tel.session = session
                        tel.driver = merged_driver
                        tel = _with_session_clock(session, tel, merged_driver)
                        tel = self._joined_laps(tel, multi_driver=False)
                        object.__setattr__(self, "_merged_telemetry_cache", (len(refs), refs, tel))
                        return tel

            # Ithome U11: when every ref already has an assembled frame
            # (in-process memo or the materialized frame tier), concat them
            # exactly like the legacy per-lap fallback below — skipping the
            # payload merge and the merged-frame rebuild entirely. Falls
            # through unchanged when frames do not cover every ref.
            frame_hits: dict[tuple[str, int], Any] = {}
            missing_frames: list[tuple[str, int]] = []
            for ref in refs:
                memoized_df = session._memo.get("telemetry_df", ref)
                if memoized_df is not None:
                    frame_hits[ref] = memoized_df
                else:
                    missing_frames.append(ref)
            missing_frames_set = set(missing_frames)
            if missing_frames and not ultra_cold and session.enable_cache:
                if session._session_cache_available():
                    tier_hits = get_cache().get_telemetry_frames_batch(
                        session.year,
                        session.gp,
                        session.session,
                        missing_frames,
                        lib="pandas",
                        whole_driver=True,
                        frame_class=Telemetry,
                    )
                    for ref, frame in tier_hits.items():
                        if session._memo.get("telemetry_df", ref) is None:
                            if isinstance(frame, Telemetry):
                                object.__setattr__(frame, "session", session)
                                object.__setattr__(frame, "driver", ref[0])
                            session._memo.set("telemetry_df", ref, frame)
                        if ref in missing_frames_set:
                            frame_hits[ref] = frame
            if refs and len(frame_hits) == len(refs):
                tel = Telemetry(pd.concat([frame_hits[ref] for ref in refs], ignore_index=True))
                tel.session = session
                tel = _with_session_clock(session, tel)
                tel = self._joined_laps(tel, multi_driver=len({d for d, _ in refs}) > 1)
                object.__setattr__(self, "_merged_telemetry_cache", (len(refs), refs, tel))
                return tel

            collected: dict[tuple[str, int], dict] = {}
            for driver, lap_num in refs:
                payload = session._get_telemetry_payload(driver, lap_num)
                if payload is not None:
                    collected[(driver, lap_num)] = payload

            if len(collected) < len(refs) and not ultra_cold and session.enable_cache:
                missing = [ref for ref in refs if ref not in collected]
                batch = get_cache().get_telemetry_batch(
                    session.year, session.gp, session.session, missing
                )
                for ref, payload in batch.items():
                    session._remember_telemetry_payload(ref[0], ref[1], payload)
                    collected[ref] = payload

            # Emit entries in laps-row order (the per-ref chain's historical
            # order) so merged telemetry row order is unchanged.
            missing = [ref for ref in refs if ref not in collected]
            prefetch = getattr(session, "_prefetch_telemetry_payloads", None)
            if len(missing) > 1 and callable(prefetch):
                # One parallel network wave for the laps no tier holds (a cold
                # car_data[n] fetched ~60 laps one after another otherwise);
                # the per-ref chain below still records the failures.
                found = prefetch(missing, ultra_cold=ultra_cold)
                if isinstance(found, list):
                    for driver, lap_num, payload in found:
                        collected[(driver, lap_num)] = payload
                    missing = [ref for ref in refs if ref not in collected]
            for driver, lap_num in missing:
                try:
                    payload = session._get_telemetry_payload_for_ref(
                        driver, lap_num, ultra_cold=ultra_cold, allow_prefetch=False
                    )
                except (
                    DataNotFoundError,
                    InvalidDataError,
                    NetworkError,
                    TypeError,
                    ValueError,
                ) as e:
                    session._record_telemetry_failure(driver, lap_num, e)
                    continue
                if payload is not None:
                    collected[(driver, lap_num)] = payload
            entries = [
                (drv, lap_num, collected[(drv, lap_num)])
                for drv, lap_num in refs
                if (drv, lap_num) in collected
            ]
        except AttributeError as e:
            logger.debug("Merged telemetry path unavailable: %s", e, exc_info=True)
            return None
        if not entries:
            return _empty_telemetry(session)
        try:
            merged = _merge_telemetry_payloads(entries)
            tel = Telemetry(_telemetry_frame_from_merged(merged))
        except Exception:
            # Fall back to the legacy per-lap path on malformed payloads.
            return None
        tel.session = session
        tel = _with_session_clock(session, tel)
        tel = self._joined_laps(tel, multi_driver=len({d for d, _ in refs}) > 1)
        object.__setattr__(self, "_merged_telemetry_cache", (len(refs), refs, tel))
        return tel

    def iterlaps(
        self, require: Iterable[str] | None = None
    ) -> Generator[_IterLapResult, None, None]:
        """Yield ``(index, Lap)`` for the laps in this frame (fastf1 parity).

        Each :class:`Lap` holds every column of the frame, missing values
        included, as fastf1's ``iterrows`` rows do.

        Args:
            require: Columns that must exist and hold a value (not
                NaN/NaT/None) for a lap to be yielded. ``None`` or empty
                yields every lap; a column the frame lacks yields none
                (fastf1).

        Yields:
            ``(label, Lap)`` pairs (``item.index``/``item.lap`` also work).
        """
        required = [] if require is None else list(require)
        columns = self.columns
        if any(column not in columns for column in required):
            return
        skip_rows: np.ndarray | None = None
        if required and len(self):
            # Positions by label match, so a duplicated column name requires
            # each of its columns.
            positions = np.concatenate([np.flatnonzero(columns == column) for column in required])
            skip_rows = self.iloc[:, positions].isna().to_numpy(dtype=bool).any(axis=1)
        # Argos A5 / Pergamon Q7: one object-array interleave for the frame
        # and each Lap built through the Series constructor's own steps
        # (``_lap_from_row``) on the shared, immutable column Index: the same
        # Series as ``iterrows`` gives, several times cheaper.
        row_values = self.to_numpy(dtype=object)
        row_index = self.index
        session = self.session
        for position in range(len(self)):
            if skip_rows is not None and skip_rows[position]:
                continue
            name = row_index[position]
            yield _IterLapResult(
                name, _lap_from_row(row_values[position].copy(), columns, name, session)
            )

    def reset_index(self, drop=False, **kwargs):  # type: ignore[ty:invalid-method-override]
        """Reset index and drop the ``level_0`` column if one was created.

        ``inplace=True`` returns ``None`` and keeps the new column, like pandas
        (``DataFrame.to_sql(index=True)`` resets the index in place and then
        inserts that column).
        """
        result = cast(Any, super()).reset_index(drop=drop, **kwargs)
        if result is None:  # inplace=True: pandas semantics, the new column stays
            return None
        # Remove level_0 column if it was created
        if not drop and "level_0" in result.columns:
            result = result.drop(columns=["level_0"])
        return result


class Lap(pd.Series):
    """Object for accessing lap (timing) data of a single lap."""

    # "_name" is pandas' own Series metadata; dropping it loses the name on
    # every copy (and with it the key names of a Laps.groupby).
    _metadata: ClassVar[list[str]] = ["_name", "session"]
    session: TelemetryProvider | None

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow Series-style subclass construction without re-implementing __new__."""
        return cast(Self, super().__new__(cls))

    def __init__(self, data=None, *args, **kwargs):
        session = kwargs.pop("session", None)
        cast(Any, super()).__init__(data, *args, **kwargs)
        object.__setattr__(self, "session", session)

    @property
    def _constructor(self):
        return Lap

    # Pergamon Q4: same direct wrap as ``Laps._constructor_from_mgr`` (column
    # Series of a Laps are Laps: every ``notna``/``isin``/comparison on them
    # otherwise re-runs ``__init__`` and its shallow manager copy).
    def _constructor_from_mgr(self, mgr, axes):
        ser = cast(Any, Lap)._from_mgr(mgr, axes=axes)
        ser._name = None  # caller sets the real name (pandas contract)
        object.__setattr__(ser, "session", None)
        return ser

    @property
    def driver(self):
        return self.get("Driver")

    @property
    def lap_number(self):
        return self.get("LapNumber")

    @property
    def telemetry(self):
        driver = self.get("Driver")
        lap_num = self.get("LapNumber")
        if driver and lap_num is not None and hasattr(self, "session") and self.session:
            try:
                ultra_cold = self.session._resolve_telemetry_ultra_cold_mode(None)
                return self.session._get_telemetry_df_for_ref(
                    driver, int(lap_num), ultra_cold=ultra_cold, allow_prefetch=False
                )
            except (
                DataNotFoundError,
                InvalidDataError,
                NetworkError,
                TypeError,
                ValueError,
            ) as e:
                self.session._record_telemetry_failure(driver, int(lap_num), e)
                return Telemetry()
        return Telemetry()

    def get_telemetry(self, *, frequency: int | Literal["original"] | None = None):
        """This lap's telemetry (fastf1 parity; see :meth:`Laps.get_telemetry`)."""
        return _apply_telemetry_frequency(self.telemetry.add_driver_ahead(), frequency)

    def _as_laps(self) -> "Laps":
        """This lap as a one-row :class:`Laps` bound to the same session."""
        laps = Laps(pd.DataFrame([self]), session=self.session)
        return laps

    def get_car_data(self, **kwargs):
        if kwargs.get("pad") and self.session is not None:
            padded = self._as_laps()._padded_car_data(kwargs)
            if padded is not None:
                return padded
        return self.telemetry

    def get_pos_data(self, **kwargs):
        return self.get_car_data(**kwargs)

    def get_weather_data(self):
        """The weather sample of this lap as a Series (fastf1 parity).

        ``Time`` then the seven channels, named after the lap. fastf1's rule
        on ``session.weather_data``: the first sample within
        ``[LapStartTime, Time]``, else the last one at or before the lap's
        ``Time``; with neither, a Series of NaN over the same keys. Without a
        session weather table the lap's own ``WeatherTime`` and channel
        values answer.
        """
        keys = list(_WEATHER_SAMPLE_COLUMNS)
        session_weather = _session_weather_table(self.session)
        if session_weather is not None:
            start = Telemetry._coerce_timedelta(self.get("LapStartTime"))
            if "Time" in self.index or "LapTime" not in self.index:
                end = Telemetry._coerce_timedelta(self.get("Time"))
            else:
                end = start + Telemetry._coerce_timedelta(self.get("LapTime"))
            row = _weather_rows_for_laps(
                _timedelta_ns(session_weather["Time"]),
                np.array([start.value], dtype="int64"),
                np.array([end.value], dtype="int64"),
            )[0]
            if row >= 0:
                sample = session_weather.iloc[int(row)]
                values = [sample[key] if key in sample.index else np.nan for key in keys]
                return pd.Series(values, index=keys, name=self.name, dtype=object)
        elif all(col in self.index for col in _LAP_WEATHER_COLUMNS):
            values = [self[col] for col in _LAP_WEATHER_COLUMNS]
            return pd.Series(values, index=keys, name=self.name, dtype=object)
        return pd.Series(np.nan, index=keys, name=self.name, dtype="float64")

    def _fetch_telemetry(self, *, ultra_cold: bool = False) -> dict:
        """Fetch telemetry data (raises DataNotFoundError if not found)."""
        assert self.session is not None  # guaranteed by the telemetry property guard
        tel_path = f"{self.driver}/{int(self.lap_number)}_tel.json"
        tel_data = (
            self.session._fetch_json_unvalidated(tel_path)
            if ultra_cold
            else self.session._fetch_json(tel_path)
        )
        tel = tel_data.get("tel", {})
        if not isinstance(tel, dict):
            tel = {}
        self.session._remember_telemetry_payload(self.driver, self.lap_number, tel)

        if self.session.enable_cache:
            if ultra_cold and tel and self.session._should_backfill_ultra_cold_cache(True):
                self.session._schedule_background_cache_fill(
                    telemetry_payload=(self.driver, self.lap_number, tel)
                )
            elif not ultra_cold:
                get_cache().set_telemetry(
                    self.session.year,
                    self.session.gp,
                    self.session.session,
                    self.driver,
                    self.lap_number,
                    tel,
                )
                self.session._mark_session_cache_populated()
        return tel


# Offset aliases pandas 3 removed, as FastF1-era code still spells them
# (``resample_channels("1S")``): upper-case second/milli/minute/hour.
_LEGACY_OFFSET_ALIASES = {"S": "s", "L": "ms", "T": "min", "H": "h"}
_LEGACY_OFFSET_RE = re.compile(r"^\s*(\d*\.?\d*)\s*([SLTH])\s*$")


def _modern_offset_alias(rule: str) -> str:
    """Translate a pandas<3 upper-case offset alias (``"1S"``) to its current form."""
    if not isinstance(rule, str):
        return rule
    match = _LEGACY_OFFSET_RE.match(rule)
    if match is None:
        return rule
    return f"{match.group(1)}{_LEGACY_OFFSET_ALIASES[match.group(2)]}"


def _frequency_rule(frequency: float) -> str:
    """The pandas offset alias of a sampling rate in Hz (``10`` -> ``"100ms"``)."""
    period_ns = round(1_000_000_000 / float(frequency))
    if period_ns % 1_000_000 == 0:
        return f"{period_ns // 1_000_000}ms"
    return f"{period_ns}ns"


def _apply_telemetry_frequency(tel: Any, frequency: Any) -> Any:
    """Resample ``tel`` to ``frequency`` (fastf1's ``get_telemetry(frequency=)`` contract).

    ``None`` means :attr:`Telemetry.TELEMETRY_FREQUENCY`, ``"original"``
    returns the frame as built, an int/float is a sampling rate in Hz.
    """
    if frequency is None:
        frequency = Telemetry.TELEMETRY_FREQUENCY
    if frequency == "original" or tel is None or getattr(tel, "empty", True):
        return tel
    if (
        isinstance(frequency, bool)
        or not isinstance(frequency, int | float | np.integer | np.floating)
        or frequency <= 0
    ):
        raise ValueError(
            f"frequency must be 'original' or a positive sampling rate in Hz, got {frequency!r}"
        )
    return tel.resample_channels(rule=_frequency_rule(float(frequency)))


#: dtypes of an empty session telemetry frame, in fastf1's column order (the
#: polars twin is ``models_polars.TELEMETRY_SCHEMA``).
_EMPTY_TELEMETRY_DTYPES: dict[str, str] = {
    "Date": "datetime64[ns]",
    "SessionTime": "timedelta64[ns]",
    "DriverAhead": "object",
    "DistanceToDriverAhead": "float64",
    "Time": "timedelta64[ns]",
    "RPM": "float64",
    "Speed": "float64",
    "nGear": "Int64",
    "Throttle": "float64",
    "Brake": "bool",
    "DRS": "Int64",
    "Source": "object",
    "Distance": "float64",
    "RelativeDistance": "float64",
    "Status": "object",
    "X": "float64",
    "Y": "float64",
    "Z": "float64",
    "AccelerationX": "float64",
    "AccelerationY": "float64",
    "AccelerationZ": "float64",
    "Driver": "object",
    "LapNumber": "Int64",
}


def _empty_telemetry(session: Any = None, driver: str | None = None) -> "Telemetry":
    """A session's empty telemetry: no samples, fastf1's columns and dtypes."""
    frame = pd.DataFrame(
        {col: pd.Series(dtype=dtype) for col, dtype in _EMPTY_TELEMETRY_DTYPES.items()}
    )
    tel = Telemetry(frame, session=session, driver=driver)
    return tel


#: Clock columns, in the order the resampler picks its axis from (fastf1
#: resamples on ``Date``; ``SessionTime`` and ``Time`` stand in without it).
_RESAMPLE_AXIS_COLUMNS = ("Date", "SessionTime", "Time")
_NAT_NS = np.iinfo("int64").min

#: Telemetry columns that are metadata, not channels: never dropped by
#: ``drop_unknown_channels`` and carried through a resampling unchanged.
_TELEMETRY_META_COLUMNS = ("Driver", "LapNumber")


def _with_session_clock(session: Any, tel: Any, driver: str | None = None) -> Any:
    """Give ``tel`` its ``Date``/``SessionTime`` columns through the session (if it can)."""
    attach = getattr(session, "_attach_session_clock", None)
    if not callable(attach):
        return tel
    return attach(tel, driver)


def _clock_ns_values(values: pd.Series) -> np.ndarray:
    """A clock column (datetime, timedelta or numeric seconds) as int64 ns; NaT is int64 min."""
    if pd.api.types.is_datetime64_any_dtype(values.dtype):
        if getattr(values.dt, "tz", None) is not None:
            values = values.dt.tz_localize(None)
        return values.astype("datetime64[ns]").to_numpy().view("i8")
    if values.dtype == object:
        parsed = pd.to_datetime(values, errors="coerce")
        if parsed.notna().any():
            return _clock_ns_values(parsed)
    return _timedelta_ns(values)


def _fill_rule(channels: dict[str, dict[str, str | None]], col: Any, dtype: Any) -> str | None:
    """How ``fill_missing`` fills ``col``: ``"interpolate"``, ``"hold"``, ``"source"`` or None.

    fastf1's rule for registered channels (continuous interpolated, discrete
    forward/backward filled, excluded untouched, ``Source`` gaps marked
    ``"interpolation"``); unregistered float columns are interpolated and
    integer/boolean ones held. The polars class applies the same rule.
    """
    if col == "Source":
        return "source"
    kind = channels.get(col, {}).get("type")
    if kind == "excluded":
        return None
    if kind == "discrete" or col in _TELEMETRY_META_COLUMNS:
        return "hold"
    if pd.api.types.is_bool_dtype(dtype) or pd.api.types.is_integer_dtype(dtype):
        return "hold"
    if pd.api.types.is_float_dtype(dtype):
        return "interpolate"
    return None


def _edge_linear(
    t: int, prev_ref: int | None, prev_value: float, next_ref: int | None, next_value: float
) -> float:
    """A continuous channel at ``t`` ns: linear between the samples around it.

    The sample present (or not NaN) alone beyond the data, NaN with
    neither. ``models_polars`` has the same function: both classes compute
    an edge sample with these Python-float operations, in this order.
    """
    if prev_ref is None or math.isnan(prev_value):
        return math.nan if next_ref is None else next_value
    if next_ref is None or math.isnan(next_value):
        return prev_value
    return prev_value + (next_value - prev_value) * ((t - prev_ref) / (next_ref - prev_ref))


def _float_at(values: Any, position: int) -> float:
    """``values[position]`` (an array of a numeric column) as a float; NaN when missing."""
    if position < 0:
        return math.nan
    value = values[position]
    return math.nan if value is None or value is pd.NA else float(value)


def _clock_order(ns: np.ndarray, *, drop_missing: bool = False) -> np.ndarray | None:
    """Stable ascending order of int64-ns clock values (None when already in order).

    Missing values (NaT) go last, or are left out with ``drop_missing``.
    """
    missing = ns == _NAT_NS
    if not missing.any() and not (np.diff(ns) < 0).any():
        return None
    if drop_missing:
        kept = np.flatnonzero(~missing)
        return kept[np.argsort(ns[kept], kind="stable")]
    return np.argsort(np.where(missing, np.iinfo("int64").max, ns), kind="stable")


def _nearest_positions(left: np.ndarray, right: np.ndarray) -> np.ndarray:
    """Position in ``right`` (sorted, no NaT) of the value nearest each ``left`` value.

    A tie goes to the earlier value, an exact match to the last of equal
    values (pandas ``merge_asof(direction="nearest")``); -1 for a missing
    ``left`` value or an empty ``right``.
    """
    out = np.full(len(left), -1, dtype="int64")
    n = len(right)
    if n == 0 or not len(left):
        return out
    valid = left != _NAT_NS
    x = left[valid]
    after = np.searchsorted(right, x, side="right")
    before = after - 1
    nearest_after = np.minimum(after, n - 1)
    gap_before = x - right[np.maximum(before, 0)]
    gap_after = right[nearest_after] - x
    take_after = (after < n) & ((before < 0) | (gap_after < gap_before))
    out[valid] = np.where(take_after, nearest_after, before)
    return out


def _first_lap_start_ns(laps: Any) -> int | None:
    """Earliest ``LapStartTime`` of a laps frame as int64 ns (None when unknown)."""
    if not isinstance(laps, pd.DataFrame) or "LapStartTime" not in laps.columns or laps.empty:
        return None
    starts = _timedelta_ns(cast(pd.Series, laps["LapStartTime"]))
    starts = starts[starts != _NAT_NS]
    return int(starts.min()) if len(starts) else None


def _join_lap_telemetry(
    tel: "Telemetry", lap_start_ns: int | None = None, *, multi_driver: bool = False
) -> "Telemetry":
    """Per-lap frames concatenated into one frame, made a multi-lap slice as fastf1 has it.

    Each lap frame repeats the sample at its lap boundary (the last sample of
    lap N is the first of lap N + 1): the repeat is dropped, the first kept.
    One driver's lap blocks are put in session-clock order, and ``Time``
    becomes continuous: ``SessionTime`` minus the first lap's
    ``LapStartTime`` (``lap_start_ns``, else the earliest lap start the
    frame implies), fastf1's ``slice_by_lap`` clock. With ``multi_driver``
    each driver keeps its own block order and ``Time`` origin. A frame of
    one lap, or without a session clock, is returned unchanged.
    """
    n = len(tel)
    columns = tel.columns
    if n < 2 or "LapNumber" not in columns or not columns.is_unique:
        return tel
    clock_col = "Date" if "Date" in columns else "SessionTime" if "SessionTime" in columns else None
    if clock_col is None:
        return tel
    lap_numbers = tel["LapNumber"]
    if not pd.api.types.is_numeric_dtype(lap_numbers.dtype):
        lap_numbers = pd.to_numeric(lap_numbers, errors="coerce")
    lap = lap_numbers.to_numpy(dtype="float64", na_value=np.nan)
    if not (lap[1:] != lap[:-1]).any():
        return tel
    clock = _clock_ns_values(tel[clock_col])
    order: np.ndarray | None = None
    if not multi_driver and not (clock == _NAT_NS).any() and (np.diff(clock) < 0).any():
        order = np.lexsort((lap, clock))
        clock = clock[order]
        lap = lap[order]
    same = np.zeros(n, dtype=bool)
    same[1:] = (lap[1:] != lap[:-1]) & (clock[1:] == clock[:-1]) & (clock[1:] != _NAT_NS)
    drivers = None
    if multi_driver and "Driver" in columns:
        drivers = tel["Driver"].to_numpy(dtype=object)
        same[1:] &= drivers[1:] == drivers[:-1]
    keep = np.flatnonzero(~same) if same.any() else None
    if order is not None:
        keep = order if keep is None else order[keep]
    new_time = None
    if "SessionTime" in columns and "Time" in columns:
        session_time = _timedelta_ns(tel["SessionTime"])
        lap_time = _timedelta_ns(tel["Time"])
        known = (session_time != _NAT_NS) & (lap_time != _NAT_NS)
        implied = session_time - lap_time  # each row's LapStartTime
        origin: Any = None
        if drivers is not None:
            codes, _ = pd.factorize(drivers)
            rows = known & (codes >= 0)
            if rows.any():
                origins = np.full(int(codes.max()) + 1, np.iinfo("int64").max, dtype="int64")
                np.minimum.at(origins, codes[rows], implied[rows])
                origin = origins[np.maximum(codes, 0)]
        elif lap_start_ns is not None:
            origin = lap_start_ns
        elif known.any():
            origin = int(implied[known].min())
        if origin is not None:
            if keep is not None:
                session_time = session_time[keep]
                if isinstance(origin, np.ndarray):
                    origin = origin[keep]
            new_time = session_time - origin
            unknown = session_time == _NAT_NS
            if unknown.any():
                new_time[unknown] = _NAT_NS
    out = tel if keep is None else tel._wrap(tel.take(keep))
    if new_time is not None:
        if out is tel:
            out = tel._wrap(tel.copy(deep=False))
        time_dtype = tel["Time"].dtype
        values = new_time.view("m8[ns]")
        if (
            isinstance(time_dtype, np.dtype)
            and time_dtype.kind == "m"
            and time_dtype != values.dtype
        ):
            values = values.astype(time_dtype)
        out["Time"] = values
    if keep is not None:
        out.index = pd.RangeIndex(len(out))
    return out


class Telemetry(pd.DataFrame):
    """Multi-channel time series telemetry data."""

    #: Sampling policy of merged/resampled telemetry (fastf1 parity; tif1
    #: telemetry arrives merged at the source, so ``"original"`` is the only
    #: value with an effect).
    TELEMETRY_FREQUENCY: ClassVar[str | int] = "original"
    #: Channel registry (fastf1's table, plus tif1's acceleration channels):
    #: name -> ``{"type": "continuous" | "discrete" | "excluded", "method": ...}``.
    #: ``fill_missing``, ``resample_channels`` and
    #: ``slice_by_time(interpolate_edges=True)`` interpolate continuous
    #: channels (linearly), hold discrete ones and leave excluded ones alone.
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

    _metadata: ClassVar[list[str]] = ["session", "driver"]

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

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow DataFrame-style subclass construction without re-implementing __new__."""
        return super().__new__(cls)

    def __init__(
        self,
        data=None,
        *args,
        session: TelemetryProvider | None = None,
        driver: str | None = None,
        drop_unknown_channels: bool = False,
        **kwargs,
    ):
        if drop_unknown_channels and data is not None:
            frame = pd.DataFrame(data, *args, **kwargs)
            known = [
                col
                for col in frame.columns
                if col in self._CHANNELS or col in _TELEMETRY_META_COLUMNS
            ]
            data, args, kwargs = frame[known], (), {}
        cast(Any, super()).__init__(data, *args, **kwargs)
        self.session = session
        self.driver = driver

    @property
    def _constructor(self):
        return Telemetry

    def _constructor_from_mgr(self, mgr, axes):
        # pandas builds every op result as ``DataFrame._from_mgr`` and then
        # calls ``Telemetry(df)``, whose ``__init__`` shallow-copies the
        # manager again (~40-60 us under copy-on-write). The manager is fresh,
        # so adopt it directly; metadata starts as None exactly as
        # ``__init__`` leaves it, and pandas' ``__finalize__`` fills it in.
        if type(self) is not Telemetry:
            return cast(Any, super())._constructor_from_mgr(mgr, axes)
        frame = cast(Any, Telemetry)._from_mgr(mgr, axes=axes)
        object.__setattr__(frame, "session", None)
        object.__setattr__(frame, "driver", None)
        return frame

    def _constructor_sliced_from_mgr(self, mgr, axes):
        # Same for column access: ``Series(ser)`` would shallow-copy a fresh
        # single-block manager just to rewrap it as the same class.
        if self._constructor_sliced is not pd.Series:
            return cast(Any, super())._constructor_sliced_from_mgr(mgr, axes)
        ser = pd.Series._from_mgr(mgr, axes)
        ser._name = None  # caller sets the real name
        return ser

    def _wrap(self, frame: pd.DataFrame):
        if type(frame) is Telemetry and frame is not self:
            # ``frame`` is a fresh result owned by the caller: re-tag it in
            # place instead of ``Telemetry(frame)`` (a shallow manager copy).
            # Mirror what that construction resets: attrs and flags.
            fresh = cast(Any, frame)
            if fresh._attrs:
                fresh._attrs = {}
            if not fresh.flags.allows_duplicate_labels:
                fresh.flags.allows_duplicate_labels = True
            object.__setattr__(fresh, "session", self.session)
            object.__setattr__(fresh, "driver", self.driver)
            return fresh
        wrapped = Telemetry(frame)
        wrapped.session = self.session
        wrapped.driver = self.driver
        return wrapped

    def _resolve_driver_code(self) -> str | None:
        """Best-effort resolve the telemetry's driver code."""
        if isinstance(self.driver, str) and self.driver:
            return self.driver
        if "Driver" not in self.columns:
            return None
        drivers = self["Driver"].dropna().unique()
        if len(drivers) != 1:
            return None
        driver = drivers[0]
        return str(driver) if driver else None

    def _get_lap_numbers(self) -> list[int]:
        """Return sorted lap numbers referenced by this telemetry slice."""
        if "LapNumber" not in self.columns:
            return []
        lap_numbers = (
            pd.to_numeric(self["LapNumber"], errors="coerce").dropna().astype(int).unique().tolist()
        )
        return sorted(lap_numbers)

    def _time_reference_column(self, other: pd.DataFrame | None = None) -> str | None:
        """Return the preferred shared time reference column."""
        candidates = ("SessionTime", "Time")
        for col in candidates:
            if col in self.columns and (other is None or col in other.columns):
                return col
        return None

    @staticmethod
    def _coerce_timedelta(value: Any) -> pd.Timedelta:
        """Coerce scalar time-like values to Timedelta.

        Numeric values are interpreted as seconds for FastF1 compatibility.
        """
        if isinstance(value, pd.Timedelta):
            return value
        if isinstance(value, int | float | np.integer | np.floating):
            return pd.to_timedelta(float(value), unit="s")
        return pd.to_timedelta(value, errors="coerce")

    @staticmethod
    def _coerce_timedelta_series(values: pd.Series) -> pd.Series:
        """Coerce a time-like series to Timedelta.

        Numeric values are interpreted as seconds for FastF1 compatibility.
        """
        if pd.api.types.is_timedelta64_dtype(values):
            return values
        if pd.api.types.is_numeric_dtype(values):
            return _numeric_seconds_to_timedelta(values)
        return pd.to_timedelta(values, errors="coerce")

    @property
    def base_class_view(self):
        """The same data as a plain :class:`pandas.DataFrame` (fastf1 parity: a property)."""
        return pd.DataFrame(self)

    def _session_time_as_time(self) -> "Telemetry":
        """This frame on fastf1's session-long clock: ``Time`` equal to ``SessionTime``.

        fastf1's ``session.car_data[n]``/``pos_data[n]`` carry
        ``Time = Date - t0_date``, which is ``SessionTime``. Frames without
        both columns come back unchanged.
        """
        if "SessionTime" not in self.columns or "Time" not in self.columns:
            return self
        tel = self._wrap(self.copy(deep=False))
        tel["Time"] = self["SessionTime"].copy()
        return tel

    def get_first_non_zero_time_index(self):
        if "Time" not in self.columns or self.empty:
            return 0
        # Argos A6: compare on the int64 view instead of building a filtered
        # Series (NaT is the int64 minimum, so it never compares > 0).
        time_col = self["Time"]
        if time_col.dtype.kind != "m":
            time_col = pd.to_timedelta(time_col, errors="coerce")
        positions = np.flatnonzero(time_col.to_numpy().view("i8") > 0)
        return int(self.index[positions[0]]) if len(positions) else 0

    def _owned_copy(self):
        """Copy to modify and return, structured exactly like ``self.copy()``.

        Under copy-on-write a shallow copy is as safe to modify as a deep one;
        a deep copy only differs by consolidating the blocks, so it is kept
        for unconsolidated frames (same block layout as before).
        """
        return self.copy(deep=not self._mgr.is_consolidated())

    def fill_missing(self):
        """Fill missing samples the way fastf1's ``fill_missing`` does (the polars class too).

        Discrete channels (``nGear``, ``DRS``, ``Brake``, ``Status``,
        ``DriverAhead``, registered discrete channels) and ``Driver``/
        ``LapNumber`` take the value in force (forward then backward fill)
        and keep their dtype; ``Source`` gaps become ``"interpolation"``;
        excluded channels (the clocks) are left alone. Other float columns
        are interpolated linearly in both directions, other integer and
        boolean columns are held like discrete ones, anything else is kept.
        Only columns with gaps are touched.
        """
        filled = self._owned_copy()
        for i, (col, dtype) in enumerate(zip(self.columns, self.dtypes, strict=True)):
            rule = _fill_rule(self._CHANNELS, col, dtype)
            if rule is None:
                continue
            values = cast(Any, self)._ixs(i, axis=1)
            if not values.hasnans:
                continue
            if rule == "interpolate":
                values = values.interpolate(limit_direction="both")
            elif rule == "hold":
                values = values.ffill().bfill()
            else:
                values = values.fillna("interpolation")
            filled.isetitem(i, values)
        return self._wrap(filled)

    def _differential_distance_fast(self) -> np.ndarray | None:
        """``speed_ms * dt`` as numpy for float64 ``Speed`` + numpy timedelta ``Time``.

        Same arithmetic as the pandas chain in
        :meth:`calculate_differential_distance` (``asi8 / pps`` seconds,
        NaN/NaT -> 0, ``diff`` clipped at 0), without its ~10 intermediate
        Series. Returns None when the dtypes need the general path.
        """
        if not self.columns.is_unique:
            return None
        speed = self["Speed"]._values
        time = self["Time"]._values
        if not isinstance(speed, np.ndarray) or speed.dtype != np.float64:
            return None
        time_dtype = time.dtype
        if not isinstance(time_dtype, np.dtype) or time_dtype.kind != "m":
            return None
        unit, count = np.datetime_data(time_dtype)
        pps = {"s": 1, "ms": 10**3, "us": 10**6, "ns": 10**9}.get(unit)
        if pps is None or count != 1:
            return None
        speed_ms = np.where(np.isnan(speed), 0.0, speed) / 3.6
        time_raw = np.asarray(time)
        seconds = time_raw.view("i8") / pps
        seconds[np.isnat(time_raw)] = 0.0
        dt = np.empty_like(seconds)
        if len(dt):
            dt[0] = 0.0
            np.subtract(seconds[1:], seconds[:-1], out=dt[1:])
        dt = np.where(dt >= 0.0, dt, 0.0)
        return speed_ms * dt

    def integrate_distance(self):
        if self.empty or "Speed" not in self.columns:
            return pd.Series(dtype=float)
        if "Time" in self.columns:
            step = self._differential_distance_fast()
            if step is not None and not np.isnan(step).any():
                return pd.Series(np.cumsum(step), index=self.index, copy=False)
        speed_ms = pd.to_numeric(self["Speed"], errors="coerce").fillna(0.0) / 3.6
        if "Time" in self.columns:
            t = pd.to_timedelta(self["Time"], errors="coerce").dt.total_seconds().fillna(0.0)
            dt = t.diff().fillna(0.0).clip(lower=0.0)
        else:
            dt = pd.Series(0.0, index=self.index)
        return (speed_ms * dt).cumsum()

    def calculate_differential_distance(self):
        if self.empty or "Speed" not in self.columns:
            return pd.Series(dtype=float)
        if "Time" in self.columns:
            step = self._differential_distance_fast()
            if step is not None:
                return pd.Series(step, index=self.index, copy=False)
        speed_ms = pd.to_numeric(self["Speed"], errors="coerce").fillna(0.0) / 3.6
        if "Time" in self.columns:
            t = pd.to_timedelta(self["Time"], errors="coerce").dt.total_seconds().fillna(0.0)
            dt = t.diff().fillna(0.0).clip(lower=0.0)
            return speed_ms * dt
        return pd.Series(0.0, index=self.index)

    def add_differential_distance(self, drop_existing: bool = True):
        if "DifferentialDistance" in self.columns and not drop_existing:
            return self
        tel = self._owned_copy()
        tel["DifferentialDistance"] = self.calculate_differential_distance()
        return self._wrap(tel)

    def _with_float_column(self, name: str, values: np.ndarray):
        """A copy with ``name`` set to ``values`` (in place when present, else appended)."""
        tel = self._owned_copy()
        tel[name] = values
        return self._wrap(tel)

    def add_distance(self, drop_existing: bool = True):
        """Add ``Distance``: metres driven since the first sample (fastf1 parity).

        Integrated from ``Speed`` over the whole frame
        (:meth:`integrate_distance`). An existing ``Distance`` (tif1 frames
        carry the source's per-lap channel) is replaced in place with
        ``drop_existing=True`` and kept, the frame returned unchanged, with
        ``False``. As in fastf1, integrate a few laps at a time: the error
        grows with the length of the slice.
        """
        if "Distance" in self.columns and not drop_existing:
            return self
        distance = self.integrate_distance().to_numpy(dtype="float64")
        if len(distance) != len(self):  # no Speed channel
            distance = np.full(len(self), np.nan)
        return self._with_float_column("Distance", distance)

    def add_relative_distance(self, drop_existing: bool = True):
        """Add ``RelativeDistance``: ``Distance`` over its last value (fastf1 parity).

        ``0.0`` is the first sample and ``1.0`` the last. The frame's
        ``Distance`` is used, integrated from ``Speed`` when the column is
        missing (no ``Distance`` column is added). An existing
        ``RelativeDistance`` is recomputed with ``drop_existing=True`` and
        kept, the frame returned unchanged, with ``False``.
        """
        if "RelativeDistance" in self.columns and not drop_existing:
            return self
        if "Distance" in self.columns:
            distance = pd.to_numeric(self["Distance"], errors="coerce")
            values = distance.to_numpy(dtype="float64", na_value=np.nan)
        else:
            values = self.integrate_distance().to_numpy(dtype="float64")
        if len(values) != len(self):  # no Speed channel
            values = np.full(len(self), np.nan)
        if len(values):
            with np.errstate(divide="ignore", invalid="ignore"):
                values = values / values[-1]
        return self._with_float_column("RelativeDistance", values)

    def calculate_driver_ahead(self, return_reference: bool = False):
        if "DriverAhead" in self.columns and "DistanceToDriverAhead" in self.columns:
            driver_ahead = self["DriverAhead"].to_numpy(copy=True)
            distance = self["DistanceToDriverAhead"]
            if distance.dtype != np.float64:  # already numeric: no to_numeric pass
                distance = pd.to_numeric(distance, errors="coerce")
            distance_to_driver_ahead = distance.to_numpy(copy=True)
            if return_reference:
                return driver_ahead, distance_to_driver_ahead, self
            return driver_ahead, distance_to_driver_ahead

        driver_ahead = np.full(len(self), None, dtype=object)
        distance_to_driver_ahead = np.full(len(self), math.nan, dtype=float)
        if return_reference:
            return driver_ahead, distance_to_driver_ahead, self
        return driver_ahead, distance_to_driver_ahead

    def add_driver_ahead(self, drop_existing: bool = True):  # noqa: ARG002
        """Add ``DriverAhead``/``DistanceToDriverAhead`` (kept when already present).

        ``drop_existing`` is accepted for fastf1 parity. The source computes
        both channels from every car's position; a frame of one driver cannot
        recompute them, so existing channels are kept and missing ones are
        added empty (``None``/NaN).
        """
        has_existing = "DriverAhead" in self.columns and "DistanceToDriverAhead" in self.columns
        if has_existing:
            return self

        driver_ahead, distance_to_driver_ahead = self.calculate_driver_ahead(return_reference=False)

        new_cols = pd.DataFrame(
            {
                "DriverAhead": pd.Series(driver_ahead, index=self.index),
                "DistanceToDriverAhead": pd.Series(
                    distance_to_driver_ahead, index=self.index, dtype=float
                ),
            }
        )
        return self._wrap(pd.DataFrame(self).join(new_cols, how="outer"))

    def add_track_status(self, drop_existing: bool = True):
        """Add ``TrackStatus``: the session's track status at each sample (fastf1 parity).

        Each sample takes the status of the last ``session.track_status`` row
        at or before its ``SessionTime`` (``Time`` when there is no session
        clock). Without a session or a status table every sample is ``"1"``.
        """
        if "TrackStatus" in self.columns and not drop_existing:
            return self
        tel = self._owned_copy()
        tel["TrackStatus"] = self._track_status_values()
        return self._wrap(tel)

    def _track_status_values(self) -> np.ndarray:
        statuses = np.full(len(self), "1", dtype=object)
        session = self.session
        table = getattr(session, "track_status", None) if session is not None else None
        ref = self._time_reference_column()
        if (
            ref is None
            or not isinstance(table, pd.DataFrame)
            or table.empty
            or not {"Time", "Status"}.issubset(table.columns)
        ):
            return statuses
        times = _timedelta_ns(cast(pd.Series, table["Time"]))
        order = np.argsort(times, kind="stable")
        times = times[order]
        values = table["Status"].astype(str).to_numpy()[order]
        ref_ns = _timedelta_ns(self[ref])
        positions = np.searchsorted(times, ref_ns, side="right") - 1
        known = positions >= 0
        statuses[known] = values[positions[known]]
        return statuses

    def slice_by_mask(self, mask, pad: int | float = 0, pad_side: str = "both"):
        mask_array = np.asarray(mask, dtype=bool)
        if mask_array.shape[0] != len(self):
            raise ValueError("Mask length must match telemetry length.")

        if pad and mask_array.any():
            # Argos A6: copy only when the mask is actually widened.
            mask_array = mask_array.copy()
            true_indices = np.flatnonzero(mask_array)
            first_idx = int(true_indices[0])
            last_idx = int(true_indices[-1])

            if pad_side in ("both", "before"):
                first_idx = max(0, first_idx - int(pad))
            if pad_side in ("both", "after"):
                last_idx = min(len(mask_array) - 1, last_idx + int(pad))

            mask_array[first_idx : last_idx + 1] = True

        # A boolean take is already a fresh frame under copy-on-write; the
        # explicit .loc[...].copy() doubled the block copies.
        return self._wrap(self[mask_array])

    def slice_by_time(
        self,
        start_time,
        end_time,
        pad: int | float = 0,
        pad_side: str = "both",
        interpolate_edges: bool = False,
    ):
        """Samples between ``start_time`` and ``end_time`` (inclusive; fastf1 parity).

        ``SessionTime`` is the reference (``Time`` without it); the result's
        ``Time`` restarts at ``start_time``. Numbers are seconds. ``pad``
        adds that many samples on ``pad_side``. ``interpolate_edges`` adds one
        sample exactly at ``start_time`` and one at ``end_time`` (unless a
        sample is already there): continuous channels interpolated linearly
        between the neighbouring samples, the clocks advanced to the edge,
        discrete channels and text the value in force, ``Source``
        ``"interpolation"``. The rest of the frame is unchanged. fastf1 fits a
        quadratic spline through ``X``/``Y``/``Z``/``Distance`` instead: on real
        data that differs by ~0.1 m in X/Y (p95 ~1 m) and ~1 cm in Distance,
        below the decimetre noise of the position data; the spline overshoots
        across gaps and glitches and would ring on the per-lap ``Distance``
        reset of a session-long frame, so tif1 keeps the linear value.
        """
        time_ref_col = "SessionTime" if "SessionTime" in self.columns else "Time"
        if time_ref_col not in self.columns:
            return self._wrap(self.copy())

        start = self._coerce_timedelta(start_time)
        end = self._coerce_timedelta(end_time)
        if pd.isna(start) or pd.isna(end):
            return self._wrap(self.iloc[0:0].copy())

        if interpolate_edges and not self.empty:
            data_slice = self._slice_with_edges(
                time_ref_col, int(start.value), int(end.value), int(pad), pad_side
            )
        else:
            ref_time = self._coerce_timedelta_series(self[time_ref_col]).to_numpy()
            start_ns = np.timedelta64(start.to_timedelta64())
            end_ns = np.timedelta64(end.to_timedelta64())
            selection_mask = (ref_time >= start_ns) & (ref_time <= end_ns)
            data_slice = self.slice_by_mask(selection_mask, pad, pad_side)

        if not data_slice.empty:
            # Keep Time zero-based relative to the start of this slice, matching FastF1.
            if time_ref_col in data_slice.columns:
                slice_ref_time = self._coerce_timedelta_series(
                    cast(pd.Series, data_slice[time_ref_col])
                )
                data_slice["Time"] = slice_ref_time - start

        return data_slice

    def _slice_with_edges(self, ref: str, start_ns: int, end_ns: int, pad: int, pad_side: str):
        """``slice_by_time(interpolate_edges=True)`` before ``Time`` is re-based.

        fastf1 merges the two edge samples into the whole frame and fills
        every gap; here the frame is read in ``ref`` order (sorted only when
        it is not), the window and its padding are sliced, and the edge
        samples, computed from their two neighbours, are inserted: no other
        sample changes. The polars class computes the same rows.
        """
        if start_ns > end_ns:
            return self._wrap(self.iloc[0:0].copy())
        ref_ns = _clock_ns_values(self[ref])
        frame = self
        order = _clock_order(ref_ns)
        if order is not None:
            frame = self.take(order)
            ref_ns = ref_ns[order]
        clock = ref_ns[: int(np.count_nonzero(ref_ns != _NAT_NS))]
        n_valid = len(clock)
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
            return self._wrap(frame.iloc[first:last].copy())
        samples = self._edge_samples(frame, clock, ref, [t for _, t in edges])
        pieces = []
        cursor = first
        for k, (position, _) in enumerate(edges):
            pieces.extend((frame.iloc[cursor:position], samples.iloc[k : k + 1]))
            cursor = position
        pieces.append(frame.iloc[cursor:last])
        return self._wrap(pd.concat([p for p in pieces if len(p)], ignore_index=True))

    def _edge_samples(self, frame: pd.DataFrame, clock: np.ndarray, ref: str, times: list[int]):
        """Samples of ``frame`` at ``times`` ns on ``ref`` (none of them a sampled instant).

        Continuous channels: linear between the samples before and after
        (the one present beyond the data). Other clocks: the held sample's
        value advanced by the edge minus its ``ref``. ``Source``:
        ``"interpolation"``. Everything else: the held sample's value (the
        sample before the edge, else the one after). The polars class
        computes the same values (``models_polars._edge_samples``).
        """
        n = len(clock)
        after = [int(np.searchsorted(clock, t, side="right")) for t in times]
        prevs = [a - 1 if a > 0 else -1 for a in after]
        nexts = [a if a < n else -1 for a in after]
        held = [p if p >= 0 else q for p, q in zip(prevs, nexts, strict=True)]
        columns: dict[int, Any] = {}
        for i, (col, dtype) in enumerate(zip(frame.columns, frame.dtypes, strict=True)):
            values = cast(Any, frame)._ixs(i, axis=1)._values
            is_date = isinstance(dtype, np.dtype) and dtype.kind == "M"
            is_delta = isinstance(dtype, np.dtype) and dtype.kind == "m"
            if col == ref and not is_delta:  # a clock in numeric seconds
                columns[i] = pd.array([t / 1e9 for t in times], dtype=dtype)
            elif is_date or is_delta:
                if col == ref:
                    ns = list(times)
                else:
                    base = _clock_ns_values(pd.Series(values.take(held)))
                    ns = [
                        _NAT_NS if b == _NAT_NS else int(b) + t - int(clock[h])
                        for b, t, h in zip(base, times, held, strict=True)
                    ]
                exact = np.array(ns, dtype="int64").view("M8[ns]" if is_date else "m8[ns]")
                columns[i] = exact.astype(dtype)
            elif col == "Source":
                columns[i] = np.array(["interpolation"] * len(times), dtype=object)
            elif self._is_continuous_column(col, dtype):
                interpolated = [
                    _edge_linear(
                        t,
                        int(clock[p]) if p >= 0 else None,
                        _float_at(values, p),
                        int(clock[q]) if q >= 0 else None,
                        _float_at(values, q),
                    )
                    for t, p, q in zip(times, prevs, nexts, strict=True)
                ]
                if pd.api.types.is_integer_dtype(dtype):
                    rounded = [None if math.isnan(v) else round(v) for v in interpolated]
                    columns[i] = pd.array(rounded, dtype=dtype)
                else:
                    columns[i] = pd.array(interpolated, dtype=dtype)
            else:
                columns[i] = values.take(held)
        samples = pd.DataFrame(columns)
        samples.columns = frame.columns
        return samples

    @staticmethod
    def _extract_lap_time_window(ref_laps: Any) -> tuple[Any, Any]:
        """Extract lap start/end timedeltas from Lap/Laps-like objects."""
        start_time: Any = pd.NaT
        end_time: Any = pd.NaT

        if isinstance(ref_laps, pd.DataFrame):
            if ref_laps.empty:
                return start_time, end_time

            if "LapStartTime" in ref_laps.columns:
                start_series = Telemetry._coerce_timedelta_series(
                    cast(pd.Series, ref_laps["LapStartTime"])
                )
                if start_series.notna().any():
                    start_time = cast(pd.Timedelta, start_series.min())

            if "Time" in ref_laps.columns:
                end_series = Telemetry._coerce_timedelta_series(cast(pd.Series, ref_laps["Time"]))
                if end_series.notna().any():
                    end_time = cast(pd.Timedelta, end_series.max())

            if pd.isna(end_time) and {"LapStartTime", "LapTime"}.issubset(ref_laps.columns):
                start_series = Telemetry._coerce_timedelta_series(
                    cast(pd.Series, ref_laps["LapStartTime"])
                )
                lap_time_series = Telemetry._coerce_timedelta_series(
                    cast(pd.Series, ref_laps["LapTime"])
                )
                end_series = start_series + lap_time_series
                if end_series.notna().any():
                    end_time = cast(pd.Timedelta, end_series.max())
            return start_time, end_time

        if isinstance(ref_laps, pd.Series):
            if "LapStartTime" in ref_laps:
                start_time = Telemetry._coerce_timedelta(ref_laps.get("LapStartTime"))
            if "Time" in ref_laps:
                end_time = Telemetry._coerce_timedelta(ref_laps.get("Time"))
            if pd.isna(end_time) and "LapTime" in ref_laps and not pd.isna(start_time):
                end_time = cast(pd.Timedelta, start_time) + Telemetry._coerce_timedelta(
                    ref_laps.get("LapTime")
                )
            return start_time, end_time

        return start_time, end_time

    @staticmethod
    def _extract_lap_numbers(ref_laps: Any) -> list[int]:
        """Extract lap numbers from Lap/Laps-compatible inputs."""
        if isinstance(ref_laps, pd.DataFrame):
            if "LapNumber" in ref_laps.columns:
                return [int(v) for v in ref_laps["LapNumber"].dropna().tolist()]
            if "lap" in ref_laps.columns:
                return [int(v) for v in ref_laps["lap"].dropna().tolist()]
            return []

        if isinstance(ref_laps, pd.Series):
            for col in ("LapNumber", "lap"):
                value = ref_laps.get(col)
                if value is not None and not pd.isna(value):
                    return [int(value)]
            return []

        if isinstance(ref_laps, int | np.integer):
            return [int(ref_laps)]

        return []

    @staticmethod
    def _extract_lap_drivers(ref_laps: Any) -> list[str]:
        """Driver codes referenced by Lap/Laps-compatible inputs (empty if unknown)."""
        if isinstance(ref_laps, pd.DataFrame):
            if "Driver" in ref_laps.columns:
                return [str(v) for v in ref_laps["Driver"].dropna().unique().tolist()]
            return []
        if isinstance(ref_laps, pd.Series):
            value = ref_laps.get("Driver")
            if value is not None and not pd.isna(value):
                return [str(value)]
        return []

    def slice_by_lap(
        self,
        ref_laps,
        pad: int | float = 0,
        pad_side: str = "both",
        interpolate_edges: bool = False,
    ):
        if isinstance(ref_laps, Laps) and len(ref_laps) > 1:
            if "DriverNumber" in ref_laps.columns and len(ref_laps["DriverNumber"].unique()) > 1:
                raise ValueError(
                    "Cannot slice telemetry because 'ref_laps' contains Laps of multiple drivers!"
                )

        # ``Time`` is lap-relative (zero at each lap start), so a lap's
        # session-time window (LapStartTime..Time) selects samples only on the
        # ``SessionTime`` axis every built frame carries; that is the fastf1
        # path below. A frame without ``SessionTime`` (hand-built, or columns
        # dropped) is matched on LapNumber (and Driver when both sides carry it).
        if "SessionTime" not in self.columns and "LapNumber" in self.columns:
            lap_numbers = self._extract_lap_numbers(ref_laps)
            if lap_numbers:
                lap_mask = self["LapNumber"].isin(lap_numbers)
                drivers = self._extract_lap_drivers(ref_laps)
                if drivers and "Driver" in self.columns:
                    lap_mask &= self["Driver"].isin(drivers)
                return self.slice_by_mask(lap_mask.to_numpy(copy=False), pad=pad, pad_side=pad_side)

        start_time, end_time = self._extract_lap_time_window(ref_laps)
        if not pd.isna(start_time) and not pd.isna(end_time):
            return self.slice_by_time(
                start_time,
                end_time,
                pad=pad,
                pad_side=pad_side,
                interpolate_edges=interpolate_edges,
            )

        if "LapNumber" not in self.columns:
            return self._wrap(self.copy())

        lap_numbers = self._extract_lap_numbers(ref_laps)
        if not lap_numbers:
            return self._wrap(self.iloc[0:0].copy())

        lap_mask = self["LapNumber"].isin(lap_numbers).to_numpy(copy=False)
        return self.slice_by_mask(lap_mask, pad=pad, pad_side=pad_side)

    def merge_channels(self, other, frequency: int | Literal["original"] | None = None):
        """Merge the channels of ``other`` into this frame, nearest sample in time.

        The frames are matched on the session clock: ``Date`` when both
        carry it, else ``SessionTime``, else ``Time`` (numbers are seconds).
        Every row of this frame is kept and takes ``other``'s sample nearest
        in time (the earlier one on a tie, the last of equal timestamps on
        an exact match); a row whose clock is missing gets nulls for
        ``other``'s columns. The result is sorted by that clock (missing
        last) with a fresh index. Columns in both frames keep this frame's
        values; ``other``'s copies get an ``_other`` suffix. Without a
        shared clock the frames are put side by side. The polars class
        matches the same rows.

        Args:
            other: Telemetry (or DataFrame) to merge.
            frequency: Resample the merged frame to this rate in Hz;
                ``"original"`` keeps the merged samples, ``None`` uses
                :attr:`TELEMETRY_FREQUENCY`.
        """
        return _apply_telemetry_frequency(self._merge_channels(other), frequency)

    def _merge_channels(self, other):
        right = pd.DataFrame(other)
        key = next(
            (c for c in _RESAMPLE_AXIS_COLUMNS if c in self.columns and c in right.columns), None
        )
        if key is None:
            return self._wrap(pd.concat([self.reset_index(drop=True), right], axis=1))
        left_ns = _clock_ns_values(self[key])
        order = _clock_order(left_ns)
        if order is not None:
            left_ns = left_ns[order]
        right_ns = _clock_ns_values(right[key])
        right_order = _clock_order(right_ns, drop_missing=True)
        if right_order is not None:
            right_ns = right_ns[right_order]
        hits = _nearest_positions(left_ns, right_ns)
        if right_order is not None:
            hits = np.where(hits >= 0, right_order[np.maximum(hits, 0)], -1)
        columns = [col for col in right.columns if col != key]
        part = right[columns]
        part.columns = pd.Index(
            [f"{col}_other" if col in self.columns else col for col in columns], dtype=object
        )
        part.index = pd.RangeIndex(len(part))
        taken = part.reindex(hits) if (hits < 0).any() else part.take(hits)
        taken.index = pd.RangeIndex(len(taken))
        left = (self if order is None else self.take(order)).reset_index(drop=True)
        return self._wrap(pd.concat([left, taken], axis=1))

    def resample_channels(
        self,
        rule: str | None = None,
        new_date_ref: pd.Series | None = None,
        **kwargs: Any,
    ):
        """Resample the telemetry (fastf1 parity; the polars class gives the same rows).

        Either ``rule`` (a pandas offset alias such as ``"100ms"``; tif1
        defaults to ``"1s"`` when neither argument is given) or
        ``new_date_ref`` (timestamps to resample onto, matched against
        ``Date``).

        With ``rule`` the samples are binned every ``rule`` from the first
        sample on the session clock (``Date``, else ``SessionTime``, else
        ``Time``), as fastf1's ``merge_channels(frequency=)`` does:
        continuous channels are the bin mean, interpolated across empty bins;
        discrete channels (gear, DRS, brake, driver ahead, status, lap
        number) and text columns take the value in force at each bin start;
        ``Date``/``SessionTime`` advance with the bins and ``Time`` restarts
        at zero. With ``new_date_ref`` the continuous channels and the clock
        columns are interpolated at each (unique, sorted) timestamp and the
        rest is held.
        """
        if rule is not None and new_date_ref is not None:
            raise ValueError("You can only specify one of 'rule' or 'new_date_ref'")
        if new_date_ref is not None:
            return self._resample_onto_dates(new_date_ref)
        _ = kwargs
        rule = _modern_offset_alias("1s" if rule is None else rule)
        if self.empty:
            return self._wrap(self._owned_copy())
        if not self.columns.is_unique:
            if "Time" not in self.columns:
                return self._wrap(self._owned_copy())
            frame = self.copy()
            frame["Time"] = pd.to_timedelta(frame["Time"], errors="coerce")
            frame = frame.dropna(subset=["Time"]).set_index("Time").sort_index()
            numeric_cols = [c for c in frame.columns if pd.api.types.is_numeric_dtype(frame[c])]
            frame = frame[numeric_cols]
            resampled = frame.resample(rule).mean().interpolate(limit_direction="both")
            return self._wrap(resampled.reset_index())
        step = int(pd.Timedelta(rule).value)
        if step <= 0:
            raise ValueError(f"Resampling rule {rule!r} must be a positive duration")
        axis = next((col for col in _RESAMPLE_AXIS_COLUMNS if col in self.columns), None)
        if axis is None:
            return self._wrap(self._owned_copy())
        axis_ns = self._clock_ns(axis)
        positions = np.flatnonzero(axis_ns != _NAT_NS)
        positions = positions[np.argsort(axis_ns[positions], kind="stable")]
        if not len(positions):
            return self._wrap(self.iloc[0:0].copy())
        x = axis_ns[positions]
        bins = (x - x[0]) // step
        n_bins = int(bins[-1]) + 1
        offsets = np.arange(n_bins, dtype="int64") * step
        held = np.searchsorted(x, x[0] + offsets, side="right") - 1
        out: dict[str, Any] = {}
        for col, dtype in zip(self.columns, self.dtypes, strict=True):
            if col in _RESAMPLE_AXIS_COLUMNS:
                out[col] = self._resampled_clock(col, dtype, positions[0], offsets)
            elif self._is_continuous_column(col, dtype):
                values = self._float_values(col)[positions]
                valid = ~np.isnan(values)
                counts = np.bincount(bins[valid], minlength=n_bins)
                sums = np.bincount(bins[valid], weights=values[valid], minlength=n_bins)
                filled = counts > 0
                means = np.full(n_bins, np.nan)
                if filled.any():
                    means[filled] = sums[filled] / counts[filled]
                    means = np.interp(np.arange(n_bins), np.flatnonzero(filled), means[filled])
                out[col] = means
            else:
                in_force = self[col].iloc[positions].reset_index(drop=True).ffill()
                out[col] = in_force.iloc[held].reset_index(drop=True).bfill()
        return self._wrap(pd.DataFrame(out, columns=self.columns))

    def _clock_ns(self, col: str) -> np.ndarray:
        """``col`` (timedelta, datetime or numeric seconds) as int64 ns; NaT is int64 min."""
        values = self[col]
        if pd.api.types.is_datetime64_any_dtype(values.dtype):
            dates = pd.to_datetime(values, errors="coerce")
            if getattr(dates.dt, "tz", None) is not None:
                dates = dates.dt.tz_localize(None)
            return dates.astype("datetime64[ns]").to_numpy().view("i8")
        return _timedelta_ns(values)

    def _resampled_clock(
        self, col: str, dtype: Any, first_position: int, offsets: np.ndarray
    ) -> np.ndarray:
        """A clock column on the new bins: ``Time`` from zero, the others from the first sample."""
        is_date = pd.api.types.is_datetime64_any_dtype(dtype)
        if col == "Time":
            values = offsets
        else:
            first = int(self._clock_ns(col)[first_position])
            values = np.full(len(offsets), _NAT_NS) if first == _NAT_NS else first + offsets
        return values.view("M8[ns]" if is_date else "m8[ns]")

    def _is_continuous_column(self, col: str, dtype: Any) -> bool:
        """Numeric, not boolean, not a discrete/excluded channel, not ``LapNumber``."""
        kind = self._CHANNELS.get(col, {}).get("type")
        if col in _TELEMETRY_META_COLUMNS or kind in ("discrete", "excluded"):
            return False
        return pd.api.types.is_numeric_dtype(dtype) and not pd.api.types.is_bool_dtype(dtype)

    def _float_values(self, col: str) -> np.ndarray:
        return pd.to_numeric(self[col], errors="coerce").to_numpy(dtype="float64", na_value=np.nan)

    def _resample_onto_dates(self, new_date_ref: pd.Series):
        """Resample every channel onto ``new_date_ref`` timestamps (needs ``Date``).

        One row per unique timestamp, sorted. Continuous channels and the
        other clock columns are interpolated on ``Date`` (constant beyond the
        first/last sample); everything else takes the value in force.
        """
        if "Date" not in self.columns:
            raise ValueError("'Date' is required to resample onto 'new_date_ref'")
        targets = pd.to_datetime(pd.Series(new_date_ref), errors="coerce").dropna()
        if getattr(targets.dt, "tz", None) is not None:
            targets = targets.dt.tz_localize(None)
        new = np.unique(targets.astype("datetime64[ns]").to_numpy().view("i8"))
        base_all = self._clock_ns("Date")
        positions = np.flatnonzero(base_all != _NAT_NS)
        if self.empty or not len(positions) or not len(new):
            return self._wrap(self.iloc[0:0].copy())
        positions = positions[np.argsort(base_all[positions], kind="stable")]
        base = base_all[positions]
        held = np.searchsorted(base, new, side="right") - 1
        # Interpolate on offsets from the first sample: epoch nanoseconds
        # exceed float64's exact integer range (~200 ns of error otherwise).
        new_t = (new - base[0]).astype("float64")
        base_t = (base - base[0]).astype("float64")
        out: dict[str, Any] = {}
        for col, dtype in zip(self.columns, self.dtypes, strict=True):
            if col == "Date":
                out[col] = new.view("M8[ns]")
            elif col in _RESAMPLE_AXIS_COLUMNS:
                clock = self._clock_ns(col)[positions]
                valid = clock != _NAT_NS
                if not valid.any():
                    out[col] = np.full(len(new), _NAT_NS).view("m8[ns]")
                    continue
                anchor = int(clock[valid].min())
                offsets = np.interp(new_t, base_t[valid], (clock[valid] - anchor).astype("float64"))
                values = np.round(offsets).astype("int64") + anchor
                is_date = pd.api.types.is_datetime64_any_dtype(dtype)
                out[col] = values.view("M8[ns]" if is_date else "m8[ns]")
            elif self._is_continuous_column(col, dtype):
                values = self._float_values(col)[positions]
                valid = ~np.isnan(values)
                if not valid.any():
                    out[col] = np.full(len(new), np.nan)
                    continue
                interpolated = np.interp(new_t, base_t[valid], values[valid])
                if pd.api.types.is_integer_dtype(dtype):
                    out[col] = pd.array(np.round(interpolated), dtype=dtype)
                else:
                    out[col] = interpolated
            else:
                in_force = self[col].iloc[positions].reset_index(drop=True).ffill()
                out[col] = in_force.iloc[np.maximum(held, 0)].reset_index(drop=True).bfill()
                if (held < 0).any():
                    first = in_force.bfill().iloc[0] if len(in_force) else None
                    out[col] = out[col].where(held >= 0, first)
        return self._wrap(pd.DataFrame(out, columns=self.columns))

    def join(self, *args, **kwargs):
        return self._wrap(cast(Any, super()).join(*args, **kwargs))

    def merge(self, *args, **kwargs):
        return self._wrap(cast(Any, super()).merge(*args, **kwargs))


#: Finishing statuses that count as classified finishes (``Finished``, ``+1 Lap``,
#: ``+2 Laps`` ..., ``Lapped``); everything else is a DNF.
_CLASSIFIED_STATUS_RE = re.compile(r"^(finished|lapped|\+\s*\d+\s*laps?)$", re.IGNORECASE)


class SessionResults(pd.DataFrame):
    """Session result with driver information (fastf1 columns first, tif1 extras last).

    See ``tif1.core.Session.results`` for the column contract.
    """

    _metadata: ClassVar[list[str]] = ["session"]

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow DataFrame-style subclass construction without re-implementing __new__."""
        return super().__new__(cls)

    def __init__(self, data=None, *args, session: TelemetryProvider | None = None, **kwargs):
        cast(Any, super()).__init__(data, *args, **kwargs)
        self.session = session

    @property
    def _constructor(self):
        return SessionResults

    @property
    def base_class_view(self):
        """The same data as a plain :class:`pandas.DataFrame` (fastf1 parity)."""
        return pd.DataFrame(self)

    @property
    def _constructor_sliced(self):
        return DriverResult


class DriverResult(pd.Series):
    """Driver and result information for a single driver."""

    _metadata: ClassVar[list[str]] = ["_name", "session"]

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow Series-style subclass construction without re-implementing __new__."""
        return cast(Self, super().__new__(cls))

    def __init__(self, data=None, *args, session: TelemetryProvider | None = None, **kwargs):
        cast(Any, super()).__init__(data, *args, **kwargs)
        self.session = session

    @property
    def _constructor(self):
        return DriverResult

    @property
    def dnf(self) -> bool:
        """True when the driver did not finish: a status other than ``Finished``/``+N Lap(s)``.

        ``Lapped`` (Jolpica's status for classified drivers a lap down) counts
        as a finish; ``Not classified`` counts as a DNF, as in Ergast. An
        empty or unknown status is not a DNF.
        """
        status = self.get("Status", "")
        if not isinstance(status, str) or not status.strip():
            return False
        return _CLASSIFIED_STATUS_RE.match(status.strip()) is None


def _empty_driver_laps(session: TelemetryProvider) -> DataFrame:
    """An empty laps frame that still carries the laps columns (a bound ``Laps`` for pandas).

    The loaded session table lends its schema (exact dtypes); without one,
    the lap pipeline runs on an empty payload. Either way a driver without
    laps (did not start) answers ``laps["LapNumber"]`` or ``pick_fastest()``
    like every other driver instead of raising on a column-less frame.
    """
    lib = session.lib
    source = getattr(session, "_laps", None)
    if lib == "polars":
        _ensure_polars_available()
        if isinstance(source, pl.DataFrame):
            return source.clear()
        payload = {key: [] for key in LAP_PAYLOAD_KEYS}
        return _process_lap_df(_create_lap_df(payload, "", "", lib), lib)
    if isinstance(source, pd.DataFrame):
        frame = source.iloc[:0].copy()
    else:
        payload = {key: [] for key in LAP_PAYLOAD_KEYS}
        frame = _process_lap_df(_create_lap_df(payload, "", "", lib), lib)
    laps = frame if isinstance(frame, Laps) else Laps(cast(pd.DataFrame, frame))
    laps.session = session
    return laps


def _load_driver_lap_payload(
    session: Any, driver: str, prefetched: dict[str, Any] | None = None
) -> dict:
    """One driver's laptime payload (``Driver.laps`` on either backend).

    The prefetched payload when :meth:`Session.get_driver` already holds it,
    else the memoised (or session-table-derived) payload, else one fetch.

    Raises:
        DataNotFoundError: If lap data doesn't exist
        NetworkError: If network request fails
        InvalidDataError: If data is corrupted
    """
    path = f"{driver}/laptimes.json"
    if isinstance(prefetched, dict):
        session._remember_local_payload(path, prefetched)
        return prefetched

    ultra_cold_enabled = session._resolve_ultra_cold_mode(None)
    if not ultra_cold_enabled:
        # Q19: a payload already memoized (or derivable from the memoized
        # session table) is what _fetch_laptime_payloads returns too; that
        # call also re-derived every other driver's payload (~1-2 ms), work
        # only an ultra-cold backfill consumes.
        local_payload = session._get_or_derive_driver_laptime_payload(driver)
        if isinstance(local_payload, dict):
            return local_payload
    payloads, cacheable_payloads = session._fetch_laptime_payloads(
        [(session._get_driver_info(driver), path)],
        operation="driver_laps",
        ultra_cold=ultra_cold_enabled,
    )
    lap_data = payloads[0] if payloads else None
    if cacheable_payloads and session._should_backfill_ultra_cold_cache(ultra_cold_enabled):
        session._schedule_background_cache_fill(json_payloads=cacheable_payloads)
    if isinstance(lap_data, dict):
        return lap_data

    raise DataNotFoundError(
        driver=driver,
        year=session.year,
        event=session.gp,
        session=session.session,
    )


def _empty_result_row() -> dict[str, Any]:
    """fastf1's result columns (then tif1's extras) with empty values, in order."""
    from .core_utils import ergast_results as _er

    row: dict[str, Any] = {}
    for column in _er.RESULTS_COLUMN_ORDER:
        if column in _er._FLOAT_COLUMNS:
            row[column] = math.nan
        elif column in _er._TIMEDELTA_COLUMNS or column in _er._DATETIME_COLUMNS:
            row[column] = pd.NaT
        elif column in _er._NO_FILL_STR:
            row[column] = None
        else:
            row[column] = ""
    return row


def _results_row_data(
    session: Any, driver: str, driver_number: str
) -> tuple[Any, dict[str, Any]] | None:
    """``(results label, row as a dict)`` of the driver (fastf1's ``DriverResult`` data).

    ``driver_number`` decides between rows sharing the abbreviation (a driver
    in two cars before the 1960s) and stands in when the code is unknown.

    Always the ``results`` row, so the object does not depend on whether
    ``results`` was read first (a practice session builds it from the laps).
    """
    try:
        results = session.results
    except (TIF1Error, AttributeError, KeyError, TypeError, ValueError):
        return None
    if not isinstance(results, pd.DataFrame) or results.empty:
        return None
    if "Abbreviation" in results.columns:
        mask = (results["Abbreviation"] == driver).to_numpy()
    else:
        mask = np.zeros(len(results), dtype=bool)
    if "DriverNumber" in results.columns:
        numbers = results["DriverNumber"].astype(str).to_numpy() == driver_number
        if mask.sum() > 1:  # a shared car: the number names the driver
            # ``to_numpy()`` of a pandas 3 (copy-on-write) Series is a
            # read-only view: combine into a new array, never in place.
            mask = mask & numbers
        elif not mask.any() and driver_number:
            mask = numbers
    if not mask.any():
        return None
    row = cast(pd.Series, results[mask].iloc[0])
    return row.name, {str(key): value for key, value in row.items()}


class Driver(DriverResult):
    """
    A driver of the session: fastf1's ``DriverResult`` row plus tif1's helpers.

    The Series holds the driver's :attr:`Session.results` row (fastf1's 22
    columns first, then tif1's extras) and keeps the identity fields from
    ``drivers.json`` when the session has no classification. ``.dnf`` comes
    from :class:`DriverResult`; ``.laps``, ``.get_lap``, ``.get_fastest_lap``
    and ``.get_fastest_lap_tel`` are tif1's.

    Args:
        session: Parent Session object
        driver: Driver code

    Attributes:
        session: Parent Session
        driver: Driver code
        laps: DataFrame with driver's laps
    """

    # Only identity/configuration fields propagate through pandas' finalize
    # hook. Derived lookup caches must remain local to each reconstructed object.
    _metadata: ClassVar[list[str]] = ["_name", "session", "driver", "_prefetched_lap_data"]
    session: TelemetryProvider
    driver: str

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow Series-style subclass construction without re-implementing __new__."""
        return cast(Self, pd.Series.__new__(cls))

    def __init__(
        self,
        session: TelemetryProvider,
        driver: str,
        prefetched_lap_data: dict[str, Any] | None = None,
        *,
        _car_number: str | None = None,
    ):
        # Build driver metadata as Series data. ``_car_number`` is the car the
        # identifier named ("8", "8/FAG"): it picks the row of a shared car.
        info = session._get_driver_info(driver)
        first_name = info.get("fn", "")
        last_name = info.get("ln", "")
        full_name = f"{first_name} {last_name}".strip()
        driver_number = str(_car_number or info.get("dn", ""))

        identity = {
            "DriverNumber": driver_number,
            "Abbreviation": driver,
            "TeamName": info.get("team", ""),
            "TeamColor": info.get("tc", ""),
            "FirstName": first_name,
            "LastName": last_name,
            "FullName": full_name,
            "HeadshotUrl": info.get("headshot_url", info.get("url", "")),
        }
        # Ergast identity (tif1.ergast) when the session carries the official
        # classification: ids, nationality, permanent number, date of birth.
        ergast_identity = getattr(session, "_ergast_identity", None)
        if callable(ergast_identity):
            identity.update(ergast_identity(driver_number, driver))

        # The results row (fastf1's DriverResult) when the session has one;
        # identity values fill whatever the row leaves empty. Without a
        # classification the row keeps fastf1's columns with empty values.
        found = _results_row_data(session, driver, driver_number)
        label: Any = driver_number if driver_number else driver
        if found is None:
            data = _empty_result_row()
            data.update(identity)
        else:
            label, data = found
            for key, value in identity.items():
                current = data.get(key)
                if key not in data or current is None or (isinstance(current, str) and not current):
                    data[key] = value
            if not driver_number:
                driver_number = str(data.get("DriverNumber") or "")

        # Initialize Series with driver data and name
        cast(Any, pd.Series).__init__(self, data, name=label)

        # Set custom attributes
        self.session = session
        self.driver = driver
        self._prefetched_lap_data = prefetched_lap_data
        self._laps = None
        self._lap_numbers = None
        self._lap_numbers_df_id = None
        self._lap_index_map: dict[int, int] | None = None
        self._lap_index_map_df_id: int | None = None
        self._lap_index_map_df_ref: pd.DataFrame | None = None

    def _reconstruct(self, data=None, *args, **kwargs):
        """Reconstruct a Driver Series without invoking its domain constructor."""
        result = object.__new__(type(self))
        cast(Any, pd.Series).__init__(result, data, *args, **kwargs)
        result.session = self.session
        result.driver = self.driver
        result._prefetched_lap_data = self._prefetched_lap_data
        # Derived lookup state belongs to the source frame and must not be
        # shared with a reconstructed Series.
        result._laps = None
        result._lap_numbers = None
        result._lap_numbers_df_id = None
        result._lap_index_map = None
        result._lap_index_map_df_id = None
        result._lap_index_map_df_ref = None
        return result

    @property
    def _constructor(self):
        return self._reconstruct

    @property
    def laps(self) -> DataFrame:
        """Get laps for this driver (returns empty DataFrame if no data found)."""
        if (
            self._laps is not None
            and self.session.lib == "pandas"
            and not isinstance(self._laps, Laps)
        ):
            self._laps = Laps(cast(pd.DataFrame, self._laps))
            self._laps.session = self.session
        if self._laps is None:
            session = self.session
            # A driver only the Ergast classification lists (did not start) has
            # no timing data; a season served from Ergast alone builds every
            # driver's laps at once from the lap timings.
            has_timing_data = getattr(session, "_driver_has_timing_data", None)
            if callable(has_timing_data) and not has_timing_data(self.driver):
                return self._set_empty_laps()
            ergast_only = bool(getattr(session, "_ergast_only", False))
            if ergast_only and session._laps is None:
                _ = session.laps
            try:
                if self.session._laps is not None and not _is_empty_df(
                    self.session._laps, self.session.lib
                ):
                    if self.session.lib == "polars":
                        _ensure_polars_available()
                        session_laps_pl = cast(Any, self.session._laps)
                        # Polars uses lazy evaluation - already optimal
                        driver_laps = session_laps_pl.filter(pl.col(COL_DRIVER) == self.driver)
                    else:
                        session_laps_pd = cast(pd.DataFrame, self.session._laps)
                        # Argos A4: a boolean mask; DataFrame.query(engine="python")
                        # re-tokenised the expression on every call (~1.7 ms vs
                        # ~0.4 ms on a 1455-row frame) for the same rows.
                        driver_mask = (session_laps_pd[COL_DRIVER] == self.driver).to_numpy(
                            dtype=bool, na_value=False
                        )
                        driver_laps = session_laps_pd[driver_mask].reset_index(drop=True)
                        if self.session.lib == "pandas":
                            driver_laps = Laps(driver_laps)
                            driver_laps.session = self.session

                    if not _is_empty_df(driver_laps, self.session.lib):
                        self._laps = driver_laps
                        self._lap_numbers = None
                        self._lap_numbers_df_id = None
                        self._lap_index_map = None
                        self._lap_index_map_df_id = None
                        self._lap_index_map_df_ref = None
                        return self._laps

                if ergast_only:
                    # The session table holds every lap Ergast knows: a driver
                    # absent from it has none, and there is nothing to fetch.
                    return self._set_empty_laps()
                lap_data = self._load_laps()
                if not lap_data:
                    return self._set_empty_laps()

                driver_info = self.session._get_driver_info(self.driver)

                # Q19: the pipeline's result from one pass per column (~25-40 ms
                # -> ~2 ms); None when a value needs the pipeline itself.
                processed = (
                    self._fast_laps_frame(lap_data, driver_info["team"])
                    if self.session.lib == "pandas"
                    else None
                )
                if processed is None:
                    self._laps = _create_lap_df(
                        lap_data, self.driver, driver_info["team"], self.session.lib
                    )
                    # Remove duplicate columns if they exist (pandas only)
                    if self.session.lib == "pandas" and isinstance(self._laps.columns, pd.Index):
                        if self._laps.columns.duplicated().any():
                            laps_pd = cast(pd.DataFrame, self._laps)
                            self._laps = laps_pd.loc[:, ~laps_pd.columns.duplicated()]

                    processed = _process_lap_df(self._laps, self.session.lib)
                if self.session.lib == "pandas":
                    self._laps = Laps(cast(pd.DataFrame, processed))
                    self._laps.session = self.session
                else:
                    self._laps = processed
                self._lap_numbers = None
                self._lap_numbers_df_id = None
                self._lap_index_map = None
                self._lap_index_map_df_id = None
                self._lap_index_map_df_ref = None
            except DataNotFoundError:
                logger.info(f"No lap data: {self.driver}")
                self._laps = _create_empty_df(self.session.lib)
                self._lap_numbers = set()
                self._lap_numbers_df_id = id(self._laps)
                self._lap_index_map = None
                self._lap_index_map_df_id = None
                self._lap_index_map_df_ref = None
            except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
                logger.warning(f"Failed to load laps for {self.driver}: {e}")
                self._laps = _create_empty_df(self.session.lib)
                self._lap_numbers = set()
                self._lap_numbers_df_id = id(self._laps)
                self._lap_index_map = None
                self._lap_index_map_df_id = None
                self._lap_index_map_df_ref = None

        return self._laps

    def _fast_laps_frame(self, lap_data: dict, team: Any) -> pd.DataFrame | None:
        """:func:`build_driver_laps_frame`, or None to run the pipeline (also on its errors)."""
        try:
            return build_driver_laps_frame(lap_data, self.driver, team)
        except (OverflowError, TypeError, ValueError) as e:
            # The pipeline then raises (or not) exactly as it always did.
            logger.debug("Fast laps build fell back for %s: %s", self.driver, e)
            return None

    def _ensure_lap_index_map(self, laps_pd: pd.DataFrame) -> dict[int, int]:
        """Build and cache lap-number -> row-position map for O(1) lap lookup."""
        current_df_id = id(laps_pd)
        if (
            self._lap_index_map is not None
            and self._lap_index_map_df_id == current_df_id
            and self._lap_index_map_df_ref is laps_pd
        ):
            return self._lap_index_map

        lap_map: dict[int, int] = {}
        if COL_LAP_NUMBER in laps_pd.columns:
            lap_values = laps_pd[COL_LAP_NUMBER].to_numpy(copy=False)
            for pos, lap_value in enumerate(lap_values):
                try:
                    lap_num = _coerce_lap_number(lap_value)
                except ValueError:
                    continue
                # Keep first occurrence for deterministic behavior
                if lap_num not in lap_map:
                    lap_map[lap_num] = pos

        self._lap_index_map = lap_map
        self._lap_index_map_df_id = current_df_id
        self._lap_index_map_df_ref = laps_pd
        return lap_map

    def _set_empty_laps(self) -> DataFrame:
        """Memoize and return an empty laps frame that carries the laps columns."""
        self._laps = _empty_driver_laps(self.session)
        self._lap_numbers = set()
        self._lap_numbers_df_id = id(self._laps)
        self._lap_index_map = None
        self._lap_index_map_df_id = None
        self._lap_index_map_df_ref = None
        return self._laps

    def _load_laps(self) -> dict:
        """
        Load lap data.

        Returns:
            Lap data dictionary

        Raises:
            DataNotFoundError: If lap data doesn't exist
            NetworkError: If network request fails
            InvalidDataError: If data is corrupted
        """
        prefetched = self._prefetched_lap_data
        if isinstance(prefetched, dict):
            self._prefetched_lap_data = None
        else:
            prefetched = None
        return _load_driver_lap_payload(self.session, self.driver, prefetched)

    def get_lap(self, lap_number: int) -> "Lap":
        """Get specific lap (raises LapNotFoundError if not found)."""
        _validate_lap_number(lap_number)
        laps = self.laps

        if self.session.lib == "pandas":
            laps_pd = cast(pd.DataFrame, laps)
            lap_index_map = self._ensure_lap_index_map(laps_pd)
            row_pos = lap_index_map.get(lap_number)
            if row_pos is None:
                raise LapNotFoundError(
                    lap_number=lap_number,
                    driver=self.driver,
                    year=self.session.year,
                    event=self.session.gp,
                    session=self.session.session,
                )
            lap_row = laps_pd.iloc[row_pos]
            if isinstance(lap_row, Lap):
                lap_row.session = self.session
                return lap_row
            lap_ctor = cast(Any, Lap)
            return cast(Lap, lap_ctor(lap_row, session=self.session))

        # Fallback for Polars
        if self.session.lib == "polars":
            _ensure_polars_available()
            laps_pl = cast(Any, laps)
            lap_row = laps_pl.filter(pl.col(COL_LAP_NUMBER) == lap_number)
            if lap_row.height == 0:
                raise LapNotFoundError(
                    lap_number=lap_number,
                    driver=self.driver,
                    year=self.session.year,
                    event=self.session.gp,
                    session=self.session.session,
                )
            # Convert single row Polars DF to pandas Series then to Lap
            lap_ctor = cast(Any, Lap)
            return cast(Lap, lap_ctor(lap_row.to_pandas().iloc[0], session=self.session))

        raise LapNotFoundError(lap_number=lap_number, driver=self.driver)

    def get_fastest_lap(self) -> DataFrame:
        """Get driver's fastest non-deleted lap (empty DataFrame if there is none)."""
        laps = self.laps
        if _is_empty_df(laps, self.session.lib):
            return _create_empty_df(self.session.lib)

        valid = _filter_valid_laptimes(laps, self.session.lib, exclude_deleted=True)
        if _is_empty_df(valid, self.session.lib):
            return _create_empty_df(self.session.lib)

        sort_col = self.session._lap_time_sort_column(valid)
        return (
            valid.sort(sort_col).head(1)
            if self.session.lib == "polars"
            else valid.nsmallest(1, sort_col).reset_index(drop=True)
        )

    def get_fastest_lap_tel(self) -> DataFrame:
        """Get telemetry from driver's fastest lap (returns empty DataFrame if not found)."""
        ultra_cold_enabled = self.session._resolve_ultra_cold_mode(None)
        lap_num: int | None = None

        raw_lap_payload = (
            self._prefetched_lap_data
            if isinstance(self._prefetched_lap_data, dict)
            else self.session._get_or_derive_driver_laptime_payload(self.driver)
        )
        candidate = self.session._extract_fastest_lap_candidate(self.driver, raw_lap_payload)
        if candidate is not None:
            lap_num = candidate[1]

        if (
            lap_num is None
            and self._laps is not None
            and not _is_empty_df(self._laps, self.session.lib)
        ):
            fastest_lap = self.get_fastest_lap()
            if not _is_empty_df(fastest_lap, self.session.lib):
                if self.session.lib == "polars":
                    fastest_lap_pl = cast(Any, fastest_lap)
                    lap_col = _get_lap_column(fastest_lap_pl, self.session.lib)
                    lap_value = (
                        fastest_lap_pl.select(lap_col).row(0)[0]
                        if lap_col in fastest_lap_pl.columns
                        else None
                    )
                else:
                    fastest_lap_pd = cast(pd.DataFrame, fastest_lap)
                    lap_col = _get_lap_column(fastest_lap_pd, self.session.lib)
                    lap_value = (
                        fastest_lap_pd.iloc[0][lap_col]
                        if lap_col in fastest_lap_pd.columns
                        else None
                    )
                try:
                    lap_num = _coerce_lap_number(lap_value)
                except ValueError:
                    lap_num = None

        if lap_num is None:
            return self.session.get_fastest_laps_tels(by_driver=True, drivers=[self.driver])

        return self.session._get_telemetry_df_for_ref(
            self.driver,
            lap_num,
            ultra_cold=ultra_cold_enabled,
        )


class _LapInternal:
    """
    Represents a single lap with telemetry data.

    Args:
        session: Parent Session object
        driver: Driver code
        lap_number: Lap number

    Attributes:
        session: Parent Session
        driver: Driver code
        lap_number: Lap number
        telemetry: DataFrame with telemetry data
    """

    def __init__(self, session: TelemetryProvider, driver: str, lap_number: int):
        self.session = session
        self.driver = driver
        self.lap_number = lap_number
        self._telemetry = None

    @property
    def telemetry(self) -> DataFrame:
        """Get telemetry data for this lap (returns empty DataFrame if not found)."""
        if self._telemetry is None:
            try:
                ultra_cold_enabled = self.session._resolve_telemetry_ultra_cold_mode(None)
                cached_tel = self.session._get_telemetry_payload(self.driver, self.lap_number)
                if cached_tel is None:
                    if (
                        self.session.enable_cache
                        and not ultra_cold_enabled
                        and self.session._session_cache_available()
                    ):
                        cache = get_cache()
                        cached_tel = cache.get_telemetry(
                            self.session.year,
                            self.session.gp,
                            self.session.session,
                            self.driver,
                            self.lap_number,
                        )
                        if isinstance(cached_tel, dict) and cached_tel:
                            self.session._remember_telemetry_payload(
                                self.driver, self.lap_number, cached_tel
                            )
                    if cached_tel is None and self.session._should_skip_telemetry_fetch(
                        self.driver
                    ):
                        return _create_empty_df(self.session.lib)

                tel = (
                    cached_tel
                    if cached_tel is not None
                    else self._fetch_telemetry(ultra_cold=ultra_cold_enabled)
                )
                telemetry_df = _create_telemetry_df(
                    tel, self.driver, self.lap_number, self.session.lib
                )
                if telemetry_df is None:
                    return _create_empty_df(self.session.lib)
                self._telemetry = telemetry_df
            except DataNotFoundError:
                logger.info(f"No telemetry: {self.driver} lap {self.lap_number}")
                return _create_empty_df(self.session.lib)
            except (InvalidDataError, NetworkError, TypeError, ValueError) as e:
                self.session._record_telemetry_failure(self.driver, self.lap_number, e)
                return _create_empty_df(self.session.lib)

        return self._telemetry

    def _fetch_telemetry(self, *, ultra_cold: bool = False) -> dict:
        """Fetch telemetry data (raises DataNotFoundError if not found)."""
        tel_path = f"{self.driver}/{int(self.lap_number)}_tel.json"
        tel_data = (
            self.session._fetch_json_unvalidated(tel_path)
            if ultra_cold
            else self.session._fetch_json(tel_path)
        )
        tel = tel_data.get("tel", {})
        if not isinstance(tel, dict):
            tel = {}
        self.session._remember_telemetry_payload(self.driver, self.lap_number, tel)

        if self.session.enable_cache:
            if ultra_cold and tel and self.session._should_backfill_ultra_cold_cache(True):
                self.session._schedule_background_cache_fill(
                    telemetry_payload=(self.driver, self.lap_number, tel)
                )
            elif not ultra_cold:
                get_cache().set_telemetry(
                    self.session.year,
                    self.session.gp,
                    self.session.session,
                    self.driver,
                    self.lap_number,
                    tel,
                )
                self.session._mark_session_cache_populated()
        return tel


__all__ = [
    "CircuitInfo",
    "Driver",
    "DriverResult",
    "Lap",
    "Laps",
    "LazyTelemetryDict",
    "SessionResults",
    "Telemetry",
    "TelemetryProvider",
]
