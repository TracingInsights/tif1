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
from collections.abc import Generator, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, Self, cast, runtime_checkable

import numpy as np
import pandas as pd

from .cache import get_cache
from .core_utils.constants import COL_DRIVER, COL_LAP_NUMBER
from .core_utils.driver_laps import build_driver_laps_frame
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
    _numeric_seconds_to_timedelta,
    _process_lap_df,
    _telemetry_frame_from_merged,
    _validate_lap_number,
)
from .exceptions import (
    DataNotFoundError,
    InvalidDataError,
    LapNotFoundError,
    NetworkError,
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

    try:
        import polars as pl

        POLARS_AVAILABLE = True
    except ImportError:
        pl = None  # type: ignore[ty:invalid-assignment]
        POLARS_AVAILABLE = False
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


@dataclass
class CircuitInfo:
    """Holds information about the circuit layout.

    This is a drop-in replacement for :class:`fastf1.mvapi.CircuitInfo`.
    Columns for all marker DataFrames: ``X <float>, Y <float>,
    Number <int>, Letter <str>, Angle <float>, Distance <float>``.

    ``marshal_lights`` and ``marshal_sectors`` are not available through
    the tif1 data source and are always returned as empty DataFrames with
    the correct column schema.
    """

    corners: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=_CORNERS_DF_COLUMNS))
    """Location of corners (FastF1-compatible DataFrame)."""

    marshal_lights: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=_CORNERS_DF_COLUMNS)
    )
    """Location of marshal lights (always empty – not in source data)."""

    marshal_sectors: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=_CORNERS_DF_COLUMNS)
    )
    """Location of marshal sectors (always empty – not in source data)."""

    rotation: float = 0.0
    """Rotation of the circuit in degrees."""

    def add_marker_distance(self, reference_lap: "Lap") -> None:
        """Compute the ``Distance`` value for each track marker.

        This is a FastF1-compatible method.  It populates the ``Distance``
        column of :attr:`corners`, :attr:`marshal_lights`, and
        :attr:`marshal_sectors` using the XY position data from the
        telemetry of *reference_lap*.

        The distance is selected via a *best-fit* approach: for each marker
        the telemetry sample whose squared XY error relative to the marker
        position is smallest is chosen, and its ``Distance`` value is
        assigned to the marker.

        Args:
            reference_lap: A :class:`Lap` whose telemetry contains
                ``X``, ``Y``, and ``Distance`` columns.
        """
        _log = logging.getLogger(__name__)

        try:
            tel = reference_lap.telemetry
        except Exception as exc:  # pragma: no cover
            _log.warning(
                "Failed to generate marker distance information: could not retrieve telemetry (%s)",
                exc,
            )
            return

        if tel is None or tel.empty:
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

        pos_tel = tel.dropna(subset=["X", "Y", "Distance"])
        if pos_tel.empty:
            _log.warning(
                "Failed to generate marker distance information: "
                "no valid position samples found in telemetry"
            )
            return

        # Numpy array of track XY coordinates  (shape: n_samples × 2)
        xy_ref_array = pos_tel[["X", "Y"]].to_numpy(dtype=float)

        for df in (self.corners, self.marshal_sectors, self.marshal_lights):
            if df.empty:
                continue

            # Numpy array of marker XY positions  (shape: n_markers × 2)
            marker_xy = df[["X", "Y"]].to_numpy(dtype=float)
            n_markers = marker_xy.shape[0]

            # Broadcast to (n_markers × n_samples × 2) and compute
            # squared Euclidean error for every marker × sample pair.
            xy_broadcast = xy_ref_array.reshape(1, -1, 2).repeat(n_markers, axis=0)
            diff = xy_broadcast - marker_xy.reshape(-1, 1, 2)
            sq_err = diff[..., 0] ** 2 + diff[..., 1] ** 2

            # Index of the closest track sample for each marker.
            indices = np.nanargmin(sq_err, axis=1)

            # Assign the Distance at that sample to the marker.
            distances = pos_tel.iloc[indices]["Distance"].to_list()
            df["Distance"] = distances


class LazyTelemetryDict(dict):
    """Lazy-loading dictionary that fetches telemetry data per driver on demand."""

    def __init__(self, session: TelemetryProvider):
        super().__init__()
        self.session = session

    def __getitem__(self, key):
        if key not in self:
            driver_code = None
            for d in self.session._drivers_data:
                if str(d.get("dn")) == str(key) or d.get("driver") == str(key):
                    driver_code = d.get("driver")
                    break
            if driver_code:
                # session.laps is typed as the DataFrame union; on the pandas
                # backend it is always a Laps instance carrying .telemetry.
                laps = cast(Any, self.session.laps)
                driver_laps = laps[laps["Driver"] == driver_code]
                self[key] = driver_laps.telemetry
            else:
                raise KeyError(key)
        return super().__getitem__(key)


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
        return self.pick_drivers([identifier])

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
        if isinstance(identifiers, str | int) or not isinstance(identifiers, list | tuple | set):
            identifiers = [identifiers]
        identifiers = [self._normalize_driver_identifier(i) for i in identifiers]
        mask = self["Driver"].isin(identifiers)
        # FastF1 accepts driver numbers as well as abbreviations: a numeric
        # identifier is matched against DriverNumber (strings upstream) too.
        if "DriverNumber" in self.columns and any(i.isdigit() for i in identifiers):
            numbers = self["DriverNumber"]
            if pd.api.types.is_numeric_dtype(numbers):
                numbers = numbers.astype("Int64").astype("string")
            elif not numbers.isin(identifiers).any():
                # object column holding ints (hand-built frames): compare as text
                numbers = numbers.astype("string")
            mask = mask | numbers.isin(identifiers)
        return self[mask]

    def pick_lap(self, lap_number):
        return self[self["LapNumber"] == lap_number]

    def pick_laps(self, laps):
        if isinstance(laps, slice):
            start = 1 if laps.start is None else laps.start
            stop = laps.stop
            if stop is None:
                return self[self["LapNumber"] >= start]
            return self[(self["LapNumber"] >= start) & (self["LapNumber"] <= stop)]
        if isinstance(laps, int):
            laps = [laps]
        return self[self["LapNumber"].isin(list(laps))]

    def pick_team(self, name):
        return self.pick_teams([name])

    def pick_teams(self, names):
        if isinstance(names, str):
            names = [names]
        return self[self["Team"].isin(names)]

    def pick_fastest(self, only_by_time=False):
        if self.empty:
            return None
        # FastF1 semantics: a deleted lap is not a candidate unless
        # only_by_time asks for the plain minimum.
        valid = _filter_valid_laptimes(self, "pandas", exclude_deleted=not only_by_time)
        if valid.empty:
            return None
        # Argos A5: the first minimum by position (what nsmallest(1,
        # keep="first") selected) without sorting/taking the whole frame;
        # NaT rows are already filtered out, so the int64 view has no
        # sentinel to mis-rank.
        lap_times = cast(pd.Series, valid["LapTime"]).to_numpy()
        if lap_times.dtype.kind == "m":
            fastest_pos = int(np.argmin(lap_times.view("i8")))
        else:
            fastest_pos = int(np.nanargmin(lap_times.astype("float64")))
        fastest = valid.iloc[fastest_pos]
        if isinstance(fastest, Lap):
            fastest.session = self.session
        return fastest

    def pick_quicklaps(self, threshold=1.07):
        if self.empty:
            return self
        best_time = self["LapTime"].min()
        if pd.isna(best_time):
            return self
        return self[self["LapTime"] <= best_time * threshold]

    def pick_tyre(self, compound):
        return self.pick_compounds([compound])

    def pick_compounds(self, compounds):
        if isinstance(compounds, str):
            compounds = [compounds]
        return self[self["Compound"].isin(compounds)]

    def pick_track_status(self, status, how="equals"):
        if how == "equals":
            return self[self["TrackStatus"] == str(status)]
        if how == "contains":
            return self[self["TrackStatus"].astype(str).str.contains(str(status), na=False)]
        return self

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

    def get_telemetry(self):
        # FastF1 compatibility: get_telemetry exposes driver-ahead channels.
        return self.telemetry.add_driver_ahead()

    def get_car_data(self, **kwargs):
        _ = kwargs
        if self.empty:
            return Telemetry()
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
                return Telemetry()
            tel = Telemetry(pd.concat(tels, ignore_index=True))
            tel.session = self.session
            return tel

    def get_pos_data(self, **kwargs):
        _ = kwargs
        return self.get_car_data()

    def get_weather_data(self):
        if self.session is not None and hasattr(self.session, "weather_data"):
            return self.session.weather_data
        return pd.DataFrame()

    def split_qualifying_sessions(self):
        qualifying_sessions = self.get("QualifyingSession")
        if qualifying_sessions is None:
            # Keep a stable shape that matches FastF1's tuple contract when
            # explicit qualifying session markers are unavailable.
            return self.copy(), self.copy(), self.copy()

        normalized = qualifying_sessions.astype("string").str.upper().str.strip()
        session_suffix = normalized.str.extract(r"([123])$", expand=False)
        if not bool(session_suffix.notna().any()):
            return self.copy(), self.copy(), self.copy()

        def _slice_for_suffix(target_suffix: str) -> "Laps":
            subset = self.loc[session_suffix == target_suffix].copy()
            if isinstance(subset, Laps):
                subset.session = self.session
            return subset

        return _slice_for_suffix("1"), _slice_for_suffix("2"), _slice_for_suffix("3")

    def join(self, *args, **kwargs):
        return cast(Any, super()).join(*args, **kwargs)

    def merge(self, *args, **kwargs):
        return cast(Any, super()).merge(*args, **kwargs)

    @property
    def telemetry(self):
        if self.empty:
            return Telemetry()
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
            return Telemetry()
        tel = Telemetry(pd.concat(tels, ignore_index=True))
        tel.session = self.session
        return tel

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
                    )
                    if merged_frame is not None:
                        tel = (
                            merged_frame
                            if isinstance(merged_frame, Telemetry)
                            else Telemetry(merged_frame)
                        )
                        tel.session = session
                        tel.driver = merged_driver
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
            tel = Telemetry()
            tel.session = session
            return tel
        try:
            merged = _merge_telemetry_payloads(entries)
            tel = Telemetry(_telemetry_frame_from_merged(merged))
        except Exception:
            # Fall back to the legacy per-lap path on malformed payloads.
            return None
        tel.session = session
        object.__setattr__(self, "_merged_telemetry_cache", (len(refs), refs, tel))
        return tel

    def iterlaps(
        self, require: Iterable[str] | None = None
    ) -> Generator[_IterLapResult, None, None]:
        required_columns = ["LapTime", "Driver"] if require is None else list(require)
        for column in required_columns:
            if column not in self.columns:
                raise KeyError(f"required column '{column}' is not present")

        # Argos A5: iterrows built one object Series per row and then ran
        # isna()/drop() on it (~0.3 ms per lap); one frame-level isna pass and
        # one object-array interleave give the same per-lap Series (same
        # values, column order, name and dtype) ~10x cheaper.
        columns = self.columns
        null_mask = self.isna().to_numpy(dtype=bool)
        # Positions by label match (an array per label) so duplicate column
        # names behave like the per-row ``lap.get(column)`` probe did.
        required_positions = np.concatenate(
            [np.flatnonzero(columns == column) for column in required_columns]
        )
        row_values = self.to_numpy(dtype=object)
        row_index = self.index
        session = self.session
        # Pergamon Q7: rows share a handful of null patterns (14 over the
        # 1455 laps of a race), so the kept positions and the column Index of
        # each pattern are built once (an Index is immutable; per-row
        # ``columns[keep]`` cost ~8 us), and each Lap is built through the
        # constructor's own steps (``_lap_from_row``).
        skip_rows = null_mask[:, required_positions].any(axis=1)
        patterns: dict[bytes, tuple[np.ndarray | None, pd.Index]] = {}
        for position in range(len(self)):
            if skip_rows[position]:
                continue
            null_row = null_mask[position]
            key = null_row.tobytes()
            pattern = patterns.get(key)
            if pattern is None:
                if null_row.any():
                    keep = ~null_row
                    pattern = (np.flatnonzero(keep), columns[keep])
                else:
                    pattern = (None, columns)
                patterns[key] = pattern
            keep_positions, lap_columns = pattern
            values = (
                row_values[position].copy()
                if keep_positions is None
                else row_values[position, keep_positions]
            )
            name = row_index[position]
            yield _IterLapResult(name, _lap_from_row(values, lap_columns, name, session))

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

    def get_telemetry(self):
        # FastF1 compatibility: get_telemetry exposes driver-ahead channels.
        return self.telemetry.add_driver_ahead()

    def get_car_data(self, **kwargs):
        _ = kwargs
        return self.telemetry

    def get_pos_data(self, **kwargs):
        _ = kwargs
        return self.telemetry

    def get_weather_data(self):
        return pd.Series()

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


class Telemetry(pd.DataFrame):
    """Multi-channel time series telemetry data."""

    _metadata: ClassVar[list[str]] = ["session", "driver"]

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow DataFrame-style subclass construction without re-implementing __new__."""
        return super().__new__(cls)

    def __init__(
        self,
        data=None,
        *args,
        session: TelemetryProvider | None = None,
        driver: str | None = None,
        **kwargs,
    ):
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

    def base_class_view(self):
        return pd.DataFrame(self)

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
        if not self.columns.is_unique:
            filled = self.copy()
            for col in filled.columns:
                if pd.api.types.is_numeric_dtype(filled[col]):
                    filled[col] = filled[col].interpolate(limit_direction="both")
            return self._wrap(filled)
        filled = self._owned_copy()
        float64 = np.dtype(np.float64)
        for i, (col, dtype) in enumerate(zip(self.columns, self.dtypes, strict=True)):
            if not pd.api.types.is_numeric_dtype(dtype):
                continue
            values = cast(Any, self)._ixs(i, axis=1)
            # A float64 column without NaN interpolates to itself: skip the
            # interpolation but keep the per-column assignment, so the block
            # layout of the result stays the same.
            if dtype != float64 or np.isnan(values.to_numpy()).any():
                values = values.interpolate(limit_direction="both")
            filled[col] = values
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

    def add_differential_distance(self):
        tel = self._owned_copy()
        tel["DifferentialDistance"] = self.calculate_differential_distance()
        return self._wrap(tel)

    def add_distance(self, drop_existing: bool = True):  # noqa: ARG002
        if "Distance" in self.columns:
            return self

        distance = self.integrate_distance()
        new_dist = pd.DataFrame({"Distance": distance}, index=self.index)
        return self.join(new_dist, how="outer")

    def add_relative_distance(self, drop_existing: bool = True):
        if "RelativeDistance" in self.columns:
            return self

        tel = self.add_distance(drop_existing=drop_existing)._owned_copy()
        distance = pd.to_numeric(cast(pd.Series, tel["Distance"]), errors="coerce")
        max_dist = distance.max()
        if pd.isna(max_dist) or max_dist == 0:
            relative_distance = pd.Series(0.0, index=self.index, dtype=float)
        else:
            relative_distance = distance / float(max_dist)
        tel["RelativeDistance"] = relative_distance.to_numpy(copy=False)
        return self._wrap(tel)

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

    def add_track_status(self):
        tel = self._owned_copy()
        if "TrackStatus" not in tel.columns:
            tel["TrackStatus"] = "1"
        return self._wrap(tel)

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
        _ = interpolate_edges
        time_ref_col = "SessionTime" if "SessionTime" in self.columns else "Time"
        if time_ref_col not in self.columns:
            return self._wrap(self.copy())

        start = self._coerce_timedelta(start_time)
        end = self._coerce_timedelta(end_time)
        if pd.isna(start) or pd.isna(end):
            return self._wrap(self.iloc[0:0].copy())

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

        # tif1 telemetry keeps ``Time`` lap-relative (zero at the lap start)
        # and carries no ``SessionTime``, so a lap's session-time window
        # (LapStartTime..Time) can never select its samples. Without a
        # session-time axis, match on LapNumber (and Driver when both sides
        # carry it); the time window stays the path for FastF1-style frames.
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

    @staticmethod
    def _is_lone_timedelta_column(frame: pd.DataFrame, col: str) -> bool:
        """True when ``col`` is a numpy timedelta column alone in its block.

        ``pd.to_timedelta`` returns such a column unchanged, and re-assigning
        it only moves its block to the end of the manager, so the
        ``frame[col] = pd.to_timedelta(frame[col])`` normalization can be
        skipped without changing the values, dtypes or block count.
        """
        columns = frame.columns
        if not columns.is_unique:
            return False
        mgr = frame._mgr
        block = mgr.blocks[mgr.blknos[columns.get_loc(col)]]
        dtype = block.dtype
        return isinstance(dtype, np.dtype) and dtype.kind == "m" and len(block.mgr_locs) == 1

    def merge_channels(self, other, **kwargs):
        _ = kwargs
        if "Time" in self.columns and "Time" in other.columns:
            left = self._owned_copy()
            right = pd.DataFrame(other)  # shallow: a new manager, safe to assign into
            if not right._mgr.is_consolidated():
                right = right.copy()  # keep the consolidating deep copy's layout
            if not self._is_lone_timedelta_column(left, "Time"):
                left["Time"] = pd.to_timedelta(left["Time"], errors="coerce")
            if not self._is_lone_timedelta_column(right, "Time"):
                right["Time"] = pd.to_timedelta(right["Time"], errors="coerce")
            merged = pd.merge_asof(
                left.sort_values("Time"),
                right.sort_values("Time"),
                on="Time",
                suffixes=("", "_other"),
                direction="nearest",
            )
        else:
            merged = pd.concat([self.reset_index(drop=True), pd.DataFrame(other)], axis=1)
        return self._wrap(merged)

    def resample_channels(self, rule: str = "1s", **kwargs):
        _ = kwargs
        rule = _modern_offset_alias(rule)
        if "Time" not in self.columns or self.empty:
            return self._wrap(self._owned_copy())
        if not self.columns.is_unique:
            frame = self.copy()
            frame["Time"] = pd.to_timedelta(frame["Time"], errors="coerce")
            frame = frame.dropna(subset=["Time"]).set_index("Time").sort_index()
            numeric_cols = [c for c in frame.columns if pd.api.types.is_numeric_dtype(frame[c])]
            frame = frame[numeric_cols]
        else:
            # Only ``Time`` and the numeric channels reach the resampler: take
            # them first so the text/object columns are not carried through
            # the NaT filter, ``set_index`` and ``sort_index``.
            numeric_cols = [
                col
                for col, dtype in zip(self.columns, self.dtypes, strict=True)
                if col != "Time" and pd.api.types.is_numeric_dtype(dtype)
            ]
            frame = self[["Time", *numeric_cols]]
            if not self._is_lone_timedelta_column(frame, "Time"):
                frame["Time"] = pd.to_timedelta(frame["Time"], errors="coerce")
            if frame["Time"].isna().any():
                frame = frame.dropna(subset=["Time"])
            frame = frame.set_index("Time").sort_index()
        resampled = frame.resample(rule).mean().interpolate(limit_direction="both")
        resampled = resampled.reset_index()
        return self._wrap(resampled)

    def join(self, *args, **kwargs):
        return self._wrap(cast(Any, super()).join(*args, **kwargs))

    def merge(self, *args, **kwargs):
        return self._wrap(cast(Any, super()).merge(*args, **kwargs))


class SessionResults(pd.DataFrame):
    """Session result with driver information."""

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
    def dnf(self):
        status = self.get("Status", "")
        if isinstance(status, str):
            return status.lower() not in ("finished", "+1 lap", "+2 laps", "not classified")
        return False


class Driver(pd.Series):
    """
    Represents a driver in a session as a pandas Series.

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

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow Series-style subclass construction without re-implementing __new__."""
        return cast(Self, super().__new__(cls))

    def __init__(
        self,
        session: TelemetryProvider,
        driver: str,
        prefetched_lap_data: dict[str, Any] | None = None,
    ):
        # Build driver metadata as Series data
        info = session._get_driver_info(driver)
        first_name = info.get("fn", "")
        last_name = info.get("ln", "")
        full_name = f"{first_name} {last_name}".strip()
        driver_number = str(info.get("dn", ""))

        data = {
            "DriverNumber": driver_number,
            "Abbreviation": driver,
            "TeamName": info.get("team", ""),
            "TeamColor": info.get("tc", ""),
            "FirstName": first_name,
            "LastName": last_name,
            "FullName": full_name,
            "HeadshotUrl": info.get("headshot_url", info.get("url", "")),
        }

        # Initialize Series with driver data and name
        cast(Any, super()).__init__(data, name=driver_number if driver_number else driver)

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

                lap_data = self._load_laps()
                if not lap_data:
                    self._laps = _create_empty_df(self.session.lib)
                    self._lap_numbers = set()
                    self._lap_numbers_df_id = id(self._laps)
                    self._lap_index_map = None
                    self._lap_index_map_df_id = None
                    self._lap_index_map_df_ref = None
                    return self._laps

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
        path = f"{self.driver}/laptimes.json"
        if isinstance(self._prefetched_lap_data, dict):
            prefetched = self._prefetched_lap_data
            self._prefetched_lap_data = None
            self.session._remember_local_payload(path, prefetched)
            return prefetched

        ultra_cold_enabled = self.session._resolve_ultra_cold_mode(None)
        if not ultra_cold_enabled:
            # Q19: a payload already memoized (or derivable from the memoized
            # session table) is what _fetch_laptime_payloads returns too; that
            # call also re-derived every other driver's payload (~1-2 ms), work
            # only an ultra-cold backfill consumes.
            local_payload = self.session._get_or_derive_driver_laptime_payload(self.driver)
            if isinstance(local_payload, dict):
                return local_payload
        payloads, cacheable_payloads = self.session._fetch_laptime_payloads(
            [(self.session._get_driver_info(self.driver), path)],
            operation="driver_laps",
            ultra_cold=ultra_cold_enabled,
        )
        lap_data = payloads[0] if payloads else None
        if cacheable_payloads and self.session._should_backfill_ultra_cold_cache(
            ultra_cold_enabled
        ):
            self.session._schedule_background_cache_fill(json_payloads=cacheable_payloads)
        if isinstance(lap_data, dict):
            return lap_data

        raise DataNotFoundError(
            driver=self.driver,
            year=self.session.year,
            event=self.session.gp,
            session=self.session.session,
        )

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
