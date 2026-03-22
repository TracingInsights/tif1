"""Core functionality for tif1."""

import asyncio
import logging
import math
import re
import threading
from collections import OrderedDict
from collections.abc import Generator, Iterable
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, cast

import niquests
import numpy as np
import pandas as pd

from .async_fetch import _validate_json_payload as _validate_json_impl
from .async_fetch import fetch_multiple_async
from .cache import get_cache
from .cdn import get_cdn_manager
from .config import get_config

# Import from core utilities modules
from .core_utils.constants import (
    CATEGORICAL_COLS,
    COL_DRIVER,
    COL_LAP_NUMBER,
    COL_LAP_NUMBER_ALT,
    COL_LAP_TIME,
    COL_LAP_TIME_SECONDS,
    COL_TEAM,
    LAP_RENAME_MAP,
    MAX_CACHE_SIZE,
    MAX_YEAR,
    MIN_YEAR,
    RACE_CONTROL_RENAME_MAP,
    WEATHER_RENAME_MAP,
)
from .core_utils.helpers import (
    DataFrame,
    _apply_categorical,
    _create_empty_df,
    _create_telemetry_df,
    _encode_url_component,
    _filter_valid_laptimes,
    _is_empty_df,
    _rename_columns,
    _reorder_laps_columns,
    _validate_drivers_list,
    _validate_lap_number,
    _validate_string_param,
    _validate_year,
)
from .core_utils.json_utils import parse_response_json
from .exceptions import (
    DataNotFoundError,
    DriverNotFoundError,
    InvalidDataError,
    LapNotFoundError,
    NetworkError,
)
from .http_session import get_session as get_http_session
from .retry import retry_with_backoff

pl = None
POLARS_AVAILABLE: bool | None = None

logger = logging.getLogger(__name__)


def _get_session():
    return get_http_session()


def _ensure_polars_available() -> bool:
    """Lazy-load Polars only when explicitly requested.

    This re-checks imports when a prior probe failed so notebook users can
    install polars mid-session and immediately use the polars lib.
    """
    global pl, POLARS_AVAILABLE
    if POLARS_AVAILABLE is True and pl is not None:
        return True

    try:
        import polars as pl

        POLARS_AVAILABLE = True
    except ImportError:
        pl = None
        POLARS_AVAILABLE = False
    return POLARS_AVAILABLE


# Get configuration
config = get_config()


def _validate_json_payload(path: str, data: dict[str, Any]) -> dict[str, Any]:
    """Validate JSON payload using global config (wrapper for async_fetch version)."""
    return _validate_json_impl(path, data, config)


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

    def __init__(self, session):
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
                laps = self.session.laps
                driver_laps = laps[laps["Driver"] == driver_code]
                self[key] = driver_laps.telemetry
            else:
                raise KeyError(key)
        return super().__getitem__(key)


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

    def __init__(self, data=None, *args, session=None, **kwargs):
        cast(Any, super()).__init__(data, *args, **kwargs)
        self.session = session

    @property
    def _constructor(self):
        return Laps

    @property
    def _constructor_sliced(self):
        return Lap

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
        return self[self["Driver"].isin(identifiers)]

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
        valid = _filter_valid_laptimes(self, "pandas")
        if valid.empty:
            return None
        _ = only_by_time
        fastest = valid.nsmallest(1, "LapTime").iloc[0]
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
        # Tracing Insights data does not provide explicit Q1/Q2/Q3 splits.
        # Keep a stable shape that matches FastF1's tuple contract.
        return self.copy(), self.copy(), self.copy()

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
        tels = []
        for _, lap in self.iterrows():
            tels.append(lap.telemetry)
        if not tels:
            return Telemetry()
        tel = Telemetry(pd.concat(tels, ignore_index=True))
        tel.session = self.session
        return tel

    def iterlaps(
        self, require: Iterable[str] | None = None
    ) -> Generator[_IterLapResult, None, None]:
        required_columns = ["LapTime", "Driver"] if require is None else list(require)
        for column in required_columns:
            if column not in self.columns:
                raise KeyError(f"required column '{column}' is not present")

        for index, lap_row in self.iterrows():
            lap = lap_row
            if isinstance(lap, Lap):
                lap.session = self.session

            null_columns = lap.index[lap.isna()]
            if len(null_columns):
                non_null_lap = lap.drop(labels=null_columns)
                if isinstance(non_null_lap, Lap):
                    non_null_lap.session = self.session
            else:
                non_null_lap = lap

            if any(pd.isna(non_null_lap.get(column)) for column in required_columns):
                continue

            yield _IterLapResult(index, non_null_lap)

    def reset_index(self, drop=False, **kwargs):  # type: ignore[override]
        """Reset index and drop level_0 column if created."""
        result = cast(Any, super()).reset_index(drop=drop, **kwargs)
        # Remove level_0 column if it was created
        if not drop and "level_0" in result.columns:
            result = result.drop(columns=["level_0"])
        return result


class Lap(pd.Series):
    """Object for accessing lap (timing) data of a single lap."""

    _metadata: ClassVar[list[str]] = ["session"]
    session: Any

    def __init__(self, data=None, *args, **kwargs):
        session = kwargs.pop("session", None)
        cast(Any, super()).__init__(data, *args, **kwargs)
        object.__setattr__(self, "session", session)

    @property
    def _constructor(self):
        return Lap

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


class Telemetry(pd.DataFrame):
    """Multi-channel time series telemetry data."""

    _metadata: ClassVar[list[str]] = ["session", "driver"]

    def __init__(self, data=None, *args, session=None, driver=None, **kwargs):
        cast(Any, super()).__init__(data, *args, **kwargs)
        self.session = session
        self.driver = driver

    @property
    def _constructor(self):
        return Telemetry

    def _wrap(self, frame: pd.DataFrame):
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
        drivers = cast(pd.Series, self["Driver"]).dropna().unique()
        if len(drivers) != 1:
            return None
        driver = drivers[0]
        return str(driver) if driver else None

    def _get_lap_numbers(self) -> list[int]:
        """Return sorted lap numbers referenced by this telemetry slice."""
        if "LapNumber" not in self.columns:
            return []
        lap_numbers = (
            pd.to_numeric(cast(pd.Series, self["LapNumber"]), errors="coerce")
            .dropna()
            .astype(int)
            .unique()
            .tolist()
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
        if pd.api.types.is_timedelta64_ns_dtype(values):
            return values
        if pd.api.types.is_numeric_dtype(values):
            return _numeric_seconds_to_timedelta(values)
        return pd.to_timedelta(values, errors="coerce")

    def base_class_view(self):
        return pd.DataFrame(self)

    def get_first_non_zero_time_index(self):
        if "Time" not in self.columns or self.empty:
            return 0
        time_vals = pd.to_timedelta(self["Time"], errors="coerce")
        non_zero = time_vals[time_vals > pd.Timedelta(0)]
        return int(non_zero.index[0]) if not non_zero.empty else 0

    def fill_missing(self):
        filled = self.copy()
        for col in filled.columns:
            if pd.api.types.is_numeric_dtype(filled[col]):
                filled[col] = filled[col].interpolate(limit_direction="both")
        return self._wrap(filled)

    def integrate_distance(self):
        if self.empty or "Speed" not in self.columns:
            return pd.Series(dtype=float)
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
        speed_ms = pd.to_numeric(self["Speed"], errors="coerce").fillna(0.0) / 3.6
        if "Time" in self.columns:
            t = pd.to_timedelta(self["Time"], errors="coerce").dt.total_seconds().fillna(0.0)
            dt = t.diff().fillna(0.0).clip(lower=0.0)
            return speed_ms * dt
        return pd.Series(0.0, index=self.index)

    def add_differential_distance(self):
        tel = self.copy()
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

        tel = self.add_distance(drop_existing=drop_existing).copy()
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
            driver_ahead = cast(pd.Series, self["DriverAhead"]).to_numpy(copy=True)
            distance_to_driver_ahead = pd.to_numeric(
                cast(pd.Series, self["DistanceToDriverAhead"]), errors="coerce"
            ).to_numpy(copy=True)
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
        tel = self.copy()
        if "TrackStatus" not in tel.columns:
            tel["TrackStatus"] = "1"
        return self._wrap(tel)

    def slice_by_mask(self, mask, pad: int = 0, pad_side: str = "both"):
        mask_array = np.asarray(mask, dtype=bool).copy()
        if mask_array.shape[0] != len(self):
            raise ValueError("Mask length must match telemetry length.")

        if pad and np.any(mask_array):
            true_indices = np.where(mask_array)[0]
            first_idx = int(true_indices.min())
            last_idx = int(true_indices.max())

            if pad_side in ("both", "before"):
                first_idx = max(0, first_idx - int(pad))
            if pad_side in ("both", "after"):
                last_idx = min(len(mask_array) - 1, last_idx + int(pad))

            mask_array[first_idx : last_idx + 1] = True

        return self._wrap(self.loc[mask_array].copy())

    def slice_by_time(
        self,
        start_time,
        end_time,
        pad: int = 0,
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

        ref_time = self._coerce_timedelta_series(cast(pd.Series, self[time_ref_col]))
        selection_mask = (ref_time >= start) & (ref_time <= end)
        data_slice = self.slice_by_mask(selection_mask.to_numpy(copy=False), pad, pad_side)

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

    def slice_by_lap(
        self,
        ref_laps,
        pad: int = 0,
        pad_side: str = "both",
        interpolate_edges: bool = False,
    ):
        if isinstance(ref_laps, Laps) and len(ref_laps) > 1:
            if "DriverNumber" in ref_laps.columns and len(ref_laps["DriverNumber"].unique()) > 1:
                raise ValueError(
                    "Cannot slice telemetry because 'ref_laps' contains Laps of multiple drivers!"
                )

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

        lap_mask = cast(pd.Series, self["LapNumber"]).isin(lap_numbers).to_numpy(copy=False)
        return self.slice_by_mask(lap_mask, pad=pad, pad_side=pad_side)

    def merge_channels(self, other, **kwargs):
        _ = kwargs
        if "Time" in self.columns and "Time" in other.columns:
            left = self.copy()
            right = pd.DataFrame(other).copy()
            left["Time"] = pd.to_timedelta(left["Time"], errors="coerce")
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

    def resample_channels(self, rule: str = "1S", **kwargs):
        _ = kwargs
        if "Time" not in self.columns or self.empty:
            return self._wrap(self.copy())
        frame = self.copy()
        frame["Time"] = pd.to_timedelta(frame["Time"], errors="coerce")
        frame = frame.dropna(subset=["Time"]).set_index("Time").sort_index()
        numeric_cols = [c for c in frame.columns if pd.api.types.is_numeric_dtype(frame[c])]
        resampled = frame[numeric_cols].resample(rule).mean().interpolate(limit_direction="both")
        resampled = resampled.reset_index()
        return self._wrap(resampled)

    def join(self, *args, **kwargs):
        return self._wrap(cast(Any, super()).join(*args, **kwargs))

    def merge(self, *args, **kwargs):
        return self._wrap(cast(Any, super()).merge(*args, **kwargs))


class SessionResults(pd.DataFrame):
    """Session result with driver information."""

    _metadata: ClassVar[list[str]] = ["session"]

    def __init__(self, data=None, *args, session=None, **kwargs):
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

    _metadata: ClassVar[list[str]] = ["session"]

    def __init__(self, data=None, *args, session=None, **kwargs):
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


class LRUCache:
    """Thread-safe LRU cache with size limit."""

    def __init__(self, maxsize: int = MAX_CACHE_SIZE):
        self.cache = OrderedDict()
        self.maxsize = maxsize
        self.lock = threading.Lock()

    def get(self, key: str):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            return None

    def set(self, key: str, value):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.maxsize:
                self.cache.popitem(last=False)

    def clear(self):
        """Clear all cached items."""
        with self.lock:
            self.cache.clear()


_global_lap_cache = LRUCache(maxsize=MAX_CACHE_SIZE)
_global_lap_cache_polars = LRUCache(maxsize=MAX_CACHE_SIZE)


def _get_backend_lap_cache(lib: Literal["pandas", "polars"]) -> LRUCache:
    """Get the global lap cache instance for a specific DataFrame library."""
    return _global_lap_cache_polars if lib == "polars" else _global_lap_cache


# Applied on-demand only when a blocking sync API is called from an async loop.
_NEST_ASYNCIO_APPLIED = False


# Helper functions
def _ensure_nested_loop_support(operation: str) -> None:
    """Apply nest_asyncio on demand for sync APIs invoked inside an async loop."""
    global _NEST_ASYNCIO_APPLIED

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return

    if _NEST_ASYNCIO_APPLIED:
        return

    try:
        import nest_asyncio

        nest_asyncio.apply()
        _NEST_ASYNCIO_APPLIED = True
        logger.info(f"Applied nest_asyncio dynamically for {operation}")
    except ImportError as e:
        raise RuntimeError(
            f"{operation} cannot run inside an active event loop without nest_asyncio. "
            "Use async APIs (e.g. laps_async) or install nest-asyncio."
        ) from e


def _get_lap_column(df, lib: str) -> str:
    """Get lap number column name."""
    return COL_LAP_NUMBER if COL_LAP_NUMBER in df.columns else COL_LAP_NUMBER_ALT


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


def _coerce_lap_number(lap_value: Any) -> int:
    """Coerce lap value to int with a stable error contract."""
    if lap_value is None:
        raise ValueError("No lap number found in row")
    try:
        return int(lap_value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid lap number: {lap_value}") from e


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


def _create_lap_df(lap_data: dict, driver: str, team: str, lib: str) -> DataFrame:
    """Create lap DataFrame with driver and team info (zero-copy optimized)."""
    # Normalize data for both backends to handle mismatched column heights
    # This is required in Python 3.12+ where both Pandas and Polars are stricter
    if lap_data:
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
    else:
        normalized_data = {}

    if lib == "polars":
        lap_df = pl.DataFrame(normalized_data, strict=False)  # type: ignore[union-attr]
        lap_df = lap_df.with_columns(
            [pl.lit(driver).alias(COL_DRIVER), pl.lit(team).alias(COL_TEAM)]  # type: ignore[union-attr]
        )
    else:
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


def _numeric_seconds_to_timedelta(values: pd.Series) -> pd.Series:
    """Convert numeric seconds to timedelta64[ns] without NaN cast warnings."""
    numeric_values = (
        cast(pd.Series, values)
        if pd.api.types.is_numeric_dtype(values)
        else pd.to_numeric(values, errors="coerce")
    )
    valid_mask = numeric_values.notna()
    result = pd.Series(pd.NaT, index=numeric_values.index, dtype="timedelta64[ns]")
    if bool(valid_mask.any()):
        result.loc[valid_mask] = pd.to_timedelta(
            numeric_values.loc[valid_mask].to_numpy(copy=False), unit="s"
        )
    return result


def _apply_laps_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce _COLUMNS dtype contract on a pandas laps DataFrame.

    Columns already handled upstream (LapTime, Time, WeatherTime, LapTimeSeconds)
    are skipped here to avoid double-conversion. All others from _COLUMNS are
    coerced to their canonical dtype. Missing columns are silently ignored.
    """
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
        if col in df.columns and not pd.api.types.is_timedelta64_ns_dtype(df[col]):
            df[col] = _numeric_seconds_to_timedelta(cast(pd.Series, df[col]))

    # ------------------------------------------------------------------
    # Datetime column: LapStartDate arrives as ISO-8601 strings
    # ------------------------------------------------------------------
    if "LapStartDate" in df.columns and not pd.api.types.is_datetime64_any_dtype(
        df["LapStartDate"]
    ):
        df["LapStartDate"] = pd.to_datetime(df["LapStartDate"], errors="coerce", utc=False)

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
    _STR_COLS = ("Driver", "DriverNumber", "Compound", "Team", "TrackStatus", "DeletedReason")
    for col in _STR_COLS:
        if col in df.columns:
            col_series = df[col]
            if not pd.api.types.is_object_dtype(col_series):
                df[col] = col_series.astype(object)

    return df


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
        if not pd.api.types.is_timedelta64_ns_dtype(lap_time_series):
            numeric_lap_times = pd.to_numeric(lap_time_series, errors="coerce")
            parsed_lap_times = pd.to_timedelta(lap_time_series, errors="coerce")
            numeric_lap_timedeltas = _numeric_seconds_to_timedelta(numeric_lap_times)
            lap_df[COL_LAP_TIME] = numeric_lap_timedeltas.where(
                numeric_lap_times.notna(),
                parsed_lap_times,
            )
        lap_df[COL_LAP_TIME_SECONDS] = (
            cast(pd.Series, lap_df[COL_LAP_TIME]).dt.total_seconds().to_numpy(copy=False)
        )
    if lib == "pandas" and "Time" in lap_df.columns:
        time_series = cast(pd.Series, lap_df["Time"])
        if not pd.api.types.is_timedelta64_ns_dtype(time_series):
            # Only convert if it's actually a Series (not already converted)
            if isinstance(time_series, pd.Series):
                lap_df["Time"] = _numeric_seconds_to_timedelta(time_series)
    if lib == "pandas" and "WeatherTime" in lap_df.columns:
        weather_time_series = cast(pd.Series, lap_df["WeatherTime"])
        if not pd.api.types.is_timedelta64_ns_dtype(weather_time_series):
            if isinstance(weather_time_series, pd.Series):
                lap_df["WeatherTime"] = _numeric_seconds_to_timedelta(weather_time_series)
    # Apply full _COLUMNS dtype contract for all remaining pandas columns
    if lib == "pandas":
        lap_df = _apply_laps_dtypes(lap_df)
    if lib == "polars" and COL_LAP_TIME in lap_df.columns:
        lap_df_pl = cast(Any, lap_df)
        lap_df = lap_df_pl.with_columns(
            cast(Any, pl)
            .col(COL_LAP_TIME)
            .cast(cast(Any, pl).Float64, strict=False)
            .alias(COL_LAP_TIME_SECONDS)
        )
    if lib == "polars" and not bool(config.get("polars_lap_categorical", False)):
        lap_df = _reorder_laps_columns(lap_df, lib)
        return lap_df
    lap_df = _apply_categorical(lap_df, CATEGORICAL_COLS, lib)
    lap_df = _reorder_laps_columns(lap_df, lib)
    return lap_df


def _create_session_df(data: dict[str, Any], rename_map: dict[str, str], lib: str) -> DataFrame:
    """Create a session-level DataFrame from a payload dict and rename columns (zero-copy optimized)."""
    if lib == "polars":
        frame = pl.DataFrame(data, strict=False)  # type: ignore[union-attr]
    else:
        # Use copy=False to avoid unnecessary data duplication
        frame = pd.DataFrame(data, copy=False)

    if _is_empty_df(frame, lib):
        return _create_empty_df(lib)
    return _rename_columns(frame, rename_map, lib)


def clear_lap_cache() -> None:
    """Clear global lap cache."""
    _global_lap_cache.clear()
    _global_lap_cache_polars.clear()


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
        gp: Grand Prix name (URL encoded)
        session: Session name (URL encoded)
        drivers_df: DataFrame with driver information
        laps: DataFrame with all laps

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
    ):
        _validate_year(year, MIN_YEAR, MAX_YEAR)
        gp_name = _resolve_gp_name(year, gp)
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
        self._fastest_lap_ref: tuple[str, int] | None = None
        self._fastest_lap_ref_laps_source_id = None
        self._fastest_lap_ref_driver_source_id = None
        self._fastest_lap_tel_ref: tuple[str, int] | None = None
        self._fastest_lap_tel_df: DataFrame | None = None
        self._race_control_messages: DataFrame | None = None
        self._weather: DataFrame | None = None
        self._car_data: DataFrame | None = None
        self._results = None
        self._circuit_info: CircuitInfo | None = None
        self._local_json_payloads: dict[str, dict[str, Any]] = {}
        self._telemetry_payloads: dict[tuple[str, int], dict[str, Any]] = {}
        self._telemetry_df_cache: dict[tuple[str, int], DataFrame] = {}
        self._telemetry_failure_counts: dict[str, int] = {}
        self._telemetry_failure_suppressed_drivers: set[str] = set()
        self._telemetry_unavailable_drivers: set[str] = set()
        self._telemetry_bulk_prefetch_lock = threading.Lock()
        self._telemetry_bulk_prefetch_attempted = False
        self._telemetry_bulk_prefetch_done = False
        self._telemetry_background_prefetch_started = False
        self._session_tables_prefetched = False
        self._cache_has_session_data: bool | None = None
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
        if self._cache_has_session_data is not None:
            return self._cache_has_session_data

        try:
            cache = get_cache()
            has_session_data = getattr(cache, "has_session_data", None)
            if callable(has_session_data):
                self._cache_has_session_data = bool(
                    has_session_data(self.year, self.gp, self.session)
                )
            else:
                logger.debug(
                    "Cache probe unavailable for %s/%s/%s; assuming cold session",
                    self.year,
                    self.gp,
                    self.session,
                )
                self._cache_has_session_data = False
        except (AttributeError, RuntimeError, TypeError, ValueError) as e:
            logger.debug(
                "Session cache probe failed for %s/%s/%s: %s",
                self.year,
                self.gp,
                self.session,
                e,
            )
            # Fall back to conservative behavior (attempt cache reads).
            self._cache_has_session_data = True

        return self._cache_has_session_data

    def _mark_session_cache_populated(self) -> None:
        """Mark that this session now has cacheable data persisted or scheduled."""
        if self.enable_cache:
            self._cache_has_session_data = True

    def _get_from_cache(self, cache_key: str):
        """Get data from cache if enabled."""
        if not self.enable_cache or not self._session_cache_available():
            return None
        return get_cache().get(cache_key)

    def _cache_result(self, cache_key: str, data: dict) -> None:
        """Cache data if caching is enabled."""
        if self.enable_cache:
            get_cache().set(cache_key, data)
            self._mark_session_cache_populated()

    def load(self, laps=True, telemetry=True, weather=True, messages=True):
        """Load session data based on requested data types.

        This method fetches only the data that is required based on the boolean parameters.
        By default, all data types are fetched (laps, telemetry, weather, and messages).

        Args:
            laps: If True, fetch laps data. Required for telemetry - if telemetry=True
                  and laps=False, laps will be automatically set to True.
            telemetry: If True, fetch telemetry data for all laps
            weather: If True, fetch weather data
            messages: If True, fetch race control messages

        Returns:
            self: The Session object for method chaining

        Example:
            >>> session = get_session(2025, "Silverstone Grand Prix", "Race")
            >>> # Fetch only laps and telemetry
            >>> session.load(laps=True, telemetry=True, weather=False, messages=False)
            >>> # Fetch everything (default)
            >>> session.load()
        """
        # Telemetry requires laps data, so ensure laps is True if telemetry is requested
        if telemetry and not laps:
            laps = True

        if laps:
            _ = self.laps
        if weather:
            _ = self.weather
        if messages:
            _ = self.race_control_messages
        if telemetry:
            # Fetch telemetry for all laps if telemetry is requested
            _ = self.fetch_all_laps_telemetry()
        return self

    def _get_local_payload(self, path: str) -> dict[str, Any] | None:
        """Get in-memory payload previously fetched during this session."""
        payload = self._local_json_payloads.get(path)
        if isinstance(payload, dict):
            return payload
        return None

    def _remember_local_payload(self, path: str, data: Any) -> None:
        """Store JSON payload in memory for subsequent access during this session."""
        if not isinstance(data, dict):
            return
        # Only store known payload types to avoid memory bloat
        known_paths = {
            "drivers.json",
            "rcm.json",
            "weather.json",
            "position.json",
            "car_data.json",
            "session_info.json",
        }
        # Also allow driver-specific paths like "VER/laptimes.json"
        if path in known_paths or "/" in path:
            self._local_json_payloads[path] = data

    def _remember_telemetry_payload(
        self, driver: str, lap_num: int, tel_payload: dict[str, Any]
    ) -> None:
        """Memoize telemetry payloads fetched in this session."""
        if isinstance(tel_payload, dict) and tel_payload:
            self._telemetry_payloads[(driver, lap_num)] = tel_payload

    def _get_telemetry_payload(self, driver: str, lap_num: int) -> dict[str, Any] | None:
        """Get memoized telemetry payload for (driver, lap)."""
        payload = self._telemetry_payloads.get((driver, lap_num))
        if isinstance(payload, dict):
            return payload
        return None

    def _prefetch_session_tables(self) -> None:
        """Batch-fetch session-level tables (weather, rcm, drivers) in parallel.

        Uses a dedicated HTTP session and thread pool to avoid corrupting the
        shared session state used by subsequent asyncio.run() calls.
        """
        if self._session_tables_prefetched:
            return
        self._session_tables_prefetched = True

        paths_to_fetch: list[str] = []
        if self._get_local_payload("weather.json") is None and self._weather is None:
            paths_to_fetch.append("weather.json")
        if self._get_local_payload("rcm.json") is None and self._race_control_messages is None:
            paths_to_fetch.append("rcm.json")
        if self._get_local_payload("drivers.json") is None and self._drivers is None:
            paths_to_fetch.append("drivers.json")

        if len(paths_to_fetch) < 2:
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
                    resp = http.get(url, timeout=timeout)
                    if resp.status_code == 404:
                        return None
                    resp.raise_for_status()
                    data = parse_response_json(resp)
                    return data if isinstance(data, dict) else None
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
                        except Exception:
                            pass
        except Exception as e:
            logger.debug("Session table prefetch failed: %s", e)

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
        fail_count = self._telemetry_failure_counts.get(driver, 0) + 1
        self._telemetry_failure_counts[driver] = fail_count
        if fail_count >= 3:
            self._telemetry_unavailable_drivers.add(driver)

        if fail_count <= 3:
            logger.warning("Failed telemetry load for %s lap %s: %s", driver, lap_num, error)
            return

        if driver not in self._telemetry_failure_suppressed_drivers:
            logger.warning(
                "Further telemetry load failures for %s are suppressed after 3 occurrences",
                driver,
            )
            self._telemetry_failure_suppressed_drivers.add(driver)
            return

        logger.debug("Suppressed telemetry load failure for %s lap %s: %s", driver, lap_num, error)

    def _should_skip_telemetry_fetch(self, driver: str) -> bool:
        """Return True when telemetry fetches should be short-circuited for a driver."""
        return driver in self._telemetry_unavailable_drivers

    def _resolve_ultra_cold_mode(self, ultra_cold: bool | None) -> bool:
        """Resolve whether ultra-cold mode should be enabled."""
        if ultra_cold is None:
            return bool(config.get("ultra_cold_start", False))
        return ultra_cold

    def _is_fastest_lap_tel_cold_start(self) -> bool:
        """Detect whether fastest-lap telemetry is being requested on a brand-new session."""
        return self._laps is None and self._fastest_lap_ref is None and self._drivers is None

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
            _ = self.drivers
            return self._drivers if self._drivers is not None else [], []

        try:
            drivers_payload = self._fetch_json_unvalidated("drivers.json")
        except (DataNotFoundError, InvalidDataError, NetworkError, TypeError, ValueError) as e:
            logger.debug("Ultra-cold driver fetch failed, falling back to validated path: %s", e)
            _ = self.drivers
            return self._drivers if self._drivers is not None else [], []

        drivers = drivers_payload.get("drivers") if isinstance(drivers_payload, dict) else None
        self._drivers = drivers if isinstance(drivers, list) else []
        self._refresh_driver_indices()
        if not self._drivers:
            logger.info(f"No drivers in session: {self.year}/{self.gp}/{self.session}")

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

                for driver, lap_num, tel_payload in telemetry_items:
                    cache.set_telemetry(year, gp, session, driver, lap_num, tel_payload)
                    cache.set(
                        f"{year}/{gp}/{session}/{driver}/{lap_num}_tel.json", {"tel": tel_payload}
                    )
            except (AttributeError, RuntimeError, TypeError, ValueError) as e:
                logger.debug("Background cache fill skipped: %s", e)

        threading.Thread(target=_worker, name="tif1-ultra-cold-cache-fill", daemon=True).start()

    def _fetch_json_unvalidated(self, path: str) -> dict[str, Any]:
        """Fetch JSON payload without validation/caching for ultra-cold paths."""
        local_payload = self._get_local_payload(path)
        if local_payload is not None:
            return local_payload

        use_fast_fetch = bool(config.get("ultra_cold_skip_retries", True))
        if use_fast_fetch:
            fetch_from_cdn_code = getattr(type(self)._fetch_from_cdn, "__code__", None)
            fetch_from_cdn_fast_code = getattr(type(self)._fetch_from_cdn_fast, "__code__", None)
            fetch_from_cdn_patched = fetch_from_cdn_code is not _SESSION_FETCH_FROM_CDN_CODE
            fetch_from_cdn_fast_patched = (
                fetch_from_cdn_fast_code is not _SESSION_FETCH_FROM_CDN_FAST_CODE
            )
            result = (
                self._fetch_from_cdn(path)
                if fetch_from_cdn_patched and not fetch_from_cdn_fast_patched
                else self._fetch_from_cdn_fast(path)
            )
        else:
            result = self._fetch_from_cdn(path)

        if isinstance(result, dict):
            self._remember_local_payload(path, result)
            return result
        if hasattr(result, "json"):
            if getattr(result, "status_code", None) == 404:
                raise DataNotFoundError(year=self.year, event=self.gp, session=self.session)
            if hasattr(result, "raise_for_status"):
                result.raise_for_status()
            data = parse_response_json(result)
            if isinstance(data, dict):
                self._remember_local_payload(path, data)
                return data
            raise InvalidDataError(reason=f"Expected dict, got {type(data).__name__}")

        raise InvalidDataError(reason=f"Expected dict, got {type(result).__name__}")

    def _fetch_from_cdn_fast(self, path: str) -> dict:
        """Fetch data from CDN without per-source retry/backoff delays."""

        def fetch_from_url(url: str) -> dict:
            from .http_session import _track_request

            response = _get_session().get(url, timeout=config.get("timeout", 30))
            _track_request(reused=True)

            if response.status_code == 404:
                raise DataNotFoundError(year=self.year, event=self.gp, session=self.session)
            response.raise_for_status()
            data = parse_response_json(response)
            if not isinstance(data, dict):
                raise InvalidDataError(reason=f"Expected dict, got {type(data).__name__}")
            return data

        cdn_manager = get_cdn_manager()
        return cdn_manager.try_sources(self.year, self.gp, self.session, path, fetch_from_url)

    def _fetch_from_cdn(self, path: str) -> dict:
        """Fetch data from CDN with retry logic."""

        @retry_with_backoff(
            max_retries=config.get("max_retries", 3),
            backoff_factor=config.get("retry_backoff_factor", 2.0),
            jitter=config.get("retry_jitter", True),
            exceptions=(niquests.RequestException,),
        )
        def fetch_from_url(url: str) -> dict:
            from .http_session import _track_request

            response = _get_session().get(url, timeout=config.get("timeout", 30))
            _track_request(reused=True)

            if response.status_code == 404:
                raise DataNotFoundError(year=self.year, event=self.gp, session=self.session)
            response.raise_for_status()
            data = parse_response_json(response)
            if not isinstance(data, dict):
                raise InvalidDataError(reason=f"Expected dict, got {type(data).__name__}")
            return data

        cdn_manager = get_cdn_manager()
        return cdn_manager.try_sources(self.year, self.gp, self.session, path, fetch_from_url)

    def _fetch_json(self, path: str) -> dict:
        """Fetch JSON data with caching, retry logic, and CDN fallback."""
        local_payload = self._get_local_payload(path)
        if local_payload is not None:
            return local_payload

        cache_key = f"{self.year}/{self.gp}/{self.session}/{path}"

        cached = self._get_from_cache(cache_key)
        if cached is not None:
            if isinstance(cached, dict):
                self._remember_local_payload(path, cached)
            return cached

        try:
            result = self._fetch_from_cdn(path)

            # Some tests patch `_fetch_from_cdn` to return a response-like object
            # (status_code, raise_for_status, json). Accept both dict and response.
            if isinstance(result, dict):
                data = result
            elif hasattr(result, "json"):
                if getattr(result, "status_code", None) == 404:
                    raise DataNotFoundError(year=self.year, event=self.gp, session=self.session)
                if hasattr(result, "raise_for_status"):
                    result.raise_for_status()
                data = parse_response_json(result)
            else:
                data = result

            # For normal operation, we expect dict payloads.
            # However, tests may fuzz/patch `_fetch_from_cdn` to return `None` or
            # non-dict JSON. In that case, pass through as-is.
            data = _validate_json_payload(path, data)
            if isinstance(data, dict):
                self._remember_local_payload(path, data)
                self._cache_result(cache_key, data)
            logger.info(f"Fetched: {cache_key}")
            return data
        except (DataNotFoundError, InvalidDataError, NetworkError, TypeError, ValueError) as e:
            if path.endswith("_tel.json"):
                logger.debug("Telemetry fetch failed for %s: %s", cache_key, e)
            else:
                logger.error(f"Failed to fetch {cache_key}: {e}")
            raise

    @property
    def drivers(self) -> list[str]:
        """Get list of driver numbers as strings (fastf1 API compatibility).

        Returns:
            List of driver numbers as strings (e.g., ['1', '11', '16', ...])
        """
        drivers_data = self._drivers_data
        if not drivers_data:
            return []

        driver_numbers = []
        for d in drivers_data:
            # Skip invalid entries
            if not isinstance(d, dict):
                continue
            # Get driver number from 'dn' field
            number = d.get("dn")
            if number:
                driver_numbers.append(str(number))

        return driver_numbers

    @property
    def _drivers_data(self) -> list[dict]:
        """Get list of driver dictionaries (internal use)."""
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
            self._drivers = data.get("drivers", [])
            if not self._drivers:
                logger.info(f"No drivers in session: {self.year}/{self.gp}/{self.session}")
        self._refresh_driver_indices()
        return self._drivers

    @property
    def driver_list(self) -> list[str]:
        """Get list of driver numbers as strings (fastf1 compatibility).

        Returns:
            List of driver numbers as strings (e.g., ['1', '11', '16', ...])
        """
        drivers_data = self._drivers_data
        if not drivers_data:
            return []

        driver_numbers = []
        for d in drivers_data:
            # Skip invalid entries
            if not isinstance(d, dict):
                continue
            number = d.get("dn", "")
            if number:
                driver_numbers.append(str(number))

        return driver_numbers

    @property
    def drivers_df(self) -> pd.DataFrame:
        """Get drivers as a pandas DataFrame.

        Returns:
            DataFrame with columns: Driver, Team, DriverNumber, FirstName, LastName, TeamColor, HeadshotUrl
        """
        drivers_list = self._drivers_data
        if not drivers_list:
            return pd.DataFrame(
                columns=[
                    "Driver",
                    "Team",
                    "DriverNumber",
                    "FirstName",
                    "LastName",
                    "TeamColor",
                    "HeadshotUrl",
                ]
            )

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
    def car_data(self) -> DataFrame:
        """Get complete telemetry data for all drivers as a DataFrame.

        Returns the same data as pos_data - complete telemetry including position,
        speed, and car data for all drivers across all laps.

        Returns:
            DataFrame with complete telemetry data including:
            - Time/SessionTime: timestamp
            - Driver: driver code
            - Speed: km/h
            - RPM: engine RPM
            - nGear: gear number
            - Throttle: 0-100
            - Brake: boolean or 0-100
            - DRS: DRS status
            - X, Y, Z: position coordinates
            - Distance: distance along track

        Example:
            >>> session = get_session(2024, "Monaco", "Race")
            >>> session.load()
            >>> car_data = session.car_data
            >>> print(car_data[["Driver", "Speed", "X", "Y"]].head())
        """
        if self._car_data is None:
            # Fetch telemetry for all drivers
            laps = self.laps
            if _is_empty_df(laps, self.lib):
                self._car_data = _create_empty_df(self.lib)
            else:
                # Collect telemetry from all laps
                all_telemetry = []
                for driver in laps["Driver"].unique():
                    driver_laps = laps[laps["Driver"] == driver]  # type: ignore[call-overload]
                    tel = driver_laps.telemetry
                    if not _is_empty_df(tel, self.lib):
                        all_telemetry.append(tel)

                if all_telemetry:
                    if self.lib == "polars":
                        self._car_data = pl.concat(all_telemetry, how="vertical")  # type: ignore[union-attr]
                    else:
                        self._car_data = pd.concat(all_telemetry, ignore_index=True)
                else:
                    self._car_data = _create_empty_df(self.lib)
        return self._car_data

    @property
    def pos_data(self) -> DataFrame:
        """Get complete telemetry data for all drivers as a DataFrame.

        Returns the same data as car_data - complete telemetry including position,
        speed, and car data for all drivers across all laps.

        Returns:
            DataFrame with complete telemetry data including:
            - Time/SessionTime: timestamp
            - Driver: driver code
            - Speed: km/h
            - RPM: engine RPM
            - nGear: gear number
            - Throttle: 0-100
            - Brake: boolean or 0-100
            - DRS: DRS status
            - X, Y, Z: position coordinates
            - Distance: distance along track

        Example:
            >>> session = get_session(2024, "Monaco", "Race")
            >>> session.load()
            >>> pos_data = session.pos_data
            >>> print(pos_data[["Driver", "X", "Y", "Speed"]].head())
        """
        # Both car_data and pos_data return the same complete telemetry
        return self.car_data

    @property
    def total_laps(self):
        return None  # Placeholder

    @property
    def results(self) -> "SessionResults":
        """Get session results with driver information."""
        if self._results is None:
            drivers_data = self._drivers_data
            if not drivers_data:
                return SessionResults()

            rows = []
            for d in drivers_data:
                row = {
                    "DriverNumber": str(d.get("dn", "")),
                    "Abbreviation": d.get("driver", ""),
                    "TeamName": d.get("team", ""),
                    "TeamColor": d.get("tc", ""),
                    "FirstName": d.get("fn", ""),
                    "LastName": d.get("ln", ""),
                    "FullName": f"{d.get('fn', '')} {d.get('ln', '')}".strip(),
                    "HeadshotUrl": d.get("url", ""),
                }
                # Add placeholders for result data (might not be available in drivers.json)
                row.update(
                    {
                        "Position": None,
                        "ClassifiedPosition": "",
                        "GridPosition": None,
                        "Status": "Finished",
                        "Points": 0.0,
                        "Laps": 0.0,
                    }
                )
                rows.append(row)

            self._results = SessionResults(rows)
            self._results.session = self
        return self._results

    @property
    def race_control_messages(self) -> DataFrame:
        """Get session race control messages."""
        if self._race_control_messages is None:
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
                                .str.to_datetime(format=None, strict=False)
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
                                .cast(pl.Float64)
                                .alias("Sector")
                            )
                        # Lap: int64
                        if "Lap" in cols:
                            exprs.append(pl.col("Lap").cast(pl.Int64))
                        if exprs:
                            self._race_control_messages = rcm_pl.with_columns(exprs)
                    else:
                        rcm_pd = cast(pd.DataFrame, self._race_control_messages)
                        # Time: ISO string → datetime64[ns]
                        if "Time" in rcm_pd.columns:
                            rcm_pd["Time"] = pd.to_datetime(
                                rcm_pd["Time"], errors="coerce", utc=False
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
                        # Lap: int64 (use nullable Int64 to tolerate any NaNs)
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
                    self.gp,
                    self.session,
                )
                self._race_control_messages = _create_empty_df(self.lib)
            except (
                InvalidDataError,
                NetworkError,
                RuntimeError,
                TypeError,
                ValueError,
            ) as e:
                logger.warning(
                    "Failed to load race control messages for %s/%s/%s: %s",
                    self.year,
                    self.gp,
                    self.session,
                    e,
                )
                self._race_control_messages = _create_empty_df(self.lib)
        return self._race_control_messages

    @property
    def session_info(self) -> dict[str, Any]:
        return {
            "Year": self.year,
            "EventName": self.gp.replace("%20", " "),
            "SessionName": self.session.replace("%20", " "),
        }

    @property
    def session_start_time(self):
        return pd.NaT

    @property
    def t0_date(self):
        return self.session_start_time

    @property
    def session_status(self):
        if not _is_empty_df(self.race_control_messages, self.lib):
            return self.race_control_messages
        return pd.DataFrame(columns=["Time", "Status"])

    @property
    def track_status(self):
        laps = self.laps
        if _is_empty_df(laps, self.lib):
            return pd.Series(dtype=object)
        if self.lib == "polars":
            laps_pl = cast(Any, laps)
            if "TrackStatus" not in laps_pl.columns:
                return pd.Series(dtype=object)
            return laps_pl.select("TrackStatus").to_pandas()["TrackStatus"]
        laps_pd = cast(pd.DataFrame, laps)
        if "TrackStatus" not in laps_pd.columns:
            return pd.Series(dtype=object)
        return laps_pd["TrackStatus"]

    @property
    def name(self) -> str:
        """Get session name (decoded from URL encoding).

        Returns:
            Session name (e.g., "Practice 1", "Qualifying", "Race")
        """
        return self.session.replace("%20", " ")

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
                if date_key in event.index:
                    ts = pd.Timestamp(event[date_key])
                    return ts.tz_localize(None) if ts.tz else ts

        return pd.NaT

    @property
    def event(self):
        """Placeholder for Event object."""
        # We will implement Event properly in events.py
        from .events import get_event_by_name

        return get_event_by_name(self.year, self.gp.replace("%20", " "))

    @property
    def weather(self) -> DataFrame:
        """Get session weather data."""
        if self._weather is None:
            self._prefetch_session_tables()
            try:
                self._weather = self._load_session_table("weather.json", WEATHER_RENAME_MAP)
                if not _is_empty_df(self._weather, self.lib):
                    if self.lib == "polars":
                        import polars as pl

                        weather_pl = cast(Any, self._weather)
                        cols = weather_pl.columns
                        exprs = []
                        # Time: seconds float → Duration
                        if "Time" in cols:
                            exprs.append(
                                pl.col("Time").cast(pl.Float64).mul(1000).cast(pl.Duration("ms"))
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
                        exprs.extend([pl.col(_col).cast(pl.Float64) for _col in float_cols])
                        # Int column
                        if "WindDirection" in cols:
                            exprs.append(pl.col("WindDirection").cast(pl.Int64))
                        # Bool column
                        if "Rainfall" in cols:
                            exprs.append(pl.col("Rainfall").cast(pl.Boolean))
                        if exprs:
                            self._weather = weather_pl.with_columns(exprs)
                    else:
                        weather_pd = cast(pd.DataFrame, self._weather)
                        # Time: seconds float → timedelta
                        if "Time" in weather_pd.columns:
                            weather_pd["Time"] = _numeric_seconds_to_timedelta(
                                cast(pd.Series, weather_pd["Time"])
                            )
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
                logger.info("No weather in session: %s/%s/%s", self.year, self.gp, self.session)
                self._weather = _create_empty_df(self.lib)
            except (
                InvalidDataError,
                NetworkError,
                RuntimeError,
                TypeError,
                ValueError,
            ) as e:
                logger.warning(
                    "Failed to load weather for %s/%s/%s: %s",
                    self.year,
                    self.gp,
                    self.session,
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
        """Return FastF1-compatible circuit info from corners.json.

        Returns a :class:`CircuitInfo` dataclass with the same attributes as
        ``fastf1.mvapi.CircuitInfo``:

        * ``corners`` - DataFrame with columns
          ``X, Y, Number, Letter, Angle, Distance``.
        * ``marshal_lights`` - always an empty DataFrame (not in source data).
        * ``marshal_sectors`` - always an empty DataFrame (not in source data).
        * ``rotation`` - circuit rotation in degrees.

        Results are cached on the session object after the first call.

        Returns:
            :class:`CircuitInfo` instance.
        """
        if self._circuit_info is None:
            self._circuit_info = self._build_circuit_info()
        return self._circuit_info

    def _build_circuit_info(self) -> "CircuitInfo":
        """Fetch corners.json and build a :class:`CircuitInfo` dataclass."""
        _empty = pd.DataFrame(columns=_CORNERS_DF_COLUMNS)

        try:
            data = self._fetch_json("corners.json")
        except DataNotFoundError:
            logger.info("No corners.json for %s/%s/%s", self.year, self.gp, self.session)
            return CircuitInfo(
                corners=_empty.copy(),
                marshal_lights=_empty.copy(),
                marshal_sectors=_empty.copy(),
                rotation=0.0,
            )
        except (InvalidDataError, NetworkError, RuntimeError, TypeError, ValueError) as e:
            logger.warning(
                "Failed to load circuit info for %s/%s/%s: %s",
                self.year,
                self.gp,
                self.session,
                e,
            )
            return CircuitInfo(
                corners=_empty.copy(),
                marshal_lights=_empty.copy(),
                marshal_sectors=_empty.copy(),
                rotation=0.0,
            )

        if not isinstance(data, dict):
            return CircuitInfo(
                corners=_empty.copy(),
                marshal_lights=_empty.copy(),
                marshal_sectors=_empty.copy(),
                rotation=0.0,
            )

        # Top-level scalar – Rotation is NOT a list, unlike the other fields.
        _rot_raw = data.get("Rotation", 0.0)
        rotation: float = 0.0
        try:
            if _rot_raw not in (None, "None"):
                rotation = float(_rot_raw)
        except (TypeError, ValueError):
            pass

        corner_nums = data.get("CornerNumber", []) or []
        xs = data.get("X", []) or []
        ys = data.get("Y", []) or []
        angles = data.get("Angle", []) or []
        distances = data.get("Distance", []) or []

        def _to_float(raw) -> float:
            if raw is None or raw == "None":
                return float("nan")
            try:
                return float(raw)
            except (TypeError, ValueError):
                return float("nan")

        def _to_int(raw) -> int:
            if raw is None or raw == "None":
                return 0
            try:
                return int(raw)
            except (TypeError, ValueError):
                return 0

        n = len(corner_nums)
        rows = [
            {
                "X": _to_float(xs[i] if i < len(xs) else None),
                "Y": _to_float(ys[i] if i < len(ys) else None),
                "Number": _to_int(corner_nums[i]),
                "Letter": "",  # not present in corners.json
                "Angle": _to_float(angles[i] if i < len(angles) else None),
                "Distance": _to_float(distances[i] if i < len(distances) else None),
            }
            for i in range(n)
        ]

        if rows:
            corners_df = pd.DataFrame(rows, columns=_CORNERS_DF_COLUMNS)
            corners_df["Number"] = corners_df["Number"].astype("int64")
            corners_df["Letter"] = corners_df["Letter"].astype(object)
            for _col in ("X", "Y", "Angle", "Distance"):
                corners_df[_col] = corners_df[_col].astype("float64")
        else:
            corners_df = _empty.copy()

        return CircuitInfo(
            corners=corners_df,
            marshal_lights=_empty.copy(),
            marshal_sectors=_empty.copy(),
            rotation=rotation,
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
        """Fetch drivers and target driver laps concurrently on first driver lookup."""
        if self._drivers is not None:
            return None
        if not bool(config.get("prefetch_driver_laps_on_get_driver", True)):
            return None

        requests = [
            (self.year, self.gp, self.session, "drivers.json"),
            (self.year, self.gp, self.session, f"{driver}/laptimes.json"),
        ]
        ultra_cold_enabled = self._resolve_ultra_cold_mode(None)
        use_cache = self.enable_cache and not ultra_cold_enabled and self._session_cache_available()
        write_cache = self.enable_cache and not ultra_cold_enabled
        validate_payload = not ultra_cold_enabled
        max_retries = (
            1 if (ultra_cold_enabled and config.get("ultra_cold_skip_retries", True)) else None
        )

        try:
            _ensure_nested_loop_support("get_driver")
            try:
                results = asyncio.run(
                    fetch_multiple_async(
                        requests,
                        use_cache=use_cache,
                        write_cache=write_cache,
                        validate_payload=validate_payload,
                        max_retries=max_retries,
                    )
                )
            except TypeError as e:
                if "unexpected keyword argument" not in str(e):
                    raise
                results = asyncio.run(fetch_multiple_async(requests))
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
                self._drivers = drivers_payload
                self._refresh_driver_indices()

        if len(results) > 1 and isinstance(results[1], dict):
            self._remember_local_payload(f"{driver}/laptimes.json", results[1])
            return results[1]
        return None

    def _has_driver_code(self, driver: str) -> bool:
        """Check whether a driver code exists for this session."""
        _ = self._drivers_data
        if self._driver_codes is None:
            return False
        return driver in self._driver_codes

    def _get_driver_info(self, driver: str) -> dict:
        """Get cached driver metadata by code."""
        _ = self._drivers_data
        if self._driver_info_by_code is None:
            return {"driver": driver, "team": ""}
        return self._driver_info_by_code.get(driver, {"driver": driver, "team": ""})

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

        # If no data, try to fetch it
        if data is None:
            try:
                data = self._fetch_json_unvalidated(path) if ultra_cold else self._fetch_json(path)
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

    def _fetch_laptime_payloads(
        self,
        driver_requests: list[tuple[dict[str, Any], str]],
        *,
        operation: str,
        ultra_cold: bool = False,
    ) -> tuple[list[dict[str, Any] | None], list[tuple[str, dict[str, Any]]]]:
        """Fetch laptime payloads in parallel with sequential fallback."""
        if not driver_requests:
            return [], []

        use_cache = self.enable_cache and not ultra_cold and self._session_cache_available()
        write_cache = self.enable_cache and not ultra_cold
        validate_payload = not ultra_cold
        max_retries = 1 if (ultra_cold and config.get("ultra_cold_skip_retries", True)) else None

        normalized_results: list[dict[str, Any] | None] = [None] * len(driver_requests)
        pending_requests: list[tuple[int, str, str, str]] = []
        pending_indexes: list[int] = []

        for idx, (_driver_info, path) in enumerate(driver_requests):
            local_payload = self._get_local_payload(path)
            if local_payload is not None:
                normalized_results[idx] = local_payload
                continue
            pending_indexes.append(idx)
            pending_requests.append((self.year, self.gp, self.session, path))

        pending_results: list[dict[str, Any] | None] = []
        if pending_requests:
            try:
                _ensure_nested_loop_support(operation)
                try:
                    pending_results = asyncio.run(
                        fetch_multiple_async(
                            pending_requests,
                            use_cache=use_cache,
                            write_cache=write_cache,
                            validate_payload=validate_payload,
                            max_retries=max_retries,
                        )
                    )
                except TypeError as e:
                    # Some tests monkeypatch fetch_multiple_async with a simplified signature.
                    if "unexpected keyword argument" not in str(e):
                        raise
                    pending_results = asyncio.run(fetch_multiple_async(pending_requests))
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

        cacheable_payloads: list[tuple[str, dict[str, Any]]] = []

        for idx, (_driver_info, path) in enumerate(driver_requests):
            data = normalized_results[idx]
            processed_payload, cacheable_entry = self._process_laptime_payload(
                data, path, ultra_cold=ultra_cold
            )
            normalized_results[idx] = processed_payload
            if cacheable_entry is not None:
                cacheable_payloads.append(cacheable_entry)

        if write_cache and any(isinstance(item, dict) for item in pending_results):
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

        # If no data, try to fetch it
        if data is None:
            try:
                data = self._fetch_json_unvalidated(path) if ultra_cold else self._fetch_json(path)
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
    ) -> tuple[list[dict[str, Any] | None], list[tuple[str, dict[str, Any]]]]:
        """Async variant of laptime fetch with memoization-first behavior."""
        if not driver_requests:
            return [], []

        use_cache = self.enable_cache and not ultra_cold and self._session_cache_available()
        write_cache = self.enable_cache and not ultra_cold
        validate_payload = not ultra_cold
        max_retries = 1 if (ultra_cold and config.get("ultra_cold_skip_retries", True)) else None

        normalized_results: list[dict[str, Any] | None] = [None] * len(driver_requests)
        pending_requests: list[tuple[int, str, str, str]] = []
        pending_indexes: list[int] = []

        for idx, (_driver_info, path) in enumerate(driver_requests):
            local_payload = self._get_local_payload(path)
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

        cacheable_payloads: list[tuple[str, dict[str, Any]]] = []

        for idx, (_driver_info, path) in enumerate(driver_requests):
            data = normalized_results[idx]
            processed_payload, cacheable_entry = self._process_laptime_payload(
                data, path, ultra_cold=ultra_cold
            )
            normalized_results[idx] = processed_payload
            if cacheable_entry is not None:
                cacheable_payloads.append(cacheable_entry)

        if write_cache and any(isinstance(item, dict) for item in pending_results):
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
            sorted by lap time (fastest first)
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

        valid_laps: list[tuple[int, int, float]] = []
        for idx, (lap_value, lap_time_value) in enumerate(zip(lap_values, time_values)):
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
            pl.DataFrame(fastest_rows, strict=False)  # type: ignore[union-attr]
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
            pl.DataFrame(fastest_rows, strict=False)  # type: ignore[union-attr]
            if self.lib == "polars"
            else pd.DataFrame(fastest_rows, copy=False)
        )
        fastest_df = _process_lap_df(fastest_df, self.lib)
        return self._select_fastest_laps(fastest_df, by_driver=by_driver)

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
            if self._fastest_lap_ref_laps_source_id == laps_source_id:
                return self._fastest_lap_ref

            fastest_lap_ref = self._extract_fastest_lap_from_loaded_laps()
            self._fastest_lap_ref = fastest_lap_ref
            self._fastest_lap_ref_laps_source_id = (
                laps_source_id if fastest_lap_ref is not None else None
            )
            self._fastest_lap_ref_driver_source_id = None
            return self._fastest_lap_ref

        drivers, driver_payloads = self._load_drivers_for_fastest_lap_reference(
            ultra_cold=ultra_cold
        )
        if driver_payloads and self._should_backfill_ultra_cold_cache(ultra_cold):
            self._schedule_background_cache_fill(json_payloads=driver_payloads)

        driver_source_id = id(drivers)
        if self._fastest_lap_ref_driver_source_id == driver_source_id:
            return self._fastest_lap_ref

        fastest_lap_ref = self._find_fastest_lap_reference_from_raw(drivers, ultra_cold=ultra_cold)
        self._fastest_lap_ref = fastest_lap_ref
        self._fastest_lap_ref_driver_source_id = (
            driver_source_id if fastest_lap_ref is not None else None
        )
        self._fastest_lap_ref_laps_source_id = None
        return self._fastest_lap_ref

    async def _get_fastest_lap_reference_async(
        self, *, ultra_cold: bool = False
    ) -> tuple[str, int] | None:
        """Async variant of overall fastest-lap metadata resolution."""
        if self._laps is not None:
            laps_source_id = id(self._laps)
            if self._fastest_lap_ref_laps_source_id == laps_source_id:
                return self._fastest_lap_ref

            fastest_lap_ref = self._extract_fastest_lap_from_loaded_laps()
            self._fastest_lap_ref = fastest_lap_ref
            self._fastest_lap_ref_laps_source_id = (
                laps_source_id if fastest_lap_ref is not None else None
            )
            self._fastest_lap_ref_driver_source_id = None
            return self._fastest_lap_ref

        drivers, driver_payloads = self._load_drivers_for_fastest_lap_reference(
            ultra_cold=ultra_cold
        )
        if driver_payloads and self._should_backfill_ultra_cold_cache(ultra_cold):
            self._schedule_background_cache_fill(json_payloads=driver_payloads)

        driver_source_id = id(drivers)
        if self._fastest_lap_ref_driver_source_id == driver_source_id:
            return self._fastest_lap_ref

        fastest_lap_ref = await self._find_fastest_lap_reference_from_raw_async(
            drivers,
            ultra_cold=ultra_cold,
        )
        self._fastest_lap_ref = fastest_lap_ref
        self._fastest_lap_ref_driver_source_id = (
            driver_source_id if fastest_lap_ref is not None else None
        )
        self._fastest_lap_ref_laps_source_id = None
        return self._fastest_lap_ref

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
        if self._laps is None:
            cache_key = f"{self.year}_{self.gp}_{self.session}_laps"
            lap_cache = _get_backend_lap_cache(self.lib) if self.enable_cache else None
            if lap_cache is not None:
                cached_laps = lap_cache.get(cache_key)
                if cached_laps is not None:
                    logger.info(f"Lap cache hit ({self.lib}): {cache_key}")
                    self._laps = cached_laps
                    return self._laps

            logger.info(f"Loading laps async ({self.lib}): {cache_key}")
            _ensure_nested_loop_support("laps property")
            laps_df = asyncio.run(self.laps_async())
            if self.lib == "pandas":
                self._laps = Laps(cast(pd.DataFrame, laps_df))
                self._laps.session = self
            else:
                self._laps = laps_df
            if lap_cache is not None:
                lap_cache.set(cache_key, self._laps)
            self._maybe_start_background_telemetry_prefetch()

        return self._laps

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

        ultra_cold_enabled = self._resolve_ultra_cold_mode(None)
        drivers = self._drivers_data
        if not drivers:
            logger.info(f"No drivers, returning empty: {self.year}/{self.gp}")
            return _create_empty_df(self.lib)

        driver_requests = self._build_driver_laptime_requests(driver_pool=drivers)
        if not driver_requests:
            logger.info(f"No valid driver requests, returning empty: {self.year}/{self.gp}")
            return _create_empty_df(self.lib)

        payloads, ultra_cold_payloads = await self._fetch_laptime_payloads_async(
            driver_requests,
            operation="laps_async",
            ultra_cold=ultra_cold_enabled,
        )

        laps_data = []
        for (driver_info, _path), lap_data in zip(driver_requests, payloads):
            if not isinstance(lap_data, dict) or not lap_data:
                continue
            driver_code = driver_info.get("driver", "")
            try:
                lap_df = _create_lap_df(
                    lap_data,
                    driver_code,
                    driver_info.get("team", ""),
                    self.lib,
                )
                laps_data.append(lap_df)
            except (KeyError, TypeError, ValueError, InvalidDataError) as e:
                logger.warning(f"Failed to process lap data for {driver_code}: {e}")

        if not laps_data:
            logger.info(f"No valid lap data: {self.year}/{self.gp}")
            return _create_empty_df(self.lib)

        if self.lib == "polars":
            self._laps = pl.concat(laps_data, how="vertical_relaxed", rechunk=False)  # type: ignore[union-attr]
        else:
            self._laps = pd.concat(laps_data, ignore_index=True, copy=False)
            # Remove duplicate columns if they exist (can happen if upstream data has Driver/Team)
            if isinstance(self._laps.columns, pd.Index) and self._laps.columns.duplicated().any():
                # Keep only the first occurrence of each column name
                self._laps = self._laps.loc[:, ~self._laps.columns.duplicated()]

        self._laps = _process_lap_df(self._laps, self.lib)

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
            logger.info(f"No drivers found: {self.year}/{self.gp}")
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
            # If it looks like a driver number, resolve to driver code first
            if value.isdigit():
                for info in self._drivers_data:
                    if not isinstance(info, dict):
                        continue
                    number = info.get("dn")
                    code = info.get("driver")
                    if str(number) == value and isinstance(code, str) and code.strip():
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

    def get_driver(self, driver: Any) -> "Driver":
        """
        Get driver-specific data.

        Args:
            driver: Driver code (e.g., "VER", "HAM")

        Returns:
            Driver object

        Raises:
            TypeError: If driver is not a string
            ValueError: If driver is empty
            DriverNotFoundError: If driver not found in session
        """
        driver_code = self._coerce_driver_code(driver)
        prefetched_laps = self._prefetch_driver_lookup_and_laps(driver_code)
        # Enforce API contract by validating against the session driver list.
        # This may trigger one network fetch when drivers are not loaded yet.
        if not self._has_driver_code(driver_code):
            raise DriverNotFoundError(
                driver=driver_code, year=self.year, event=self.gp, session=self.session
            )

        return Driver(self, driver_code, prefetched_lap_data=prefetched_laps)  # type: ignore[return-value]

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

        # Filter by drivers first to reduce data processing.
        if drivers:
            if self.lib == "polars":
                laps_pl = cast(Any, laps)
                laps = laps_pl.filter(pl.col(COL_DRIVER).is_in(drivers))  # type: ignore[union-attr]
            else:
                laps_pd = cast(pd.DataFrame, laps)
                driver_list_str = ", ".join(f"'{d}'" for d in drivers)
                laps = laps_pd.query(f"{COL_DRIVER} in [{driver_list_str}]", engine="python")
            if _is_empty_df(laps, self.lib):
                return _create_empty_df(self.lib)

        valid = _filter_valid_laptimes(laps, self.lib)
        if _is_empty_df(valid, self.lib):
            return _create_empty_df(self.lib)

        sort_col = self._lap_time_sort_column(valid)
        if self.lib == "polars":
            return (
                valid.group_by(COL_DRIVER).agg(pl.all().sort_by(sort_col).first()).sort(sort_col)  # type: ignore[union-attr]
                if by_driver
                else valid.sort(sort_col).head(1)
            )

        valid_pd = cast(pd.DataFrame, valid)
        if by_driver:
            result = valid_pd.loc[valid_pd.groupby(COL_DRIVER, observed=True)[sort_col].idxmin()]
            return result.sort_values(sort_col).reset_index(drop=True)
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
                return fastest_from_raw

        laps = self.laps
        return self._select_fastest_laps(laps, by_driver=by_driver, drivers=drivers)

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
                return fastest_from_raw

        laps = await self.laps_async()
        return self._select_fastest_laps(laps, by_driver=by_driver, drivers=drivers)

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
            if memoized_tel is not None:
                cached_df = _create_telemetry_df(memoized_tel, driver, lap_num, self.lib)
                if cached_df is not None:
                    tels.append(cached_df)
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
            if isinstance(cached_tel, dict) and cached_tel:
                self._remember_telemetry_payload(driver, lap_num, cached_tel)
                cached_df = _create_telemetry_df(cached_tel, driver, lap_num, self.lib)
                if cached_df is not None:
                    tels.append(cached_df)
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
            - tels: List of DataFrames for cache hits
        """
        cache = None
        if not skip_cache and (not self.enable_cache or self._session_cache_available()):
            cache = get_cache()

        requests, lap_info, tels = [], [], []

        # First check in-memory memoized payloads (synchronous, fast)
        cache_check_refs = []
        for driver, lap_num in fastest_refs:
            memoized_tel = self._get_telemetry_payload(driver, lap_num)
            if memoized_tel is not None:
                cached_df = _create_telemetry_df(memoized_tel, driver, lap_num, self.lib)
                if cached_df is not None:
                    tels.append(cached_df)
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

            if isinstance(telemetry_data, dict) and telemetry_data:
                self._remember_telemetry_payload(driver, lap_num, telemetry_data)
                cached_df = _create_telemetry_df(telemetry_data, driver, lap_num, self.lib)
                if cached_df is not None:
                    tels.append(cached_df)
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
            _ensure_nested_loop_support("bulk telemetry prefetch")

            max_retries = (
                1 if (ultra_cold and config.get("ultra_cold_skip_retries", True)) else None
            )
            max_concurrent = max(
                1,
                config.get("telemetry_prefetch_max_concurrent_requests", 128),
            )
            try:
                results = asyncio.run(
                    fetch_multiple_async(
                        requests,
                        use_cache=not ultra_cold,
                        write_cache=False,
                        validate_payload=not ultra_cold,
                        max_retries=max_retries,
                        max_concurrent_requests=max_concurrent,
                    )
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
        """Pre-create all telemetry DataFrames from memoized payloads in a single batch."""
        payloads = self._telemetry_payloads
        if not payloads:
            return

        from .core_utils.constants import TELEMETRY_RENAME_MAP

        merged_cols: dict[str, list] = {}
        driver_col: list[str] = []
        lap_col: list[int] = []
        key_ranges: list[tuple[tuple[str, int], int, int]] = []
        offset = 0

        for (driver, lap_num), tel_payload in payloads.items():
            if (driver, lap_num) in self._telemetry_df_cache:
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
            if self.lib == "pandas":
                telemetry = Telemetry(frame, copy=False)
                telemetry.session = self
                telemetry.driver = key[0]
                self._telemetry_df_cache[key] = telemetry
            else:
                self._telemetry_df_cache[key] = frame

    def _maybe_start_background_telemetry_prefetch(self) -> None:
        """Kick off all-laps telemetry prefetch after laps are available."""
        if self._telemetry_background_prefetch_started:
            return
        if not bool(config.get("prefetch_all_telemetry_after_laps_load", True)):
            return
        if not bool(config.get("prefetch_all_telemetry_on_first_lap_request", True)):
            return
        if self._laps is None or _is_empty_df(self._laps, self.lib):
            return
        if self._telemetry_bulk_prefetch_done or self._telemetry_bulk_prefetch_attempted:
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
        self, telemetry_frames: list[DataFrame], fastest_ref: tuple[str, int]
    ) -> DataFrame | None:
        """Find a telemetry frame in-memory matching the given fastest-lap reference."""
        driver, lap_num = fastest_ref
        for telemetry_df in telemetry_frames:
            if _is_empty_df(telemetry_df, self.lib):
                continue

            if self.lib == "polars":
                telemetry_df_pl = cast(Any, telemetry_df)
                if (
                    COL_DRIVER not in telemetry_df_pl.columns
                    or COL_LAP_NUMBER not in telemetry_df_pl.columns
                ):
                    continue
                first_driver, first_lap = telemetry_df_pl.select([COL_DRIVER, COL_LAP_NUMBER]).row(
                    0
                )
            else:
                telemetry_df_pd = cast(pd.DataFrame, telemetry_df)
                if (
                    COL_DRIVER not in telemetry_df_pd.columns
                    or COL_LAP_NUMBER not in telemetry_df_pd.columns
                ):
                    continue
                first_row = telemetry_df_pd.iloc[0]
                first_driver = first_row[COL_DRIVER]
                first_lap = first_row[COL_LAP_NUMBER]

            if first_driver != driver:
                continue
            try:
                if _coerce_lap_number(first_lap) == lap_num:
                    return telemetry_df
            except ValueError:
                continue
        return None

    def _hydrate_fastest_lap_tel_from_batch(
        self,
        telemetry_frames: list[DataFrame],
        fastest_ref: tuple[str, int] | None,
    ) -> None:
        """Hydrate overall fastest-lap telemetry cache from already-fetched batch telemetry."""
        if fastest_ref is None:
            return

        fastest_tel_df = self._find_telemetry_df_for_ref(telemetry_frames, fastest_ref)
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

                telemetry_df = _create_telemetry_df(tel_payload, driver, lap_num, self.lib)
                if telemetry_df is not None:
                    tels.append(telemetry_df)
            except (InvalidDataError, NetworkError, TypeError, ValueError) as e:
                logger.warning(f"Failed to process telemetry for {driver} lap {lap_num}: {e}")
                continue

        if telemetry_backfill_payloads:
            self._schedule_background_cache_fill(telemetry_payloads=telemetry_backfill_payloads)

        return tels

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
                _ensure_nested_loop_support("get_fastest_laps_tels")
                if ultra_cold_enabled:
                    max_retries = 1 if config.get("ultra_cold_skip_retries", True) else None
                    try:
                        results = asyncio.run(
                            fetch_multiple_async(
                                requests,
                                use_cache=False,
                                write_cache=False,
                                validate_payload=False,
                                max_retries=max_retries,
                            )
                        )
                    except TypeError as e:
                        if "unexpected keyword argument" not in str(e):
                            raise
                        results = asyncio.run(fetch_multiple_async(requests))
                else:
                    results = asyncio.run(fetch_multiple_async(requests))
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

        return (
            pl.concat(tels, how="vertical_relaxed", rechunk=False)  # type: ignore[union-attr]
            if self.lib == "polars"
            else pd.concat(tels, ignore_index=True, copy=False)
        )

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
                            write_cache=False,
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

        return (
            pl.concat(tels, how="vertical_relaxed", rechunk=False)  # type: ignore[union-attr]
            if self.lib == "polars"
            else pd.concat(tels, ignore_index=True, copy=False)
        )

    def _get_telemetry_df_for_ref(
        self, driver: str, lap_num: int, *, ultra_cold: bool, allow_prefetch: bool = True
    ) -> DataFrame:
        """Get telemetry DataFrame for a specific (driver, lap) reference."""

        def _as_pandas_telemetry(frame: DataFrame) -> DataFrame:
            if self.lib != "pandas":
                return frame
            if isinstance(frame, Telemetry):
                frame.session = self
                frame.driver = driver
                return frame
            telemetry = Telemetry(cast(pd.DataFrame, frame), copy=False)
            telemetry.session = self
            telemetry.driver = driver
            return telemetry

        cached_df = self._telemetry_df_cache.get((driver, lap_num))
        if cached_df is not None:
            return _as_pandas_telemetry(cached_df)

        memoized_tel = self._get_telemetry_payload(driver, lap_num)
        if memoized_tel is not None:
            memoized_df = _create_telemetry_df(memoized_tel, driver, lap_num, self.lib)
            if memoized_df is not None:
                wrapped_df = _as_pandas_telemetry(memoized_df)
                self._telemetry_df_cache[(driver, lap_num)] = wrapped_df
                return wrapped_df

        if allow_prefetch:
            self._prefetch_all_loaded_laps_telemetry(ultra_cold=ultra_cold)
            memoized_tel = self._get_telemetry_payload(driver, lap_num)
            if memoized_tel is not None:
                memoized_df = _create_telemetry_df(memoized_tel, driver, lap_num, self.lib)
                if memoized_df is not None:
                    wrapped_df = _as_pandas_telemetry(memoized_df)
                    self._telemetry_df_cache[(driver, lap_num)] = wrapped_df
                    return wrapped_df

        cache = None
        if self.enable_cache and not ultra_cold and self._session_cache_available():
            cache = get_cache()
            cached_tel = cache.get_telemetry(self.year, self.gp, self.session, driver, lap_num)
            if isinstance(cached_tel, dict) and cached_tel:
                self._remember_telemetry_payload(driver, lap_num, cached_tel)
                cached_df_from_cache = _create_telemetry_df(cached_tel, driver, lap_num, self.lib)
                if cached_df_from_cache is not None:
                    wrapped_df = _as_pandas_telemetry(cached_df_from_cache)
                    self._telemetry_df_cache[(driver, lap_num)] = wrapped_df
                    return wrapped_df

        if self._should_skip_telemetry_fetch(driver):
            return _as_pandas_telemetry(_create_empty_df(self.lib))

        try:
            if ultra_cold:
                tel_data = self._fetch_json_unvalidated(f"{driver}/{lap_num}_tel.json")
            else:
                tel_data = self._fetch_json(f"{driver}/{lap_num}_tel.json")
        except (DataNotFoundError, InvalidDataError, NetworkError, TypeError, ValueError) as e:
            logger.debug("Telemetry fetch failed for %s lap %s: %s", driver, lap_num, e)
            raise

        if not tel_data or not isinstance(tel_data, dict):
            return _create_empty_df(self.lib)

        tel_payload = tel_data.get("tel")
        if not isinstance(tel_payload, dict) or not tel_payload:
            return _create_empty_df(self.lib)

        self._remember_telemetry_payload(driver, lap_num, tel_payload)
        if self.enable_cache:
            if ultra_cold and self._should_backfill_ultra_cold_cache(ultra_cold):
                self._schedule_background_cache_fill(
                    telemetry_payload=(driver, lap_num, tel_payload)
                )
            elif not ultra_cold:
                if cache is None:
                    cache = get_cache()
                cache.set_telemetry(self.year, self.gp, self.session, driver, lap_num, tel_payload)
                self._mark_session_cache_populated()

        tel_df = _create_telemetry_df(tel_payload, driver, lap_num, self.lib)
        if tel_df is not None:
            wrapped_df = _as_pandas_telemetry(tel_df)
            self._telemetry_df_cache[(driver, lap_num)] = wrapped_df
            return wrapped_df
        return _create_empty_df(self.lib)

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
                            write_cache=False,
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

        tel_df = tels[0]
        self._fastest_lap_tel_ref = fastest_lap_ref
        self._fastest_lap_tel_df = tel_df
        return tel_df

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
        ultra_cold_enabled = self._resolve_telemetry_ultra_cold_mode(ultra_cold)

        # Get all laps first
        laps = await self.laps_async()
        if _is_empty_df(laps, self.lib):
            return {}

        # Extract all (driver, lap_number) references
        lap_refs: list[tuple[str, int]] = []
        if self.lib == "polars":
            laps_pl = cast(Any, laps)
            for row in laps_pl.iter_rows(named=True):
                driver = row.get("Driver")
                lap_num = row.get("LapNumber")
                if driver and lap_num is not None:
                    lap_refs.append((str(driver), int(lap_num)))
        else:
            for _, row in laps.iterrows():  # type: ignore[union-attr]
                driver = row.get("Driver")
                lap_num = row.get("LapNumber")
                if pd.notna(driver) and pd.notna(lap_num):
                    lap_refs.append((str(driver), int(lap_num)))

        if not lap_refs:
            return {}

        # Fetch telemetry in batch
        requests, lap_info, cached_tels = await self._fetch_telemetry_batch_from_refs_async(
            lap_refs, skip_cache=ultra_cold_enabled
        )

        # Build result map from cached telemetry
        telemetry_map: dict[tuple[str, int], DataFrame] = {}
        for tel_df in cached_tels:
            if not _is_empty_df(tel_df, self.lib):
                if self.lib == "polars":
                    driver = tel_df["Driver"][0]
                    lap_num = tel_df["LapNumber"][0]
                else:
                    driver = tel_df["Driver"].iloc[0]
                    lap_num = tel_df["LapNumber"].iloc[0]
                telemetry_map[(str(driver), int(lap_num))] = tel_df

        # Fetch remaining telemetry from network
        if requests:
            from .async_fetch import fetch_multiple_async

            results = await fetch_multiple_async(
                requests,
                use_cache=not ultra_cold_enabled,
                write_cache=not ultra_cold_enabled,
                validate_payload=not ultra_cold_enabled,
            )

            for (driver, lap_num), result in zip(lap_info, results):
                if result is not None and isinstance(result, dict):
                    tel_data = result.get("tel", {})
                    if tel_data:
                        self._remember_telemetry_payload(driver, lap_num, tel_data)
                        tel_df = _create_telemetry_df(tel_data, driver, lap_num, self.lib)
                        if tel_df is not None and not _is_empty_df(tel_df, self.lib):
                            telemetry_map[(driver, lap_num)] = tel_df

                            # Cache if not in ultra cold mode
                            if not ultra_cold_enabled and self.enable_cache:
                                get_cache().set_telemetry(
                                    self.year, self.gp, self.session, driver, lap_num, tel_data
                                )

        return telemetry_map

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
        return asyncio.run(self.fetch_all_laps_telemetry_async(ultra_cold=ultra_cold))


_SESSION_FETCH_FROM_CDN_CODE = getattr(Session._fetch_from_cdn, "__code__", None)
_SESSION_FETCH_FROM_CDN_FAST_CODE = getattr(Session._fetch_from_cdn_fast, "__code__", None)


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

    _metadata: ClassVar[list[str]] = [
        "session",
        "driver",
        "_prefetched_lap_data",
        "_laps",
        "_lap_numbers",
        "_lap_numbers_df_id",
        "_lap_index_map",
        "_lap_index_map_df_id",
        "_lap_index_map_df_ref",
    ]

    def __init__(
        self, session: Session, driver: str, prefetched_lap_data: dict[str, Any] | None = None
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

    @property
    def _constructor(self):
        return Driver

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
                        session_laps_pl = cast(Any, self.session._laps)
                        # Polars uses lazy evaluation - already optimal
                        driver_laps = session_laps_pl.filter(pl.col(COL_DRIVER) == self.driver)  # type: ignore[union-attr]
                    else:
                        session_laps_pd = cast(pd.DataFrame, self.session._laps)
                        # Use query() for in-place filtering (avoids copy)
                        driver_laps = session_laps_pd.query(
                            f"{COL_DRIVER} == @self.driver", engine="python"
                        ).reset_index(drop=True)
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

        if self.session._resolve_ultra_cold_mode(None):
            lap_data = self.session._fetch_json_unvalidated(path)
            if isinstance(lap_data, dict) and self.session._should_backfill_ultra_cold_cache(True):
                self.session._schedule_background_cache_fill(json_payloads=[(path, lap_data)])
            if isinstance(lap_data, dict):
                self.session._remember_local_payload(path, lap_data)
            return lap_data

        return self.session._fetch_json(path)

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
            laps_pl = cast(Any, laps)
            lap_row = laps_pl.filter(pl.col(COL_LAP_NUMBER) == lap_number)  # type: ignore[union-attr]
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
        """Get driver's fastest lap (returns empty DataFrame if no valid laps)."""
        laps = self.laps
        if _is_empty_df(laps, self.session.lib):
            return _create_empty_df(self.session.lib)

        valid = _filter_valid_laptimes(laps, self.session.lib)
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
        lap_payload_path = f"{self.driver}/laptimes.json"

        raw_lap_payload = (
            self._prefetched_lap_data
            if isinstance(self._prefetched_lap_data, dict)
            else self.session._get_local_payload(lap_payload_path)
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

    def __init__(self, session: Session, driver: str, lap_number: int):
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

GP_ALIAS_MAP = {
    "silverstone": "British Grand Prix",
    "hungary": "Hungarian Grand Prix",
    "azerbaijan": "Azerbaijan Grand Prix",
    "barcelona": "Spanish Grand Prix",
    "monza": "Italian Grand Prix",
    "spa": "Belgian Grand Prix",
    "interlagos": "São Paulo Grand Prix",
    "imola": "Emilia Romagna Grand Prix",
    "jeddah": "Saudi Arabian Grand Prix",
}


def _normalize_event_key(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9 ]+", " ", name.lower())
    normalized = normalized.replace("grand prix", " ")
    normalized = normalized.replace("gp", " ")
    return " ".join(normalized.split())


def _normalize_session_name(session: str) -> str:
    return SESSION_MAPPING.get(session.upper(), session)


def _resolve_gp_name(year: int, gp: str | int) -> str:
    if isinstance(gp, int):
        from .events import get_event

        event = get_event(year, gp)
        if event is None:
            raise ValueError(f"could not resolve event round '{gp}' for year={year}")
        event_obj = cast(Any, event)
        event_name_value = (
            event_obj.get("EventName", "")
            if hasattr(event_obj, "get")
            else getattr(event_obj, "EventName", "")
        )
        event_name = str(event_name_value).strip()
        if not event_name:
            raise ValueError(f"resolved event for round '{gp}' has no event name")
        return event_name
    gp_str = str(gp).strip()
    if not gp_str:
        return gp_str

    from .events import get_event_by_name

    # Use get_event_by_name which has fuzzy matching to resolve abbreviated names
    try:
        event = get_event_by_name(year, gp_str, exact_match=False)
        return str(event.EventName)
    except (ValueError, AttributeError):
        # If event resolution fails, return the original string
        return gp_str


def _resolve_session_name(year: int, gp_name: str, session: str | int) -> str:
    if isinstance(session, int):
        from .events import get_sessions

        sessions = get_sessions(year, gp_name)
        if 1 <= session <= len(sessions):
            return sessions[session - 1]
        raise ValueError(f"session index out of range: {session}")
    return _normalize_session_name(session)


def get_session(
    year: int,
    gp: str | int,
    session: str | int,
    enable_cache: bool | None = None,
    lib: Literal["pandas", "polars"] | None = None,
) -> Session:
    """
    Get a session object.

    Args:
        year: Year (2018-current)
        gp: Grand Prix name or round number (e.g., "Abu Dhabi Grand Prix" or 1)
        session: Session name (e.g., "Practice 1", "Qualifying", "Race")
        enable_cache: Enable caching. If None, uses configured default.
        lib: Data lib choice - 'pandas' or 'polars'. If None, uses configured default.

    Returns:
        Session object with lap and telemetry data

    Raises:
        ValueError: If year is out of supported range or session doesn't exist for the event

    Example:
        >>> session = get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
        >>> session = get_session(2025, 1, "Practice 1")  # Using round number
        >>> session_polars = get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", lib="polars")
        >>> session.drivers
        [{'driver': 'VER', 'team': 'Red Bull Racing'}, ...]
        >>> laps = session.laps
    """
    gp_name = _resolve_gp_name(year, gp)
    session_name = _resolve_session_name(year, gp_name, session)

    # Validate that the session exists for this event
    from .events import get_sessions

    available_sessions = get_sessions(year, gp_name)
    if session_name not in available_sessions:
        raise ValueError(
            f"Session '{session_name}' does not exist for {year} {gp_name}.\n"
            f"Available sessions: {', '.join(available_sessions)}"
        )

    return Session(year, gp_name, session_name, enable_cache, lib)
