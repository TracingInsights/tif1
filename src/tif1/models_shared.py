"""Backend-neutral session models: :class:`CircuitInfo` and :class:`LazyTelemetryDict`.

Both serve pandas and polars sessions alike, so they live apart from
:mod:`tif1.models` (whose DataFrame classes subclass pandas): a polars session
imports this module without pandas. :mod:`tif1.models` re-exports both, so
``tif1.models.CircuitInfo`` and ``tif1.models.LazyTelemetryDict`` are these
classes. Type checks against pandas go through
:func:`~tif1.core_utils.lazy_import.is_pandas_frame`, which never imports it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import numpy as np

from .core_utils.constants import LAP_PAYLOAD_KEYS
from .core_utils.ergast_results import unique_driver_labels
from .core_utils.helpers import _create_lap_df, _process_lap_df
from .core_utils.lazy_import import is_pandas_frame
from .core_utils.runtime import import_supported_polars
from .exceptions import DataNotFoundError

if TYPE_CHECKING:
    import pandas as pd

    from .models import Lap, TelemetryProvider

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


def _empty_pandas_frame(columns: list[str]) -> pd.DataFrame:
    import pandas as pd

    return pd.DataFrame(columns=columns)


def _empty_marker_frame() -> pd.DataFrame:
    return _empty_pandas_frame(_CORNERS_DF_COLUMNS)


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

    track: pd.DataFrame = field(default_factory=lambda: _empty_pandas_frame(_TRACK_DF_COLUMNS))
    """Track outline: ``X``, ``Y`` and the session ``Time`` of each outline sample."""

    sections: pd.DataFrame = field(
        default_factory=lambda: _empty_pandas_frame(_SECTIONS_DF_COLUMNS)
    )
    """Track sections per corner: ``Number, Section, SectionType, StartPercent,
    EndPercent, StartDistance, EndDistance``."""

    mini_sector_index: list[int] = field(default_factory=list)
    """Mini-sector boundaries as indices into the track outline."""

    source: str = ""
    """Where the data came from: ``"corners.json"``, ``"multiviewer"`` or both joined by ``+``."""

    def add_marker_distance(self, reference_lap: Lap) -> None:
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
            if is_pandas_frame(df):
                df["Distance"] = distances
            else:
                import polars as pl

                setattr(self, name, df.with_columns(pl.Series("Distance", distances)))


def _float_columns(frame: Any, columns: list[str]) -> np.ndarray:
    """``columns`` of a pandas or polars frame as one float64 array (NaN for missing)."""
    if is_pandas_frame(frame):
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
            elif is_pandas_frame(laps):
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


def _empty_polars_driver_laps(session: Any) -> Any:
    """An empty polars laps frame that still carries the laps columns (unbound).

    The polars twin of :func:`tif1.models._empty_driver_laps`: the loaded
    session table lends its schema; without one, the lap pipeline runs on an
    empty payload.
    """
    pl = import_supported_polars()
    source = getattr(session, "_laps", None)
    if pl is not None and isinstance(source, pl.DataFrame):
        return source.clear()
    payload = {key: [] for key in LAP_PAYLOAD_KEYS}
    return _process_lap_df(_create_lap_df(payload, "", "", "polars"), "polars")


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


__all__ = ["CircuitInfo", "LazyTelemetryDict"]
