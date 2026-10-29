"""pandas-backed event containers (:class:`Event`, :class:`EventSchedule`).

They live apart from :mod:`tif1.events` so the schedule itself (event and
session names, name resolution) loads without pandas (~340 ms of import):
``tif1.get_sessions`` and the ``tif1 events``/``tif1 sessions`` commands never
build a frame. Both classes report ``tif1.events`` as their module, and
``tif1.events.Event``/``tif1.events.EventSchedule`` resolve here lazily, so
their public path, ``repr`` and pickles are unchanged.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, Self, cast

import pandas as pd

from . import events as _events
from .types import ScheduleBackend


class Event(pd.Series):
    __module__ = "tif1.events"
    _metadata: ClassVar[list[str]] = ["_name", "_year", "_event_name", "_backend"]
    _year: int
    _event_name: str
    _backend: ScheduleBackend | None

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow Series-style subclass construction without re-implementing __new__."""
        return cast(Self, super().__new__(cls))

    def __init__(
        self,
        year: int,
        name: str,
        metadata: dict | None = None,
        *,
        backend: ScheduleBackend | None = None,
    ):
        from datetime import timezone as tz

        sessions = _events.get_sessions(year, name, backend=backend)
        data = {
            "RoundNumber": 0,
            "Country": "",
            "Location": self._derive_location(name),
            "OfficialEventName": name,
            "EventDate": pd.NaT,
            "EventName": name,
            "EventFormat": "conventional",
        }

        for i, session in enumerate(sessions, 1):
            data[f"Session{i}"] = session
            data[f"Session{i}Date"] = None
            data[f"Session{i}DateUtc"] = pd.NaT

        data["F1ApiSupport"] = True

        if metadata:
            gmt_offset = _events._parse_gmt_offset(metadata.get("GmtOffset"))
            for key, value in metadata.items():
                if key == "EventDate" and value:
                    data[key] = pd.Timestamp(value.split("T")[0])
                elif key.endswith("DateUtc") and value:
                    data[key] = pd.Timestamp(value)
                elif key.endswith("Date") and value and gmt_offset is not None:
                    # Local track time, made tz-aware with the event's GMT offset
                    data[key] = datetime.fromisoformat(value).replace(tzinfo=tz(gmt_offset))
                else:
                    data[key] = value

        super().__init__(data=data, name=data.get("RoundNumber", 0))  # type: ignore[ty:unknown-argument]
        self._year = year
        self._event_name = name
        self._backend = backend

    def _reconstruct(self, data=None, *args, **kwargs):
        """Reconstruct an Event Series without invoking schedule lookup."""
        result = object.__new__(type(self))
        cast(Any, pd.Series).__init__(result, data, *args, **kwargs)
        result._year = self._year
        result._event_name = self._event_name
        result._backend = getattr(self, "_backend", None)
        return result

    @property
    def _constructor(self):
        return self._reconstruct

    @staticmethod
    def _derive_location(name: str) -> str:
        if not isinstance(name, str):
            return ""
        stripped = name.replace("Grand Prix", "").strip()
        return stripped if stripped else name

    @property
    def year(self):
        return self._year

    def is_testing(self) -> bool:
        """True when this event is a pre-season testing event."""
        return bool(self.get("EventFormat") == "testing")

    def get_session_name(self, identifier: int | str) -> str:
        """Return a full session name for a session identifier.

        Args:
            identifier: Session identifier as number, abbreviation (for example
                ``"FP1"`` or ``"Q"``), or full/partial session name.

        Returns:
            Canonical session name for this event.

        Raises:
            ValueError: If the identifier is invalid or the resolved session
                does not exist for this event.
        """
        available_sessions = _events.get_sessions(
            self._year, self._event_name, backend=getattr(self, "_backend", None)
        )

        try:
            session_num = float(identifier)
        except (TypeError, ValueError):
            if not isinstance(identifier, str):
                raise ValueError(f"Invalid session type '{identifier}'") from None

            session_name = _events._SESSION_TYPES_BY_CASEFOLD.get(identifier.casefold())
            if session_name is None:
                try:
                    session_name = _events._SESSION_TYPE_ABBREVIATIONS[identifier.upper()]
                except KeyError:
                    raise ValueError(f"Invalid session type '{identifier}'") from None

            # Backward compatibility for older sprint weekends.
            if session_name == "Sprint Qualifying" and self.year in (2021, 2022):
                session_name = "Sprint"

            if session_name not in available_sessions:
                raise ValueError(f"Session type '{identifier}' does not exist for this event")

            return session_name

        if not session_num.is_integer() or int(session_num) not in (1, 2, 3, 4, 5):
            raise ValueError(f"Invalid session type '{session_num}'")

        session_index = int(session_num) - 1
        if session_index >= len(available_sessions):
            raise ValueError(f"Session number {int(session_num)} does not exist for this event")

        return available_sessions[session_index]

    def get_session_date(self, identifier: int | str, utc: bool = False) -> pd.Timestamp:
        """Return date and time of a specific session from this event.

        Args:
            identifier: Session name, abbreviation or number.
            utc: If True, return non-timezone-aware UTC timestamp.

        Raises:
            ValueError: If there is no matching session, identifier is invalid
                or local timestamp is unavailable.
        """
        session_name = self.get_session_name(identifier)
        relevant_columns = self.loc[["Session1", "Session2", "Session3", "Session4", "Session5"]]
        mask = relevant_columns == session_name

        if not mask.any():
            raise ValueError(f"Session type '{identifier}' does not exist for this event")

        name = mask.idxmax()
        date_utc = self[f"{name}DateUtc"]
        date = self[f"{name}Date"]

        if (not utc) and pd.isna(date) and (not pd.isna(date_utc)):
            raise ValueError("Local timestamp is not available")

        if utc:
            return pd.Timestamp(date_utc)
        return pd.Timestamp(date)

    def get_session(
        self, identifier: int | str | None = None, *, session_name: int | str | None = None
    ):
        """Return a session of this event.

        Args:
            identifier: Session name, abbreviation or number (fastf1's
                parameter name; positional use is unchanged).
            session_name: tif1's older name for ``identifier``; pass one or
                the other.

        Raises:
            ValueError: Neither or both of ``identifier``/``session_name``
                were given, or the identifier matches no session of this
                event.
        """
        if session_name is not None:
            if identifier is not None:
                raise ValueError(
                    "Cannot pass both `identifier` and `session_name`; "
                    "`session_name` is an alias of `identifier`."
                )
            identifier = session_name
        if identifier is None:
            raise ValueError("get_session() missing 1 required argument: 'identifier'")

        from .core import get_session

        resolved_session_name = self.get_session_name(identifier)
        return get_session(
            self._year,
            self._event_name,
            resolved_session_name,
            backend=getattr(self, "_backend", None),
        )

    def get_race(self):
        """Return the race session."""
        return self.get_session("Race")

    def get_qualifying(self):
        """Return the qualifying session."""
        return self.get_session("Qualifying")

    def get_sprint(self):
        """Return the sprint session."""
        return self.get_session("Sprint")

    def get_sprint_shootout(self):
        """Return the sprint shootout session."""
        return self.get_session("Sprint Shootout")

    def get_sprint_qualifying(self):
        """Return the sprint qualifying session."""
        return self.get_session("Sprint Qualifying")

    def get_practice(self, number: int):
        """Return the specified practice session.

        Args:
            number: Practice session number (1, 2, or 3).
        """
        return self.get_session(f"Practice {number}")


class EventSchedule(pd.DataFrame):
    """FastF1-compatible event schedule container."""

    __module__ = "tif1.events"
    _metadata: ClassVar[list[str]] = ["year", "backend"]
    year: int | None
    backend: ScheduleBackend | None

    def __init__(
        self,
        *args: Any,
        year: int | None = None,
        backend: ScheduleBackend | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.year = year
        self.backend = backend

    @property
    def _constructor(self):
        return EventSchedule

    @property
    def base_class_view(self) -> pd.DataFrame:
        """The schedule as a plain :class:`pandas.DataFrame`.

        fastf1 parity: a nicer view in debuggers and IDEs that render
        ``DataFrame`` subclasses poorly.
        """
        return pd.DataFrame(self)

    def is_testing(self) -> pd.Series:
        """One boolean per event: True for pre-season testing events."""
        if "EventFormat" not in self.columns:
            return pd.Series([False] * len(self), index=self.index, dtype=bool)
        return pd.Series(self["EventFormat"] == "testing")

    def get_event_by_round(
        self,
        round_number: int | None = None,
        *,
        round: int | None = None,  # noqa: A002
    ) -> Event:
        """Return the event of a championship round.

        Args:
            round_number: The round number.
            round: fastf1's keyword (deprecated there since 3.9.0) for the
                same value; pass one or the other.

        Raises:
            ValueError: Neither or both keywords were given, the round is 0
                (testing events have no round) or no event has that round.
        """
        if round is not None:
            if round_number is not None:
                raise ValueError(
                    "Cannot pass both `round_number` and `round`. "
                    "Use `round_number` only; `round` is deprecated."
                )
            round_number = round
        if round_number is None:
            raise ValueError("get_event_by_round() missing 1 required argument: 'round_number'")
        if round_number == 0:
            raise ValueError("Cannot get testing event by round number!")
        if "RoundNumber" not in self.columns:
            raise ValueError(f"Invalid round: {round_number}")

        rounds = pd.to_numeric(self["RoundNumber"], errors="coerce")
        matches = self.loc[rounds == round_number]
        if len(matches) == 1:
            event_name = str(matches.iloc[0].get("EventName", ""))
            return _events._get_event_for_year(
                int(self.year or 0), event_name, getattr(self, "backend", None)
            )
        if len(matches) == 0:
            raise ValueError(f"Invalid round: {round_number}")
        raise ValueError("Something went wrong, cannot determine a unique event.")

    def get_event_by_name(
        self, name: str, strict_search: bool = False, *, exact_match: bool = False
    ) -> Event | None:
        """Return the event ``name`` resolves to, or ``None`` when it matches nothing.

        Args:
            name: Event name, location or country; fuzzy-matched unless an
                exact match is required.
            strict_search: Require an exact (case-insensitive) event-name
                match.
            exact_match: fastf1's name for ``strict_search``; either enables
                the exact match.
        """
        if self.year is None:
            return None
        event_names: list[str] = []
        if "EventName" in self.columns:
            event_names = [item for item in self["EventName"].dropna().astype(str).tolist() if item]
        return _events._find_event_by_name(
            int(self.year),
            event_names,
            name,
            exact_match=strict_search or exact_match,
            backend=getattr(self, "backend", None),
        )

    def get_event(
        self, identifier: int | str, strict_search: bool = False, *, exact_match: bool = False
    ) -> Event | None:
        """Return event by round number or name.

        Args:
            identifier: Round number or event name.
            strict_search: Require an exact event-name match.
            exact_match: fastf1's name for ``strict_search``.
        """
        strict_search = strict_search or exact_match
        if isinstance(identifier, int):
            return self.get_event_by_round(identifier)
        event = self.get_event_by_name(identifier, strict_search=strict_search)
        if event is not None:
            return event
        if not strict_search:
            try:
                return self.get_event_by_round(int(identifier))
            except (TypeError, ValueError):
                return None
        return None
