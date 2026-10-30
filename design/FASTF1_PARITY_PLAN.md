# fastf1 API parity in tif1: closing the remaining gaps

> **Status (2026-10-29): implemented, not yet released.** Every decision
> below was taken in the design interview of 2026-10-29; section 16 lists
> the assumptions made on the way, section 18 what the implementation
> changed or added against this text (the code is the reference where they
> differ). Ships as 0.11.0 (one change set, breaking changes listed in
> `CHANGELOG.md`).

Implementation plan, written 2026-10-29 after a gap analysis of fastf1 3.9.0
against tif1 0.10.1. The analysis introspected both public surfaces (every
class, method, keyword and return annotation), then loaded the 2024 Bahrain
Grand Prix Race and Qualifying and the 2024 Canadian Grand Prix Race through
tif1 and compared the frames against fastf1's column tables
(`Laps._COLUMNS`, `Telemetry._CHANNELS`, `SessionResults._COLUMNS`,
`EventSchedule._COLUMNS`) and the `_api.py` parsers. fastf1 3.9 can no longer
be loaded live without an F1 account, so its side of the data comparison comes
from source, not from a run. The MultiViewer circuit API and the CDN
`corners.json` of every season 2018-2026 were probed as well.

Previous rounds closed the Ergast tier (`ERGAST_PLAN.md`), the plotting
module, the utils module, the schedule family and the results table. What is
left is the session-state surface (`track_status`, `session_status`,
`session_info`, `t0_date`, `session_start_time`, `car_data`, `pos_data`), the
`get_driver` return type, telemetry channels and keywords, circuit info
beyond corners, a tail of shims (`Cache.configure`, `logger`, exception
names), and the polars backend, which today returns plain frames without any
of the fastf1 methods.

## 1. Goals

1. A fastf1 script that only reads data (no live timing) runs unchanged after
   `import tif1 as fastf1`: every fastf1 column is present, named and typed as
   fastf1 names and types it, with tif1's extra columns alongside.
2. Every fastf1 keyword argument is accepted. The one keyword tif1 cannot
   honour (`livedata`) raises explicitly; none is silently ignored.
3. The session-state attributes return fastf1's shapes: a `dict` of
   per-driver `Telemetry` for `car_data`/`pos_data`, a `Time/Status/Message`
   frame for `track_status`, a `Time/Status` frame for `session_status`, a
   populated `session_info`, real `t0_date` and `session_start_time`.
4. `get_circuit_info()` returns everything fastf1's `CircuitInfo` holds
   (corners, marshal lights, marshal sectors, letters, rotation) for every
   season, from `corners.json` first and the MultiViewer API as backup.
5. The polars backend reaches full parity: polars-native `Laps`, `Lap`,
   `Telemetry`, `SessionResults`, `DriverResult` and `Driver` classes carry
   the fastf1 method set, every session surface (including `results`,
   `drivers_df`, `get_driver`) has a dedicated polars implementation, and no
   polars path converts through pandas.
6. Keep the performance budget: nothing new on the laps/telemetry hot path
   beyond two vectorised additions and two constant columns per lap frame;
   every new table is lazy, memoised and served from the existing cache
   tiers; no pandas round trip on the polars backend.
7. Lock the parity with an automated surface-diff test pinned to fastf1
   3.9.0 that runs whenever fastf1 is importable, so the next fastf1 release
   surfaces its own gaps.

## 2. Findings

### 2.1 Session attributes (fastf1 3.9.0 vs tif1 0.10.1)

| Attribute | fastf1 | tif1 today | Gap |
| --- | --- | --- | --- |
| `get_driver(identifier)` | `DriverResult`: the results row (22 columns: `Position`, `GridPosition`, `Points`, `Status`, `Time`, `Q1`-`Q3`, `Laps`, ...), `.dnf` | `Driver`: 13 identity keys (`DriverNumber`, `Abbreviation`, `TeamName`, ..., `DateOfBirth`), no `.dnf`, plus `.laps`, `.fastest_lap`, ... | result columns and `.dnf` missing; `d["Position"]` raises `KeyError` |
| `car_data` / `pos_data` | `dict[str, Telemetry]` keyed by driver number. Car channels `Date, SessionTime, Time, RPM, Speed, nGear, Throttle, Brake, DRS, Source`; pos channels `Date, SessionTime, Time, Status, X, Y, Z, Source` | one `Telemetry` of every lap of every driver concatenated (840 934 rows for Bahrain 2024); `car_data["1"]` raises `KeyError`; `pos_data is car_data` | wrong type, eager and very expensive |
| `track_status` | `DataFrame(Time: timedelta, Status: str, Message: str)` from the TrackStatus stream | the laps' `TrackStatus` category Series | wrong type and granularity |
| `session_status` | `DataFrame(Time, Status)`, statuses `Inactive, Started, Aborted, Finished, Finalised, Ends` | returns `race_control_messages` | wrong table |
| `session_info` | the livetiming SessionInfo payload: `Meeting{Key, Name, OfficialName, Location, Country{Key, Code, Name}, Circuit{Key, ShortName}}, ArchiveStatus, Key, Type, Name, StartDate, EndDate, GmtOffset, Path` | `{Year, EventName, SessionName}` | keys missing |
| `t0_date` | `Timestamp`: the date at SessionTime zero | `NaT` | derivable: `LapStartDate - LapStartTime` is constant to 0 ms across every lap (checked on Canada 2024) |
| `session_start_time` | `Timedelta`: SessionTime of the `Started` status | `NaT` | derivable from race control / schedule |
| `total_laps` | from the LapCount stream (`None` outside races) | Ergast leader lap count (races, sprints) | parity |
| `load(..., livedata=)` | keyword-only `livedata: LiveTimingData` | not accepted | `TypeError` for callers passing it |
| `api_path`, `f1_api_support` | instance attributes | absent | trivially synthesised |
| `Session(event, session_name, f1_api_support)` | constructor | `Session(year, gp, session, ...)` | document only; users go through `get_session` |

### 2.2 Laps and Lap

| Item | fastf1 | tif1 today |
| --- | --- | --- |
| columns | 31 (`Time` ... `IsAccurate`) | the same 31 in the same order, preceded by `index` and followed by `WeatherTime` + 7 weather columns, `LapTimeSeconds`, `ms1`-`ms3`, `drv`, `team`, `QualifyingSession` (qualifying) |
| dtypes | `Driver`, `Team`, `Compound`, `TrackStatus`, `DriverNumber` str; `Deleted` object (bool or None) | `Driver`, `Team`, `Compound`, `TrackStatus` category; `Deleted` boolean |
| `get_telemetry(frequency=)` | `int`, `"original"` or `None` (class default) | no parameter: `TypeError` |
| `pick_quicklaps(threshold=None)` | `None` means `QUICKLAP_THRESHOLD` | `None` raises `TypeError` |
| `pick_laps(lap_numbers=)` | keyword `lap_numbers` | keyword `laps` |
| `Lap.get_weather_data()` | the weather row covering the lap (Series) | empty Series |
| `Laps.get_weather_data()` | one weather row per lap, index aligned with the laps | the whole session weather table |
| `base_class_view` | on `Laps`, `SessionResults`, `EventSchedule` (and `Telemetry`) | only on `Telemetry` |
| `split_qualifying_sessions()` | `[Q1, Q2, Q3]`, `None` entries when a segment is missing | three full copies when `QualifyingSession` is absent |
| `Laps(session=)`, `Lap`, `iterlaps` | | parity (`iterlaps` yields a tuple subclass) |

### 2.3 Telemetry

| Item | fastf1 | tif1 today |
| --- | --- | --- |
| channels | `Date, SessionTime, DriverAhead, DistanceToDriverAhead, Time, RPM, Speed, nGear, Throttle, Brake, DRS, Source, Distance, RelativeDistance, Status, X, Y, Z` | `Time, RPM, Speed, nGear, Throttle, Brake, DRS, Distance, RelativeDistance, DriverAhead, DistanceToDriverAhead, AccelerationX/Y/Z, X, Y, Z, Driver, LapNumber`: missing `Date`, `SessionTime`, `Source`, `Status` |
| dtypes | `RPM`, `Speed`, `nGear`, `Throttle`, `DRS` int64; `Brake` bool | `RPM`, `Speed`, `Throttle` float64; `nGear`, `DRS` Int64; `Brake` bool |
| `DRS` values | raw stream values (`0, 1, 8, 9, 10, 12, 14`; open is `>= 10`) | `0`/`1` flag (the CDN payload stores the flag); `tel["DRS"] >= 10` is always False |
| `get_car_data()` / `get_pos_data()` | car-only / pos-only channel sets; `pad`, `pad_side`, `interpolate_edges` | both return the full merged frame |
| `merge_channels(other, frequency=)` | resamples to `frequency` | `frequency` ignored (`**kwargs`) |
| `resample_channels(rule=None, new_date_ref=None)` | `rule` or `new_date_ref` | `rule="1s"` default; no `new_date_ref` |
| `add_differential_distance(drop_existing=)`, `add_track_status(drop_existing=)` | keyword accepted | no keyword: `TypeError` |
| `add_track_status()` | maps `SessionTime` onto `session.track_status` | fills a constant `"1"` |
| `add_driver_ahead(drop_existing=)` | recomputes from `pos_data` | returns the integrated columns (the better answer); keyword accepted and ignored |
| `Telemetry(drop_unknown_channels=)` | keyword | not accepted |
| `TELEMETRY_FREQUENCY`, `register_new_channel`, `slice_by_*`, `fill_missing`, `integrate_distance`, `calculate_*` | | parity |

### 2.4 Circuit info

fastf1's `CircuitInfo` is built from the MultiViewer API
(`https://api.multiviewer.app/api/v1/circuits/{circuit_key}/{year}`, no
authentication): `corners`, `marshal_lights`, `marshal_sectors` as
`X, Y, Number, Letter, Angle, Distance` frames (distance fitted from the
fastest lap) and `rotation`. The raw payload also carries `pitLoss`,
`candidateLap`, `circuitKey`, `circuitName`, `countryIocCode`, `countryKey`,
`countryName`, `location`, `meetingKey`, `meetingName`,
`meetingOfficialName`, `miniSectorsIndexes`, `raceDate`, `round`, `x`/`y`
(track outline), which fastf1 drops. `/api/v1/circuits` lists 32 circuits
(`name`, `country`, `years`, `circuitKey`, `countryKey`, `iocCountryCode`,
`pitLoss`).

The CDN's `corners.json` has two schemas:

| Schema | Seasons (probed, Race of one round each) | Keys |
| --- | --- | --- |
| old | 2021, 2022, 2023, 2024 | `CornerNumber, X, Y, Angle, Distance, Rotation` |
| new | 2018, 2019, 2020, 2025, 2026 | old keys plus `CornerLetter, CornerLabel, MVDistance, Section, SectionType, SectionStartPercent, SectionEndPercent, SectionStartDistance, SectionEndDistance, MarshalLightNumber/X/Y/Angle/Distance/MVDistance, MarshalSectorNumber/X/Y/Angle/Distance/MVDistance, CircuitKey, CircuitName, Location, CountryName, CountryKey, CountryIocCode, PitLossNormal/SC/VSC, LayoutYear/Round/RaceDate/MeetingKey/MeetingName/MeetingOfficialName, LayoutLapDriverNumber/Number/Session/Time/StartDate/StartSessionTime/SessionStartTime, TrackX, TrackY, TrackTime, MiniSectorIndex` |

tif1 reads only the old keys, so `marshal_lights`/`marshal_sectors` are
always empty and `Letter` is always `""` even where the new file has them.
The new schema covers 100 % of fastf1's `CircuitInfo`.

### 2.5 Results, events, plotting, utils, ergast

| Area | State |
| --- | --- |
| `Session.results` | parity: fastf1's 22 columns first, same dtypes, `DriverNumber` index, `DriverResult.dnf` |
| `EventSchedule.get_event_by_name(name, *, exact_match=)` | tif1 spells it `strict_search` (fastf1's old name); `exact_match` raises `TypeError` |
| `EventSchedule.get_event_by_round(round_number=None, *, round=None)` | tif1 lacks the `round` keyword |
| `Event.get_session(identifier)` | tif1's parameter is `session_name`; keyword callers break |
| schedule columns | parity plus `GmtOffset`, `CircuitId`, `CircuitName`, `CircuitLatitude`, `CircuitLongitude`, `EventUrl`, `CircuitUrl`; `EventDate`/`Session*DateUtc` are `datetime64[us]` (pandas 3) |
| `tif1.plotting`, `tif1.utils`, `tif1.ergast` | parity (plotting signatures are supersets: `session` optional where tif1 can answer without one) |

### 2.6 Top-level and modules

| fastf1 name | tif1 today |
| --- | --- |
| `Cache.configure(*, cache_dir, force_renew, ignore_version, use_requests_cache)` | missing |
| `Cache.disabled(*, disable_http_cache, disable_func_cache)`, `Cache.set_disabled(...)` | no keywords |
| `set_auth_method(method)` | missing (live timing sign-in; out of scope) |
| `fastf1.logger`: `get_logger`, `set_log_level`, `soft_exceptions`, `LoggingManager` | only `tif1.set_log_level` |
| `fastf1.exceptions`: `DataNotLoadedError`, `NoLapDataError`, `FuzzyMatchError`, `FastF1CriticalError` (`F1AuthError` out of scope) | missing (`SessionNotLoadedError`, `LapNotFoundError`, `EventNotFoundError` are the tif1 analogues) |
| `fastf1.mvapi`: `CircuitInfo`, `get_circuit_info(*, year, circuit_key)` | missing (tif1's `CircuitInfo` lives in `models.py`) |
| `fastf1.livetiming`, `fastf1.req`, `fastf1.api`, `fastf1.internals` | missing: live timing, HTTP layer, private API (out of scope) |

### 2.7 The polars backend today

`get_session(..., lib="polars")` returns a plain `polars.DataFrame` for
`laps`, `driver.laps`, `get_fastest_laps()` and telemetry (no `pick_*`, no
`add_distance`, no `get_telemetry`), while `results`, `drivers_df` and
`get_driver()` are pandas objects. polars 2.0 keeps a `pl.DataFrame`
subclass through `head`, slicing and `clone` but returns a plain `DataFrame`
from `filter`, `select`, `sort`, `with_columns` and `lazy().collect()`; a
namespace registered with `pl.api.register_dataframe_namespace` is available
on every frame.

### 2.8 What the CDN serves per session

`drivers.json` (`driver, team, dn, fn, ln, tc, url`), `corners.json` (2.4),
`weather.json` (`wT, wAT, wH, wP, wR, wTT, wWD, wWS`), `rcm.json`
(`time, cat, msg, status, flag, scope, sector, dNum, lap`),
`session_laptimes.json` and `{driver}/laptimes.json` (41 keys including
`lST`/`lSD` = `LapStartTime`/`LapStartDate`, `sesT` = `Time`, per-lap
weather), `{driver}/{lap}_tel.json` (`time, rpm, speed, gear, throttle,
brake, drs, distance, rel_distance, DriverAhead, DistanceToDriverAhead,
acc_x/y/z, x, y, z`). There is no session-info, track-status, session-status
or lap-count file; telemetry carries neither `Date`/`SessionTime` nor the pos
`Status` nor the car/pos `Source`. No new upstream files are planned (D-1):
everything below is derived from these files, `corners.json` and MultiViewer.

## 3. Session state (D-1, D-1b)

All five surfaces are lazy, memoised properties on `Session` (`SessionMemo`
in-process; the derived tables are cheap enough not to need the SQLite tier).
Nothing is fetched that the session does not already load for laps and
messages.

### 3.1 `t0_date`

`median(LapStartDate - LapStartTime)` over the loaded laps (one vectorised
subtraction; the difference is constant to the millisecond because both
columns come from fastf1's own session clock in the extraction). `NaT` when
there are no laps (Ergast-only seasons).

### 3.2 `session_start_time`

In order: the first `GREEN LIGHT - PIT EXIT OPEN` race-control message
(practice, qualifying, sprint qualifying), converted with
`Time - t0_date`; otherwise `Event.get_session_date(identifier, utc=True) -
t0_date` (races, where the formation lap starts on the scheduled time);
`NaT` when `t0_date` is `NaT`.

### 3.3 `track_status`

fastf1 reads the live-timing `TrackStatus` stream
(`api.track_status_data`), which is not reachable without an F1 account
(fastf1 3.9 fails to load it anonymously) and is outside tif1's
historical-CDN scope, so the table is derived. Its shape, status codes and
message strings are fastf1's exactly:
`DataFrame(Time: timedelta64[ns], Status: str, Message: str)` with the
vocabulary `1` AllClear, `2` Yellow, `4` SCDeployed, `5` Red, `6`
VSCDeployed, `7` VSCEnding, built from `race_control_messages` in one pass:

- `Time = rcm.Time - t0_date`.
- `Category == "Flag"`, `Scope == "Track"`: `GREEN`/`CLEAR` -> `1`
  (`AllClear`), `YELLOW`/`DOUBLE YELLOW` -> `2` (`Yellow`), `RED` -> `5`
  (`Red`). `CHEQUERED`, `BLUE`, `BLACK AND WHITE` and driver-scoped flags do
  not change the status.
- `Category == "Flag"`, `Scope == "Sector"`: a set of yellow sectors;
  `YELLOW`/`DOUBLE YELLOW` adds the sector and emits `2` on the first
  addition, `CLEAR` removes it and emits `1` when the set becomes empty and
  no safety car is active.
- `Category == "SafetyCar"`: `Mode`/message `SAFETY CAR` with `Status ==
  "DEPLOYED"` -> `4` (`SCDeployed`); `VIRTUAL SAFETY CAR DEPLOYED` -> `6`
  (`VSCDeployed`); `VIRTUAL SAFETY CAR ENDING` -> `7` (`VSCEnding`);
  `IN THIS LAP` leaves the status; the following `TRACK CLEAR` emits `1`.
- Consecutive identical statuses collapse; the first row is `1`
  (`AllClear`) at `Time = 0` as in the live stream.

Fallback when `rcm.json` is missing or empty: one row per status change at
`LapStartTime` from the laps' `TrackStatus` column; a multi-code lap
(`"124"`) emits its codes in order at the lap start, messages from the
vocabulary. The derivation is documented as an approximation of the live
stream (exact times, inferred codes).

### 3.4 `session_status`

`DataFrame(Time: timedelta64[ns], Status: str)`: `Started` at
`session_start_time`; `Aborted` at each `RED` track flag; `Started` at the
next `GREEN LIGHT` message after a red flag; `Finished` at `CHEQUERED FLAG`.
`Finalised` and `Ends` are not emitted (not derivable). Empty frame with the
two columns when nothing is derivable.

### 3.5 `session_info`

fastf1's nested dict from the schedule row and, when already known, the
circuit info:

| Key | Source |
| --- | --- |
| `Meeting.Name`, `Meeting.OfficialName`, `Meeting.Location` | `EventName`, `OfficialEventName`, `Location` |
| `Meeting.Country.Name`, `.Code`, `.Key` | `Country` from the schedule; `CountryIocCode`/`CountryKey` from the new-schema `corners.json` or the vendored circuit table (section 6), else `None` |
| `Meeting.Circuit.Key`, `.ShortName` | `CircuitKey` from the new-schema `corners.json` or the vendored circuit table; `CircuitName` from the schedule |
| `Meeting.Key`, `Key`, `Path`, `ArchiveStatus` | `None` (not derivable) |
| `Type` | `Practice`, `Qualifying`, `Race` family as fastf1 spells it |
| `Name` | the session name |
| `StartDate`, `EndDate` | `Timestamp`: the session's local date; `EndDate` = start plus the usual duration (`Race` 2 h, `Qualifying` 1 h, practice 1 h / 1.5 h, sprint 1 h) |
| `GmtOffset` | `Timedelta` from the schedule's `GmtOffset` |

Reading `session_info` never triggers a `corners.json` or MultiViewer
request; the circuit keys are filled from the vendored table (zero network)
or from the memoised circuit info when `get_circuit_info()` already ran.

## 4. `get_driver` (D-2)

`class Driver(DriverResult)`: the object is the driver's `results` row (all
22 fastf1 columns plus `TIF1_RESULTS_EXTRA_COLUMNS`) with `.dnf` inherited
and every tif1 helper kept (`.laps`, `.fastest_lap`, `.telemetry`,
`.get_telemetry()`, `.driver`). `get_driver` accepts an abbreviation, a
driver number (int or str), a `number/ABB` shared-car label and a full name,
exactly as `results.loc` and `unique_driver_labels` do; the car number an
identifier names (`"8"`, `"8/FAG"`) picks the row when a driver has two, and
the object is named after its `results` label. The row is always the
`results` row, whatever was read before (a practice session builds
`results` from the laps: fastf1's best-lap `Position` and `Time`, tif1's
`Status`/`Laps`); identity values from `drivers.json` and Ergast fill
whatever it leaves empty. `isinstance(session.get_driver("VER"),
DriverResult)` holds.

## 5. Telemetry (D-3, D-4, D-5, D-6)

### 5.1 Channels

Every lap frame (pandas and polars) gains at build time:

- `SessionTime = LapStartTime + Time` and `Date = LapStartDate + Time`: two
  vectorised timedelta additions. Added also when a frame is rehydrated from
  the SQLite frame tier (the frame tier does not store them; the lap row is
  at hand in both paths). `_FRAME_TIER_SCHEMA_VERSION` is bumped so cached
  frames built before the column-order change are rebuilt once.
- `Source = "merged"` and `Status = "OnTrack"` as object (pandas, fastf1's
  own dtype) / `Categorical` (polars) constant columns, so column-level
  fastf1 code runs;
  documented as constants because the payload is already the car+pos merge
  and has no on/off-track channel.

Column order becomes fastf1's (`Date, SessionTime, DriverAhead,
DistanceToDriverAhead, Time, RPM, Speed, nGear, Throttle, Brake, DRS, Source,
Distance, RelativeDistance, Status, X, Y, Z`) followed by tif1's extras
(`AccelerationX`, `AccelerationY`, `AccelerationZ`, `Driver`, `LapNumber`).

`DRS` stays the 0/1 flag the CDN stores; `RPM`, `Speed`, `Throttle` stay
float64 (interpolated payload values); `nGear`/`DRS` stay `Int64`. The
migration guide flags the `DRS >= 10` idiom and the dtype families.

### 5.2 `car_data` and `pos_data`

Both are `LazyTelemetryDict` instances keyed by driver number (abbreviations
accepted too, as the class does today). Nothing is fetched until a key is
read; the value is the driver's laps concatenated in `SessionTime` order
(one `Telemetry`, memoised per driver). `car_data[n]` and `pos_data[n]` are
the same merged frame (every channel): fastf1's column expectations are a
subset of both. `len()`/iteration list the session's drivers without
fetching when `drivers.json` carries the car numbers; a payload without them
resolves the numbers first (Ergast classification, else the laps), so the
keys never depend on what was read before. A car two drivers shared keys
its later drivers `number/Abbreviation`, as `results` does.
`Lap.get_car_data()`/`get_pos_data()` likewise keep returning the
merged frame and honour `pad`, `pad_side`, `interpolate_edges` like
`slice_by_lap`.

### 5.3 Methods and keywords

- `Laps.get_telemetry(*, frequency=None)` and `Lap.get_telemetry(*,
  frequency=None)`: `None` -> `Telemetry.TELEMETRY_FREQUENCY` (`"original"`),
  `"original"` -> the frame as built, `int` -> `resample_channels` with a
  rule of one sampling period (`10` Hz -> `"100ms"`) after the merge.
- `merge_channels(other, frequency=None)`: `frequency` is applied with the
  same rule after the `merge_asof`.
- `resample_channels(rule=None, new_date_ref=None, **kwargs)`: at most one
  of `rule`/`new_date_ref`; neither keeps today's `"1s"`. `rule` bins from
  the first sample on the session clock (`Date`, else `SessionTime`, else
  `Time`) as fastf1's `merge_channels(frequency=)` does: continuous
  channels are bin means interpolated across empty bins, discrete channels
  and text columns the value in force at each bin start, `Date`/
  `SessionTime` advance with the bins and `Time` restarts at zero.
  `new_date_ref` gives one row per unique, sorted timestamp, continuous
  channels and the other clock columns interpolated on `Date`. The pandas
  and polars classes compute the same rows (see section 18).
- `add_differential_distance(drop_existing=True)`,
  `add_track_status(drop_existing=True)`, `add_driver_ahead(drop_existing=True)`:
  `drop_existing=True` recomputes, `False` returns the frame unchanged when
  the column exists.
- `add_track_status()` maps `SessionTime` onto `session.track_status` with
  fastf1's algorithm (searchsorted on the status times); when no session is
  attached it falls back to `"1"` as today.
- `Telemetry(data, *, session=None, driver=None, drop_unknown_channels=False)`:
  `True` drops columns not in `_CHANNELS`.

## 6. Circuit info and `tif1.mvapi` (D-11b)

### 6.1 `corners.json` reading

`_build_circuit_info` reads the new-schema keys when present:
`CornerLetter` -> `Letter`, the `MarshalLight*` and `MarshalSector*` lists ->
`marshal_lights`/`marshal_sectors` (`Number`, `X`, `Y`, `Angle`, `Distance`;
`Distance` from the file, refitted with `add_marker_distance` only when NaN).
Old-schema files keep producing corners and rotation as today.

### 6.2 MultiViewer backup

When `corners.json` is missing, fails validation, or is old-schema (no
`MarshalLightX`), `get_circuit_info()` calls
`tif1.mvapi.get_circuit(year=, circuit_key=)` once; corners and rotation
from `corners.json` win when they exist, MultiViewer fills `marshal_lights`,
`marshal_sectors` and `Letter`. The MultiViewer response is cached
permanently in the SQLite `json` tier (`mvapi/circuits/{key}/{year}.json`,
no TTL: the layouts are static per year) and memoised per session. A
MultiViewer failure (network, non-200, bad JSON) logs at `info` and leaves
the frames empty; `get_circuit_info()` never raises for the backup.

Circuit key resolution for old-schema and missing files:

1. `CircuitKey` from a new-schema `corners.json` (same session).
2. The vendored table `data/circuits/mv_circuits.json` keyed by Ergast
   `CircuitId` (the schedule already joins `CircuitId` by round):
   `{"bahrain": {"circuitKey": 63, "countryKey": 36, "iocCountryCode": "BRN",
   "name": "Sakhir", "country": "Bahrain"}, ...}`, generated by
   `tools/vendor_mv_circuits.py` from `/api/v1/circuits` with name/country
   matching and a hand-maintained override map for the mismatches (Mugello
   149 -> 146 as fastf1 does; multi-layout circuits by year).
3. The live `/api/v1/circuits` list (cached in the `json` tier for
   `schedule_ttl_hours`), matched the same way, as last resort.

### 6.3 Module and model

- `tif1/mvapi/__init__.py` exports `CircuitInfo` (the `models.CircuitInfo`
  class, unchanged identity), `get_circuit_info(*, year, circuit_key)`
  (fastf1's parser: corners/marshalLights/marshalSectors with `letter`,
  rotation; `Distance` NaN) and `get_circuit(*, year, circuit_key)` (raw
  dict or `None`). Requests go through `payload_loader.get_url` with the
  shared transport (works in Pyodide), `User-Agent: tif1/{version}`.
- `CircuitInfo` gains tif1 extras, all optional with empty defaults:
  `circuit_key: int | None`, `circuit_name: str`, `location: str`,
  `country: str`, `country_code: str`, `pit_loss: dict[str, float]`
  (`normal`, `sc`, `vsc`), `track: DataFrame(X, Y, Time)` (outline),
  `sections: DataFrame(Number, Section, SectionType, StartPercent,
  EndPercent, StartDistance, EndDistance)`, `mini_sector_index: list[int]`.
  On polars sessions the marker frames and extras are polars frames.

## 7. Laps columns and weather (D-7, D-9)

- The laps frame keeps `index`, `drv` and `team` (tif1 extras; nothing
  downstream reads them, but they stay). fastf1's 31 columns are therefore
  present and in fastf1's relative order but not a strict prefix (`index`
  comes first); the compliance test checks presence, relative order and
  dtype family, not prefix position. `Driver`, `Team`, `Compound`,
  `TrackStatus` stay categorical, `Deleted` stays nullable boolean
  (documented deviations).
- `Laps.get_weather_data()` returns one row per lap aligned with the laps
  index: `Time` then `AirTemp, Humidity, Pressure, Rainfall, TrackTemp,
  WindDirection, WindSpeed`, picked from `session.weather_data` by fastf1's
  rule (first sample within `[LapStartTime, Time]`, else the last sample
  before the lap's end; searchsorted, ~1 ms for a race). The laps table's own
  `WeatherTime` sample turned out to be misaligned upstream (the 0.11 audit:
  435 of 1,128 race laps, samples an hour off in qualifying), so it is read
  only without a session weather table. `Lap.get_weather_data()` returns that
  lap's Series (NaN when there is no sample, as fastf1).
  `session.weather_data` stays the whole table.
- `pick_quicklaps(threshold=None)` uses `QUICKLAP_THRESHOLD`;
  `pick_laps(lap_numbers, *, laps=None)` keeps `laps=` as an alias.
- `pick_track_status(status, how=)` keeps fastf1's semantics: it matches the
  laps' `TrackStatus` code string (`equals`, `contains`, `excludes`,
  `any`, `none`), never the derived time-based table.
- `split_qualifying_sessions()` returns `None` for a segment with no laps;
  when `QualifyingSession` is absent it splits on `session.session_status`
  with fastf1's algorithm (the two longest gaps over five minutes only when
  there is no status), identically on both backends, and raises fastf1's
  `ValueError` for a session that is not qualifying-like. A qualifying-like
  session without official results is classified from its laps the way
  fastf1 does (best accurate lap per segment, ordered Q3, Q2, Q1, "N" outside
  107%).
- `base_class_view` on `Laps`, `SessionResults`, `EventSchedule`.

## 8. Keyword parity and shims (D-10, D-11)

- `Session.load(*, laps=True, telemetry=True, weather=True, messages=True,
  livedata=None)`: positional use keeps working; a non-`None` `livedata`
  raises `NotImplementedError("tif1 serves historical data only; live
  timing recordings are not supported")`.
- `Session.f1_api_support` (`year >= F1_API_FIRST_YEAR`), `Session.api_path`
  (`"{year}/{gp}/{session}/"`, the CDN prefix).
- `EventSchedule.get_event_by_name(name, *, exact_match=False, strict_search=False)`
  (either keyword), `get_event_by_round(round_number=None, *, round=None)`,
  `Event.get_session(identifier)` with `session_name=` kept as alias.
- `Cache.configure(*, cache_dir=None, force_renew=False, ignore_version=False,
  use_requests_cache=True)` maps onto `enable_cache`; `Cache.disabled(*,
  disable_http_cache=True, disable_func_cache=True)` and `set_disabled(...)`
  accept the keywords (both map onto tif1's single cache toggle; passing
  both `False` is a no-op).
- `tif1.logger`: `get_logger(name)` (returns `logging.getLogger(f"tif1.{name}")`),
  `set_log_level(level)` (the existing function), `soft_exceptions(descr_name,
  msg, logger)` (fastf1's decorator semantics: log and swallow when the
  session is not in strict mode, re-raise otherwise), `LoggingManager`
  (tif1's logging configuration under fastf1's name). Logger names are
  `tif1.*`, never `fastf1.*`.
- `tif1.exceptions`: `DataNotLoadedError = SessionNotLoadedError`,
  `class NoLapDataError(LapNotFoundError)`, `class FuzzyMatchError(EventNotFoundError)`,
  `class FastF1CriticalError(TIF1Error)` with `RateLimitExceededError`
  re-parented under it (its `TIF1Error` ancestry is unchanged).
- Not provided, documented as out of scope: `set_auth_method`,
  `F1AuthError`, `fastf1.livetiming`, `fastf1.req`, `fastf1.api`,
  `fastf1.internals`. The compliance test carries this exclusion list.

## 9. Polars parity (D-12, D-12b)

### 9.1 Classes

`tif1/models_polars.py` (never imports pandas; imported lazily by `core.py`
behind `_ensure_polars_available()`):

- `class PolarsLaps(pl.DataFrame)`: `session` attribute, the fastf1 method set
  (`pick_driver`, `pick_drivers`, `pick_team`, `pick_teams`, `pick_lap`,
  `pick_laps`, `pick_fastest`, `pick_quicklaps`, `pick_tyre`,
  `pick_compounds`, `pick_track_status`, `pick_wo_box`, `pick_box_laps`,
  `pick_not_deleted`, `pick_accurate`, `split_qualifying_sessions`,
  `iterlaps`, `get_telemetry`, `get_car_data`, `get_pos_data`,
  `get_weather_data`, `telemetry`, `base_class_view`, `QUICKLAP_THRESHOLD`)
  implemented with native expressions; every method re-wraps its result
  (`PolarsLaps._from_frame(df, session)`) so fastf1 chains keep the class.
- `class PolarsLap`: a one-row `PolarsLaps` wrapper with scalar
  `__getitem__`/`get`, attribute access to columns, `.telemetry`,
  `get_telemetry(frequency=)`, `get_car_data`, `get_pos_data`,
  `get_weather_data`, `driver`, `lap_number`, `session`; `iterlaps` yields
  `(index, PolarsLap)`.
- `class PolarsTelemetry(pl.DataFrame)`: `session`, `driver`,
  `TELEMETRY_FREQUENCY`, `_CHANNELS`, `register_new_channel`,
  `add_distance`, `add_relative_distance`, `add_differential_distance`,
  `add_driver_ahead`, `add_track_status`, `calculate_differential_distance`,
  `calculate_driver_ahead`, `integrate_distance`, `fill_missing`,
  `get_first_non_zero_time_index`, `merge_channels` (`join_asof`),
  `resample_channels` (`group_by_dynamic` / `upsample` + interpolate),
  `slice_by_lap`, `slice_by_mask`, `slice_by_time`, `base_class_view`.
- `class PolarsSessionResults(pl.DataFrame)`: fastf1's 22 columns first in
  fastf1's dtypes (`Duration` for `Q1`-`Q3`/`Time`/`TimeGap`, `Float64` for
  `Position`/`GridPosition`/`Points`/`Laps`, `String` elsewhere), then
  `TIF1_RESULTS_EXTRA_COLUMNS`; `DriverNumber` is a column (polars has no
  index) and `base_class_view`, `get_driver(identifier)`-style row lookup
  (`loc`-like helper `row_for(identifier)`) are provided.
- `class PolarsDriverResult`: a one-row wrapper with scalar `__getitem__`/
  `get`, attribute access to columns, `.dnf` (same status rule as pandas),
  `session`.
- `class PolarsDriver(PolarsDriverResult)`: adds `.laps` (`PolarsLaps`),
  `.fastest_lap`, `.telemetry`, `get_telemetry(frequency=)`, `driver`,
  `driver_number`, mirroring the pandas `Driver`.
- A `pl.api.register_dataframe_namespace("f1")` namespace exposes the same
  methods on any plain frame (`df.f1.pick_fastest()`, `df.f1.add_distance()`,
  `df.f1.row_for("VER")`); it looks up the session from the frame's
  `_tif1_session` attribute when the frame came out of a tif1 class, else
  requires `session=`.
- `Session.laps`, `Driver.laps`, `get_fastest_laps`, `results`,
  `drivers_df`, `get_driver`, the lazy telemetry dict values and every chart
  entry point return these classes on `lib="polars"`.

### 9.2 Session surfaces on polars

`track_status`, `session_status`, `Laps.get_weather_data()`, the circuit
marker frames and extras are polars frames built natively (the derivations
operate on the already polars `race_control_messages`/laps). `session_info`,
`t0_date`, `session_start_time` are backend-neutral. `car_data`/`pos_data`
hold `PolarsTelemetry` values.

`results` is a `PolarsSessionResults` on polars sessions, `drivers_df` a
plain polars frame (pandas' `drivers_df` is a plain frame too, not a
`SessionResults`) and `get_driver()` returns a `PolarsDriver`. The Ergast merge
(`core_utils/ergast_results.py`) gets a dedicated polars path: the Stats
payload is flattened through the existing `ErgastPolarsResponse` machinery
(`tif1.ergast` with `result_type="polars"`) and joined to the `drivers.json`
rows with polars expressions (`join` on `DriverNumber`/`Abbreviation`,
did-not-start synthesis with `concat`, `unique_driver_labels` on the
`DriverNumber` column, the classification order kept with a `sort` on an
order column). The lap-derived classification fallback
(`_summarize_laps_for_classification`) already runs on either backend.
Nothing on the polars results path builds a pandas frame.

### 9.3 Dedicated implementations

Every polars surface on the hot paths (laps pipeline, telemetry build and
methods, results merge, driver lookup, weather slices, circuit frames) is
written with native polars expressions on the polars frames the session
already holds; no polars path converts to pandas and back (the single
exception stays the charts, which draw from a pandas session by design).
The two tiny sequential derivations (the track-status state machine and
the session-status walk over at most a few hundred race-control rows) run
on plain Python lists extracted once per backend (`to_list()`), so the
state machine has one implementation; the resulting frame is built per
backend. `SessionTime`/`Date` are two expressions on polars
(`pl.col("Time") + lap_start`). `PolarsLap`/`PolarsDriverResult` read their
row once (`row(0, named=True)`) and answer scalar access from the dict.

## 10. Compliance lock (D-13)

`tests/unit/test_fastf1_surface_compliance.py` (skips unless `fastf1` is
importable; install recipe as the Ergast compliance test):

- `FASTF1_PINNED_VERSION = "3.9.0"`; the test fails when the installed
  fastf1 differs, so a bump is explicit.
- Member parity: every public member of `Session`, `Laps`, `Lap`,
  `Telemetry`, `SessionResults`, `DriverResult`, `CircuitInfo`, `Event`,
  `EventSchedule`, `Ergast` and the response classes, `plotting`, `utils`,
  `Cache`, `logger`, `mvapi`, `exceptions` and the top-level module exists in
  tif1 (pandas-derived members excluded; the out-of-scope exclusion list of
  section 8 is the only allow-list).
- Parameter parity: every fastf1 parameter name is accepted by the tif1
  counterpart (`inspect.signature`, `**kwargs` does not count).
- Column parity: `Laps._COLUMNS`, `Telemetry._CHANNELS` (minus `excluded`),
  `SessionResults._COLUMNS`, `EventSchedule._COLUMNS` are present in tif1's
  in-memory fixture frames in fastf1's relative order with matching dtype
  families (timedelta/datetime/float/int-like/bool-like/str-like).
- Polars: the same member and parameter checks against `PolarsLaps`,
  `PolarsLap`, `PolarsTelemetry`, `PolarsSessionResults`,
  `PolarsDriverResult`, `PolarsDriver`, and the column checks against the
  polars fixture frames with polars dtype families (`Duration`, `Datetime`,
  `Float64`, integer, `Boolean`, `String`/`Categorical`).

## 11. Performance budget

- Telemetry build: two timedelta additions and two constant columns per lap
  frame; no extra request; benchmarked (`tests/benchmarks`) before and after
  with the same fixtures; the frame-tier bump rebuilds cached frames once.
- `car_data`/`pos_data`: zero cost until indexed; one concat per driver on
  first read, memoised.
- `track_status`/`session_status`/`session_info`/`t0_date`/
  `session_start_time`: no request beyond laps and messages; one pass over
  the race-control frame; memoised.
- `get_circuit_info()`: unchanged for new-schema seasons; one cached
  MultiViewer request for old-schema seasons on first call per
  `(circuit_key, year)` per cache directory; zero network for the circuit
  key (vendored table).
- `get_driver`: one Series copy of the results row; no new request.
- `Laps.get_weather_data()`: one searchsorted over the session weather table.
- Polars classes: native expressions, no pandas round trip; `PolarsLap`
  and `PolarsDriverResult` scalar access through `row(0, named=True)` once
  per row; the polars Ergast merge is one `join` plus one `sort` on frames
  of at most a few dozen rows and never materialises a pandas frame.

## 12. Phases

1. **Keyword parity and shims** (sections 7-8): pure API, in-memory fixtures.
2. **Telemetry channels and methods** (section 5): `SessionTime`/`Date`/
   `Source`/`Status`, frame-tier bump, `get_telemetry(frequency=)`,
   `merge_channels(frequency=)`, `resample_channels(new_date_ref=)`,
   `drop_existing`, `drop_unknown_channels`, lazy `car_data`/`pos_data`.
3. **Session state** (section 3): derivations, `add_track_status` on top.
4. **`get_driver`** (section 4).
5. **Circuit info and `tif1.mvapi`** (section 6): new-schema reading,
   vendored circuit table and tool, MultiViewer backup, `CircuitInfo`
   extras, `session_info` circuit keys.
6. **Polars parity** (section 9): `models_polars.py` (laps, lap, telemetry,
   results, driver classes), the `.f1` namespace, the polars Ergast merge,
   session wiring, charts.
7. **Compliance lock, docs, changelog** (sections 10, 14).

## 13. Tests

- Unit: every new keyword and property on `InMemoryTransport` fixtures;
  the track-status state machine on hand-built rcm payloads (sector
  yellows, SC, VSC, red flag, resume); `session_status` rows; `session_info`
  keys; `t0_date` from laps; `LazyTelemetryDict` laziness (no fetch until
  indexed, `len()` without fetch); `Driver` is a `DriverResult` with `.dnf`
  and tif1 helpers; `base_class_view` types; `corners.json` old and new
  schema fixtures; MultiViewer backup with an in-memory transport (old
  schema triggers it, new schema does not, failure leaves empty frames);
  vendored circuit table resolution; `tif1.logger` and exception MROs;
  `livedata` raises.
- Polars: the same suite against `PolarsLaps`/`PolarsLap`/`PolarsTelemetry`
  /`PolarsSessionResults`/`PolarsDriverResult`/`PolarsDriver` plus chain
  survival (`pick_driver(...).pick_fastest().get_telemetry()`), namespace
  access after a native `filter`, the polars Ergast merge against the
  `ergast_offline` fixtures (same rows, points, positions and labels as the
  pandas merge, compared through Arrow), and a guard that no polars code
  path imports pandas (`sys.modules` check in a subprocess, as the
  schedule-lookup tests do).
- Property: `SessionTime - Time == LapStartTime` and `Date - Time ==
  LapStartDate` for every row; `track_status.Time` monotonic and statuses in
  the vocabulary; `session_status` ordered.
- Integration (`integration` marker): 2024 Bahrain R/Q, 2024 Canada R
  (safety cars), 2022 Bahrain R (old-schema corners -> MultiViewer backup),
  one pre-2018 session; `car_data["1"]`, `track_status`, `session_status`,
  `t0_date` non-null, marshal frames non-empty.
- Compliance: section 10.
- Benchmarks: telemetry build, `session.laps`, `get_circuit_info()` within
  noise.

## 14. Documentation

`COMPATIBILITY.md` gains the session-state, telemetry, circuit-info and
polars rows and loses the "Simplified" caveats that no longer hold;
`docs/migration-from-fastf1.mdx` "Key API Differences" is rewritten around
what still differs (lazy loading, `DRS` flag, categorical lap columns, float
telemetry dtypes, extra lap columns, derived session state, no live timing);
`docs/api-reference/core.mdx` documents the five session-state properties,
the dict shape of `car_data`/`pos_data`, `get_driver`'s result columns and
the polars classes; a new `docs/api-reference/mvapi.mdx`;
`DATA_REFERENCE.md` documents both `corners.json` schemas and the vendored
circuit table; `CHANGELOG.md` `[Unreleased]` lists every behaviour change
under a **Breaking** heading (D-14): `track_status`, `session_status`,
`car_data`/`pos_data`, `Laps.get_weather_data()`, `get_driver` columns,
telemetry columns and order, polars return types.

## 15. Decisions

| # | Decision | Rationale |
| --- | --- | --- |
| D-1 | Session state is derived from existing files only; no new upstream files | no data-repo changes; every season is covered the same way |
| D-1b | `track_status` from race control (exact times, inferred codes), laps as fallback | SC/VSC/red periods need exact timestamps; lap codes confirm sector yellows raise `2` |
| D-2 | `Driver(DriverResult)` superset | fastf1 scripts get the results row; no tif1 caller breaks |
| D-3 | `SessionTime`/`Date` always added at build time | one frame per lap; fastf1's time-based methods need them |
| D-4/5 | constant `Source`/`Status`; `DRS` stays the flag; dtypes documented | honest about the data; column-level fastf1 code runs |
| D-6 | lazy dict keyed by driver number, same merged frame for `car_data` and `pos_data` | zero cost until indexed; one copy per driver |
| D-7 | `index`/`drv`/`team` stay in the laps frame | existing tif1 contract; compliance checks presence and order, not prefix |
| D-9 | fastf1's per-lap weather contract | fastf1's rule on `session.weather_data` (the laps' own sample is misaligned upstream); aligns with the laps index |
| D-10 | `livedata` raises `NotImplementedError` | never return different data than asked |
| D-11 | no auth shims; `tif1.logger` with tif1 logger names; livetiming out of scope | tif1 has no sign-in or live path |
| D-11b | new `corners.json` fields + MultiViewer fill for old-schema and errors; vendored circuit-key table | full `CircuitInfo` for every season; zero network for the key |
| D-12 | full polars parity | polars is a first-class backend |
| D-12b | `pl.DataFrame` subclasses re-wrapping their outputs plus a `.f1` namespace | fastf1 spelling on polars; methods survive native ops |
| D-13 | surface-diff test pinned to fastf1 3.9.0 | the next fastf1 release surfaces its own gaps |
| D-14 | one minor release, 0.11.0, breaking changes listed, no deprecation shims | pre-1.0; matches the Ergast tier's shipping |
| D-15 | `results`, `drivers_df`, `get_driver` are polars-native on polars sessions (`PolarsSessionResults`, `PolarsDriverResult`, `PolarsDriver`) with a dedicated polars Ergast merge | full parity and polars performance; no pandas round trip |
| D-16 | `track_status` keeps fastf1's shape, codes and messages; the stream fastf1 reads is unreachable (F1 account, live timing) so it stays derived; `pick_track_status` keeps fastf1's lap-code semantics | "whatever fastf1 is using" where possible |
| D-17 | MultiViewer payloads live in the SQLite `json` tier only; nothing vendored | the cache is enough |

## 16. Assumptions (author's, overridable)

- `session_status` emits `Started`, `Aborted`, `Started` (resume) and
  `Finished` only; `Finalised`/`Ends` are not derivable.
- `session_start_time` prefers the first `GREEN LIGHT - PIT EXIT OPEN`
  message and falls back to the scheduled UTC start minus `t0_date`.
- `session_info.EndDate` uses fixed nominal durations per session type.
- `Lap.get_car_data()`/`get_pos_data()` keep returning the merged frame
  (consistent with D-6).
- `Driver`, `Team`, `Compound`, `TrackStatus` stay categorical and `Deleted`
  nullable boolean (performance; consistent with D-7's "keep").
- `get_telemetry(frequency=int)` resamples with `resample_channels` at one
  sampling period; `"original"` and `None` return the built frame.
- `split_qualifying_sessions()` without `QualifyingSession` splits on
  session-time gaps as fastf1 does.
- `RateLimitExceededError` is re-parented under `FastF1CriticalError`
  (still a `TIF1Error`).
- The MultiViewer response is cached without TTL; the circuits list for
  `schedule_ttl_hours`.
- The vendored circuit table is refreshed by `tools/vendor_mv_circuits.py`
  alongside the schedule vendoring; mismatches are fixed in its override map,
  never by fuzzy matching at runtime.
- `CircuitInfo` extras are empty (not `None`) when unavailable so attribute
  access never raises.
- Charts on polars sessions continue to draw from the pandas view
  (`lib="polars"` on a chart loads the pandas session), unchanged.

## 17. Open questions

None. The three questions left after the interview were closed on
2026-10-29: polars results/driver classes ship in 0.11.0 (D-15),
`track_status`/`pick_track_status` follow fastf1 as far as the data allows
(D-16), MultiViewer payloads are cached in SQLite only (D-17).

## 18. Implementation notes (2026-10-29)

What the implementation changed or added against the text above, each
verified on real data (2025 Bahrain Race, Qualifying and Practice 2, 2025
Chinese Sprint, 2024 Canadian and Bahrain Races, 2022 Bahrain Race, 2016
Monaco Race) by comparing every surface between the pandas and polars
sessions value by value.

- **Resampling semantics (both backends).** The pandas class binned the
  lap-relative `Time` (a multi-lap frame such as `car_data[n]` folded onto
  itself) and the polars class averaged discrete channels; both now follow
  section 5.3 and return the same rows. `new_date_ref` interpolates on
  offsets from the first sample (epoch nanoseconds lose ~200 ns in float64).
- **Batch telemetry.** `Session._telemetry_frames_for_refs` (memo, frame
  tier, payload tier, then one parallel network wave) serves
  `PolarsLaps.telemetry`; the pandas `Laps.telemetry` borrows its payload
  wave (`_prefetch_telemetry_payloads`) before the per-lap chain. A cold
  `car_data[n]` went from 11-20 s to 0.4-0.8 s on both backends.
- **Seasons before 2018.** Every telemetry entry point returns an empty
  frame with fastf1's columns at once (no request, no warning), and
  `get_driver` prefetches nothing.
- **polars laps contract.** The flags are `Boolean` with a missing flag read
  as False and all-null columns carry their dtype, as on pandas
  (`_FRAME_TIER_SCHEMA_VERSION` 4).
- **Keywords found by the compliance test.** `get_session(identifier=,
  exact_match=)`, `get_driver(identifier)` (full names accepted, `driver=`
  kept as an alias), `tif1.legacy.inject_driver_ahead`, `base_class_view` on
  the Ergast result frames.
- **Performance found while benchmarking against 0.10.1.** The frame tier
  was never written by a script that exits right after a load (the writer's
  compression pool is closed before the exit hook joins it); it now
  compresses inline then. `Date`/`SessionTime` are built with each frame
  (`_create_telemetry_df(clock=)`) instead of two inserts per lap; polars
  frames are built from numpy arrays (`_polars_typed_telemetry_frame`, the
  expression path stays for payloads it cannot type exactly); frame-tier
  rows decompress into pyarrow's system allocator.
- **Bugs fixed on the way.** polars frame-tier telemetry lost its session
  clock; the bulk prefetch memoised pandas frames on polars sessions; the
  cold polars `get_fastest_laps()` reported `LapTimeSeconds` in nanoseconds
  and ordered ties at random; `timedelta64[ms]`/`[us]` columns were read as
  nanoseconds.
