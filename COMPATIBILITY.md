# Compatibility with FastF1

`tif1` is designed to be a high-performance replacement for `fastf1` in most common use cases, often with a one-line import change. It keeps the fastf1 API names, but there are architectural differences and some features that are still in development.

## Comparison at a Glance

| Feature | fastf1 | tif1 |
| :--- | :--- | :--- |
| **Data Source** | Ergast + F1 Live Timing | TracingInsights (CDN); Ergast tables from the TracingInsights Stats repository, Jolpica only as fallback |
| **Loading** | `session.load()` downloads the whole session | **Lazy, per-file fetching** (async, parallel) |
| **Backends** | Pandas only | **Pandas + Polars** |
| **Caching** | Pickle files + SQLite HTTP cache | **SQLite + Memory LRU** |
| **Live Timing** | Supported | **Historical only (timing/telemetry 2018-present, Ergast results 1950-present)**; track/session status derived from the race-control messages |

## Current Status

### 🏁 Event & Session API
Most `EVENT` and `SESSION` level APIs work as expected.
- [x] `get_session(year, event, type)` (with fastf1's `identifier=`, `exact_match=` and `backend=` keywords; tif1 also keeps `force_ergast=` as a deprecated alias of `backend="ergast"`, which fastf1 3.9.0 does not have)
- [x] Lazy loading (no need for `session.load()`)
- [x] `is_testing()` on `Event` and `EventSchedule`
- [x] `get_events_remaining`, `get_testing_event`, `get_testing_session`
- [x] Schedules from 1950 (`backend="ergast"` for any season)
- [x] Session Results - the official Ergast classification: `GridPosition`, `Points`, `Status`, `ClassifiedPosition`, `Time`, `Q1`/`Q2`/`Q3`, `DriverId`, `TeamId`; tif1 adds `TimeGap`, fastest-lap facts and identity columns. `GridPosition`/`Points` are NaN outside races and sprints.
- [x] Sessions without an Ergast classification are classified from the laps as fastf1 does: practice (`Position` by best lap, `Status` `"Finished"`, `Time` the best non-deleted lap), Sprint Qualifying/Shootout and Qualifying with `ergast=False` (`Q1`-`Q3` from each segment's best accurate lap, order Q3 > Q2 > Q1, `"N"` outside 107%); practice and the sprint qualifying formats take `DriverId`/`TeamId` from the race results. Sprint Qualifying/Shootout results have no SQ1/SQ2/SQ3 columns (fastf1 has none either): the laps carry the segment in `QualifyingSession`.

### 📚 Ergast
- [x] `tif1.ergast.Ergast` with the thirteen fastf1 methods, response classes and paging
- [x] `fetch_season` / `fetch_day` / `fetch_results`
- [x] `Session.pit_stops`, `Session.driver_standings`, `Session.constructor_standings`, `tif1.get_driver_standings`, `tif1.get_constructor_standings`
- [x] Sessions before 2018 (`Qualifying`, `Race`) with results, Ergast lap timings (1996+) and pit stops (2011+)
- [/] `totalRaceTime` is the true total race time (fastf1 stores the gap for every driver but the winner); the gap is `totalRaceTimeGap`

### 🏎️ Telemetry

- [x] `lap.telemetry`, `lap.get_telemetry(frequency=)`, `laps.get_telemetry(frequency=)`
- [x] fastf1's channels in fastf1's order: `Date`, `SessionTime`, `DriverAhead`, `DistanceToDriverAhead`, `Time`, `RPM`, `Speed`, `nGear`, `Throttle`, `Brake`, `DRS`, `Source`, `Distance`, `RelativeDistance`, `Status`, `X`, `Y`, `Z`; tif1 adds `AccelerationX/Y/Z`, `Driver`, `LapNumber`
- [x] fastf1 `Telemetry` methods with their keywords: `add_distance`, `add_relative_distance`, `add_differential_distance`, `add_driver_ahead`, `add_track_status` (maps `session.track_status`), `merge_channels(frequency=)`, `resample_channels(rule=, new_date_ref=)`, `slice_by_*`, `fill_missing`, `register_new_channel`
- [/] `resample_channels(rule)` bins from the first sample on the session clock like fastf1's `merge_channels(frequency=)` (bin means, discrete channels held, `Time` from zero); fastf1's own `resample_channels(rule)` interpolates onto a grid anchored at midnight, so its samples sit up to one `rule` apart from tif1's
- [x] `session.car_data` / `session.pos_data`: a lazy dict keyed by driver number (shared cars `number/Abbreviation`), each value the driver's session-long telemetry with fastf1's clock (`Time` equals `SessionTime`; both keys hold the same merged frame)
- [x] Telemetry of several laps (`Laps.telemetry`, `get_telemetry`, `get_car_data`, `get_pos_data`) is one fastf1 slice: `Time` from the first lap's start, the shared lap-boundary sample once; `merge_channels` matches on `Date`/`SessionTime`; `add_distance()` re-integrates (`drop_existing=True`); `add_relative_distance()` divides by the last `Distance`; `interpolate_edges` adds the edge samples; `fill_missing` keeps the discrete dtypes
- [/] `Source` is `"merged"` (`"interpolation"` on interpolated samples) and `Status` always `"OnTrack"` (the payload is already the car+pos merge); `DRS` is a 0/1 flag, not the raw stream value (`>= 10` idioms must become `== 1`); `RPM`/`Speed`/`Throttle` are float64

### 🚦 Session state

- [x] `track_status` (`Time`, `Status`, `Message`), `session_status` (`Time`, `Status`), `session_info`, `t0_date`, `session_start_time`
- [/] Derived, not streamed: fastf1 reads the live-timing `TrackStatus`/`SessionStatus`/`SessionInfo` streams, which need an F1 account. tif1 derives the tables from the race-control messages (exact timestamps, inferred codes), the laps (`t0_date`) and the schedule; `session_info["Key"]`, `["Path"]`, `["ArchiveStatus"]` and `["Meeting"]["Key"]` are `None`; `session_status` has no `Finalised`/`Ends` rows
- [x] `get_driver(identifier)` returns the `results` row (`.dnf`) plus tif1's `.laps` and friends; abbreviation, car number, shared-car label or full name (`"8"` and `"8/fagioli"` pick the two rows of a shared car); an unknown identifier raises `DriverNotFoundError`, which is also fastf1's `ValueError`
- [x] `Laps`: the `pick_*` selectors with fastf1's rules (any iterable, `pick_drivers`/`pick_compounds` upper-case, `pick_driver`/`pick_team` exact, `pick_quicklaps` strictly below the threshold, `pick_fastest` among the `IsPersonalBest` laps), `iterlaps()` yields every lap, `split_qualifying_sessions()` with fastf1's algorithm and its `ValueError` outside qualifying, `get_weather_data()` with fastf1's sample rule on `session.weather_data`
- [/] `Laps.get_weather_data()` keeps the laps index (fastf1 indexes the rows by weather sample); the laps' own `WeatherTime` columns are misaligned upstream and read only without a session weather table
- [x] `load(laps=, telemetry=, weather=, messages=, livedata=)`: a `livedata` recording raises `NotImplementedError`

### 🗺️ Circuit info

- [x] `get_circuit_info()`: corners, marshal lights, marshal sectors (`X, Y, Number, Letter, Angle, Distance`) and rotation from 2018; `tif1.mvapi.get_circuit_info(year=, circuit_key=)` for fastf1's entry point
- [/] Before 2018 there is no `corners.json`: MultiViewer knows 32 circuits and has no layout before 2018, so a session gets the earliest MultiViewer layout of its circuit, and a circuit MultiViewer does not know (1995 Pacific Grand Prix) gets empty frames and `source` `""`
- [x] tif1 extras: `circuit_key`, `circuit_name`, `location`, `country`, `country_code`, `pit_loss`, `track` outline, `sections`, `mini_sector_index`, `source`

### 🧰 Modules and names

- [x] `tif1.logger` (`LoggingManager`, `get_logger`, `set_log_level`, `soft_exceptions`), `tif1.Cache.configure`, `Cache.disabled(disable_http_cache=, disable_func_cache=)`, exceptions `DataNotLoadedError`, `NoLapDataError`, `FuzzyMatchError`, `FastF1CriticalError`
- [x] `EventSchedule.get_event_by_name(exact_match=)`, `get_event_by_round(round=)`, `Event.get_session(identifier)`, `base_class_view` (also on the Ergast result frames)
- [x] `tif1.legacy.inject_driver_ahead(session)` (tif1's telemetry already carries the two driver-ahead channels)
- [ ] Out of scope: `fastf1.livetiming`, `set_auth_method`, `F1AuthError`, `fastf1.req`, `fastf1.api`, `fastf1.internals` (live timing, sign-in and the private HTTP layer)

### 🐻‍❄️ polars

- [x] `get_session(..., lib="polars")` returns `PolarsLaps`, `PolarsLap`, `PolarsTelemetry`, `PolarsSessionResults`, `PolarsDriverResult`, `PolarsDriver` with the fastf1 method set, built with native polars expressions; `df.f1.<method>()` on any plain frame that came out of a native polars operation
- [x] fastf1's dtype families on polars: `Duration("ns")` time columns, `Datetime("ns")` dates, `Boolean` flags, `Int64` gears and lap numbers
- [x] the same rows and values as the pandas session for laps, telemetry, `car_data`, results, `drivers_df`, `get_driver`, the session state and the resampling methods (checked on six 2022-2025 sessions and a 2016 Ergast-only race)
- [/] no index: `PolarsSessionResults` keeps `DriverNumber` as a column (`results.row_for("1")` looks a driver up); a lap row is `PolarsLap` (scalar access, `.telemetry`), its weather a `dict`

### 🛠️ Key Differences
- **Lazy Loading**: `tif1` fetches data automatically on property access (e.g., `session.laps`); `session.load()` prefetches.
- **Laps extras**: the laps table keeps tif1's `index`, `drv`, `team`, `WeatherTime` + weather columns, `LapTimeSeconds`, `ms1`-`ms3` next to fastf1's 31 columns; `Driver`/`Team`/`Compound`/`TrackStatus` are categorical.
- **Compliance lock**: `tests/unit/test_fastf1_surface_compliance.py` checks members, parameters and column tables against fastf1 3.9.0 whenever fastf1 is importable (`uv pip install fastf1 --no-deps`).
