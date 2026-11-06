# Compatibility with FastF1

`tif1` is designed to be a high-performance replacement for `fastf1` in most common use cases, often with a one-line import change. It keeps the fastf1 API names, but there are architectural differences and some features that are still in development.

## Comparison at a Glance

| Feature | fastf1 | tif1 |
| :--- | :--- | :--- |
| **Data Source** | Ergast + F1 Live Timing | TracingInsights (CDN); Ergast tables from the TracingInsights Stats repository, Jolpica only as fallback |
| **Loading** | `session.load()` downloads the whole session, telemetry included | **Lazy, per-file fetching** (async, parallel); `session.load()` fetches no telemetry (see [Lazy telemetry in `load()`](#lazy-telemetry-in-load)) |
| **Backends** | Pandas only | **Pandas + Polars** |
| **Caching** | Pickle files + SQLite HTTP cache | **SQLite + Memory LRU** |
| **Live Timing** | Supported | **Historical only (timing/telemetry 2018-present, Ergast results 1950-present)**; track/session status, session info and `t0_date` from the published `session_info.json` (2018-present) |

## Current Status

### 🏁 Event & Session API
Most `EVENT` and `SESSION` level APIs work as expected.
- [x] `get_session(year, event, type)` (with fastf1's `identifier=`, `exact_match=` and `backend=` keywords; tif1 also keeps `force_ergast=` as a deprecated alias of `backend="ergast"`, which fastf1 3.9.0 does not have)
- [x] Lazy loading (no need for `session.load()`)
- [x] `is_testing()` on `Event` and `EventSchedule`
- [x] `get_events_remaining`, `get_testing_event`, `get_testing_session`
- [x] Schedules from 1950 (`backend="ergast"` for any season)
- [x] Session Results - the official Ergast classification: `GridPosition`, `Points`, `Status`, `ClassifiedPosition`, `Time`, `Q1`/`Q2`/`Q3`, `DriverId`, `TeamId`; tif1 adds `TimeGap`, fastest-lap facts and identity columns. `GridPosition`/`Points` are NaN outside races and sprints.
- [x] Sessions without an Ergast classification are classified from the laps as fastf1 does: practice (`Position` by best lap, `Status` `"Finished"`, `Time` the best non-deleted lap), Sprint Qualifying/Shootout and Qualifying with `ergast=False` (`Q1`-`Q3` from each segment's best accurate lap, order Q3 > Q2 > Q1, `"N"` outside 107%, the knockout voiding later segment times of an eliminated driver); practice and the sprint qualifying formats take `DriverId`/`TeamId` from the race results. Sprint Qualifying/Shootout results have no SQ1/SQ2/SQ3 columns (fastf1 has none either): the laps carry the segment in `QualifyingSession`.

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
- [x] From the published `session_info.json` (every session from 2018): fastf1's own live-timing `TrackStatus`/`SessionStatus`/`SessionInfo` values and `t0_date`, without an F1 account. A session without the file falls back to a derivation from the race-control messages (exact timestamps, inferred codes), the laps (`t0_date`) and the schedule; then `session_info["Key"]`, `["Path"]`, `["ArchiveStatus"]` and `["Meeting"]["Key"]` are `None` and `session_status` has no `Finalised`/`Ends` rows. On a polars session `t0_date` and `session_start_time` are `datetime.datetime` and `datetime.timedelta` (`None` when unknown); pandas sessions keep `Timestamp`/`Timedelta`/`NaT`
- [x] `get_driver(identifier)` returns the `results` row (`.dnf`) plus tif1's `.laps` and friends; abbreviation, car number, shared-car label or full name (`"8"` and `"8/fagioli"` pick the two rows of a shared car); an unknown identifier raises `DriverNotFoundError`, which is also fastf1's `ValueError`
- [x] `Laps`: the `pick_*` selectors with fastf1's rules (any iterable, `pick_drivers`/`pick_compounds` upper-case, `pick_driver`/`pick_team` exact, `pick_quicklaps` strictly below the threshold, `pick_fastest` among the `IsPersonalBest` laps), `iterlaps()` yields every lap, `split_qualifying_sessions()` with fastf1's algorithm and its `ValueError` outside qualifying, `get_weather_data()` with fastf1's sample rule on `session.weather_data`
- [/] `Laps.get_weather_data()` keeps the laps index (fastf1 indexes the rows by weather sample); the laps' own `WeatherTime` columns are misaligned upstream and read only without a session weather table
- [/] `load(laps=, telemetry=, weather=, messages=, livedata=)`: a `livedata` recording raises `NotImplementedError`; `telemetry=True` is lazy by default (see [Lazy telemetry in `load()`](#lazy-telemetry-in-load)); `telemetry="eager"` is fastf1's load

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
- **Lazy Loading**: `tif1` fetches data automatically on property access (e.g., `session.laps`); `session.load()` prefetches laps, weather and messages, but not telemetry (next section).
- **Laps extras**: the laps table keeps tif1's `index`, `drv`, `team`, `WeatherTime` + weather columns, `LapTimeSeconds`, `ms1`-`ms3` next to fastf1's 31 columns; `Driver`/`Team`/`Compound`/`TrackStatus` are categorical.
- **HTTP**: fastf1 uses `requests` for each request. tif1 uses niquests. A cold load gets its first files, and the first telemetry requests, with the Python standard library until niquests has an open connection. These requests verify the certificate chain and the host name, but they do not do the revocation check (OCSP). `cdn_first_wave=False` (`TIF1_CDN_FIRST_WAVE=false`) sends every request through niquests.
- **Compliance lock**: `tests/unit/test_fastf1_surface_compliance.py` checks members, parameters and column tables against fastf1 3.9.0 whenever fastf1 is importable (`uv pip install fastf1 --no-deps`).

## Lazy telemetry in `load()`

fastf1's `session.load()` downloads the car data and position data of every
driver. tif1's `session.load()` (and `load(telemetry=True)`) does not fetch
telemetry: the `load_telemetry_lazy` config key is `True` by default. The
frames a script reads are the same; what differs is when the downloads
happen, when errors show, and what the cache holds.

- **When telemetry is downloaded.** Telemetry is fetched per driver, when the
  script first reads it:
  - A read that needs several laps of a driver (`laps.telemetry`,
    `laps.get_telemetry()`, `session.car_data[n]` / `session.pos_data[n]`)
    fetches all of that driver's laps in one parallel wave.
  - On a cold session, a driver's first read that asks for one lap
    (`lap.telemetry`, `lap.get_telemetry()`, `lap.get_car_data()`,
    `lap.get_pos_data()`, `pick_fastest().get_telemetry()`,
    `get_fastest_lap_tel()`) fetches only that lap's file. The driver's next
    read brings the rest of its laps in one wave. One driver's fastest-lap
    telemetry on a cold 2025 Bahrain race: 1.05 s instead of 2.9 s (pandas),
    0.92 s instead of 2.8 s (polars); four drivers' fastest laps 1.1 s
    instead of 4.2 s (pandas), 1.5 s instead of 4.4 s (polars).
  - An `iterlaps()` loop over two or more laps of a driver fetches that
    driver's wave at its first lap, not one lap and then the wave.
  - The driver that the background lookahead expects next, and a lap the
    background fetch already holds, also go through the wave.

  Later laps of the driver come from memory. When the script reads drivers
  in laps-table order, the next driver is fetched in the background
  (`lazy_telemetry_lookahead`, default 1; `-1` every pending driver; `0`
  off), in the free request slots only (`lazy_telemetry_background_requests`,
  default 22). Pyodide has no threads, so there each driver is one
  foreground wave. `fetch_all_laps_telemetry()` fetches every driver not
  read yet in one batch. Sessions the cache holds never take the one-lap
  path.
- **Errors.** `load()` reports no telemetry error. A failed request is logged
  as a warning (`Failed telemetry load for VER lap 12: ...`; after three per
  driver, one "suppressed" warning, then debug level) when the lap is first
  read, and the lap gets an empty telemetry frame with fastf1's columns. No
  exception is raised; the eager load behaves the same per lap, but warns
  inside `load()`. A failed background request is logged (`Lazy telemetry
  fetch failed for ...`) and the lap's first access requests it again with
  the normal CDN fallback and retries. A failed one-lap fetch is retried the
  same way, by the per-lap path, which logs the failure if it fails again.
- **Cache.** `load()` writes no telemetry to the cache. Each driver is cached
  when the script reads it (payload and frame tiers, as the eager load writes
  them); a later process reads those drivers from the cache and the others
  from the CDN. A lap fetched alone goes to the payload tier only; a later
  process builds its frame from that payload, without the network.
- **Work without a network.** With `offline_mode` on, or no network, a driver
  the script did not read before has no telemetry: its first access logs a
  warning and returns an empty frame. Fetch everything while online with
  `session.load(telemetry="eager")` or `session.fetch_all_laps_telemetry()`.
- **Warm cache.** pandas sessions read each cached driver row on first access
  with either setting. polars sessions do the same with either setting (the
  eager `load()` of a polars session whose frame tier holds every lap reads
  no driver row either): the
  first two drivers read their rows alone, the third read takes every row
  still pending in one batch, and an `iterlaps()` loop reads the rows of all
  its drivers at once. A warm polars `load()` takes ~7-14 ms instead of
  110-160 ms, and the first lap loop takes that time instead.
- **Other.** `load_async()` takes the same values (`"eager"` awaits
  `fetch_all_laps_telemetry_async()`). Sessions before 2018 have no telemetry
  either way. At interpreter exit the background requests are cancelled; one
  already running can delay the exit by ~0.2-0.5 s. `"lazy"` / `"eager"` are
  case-insensitive; any other string raises `ValueError`.

Cold cache, 2025 Bahrain Grand Prix race, whole process (Elis E10):

| Script | `telemetry="eager"` | lazy (default) |
| :--- | ---: | ---: |
| `load()`, then laps and results | 8.5 s in `load()` | 0.13 s in `load()` |
| `load()`, then four drivers' telemetry | 10.0 s | 3.0 s |
| `load()`, then every lap of every driver | 10.3 s | 9.2 s |

fastf1's behaviour, per call or for every call:

```python
session.load(telemetry="eager")
tif1.get_config().set("load_telemetry_lazy", False)
# or TIF1_LOAD_TELEMETRY_LAZY=false, or {"load_telemetry_lazy": false} in .tif1rc
```
