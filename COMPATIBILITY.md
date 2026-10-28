# Compatibility with FastF1

`tif1` is designed to be a high-performance replacement for `fastf1` in most common use cases, often with a one-line import change. It keeps the fastf1 API names, but there are architectural differences and some features that are still in development.

## Comparison at a Glance

| Feature | fastf1 | tif1 |
| :--- | :--- | :--- |
| **Data Source** | Ergast + F1 Live Timing | TracingInsights (CDN); Ergast tables from the TracingInsights Stats repository, Jolpica only as fallback |
| **Loading** | `session.load()` downloads the whole session | **Lazy, per-file fetching** (async, parallel) |
| **Backends** | Pandas only | **Pandas + Polars** |
| **Caching** | Pickle files + SQLite HTTP cache | **SQLite + Memory LRU** |
| **Live Timing** | Supported | **Historical only (timing/telemetry 2018-present, Ergast results 1950-present)** |

## Current Status

### 🏁 Event & Session API
Most `EVENT` and `SESSION` level APIs work as expected.
- [x] `get_session(year, event, type)` (with fastf1's `backend=`/`force_ergast=` keywords)
- [x] Lazy loading (no need for `session.load()`)
- [x] `is_testing()` on `Event` and `EventSchedule`
- [x] `get_events_remaining`, `get_testing_event`, `get_testing_session`
- [x] Schedules from 1950 (`backend="ergast"` for any season)
- [x] Session Results - the official Ergast classification: `GridPosition`, `Points`, `Status`, `ClassifiedPosition`, `Time`, `Q1`/`Q2`/`Q3`, `DriverId`, `TeamId`; tif1 adds `TimeGap`, fastest-lap facts and identity columns. Sprint Qualifying/Shootout results have no SQ1/SQ2/SQ3 columns: the laps carry the segment in `QualifyingSession`, and `laps.split_qualifying_sessions()` splits on it.

### 📚 Ergast
- [x] `tif1.ergast.Ergast` with the thirteen fastf1 methods, response classes and paging
- [x] `fetch_season` / `fetch_day` / `fetch_results`
- [x] `Session.pit_stops`, `Session.driver_standings`, `Session.constructor_standings`, `tif1.get_driver_standings`, `tif1.get_constructor_standings`
- [x] Sessions before 2018 (`Qualifying`, `Race`) with results, Ergast lap timings (1996+) and pit stops (2011+)
- [/] `totalRaceTime` is the true total race time (fastf1 stores the gap for every driver but the winner); the gap is `totalRaceTimeGap`

### 🏎️ Telemetry

- [x] `lap.telemetry`
- [x] Integrated `DriverAhead` and `DistanceToDriverAhead`
- [x] fastf1 `Telemetry` methods: `add_distance`, `add_relative_distance`, `add_differential_distance`, `merge_channels`, `resample_channels`, `slice_by_*`, `fill_missing`
- [/] **Simplified**: `add_driver_ahead` and `add_track_status` do not recompute data, and `merge_channels` / `resample_channels` ignore extra fastf1 keyword arguments. Fields like `Distance`, `RelativeDistance`, `DriverAhead` and the acceleration channels already come in the `Telemetry` object, making some `fastf1` utility functions redundant.

### 🛠️ Key Differences
- **`circuit_key`**: Not needed. `session.get_circuit_info()` reads the session's `corners.json` instead of calling fastf1's `mvapi` with a circuit key.
- **Lazy Loading**: `tif1` fetches data automatically on property access (e.g., `session.laps`).
