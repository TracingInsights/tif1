# Compatibility with FastF1

`tif1` is designed to be a high-performance replacement for `fastf1` in most common use cases, often with a one-line import change. It keeps the fastf1 API names, but there are architectural differences and some features that are still in development.

## Comparison at a Glance

| Feature | fastf1 | tif1 |
| :--- | :--- | :--- |
| **Data Source** | Ergast + F1 Live Timing | TracingInsights (CDN) |
| **Loading** | `session.load()` downloads the whole session | **Lazy, per-file fetching** (async, parallel) |
| **Backends** | Pandas only | **Pandas + Polars** |
| **Caching** | Pickle files + SQLite HTTP cache | **SQLite + Memory LRU** |
| **Live Timing** | Supported | **Historical only (2018-present)** |

## Current Status

### 🏁 Event & Session API
Most `EVENT` and `SESSION` level APIs work as expected.
- [x] `get_session(year, event, type)`
- [x] Lazy loading (no need for `session.load()`)
- [ ] `is_testing()` - Currently not working/implemented.
- [ ] Session Results - Quali/Sprint Quali results have no Q1/Q2/Q3 (or SQ1/SQ2/SQ3) time columns. The laps carry the segment in `QualifyingSession`, and `laps.split_qualifying_sessions()` splits on it.

### 🏎️ Telemetry

- [x] `lap.telemetry`
- [x] Integrated `DriverAhead` and `DistanceToDriverAhead`
- [x] fastf1 `Telemetry` methods: `add_distance`, `add_relative_distance`, `add_differential_distance`, `merge_channels`, `resample_channels`, `slice_by_*`, `fill_missing`
- [/] **Simplified**: `add_driver_ahead` and `add_track_status` do not recompute data, and `merge_channels` / `resample_channels` ignore extra fastf1 keyword arguments. Fields like `Distance`, `RelativeDistance`, `DriverAhead` and the acceleration channels already come in the `Telemetry` object, making some `fastf1` utility functions redundant.

### 🛠️ Key Differences
- **`circuit_key`**: Not needed. `session.get_circuit_info()` reads the session's `corners.json` instead of calling fastf1's `mvapi` with a circuit key.
- **Lazy Loading**: `tif1` fetches data automatically on property access (e.g., `session.laps`).
