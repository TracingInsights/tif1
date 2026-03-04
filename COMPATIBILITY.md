# Compatibility with FastF1

`tif1` is designed to be a high-performance, drop-in replacement for `fastf1` in most common use cases. While it maintains high API parity, there are architectural differences and some features that are still in development.

## Comparison at a Glance

| Feature | fastf1 | tif1 |
| :--- | :--- | :--- |
| **Data Source** | Ergast + F1 Live Timing | TracingInsights (CDN) |
| **Loading Speed** | Baseline | **4-5x faster** (Async by default) |
| **Backends** | Pandas only | **Pandas + Polars** |
| **Caching** | File-based Pickle | **SQLite + Memory LRU** |
| **Live Timing** | Supported | **Historical only (2018-present)** |

## Current Status

### 🏁 Event & Session API
Most `EVENT` and `SESSION` level APIs work as expected.
- [x] `get_session(year, event, type)`
- [x] Lazy loading (no need for `session.load()`)
- [ ] `is_testing()` - Currently not working/implemented.
- [ ] Session Results - Quali/Sprint Quali results are not yet subdivided into Q1/Q2/Q3 or SQ1/SQ2/SQ3.

### 🏎️ Telemetry

- [x] `lap.telemetry`
- [x] Integrated `DriverAhead` and `DistanceToDriverAhead`
- [/] **Work in Progress**: Most legacy telemetry methods are being migrated. Many "essential" fields like `Distance`, `DriverAhead`, and `TrackStatus` are already available directly in the `Telemetry` object, making some `fastf1` utility functions redundant.

### 🛠️ Key Differences
- **`circuit_key`**: Not needed or used in `mvapi` (TracingInsights backend).
- **Lazy Loading**: `tif1` fetches data automatically on property access (e.g., `session.laps`).
