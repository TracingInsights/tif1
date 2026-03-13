# tif1

A fast, canonical Formula 1 data library fetched from TracingInsights data https://github.com/TracingInsights/2026 (2018-current).

[![CI/CD](https://github.com/TracingInsights/tifone/workflows/CI%2FCD/badge.svg)](https://github.com/TracingInsights/tif1/actions)
[![codecov](https://codecov.io/gh/TracingInsights/tifone/branch/main/graph/badge.svg)](https://codecov.io/gh/TracingInsights/tif1)
[![PyPI version](https://badge.fury.io/py/tifone.svg)](https://badge.fury.io/py/tifone)




Data is available ~30 minutes after the session ends. Data in fastf1 is available ~20-25 minutes after the session ends. So there is a slightly longer delay of 2~5 minutes for data availability in tif1 compared to fastf1, but this is because tif1 has more data and does more processing to enrich the data before making it available.


## Installation

```bash
pip install tifone
```

### Optional Dependencies

```bash
# For development
pip install tifone[dev]


```

## Quick Start

```python
import tif1

# Get available events for a year
events = tif1.get_events(2025)
print(events)  # ['Australian Grand Prix', 'Chinese Grand Prix', ...]

# Get sessions for an event
sessions = tif1.get_sessions(2025, "Chinese Grand Prix")
print(sessions)  # ['Practice 1', 'Sprint Qualifying', 'Sprint', 'Qualifying', 'Race']

# Get a session
session = tif1.get_session(2021, "Belgian Grand Prix", "Race")

# Get all drivers as DataFrame
print(session.drivers_df)

# Get all laps
laps = session.laps
print(laps.head())

# Get specific driver
ver = session.get_driver("VER")
ver_laps = ver.laps

# Get telemetry for a specific lap
lap = ver.get_lap(19)
telemetry = lap.telemetry
print(telemetry[["Time", "Speed", "Throttle"]].head())
```

## Features

- **Fast**: Direct CDN access via jsDelivr with SQLite caching
- No need to session.load() - only the required data is fetched when necessary. You can just get the telemetry data of any specific lap within seconds.
- **Canonical**: Focused tif1 API surface
- **Complete**: Lap times, sectors, telemetry, tire compounds, and more
- **Historical**: Data from 2018-current
- **Reliable**: Automatic retry logic with circuit breaker and CDN fallback
- **Async**: Parallel data fetching for better performance
- **Optimized**: SQLite cache with JSON storage
- **Flexible**: Supports both pandas and polars backends
- **Validated**: Optional data validation with Pydantic models
- **Configurable**: .tif1rc configuration file support
- **Type-Safe**: Comprehensive type hints for IDE support
- **Jupyter-Ready**: Rich HTML display in notebooks

## Advanced Usage

### Canonical API Notes

- `tif1` exposes canonical data/session APIs directly (`get_session`, `Session`, `Laps`, `Lap`, `Driver`).
- Event/session schedules are loaded from packaged JSON assets and schema-validated at runtime.

### Configuration File

```python
import tif1

# Create ~/.tif1rc with your settings
# {
#   "max_retries": 5,
#   "validate_data": true,
#   "backend": "polars"
# }

# Get configuration
config = tif1.get_config()
print(f"Max retries: {config.get('max_retries')}")

# Set configuration
config.set("validate_data", True)
config.save()
```

### Circuit Breaker & CDN Fallback

```python
import tif1

# Check circuit breaker status
cb = tif1.get_circuit_breaker()
print(f"Circuit breaker: {cb.state}")

# Check CDN sources
cdn = tif1.get_cdn_manager()
sources = cdn.get_sources()
print(f"Available CDNs: {len(sources)}")

# Reset if needed
if cb.state == "open":
    tif1.reset_circuit_breaker()
```


### Fastest Laps & Telemetry (Optimized for Speed)

```python
import tif1

session = tif1.get_session(2021, "Belgian Grand Prix", "Race")

# Get fastest lap per driver
fastest_by_driver = session.get_fastest_laps(by_driver=True)
# Pandas backend: LapTime is Timedelta, LapTimeSeconds is numeric seconds
print(fastest_by_driver[["Driver", "LapTime", "LapTimeSeconds"]].head())

# Get overall fastest lap
overall_fastest = session.get_fastest_laps(by_driver=False)

# Get driver's fastest lap
ver = session.get_driver("VER")
ver_fastest = ver.get_fastest_lap()

# Get telemetry from overall fastest lap
fastest_tel = session.get_fastest_lap_tel()  # ~1.3s

# Get telemetry from each driver's fastest lap (parallel fetching!)
fastest_tels = session.get_fastest_laps_tels(by_driver=True)  # ~0.4s for 19 drivers

# Get telemetry for specific drivers' fastest laps
top3_tels = session.get_fastest_laps_tels(by_driver=True, drivers=["VER", "HAM", "LEC"])  # ~0.13s

# Get specific driver's fastest lap telemetry
ver_fastest_tel = ver.get_fastest_lap_tel()  # ~0.08s
```



### Logging

```python
import tif1
import logging

# Enable debug logging
tif1.setup_logging(logging.DEBUG)

session = tif1.get_session(2021, "Belgian Grand Prix", "Race")
```

### Cache Management

```python
import tif1

# Get cache instance (SQLite)
cache = tif1.get_cache()
print(f"Cache location: {cache.cache_dir}")

# Clear cache
cache.clear()

# Disable caching for a session
session = tif1.get_session(2021, "Belgian Grand Prix", "Race", enable_cache=False)
```

### Error Handling

```python
import tif1

try:
    session = tif1.get_session(2025, "Invalid GP", "Practice 1")
    laps = session.laps
except tif1.DataNotFoundError:
    print("Data not available")
except tif1.NetworkError:
    print("Network error occurred")
except tif1.InvalidDataError:
    print("Data is corrupted")
```


## Data Available

- Lap times and sectors (S1, S2, S3)
  - `LapTime` is canonicalized as `Timedelta` on pandas backends
  - `LapTimeSeconds` is provided as a numeric helper for sorting/comparisons
- Tire compounds and stint information
- Telemetry: speed, throttle, brake, RPM, gear, DRS
- Position data (X, Y, Z coordinates)
- Acceleration data (X, Y, Z axes)
- Distance and relative distance
- Unique data identifiers

## Session Types

- Practice 1, Practice 2, Practice 3
- Qualifying
- Sprint, Sprint Qualifying, Sprint Shootout
- Race

## Development

```bash
# Install with uv
uv sync --all-extras

# Install git hooks with prek (commit + push)
uv run prek install

# Run the full local quality gate (same checks as CI lint job)
uv run prek run --all-files

# Run example
uv run python examples/basic_usage.py

# Run tests (parallel by default via xdist)
uv run pytest tests/ -v

# Run linting
uv run ruff check src/ tests/

# Run type checking
uv run ty check src/tif1

# Run benchmarks (serial for stable timing)
uv run pytest -o addopts='' tests/test_benchmarks.py -v -m benchmark --benchmark-only --no-cov -n 0
```

## Documentation

Full documentation available at: [docs.tracinginsights.com](docs.tracinginsights.com)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details.



## What's available from fastf1

Verified against `docs.fastf1.dev` (FastF1 API reference) and current `tif1` code/tests.

Verification basis:
- FastF1 docs: session/event API, `core.Session`, `core.Laps`, `core.Lap`, `core.Telemetry`


Status key:
- `supported`: implemented with compatible behavior
- `partial`: implemented with simplified/placeholder behavior
- `missing`: not implemented yet

### Top-level and event/session API

| FastF1 API | tif1 status | Notes |
|---|---|---|
| `get_session` | supported | round/event aliases supported |
| `get_event` | supported | event lookup supported |
| `get_event_schedule` | partial | minimal schedule shape |
| `Cache.enable_cache` | supported | compatibility shim |
| `set_log_level` | supported | compatibility shim |
| `plotting.setup_mpl` | supported | compatibility helper module |
| `utils` module | partial | core helpers implemented; not full parity |
| `get_testing_session` | missing | planned |
| `get_testing_event` | missing | planned |
| `get_events_remaining` | missing | planned |

### `core.Session`

| FastF1 API | tif1 status | Notes |
|---|---|---|
| `load` | supported | lazy-load compatible |
| `laps` | supported | main lap timing surface |
| `get_driver` | supported | driver validation present |
| `get_circuit_info` | supported | compatibility object returned |
| `results` | partial | reduced classification richness |
| `session_info` | partial | minimal metadata dict |
| `session_start_time` / `t0_date` | partial | may be placeholder when unavailable |
| `session_status` | partial | derived compatibility behavior |
| `track_status` | partial | derived compatibility behavior |
| `race_control_messages` | partial | available when source data exists |
| `weather_data` | partial | passthrough compatibility |

### `core.Laps`, `core.Lap`, `core.Telemetry`

| FastF1 API | tif1 status | Notes |
|---|---|---|
| `Laps.pick_driver(s)` | supported | implemented |
| `Laps.pick_lap(s)` | supported | implemented |
| `Laps.pick_fastest` | supported | implemented |
| `Laps.get_car_data` / `get_pos_data` | supported | telemetry-compatible output |
| `Laps.get_weather_data` | partial | session passthrough |
| `Laps.split_qualifying_sessions` | partial | tuple contract kept, no true Q1/Q2/Q3 split |
| `Lap.get_telemetry` / `get_car_data` / `get_pos_data` | supported | implemented |
| `Telemetry.add_distance` | supported | implemented |
| `Telemetry.add_relative_distance` | supported | implemented |
| `Telemetry.add_differential_distance` | supported | implemented |
| `Telemetry.merge_channels` / `resample_channels` | supported | implemented |
| `Telemetry.slice_by_lap` / `slice_by_time` / `slice_by_mask` | supported | implemented |
| `Telemetry.fill_missing` | supported | implemented |
| `Telemetry.add_driver_ahead` | partial | placeholder-compatible behavior |
| `Telemetry.add_track_status` | partial | column injection compatibility |

## What's planned to be added in tif1 from fastf1

High-priority API gaps for parity with `docs.fastf1.dev`:

- `get_testing_session`
- `get_testing_event`
- `get_events_remaining`
- richer `get_event_schedule` parity
- deeper parity for `Session` metadata/classification fields

## What's out of scope for tif1 from fastf1

- Ergast (`fastf1.ergast`)
- LiveTiming (`fastf1.livetiming`)


Full detailed matrix: `docs/fastf1_compliance_matrix.md`
