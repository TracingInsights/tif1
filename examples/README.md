# tif1 Examples

This directory contains runnable scripts that show the main features of tif1.
Most scripts load the 2025 Abu Dhabi Grand Prix. The first run downloads the data
from the CDN. Later runs read it from the cache.

## Quick Start

### Simple Example
Start with this script:
```bash
python examples/example.py
```

### Basic Usage
Drivers, laps, one lap of telemetry, and telemetry statistics:
```bash
python examples/basic_usage.py
```

## Examples by Feature

### Data Analysis
- **`fastest_laps.py`**: Fastest lap per driver, overall fastest lap, one driver's fastest lap, and a teammate comparison (2025 Abu Dhabi Qualifying).
- **`fastest_telemetry.py`**: Telemetry of the fastest laps, for all drivers and for a selected set of drivers (2025 Abu Dhabi Qualifying).
- **`fast_telemetry.py`**: Timed telemetry fetches for the fastest laps, and a top-speed comparison (2025 Las Vegas Race).
- **`data_exploration.py`**: Events, sessions, the lap and telemetry columns, and their units.

### Events and Schedule
- **`get_event_by_round_example.py`**: Find an event by round number with `get_event_by_round()` and `get_event()`.
- **`get_event_by_name_example.py`**: Find an event by name: exact match, circuit and country aliases, fuzzy match, and a name that matches no event.
- **`event_lookup_examples.py`**: Both lookups in one script.

### Performance and Backends
- **`async_loading.py`**: `await session.laps_async()` compared with `session.laps`. Both use the same async fetcher.
- **`polars_backend.py`**: Load laps with `lib="polars"`, aggregate them with polars, and compare with pandas.
- **`cache_management.py`**: Cache location, cached and uncached loads, and `cache.clear()`.
- **`sqlite_cache.py`**: The SQLite cache file and the three ways to set the cache directory.

### Other Features
- **`error_handling.py`**: `EventNotFoundError`, `EventNotInSeasonError`, `ValueError`, `DriverNotFoundError`, and `LapNotFoundError` examples, and a safe loading workflow.
- **`new_features_demo.py`**: Configuration, logging, the circuit breaker, the CDN manager, type definitions, and a full session load.
- **`plotting_colors_demo.py`**: Team, driver, and compound colors from `tif1.plotting`. This script uses a mock session and does not download data.

## Running Examples

Run each script from the repository root:
```bash
# Run any example
python examples/<example_name>.py

# Or with uv
uv run python examples/<example_name>.py
```

**Warning:** `cache_management.py` calls `cache.clear()`, which deletes all cached data.
To keep the cache, set `TIF1_CACHE_DIR` to an empty directory before running it:

```bash
TIF1_CACHE_DIR=/tmp/tif1-demo-cache python examples/cache_management.py
```

## Requirements

The scripts work with the base installation:
```bash
pip install tif1
```

## Learning Path

1. **Start**: `example.py`: simple introduction
2. **Learn**: `basic_usage.py`: core features
3. **Explore**: `data_exploration.py`: data structure
4. **Analyze**: `fastest_laps.py`, `fastest_telemetry.py`: analysis patterns
5. **Tune**: `async_loading.py`, `cache_management.py`: loading and caching
6. **Backends**: `polars_backend.py`: the polars backend
7. **Production**: `error_handling.py`: error handling

## Common Patterns

### Loading a Session
```python
import tif1

session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
```

### Getting Lap Data
```python
laps = session.laps  # All laps
driver = session.get_driver("VER")  # Specific driver
driver_laps = driver.laps
```

### Getting Telemetry
```python
lap = driver.get_lap(19)
telemetry = lap.telemetry
```

### Error Handling
```python
try:
    driver = session.get_driver("XXX")
except tif1.DriverNotFoundError:
    print("Driver not in this session")
```

`get_session()` raises `ValueError` for a session name that the event does not have.
It corrects a misspelled event name to the closest match and logs a warning.
It raises `tif1.EventNotFoundError` for an event name that resembles no event.

## Tips

- Use `lib="polars"` for large datasets.
- Keep the cache enabled (the default) for repeated access to the same data.
- Use `await session.laps_async()` in code that already runs in an event loop.
- Catch the tif1 exceptions (`tif1.TIF1Error` and its subclasses) in production code.

## Need Help?

- Read the [README](../README.md) for an overview.
- Read the documentation at https://tif1.tracinginsights.com.
- Open an issue at https://github.com/TracingInsights/tif1/issues for bugs or questions.
