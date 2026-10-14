# tif1

A fast, canonical Formula 1 data library fetched from TracingInsights data https://github.com/TracingInsights/2026 (2018-current).

[![CI/CD](https://github.com/TracingInsights/tif1/workflows/CI%2FCD/badge.svg)](https://github.com/TracingInsights/tif1/actions)
[![codecov](https://codecov.io/gh/TracingInsights/tif1/branch/main/graph/badge.svg)](https://codecov.io/gh/TracingInsights/tif1)
[![PyPI version](https://badge.fury.io/py/tif1.svg)](https://badge.fury.io/py/tif1)




Data is available ~30 minutes after the session ends. tif1's files are built from the same F1 timing data that fastf1 reads, so they appear a few minutes after that data reaches fastf1: tif1 has more data and does more processing to enrich it before making it available.


## Why use tif1 instead of fastf1?

First, a note of respect: `fastf1` is a great library and does a lot for the F1 data community — I've used it myself. `tif1` started as a personal project with a different approach in a few areas. Here's how the two differ:

### 1. Fetch only what you need — no full-session downloads

`fastf1` is session-oriented: you call `session.load()` and, by default, it downloads the whole session (all drivers, every lap, every telemetry point) before the data is usable. Telemetry comes as the session's full car-data and position streams for every driver, so even one lap of telemetry means downloading and storing all of it.

`tif1` is lazy and fine-grained. A `Session` object costs almost nothing until you touch a property, and you can pull exactly the one lap of telemetry you need in seconds:

```python
import tif1

session = tif1.get_session(2024, "Monaco Grand Prix", "Race")

# Fetch only Verstappen's lap 19 telemetry: the driver list, the session's lap
# table and that one lap's telemetry file, nothing else.
telemetry = session.get_driver("VER").get_lap(19).telemetry
print(telemetry[["Time", "Speed", "Throttle"]].head())
```

You pay for exactly the bytes you use: less waiting, less hard-disk space, less bandwidth. That efficiency also makes `tif1` the more energy- and environmentally-friendly choice for large or repeated analysis workloads.

### 2. No rate-limited API in the loop

`fastf1` pulls historical and current data from the Ergast-compatible **jolpica-f1** API and the unofficial F1 live-timing API. jolpica-f1 rate-limits unauthenticated access to:

- **Burst limit:** 4 requests per second
- **Sustained limit: 500 requests per hour**

and answers excessive traffic with `HTTP 429 Too Many Requests` ("Request was throttled"). Those limits are scheduled to **decrease** further as token-based access rolls out.

Apps that loop over many sessions in a single run — a whole season, an entire grid, backtesting, live dashboards — can approach these ceilings quickly. It's a real consideration in production, and one of the reasons I built `tif1` for my own app at [tracinginsights.com/analysis](https://tracinginsights.com/analysis).

`tif1` does **not call a rate-limited API**. It reads public static files from free, global CDNs (jsDelivr primary, Hugging Face bucket fallback, StaticDelivr backup) with automatic failover, retries, and a circuit breaker — no API keys, no tokens, no request quotas to track in your code.

Sources: [jolpica-f1 Terms of Use](https://github.com/jolpica/jolpica-f1/blob/main/TERMS.md) and the [jolpica-f1 Rate Limits guide](https://github.com/jolpica/jolpica-f1/blob/main/docs/rate_limits.md).

### 3. Batteries included — 22 one-call chart functions

If you enjoy hand-building charts, fastf1 gives you all the pieces to do so. `tif1` additionally ships **22 ready-made chart functions** that load the data, run the analysis, and plot in a single call:

```python
import tif1
import matplotlib.pyplot as plt

# Top speeds by team at the 2023 Italian Grand Prix — one call.
fig, ax = tif1.plot_top_speeds(2023, "Italian Grand Prix", "Q")
plt.show()

# Alonso's lap times at the 2023 Azerbaijan Grand Prix — one call.
fig, ax = tif1.plot_driver_laptimes(2023, "Azerbaijan", "R", drivers=["ALO"])
plt.show()

# Or save straight to a PNG, no interactive session needed.
tif1.plot_telemetry_comparison(
    2024, "Monaco", "Q", drivers=["VER", "LEC"], save_path="monaco_ver_lec.png"
)
```

From track maps, speed traces, and telemetry comparisons to tire degradation, qualifying grids, GG diagrams, and lap-time heatmaps, there is a ready-made analysis for almost every F1 question. See the [Charts tutorial section](docs/tutorials/top-speeds.mdx) and the [Charts API reference](docs/api-reference/charts.mdx) for the full list.

### 4. Works from anywhere — no IP restrictions

The official F1 live-timing endpoints that `fastf1` uses have been reported to block VPNs, cloud/data-center IPs, and some hosting providers, which can make fastf1 tricky to run on servers. `tif1` avoids that class of problem by serving data from static CDN files.

`tif1` serves its data from free global CDNs (jsDelivr hosting the TracingInsights data repos, with Hugging Face buckets as the fallback and StaticDelivr as the last-resort backup), so it works from home, office, cloud, CI runners and notebooks alike, with no proxies.

### 5. Extra data — mini sectors and more

`tif1` also includes a few things beyond fastf1's current scope:

- **Mini-sector data** — lap data carries the OpenF1 mini-sector status of every lap (`ms1`/`ms2`/`ms3`: one digit per mini-sector for yellow, green, purple, pit lane, and so on), when OpenF1 data can be matched to the session.
- **Per-lap weather** — weather conditions (`AirTemp`, `TrackTemp`, `Rainfall`, wind, …) merged into the lap rows.
- **Derived channels** — `DriverAhead`, `DistanceToDriverAhead`, and acceleration channels (`AccelerationX/Y/Z`) computed and included in the telemetry.
- **Flexible backends** — pandas *and* polars, with SQLite + in-memory LRU caching under the hood.
- **FastF1-style schema** — fastf1 column names for the lap and telemetry data (`Sector1Time`, `SpeedST`, `nGear`, …), so migration is often a one-line import change.

One honest trade-off: data becomes available in `tif1` a few minutes later than in fastf1, because the data is enriched and processed before being published.

## Installation

```bash
pip install tif1
```

Requires Python 3.11 or newer. Versions 3.11 through 3.15 are tested in CI.

### Optional Dependencies

```bash
# For development
pip install tif1[dev]


```

## Quick Start

```python
import tif1

# Get the event schedule for a year (a DataFrame, one row per event)
events = tif1.get_events(2025)
print(events["EventName"].tolist())  # ['Australian Grand Prix', 'Chinese Grand Prix', ...]

# Get sessions for an event
sessions = tif1.get_sessions(2025, "Chinese Grand Prix")
print(sessions)  # ['Practice 1', 'Sprint Qualifying', 'Sprint', 'Qualifying', 'Race']

# Get a session
session = tif1.get_session(2024, "Monaco Grand Prix", "Race")

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

- **Fast**: Multi-CDN access via jsDelivr (primary), Hugging Face buckets (fallback), and StaticDelivr (backup) with automatic failover and SQLite caching
- No need to session.load() - only the required data is fetched when necessary. You can just get the telemetry data of any specific lap within seconds.
- **Canonical**: Focused tif1 API surface
- **Complete**: Lap times, sectors, telemetry, tire compounds, and more
- **Historical**: Data from 2018-current
- **Reliable**: Automatic retry logic with circuit breaker and multi-CDN fallback
- **Async**: Parallel data fetching for better performance
- **Optimized**: SQLite cache holding compressed JSON payloads and ready-built DataFrames
- **Flexible**: Supports both pandas and polars backends
- **Validated**: Optional data validation with Pydantic models
- **Configurable**: .tif1rc configuration file support
- **Type-Safe**: Comprehensive type hints for IDE support
- **Jupyter-Ready**: Rich HTML display in notebooks after `import tif1.jupyter`

## Advanced Usage

### Canonical API Notes

- `tif1` exposes canonical data/session APIs directly (`get_session`, `Session`, `Laps`, `Lap`, `Driver`).
- Event/session schedules come from the f1schedule files that fastf1 also reads (`theOehrly/f1schedule`, `schedule_{year}.json`). Past seasons are served from copies vendored in the package. For the current and future seasons, tif1 downloads the live file, caches it in the SQLite cache for `schedule_ttl_hours` (default 6, env `TIF1_SCHEDULE_TTL_HOURS`), and falls back to the last cached copy, then the vendored copy, when the download fails.

### Configuration File

```python
import tif1

# Create ~/.tif1rc with your settings
# {
#   "max_retries": 5,
#   "validate_data": true,
#   "lib": "polars",
#   "cdns": [
#     "https://cdn.jsdelivr.net/gh/TracingInsights",
#     "https://huggingface.co/buckets/tracinginsights",
#     "https://cdn.staticdelivr.com/gh/TracingInsights"
#   ],
#   "cdn_use_minification": false
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

session = tif1.get_session(2024, "Monaco Grand Prix", "Race")

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
fastest_tel = session.get_fastest_lap_tel()

# Get telemetry from each driver's fastest lap (parallel fetching!)
fastest_tels = session.get_fastest_laps_tels(by_driver=True)

# Get telemetry for specific drivers' fastest laps
top3_tels = session.get_fastest_laps_tels(by_driver=True, drivers=["VER", "HAM", "LEC"])

# Get specific driver's fastest lap telemetry
ver_fastest_tel = ver.get_fastest_lap_tel()
```



### Logging

```python
import tif1
import logging

# Enable debug logging
tif1.setup_logging(logging.DEBUG)

session = tif1.get_session(2024, "Monaco Grand Prix", "Race")
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
session = tif1.get_session(2024, "Monaco Grand Prix", "Race", enable_cache=False)
```

### Error Handling

```python
import tif1

try:
    # Monaco had no Sprint: a session that is not on the event's schedule raises ValueError.
    # An event name that matches no event raises tif1.EventNotFoundError, also a ValueError.
    session = tif1.get_session(2024, "Monaco Grand Prix", "Sprint")
    laps = session.laps
except ValueError as e:
    print(f"Invalid event or session: {e}")
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
- Driver ahead and distance to the driver ahead
- `Driver` and `LapNumber` on every telemetry row

## Data Reference

`tif1` serves every session from the TracingInsights GitHub telemetry data repositories (per-year repos like `{year}`, 2018-current). Each session is stored as a set of raw JSON files. The fields of each file are documented field-by-field in [`DATA_REFERENCE.md`](DATA_REFERENCE.md):

| File | Description |
| :--- | :--- |
| `{driver}/laptimes.json` | Per-driver lap timing: lap/sector times, session times, speed traps (`vi1`/`vi2`/`vfl`/`vst`), tire compound/life/stint, OpenF1 mini-sectors (`ms1`/`ms2`/`ms3`), qualifying segment (`qs`), position, track status, personal best, pit in/out times, and data-quality flags. Also enriched with per-lap weather when it can be matched. |
| `session_laptimes.json` | The same lap fields for every driver of the session in one file (rows in alphabetical driver order). |
| `{driver}/{lap}_tel.json` | Per-lap telemetry, about 7-8 samples per second (car and position samples merged): RPM, speed, gear, throttle, brake, DRS, distance, relative distance, X/Y/Z position, driver-ahead info (`DriverAhead`, `DistanceToDriverAhead`), and derived acceleration (`acc_x`/`acc_y`/`acc_z`). |
| `weather.json` | Environmental conditions recorded ~once per minute: air/track temperature, humidity, pressure, rainfall, wind direction, wind speed. |
| `rcm.json` | Race control messages: time, category, message, status, flag, scope (Track/Sector/Driver), affected track sector, driver number, lap. |
| `drivers.json` | Static driver metadata: 3-letter code, team, car number, first/last name, team color (hex), headshot URL. |
| `corners.json` | Circuit corner geometry: corner number, X/Y coordinates, angle, distance from start/finish, circuit rotation. |

The raw JSON files use short abbreviated keys (e.g. `s1`, `vi1`, `wWS`). `tif1` maps them to fastf1-compatible PascalCase columns (`Sector1Time`, `SpeedI1`, `WindSpeed`) in its DataFrames — see the [Data Schema Reference](docs/reference/data-schema.mdx) for the canonical DataFrame layout.

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
uv run python examples/async_loading.py

# Run tests (parallel by default via xdist)
uv run pytest tests/ -v

# Run linting
uv run ruff check src/ tests/

# Run type checking
uv run ty check src/tif1

# Run benchmarks (serial for stable timing)
uv run pytest -o addopts='' tests/benchmarks/ -v -m benchmark --benchmark-only --no-cov -n 0
```

## Documentation

Full documentation available at: [tif1.tracinginsights.com](https://tif1.tracinginsights.com)

## Contributing


Contributions are welcome! A few ideas what you can contribute:

    Improve documentation.
    Add more tests.
    Improve performance.
    Found a bug? Fix it!
    Made an article about tif1? Great! Let's add it into the README.md.
    Don't have time to code? No worries! Just tell your friends and subscribers about the project. More users -> more contributors -> more cool features.


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
| `get_event_schedule` | supported | fastf1 schedule columns plus `GmtOffset`; testing rows (round 0) kept unless `include_testing=False` |
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
| `get_circuit_info` | partial | `corners` and `rotation` from `corners.json`; `marshal_lights` / `marshal_sectors` are empty |
| `results` | partial | classification order, status and laps; no `Q1`/`Q2`/`Q3` or `Time`; `GridPosition` and `Points` are NaN |
| `session_info` | partial | minimal metadata dict (`Year`, `EventName`, `SessionName`) |
| `session_start_time` / `t0_date` | partial | placeholder: always `NaT` |
| `session_status` | partial | returns the race control messages table |
| `track_status` | partial | returns the per-lap `TrackStatus` column of `session.laps` |
| `race_control_messages` | partial | available when source data exists |
| `weather_data` | partial | passthrough compatibility |

### `core.Laps`, `core.Lap`, `core.Telemetry`

| FastF1 API | tif1 status | Notes |
|---|---|---|
| `Laps.pick_driver(s)` | supported | implemented |
| `Laps.pick_lap(s)` | supported | implemented |
| `Laps.pick_fastest` | supported | implemented |
| `Laps.get_car_data` / `get_pos_data` | supported | both return the merged telemetry (car and position channels) |
| `Laps.get_weather_data` | partial | returns the whole session's weather table |
| `Laps.split_qualifying_sessions` | supported | splits on the `QualifyingSession` (Q1/Q2/Q3) column; returns three full copies when that column is missing |
| `Lap.get_telemetry` / `get_car_data` / `get_pos_data` | supported | all return the merged telemetry (car and position channels) |
| `Telemetry.add_distance` | supported | keeps an existing `Distance` column (`drop_existing` is ignored) |
| `Telemetry.add_relative_distance` | supported | keeps an existing `RelativeDistance` column |
| `Telemetry.add_differential_distance` | supported | implemented |
| `Telemetry.merge_channels` / `resample_channels` | partial | extra fastf1 keyword arguments are ignored; `merge_channels` merges on the nearest `Time`, `resample_channels` keeps numeric columns only |
| `Telemetry.slice_by_lap` / `slice_by_time` / `slice_by_mask` | supported | `interpolate_edges` is accepted but ignored |
| `Telemetry.fill_missing` | supported | implemented |
| `Telemetry.add_driver_ahead` | partial | the columns come precomputed in the data; on a frame without them it adds empty (`None`/NaN) columns |
| `Telemetry.add_track_status` | partial | adds a constant `TrackStatus` of `"1"` when the column is missing |

## What's planned to be added in tif1 from fastf1

High-priority API gaps for parity with `docs.fastf1.dev`:

- `get_testing_session`
- `get_testing_event`
- `get_events_remaining`
- deeper parity for `Session` metadata/classification fields

## What's out of scope for tif1 from fastf1

- Ergast (`fastf1.ergast`)
- LiveTiming (`fastf1.livetiming`)
