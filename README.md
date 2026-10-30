# tif1

A fast, canonical Formula 1 data library fetched from TracingInsights data https://github.com/TracingInsights/2026 (timing and telemetry 2018-current) and the TracingInsights Stats repository https://github.com/TracingInsights-Archive/Stats (Ergast results, lap times, pit stops and standings 1950-current).

[![CI/CD](https://github.com/TracingInsights/tif1/workflows/CI%2FCD/badge.svg)](https://github.com/TracingInsights/tif1/actions)
[![codecov](https://codecov.io/gh/TracingInsights/tif1/branch/main/graph/badge.svg)](https://codecov.io/gh/TracingInsights/tif1)
[![PyPI version](https://badge.fury.io/py/tif1.svg)](https://badge.fury.io/py/tif1)




The timing and telemetry data is generated with [fastf1](https://github.com/theOehrly/Fast-F1). Extra channels are added, and the files are published in under a minute after fastf1 has the data.


## Why use tif1 instead of fastf1?

I still use fastf1. The timing and telemetry data in `tif1` is generated with fastf1, then processed and published to the TracingInsights data repositories. `tif1` differs in how you get that data:

### 1. Download only the data you use

fastf1 loads a whole session at a time. By default `session.load()` downloads every driver, every lap and all telemetry before you can use any of it. Telemetry comes as the full car-data and position streams for every driver, so one lap of telemetry still means downloading all of them.

tif1 loads data per file, when you ask for it. Creating a `Session` downloads no data; each property fetches only the files it needs:

```python
import tif1

session = tif1.get_session(2024, "Monaco Grand Prix", "Race")

# Downloads the driver list, the lap table and the telemetry file of
# Verstappen's lap 19. Nothing else.
telemetry = session.get_driver("VER").get_lap(19).telemetry
print(telemetry[["Time", "Speed", "Throttle"]].head())
```

Less data means faster loads, less disk space and less bandwidth. This matters most when you process many sessions.

### 2. No F1TV account needed

In October 2026 Formula 1 started requiring authentication on its data APIs. Since fastf1 3.9.0, most fastf1 features need an F1TV Access (or higher) subscription, and fastf1 signs in to your F1 account: with the `F1_USERNAME`/`F1_PASSWORD` environment variables, a prompt in the terminal, or a browser add-on. The login token usually expires after four days. fastf1 cannot refresh it, so you sign in again (fastf1 does this for you when the credentials are in environment variables).

tif1 does not call the F1 data APIs. It downloads files that were already generated with fastf1 and published to the CDNs, so you need no F1 account, no subscription and no sign-in, and there is no token to expire. Servers, CI jobs and notebooks need no stored credentials.

Source: [fastf1 F1TV Account Authentication](https://docs.fastf1.dev/api_reference/accounts_auth.html).

### 3. No rate limits on the session data

fastf1 sends every network request through its own rate limiter (`fastf1/req.py` in fastf1 3.9.0). Requests to the F1 live-timing archive (`livetiming.formula1.com` and the fastf1 mirror), MultiViewer circuit data, jolpica-f1 and the schedule file on GitHub all count toward the same limits:

| Requests | Limit | When the limit is reached |
| :--- | :--- | :--- |
| All APIs together | 4 per second (at least 0.25 s between requests) | fastf1 waits |
| All APIs together | 500 per hour | fastf1 raises `RateLimitExceededError` |
| jolpica-f1 only | 200 per hour (these also count toward the 500) | fastf1 raises `RateLimitExceededError` |

Responses from the fastf1 cache do not count. An uncached session load makes one request per live-timing stream, plus jolpica-f1 requests for the results. A script that loops over many uncached sessions (a full season, backtests, live dashboards) reaches 500 per hour and stops with an error. This is one reason I built tif1 for my own app, [tracinginsights.com/analysis](https://tracinginsights.com/analysis).

jolpica-f1 also has its own limits on the server: 4 requests per second and 500 per hour for clients without a token, and `HTTP 429 Too Many Requests` above that. It plans to lower these limits when it adds token access.

tif1 reads timing, telemetry and the Ergast tables (results, standings, lap times, pit stops) as static files from public CDNs: jsDelivr first, then Hugging Face buckets, then StaticDelivr. There are no API keys or request quotas. tif1 retries failed requests, moves to the next CDN when one fails, and has a circuit breaker.

The Ergast tables come from the [TracingInsights Stats repository](https://github.com/TracingInsights-Archive/Stats) on GitHub, through the same CDNs, so in normal use tif1 does not call jolpica-f1 and its limits do not apply. tif1 calls jolpica-f1 only:

- for a round that is not in the Stats repository yet (the repository is updated a few hours after jolpica-f1 has the data), and
- for the few queries the files do not cover: the season and driver lists, `status=` filters, queries without a season, and `current`/`last`.

These calls keep to the stricter of the fastf1 and jolpica-f1 limits:

- At most 4 requests in any second and 200 in any hour, shared by all threads of the process. When a limit is reached, tif1 waits. It does not raise an error.
- On `HTTP 429`, tif1 waits for the `Retry-After` time and retries, up to 3 times.
- Answers are cached for `ergast_ttl_hours`, so a repeated query makes no new request.
- `ergast_rate_limit_burst` and `ergast_rate_limit_hourly` (env `TIF1_ERGAST_RATE_LIMIT_BURST`, `TIF1_ERGAST_RATE_LIMIT_HOURLY`) change the limits.

The MultiViewer API (circuit data) is served through Cloudflare's free CDN at no cost to MultiViewer, so tif1 does not rate-limit it. Its answers are cached anyway: a circuit layout is fetched once and kept, the circuit index is kept for `schedule_ttl_hours`.

Sources: [fastf1 Rate Limits and Caching](https://docs.fastf1.dev/api_reference/cache_and_rate_limits.html), `fastf1/req.py`, [jolpica-f1 Terms of Use](https://github.com/jolpica/jolpica-f1/blob/main/TERMS.md) and the [jolpica-f1 Rate Limits guide](https://github.com/jolpica/jolpica-f1/blob/main/docs/rate_limits.md).

### 4. Built-in charts

fastf1 gives you the data and plotting helpers, and you build each chart yourself. tif1 also has 22 chart functions. Each one loads the data, runs the analysis and draws the chart in one call:

```python
import tif1
import matplotlib.pyplot as plt

# Top speeds by team, 2023 Italian Grand Prix qualifying.
fig, ax = tif1.plot_top_speeds(2023, "Italian Grand Prix", "Q")
plt.show()

# Alonso's lap times, 2023 Azerbaijan Grand Prix race.
fig, ax = tif1.plot_driver_laptimes(2023, "Azerbaijan", "R", drivers=["ALO"])
plt.show()

# Save to a PNG instead of showing the figure.
tif1.plot_telemetry_comparison(
    2024, "Monaco", "Q", drivers=["VER", "LEC"], save_path="monaco_ver_lec.png"
)
```

The charts include track maps, speed traces, telemetry comparisons, tire degradation, qualifying grids, GG diagrams and lap-time heatmaps. See the [Charts tutorial section](docs/tutorials/top-speeds.mdx) and the [Charts API reference](docs/api-reference/charts.mdx) for the full list.

### 5. No IP blocking

The F1 live-timing endpoints that fastf1 uses have been reported to block VPNs, cloud and data-center IPs, and some hosting providers, so fastf1 can fail on servers.

tif1 reads static files from the CDNs listed above, so it works the same on a laptop, a cloud server, a CI runner or in a notebook, without a proxy.

### 6. Extra data and features

- **Mini sectors**: each lap has the OpenF1 mini-sector status (`ms1`/`ms2`/`ms3`, one digit per mini-sector: yellow, green, purple, pit lane, and so on) when OpenF1 data can be matched to the session.
- **Per-lap weather**: weather (`AirTemp`, `TrackTemp`, `Rainfall`, wind, ...) is merged into each lap row.
- **Precomputed channels**: telemetry includes `DriverAhead`, `DistanceToDriverAhead` and acceleration (`AccelerationX/Y/Z`).
- **pandas and polars**: both backends are supported. Data is cached in SQLite and in an in-memory LRU cache.
- **fastf1 column names**: laps and telemetry use fastf1's column names (`Sector1Time`, `SpeedST`, `nGear`, ...), so switching from fastf1 often needs only a change of import.

## Why not use tif1?

- **No live timing.** tif1 reads published files, so a session is available only after it ends. If you want live data during a session, use fastf1 (`fastf1.livetiming`, which signs in with your F1TV account).
- **Data arrives later.** New session data reaches tif1 after it reaches fastf1 (under a minute), because the files are generated with fastf1 and then processed and published.

## Installation

```bash
pip install tif1
```

Requires Python 3.11 or newer. Versions 3.11 through 3.15 are tested in CI.

### Optional Dependencies

```bash
# For development
pip install tif1[dev]

# Native telemetry decoder (Rust): parses telemetry payloads into typed numpy
# arrays with the GIL released. `pip install tif1` brings it in as the
# `tif1-native` wheel on CPython for Linux, macOS and Windows (x86_64 and
# arm64); elsewhere tif1 falls back to the pure-Python decoder, or build it:
cd native && uvx maturin develop --release
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
| `get_testing_session` | supported | f1schedule testing events |
| `get_testing_event` | supported | f1schedule testing events |
| `get_events_remaining` | supported | implemented |
| `get_session`/`get_event`/`get_event_schedule` `backend=` | supported | `"fastf1"`/`"f1timing"` read the f1schedule calendar, `"ergast"` the Ergast race table (1950 onward) |
| `fastf1.ergast` | supported | `tif1.ergast`: the thirteen methods, response classes, paging, legacy helpers; Stats repository first, Jolpica fallback |

### `core.Session`

| FastF1 API | tif1 status | Notes |
|---|---|---|
| `load` | supported | lazy-load compatible |
| `laps` | supported | main lap timing surface |
| `get_driver` | supported | driver validation present |
| `get_circuit_info` | partial | `corners` and `rotation` from `corners.json`; `marshal_lights` / `marshal_sectors` are empty |
| `results` | supported | the official Ergast classification: grid, points, status, total race time, Q1 to Q3, driver and team ids, plus tif1 extras (`TimeGap`, fastest-lap facts) |
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

- deeper parity for `Session` metadata (`session_info`, `session_start_time`)

## What's out of scope for tif1 from fastf1

- LiveTiming (`fastf1.livetiming`)
