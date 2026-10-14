# tif1 Project Architecture

## Overview

tif1 is a high-performance Python library for accessing Formula 1 timing data from the TracingInsights data repositories (2018-current). It provides a fastf1-compatible API built on lazy, per-file fetching, async parallel requests and a multi-tier cache.

## Design Principles

1. **Performance First**: Optimize for speed at every layer (network, parsing, caching)
2. **API Compatibility**: Maintain fastf1-like interface for easy migration
3. **Reliability**: Robust error handling and automatic retry logic
4. **Flexibility**: Support multiple backends (pandas/polars) and async operations
5. **Developer Experience**: Simple API, clear errors, comprehensive logging

## System Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        User Application                      │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                      Public API Layer                        │
│  • get_events()  • get_sessions()  • get_session()           │
│  • Session  • Driver  • Lap  • Laps  • Telemetry  • charts   │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                 Payload Loading Layer                        │
│  • Session memo -> SQLite cache -> CDN fetch                 │
│  • Async parallel requests (niquests, HTTP/2)                │
│  • Optional validation (Pydantic)                            │
└──────────────────────────────────────────────────────────────┘
                              │
                ┌─────────────┴─────────────┐
                ▼                           ▼
┌───────────────────────────┐   ┌───────────────────────────┐
│      Cache Layer          │   │    CDN/Network Layer      │
│  • SQLite (one file)      │   │  • jsDelivr (primary)     │
│  • zstd / lz4 compression │   │  • Hugging Face (fallback)│
│  • In-memory LRU tiers    │   │  • StaticDelivr (backup)  │
│  • Per-session memo       │   │  • Retry, circuit breaker │
└───────────────────────────┘   └───────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                      Processing Layer                        │
│  • JSON parsing (orjson)                                     │
│  • DataFrame construction (pandas/polars)                    │
│  • Column renaming to fastf1 names, dtype conversion         │
└──────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Public API (`src/tif1/__init__.py`, `src/tif1/events.py`)

**Purpose**: User-facing interface for data access

**Key Functions**:
- `get_events(year)`: Event schedule for a season (an `EventSchedule` DataFrame)
- `get_sessions(year, event)`: List sessions for an event
- `get_session(year, event, session)`: Create a lazy `Session`
- `get_event`, `get_event_by_name`, `get_event_by_round`, `get_event_schedule`
- One-call chart functions (`plot_*`) from the `tif1.charts` package

**Design Decisions**:
- Public names are lazy exports (module `__getattr__`), so `import tif1` stays cheap
- Automatic session type normalization (`"FP1"`, `"Q"`, `"R"`, ...)
- Event names resolve by exact name, then the alias table `EVENT_NAME_ALIASES` (`core_utils/constants.py`), then fuzzy matching (`fuzzy.py`, minimum score 70, else `EventNotFoundError`)
- Schedules come from the f1schedule files (`schedule_{year}.json`): vendored copies for past seasons; for the current and future seasons a live download cached for `schedule_ttl_hours`, with the vendored copy as fallback (`events.py`, `schedule_schema.py`)

### 2. Session and Models (`src/tif1/core.py`, `src/tif1/models.py`)

**Purpose**: Core session data container and the data model classes

**Key Classes**:
- `Session` (`core.py`): Main session object with laps, drivers, telemetry, weather and race control messages
- `Laps`, `Lap`, `Telemetry`, `Driver`, `SessionResults`, `CircuitInfo` (`models.py`)
- `session.py` is a thin re-export shim pointing into `core.py`

**Features**:
- Lazy property loading (laps, drivers loaded on demand); `Session.load()` is optional
- Async variants (`laps_async()`, `get_fastest_laps_async()`, ...)
- Backend flexibility (pandas/polars)
- Fastest lap and fastest-lap telemetry queries

### 3. Payload Loading (`src/tif1/payload_loader.py`, `src/tif1/async_fetch.py`, `src/tif1/cdn.py`, `src/tif1/http_session.py`, `src/tif1/retry.py`)

**Purpose**: Fetch and parse JSON payloads from the CDN

**Key Components**:
- `PayloadLoader.get()`: single-payload pipeline: session memo -> SQLite cache -> CDN fetch -> validate -> memo/cache write-back
- `PayloadLoader.get_many()` / `async_fetch.fetch_multiple_async()`: parallel async fetching
- `CDNManager.try_sources()` / `try_sources_async()` (`cdn.py`): the one CDN fallback loop for the sync and async paths
- `http_session.py`: shared niquests session (HTTP/2, connection pooling)
- `retry.py`: retry with exponential backoff and a circuit breaker
- `HttpTransport` protocol: `NiquestsTransport` in production, `InMemoryTransport` in tests

**Optimization Strategies**:
- Connection pooling and HTTP/2 multiplexing
- Automatic retry with exponential backoff
- Parallel requests for multiple data files
- One session-wide `session_laptimes.json` request for all laps, with per-driver `laptimes.json` as fallback

### 4. Cache System (`src/tif1/cache.py`)

**Purpose**: Local caching with SQLite and in-memory tiers

**Features**:
- `Cache`: persistent SQLite tier with a kind-based interface (`get_entry`, `set_entry`, `invalidate`)
- JSON payloads stored as text, or zstd-compressed (zlib fallback) above a size threshold
- Materialized DataFrames ("frame tiers") pickled and lz4-compressed (zstd fallback), versioned through SQLite `PRAGMA user_version`
- `LRUCache`: the in-memory LRU used by the memory tiers and the process-global backend lap caches
- `SessionMemo`: per-session memo of payloads and frames
- Payloads missing on every CDN are remembered for `missing_payloads_ttl_days`

**Storage Format** (main tables):
```sql
CREATE TABLE cache (key TEXT PRIMARY KEY, data TEXT)
CREATE TABLE missing_payloads (key TEXT PRIMARY KEY, recorded_at REAL)

CREATE TABLE telemetry_cache (
    year INTEGER,
    gp TEXT,
    session TEXT,
    driver TEXT,
    lap INTEGER,
    data TEXT,
    PRIMARY KEY (year, gp, session, driver, lap)
)

-- Frame tiers; *_pl tables hold the polars frames
CREATE TABLE telemetry_frames (year, gp, session, driver, lap, frame BLOB, ...)
CREATE TABLE telemetry_frames_pl (year, gp, session, driver, lap, frame BLOB, ...)
CREATE TABLE laps_frames (year, gp, session, frame BLOB, ...)
CREATE TABLE laps_frames_pl (year, gp, session, frame BLOB, ...)
```

**Default Cache Location**:
The cache directory is OS-dependent and mirrors FastF1's platform layout, using `tif1` as the application directory:

- Windows: `%LOCALAPPDATA%\Temp\tif1`
- macOS: `~/Library/Caches/tif1`
- Linux and other POSIX platforms (FreeBSD, etc.): `~/.cache/tif1` when `~/.cache` exists, otherwise `~/.tif1`

Override the default with the `TIF1_CACHE_DIR` environment variable or the `cache_dir` key in `.tif1rc`.

### 5. Validation Layer (`src/tif1/validation.py`)

**Purpose**: Optional data integrity checks (off by default: `validate_data`, `validate_lap_times`, `validate_telemetry`)

**Schemas** (Pydantic):
- `DriverInfo` / `DriversData`: Driver metadata validation
- `LapData`: Lap timing data validation
- `TelemetryData`: Telemetry array validation
- `RaceControlData`, `WeatherData`: Session table validation

**Validation Rules**:
- Required field presence
- Type correctness
- Array length consistency
- Value range checks

### 6. Backends (`src/tif1/core_utils/backend_conversion.py`)

**Purpose**: Support pandas and polars DataFrames

- pandas is the default backend (`lib="pandas"`)
- polars is a declared dependency but lazy-loaded: `_ensure_polars_available()` in `core.py` falls back to pandas when it is missing
- `pandas_to_polars`, `polars_to_pandas` and `convert_backend` convert between the two (through Arrow; pyarrow is a runtime dependency)

### 7. Exception Hierarchy (`src/tif1/exceptions.py`)

**Purpose**: Clear, actionable error messages with structured context (`**context` keyword arguments; `SessionNotLoadedError` takes only `attribute`)

**Exception Types**:
- `TIF1Error`: Base exception
- `DataNotFoundError`: Missing data (404 on every CDN); subclasses `DriverNotFoundError`, `LapNotFoundError`
- `NetworkError`: Connection issues; subclass `CircuitBreakerOpenError`
- `InvalidDataError`: Corrupted/invalid data
- `CacheError`: Cache operation failures
- `SessionNotLoadedError`: Data accessed before it is loaded
- `EventNotFoundError`: An event name matches no event of the season (fuzzy score below 70; a `ValueError`, not a `TIF1Error`)
- `EventNotInSeasonError`: Subclass of `EventNotFoundError`; an event alias names a Grand Prix that is not on that season's calendar

### 8. Other Modules

- `config.py`: singleton `Config` (defaults, `~/.tif1rc`, `TIF1_*` environment variables)
- `charts/`, `plotting.py`, `plotting_constants.py`, `assets.py`: charts, plotting helpers, team colors and bundled assets
- `cli.py`: the `tif1` command line (typer + rich)
- `jupyter.py`: rich HTML display in notebooks
- `fastf1_compat.py`: fastf1 compatibility shims (`Cache.enable_cache`, `set_log_level`)
- `utils.py`: fastf1-style helpers (`delta_time`, `to_timedelta`, `to_datetime`, `recursive_dict_get`)
- `types.py`: TypedDicts and Literal types
- `core_utils/`: constants (column rename maps), DataFrame helpers, JSON parsing, resource management

## Data Flow

### Cold Cache Flow (First Request)

```
User Request
    ↓
get_session() -> Session (lazy, nothing fetched yet)
    ↓
User accesses .laps property
    ↓
Process lap cache miss -> SQLite laps frame tier miss
    ↓
Async fetch: drivers.json, session_laptimes.json
    ↓
CDN fallback: jsDelivr -> Hugging Face -> StaticDelivr (retry + circuit breaker)
    ↓
JSON parse (orjson) -> optional validation (Pydantic)
    ↓
DataFrame construction (pandas/polars), fastf1 column names, dtypes
    ↓
Cache write (SQLite payload tier + laps frame tier) and in-memory lap cache
    ↓
Return to User
```

### Warm Cache Flow (Subsequent Requests)

```
User Request
    ↓
get_session() -> Session (lazy)
    ↓
User accesses .laps property
    ↓
In-memory lap cache hit (same process)
  or SQLite laps frame tier hit (new process: unpickle the stored DataFrame)
    ↓
Return to User
```

### Async Flow (Parallel Loading)

```
User Request (async)
    ↓
await session.laps_async() / session.get_fastest_laps_tels_async()
    ↓
async_fetch.fetch_multiple_async(): many payloads in parallel
    ↓
CDNManager.try_sources_async() per payload (concurrent HTTP/2 requests)
    ↓
Parse and build DataFrames
    ↓
Return Combined Data
```

## Performance Optimizations

### 1. Network Layer
- **HTTP/2**: Multiplexing over a shared connection (niquests; HTTP/3 off by default)
- **Connection Pooling**: Reuse TCP connections
- **CDN**: jsDelivr global edge network, with Hugging Face and StaticDelivr fallbacks
- **Preconnect**: The CDN connection is warmed in a background thread while `get_session()` returns (`cdn_preconnect`)

### 2. Data Processing
- **Categorical Types**: `Driver`, `Team`, `Compound` and `TrackStatus` are categorical
- **Nullable Types**: Proper null handling without object dtype
- **Lazy Loading**: Only load data when accessed
- **orjson**: Fast JSON parsing

### 3. Caching
- **SQLite**: One cache file with primary-key lookups
- **Compression**: zstd for payloads, lz4 for frames
- **Frame Tiers**: Warm loads unpickle built DataFrames instead of re-parsing JSON
- **Built-in**: SQLite ships with Python

### 4. Async Operations
- **Parallel Fetching**: Many payloads fetched concurrently with asyncio (`max_concurrent_requests`)
- **Non-blocking I/O**: Concurrent network requests
- **Task Batching**: Group related operations

## Configuration

### Environment Variables
```bash
TIF1_CACHE_DIR=/path/to/tif1-cache    # Override OS-specific cache location
TIF1_TIMEOUT=30                  # Request timeout (seconds)
TIF1_MAX_RETRIES=3               # Attempts per payload (0 or 1 = one attempt)
TIF1_LIB=pandas                  # Default backend (pandas or polars)
```

### Runtime Configuration
```python
import logging

import tif1

# Logging
tif1.setup_logging(logging.DEBUG)

# Cache management
cache = tif1.get_cache()
cache.clear()

# Backend selection
session = tif1.get_session(2024, "Monaco Grand Prix", "Race", lib="polars")

# Disable caching
session = tif1.get_session(2024, "Monaco Grand Prix", "Race", enable_cache=False)
```

## Testing Strategy

### Unit Tests (`tests/unit/`)
- Individual component testing
- Mock external dependencies (`InMemoryTransport`); real sockets are blocked by an autouse fixture
- Edge case validation
- Error handling verification

### Integration Tests (`tests/integration/`)
- End-to-end data flow
- Real CDN requests (`integration` marker)
- Backend compatibility
- Cache operations

### Property-Based Tests (`tests/property/`)
- Hypothesis-generated inputs

### Performance Tests (`tests/benchmarks/`)
- Benchmark critical paths with pytest-benchmark (`benchmark` marker, excluded from the default run)
- Regression detection

### Coverage
- pytest runs with coverage by default; the fail threshold is 80% (branch coverage is measured)

## Deployment

### Package Structure
```
tif1/
├── src/tif1/
│   ├── __init__.py          # Public API (lazy exports)
│   ├── core.py              # Session, data loading and parsing
│   ├── models.py            # Laps, Lap, Telemetry, Driver, SessionResults, CircuitInfo
│   ├── session.py           # Re-export shim into core.py
│   ├── events.py            # Event schedule, get_events/get_sessions
│   ├── schedule_schema.py   # Schedule payload validation
│   ├── fuzzy.py             # Fuzzy event-name matching
│   ├── payload_loader.py    # Single payload-loading pipeline
│   ├── async_fetch.py       # Async parallel fetching
│   ├── cdn.py               # CDN sources and fallback
│   ├── http_session.py      # Shared niquests session
│   ├── retry.py             # Retry and circuit breaker
│   ├── cache.py             # SQLite, LRU and session-memo caches
│   ├── config.py            # Configuration
│   ├── validation.py        # Pydantic schemas
│   ├── exceptions.py        # Error types
│   ├── types.py             # TypedDicts and Literals
│   ├── utils.py             # fastf1-style utilities
│   ├── fastf1_compat.py     # fastf1 compatibility shims
│   ├── cli.py               # Command line interface
│   ├── jupyter.py           # Notebook display
│   ├── plotting.py          # Plotting helpers
│   ├── plotting_constants.py
│   ├── assets.py            # Bundled assets
│   ├── charts/              # One-call chart functions
│   ├── core_utils/          # Constants, helpers, JSON, backend conversion
│   ├── assets/              # Fonts, car and tyre images
│   └── data/schedules/      # Vendored f1schedule files
├── tests/                   # unit/, integration/, property/, benchmarks/
├── examples/                # Usage examples
└── pyproject.toml           # Project metadata
```

### Dependencies
- **Core**: pandas, polars (lazy-loaded), niquests, pydantic, orjson, rapidfuzz, typer, rich, matplotlib, seaborn, scipy, zstandard / lz4 (by Python version)
- **Dev**: pytest (+ xdist, cov, asyncio, benchmark, mock), hypothesis, ruff, ty, prek

### Distribution
- PyPI package: `pip install tif1`
- Version scheme: Semantic versioning (MAJOR.MINOR.PATCH)

## Future Enhancements

### Short Term
- [ ] Radio messages

### Medium Term
- [ ] Real-time data support (live timing)

### Long Term
- [ ] Machine learning features
- [ ] Predictive analytics
- [ ] Multi-season analysis tools

## Security Considerations

1. **No Credentials**: Public CDN, no authentication required
2. **Input Validation**: Year, event, session and driver arguments are validated
3. **Deserialization**: JSON payloads are parsed with orjson; the frame tiers unpickle DataFrames that tif1 wrote itself, and the cache directory is created with owner-only permissions (0700) where the OS supports it
4. **Config Trust**: `.tif1rc` is read from `$HOME` (or `TIF1_CONFIG_FILE`); a `.tif1rc` in the working directory is read only when `TIF1_TRUST_CWD_CONFIG` is true
5. **Resource Limits**: Request timeout (`timeout`, default 30 s) and bounded concurrency

## Monitoring & Observability

### Logging Levels
- **DEBUG**: Detailed operation logs, cache hits/misses
- **INFO**: High-level operations, data loading
- **WARNING**: Retry attempts, fallback operations
- **ERROR**: Failed operations, invalid data

### Metrics to Track
- Cache hit rate
- Request latency (p50, p95, p99)
- Data loading time
- Memory usage
- Error rates by type

## Contributing Guidelines

1. **Code Style**: Follow ruff configuration
2. **Type Hints**: Full type coverage, checked with ty (`uv run ty check src/tif1`)
3. **Tests**: Add tests for new features
4. **Documentation**: Update README and docstrings
5. **Performance**: Benchmark critical changes
6. **Compatibility**: Maintain fastf1 API compatibility

## License

MIT License - Open source, permissive use
