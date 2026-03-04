# tif1 Project Architecture

## Overview

tif1 is a high-performance Python library for accessing Formula 1 timing data from the TracingInsights (2018-current). It provides a fastf1-compatible API with significant performance improvements through modern Python tooling and optimized data pipelines.

## Design Principles

1. **Performance First**: Optimize for speed at every layer (network, parsing, caching)
2. **API Compatibility**: Maintain fastf1-like interface for easy migration
3. **Reliability**: Robust error handling and automatic retry logic
4. **Flexibility**: Support multiple backends (pandas/polars) and async operations
5. **Developer Experience**: Simple API, clear errors, comprehensive logging

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        User Application                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Public API Layer                        │
│  • get_events()  • get_sessions()  • get_session()          │
│  • Session  • Driver  • Lap                                  │
└─────────────────────────────────────────────────────────────┘
                              │
                ┌─────────────┴─────────────┐
                ▼                           ▼
┌───────────────────────────┐   ┌──────────────────────────┐
│    Data Loading Layer     │   │   Validation Layer       │
│  • Async fetching         │   │  • Pydantic schemas      │
│  • Parallel requests      │   │  • Type checking         │
│  • HTTP/2 (niquests)      │   │  • Data integrity        │
└───────────────────────────┘   └──────────────────────────┘
                │                           │
                └─────────────┬─────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Processing Layer                        │
│  • DataFrame construction (pandas/polars)                    │
│  • Type optimization (categorical, nullable)                 │
│  • Data transformation                                       │
└─────────────────────────────────────────────────────────────┘
                              │
                ┌─────────────┴─────────────┐
                ▼                           ▼
┌───────────────────────────┐   ┌──────────────────────────┐
│      Cache Layer          │   │    CDN/Network Layer     │
│  • DuckDB storage         │   │  • jsDelivr CDN          │
│  • Compression            │   │  • Retry logic           │
│  • Fast lookups           │   │  • Connection pooling    │
└───────────────────────────┘   └──────────────────────────┘
```

## Core Components

### 1. API Layer (`src/tif1/api.py`)

**Purpose**: User-facing interface for data access

**Key Functions**:
- `get_events(year)`: List available events for a season
- `get_sessions(year, event)`: List sessions for an event
- `get_session(year, event, session)`: Load session data

**Design Decisions**:
- Simple, intuitive function names
- Automatic session type normalization
- Lazy loading for performance

### 2. Session Management (`src/tif1/session.py`)

**Purpose**: Core session data container and operations

**Key Classes**:
- `Session`: Main session object with laps, drivers, telemetry
- `Driver`: Driver-specific data and operations
- `Lap`: Individual lap with telemetry access

**Features**:
- Lazy property loading (laps, drivers loaded on demand)
- Async data fetching support
- Backend flexibility (pandas/polars)
- Fastest lap queries

### 3. Data Loading (`src/tif1/loader.py`)

**Purpose**: Fetch and parse data from CDN

**Key Components**:
- `DataLoader`: Synchronous data fetching
- `AsyncDataLoader`: Parallel async fetching (4-5x faster)
- HTTP/2 support via niquests (20-30% faster)

**Optimization Strategies**:
- Connection pooling
- Automatic retry with exponential backoff
- Parallel requests for multiple data files
- Streaming for large files

### 4. Cache System (`src/tif1/cache.py`)

**Purpose**: Local caching with SQLite

**Features**:
- Automatic cache key generation
- Fast lookups via SQL
- JSON storage format
- Built-in Python support

**Storage Format**:
```sql
CREATE TABLE cache (
    key TEXT PRIMARY KEY,
    data TEXT
)

CREATE TABLE telemetry_cache (
    year INTEGER,
    gp TEXT,
    session TEXT,
    driver TEXT,
    lap INTEGER,
    data TEXT,
    PRIMARY KEY (year, gp, session, driver, lap)
)
```

### 5. Validation Layer (`src/tif1/validation.py`)

**Purpose**: Data integrity and type safety

**Schemas**:
- `DriverInfo`: Driver metadata validation
- `LapData`: Lap timing data validation
- `TelemetryData`: Telemetry array validation

**Validation Rules**:
- Required field presence
- Type correctness
- Array length consistency
- Value range checks

### 6. Backend Abstraction (`src/tif1/backends/`)

**Purpose**: Support multiple DataFrame libraries

**Implementations**:
- `pandas_backend.py`: Default pandas implementation
- `polars_backend.py`: High-performance polars (2x faster)

**Interface**:
```python
class Backend:
    def create_dataframe(data: dict) -> DataFrame
    def optimize_types(df: DataFrame) -> DataFrame
    def filter(df: DataFrame, condition) -> DataFrame
```

### 7. Exception Hierarchy (`src/tif1/exceptions.py`)

**Purpose**: Clear, actionable error messages

**Exception Types**:
- `TIF1Error`: Base exception
- `DataNotFoundError`: Missing data (404)
- `NetworkError`: Connection issues
- `InvalidDataError`: Corrupted/invalid data
- `CacheError`: Cache operation failures

## Data Flow

### Cold Cache Flow (First Request)

```
User Request
    ↓
API Layer (get_session)
    ↓
Session.__init__ (lazy)
    ↓
User accesses .laps property
    ↓
Cache Miss
    ↓
DataLoader.fetch_laps()
    ↓
HTTP Request → jsDelivr CDN
    ↓
JSON Response
    ↓
Validation (Pydantic)
    ↓
DataFrame Construction (pandas/polars)
    ↓
Type Optimization
    ↓
Cache Store (SQLite)
    ↓
Return to User
```

### Warm Cache Flow (Subsequent Requests)

```
User Request
    ↓
API Layer (get_session)
    ↓
Session.__init__ (lazy)
    ↓
User accesses .laps property
    ↓
Cache Hit
    ↓
SQLite Query
    ↓
Deserialize DataFrame
    ↓
Return to User (10-100x faster)
```

### Async Flow (Parallel Loading)

```
User Request (async)
    ↓
Session.laps_async()
    ↓
Parallel Tasks:
    ├─ fetch_laps()
    ├─ fetch_drivers()
    └─ fetch_telemetry()
    ↓
await asyncio.gather()
    ↓
Concurrent HTTP/2 Requests
    ↓
Parallel Processing
    ↓
Return Combined Data
```

## Performance Optimizations

### 1. Network Layer
- **HTTP/2**: Multiplexing, header compression (niquests)
- **Connection Pooling**: Reuse TCP connections
- **CDN**: jsDelivr global edge network
- **Compression**: Gzip/Brotli support

### 2. Data Processing
- **Categorical Types**: 50% memory reduction for repeated strings
- **Nullable Types**: Proper null handling without object dtype
- **Lazy Loading**: Only load data when accessed
- **Polars Backend**: 2x faster for large datasets

### 3. Caching
- **SQLite**: Fast SQL-based lookups
- **JSON Storage**: Simple text-based storage
- **Indexing**: Primary key on cache keys
- **Built-in**: No external dependencies

### 4. Async Operations
- **Parallel Fetching**: 4-5x faster with asyncio
- **Non-blocking I/O**: Concurrent network requests
- **Task Batching**: Group related operations

## Configuration

### Environment Variables
```bash
TIF1_CACHE_DIR=~/.tif1/cache    # Cache location
TIF1_LOG_LEVEL=INFO              # Logging level
TIF1_TIMEOUT=30                  # Request timeout (seconds)
TIF1_MAX_RETRIES=3               # Retry attempts
```

### Runtime Configuration
```python
import tif1

# Logging
tif1.setup_logging(logging.DEBUG)

# Cache management
cache = tif1.get_cache()
cache.clear()

# Backend selection
session = tif1.get_session(..., backend="polars")

# Disable caching
session = tif1.get_session(..., enable_cache=False)
```

## Testing Strategy

### Unit Tests
- Individual component testing
- Mock external dependencies
- Edge case validation
- Error handling verification

### Integration Tests
- End-to-end data flow
- Real CDN requests (cached)
- Backend compatibility
- Cache operations

### Performance Tests
- Benchmark critical paths
- Memory profiling
- Load testing
- Regression detection

### Coverage Goals
- Line coverage: >90%
- Branch coverage: >85%
- Critical paths: 100%

## Deployment

### Package Structure
```
tif1/
├── src/tif1/
│   ├── __init__.py          # Public API exports
│   ├── api.py               # User-facing functions
│   ├── session.py           # Session/Driver/Lap classes
│   ├── loader.py            # Data fetching
│   ├── cache.py             # DuckDB caching
│   ├── validation.py        # Pydantic schemas
│   ├── exceptions.py        # Error types
│   ├── utils.py             # Utilities
│   └── backends/
│       ├── base.py          # Backend interface
│       ├── pandas_backend.py
│       └── polars_backend.py
├── tests/                   # Test suite
├── examples/                # Usage examples
└── pyproject.toml          # Project metadata
```

### Dependencies
- **Core**: niquests, pandas, pydantic
- **Optional**: polars (performance)
- **Dev**: pytest, ruff, mypy, prek

### Distribution
- PyPI package: `pip install tif1`
- Optional extras: `pip install tif1[polars]`
- Version scheme: Semantic versioning (MAJOR.MINOR.PATCH)

## Future Enhancements

### Short Term
- [ ] Weather data integration
- [ ] Track status information
- [ ] Radio messages
- [ ] Pit stop data

### Medium Term
- [ ] Real-time data support (live timing)
- [ ] Advanced analytics (tire degradation, pace analysis)
- [ ] Visualization helpers
- [ ] Data export formats (CSV, Parquet)

### Long Term
- [ ] Machine learning features
- [ ] Predictive analytics
- [ ] Multi-season analysis tools
- [ ] Custom data sources

## Security Considerations

1. **No Credentials**: Public CDN, no authentication required
2. **Input Validation**: All user inputs validated
3. **Safe Deserialization**: Pydantic for type safety
4. **Path Traversal**: Cache paths sanitized
5. **Resource Limits**: Timeout and size limits on requests

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
2. **Type Hints**: Full type coverage with mypy
3. **Tests**: Add tests for new features
4. **Documentation**: Update README and docstrings
5. **Performance**: Benchmark critical changes
6. **Compatibility**: Maintain fastf1 API compatibility

## License

MIT License - Open source, permissive use
