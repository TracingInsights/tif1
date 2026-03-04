Build/lint/test:
- Install deps: `uv sync --all-extras`
- Tests: `uv run pytest tests/ -v`
- Single test file: `uv run pytest tests/unit/test_core.py -v`
- Single test by name: `uv run pytest tests/ -v -k "test_name"`
- Unit only: `uv run pytest tests/unit/ -v`
- Integration only: `uv run pytest tests/integration/ -v`
- Property-based: `uv run pytest tests/property/ -v`
- Benchmarks: `uv run pytest tests/benchmarks/ -v -m benchmark`
- Lint: `uv run ruff check src/ tests/`
- Format: `uv run ruff format src/ tests/`
- Types: `uv run ty check src/tif1`
- Coverage: pytest runs with `--cov` by default; fail threshold is 80%.

Architecture:
- Library source in `src/tif1/`; public API via lazy `__getattr__` exports in `__init__.py`.
- `core.py` is the monolith (~4900 lines): Session class, data loading, lap/telemetry/weather/race-control parsing, driver model classes (Laps, Lap, Driver, Telemetry, SessionResults, CircuitInfo, DriverResult).
- `session.py` and `models.py` are thin re-export shims pointing into `core.py`.
- `io_pipeline.py` re-exports internal helpers from `core.py` (_create_lap_df, _create_session_df, etc.).
- HTTP via `http_session.py` (niquests session) + `async_fetch.py` (async parallel fetching with niquests).
- Cache in `cache.py` (SQLite-backed); CDN fallback in `cdn.py` (jsdelivr only, never raw.githubusercontent.com).
- Config in `config.py` (singleton Config class, env vars + `.tif1rc` file support).
- Retry/circuit-breaker in `retry.py`; event schedule in `events.py` + `schedule_schema.py`.
- Validation (pydantic) in `validation.py`; errors in `exceptions.py` (hierarchy rooted at `TIF1Error`).
- Types in `types.py`: TypedDicts (LapDataDict, TelemetryDataDict, etc.), Literals (SessionType, BackendType, CompoundType).
- Shared helpers in `core_utils/`: `constants.py` (column names, rename maps), `helpers.py` (DataFrame utils, validation), `json_utils.py` (JSON parsing), `backend_conversion.py` (pandas↔polars), `resource_manager.py`.
- Optional polars backend: lazy-loaded, gate behind `_ensure_polars_available()` in `core.py`.
- CLI (typer + rich) in `cli.py`; Jupyter support in `jupyter.py`; plotting in `plotting.py`.
- `fastf1_compat.py` provides fastf1 compatibility shims (e.g. `set_log_level`).
- Tests in `tests/` split into `unit/`, `integration/`, `property/`, `benchmarks/`.

Key patterns:
- Session.load() accepts `laps`, `telemetry`, `messages`, `weather` booleans to control what data gets fetched.
- Data flows: CDN URL → async HTTP fetch → JSON parse → DataFrame construction → column rename/reorder → cache.
- The CDN system fetches from TracingInsights GitHub data repos (per-year repos like `{year}`), served via jsdelivr CDN.
- Exception hierarchy: TIF1Error → DataNotFoundError → {DriverNotFoundError, LapNotFoundError}; TIF1Error → {NetworkError, InvalidDataError, CacheError, SessionNotLoadedError}.
- All exceptions accept `**context` kwargs for structured error info.

Constraints:
- Never use `https://raw.githubusercontent.com` CDN (rate limits). Use jsdelivr CDN only.
- Python >=3.10; use type hints everywhere; Google-style docstrings for public APIs.
- Ruff ruleset (see pyproject.toml for full select/ignore); line length 100, double quotes, space indent.
- Keep imports sorted (ruff/format), avoid unused imports, prefer explicit names.
- Handle errors via custom exceptions in `exceptions.py`; never swallow network/data errors.
- orjson for JSON parsing (not stdlib json).
- pandas >=2.3 as primary backend; polars >=1.36 optional.
- Always optimize for performance. Entire existence of this library is to focus on optimization, speed and performance. Performance is critical
