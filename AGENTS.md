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
- `core.py` is the monolith (~4200 lines): Session class, data loading, lap/telemetry/weather/race-control parsing; re-exports the model family for backward compatibility (`tif1.core.Laps` etc. are aliases of the `tif1.models` classes). Its fetch seams `Session._fetch_from_cdn(_fast)`/`_fetch_json(_unvalidated)` are thin delegates into `payload_loader.py`, kept as one override seam for older tests.
- `models.py` is the canonical model module: `Laps`, `Lap`, `Telemetry`, `Driver`, `SessionResults`, `DriverResult`, `CircuitInfo`, `LazyTelemetryDict`, plus the `TelemetryProvider` protocol — the narrow (mostly private) session seam the models consume. `Session` satisfies it structurally; tests can use in-memory fakes. `models.py` must never import `core.py` at runtime (import cycle).
- `session.py` is a thin re-export shim pointing into `core.py`.
- `io_pipeline.py` and `lap_ops.py` were removed (pass-through shims); internal helpers live in `core.py`/`core_utils/`.
- `payload_loader.py` is the single payload-loading pipeline: `PayloadLoader.get` (session memo -> SQLite cache -> fetch -> validate -> memo/cache write-back), `fetch_from_cdn(path, fast=)` (retry/backoff unless `fast`), async `get_many` (delegates to `fetch_multiple_async`), and `get_url` + thread-safe `get_url_loader()` for absolute-URL payloads (used by `events.py`). HTTP goes through injectable `HttpTransport` implementations (`NiquestsTransport` in production, `InMemoryTransport` in tests). Validation failures always raise — no patched-callable escape hatch (the old `__code__`-sniffing test-compat machinery was removed).
- HTTP via `http_session.py` (niquests session) + `async_fetch.py` (async parallel fetching with niquests); sync and async payload entry points share the CDN fallback loops owned by `cdn.py` (`CDNManager.try_sources` / `try_sources_async`).
- Cache in `cache.py` (all tiers: `LRUCache` (the single LRU impl), SQLite `Cache` with kind-based `get_entry`/`set_entry`/`invalidate`, process-global backend lap caches `get_backend_lap_cache`/`clear_lap_cache`, and the per-session `SessionMemo` tier held as `Session._memo`); CDN fallback in `cdn.py` (StaticDelivr primary, jsDelivr fallback, never raw.githubusercontent.com).
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
- The CDN system fetches from TracingInsights GitHub data repos (per-year repos like `{year}`), served via StaticDelivr CDN (primary) with jsDelivr as fallback.
- Exception hierarchy: TIF1Error → DataNotFoundError → {DriverNotFoundError, LapNotFoundError}; TIF1Error → {NetworkError, InvalidDataError, CacheError, SessionNotLoadedError}.
- All exceptions accept `**context` kwargs for structured error info.

Constraints:
- Never use `https://raw.githubusercontent.com` CDN (rate limits). Use StaticDelivr (primary) or jsDelivr (fallback) CDNs only.
- Python >=3.11; use type hints everywhere; Google-style docstrings for public APIs.
- Ruff ruleset (see pyproject.toml for full select/ignore); line length 100, double quotes, space indent.
- Keep imports sorted (ruff/format), avoid unused imports, prefer explicit names.
- Handle errors via custom exceptions in `exceptions.py`; never swallow network/data errors.
- orjson for JSON parsing (not stdlib json).
- pandas >=2.3 as primary backend; polars >=1.36 optional.
- Always optimize for performance. Entire existence of this library is to focus on optimization, speed and performance. Performance is critical

Documentation (Mintlify):
- For all documentation work, use the mintlify skill: `.kiro/skills/mintlify/SKILL.md`
- All docs in `docs/` directory; configuration in `docs/docs.json`
