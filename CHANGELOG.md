# Changelog

All notable changes to this project are documented in this file.

The project uses semantic versioning. Release dates are listed in `YYYY-MM-DD` format.


## [Unreleased]

## [0.10.0] - 2026-10-28

### Summary

`0.10.0` adds the Ergast data tier: `tif1.ergast` is a drop-in for
`fastf1.ergast`, `Session.results` carries the official classification (grid,
points, status, race time, qualifying segments), and sessions, schedules and
standings reach back to 1950. The answers come from the TracingInsights Stats
repository (one static file per table and round) with the Jolpica API only as
fallback, so season-wide queries are one parallel fetch instead of a paged,
rate-limited API walk. The library now also runs in the browser under Pyodide
(`Session.load_async()`, IndexedDB persistence, a checked wheel), and the
Thebes and Pergamon performance series add CDN hedging, typed and per-driver
columnar telemetry frame tiers, an optional Rust decoder, GIL-released
decompression and an asyncio-free warm path (per-series numbers below). A
verification sweep fixed cleanup, cache and dtype bugs found by running the
documentation against real data.

Upgrade notes:

- `Session.results`, `drivers` and `drivers_df` follow the official Ergast
  classification for Race, Sprint and Qualifying sessions (post-race penalties
  included, drivers that did not start added with NaN `Position`); one small
  classification file is fetched alongside `drivers.json`. Pass
  `get_session(..., ergast=False)` or set `TIF1_ERGAST_RESULTS=0` for the
  previous lap-derived view.
- `DriverResult.dnf` counts every `+N Laps` and `Lapped` status as a
  classified finish.
- `MIN_YEAR` is 1950. Seasons before 2018 load `Qualifying`, `Sprint` and
  `Race` from Ergast; their schedules list all five sessions, and loading a
  practice session or any timing payload raises `DataNotFoundError`.
- The `results` index is always unique: the later drivers of a car two drivers
  shared (classifications before the 1960s) are labelled
  `number/Abbreviation`; `session.drivers` lists the same labels.
- New configuration keys (`ergast_results`, `ergast_fallback`,
  `jolpica_base_url`, `ergast_ttl_hours`, `ergast_timeout`,
  `ergast_rate_limit_*`, `stats_repo`, `stats_branch`) and the
  `TIF1_ERGAST_*`, `TIF1_JOLPICA_BASE_URL`, `TIF1_STATS_*` variables.

### Added — Ergast data: results, standings, lap times from 1950 (2026-10-28)

- **`tif1.ergast`**, a drop-in for `fastf1.ergast`: `Ergast` with the
  thirteen endpoint methods, fastf1's keyword arguments, flattened column
  names, `ErgastSimpleResponse`/`ErgastMultiResponse`/`ErgastRawResponse`
  with paging, the `fetch_season`/`fetch_day`/`fetch_results` helpers and the
  `ErgastError`/`ErgastJsonError`/`ErgastInvalidRequestError`/
  `RateLimitExceededError` exceptions. Answers come from the
  `TracingInsights-Archive/Stats` repository (one static file per table and
  round, no rate limit, season-wide queries assembled in one parallel fetch)
  and from the Jolpica API only for what that repository cannot answer
  (paced to 4 req/s and 500 req/h, `Retry-After` honoured). A Stats-backed
  answer is complete by default; a Jolpica-backed one keeps the API default of
  30 rows. `result_type="polars"` (`ErgastPolarsResponse`, a polars frame
  with the same paging attributes) and a `source` attribute are additions.
  `totalRaceTime` is the true total race time (fastf1 stores the gap for every
  driver but the winner); the gap is the new `totalRaceTimeGap`.
- **Official classifications in `Session.results`**: `GridPosition`,
  `Points`, `Status`, `ClassifiedPosition`, `Time` (the total race time),
  `Q1`/`Q2`/`Q3`, `DriverId`, `TeamId`, `BroadcastName`, plus the extras
  `TimeGap`, `FastestLapRank`/`Number`/`Time`/`AvgSpeed`, `PermanentNumber`,
  nationalities, `DateOfBirth` and URLs. `Session.drivers`, `drivers_df` and
  `results` follow the official order (penalties included); drivers that did
  not start are added. The frame is indexed by `DriverNumber`, kept unique
  for a car two drivers shared (pre-1960s) by labelling the later drivers
  `number/Abbreviation`; fastf1's 22 columns come first. `get_session(..., ergast=False)` or
  `TIF1_ERGAST_RESULTS=0` restores the lap-derived view.
- **`Session.pit_stops`**, **`Session.driver_standings`**,
  **`Session.constructor_standings`**, **`Session.total_laps`**, and the
  top-level **`get_driver_standings`** / **`get_constructor_standings`**.
- **Seasons from 1950**: `get_event_schedule(1995)` (packaged Ergast race
  tables, `tools/vendor_ergast_schedules.py`, all five sessions listed as
  fastf1 does), `get_session(1995, "Monaco", "Race")` with results, lap
  timings (1996+) and pit stops (2011+); a practice session of those seasons
  raises `DataNotFoundError` (Ergast has no practice data); every
  schedule carries `CircuitId`, `CircuitName`, `CircuitLatitude`,
  `CircuitLongitude`, `EventUrl`, `CircuitUrl`.
- **fastf1 parity**: `backend=`/`force_ergast=` on `get_session`, `get_event`,
  `get_event_schedule`; `strict_search=` on `get_event`;
  `get_events_remaining`, `get_testing_event`, `get_testing_session`;
  `Event.is_testing()`, `EventSchedule.is_testing()`;
  `Laps.QUICKLAP_THRESHOLD`, `Telemetry.TELEMETRY_FREQUENCY`,
  `Telemetry.register_new_channel`, `Cache.api_request_wrapper`.
- **CLI**: `tif1 results YEAR EVENT SESSION`, `tif1 standings YEAR [ROUND]
  [--constructors]`, `tif1 cache-clear --ergast`.
- **Configuration**: `ergast_results`, `ergast_fallback`, `jolpica_base_url`,
  `ergast_ttl_hours`, `ergast_timeout`, `ergast_rate_limit_burst`,
  `ergast_rate_limit_hourly`, `stats_repo`, `stats_branch` (env
  `TIF1_ERGAST_*`, `TIF1_JOLPICA_BASE_URL`, `TIF1_STATS_*`).
  `Cache.invalidate("ergast")` drops only the Ergast tier.
- **CDN**: `CDNSource.format_repo_url`/`serves` and
  `CDNManager.try_sources_repo` for repositories outside the
  per-year layout; a "missing" verdict needs a 404 from every mirror, a
  transient jsDelivr 403 is retried once and never persisted. Requests carry a
  `tif1/{version}` User-Agent.

### Changed

- `DriverResult.dnf` treats every `+N Laps` and `Lapped` status as a
  classified finish (`+3 Laps` counted as a DNF before).
- `MIN_YEAR` is 1950. Schedules before 2018 list all five sessions (fastf1
  parity); `get_session` loads `Qualifying`, `Sprint` and `Race` there and
  raises `DataNotFoundError` for practice, as every timing payload of those
  seasons does.
- `Session.results` is indexed by `DriverNumber` and always has every column
  of the fastf1 contract.
- Dependencies (Dependabot alerts, `uv.lock`): Pillow 13.0.0, certifi
  2026.10.23, urllib3 2.8.0, filelock 4.2.4; the rejected `telparse` probe
  crate under `.agents/perf-lindos` builds against pyo3 0.29.

### Added — Pyodide (2026-10-16)

`tif1` runs in the browser: Pyodide 314 (JupyterLite, PyScript, marimo).
`await micropip.install("tif1")`, `get_session(...)` and
`await session.load_async()` work in every browser; the synchronous API works
where JavaScript Promise Integration is available (Chrome 137+).

- **Install.** `pyproject.toml` carries `sys_platform == 'emscripten'` twins
  with major-only floors for the packages the Pyodide distribution ships
  (pandas, pyarrow, orjson, pydantic, matplotlib, scipy, lz4) and excludes
  niquests, polars, rapidfuzz and nest-asyncio2, which have no WebAssembly
  build. `tests/unit/test_pyproject_markers.py` checks the twins against
  `tools/pyodide_target.json`, so a floor bump that breaks the browser
  install fails without a network.
- **Transport.** `tif1.browser.PyodideTransport` fetches through the browser:
  Pyodide's `urllib3` on the synchronous `HttpTransport` seam (`fetch` under
  JSPI, `XMLHttpRequest` otherwise) and `pyodide.http.pyfetch` for the async
  fan-out, which runs without a thread pool there (`AsyncHttpTransport`).
- **`Session.load_async()`**, the async twin of `load()`: one concurrent
  wave per phase, never blocking the event loop.
- **One sync bridge.** `core._run_coroutine_sync` replaces the five
  `asyncio.run` sites. Under Pyodide it blocks through
  `pyodide.ffi.run_sync` (JSPI) and otherwise raises the new
  `AsyncRequiredError`, whose message names the coroutine to await.
- **No threads.** The import-time preload, CDN pre-connect, session-table
  prefetch, frame writers, schedule refresh, cache codec pools and the
  telemetry prefetch are gated on `THREADS_AVAILABLE`; writers run inline.
- **Cache.** SQLite uses an in-memory journal on Emscripten (no shared-memory
  WAL there); `tif1.browser.enable_persistent_cache()` / `persist_cache()`
  keep the cache directory in IndexedDB across reloads; `Cache.flush()`.
- **Fallbacks.** Event-name fuzzy matching uses a pure Python Indel ratio
  with the same scores as rapidfuzz when rapidfuzz is missing; polars 1.x
  (what Pyodide ships) is treated as not installed (`import_supported_polars`).
- **CI.** `tools/pyodide_smoke.mjs` installs the built wheel in Node's
  Pyodide and loads a session offline and against the CDNs (`pyodide-smoke`
  job, `node --experimental-wasm-jspi` for the synchronous API);
  `tools/pyodide_browser_check.mjs` repeats the checks in Chromium and
  Firefox through Playwright: main thread and Web Worker, the
  `XMLHttpRequest` path, and IndexedDB persistence across a page reload.
  Both run `tools/pyodide_checks.py`.
- `Cache.requests_get()` / `requests_post()` (fastf1 compat) raise a clear
  `TIF1Error` inside Pyodide instead of an import error.
- Docs: `concepts/pyodide`, `api-reference/browser`.

### Fixed — verification sweep (2026-10-16)

- `async_fetch.cleanup_resources()` closed the shared niquests session
  without resetting the `http_session` singleton, so a fetch after an
  explicit cleanup reused a closed session and failed to parse its response.
  The shared session is now closed through its owner and rebuilt on demand.
- `tools/run_doc_examples.py`: skips `.py`-titled module listings and
  Pyodide-titled blocks, and runs the `Pandas` / `Polars` tab examples in
  their own namespaces (the tabs are alternatives, not a sequence).
- Doc examples: the `ResourceManager` listing imports its type names; the
  HTTP examples fetch from the Hugging Face mirror (jsDelivr refuses the
  2024 and 2025 data repositories, which exceed its 50 MB package limit,
  and the CDN manager falls through to the mirrors).
- Benchmarks: the telemetry parity check accepts the typed arrays of the
  native decoder; the concurrent-read benchmark measures lock contention
  (concurrent versus sequential time) instead of a parallel speedup that
  GIL-bound reads cannot deliver.
- `native/README.md`: the maturin commands take `-m native/Cargo.toml`.

### Performance — Pergamon wave 2 (2026-10-15)

Twenty more warm-path hypotheses, each run in its own worktree against the
wave-1 tree (`.agents/perf-pergamon/q/RESULTS.md`, one report per
hypothesis); fifteen shipped, five rejected by measurement. Analysis-notebook
benchmark on a warm 2026 Monaco race (fresh process, 8 interleaved rounds):
whole process 988 → 705 ms against wave 1 and 1350 → 705 ms against the
Thebes base, faster 8/8. Warm telemetry phase 98.7 → 74.6 ms (pandas, 10/10)
and 90.6 → 58.8 ms (polars, 8/8) against wave 1.

- **Lazy lap frames in `load()` (Q1).** `load()` decodes every driver row but
  builds a lap's frame when it is first touched (58 → 39 ms);
  `fetch_all_laps_telemetry()` still returns built frames.
- **Interactive preload (Q2).** In IPython, Jupyter or a REPL, `import tif1`
  starts a daemon thread that imports numpy, pandas and pyarrow (never a
  `tif1` submodule), so `get_session` in the next cell takes 0.02-0.03 s
  instead of 0.22-0.30 s. Scripts are unchanged. `TIF1_PRELOAD_IMPORTS=0/1`
  overrides the detection.
- **No subclass re-wrapping (Q4, Q8).** `Laps`, `Lap` and `Telemetry` results
  wrap the operation's fresh manager instead of calling `__init__` (a shallow
  copy each time): every `pick_*` 15-28 % faster; `Telemetry` methods 24-89 %
  faster (`fill_missing`, `resample_channels`, `merge_channels`, distance and
  slicing helpers).
- **Row access (Q7).** `iterlaps()` and `laps.iloc[i]` 2-3x faster; an
  `iterlaps()` loop calling `lap.telemetry` 4.7x faster.
- **Frame tiers (Q6, Q11, Q13, Q18).** Streamed SQLite cursors into a shared
  decompression pool; one lz4 task per large driver row; single-threaded IPC
  reads and `string_view` storage for polars rows (old rows still read);
  a per-group plan for lap frames; the writer encodes into one buffer and
  compresses in a thread pool (1452-frame write 157 → 83 ms).
- **Payload-tier warm load (Q15).** When the frame tiers are empty, zstd and
  the native decoder run in worker threads and the load skips asyncio:
  1.07 → 0.65 s with `tif1-native`.
- **Fastest-lap and `Driver` paths (Q10, Q19).** Cache hits no longer go
  through `asyncio.run`; the fastest lap comes from the laps frame tier;
  `Driver.laps` is built directly from the payload (41 → 7.5 ms).
- **Charts and CLI (Q14, Q20).** seaborn/scipy and pyarrow are imported only
  where used, and `tif1.cache`/`tif1.events` no longer import pandas: first
  chart to PNG 1.84 → 1.05 s; `cache-info`, `events`, `sessions` and
  `cache-clear` 3.5x faster. `Event`/`EventSchedule` live in
  `tif1.event_frames` (still importable from `tif1.events`).
- **Memory (Q9).** The process-wide laps cache stores a session-free copy, so
  deleted sessions and their telemetry can be collected (11 sessions: full GC
  0.10 → 0.017 s, RSS 1.4 → 0.4 GB after collection).
- **Fixed:** inside Jupyter, `load()` / `fetch_all_laps_telemetry()` raised
  `asyncio.run() cannot be called from a running event loop` when some
  telemetry was not in the frame tier (Q16). The synchronous payload path now
  reads and records missing-everywhere verdicts, so a missing lap no longer
  costs a CDN walk (up to a 30 s timeout) in every process (Q16, Q17). Frame
  rows read from worker threads no longer share one zstd decompressor (Q15).
- Rejected by measurement: tif1's own import trim (Q3, 2.7 ms), tiers for
  weather/race control (Q5, 3.8 ms under noise), stronger lz4 levels (Q12,
  ~2 ms read gain for 10x write cost), and a Rust lap-frame builder (inside
  Q18, 1 ms per session).

### Performance — Pergamon series (2026-10-15)

Ten hypotheses on the warm path (sessions already in the persistent cache),
measured on a 32-core workstation (`.agents/perf-pergamon/RESULTS.md`,
harnesses `tools/pergamon_*.py`); seven shipped, three rejected by measurement.
Warm pandas load of the 2026 Monaco race (1452 frames, fresh process,
8 interleaved pairs vs the Thebes tree): total 0.430 → 0.279 s (−35 %),
telemetry 0.179 → 0.057 s (−68 %), candidate faster 8/8. Warm polars:
telemetry 0.087 → 0.053 s (−40 %), 6/6. Caches written by earlier releases
read 65-67 % faster after a one-time background rewrite (2018-2025 sessions).

- **Per-driver typed-columnar frame rows (P3, `telemetry_frames_drv`,
  `core_utils/frame_codec.py`).** Pandas telemetry frames are materialized
  per driver: the laps are grouped by block layout and each group stores one
  array per block over all of its rows. A lap frame is a set of views into
  those arrays (no copy, no constructor), and a session reads ~22 rows
  instead of ~1450. Rows from earlier releases (per-lap pickles and per-lap
  typed-columnar blobs) stay readable; a session served from them is
  rewritten as driver rows in a background thread and the per-lap rows are
  deleted. Frames the codec cannot encode keep the per-lap table.
  Materialization write 237 → 141 ms, stored bytes −14 %. Parity: 0
  mismatches on 1452 (2026) + 9859 (2018-2025) frames.
- **Fast frame-decode primitives (P1, `core_utils/helpers.py`).** Block
  classes are resolved once and called directly (no `new_block` dispatch),
  `TimedeltaArray`/`IntegerArray` are built through `_simple_new`, and
  `RangeIndex`/`BlockPlacement` objects are shared between frames. Per-lap
  decode 80 → 42 ms per 1452 frames; the cold typed assembly uses the same
  primitives.
- **GIL-released parallel lz4 decompression (P2, `cache.py`).** Frame-tier
  blobs are decompressed through pyarrow's lz4 codec (same stored format,
  releases the GIL, returns a mutable buffer) in a thread pool, and decoded
  chunk by chunk as they complete: 33 → 10 ms for 1452 blobs on 8 threads,
  no bytearray copy. python-lz4 stays the serial fallback.
- **Batched polars tier read (P4, `cache.py`).** The per-driver IPC blobs are
  read in a thread pool and converted to polars once from the concatenated
  table (25 → 10 ms for 22 drivers); laps are slices at a global offset.
- **asyncio-free warm path (P5, `core.py`, `cache.py`, `cdn.py`,
  `async_fetch.py`).** `asyncio` is imported lazily (23 ms of the import
  chain, with `ssl` and `concurrent.futures`), laps and telemetry tier hits
  are served synchronously, and laps the negative-result cache knows are
  missing everywhere do not start the fetch pipeline. A warm `load()` never
  creates an event loop.
- **Single-lap telemetry from the frame tier (P6, `core.py`).**
  `lap.get_telemetry()` / `lap.telemetry` read the driver's frame-tier row
  before the payload tier and memoize every lap of that driver: the next lap
  of the same driver 1.1 → 0.2 ms (first touch of a driver 1.6 → 4.3 ms,
  the cost of decoding the whole row once). Frames are built as `Telemetry`
  objects directly, so a memo hit no longer pays a shallow manager copy
  (memo hit 28 → 1 µs); the wrapped object is kept in the memo.
- **Driver-merged telemetry from the driver row (P10, `models.py`,
  `Cache.get_driver_merged_frame`).** `laps.pick_driver(x).telemetry` is a
  slice of the driver's arrays (one slice per run of consecutively stored
  laps) instead of a concat of 60-80 lap frames: 14 → 5.4 ms for a 76-lap
  driver, dtypes and values equal to the concat.
- Measured and rejected: speculative tier prefetch under the pandas import
  (P7: the import lock and GIL made the overlapped run slower, 0.29-0.36 →
  0.38-0.65 s), batch orchestration trimming (P8: 2.5 ms left after P5),
  compact integer encoding of float channels (P9: no telemetry channel is
  integral — RPM, Speed and Throttle are interpolated floats).
- Fixed while wiring the tier: `set_telemetry_frames_batch` reported only the
  per-lap fallback rows when a batch mixed driver rows and fallback rows.

### Performance — Thebes series (2026-10-15)

Ten hypotheses measured on a 32-core workstation (`.agents/perf-thebes/RESULTS.md`,
harnesses `tools/thebes_*.py`); eight shipped, two rejected by measurement.

- **Latency-aware CDN routing and straggler hedging (TH3, `cdn.py`).** Each
  source keeps a moving average of its request latency; a request on the
  primary that has not answered within twice that average (clamped to 1 s -
  `cdn_hedge_max_delay`, default 4 s) is re-issued on the next source and the
  first answer wins. A source measured at more than twice the latency of a
  lower-priority mirror is re-ranked behind it for the process. Only stalled
  requests send a second request; `cdn_hedge=false` (`TIF1_CDN_HEDGE`) restores
  the sequential loop. Replay of the live cold-edge distributions, 300 files:
  79.6 → 13.6 s cold, 31.1 → 4.4 s with 503s and read timeouts, 6.9 → 4.6 s
  warm. Live cold edge (2026 Hungarian GP race, disjoint 200-file slices):
  47.3 / 72.3 s → 20.1 / 18.7 s, no read timeouts.
- **Typed-columnar frame-tier rows (TH1, `core_utils/frame_codec.py`).**
  Pandas telemetry frames are stored as their blocks' raw bytes behind a small
  header and rebuilt without the DataFrame constructor (most of a frame's
  unpickle time was the pandas skeleton). Rows from earlier releases stay
  readable. Warm telemetry phase 0.247 → 0.167 s (−32 %), total warm load
  0.552 → 0.446 s (−19 %), 8/8 interleaved pairs; 0 parity mismatches.
- **Direct block assembly for telemetry frames (TH2, `core_utils/helpers.py`).**
  `_typed_telemetry_frame` lays out the blocks itself instead of calling
  `DataFrame._from_arrays`, which re-sanitized every column: assembly of the
  1452 Monaco frames 877 → 562 ms (−36 %), identical frames on 10 335 payloads
  of 2018-2026. Probed at import; any drift falls back to `_from_arrays`.
- **Optional native telemetry decoder (TH4, `native/`, `tif1-native`).** A
  pyo3 extension parses one telemetry payload into typed numpy arrays with the
  GIL released; `tif1` uses it when importable and falls back to orjson. Decode
  of the Monaco corpus 413 → 223 ms serial and 35 ms on 8 threads; the
  post-fetch parse + assembly of a cold load 1.25 → 0.57 s. Built from source
  (`uvx maturin develop --release` in `native/`); `pip install tif1` is
  unchanged. With the decoder installed, telemetry payload channels returned
  by `Cache.get_telemetry()` may be numpy arrays.
- **One-wave laps prefetch (TH6, `core.py`).** `drivers.json`,
  `session_laptimes.json` and the requested weather/race-control tables are
  fetched in one concurrent async wave; the laps phase of a cold load costs
  one CDN round trip instead of two (simulated cold edge: 9.6 / 5.0 / 6.4 s →
  4.7 / 3.5 / 4.5 s).
- **Stale-while-revalidate live schedule (TH7, `events.py`).** After
  `schedule_ttl_hours` the cached calendar is served immediately and refreshed
  in a daemon thread; `get_session` no longer blocks on GitHub.
- **Polars frames as per-driver Arrow IPC rows (TH8, `cache.py`,
  `telemetry_frames_pl_ipc`).** Read through pyarrow and sliced per lap. Warm
  polars telemetry 0.146 → 0.077 s (−47 %), total 0.501 → 0.414 s, 6/6 pairs.
  Partial batches merge per driver; per-lap rows from earlier releases stay
  readable.
- Rejected with numbers: pysimdjson typed decode (TH5, parity with orjson on
  the sentinel-heavy 2026 corpus), byte-level `"None"` → `null` rewriting (TH9,
  `bytes.replace` costs more than the parse saves), cold-edge concurrency above
  22 (TH10, more read timeouts on the slow mirror, parity on the fast one).
- Fixed while measuring: an all-null numeric telemetry channel on the polars
  backend is cast to `Float64` like the all-`"None"` case.

## [0.9.0] - 2026-10-15

### Summary

`0.9.0` adds Python 3.15 support, moves to polars 2.0, and reads the current
season's calendar live from `theOehrly/f1schedule`. Several performance series
(K, L, N, Rhodes, Tyria, Ithome, Halikarnassos, Kroton, Argos) add materialized
laps and telemetry frame tiers, a negative-result cache, and a cache that works
under the default config, so repeat loads of a cached session skip most of the
payload parsing and frame assembly (per-series numbers below). A documentation
audit fixed behaviour bugs across offline/CI mode, the circuit breaker, schedule
UTC dates, deleted fastest laps, and charts on polars sessions.

Upgrade notes:

- Names that resemble no event now raise `tif1.EventNotFoundError` (a
  `ValueError`) instead of loading the closest event.
- polars `>=2.0.0,<3` and pyarrow are runtime dependencies; pandas `>=3.0.6`.
- The unused `log_level`, `http2_max_connections` and `http2_max_pool_size`
  config keys (and their environment variables) were removed.
- Charts default to the `dark` theme (was `fastf1`); pass
  `color_scheme="fastf1"` for the previous look.
- Chart themes are `fastf1`, `dark` and `light`. `default-dark` and
  `default-light` were renamed to `dark` and `light`; the old names still work
  with a `DeprecationWarning`. The former plain `light` theme was removed, so
  `light` now also hides the top/right spines and y tick marks.

### Changed — chart theme names

- `setup_mpl(color_scheme=...)`, every chart's `color_scheme` and
  `get_plot_style()` accept `"dark"` (`#011627` background, lime text) and
  `"light"` (lightblue background, black text), the TracingInsights v2
  themes formerly called `default-dark`/`default-light`.
  Every chart function now defaults to `color_scheme="dark"` (all but
  `plot_race_launch_ratings` used `"fastf1"`); `setup_mpl()` still applies no
  theme unless one is passed, as in FastF1. `get_plot_style()` defaults to
  `"light"`. The documentation charts are rendered with `dark`.

### Fixed — found by the documentation audit (2026-10-14)

- **`Laps.groupby` keeps its key names.** `Lap`, `DriverResult`, `Driver` and
  `Event` replaced pandas' own `Series._metadata = ["_name"]`, so every copy
  dropped the Series name; groupby copies its key columns, so the result index
  had no names and `reset_index()` produced `level_0`/`level_1` columns.
- **`offline_mode` and `ci_mode` apply to every payload fetch.**
  `PayloadLoader.get` now matches `fetch_json_async`: offline mode serves
  cached payloads and raises `NetworkError` on a miss without a request; CI
  mode skips the persistent cache. The parallel session-table prefetch in
  `Session.load()` (its own HTTP session) is skipped in offline mode and
  bypasses the persistent cache in CI mode.
- **A 404 no longer trips the circuit breaker.** On the sync path every
  `DataNotFoundError` counted as a breaker failure, so two missing optional
  payloads opened the process-wide breaker and blocked all fetches for 60 s.
  `CircuitBreaker.call` and `retry_with_backoff` now treat missing, refused
  (403/404) or malformed payloads as answers from a healthy server: not
  counted, not retried — as the async path already did.
- **jsDelivr 403s on the sync path are file refusals.** `NiquestsTransport`
  raises `NetworkError(status_code=403)` without a response object, so the
  status was invisible and the 403 was retried with backoff and counted against
  the mirror. An unknown driver code on a cold session took 30-60 s to fail; it
  now fails in about 3 s.
- **408/429 are not "missing everywhere".** A CDN that answered "try again
  later" on every mirror produced `DataNotFoundError` and a 7-day missing
  verdict; it now raises a retryable `NetworkError`.
- **`session.laps` stays a `Laps` after `await session.laps_async()`.** The
  async path stored the plain frame, so `pick_*` methods disappeared.
- **`get_cache()` reopens after `close()`.** Closing the global cache left
  `get_cache()` returning the closed instance, which dropped every later read
  and write.
- **`tif1 events 2017`** prints "No events found for 2017" and exits 1 instead
  of a `KeyError` traceback.
- **`SessionNotLoadedError` accepts `**context`** like every other `TIF1Error`.
- **`Session1DateUtc`-`Session5DateUtc` hold UTC.** The f1schedule dates are
  local track time and were copied into the UTC columns unchanged; they are now
  local time minus the event's `GmtOffset` (Monaco 2024 race: 15:00+02:00 local,
  13:00 UTC). Negative half-hour offsets such as `-04:30` also parse correctly.
- **`max_retries=0` makes one attempt.** `retry_with_backoff` never called the
  function for values below 1 and raised `RuntimeError`; the async path already
  treated 0 as a single attempt.
- **Charts:** `plot_driver_laptimes` puts the fastest laps on top (seaborn
  already inverts a timedelta y-axis, and the extra `invert_yaxis()` flipped it
  back); `plot_track_temperature` colors several drivers by team instead of
  one shared red; `plot_throttle_distance` and `plot_downforce_levels` titles
  name the loaded session instead of always "Qualifying".
- **CI mode skips every persistent tier.** `ci_mode` now forces the session's
  `enable_cache` off, so the laps and telemetry frame tiers are neither read nor
  written (before, only payload reads and writes were bypassed).
- **Chart `drivers` accept names.** Every chart with a `drivers` parameter
  fuzzy-resolves the entries to abbreviations first. Before, only
  `plot_driver_laptimes` and `plot_position_changes` did; `plot_lap_delta`
  raised "No common laps" for `["Verstappen", "Leclerc"]` and
  `plot_track_temperature` drew empty lines.
- **Charts work with `lib="polars"`.** Every chart failed part-way on a
  polars session with an `AttributeError`. Charts now load the pandas view of
  the session for either backend (same payloads and payload cache) and draw
  the identical figure; any other `lib` raises `ValueError`.
- **A deleted lap is never the fastest lap.** `Laps.pick_fastest()`,
  `Session.get_fastest_laps()` (cold and loaded paths), `Driver.get_fastest_lap()`
  and everything built on them skip laps marked `Deleted`, as FastF1 does
  (2023 Spanish GP qualifying: TSU's fastest was his deleted 1:13.316, ranking
  him 11th; it is now 1:13.862, 15th, the official result).
  `pick_fastest(only_by_time=True)`, ignored before, returns the plain minimum.
- **Event-name tie-break.** When several events tied on the best fuzzy score,
  the tie-break could hand the match to an untied event: "Bahrein" resolved to
  the Spanish Grand Prix. Only the tied events are compared now.
- **Jupyter session card counts drivers after `load()`.** A warm laps frame-tier
  hit left the driver list unread, so the card said "Not loaded".
- The missing-`timple` warning no longer points at a nonexistent `plotting`
  extra (`timple` is a required dependency).

### Changed — documentation audit follow-ups (2026-10-14)

- **Names that resemble no event raise `EventNotFoundError`.** Fuzzy event
  matching had no minimum score, so `get_session(2025, "Invalid GP", "Race")`
  silently loaded the Italian Grand Prix. A fuzzy match now needs a score of
  70 (typos of real events score 75-97); below that `get_event_by_name`,
  `get_session` and `get_sessions` raise the new `tif1.EventNotFoundError` (a
  `ValueError`). `EventNotInSeasonError` is now its subclass. `get_sessions`
  (and `tif1 sessions`) used to answer the generic five sessions for any
  unknown name. `get_event()` and `EventSchedule.get_event_by_name()` still
  return `None` for no match.
- **pyarrow is a runtime dependency.** polars sessions converted lap rows and
  the track-status column through Arrow, so `get_lap()` and `track_status`
  raised `ModuleNotFoundError` on a plain `pip install tif1`.

### Removed

- The `log_level`, `http2_max_connections` and `http2_max_pool_size` config
  keys and their `TIF1_LOG_LEVEL`, `TIF1_HTTP2_MAX_CONNECTIONS` and
  `TIF1_HTTP2_MAX_POOL_SIZE` environment variables. Nothing read them. Set the
  log level with `tif1.set_log_level()` or the `logging` module. Old `.tif1rc`
  files that still contain the keys load as before.

### Changed — live event schedule (2026-10-14)

- **The current season's calendar is fetched live from `theOehrly/f1schedule`
  on `raw.githubusercontent.com`, the same file FastF1 reads.** It used to
  come from the vendored `schedule_{year}.json` snapshot, which only lagged
  behind upstream; jsDelivr (the old fallback for years without a snapshot)
  pins branch files for 12 hours, raw GitHub for 5 minutes. With the 2026
  calendar being rewritten mid-season, the vendored snapshot was already
  missing a round. Order for the current and future seasons: SQLite row
  younger than `schedule_ttl_hours` (default 6, `TIF1_SCHEDULE_TTL_HOURS`,
  `0` re-fetches every lookup) → live fetch → stale row → vendored file.
  Past seasons still come from the vendored files with no request. A failed
  fetch logs a warning and keeps the fallback for 5 minutes;
  `offline_mode` never fetches. The in-process memo honours the same TTL,
  so a long-running process picks up calendar changes too. This is the one
  sanctioned `raw.githubusercontent.com` request in tif1: one small file per
  season, outside the data-repository CDN chain.

### Performance — Argos series (2026-10-14)

Twelve hypotheses measured on a 32-core workstation (every prior series ran on
1-core sandboxes); nine shipped, three measured and documented
(`.agents/perf-argos/RESULTS.md`, harnesses `tools/argos_*.py`).

- **Bulk fetch no longer serialises on the event loop (A12).** niquests returns
  lazy multiplexed responses and the first `status_code` access resolved them
  on the loop thread: 1461 resolutions summed to 9.7 s of a 10.2 s telemetry
  batch. The response is now resolved inside the worker hop; interleaved
  parse-phase A/B on the full Monaco 2026 race: candidate faster 3/3 pairs
  (−34 % to −72 %). End-to-end cold Monaco 2026 load, 3 interleaved
  pairs on a warm edge: candidate faster 3/3, steady-state pair 13.2 → 10.65 s
  (−20 %), medians 86.5 → 10.7 s because the control hit the 403/timeout
  collapse regime once (614 s) and the candidate never did.
- **A 403 is a per-file refusal, not a CDN failure (A11).** jsDelivr answers
  403 "Package size exceeded the configured limit of 50 MB" for files it has
  not cached from the per-year data repositories; three of them used to
  disable jsDelivr for the whole process and route every later payload
  through the slowest mirror. 403 now falls through (and races the remaining
  mirrors) exactly like 404; deterministic simulation with 20 % cold files:
  median 1.33 → 0.68 s per 300-file batch, worst case 3.39 → 0.76 s.
- **Typed telemetry frames are built with `DataFrame._from_arrays` (A1)** from
  `array.array`-converted channels with a one-pass "None"-sentinel coercion:
  full-corpus assembly −7 % to −16 % per interleaved pair (1452/1452 frames
  identical in columns, dtypes and values) and frames leave the builder with
  2.7x fewer blocks, so the materialized-frame tier unpickles 22 % faster
  (206 → 160 ms) and every later `copy()`/slice of a frame is cheaper.
- **Float seconds → `timedelta64[ns]` is a vectorized, bit-exact replica of
  pandas' cast (A2)** for telemetry `Time` and the laps timedelta columns
  (91 → 8 µs per 400-sample channel; `pd.to_timedelta` converted every
  sequence twice).
- **The deferred payload-tier flush runs after `fetch_all_laps_telemetry`
  returns (A3)**, in the same joined background writer the frame tier uses
  (277-293 ms off the cold critical path here, 0.78 s on the K-series sandbox).
- **Laps frames are consolidated after dtype processing and `Driver.laps`
  uses a boolean mask instead of `DataFrame.query` (A4)**: one block per
  column (46 on a race) became 17; `pick_driver` −48 %, `pick_quicklaps`
  −37 %, row slices ~2x.
- **`pick_fastest`, `_filter_valid_laptimes` and `iterlaps` stop copying the
  frame (A5)**: `pick_fastest` 2.74 → 1.19 ms, `iterlaps` 18.3 → 1.9 ms per
  60 laps, identical rows, names and dtypes.
- **`Telemetry.slice_by_time`/`slice_by_mask`/`get_first_non_zero_time_index`
  skip redundant coercions and copies (A6)**: −32 % / −36 % / −67 %.
- **`http_disable_http3` works (A7).** The replacement adapter dropped the
  switch, so HTTP/3 was always negotiated; it is now passed through. With the switch working, HTTP/2 was at parity or faster in 5/6 full-session and 3/3 small-batch interleaved pairs, and every stalled fetch seen this series ran over HTTP/3, so **HTTP/2 is now the default** (`TIF1_HTTP_DISABLE_HTTP3=false` re-enables QUIC).
- **`get_session()` pre-connects the CDN for uncached sessions (A8,
  `cdn_preconnect`, `TIF1_CDN_PRECONNECT`)**: the first payload request of a
  cold load skips DNS + TLS (cold laps phase median −6 % to −12 %, 10/14
  interleaved pairs). Skipped for cached sessions and in offline/CI mode.
- Measured, documented, not shipped: process-pool parse+assembly (A9: −68 %
  on the post-gather phase with 8 workers, needs raw-byte plumbing and ~1 GB
  of worker RSS) and alternative transports (A10: aiohttp −21 % on the raw
  84 MB transfer; curl_cffi erratic) — see RESULTS.md.

### Summary — Kroton-series performance work

Ten new hypotheses were measured (full log in
`.agents/perf-kroton/RESULTS.md`); all ten ran to a verdict, three shipped,
seven were rejected by measurement. Headline numbers on the 2026 Monaco GP
race benchmark (~1452 telemetry frames, 1455 laps rows):

- **Laps frame tier (pandas + polars)**: the processed laps DataFrame
  materializes once per (year, gp, session) in `laps_frames` /
  `laps_frames_pl`; warm first-touch `session.laps` **67.8 → 4.6 ms (14x)**
  on pandas and **29.2 → 5.6 ms (5.2x)** on polars. Corrupt or
  foreign-backend rows degrade to the payload path; ultra-cold loads never
  read the tiers.
- **Typed direct block assembly** for `_typed_telemetry_frame`: a bit-exact
  vectorized replica of pandas' `cast_from_unit_vectorized` Time conversion
  (~26x faster than `pd.to_timedelta` on float arrays), pre-typed
  Driver/Brake/nGear/DRS/LapNumber columns, an arrow string fast path, and
  direct `BlockManager` construction. Cold per-frame assembly
  **1447 → 1155 ms median (1.25x)**; offline best-of 940 → 576 µs/frame.
  Falls back to the plain constructor on any pandas-internals drift.
  *Merge note:* the Argos series landed its own version of this builder
  (`DataFrame._from_arrays`, consolidated blocks, `array.array` channel
  conversion); measured head-to-head on the same corpus it was faster in
  5/5 interleaved pairs, so the direct `BlockManager` helpers were dropped in
  the merge and the vectorized Time cast kept its finite/huge-value guard.
- Rejected with numbers: typed merged-dict assembly (+5.6% regression — the
  per-frame win does not transfer to 614k-row merged frames), zstd-1 codec
  re-eval (lz4 wins both directions on current deps), pydantic validation
  share (4 ms), `get_session` first-call cost (it is the pandas import),
  results first-touch (0.7 ms), block-concat of frame hits (13.8 ms),
  laps dtype-pass rework (sub-costs scattered; subsumed by the tier).

### Added — Python 3.15 support

- Python 3.15 is a supported, CI-tested interpreter: the `3.15` trove
  classifier is declared and the test matrix runs 3.11 through 3.15
  (`setup-python` falls back to a release candidate when the just-released
  minor is not yet in its manifest).
- Runtime lower bounds raised to the first releases that publish 3.15 wheels so
  `pip install tif1` on 3.15 never builds from source: pandas 3.0.6,
  pydantic 2.14.0 (pydantic-core 2.50.0), matplotlib 3.11.2. scipy 1.18 dropped
  Python 3.11, so scipy is split by marker: `>=1.17.1` on 3.11-3.14 and
  `>=1.18.1` on 3.15. Dev extra allows pyarrow 26 (`<27`).
- The SQLite cache uses the standard-library `compression.zstd` module
  (PEP 784) on Python 3.14+ and the `zstandard` wheel only on 3.11-3.13
  (`python_version < "3.14"` marker). Both bind the same libzstd and emit
  byte-identical single-frame blobs with the content size in the header, so
  caches written under either binding read under the other; compress and
  decompress throughput measured within noise of each other.
- `lz4` publishes no 3.15 wheels yet, so it is declared for
  `python_version < "3.15"` only; on 3.15 the materialized-frame tier uses
  the zstd fallback that was already in place (the pre-Ithome codec). Drop the
  marker once lz4 ships 3.15 wheels.
- `uv.lock` refreshed for the above (kiwisolver 1.5.1, msgpack 1.2.3,
  numpy 2.5.3 on 3.12+, contourpy 1.4.0, fonttools 4.66.1, pyarrow 26.0.0,
  typing-inspection 0.4.4).

### Changed — polars 2.0

- polars requirement raised to `>=2.0.0,<3` (was `>=1.44.1,<2`); `uv.lock`
  pins polars 2.0.0. polars 2.0 ships `cp310-abi3` wheels, so Python
  3.11-3.15 are all covered. tif1 itself needed no API changes: it uses only
  eager frames (the new streaming default applies to `LazyFrame.collect()`),
  never casts strings to temporal types, never concatenates horizontally, and
  calls none of the removed APIs.
- Verified on real data: every polars-backend output (laps, weather, race
  control, fastest laps warm and cold, fastest-lap telemetry, and the full
  476k-row Monaco qualifying telemetry) is identical between polars 1.44.1 and
  2.0.0 in values, dtypes, and row order. Telemetry frames that polars 1.44.1
  wrote to the `telemetry_frames_pl` cache tier load unchanged under 2.0, so
  existing caches stay warm across the upgrade. Polars-backend load and
  telemetry assembly times are unchanged within noise.
- Docs: polars version statements updated; `pl.NUMERIC_DTYPES` (removed) →
  `pl.selectors.numeric()`, `collect(streaming=True)` (removed) →
  `collect(engine="streaming")`; added a note on the streaming default and row
  order for lazy queries. Polars examples that were already wrong before 2.0
  are fixed as well: boolean-mask indexing (`laps[mask]`, which polars reads
  as a column selector) → `filter`, `.dt.total_seconds()` on the Float64
  `LapTime` → a `Duration` cast, `map_elements` inside `agg` (called per value,
  not per group) → an expression or `map_batches(returns_scalar=True)`,
  horizontal `pl.concat` of laps and telemetry (duplicate `Time` column) → a
  same-height rank frame, and the pandas-only `to_parquet` in the export
  tutorial is labelled as such with a pointer to `write_parquet`.

### Fixed — polars backend

- Race control messages no longer crash the polars backend when the `Lap`
  column carries the upstream `"None"` sentinel (every non-race session, and
  any race with an unnumbered message). `Lap` and `Sector` are now coerced like
  the pandas backend's `to_numeric(errors="coerce")`, with sentinels becoming
  null. Previously the strict cast raised polars' `InvalidOperationError`,
  which the loader's error handling did not catch, under both polars 1.x and 2.x.
- Weather and race control tables on the polars backend now get the same
  sentinel handling as laps: `"None"`-style strings are replaced by null
  before the frame is built (`_coerce_null_like_payload`, shared with the lap
  path), so polars infers Boolean/Float/Int columns instead of String ones
  that no cast can recover (String → Boolean is unsupported even with
  `strict=False`). The weather casts are non-strict like pandas'
  `to_numeric(errors="coerce")`, and `WindDirection` is rounded rather than
  truncated, matching the pandas branch. Outputs for real sessions are
  byte-identical to before.
- The pandas backend had the same weather bug with a quieter failure: one
  `"None"` sentinel in `Rainfall` made `astype("boolean")` raise `TypeError`,
  which the loader swallowed into an **empty weather table** plus a warning.
  The pandas branch now runs the existing `_replace_null_like_strings` pass
  before its casts, so the row keeps its other readings and `Rainfall` becomes
  `<NA>`. Race control on pandas already coerced sentinels and is unchanged.
  Verified on four real sessions (two backends, 32 tables): identical output.

### Fixed — static typing (`uv run ty check src/tif1` is clean)

- `cache.py`: the optional `zstandard` and `lz4.frame` codecs are imported
  with `importlib.import_module` behind a declared `ModuleType | None`, so the
  checker neither fails to resolve a wheel that is not declared for the
  running interpreter (`zstandard` on 3.14+, `lz4` on 3.15) nor rejects the
  `None` fallback assignment. Runtime behaviour is unchanged.
- `charts/track_maps.py`: gear colorbar tick labels are passed as strings, as
  `Colorbar.set_ticklabels` is typed; matplotlib rendered them as strings
  already.
- `jupyter.py`: pandas is a hard runtime dependency, so the dead
  `except ImportError: pd = None` guard (which made every `pd.DataFrame`
  annotation `None | ...`) is gone.
- Removed four `# type: ignore` comments the checker reported as unused.

### Fixed — behaviour found by running the docs against real data (both backends)

- Telemetry `"None"` sentinels no longer stringify numeric channels. Upstream
  writes the literal string `"None"` for `DistanceToDriverAhead` on a race
  leader's lap (nobody ahead) and for `DriverAhead` in the first samples of a
  lap; one such value made the whole column `str` on pandas and `String` on
  polars, so `get_fastest_lap_tel()` and `get_lap(n).telemetry` disagreed on
  dtypes from lap to lap. Every numeric channel (`RPM`, `Speed`, `Throttle`,
  `Distance`, `RelativeDistance`, `DistanceToDriverAhead`, `AccelerationX/Y/Z`,
  `X`, `Y`, `Z`) is now float64/Float64 with NaN/null for the sentinel, and
  `DriverAhead` holds driver-number strings with NaN/null when nobody is
  ahead. The fast pandas constructor, the legacy per-column pass, the
  merged-dict assembly and the polars builder all apply the same rule; clean
  payloads take the same code path as before (verified dtype-identical).
- Materialized telemetry frames cached by earlier releases carry the
  stringified column and have no per-row version, so the SQLite cache now
  records a frame-tier schema version in `PRAGMA user_version` and drops the
  two frame tables once when it is older. The payload tier is untouched, so
  the frames rebuild from local data without a network fetch.
- `Laps.pick_driver` / `pick_drivers` accept driver numbers (`pick_driver(1)`,
  `"1"`, mixed with codes) by matching `DriverNumber` as well as `Driver`, as
  FastF1 does; previously a number silently returned an empty frame.
- `Telemetry.slice_by_lap` returned no rows on tif1 frames: it compared a
  lap's session-time window (`LapStartTime`..`Time`) with telemetry `Time`,
  which tif1 keeps lap-relative. Frames without a `SessionTime` column now
  match on `LapNumber` (and `Driver`, so a multi-driver frame returns only
  that driver's lap); frames that do carry `SessionTime` keep the window.
- `Telemetry.resample_channels()` defaulted to `"1S"`, an offset alias pandas 3
  removed (`ValueError: Invalid frequency`). The default is `"1s"`, and the
  upper-case aliases FastF1-era code passes (`S`, `L`, `T`, `H`) are translated
  to their current spelling.
- `tif1.utils.delta_time` was a placeholder that returned an empty Series. It
  now implements FastF1's algorithm (comparison-lap time interpolated onto the
  reference lap's distance axis; positive delta = comparison lap slower at
  that point), accepts `Lap` series, `Driver.get_lap` laps or telemetry frames
  on either backend, and adds `Distance` when a frame lacks it.
- `tif1.jupyter.display_lap_info` raised `AttributeError: '_telemetry'` for
  laps from `Driver.get_lap` (a `Lap` series keeps its telemetry in the
  session memo, not on the object); it now probes the memo instead.
- Session names are case-insensitive: `get_session(2025, "Bahrain", "race")`
  and `"qualifying"` resolve like `"Race"` and `"R"` instead of raising
  "does not exist".
- Event names typed as a circuit, city or country no longer load the wrong
  race. Fuzzy matching resolved `"Spa"` to the Spanish Grand Prix (`spa` is a
  substring of `spain`), `"USA"` to Qatar (a substring of `Lusail`),
  `"Interlagos"` to the Dutch Grand Prix and `"Britain"` to Bahrain, in every
  season, for `get_event`, `get_event_by_name` and `get_session` alike. A
  shared alias table (`EVENT_NAME_ALIASES`; the previously unused
  `core.GP_ALIAS_MAP` is now a view of it) maps such names to the event they
  mean, per season: `"Barcelona"` is the Spanish Grand Prix until 2025 and the
  Barcelona Grand Prix in 2026, `"Interlagos"` the Brazilian Grand Prix before
  2021. An alias whose event is not on that season's calendar
  (`get_event_by_name(2021, "Suzuka")`) raises a `ValueError` naming the event,
  instead of returning a different race; `get_session` logs the same message
  and loading then raises `DataNotFoundError`. Exact event names still win,
  `exact_match=True` ignores aliases, and countries that hosted several events
  (Germany in 2020) still use fuzzy matching.
- The fuzzy matcher now also scores the event's own name (the schedule
  metadata has no `EventName` key, so `"British"`-like queries were compared
  only with location, country and the sponsor-laden official name), and it
  pads incomplete feature lists instead of letting numpy raise on a ragged
  array.
- `session.drivers`, `driver_list`, `drivers_df` and `results` are in
  classification order, as FastF1's `Session.drivers` is. The upstream
  `drivers.json` already lists the official classification for most sessions,
  but several seasons before 2025 sort it by car number as strings
  (`"1", "10", "11", "14", "16", ..., "2", "20"`), so every chart's
  "top-N finishers" default (`charts._common.finishing_order`) picked the
  wrong drivers for those sessions and `session.drivers[:10]` in the docs was
  not a top ten. A number-sorted payload is now reordered from the laps table:
  laps completed, then position on the last lap for races and sprints; best
  non-deleted lap time for every other session. Payloads that already are a
  classification are left alone and never load laps. Post-session penalties
  are not visible in lap data, so a rebuilt order can differ from the official
  result by a place or two. `finishing_order` maps numbers through
  `drivers_df` instead of building one `Driver` per entry.
- `session.drivers` was empty, `drivers_df["DriverNumber"]` blank and
  `get_driver("1")` failed for sessions whose `drivers.json` omits car numbers
  (2023 Monaco Grand Prix Race among others). Numbers are filled in from each
  driver's lap rows.
- `session.results` is a classification instead of placeholders. `Position`
  and `ClassifiedPosition` number the classification order, `Laps` is the laps
  completed, and `Status` is derived from lap counts for races and sprints
  (`"Finished"` on the leader's lap count, `"+N Lap(s)"` behind it, `"Did not
  start"` with no laps) and `"Finished"` otherwise. Previously every driver
  was `"Finished"` with `Position None`, `Points 0.0` and `Laps 0.0`. The data
  source publishes no official classification, so `GridPosition` and `Points`
  are NaN (float64, like FastF1's columns) rather than invented values, and a
  session without a laps payload keeps its entrants with NaN
  `Position`/`Laps` and `None` `Status`.
- `Laps.to_sql(..., index=True)` raised `AttributeError: 'NoneType' object
  has no attribute 'columns'`: the `Laps.reset_index` override assumed a
  returned frame, but pandas resets the index in place inside `to_sql` and
  then inserts that column. `inplace=True` now returns `None` and keeps the
  new column, like pandas.
- Error messages and `.context` use the plain event and session names.
  `DriverNotFoundError` (and every `DataNotFoundError` raised from the fetch
  path, which builds CDN paths from the encoded form) reported
  `event='Bahrain%20Grand%20Prix', session='Practice%201'`. `Session` gained
  `event_name` and `session_name` properties with the names as the schedule
  spells them (`gp`/`session` stay URL-encoded because every CDN path is built
  from them), and `session.name`, `session_info` and `session.event` decode
  every percent-escape instead of only `%20` (`"São Paulo Grand Prix"` was
  `"S%C3%A3o Paulo Grand Prix"`).
- `get_session(2021, "Suzuka", "Race")` raises
  `EventNotInSeasonError("'Suzuka' refers to the Japanese Grand Prix, which is
  not on the 2021 calendar")` immediately, a `ValueError` subclass exported
  from `tif1` and `tif1.exceptions`, instead of building a session named
  "Suzuka" that failed at load with `DataNotFoundError`.
  `get_event_by_name` raises the same class (previously a plain `ValueError`
  with the same message, so `except ValueError` still works).
- `tif1.plot_driver_laptimes` and `plot_qualifying_results` no longer emit
  numpy 2.5's `DeprecationWarning` about a unit-less `np.timedelta64("NaT")`:
  it comes from the timple package's own converter, so `setup_mpl` filters
  that warning for timple's modules only.
- `charts/track_maps.py` docstring counted five `plot_track_*` charts; there
  are four (plus the gear-shift and corner-annotation maps).
- Infrastructure findings from running the API reference pages against the
  library:
  - `cdn_use_minification=True` rewrote every request to `.min.json`, which
    jsDelivr (it minifies only JavaScript and CSS) and StaticDelivr answer
    with 404, so each fetch paid a wasted round trip before the next CDN
    served the file. The default CDNs now ignore the option (as Hugging Face
    already did); it still applies to custom mirrors that serve minified
    copies, and `CDNSource(use_minification=True)` keeps its per-source meaning.
  - `DataNotFoundError()` and `NetworkError()` without context read
    "Data not foundData not found" / "Network request failedNetwork request
    failed".
  - The DNS-over-HTTPS fallbacks never worked: `doh://cloudflare` is parsed by
    niquests as a DoH server literally named "cloudflare". The default
    `http_resolvers` are now `["standard", "doh+cloudflare://",
    "doh+google://"]`, the legacy `doh://<provider>` spelling in existing
    `.tif1rc` files is translated, and the resolver is passed to the pooled
    `HTTPAdapter` that `_create_session` mounts (it previously dropped it).
  - Connection-pool statistics counted every request as a reused connection;
    reuse is now read from niquests' `conn_info` (no new TCP/TLS handshake).
  - `pool_exhaustion_backoff_*` fallbacks in `async_fetch` matched neither the
    documented nor the configured defaults (0.5/5.0/0.5 vs 0.01/0.5/0.01).
  - `validate_race_control()` rejected every real `rcm.json` (`time` holds ISO
    wall-clock strings, the model allowed only floats) and `validate_laps()`
    rejected every real `laptimes.json` (`"None"` strings are now normalized
    first, like the other validators).
  - `json_dumps`' stdlib fallback used spaced separators; it now emits the
    same compact shape as orjson.
  - `Cache.has_session_data()` matched only URL-encoded names; plain names
    work too.
  - `tif1 drivers` always crashed (it indexed driver-number strings as dicts)
    and `tif1 fastest` crashed on the pandas backend (`LapTime` is a
    Timedelta, formatted with `:.3f`). The drivers table also shows the car
    number.
  - Weather and race-control log lines, and the Jupyter lap card, show the
    plain event name; the lap card title reads "Lap 10", not "Lap 10.0".
  - `session.laps` / `load()` returned an empty table and only logged when
    every CDN request failed with a transport error, hiding an outage (and the
    next `get_lap` then raised a misleading `LapNotFoundError`). A load that
    produced no laps while the CDN failure counts grew now raises
    `NetworkError`; a session whose laps genuinely do not exist (404 on every
    CDN, which never counts as a failure) still returns an empty table.
  - `get_sessions(year, event)` only matched exact event names, so an alias
    such as `"Spielberg"` silently returned the generic five-session weekend;
    it now resolves names like `get_event_by_name` (exact, alias, fuzzy).
  - `setup_mpl(color_scheme="official")` silently did nothing; an unknown
    scheme raises `ValueError` listing the valid ones.
  - Driver-name lookups in `tif1.plotting` fuzzy-matched a name against bare
    abbreviations when a season publishes no driver names (2024 Belgian Grand
    Prix: `"Hamilton"` resolved to SAI with only a warning). Such a lookup now
    raises `KeyError` and asks for the abbreviation.
  - `CDNManager.reload()` rebuilds the source list from the current config, so
    `config.set("cdns", ...)` after import can take effect
    (`get_cdn_manager().reload()`).
  - `LapNotFoundError.context["event"]` was still URL-encoded (the lap lookup
    passes the session's encoded `gp`); every `DataNotFoundError` flavour now
    decodes `event`/`session`.
  - An open circuit breaker raised a bare `Exception("Circuit breaker is
    open")`, which `retry_with_backoff` and `except NetworkError` handlers did
    not catch. It raises `CircuitBreakerOpenError`, a `NetworkError` subclass
    with the same message, exported from `tif1` and `tif1.exceptions`.
  - The unit suite isolates the cache directory per test (`tests/conftest.py`):
    tests that build `Cache()` under a stub config used to write into `./None`,
    and after that fallback was fixed they wrote into, and cleared, the
    developer's real cache directory.
- Regression tests for each item in `tests/unit/test_real_data_regressions.py`.

### Docs — backend data contract

- New "Data Contract by Backend" section in the backends guide: per-column
  dtype tables for laps, weather, race control messages, and telemetry on
  both backends (taken from real 2025 Bahrain data, including the pandas-only
  `index` column and the `LapStartDate` String/datetime difference), how
  `"None"` sentinels are normalised on each backend, and what an empty
  weather or race control table means and how to see the warning.
- New "Polars 2 notes" section: the polars 2.0 changes that affect code built
  on tif1 frames (streaming default, removed string→temporal casts,
  horizontal `concat`, strict `is_in`, removed APIs) with the replacement for
  each, and the cache-compatibility guarantee.
- The data-flow page's dtype comparison listed types tif1 never produces
  (`uint8`, `float32`, `Categorical` on polars); replaced with the real table.
- Examples that failed on real data on **both** backends are fixed: pandas
  `laps[laps["LapTime"] < 90]` and `query("LapTime < 90")` raise on the
  timedelta column (use `LapTimeSeconds` or `pd.Timedelta`), `fillna(method=)`
  was removed in pandas 3 (use `ffill()`), `astype(float)`/`to_datetime` on
  `LapTime` raise, `dropna()` on laps returns no rows, and `Speed` is not a
  laps column (use `SpeedST`).
- Troubleshooting and FAQ entries added for the polars 2 version conflict,
  an empty weather or race control table, and the `LapTime` comparison
  `TypeError`; the sessions page documents the weather dtypes per backend.
- Four pre-existing style-guide violations (sentences over 25 words in the
  charts and plotting references) and an MDX parse error in
  `DOCS_STE100_PLAN.md` (angle-bracket autolink) are fixed; `mint validate`,
  `mint broken-links`, and `tools/ste_check.py --strict` all pass.
- A dtype failure polars itself raises (`PolarsError`, which is neither a
  `ValueError` nor a `TypeError`) while loading weather or race control now
  degrades to an empty frame with a warning, exactly as the pandas branch does
  for its equivalent errors, instead of aborting `session.load()`. The error
  tuple is extended only when polars is already loaded, so pandas sessions
  never pay the polars import.
- A polars frame-tier cache row that polars cannot deserialize (raising
  `polars.exceptions.ComputeError`, which is not a `ValueError`) now degrades
  to a per-row cache miss, as the tier documents, instead of crashing the
  telemetry load. This guards against future polars serialization-format
  changes.

### Fixed — audit follow-ups

- The wheel now ships the `py.typed` marker that the `Typing :: Typed`
  classifier and `MANIFEST.in` already promised, so downstream type checkers
  pick up tif1's annotations.
- The materialized-frame cache tier is enabled whenever lz4 **or** a zstd
  binding is importable (previously it required zstd even when lz4 was
  present). A frame row that no importable codec can read is treated as a
  corrupt-row miss instead of tripping an assertion inside the batch reader.
  Interpreters with no zstd binding at all log an info message about the
  zlib fallback.
- Async cache methods use `asyncio.get_running_loop()`.
- The unit suite is hermetic: an autouse fixture in `tests/conftest.py`
  blocks real socket connections for every test not marked `integration`,
  `compliance_online` or `benchmark`. Several unit tests were quietly
  loading real sessions from the CDN when the network was reachable and
  could stall the CI matrix on a slow CDN; they now fail the connection
  fast and exercise the same graceful-degradation paths deterministically.
- `Cache.clear()` and `invalidate("telemetry")` now also drop the polars
  frame table (`telemetry_frames_pl`); previously polars frame rows survived
  a full clear. `invalidate("memory")` drops the in-memory mirror of the
  negative-verdict tier so it reloads from SQLite, and a successful `set()`
  for a key with a stale all-CDN-missing verdict removes the SQLite row as
  well as the in-memory entry (only when a verdict existed, so the hot path
  is unchanged).
- Documentation audit: `docs/concepts/caching-strategy.mdx` rewritten to
  describe the real cache (tiers, SQLite schema, codecs, config keys, public
  API) instead of a fictional one; cache-related corrections in
  `data-flow.mdx`, the cache API reference, the environment-variable and
  config references (`TIF1_ENABLE_CACHE`, JSON `.tif1rc`, memory-tier
  settings, negative-verdict TTL); the CLI reference now documents only the
  commands that exist; the non-existent `polars` and `plotting` extras are
  gone from the docs and polars is described as the declared dependency it
  is; `AGENTS.md` points at the real mintlify skill path.

### Summary — Halikarnassos-series performance work

Eleven new hypotheses were measured (full log in
`.agents/perf-halikarnassos/RESULTS.md`); all eleven ran to a verdict, three
shipped, eight were rejected by measurement. Headline numbers on the 2026
Monaco GP race benchmark (~1452 telemetry frames):

- Cold telemetry assembly **1568 → ~1354-1397 ms (−11 to −13%)** via
  preconverted ndarray columns and direct `IntegerArray` construction in
  `_typed_telemetry_frame` (1452/1452 frames dtype- and value-identical).
- Warm `get_session` **0.435 → 0.339 s (−22%)**: exact (case-insensitive)
  event names return without importing rapidfuzz or running the fuzzy
  matcher.
- Frame-tier whole-session batch reads use one range scan instead of a
  1452-pair `(driver, lap) IN (...)` list; SQL-only read **46 → 19 ms (−59%)**.
- Live cold fetch A/B: total **−5.5%**, telemetry phase **−9.8%** (3 runs per
  arm, interleaved).
- Rejected with numbers: `TimedeltaIndex` conversion (value parity fail),
  null-like probe rework (variants tie at 3.7 ms), merged-path ndarray
  preconversion (+17%, reverted), lazy cache init, further import trim,
  SQLite page-cache PRAGMA, lz4/unpickle thread overlap (lz4 holds the GIL),
  raw-ndarray Driver column (infers `str` dtype).
- Verified that warm loads make zero network requests; one probe that seemed
  to show warm-path network calls had set `TIF1_CACHE_DIR` after tif1
  imports (config resolved a cold default dir) — pitfall documented.

### Summary — Ithome-series performance work

Ten new hypotheses were measured (full log in `.agents/perf-ithome/RESULTS.md`);
all ten ran to a verdict, seven shipped, three were rejected by measurement.
Headline numbers on the 2026 Monaco GP race benchmark (~1452 telemetry frames):

- **`import tif1` 47 → 13.6 ms (−71%)** — `tif1.__version__` resolves lazily
  via PEP 562; the eager `importlib.metadata` lookup no longer loads the
  email/parser machinery on import.
- **Warm pandas telemetry 0.803 → 0.680 s (−15%)** — the materialized frame tier
  switches from zstd-1 to **lz4** (level 0): decompress 75 → 40 ms and compress
  170 → 79 ms on the 1452-frame set, at +26% stored bytes; legacy zstd rows stay
  readable and zstd remains the fallback when lz4 is unavailable.
- **Cold assembly 2136 → 1677 ms (−21%)** — `_typed_telemetry_frame` converts
  the `Time` channel through `np.asarray` before `pd.to_timedelta`
  (790 → 215 µs/frame), verified value-identical on all 1452 frames.
- **Cold `fetch_all_laps_telemetry()` returns ~1 s sooner** — the frame-tier
  bulk write (1034 ms: pickle + lz4 + executemany) moves to a background
  writer joined by `Cache.close()`/`invalidate()`/`clear()` and the atexit
  cache close; process-exit wall time is unchanged (latency win, not
  throughput).
- **Polars laps assembly 20.4 → 9.1 ms (−55%)** — polars laps build one
  dict-of-lists frame instead of per-driver frames + `pl.concat` (the pandas
  single-shot pattern; schema/values verified identical).
- **`Laps.telemetry` gets a frame-first route** — memoized frames, then the
  materialized frame tier, serve the merge without re-reading payloads
  (~90-105 → ~65 ms on a 78-lap driver, −35%); the payload-merge path remains
  the fallback for uncovered refs. A `(len, refs)`-keyed result memo makes
  repeat access ~30.5 → ~1 ms.
- **`import tif1.cli` 387 → 59 ms (−85%)** — pandas/rich imports moved into
  the command bodies; `tif1 --help` and non-data commands start much faster.

Rejected by measurement (documented with numbers in the results log):
background-prefetch deduplication (premise was an instrumentation artifact —
corrected live count shows 1465 requests for 1465 unique URLs and the default
config never starts the background thread), merged-loop numpy concatenation
(the 4.7 ms list merge has no headroom), and batched prefetch payload
persistence (−2.5%, within noise).

### Summary — Rhodes-series performance work

Rhodes-series performance work (10 hypotheses, all run; full log in
`.agents/perf-rhodes/RESULTS.md`). The headline: **warm loads get GC-aware
batch loops and the polars backend gains a materialized frame tier** — pandas
warm telemetry drops ~20% (0.875 → ~0.70 s on the Monaco 1452-frame set,
unpickle loop −44%), polars warm telemetry drops **−78–85%** (1.53–1.66 s →
0.25–0.37 s; previously polars re-read the payload tier and re-constructed
every frame on each load), cold assembly is −20%, and the `laps.pick_driver(x)
.telemetry` flow uses vectorized refs + one batched cache read. Seven of the
ten hypotheses were rejected or falsified by measurement — including a
columnar frame-tier rewrite (construction costs more than unpickle saves), raw
uncompressed blobs (parity at 4.3× storage), and cold orchestration overlap
(sub-noise prelude) — all documented with their numbers.

### Changed

- **GC suspension for short-lived allocation bursts** (`cache.py`,
  `async_fetch.py`, `core.py`): a `_suspend_gc()` context manager wraps the
  materialized-frame batch read/write, the payload batch read, the deferred
  telemetry flush, and the assembly loops. These tight loops allocate ~100 MB
  of short-lived objects; gen-0 collections cost ~44% of the unpickle loop
  (470 → 264 ms) and ~20% of the cold assembly (2.17 → 1.73 s). GC state is
  restored on exit (nested/disabled-safe) and never suspended across `await`.
- **Polars materialized frame tier** (`cache.py`, `core.py`): polars frames
  persist in a dedicated `telemetry_frames_pl` table (pickle protocol 5 +
  zstd-1, same as pandas) and warm polars loads read them directly instead of
  re-parsing the payload tier and re-constructing `pl.DataFrame` per frame.
  Both backends materialize from payload-tier hits and network fetches; tables
  are backend-isolated and foreign/corrupt blobs degrade to tier misses.
- **Vectorized `Laps.telemetry` refs + batched cache read** (`models.py`):
  `_telemetry_merged` extracts `(driver, lap)` refs with vectorized column ops
  (no `iterrows`) and reads warm-cache payloads in one batched IN query;
  still-missing refs keep the original per-ref chain (skip-verdicts, network
  fallback, failure recording).

### Changed

- **LapTime subset-fallback parse** (`core_utils/helpers.py`):
  `_process_lap_df` runs `pd.to_numeric` once and only string-parses the
  non-numeric subset instead of the full column + `.where`; exact dtype+value
  parity on realistic numeric+None input.
- **Explicit datetime formats on uniform-shape columns** (`core_utils/helpers.py`,
  `core.py`): `format="ISO8601"` for `LapStartDate` and pandas RCM `Time`,
  explicit `"%Y-%m-%dT%H:%M:%S%.f"` for polars RCM `Time`. Real payloads are
  uniform all-9-digit-fraction (verified live across 2021–2025 sessions), so
  the explicit format is parity-exact — and strictly more correct than
  first-format-wins inference on mixed shapes.
- **Import-time trim** (`exceptions.py`, `validation.py`,
  `core_utils/helpers.py`): `_NULL_LIKE_STRINGS`/`_coerce_null_like_string_list`
  live in pydantic-free `tif1.exceptions`; `validation` re-exports them for
  backward compatibility.
- **Inverted async cache-enable condition** (`core.py`): the telemetry fetch
  plan opened the SQLite cache exactly when caching was disabled; now gated on
  `self.enable_cache and self._session_cache_available()`.

### Rejected (measured, not shipped)

- Single-C-call `_numeric_seconds_to_timedelta`: ~−5% standalone but breaks the
  `never_passes_nan_to_seconds_timedelta` contract; NaN-safe variants slower.
- Single-blob session frames tier (~9.35x slower), batch JSON-tier cache reads
  (cold-SQLite-only ~37%, negligible warm benefit — excluded as marginal),
  exact-`isin` null-like probe (no measurable win), merged-dict cold-path
  assembly (already shipped), per-request fetch overhead (~0.008 ms/req —
  negligible).

### Added

- Tyria-series measurement harness and reports
  (`.agents/perf-tyria/tools/offline_assembly_bench.py`,
  `.agents/perf-tyria/PLAN.md`, `.agents/perf-tyria/RESULTS.md`).

### Summary — N-series performance work

N-series performance work (10 hypotheses, all run; full log in
`.agents/perf-lindos/RESULTS.md`). The headline: **the second load of a
full-telemetry session drops 5.37 → ~1.7-2.0 s total** — cold fetches now
materialize assembled telemetry frames into the frame tier directly (so the
next process loads from the fast tier instead of re-parsing 84 MB of payload
JSON first), and the session-table prefetch (drivers/weather/rcm) now both
persists what it fetches and consults the persistent cache before going to
the network (previously those tables were memo-only and re-downloaded by
every fresh process after a cold `load()`). Frame-tier bulk writes use
`executemany` (0.36 → 0.21 s per session write pass). Cold loads stay at
parity-or-better (final interleaved A/B, medians favor the candidate; the
added materialization write is post-gather and amortized). Seven of the ten
hypotheses were rejected by measurement — including a fully-built Rust/pyo3
columnar parser (parity-exact, 0% speedup) and pyarrow/msgspec/Arrow-IPC
alternatives — all documented with their numbers.

### Changed

- **Cold-path telemetry frame materialization** (`core.py`): network-fetched
  telemetry batches now append their assembled frames to the same bulk
  materialization write the payload-tier path uses, gated on the session's
  cache enablement. A cold `fetch_all_laps_telemetry()` leaves the frame tier
  fully populated, so the *second* load hits the materialized tier directly:
  warm-#1 telemetry 4.66 → 1.08 s (−77%), total 5.37 → ~1.7-2.0 s.
- **Session-table prefetch persists + reads cache** (`core.py`):
  `_prefetch_session_tables` now writes fetched drivers/weather/rcm payloads
  through the payload loader's cache seam (respecting `enable_cache` and test
  overrides) and checks the persistent cache before fetching — warm-#1 laps
  0.456 → 0.068 s (−85%), and `load()`-only sessions stop re-downloading
  those tables in every later process.
- **Bulk frame writes via `executemany`** (`cache.py`):
  `set_telemetry_frames_batch` batches the per-frame `INSERT OR REPLACE` rows
  into one C-level executemany loop (1452-frame pass 0.36 → 0.21 s); round-trip
  parity, corrupt-row fallback and commit cadence unchanged.

### Rejected (measured, not shipped)

- Brotli Accept-Encoding negotiation: ~5% smaller on-wire than gzip; A/B
  median −3.4%, pairs split 2-2 (noise).
- Concurrency slow-start ramp (6 → 22): A/B median +8.96% total; ramping
  delays the TLS warm-up without a compensating win.
- Custom Rust/pyo3 columnar parser + numpy-native assembly: built and measured
  (parity 0/1452 mismatches); pipeline 2.38 s vs 2.39 s — no speedup; the
  costs are pandas construction (parity-bound) and number parsing (orjson is
  already near-optimal).
- pyarrow.json parse (1.70 s vs orjson 0.84 s), Arrow IPC frame tier (3.05 s
  vs pickle+zstd 1.42 s), msgspec decode (1.04 s vs 0.97 s), zstd level -3
  (81% larger blobs), numpy-first Int64 construction (4.06 s vs 2.55 s,
  parity-breaking), merged-dict assembly + slicing (2-3x faster build but
  session-wide dtype inference breaks per-lap dtype parity 1452/1452 —
  confirms F3's mechanism), pool-sizing alignment (auto-sized, no mismatch).

### Added

- Contract tests for the kept changes (`tests/unit/test_n_series_contracts.py`):
  cold-batch frame materialization (+ cache-disable gating), prefetch
  persistence and cache-first behavior, and bulk-write round-trip parity.
- N-series measurement harnesses and reports (`tools/lindos_*.py`,
  `.agents/perf-lindos/`).

### Summary — L-series performance work

L-series performance work (10 hypotheses, all run; full log in
`.agents/perf-sestos/RESULTS.md`). The headline: **warm loads of a cached
full-telemetry session drop 5.13 → ~1.0 s on the telemetry phase (−80%)** via
a materialized-frame cache tier, everywhere-missing payloads stop being
re-probed on every load (negative-result cache), and repeat
`fetch_all_laps_telemetry()` calls in one process become ~free (frame-memo
reuse). Cold loads are unchanged to faster (no cold-path work was added; the
kept set was measured at parity-or-better in a final interleaved A/B,
median −24% on that suite with no pair regressing materially). Seven of the
ten hypotheses were rejected by measurement and are documented with their
numbers; one (L8, overlapped cache-write flush) was fully implemented,
measured at +23%/+36% cold regression on 1-core vantages, and reverted.

### Added

- **Materialized telemetry-frame tier** (`cache.py`, `core.py`): the first
  warm `fetch_all_laps_telemetry()` after a cold load persists the assembled
  per-(driver, lap) DataFrames (pickle protocol 5 + zstd-1, new
  `telemetry_frames` table); subsequent warm loads batch-read the frames
  directly, skipping the payload blob read, decompress, orjson parse and
  pandas frame construction (measured 4.9 → ~1.0 s read+assembly; live
  parity 0/1452 mismatched on columns, dtypes and values). Frames materialize
  only from cache/memo-sourced payloads — network fresh-result frames never
  write the tier, so cold fetches pay zero. Corrupt rows degrade to tier
  misses; `invalidate("telemetry")`/`clear()` drop the tier.
- **Negative-result cache for everywhere-missing payloads** (`cache.py`,
  `async_fetch.py`, `config.py`): when every CDN refuses a payload with a 4xx
  (the K9 all-missing verdict), the key is remembered in a new
  `missing_payloads` table (TTL `missing_payloads_ttl_days`, default 7, env
  `TIF1_MISSING_PAYLOADS_TTL_DAYS`); later loads raise `DataNotFoundError`
  without re-walking the CDN chain. Verdicts are dropped on successful
  writes and by `invalidate()`/`clear()`, and discards are no-ops when no
  verdict exists (no per-fetch SQL on the cold path — the first A/B caught a
  DELETE-per-fetch version of this at +23% cold and it was fixed). Measured
  on the Monaco set: warm telemetry −17.8% (3/3 paired runs), 9 → 0 HTTP
  GETs for the 3 everywhere-missing files.

### Changed

- **`fetch_all_laps_telemetry_async` reuses memoized frames** (`core.py`):
  frames built by a previous call (or by the per-lap telemetry path) are
  returned as-is instead of re-reading and re-assembling every payload.
  Repeat calls in one process: 2.90 → 0.13 s (22.6x; ~5 ms once the frame
  tier is warm).

### Rejected by measurement (documented in `.agents/perf-sestos/RESULTS.md`)

- L1 brotli transfer encoding (gzip already negotiated; 2% byte delta),
  L4 multi-connection batch fetch (parity probe: 1×22 vs 2×22/2×11/4×6),
  L5 SQLite `mmap_size` (12 ms of a 5.13 s phase), L6 zstd trained
  dictionary (ratio 0.234 → 0.232 on 50–120 KB blobs), L8 overlapped
  deferred-write flush (+23%/+36% cold, 10/10 interleaved pairs — reverted),
  L9 skipping per-request JSON-tier probes (the default cold path already
  does zero probes), L10 trimming the `get_session` schedule path (5.2 ms;
  the 0.42–0.65 s is the already-adjudicated lazy-import chain).

### Summary — K-series performance work

K-series performance work (10 hypotheses, all run; full log in
`.agents/perf-kalchedon/RESULTS.md`). The headline: **the persistent cache now
works under default config** — cold sessions persist what they fetch, so the
second load of a cached full-telemetry session drops **9.31 → 4.68 s (−50%)**
instead of re-downloading ~84 MB every process. Cold loads are unchanged to
slightly faster, the missing-file retry storm is structurally eliminated, and
bulk cache writes are ~4.5x faster via a zstd storage codec and single-tier
telemetry writes.

### Changed

- **Ultra-cold mode now skips cache READS only — writes always persist**
  (`core.py`): every fetch path (laps wave, telemetry bulk wave, fastest-lap
  single fetches, session tables) keeps `write_cache` gated on `enable_cache`
  alone. Previously `ultra_cold_start=True` (the default) made
  `write_cache = enable_cache and not ultra_cold`, so a default-config user's
  persistent cache stayed empty forever and every process re-downloaded the
  full session. A session that starts cold keeps its cold-start semantics for
  the rest of the process (`_mark_session_cache_populated` only upgrades an
  unresolved probe); the *next* process detects the warm cache and reads it.
- **Telemetry payloads persist to a single cache tier** (`async_fetch.py`,
  `core.py`): the bulk fetch path no longer writes both the JSON tier and the
  telemetry table per payload. Telemetry-path writes route to
  `cache.set_telemetry`; every reader (batch and single-ref) consults the
  telemetry table first, so the JSON-tier copy was duplicate dumps/compress/
  insert work. Measured on the 1452-payload Monaco set: 3.08 → 0.69 s per
  full-session write pass.
- **Bulk telemetry writes are deferred out of the fetch slots**
  (`async_fetch.py`): `fetch_multiple_async(defer_telemetry_writes=True)`
  collects telemetry writes during the batch and flushes them in one serial
  executor job after the gather. Writing inside the 22-wide semaphore slots
  serialized slots on the SQLite lock and added seconds to cold batches on
  small machines; the post-gather flush costs 0.78 s (measured) and the cold
  interleaved A/B ended at median −9.2%.
- **SQLite-tier compression is zstd-1** (`cache.py`, new `zstandard`
  dependency): compresses ~4.5x faster and decompresses ~2x faster than the
  previous zlib-3 at a slightly better ratio (23.4% vs 26.2% of original
  size, measured on 1452 real telemetry payloads). Legacy zlib rows stay
  readable via magic-byte detection; small rows keep the plain-TEXT shape.
- **A 404 mixed with other 4xx refusals on every CDN is now
  `DataNotFoundError`** (`cdn.py`): previously a 403 from one CDN (e.g. a
  jsDelivr rate-limit window) plus 404s from every mirror produced a
  retryable `NetworkError`, burning 3 attempts × backoff sleeps (~3.2 s of
  pure sleep per everywhere-missing file, observed live). All-4xx exhaustion
  now resolves in one raced CDN round (~0.2 s). 5xx and transport errors keep
  the retryable `NetworkError` contract.
- Cache writes serialize via `json_dumps_bytes` (orjson emits bytes; the old
  path paid a str round-trip on every write).

### Performance

- Warm second load of a cached full-telemetry session: **9.31 → 4.68 s (−50%)**
  under default config (two-run protocol, fresh process per run).
- Cold full-telemetry loads: median −9.2% in the final interleaved A/B suite
  (4/5 pairs faster than control), including the deferred write-back cost.
- Bulk cache write pass: **3.08 → 0.69 s (4.47x)** (zstd + single tier).
- Everywhere-missing files: retry storm **3.15 s / 9 CDN probes → 0.18 s /
  3 probes (−94%)** (deterministic harness); the live win is situational on
  whether the primary CDN is rate-limiting, but the storm is now impossible.

### Rejected by measurement (documented in `.agents/perf-kalchedon/RESULTS.md`)

- K2 parallel warm-read decode (0.85–1.01x on this 1-core vantage; the
  GIL-release mechanism needs ≥2 cores — not shipped since it regresses
  single-core machines).
- K5 batched `executemany` writes and `synchronous=OFF` (no gain; WAL + commit
  interval already absorb the fsync).
- K6 numpy-first typed-frame construction (0.83x with perfect parity —
  pandas' internal sanitize is already at the floor).
- K7 pre-typed laps assembly (the whole laps assembly is 79 ms, ~0.8% of the
  pipeline).
- K8 msgpack SQLite-tier encoding (decode 1.1x, encode slower, stored size
  bigger than json+zstd).
- K10 uvloop (below the noise floor; matches the F-series finding that loop
  scheduling is ~9 ms per 1452-request batch).
- Rust/native extension: evaluated against the measured hot loops (pandas-C
  construction, orjson-C parse, zstd-C codec); the remaining Python slice is
  too small to pay the PyO3 boundary + build-backend costs. Evidence in
  RESULTS.md.

### Testing

- 1183 unit tests pass; 3 updated to the new ultra-cold write-back contract
  (ultra-cold now asserts writes-persist + reads-skipped); 11 new contract
  tests in `tests/unit/test_k_series_contracts.py` covering the all-4xx CDN
  semantics, the zstd codec round-trip + legacy-row readability, and the
  deferred single-tier telemetry write path.

## [0.8.0] - 2026-09-10

### Summary

`0.8.0` is a performance release. Cold starts drop polars until the polars backend
is used (`get_session` median 0.733 → 0.447 s, −39%), default ultra-cold mode now
reads a warm cache instead of re-downloading (full-telemetry 10.72 → 6.29 s, −41%),
and the default CDN chain is jsDelivr → Hugging Face buckets → StaticDelivr after a
live bake-off. Fetch-path decode/parse moves off the event loop, telemetry frames are
pre-typed, and a 404 races the remaining CDNs instead of walking them serially. No
public APIs are removed. Custom `cdns` configurations are unchanged.

### Changed

- **Default CDN order is jsDelivr-first** (`config.py`):
  jsDelivr (`https://cdn.jsdelivr.net/gh/TracingInsights`) → Hugging Face buckets
  (`https://huggingface.co/buckets/tracinginsights`) → StaticDelivr
  (`https://cdn.staticdelivr.com/gh/TracingInsights`). A live bake-off superseded the
  earlier StaticDelivr-first trial (E9). Existing custom `cdns` values are unaffected.
- **A 404 falls through to the next CDN** — `DataNotFoundError` is raised only when
  every source returns 404, so a stale primary mirror no longer hides files that a
  fallback still has. After a 404, the async fallback races the remaining sources
  instead of walking them serially (missing-file walk median 0.09 → 0.05 s).
- **`ultra_cold_start=True` (the default) skips cache reads only when the session is
  not already cached.** Warm-cache full-telemetry loads 10.72 → 6.29 s (−41%).
  Explicit `ultra_cold=` arguments are unchanged.
- **Prefetch fetches only the session tables that were requested** — laps-only flows
  no longer pull weather/rcm payloads alongside `drivers.json`. `load()`'s "fetches
  only the data that is required" promise now holds for the prefetch wave.
- **Polars is imported lazily** — `helpers`, `models`, `types`, and
  `backend_conversion` no longer import polars at module load (~260 ms of every cold
  start for pandas-default users). It loads via `_ensure_polars_available()` on first
  polars-backend use.
- Default `keepalive_max_requests` 1000 → 10000 (live CDN: 4/5 interleaved pairs
  faster; aggregate median −16.5%).
- Runtime dependency lower bound raised: rapidfuzz 3.14.6.

### Performance

- Full experiment logs: `.agents/perf-monaco/RESULTS.md` (E-series),
  `.agents/perf-fab5/RESULTS.md` (F- and G-series); live harnesses under `tools/`
  (`monaco_ab_suite.py`, `monaco_cdn_bakeoff.py`, `cdn_parallel_experiment.py`).
- Accepted (E-series, live 2026 Monaco GP Race): decode/parse off the event loop
  (E1: median −12.6%, mean −51%; eliminated a 60–72 s collapse mode), pre-typed
  telemetry frames (E2: assembly 4.14 → 2.36 s), `retry_jitter_max` validation fix
  (E4), vectorized (driver, lap) ref extraction (E6), `keepalive_max_requests` 10000
  (E10), jsDelivr-first CDN order (bake-off; supersedes E9).
- Accepted (G-series): lazy polars import (G1), race remaining CDNs after a 404 (G2),
  prefetch only requested session tables (G4), ultra-cold warm-cache auto-detect (G5).
- Rejected by measurement (F-series, ten hypotheses): CDN host sharding, overlapped
  assembly, merged-dict slice views, timedelta micro-casts, fused get+decode hops,
  tracking/import/pool micros, minified jsDelivr payloads, and thread-parallel
  frame assembly. None beat the E-series stack.

### Fixed

- Default `retry_jitter_max` failed its own validation and silently inflated retry
  delay (0 → 1.0 s) while emitting a per-request warning on the fetch loop.
- `_RaceFailureError.error` is typed `Exception` (only `Exception` is ever wrapped),
  keeping `ty` diagnostics at or below the pre-existing main baseline.

### Documentation

- CDN chain, config defaults, and architecture diagrams updated to jsDelivr-first
  with Hugging Face fallback and StaticDelivr last resort.
- Restored E-series measurement artifacts under `.agents/perf-monaco/` that merge
  931e9f3 dropped when PR #66 superseded that stack.

### Known Issues

- None reported at release time.

## [0.7.0] - 2026-09-04

### Summary

`0.7.0` is a performance release. Fetch-path validation no longer uses pydantic
(validation-on full-session fetch 1971 → 1024 ms, 1.9x), the SQLite cache compresses
large blobs and keeps parsed objects for warm hits, and the default concurrency and
cache-commit settings are retuned from live CDN measurements. The docs site is converted
to Simplified Technical English, with five new API reference pages. No public APIs are
removed; `Cache.set_raw()` is added. Default-config users (validation off) still see the
cache and concurrency gains.

### Added

- **`Cache.set_raw()`** — persist already-serialized JSON blobs without re-encoding.
  The async fetch pipeline uses this when validation left the payload byte-identical
  to the HTTP body. Blobs ≥4 KB are zlib-compressed at the SQLite boundary.

### Changed

- **Pydantic removed from the fetch-pipeline validation path** (`validation.py`): the four
  `validate_*_data` functions now use a pydantic-free fast path (alias→field mapping, default
  filling, length checks) with identical output on well-typed payloads. Validation-enabled
  full-session fetch: 1971 → 1024 ms (1.9x). The pydantic entry points (`validate_laps`,
  `validate_telemetry`, ...) remain for API compatibility. Note: no per-element type coercion
  or driver-code pattern check on the fetch path.
- **Null-like normalization deferred to DataFrame construction** for laps/telemetry/rcm on the
  fetch path (`normalize=False`), relying on the existing vectorized
  `helpers._replace_null_like_strings`; weather keeps inline coercion. Per-payload validation
  cost drops to ~0.006-0.008 ms.
- **SQLite cache tier compresses large blobs** (zlib-3, ≥4 KB; telemetry payloads compress ~12x).
  Default-config cold write-cache full-session load: 3750 → ~2100 ms (-44%). Legacy TEXT rows
  still read; corrupt rows degrade to a cache miss. Caches written by 0.7.0 are not readable
  by 0.6.x — clear the cache if you downgrade.
- **Parsed-object read-through tiers** in `Cache` (128 json / 256 telemetry entries): repeat warm
  hits skip orjson parsing (190 µs → 0.4 µs); writes drop parsed entries so stale objects cannot
  survive.
- Default `max_concurrent_requests` 20 → 22 (live CDN, 2025 Abu Dhabi Practice 1, 96 telemetry
  payloads: 22 beat 20 in 5/5 interleaved pairs, median -26%; cap 64 was slower than 20).
- Default `cache_commit_interval` 25 → 100 (~20% of the compressed-write path; `close()` still
  force-commits).
- Runtime dependency lower bounds raised: niquests 3.21.1, pydantic 2.13.5, typer 0.27.2,
  typing-extensions 4.16.0, matplotlib 3.11.1, scipy 1.17.1, orjson 3.12.0, polars 1.44.1.

### Performance

- Full experiment log with baselines, methodology, and per-hypothesis verdicts:
  `.agents/perf/RESULTS.md`; reproducible offline harness: `tools/perf_validation_experiment.py`.
- Accepted: pydantic removal (H1), deferred normalization (H2), raw+compressed cache writes (H5),
  parsed-object tiers (H4), commit interval (H6), concurrency cap (H7: 22 after live CDN).
- Rejected by measurement: event-loop parse offload (H3, GIL-bound), lazy pandas import (H10),
  polars-first assembly for the pandas backend (H9 — the merged-dict batch path is already at its
  dependency-free floor; faster routes require pyarrow or change dtype inference semantics).
- Already implemented previously: single-shot laps/telemetry frame assembly (H8).

### Documentation

- Converted the docs site (guides, concepts, tutorials, API reference) to Simplified Technical
  English (ASD-STE100).
- Added API reference pages for `session`, `payload-loader`, `schedule-schema`,
  `plotting-constants`, and `assets`.
- Corrected factual drift from 0.6.0 (three-source CDN chain, config defaults, Hugging Face
  minification rule).

### Known Issues

- None reported at release time.

## [0.6.0] - 2026-08-26

### Summary

`0.6.0` adds Hugging Face storage buckets as a third, last-resort CDN source. The fetch chain is now StaticDelivr (primary) → jsDelivr (fallback) → Hugging Face buckets (backup), keeping data available when the GitHub-backed CDNs are down. No public API changes.

### Added

- **Hugging Face buckets as backup CDN source** — `cdn.py` now defaults to three sources in priority order:
  - StaticDelivr: `https://cdn.staticdelivr.com/gh/TracingInsights` (`{year}/main/{gp}/{session}/{path}`)
  - jsDelivr: `https://cdn.jsdelivr.net/gh/TracingInsights` (`{year}@main/{gp}/{session}/{path}`)
  - Hugging Face: `https://huggingface.co/buckets/tracinginsights` (`{year}/resolve/{gp}/{session}/{path}`)
- The Hugging Face buckets mirror the TracingInsights GitHub data repos (`TracingInsights/{year}`, seasons 2018–2026) with an identical directory layout, served through the `/resolve/` endpoint with no branch segment.
- `CDNManager._name_for_url()` identifies Hugging Face URLs as `"HuggingFace"`; both sync (`try_sources`) and async (`try_sources_async`) fallback loops transparently include it.

### Changed

- Default `cdns` config value now lists all three sources; existing custom `cdns` configurations are unaffected.
- Minification (`cdn_use_minification`) is never applied to Hugging Face sources — they do not serve `.min.json`, and a 404 would otherwise abort the fallback chain as a fatal `DataNotFoundError`.
- Documentation updated across `README.md`, `AGENTS.md`, and the docs site (`cdn.mdx`, `http.mdx`, `data-flow.mdx`, `utilities.mdx`, `environment-variables.mdx`) to describe the three-source chain.

### Fixed

- Repaired duplicate node IDs and conflicting style declarations in the data-flow architecture mermaid diagram (`docs/concepts/data-flow.mdx`).

### Known Issues

- None reported at release time.

## [0.5.1] - 2026-08-12

### Summary

`0.5.1` is a patch release focused on documentation cleanup, test reliability, and removing unused repository scripts. No public API changes.

### Changed

- Streamlined documentation by removing references to the machine-readable `data_dictionary.json` and `telemetry.schema.json` from `README.md`, `DATA_REFERENCE.md`, and `docs/reference/data-reference.mdx`.
- Minor docs/branding consistency updates (`docs/docs.json`, guides, and related pages).
- Added `from __future__ import annotations` in `cache.py` for clearer type hints.

### Fixed

- Hardened `test_models_module_does_not_import_core` so it dynamically reloads `tif1.models` without polluting `sys.modules` for later tests (prevents import-cycle false positives/side effects).
- Updated cold-path fastest-lap tests to use current event/session naming (`Pre-Season Testing` / `Practice 1`).

### Removed

- Deleted unused repository scripts and baselines (`scripts/baseline_benchmark.py`, `scripts/download_assets.sh`, `scripts/extract_telemetry_sectors.py`, and related baseline results). These were not part of the published package.

### Known Issues

- None reported at release time.

## [0.5.0] - 2026-08-11

### Summary

`0.5.0` ships bundled chart assets (car/tyre images, brand fonts), two new curated plot styles (`default-light`, `default-dark`), and a new `plot_race_launch_ratings` native chart. It also includes the `tif1.assets` module for offline asset loading and rendering, plus pixel-perfect matching with the reference v2 analysis scripts.

### Added

- **New plot styles: `default-light` and `default-dark`** — `tif1.plotting` now ships two curated matplotlib themes that work with every chart's `color_scheme` parameter and with `setup_mpl(color_scheme=...)`:
  - `default-light` is extracted from the v2 `Fastest_Lap.py` analysis script (lightblue background, black text, Tenada heading font, hidden top/right spines, 32pt ticks, 20×20 @ 300 dpi, bar height 0.1, tyre/car image offsets).
  - `default-dark` is extracted from the v2 `Race_Launch_Performance_Ratings.py` script (TracingInsights dark brand `#011627` background, lime text, black grid, white bar/ytick labels, Coolvetica/Azonix/GreatVibes fonts, subplot margins, `car_threshold: 2.5`).
  - New `get_plot_style(name)` registry returns deep copies of the style configs, and `get_team_code()` / `team_code_mapping()` / `team_color_mapping()` expose session/year-aware team lookups.
- **Bundled chart assets** — new `tif1.assets` module ships car images for every team 2018–2026 (91 PNGs), 7 tyre compound images, and 4 brand fonts directly inside the package (no network access at plot time). Includes cached `load_car_image` / `load_tyre_image` loaders, path/discovery helpers, and matplotlib `AnnotationBbox` helpers (`add_car_images`, `add_tyre_images`, `add_car_at_position`, `add_tyre_at_position`) that mirror the v2 scripts' rendering.
- **New native chart: `plot_race_launch_ratings`** — added to `tif1.charts` (and exported at the top level). It loads each driver's first race lap, interpolates 50/100/150/200 km/h crossing times from telemetry, derives 0–10 launch ratings, and renders a barh chart in the `default-dark` style with bundled car/tyre images and the 2.5 rating threshold.

### Changed

- **Performance: telemetry sector extraction** — new `extract_telemetry_sectors` utility splits telemetry into per-sector snapshots for downstream analysis.
- **Documentation restructuring** — reorganized docs navigation, removed deprecated `io-pipeline` and `lap-operations` reference pages, added `data-reference` and `why-tif1` pages.
- **Cache directory handling** — improved cache path resolution and configuration.

### Fixed

- **`plot_race_launch_ratings` output now matches the v2 script pixel-for-pixel** (verified against `F1-analysis/v2/Race_Launch_Performance_Ratings.py` on 2024 Bahrain: identical 6000×6000 renders, mean-abs pixel diff 0.000):
  - The chart now exports the full canvas exactly like the v2 script (no `tight_layout`, no `bbox_inches` cropping; the manual `subplots_adjust` margins are preserved). `charts._common.finalize_figure` gained optional `tight_layout`/`bbox_inches` kwargs (defaults unchanged for all other charts).
  - Crossing times are rounded to 3 decimals **before** the 0-10 rating normalization (matching the script's `.round(3)` on each `Time_XX` column), removing 0.01 rating discrepancies.
  - The driver sort replicates the script's exact pipeline (`groupby("Driver").first()` + pandas' default unstable `sort_values`), preserving its tie-breaking for identical ratings.

## [0.4.0] - 2026-08-08

### Summary

`0.4.0` is a major release that introduces the native `tif1.charts` module (21 one-call chart functions), migrates the library to **pandas 3.0**, and raises the minimum supported Python version to **3.11**. The pandas 3.0 migration fixes a critical timedelta unit-resolution bug and removes deprecated `pd.concat(copy=False)` calls ahead of pandas 4.

### Breaking Changes

- **Python 3.10 support dropped** — minimum supported version is now **3.11** (required by pandas 3.0).
- **pandas 2.x support dropped** — `tif1` now requires **pandas >=3.0.5,<4**.

### Migration Notes

- Upgrade to Python 3.11+ and pandas 3.0+ before installing `tif1` 0.4.0.
- No public API changes; the pandas 3.0 `str`-dtype default is transparent to users (telemetry `Driver` is kept as `object` for FastF1 compatibility).

### Added

- **Native chart functions** — new `tif1.charts` module with 21 one-call chart functions, also re-exported at the top level (`tif1.plot_<name>`):
  - Track maps: `plot_track_speed_map`, `plot_track_throttle_map`, `plot_track_brake_zones`, `plot_track_acceleration_map`, `plot_gear_shifts`, `plot_multi_driver_speed_comparison`
  - Telemetry: `plot_speed_traces`, `plot_annotated_speed_trace`, `plot_telemetry_comparison`, `plot_gg_diagram`
  - Lap times: `plot_driver_laptimes`, `plot_laptimes_distribution`, `plot_laptime_heatmap`, `plot_qualifying_grid`, `plot_lap_delta`, `plot_position_changes`, `plot_track_temperature`
  - Performance: `plot_downforce_levels`, `plot_throttle_distance`, `plot_tire_degradation`
  - Top speeds: `plot_top_speeds`
- Every chart takes `(year, event, session)`, loads its own data, and returns a `(fig, ax)` pair (`plot_telemetry_comparison` returns the 4 panel axes as a numpy array). Optional `save_path`, `dpi`, `figsize`, `facecolor`, `color_scheme`, `enable_cache`, and `lib` parameters are shared across all charts, plus the shared filters `drivers`, `teams`, `n_drivers`, `laps`, `laptime_cutoff`, `include_deleted`, and `include_pit_laps`.
- Added `scipy>=1.14,<2` dependency (used by `plot_gg_diagram`'s convex-hull performance envelope).

### Changed

- Rewrote all 21 `docs/assets/generate_*.py` scripts as thin wrappers around the native chart functions; `generate_all_charts.py` now calls `tif1.charts` as the canonical "regenerate all docs charts" entrypoint (standalone-owned outputs like `race_position_changes.png` are no longer overwritten).
- Updated 21 tutorials to teach the native chart API and added the `api-reference/charts` reference page.
- **Performance: telemetry batch assembly via merged dict-of-lists** — `get_fastest_laps_tels` / `get_fastest_laps_tels_async` now assemble telemetry from raw payloads into a single DataFrame (one dtype-conversion pass) instead of building a per-driver DataFrame and calling `pd.concat`. Measured **~2.5-3x faster** on pandas 3.0 (and ~2x faster on pandas 2.3) with byte-identical output, mitigating the pandas 3.0 regression in repeated per-frame `pd.to_timedelta`/`astype` machinery. The polars backend keeps the per-driver `_create_telemetry_df` + `pl.concat` path unchanged.
- **Performance: `Laps.telemetry` / `get_car_data` via merged dict-of-lists** — these accessors now collect raw telemetry payloads through a new `Session._get_telemetry_payload_for_ref` (payload-only version of the per-lap source chain) and build a single merged DataFrame instead of building a per-lap DataFrame and calling `pd.concat`. Measured **~2x faster** with identical output. Falls back to the legacy per-lap path for stand-in session objects and on malformed payloads; the polars backend is unaffected.

### Fixed

- Fixed crash on 2026 lap data where missing values are encoded as the string `"None"` (e.g. in the `Deleted` column): `_apply_laps_dtypes` now normalizes null-like string sentinels before dtype coercion, so `astype("boolean")` no longer raises and bool columns no longer turn missing values into `True`.
- Applied the same null-like string normalization to the polars laps path: `_create_lap_df` normalizes payload lists before DataFrame construction (so polars infers proper `Boolean`/`Float` dtypes instead of stringifying mixed columns), and `_process_lap_df` normalizes String columns as a defensive boundary.
- Non-strict `validate_lap_data` now returns the normalized payload (null-like strings converted to `None`) even when validation fails, instead of returning raw un-normalized data.

### Dependencies

Updated all dependencies to their latest stable versions, one at a time, with breaking changes resolved:

- **`pandas` 2.3.3 → 3.0.5** (major) and **`pandas-stubs` 2.3.3.260113 → 3.0.5.260730**: migrated the codebase for pandas 3.0 breaking changes:
  - Timedelta unit inference (`timedelta64[us]` is now produced for sub-second inputs): all `is_timedelta64_ns_dtype` guards were widened to `is_timedelta64_dtype` and `_numeric_seconds_to_timedelta` no longer reinterprets already-timedelta columns, preventing a 1e6× lap-time scaling bug in `pick_fastest`/`slice_by_time`.
  - `str` dtype is now the default for string columns: telemetry `Driver` is explicitly kept as `object` dtype for FastF1 compatibility, matching the laps dtype contract.
- **Python minimum raised to 3.11** (required by pandas 3.0): updated `requires-python`, classifiers, CI matrix, ty config, and documentation.
- **Fastest-lap selection optimized**: `_select_fastest_laps` now uses `sort_values + drop_duplicates` instead of `groupby(...).idxmin()` — measurably faster on both pandas 2.x and 3.x (pandas 3.0 made `groupby.idxmin` ~2× slower), recovering most of the pandas-3.0 regression in `get_fastest_laps(by_driver=True)`.
- **Lap-table assembly optimized**: the pandas laps path now builds one DataFrame from a single merged dict-of-lists (`_merge_lap_payloads`) instead of concatenating per-driver frames with `pd.concat`, which regressed ~2× in pandas 3.0. Measured ~20× faster DataFrame assembly with identical output (verified against the concat path, including drivers with differing column sets).
- `typer` 0.26.7 → 0.27.1 (metavar help-printing change only)
- `niquests` 3.19.1 → 3.21.0
- `matplotlib` 3.10.9 → 3.11.1
- `polars` 1.41.2 → 1.43.2
- `typing-extensions` 4.15.0 → 4.16.0
- `pyarrow` 24.0.0 → 25.0.0
- `hypothesis` 6.155.3 → 6.165.2
- `prek` 0.4.5 → 0.4.12
- `ty` 0.0.49 → 0.0.69
- `ruff` 0.15.17 → 0.16.2 (new `PLR0917` rule added to the ignore list alongside the existing `PLR09xx` rules; fixed a `B033` duplicate set item in tests)
- Transitive upgrades via lockfile: `numpy` 2.3.5 → 2.5.1, plus `certifi`, `charset-normalizer`, `idna`, `click`, `requests`, `pygments`, `pillow`, and others.

## [0.3.1] - 2026-05-15

### Summary

`0.3.1` is a patch release containing CDN URL formatting fixes, CI workflow improvements, pre-commit hook infrastructure, documentation corrections, and dependency updates.

### Added

- Added release discussion workflow (`release-discussion.yml`) for automated release announcements.
- Added pre-commit hook configuration with git hooks for automated linting before commits.
- Added Mintlify documentation validation to pre-commit hooks.
- Added explicit read permissions to GitHub Actions workflow for improved security posture.
- Added benchmark test support for real cold-start CDN scenarios.

### Changed

- Updated CDN URL formatting in `cdn.py` to use `/branch/` path format for StaticDelivr compatibility.
- Improved pre-commit hook cross-platform compatibility and robustness.
- Updated `AGENTS.md` with corrected documentation references.
- Expanded benchmark test file with cold-start CDN test scenario.

### Fixed

- Fixed CDN references across documentation and code to correctly reflect StaticDelivr as primary CDN.
- Fixed documentation links and HTML entity encoding in code examples.
- Fixed trailing whitespace in environment variables documentation.
- Fixed Template Injection security issue in GitHub Actions workflow.

### Dependencies

Updated dependencies to latest stable versions:

- Updated `niquests` from `>=3.18.7,<4` to `>=3.18.8,<4`
- Updated `hypothesis` from `>=6.152.4,<7` to `>=6.152.7,<7`
- Updated `ty` from `>=0.0.34,<0.1.0` to `>=0.0.35,<0.1.0`

### Known Issues

- None reported at release time.

## [0.3.0] - 2026-05-08

### Summary

`0.3.0` introduces StaticDelivr as the primary CDN provider with improved fallback handling, enhanced API reference documentation, and updated F1 2026 schedule data. This release focuses on reliability improvements and better documentation coverage.

### Added

#### CDN Infrastructure
- **StaticDelivr as primary CDN** with jsDelivr as fallback for improved reliability and performance.
- Added configurable CDN sources via `cdns` configuration option.
- Added optional JSON minification support via `cdn_use_minification` configuration flag (20-40% file size reduction).
- Added intelligent CDN fallback system with automatic source switching on failures.
- Added per-source failure tracking with automatic disabling after 3 consecutive failures.
- Added `CDNSource` dataclass for structured CDN configuration.
- Added `CDNManager.try_sources()` method for robust multi-CDN fetching with fallback.

#### Documentation
- Expanded API reference documentation with comprehensive coverage of all modules.
- Added detailed documentation for CDN system, configuration options, and utilities.
- Updated tooling guidelines in `AGENTS.md` for improved developer experience.

#### Data
- Updated F1 2026 schedule with current calendar reflecting cancelled races.

### Changed

#### CDN System
- Migrated from jsDelivr-only to multi-CDN architecture with StaticDelivr as primary source.
- Enhanced URL formatting to support both jsDelivr and StaticDelivr URL patterns.
- Improved CDN source validation to reject invalid or unsupported URLs (e.g., raw.githubusercontent.com).
- Enhanced logging for CDN operations with detailed source tracking and failure reporting.

#### Code Quality
- Fixed pre-commit checks for CI pipeline.
- Improved code formatting and linting compliance.

### Fixed

- Fixed CDN fallback handling to properly propagate `DataNotFoundError` (404) without retrying other sources.
- Fixed CDN source naming to avoid duplicates when multiple sources are configured.
- Fixed JSON file path handling for minification support.

### Performance

- **Improved data fetching reliability** through multi-CDN architecture with automatic fallback.
- Optional JSON minification can reduce bandwidth usage by 20-40% for JSON payloads.
- Reduced latency through StaticDelivr's optimized edge network.

### Dependencies

Updated core dependencies to latest stable versions:

- Updated `pandas` from `>=2.2.0` to `>=2.3.3,<3` for improved performance and bug fixes
- Updated `niquests` from `>=3.10.2` to `>=3.18.7,<4` for enhanced HTTP/2 support
- Updated `pydantic` from `>=2.10.6` to `>=2.13.4,<3` for better validation performance
- Updated `typer` from `>=0.15.1` to `>=0.25.1,<0.26.0` for CLI improvements
- Updated `rich` from `>=13.9.4` to `>=15.0.0,<16` for enhanced terminal output
- Updated `matplotlib` from `>=3.9.4` to `>=3.10.9,<4` for plotting improvements
- Updated `seaborn` from `>=0.13.2` to `>=0.13.2,<1` (maintained)
- Updated `polars` from `>=1.36.0` to `>=1.40.1,<2` for better DataFrame performance
- Updated `orjson` from `>=3.10.14` to `>=3.11.9,<4` for faster JSON parsing
- Updated `rapidfuzz` from `>=3.10.2` to `>=3.14.5,<4` for improved fuzzy matching

Development dependencies:
- Updated `pytest` from `>=8.3.4` to `>=9.0.3,<10` for better test framework features
- Updated `pytest-xdist` from `>=3.6.1` to `>=3.8.0,<4` for parallel test execution
- Updated `pytest-cov` from `>=6.0.0` to `>=7.1.0,<8` for coverage reporting
- Updated `pytest-asyncio` from `>=0.25.2` to `>=1.3.0,<2` for async test support
- Updated `pytest-benchmark` from `>=5.1.0` to `>=5.2.3,<6` for performance benchmarking
- Updated `pytest-mock` from `>=3.14.0` to `>=3.15.1,<4` for mocking utilities
- Updated `pyarrow` from `>=18.1.0` to `>=24.0.0,<25` for Arrow format support
- Updated `ruff` from `>=0.8.6` to `>=0.15.12,<0.16.0` for linting and formatting
- Updated `ty` from `>=0.0.33` to `>=0.0.34,<0.1.0` for type checking
- Updated `prek` from `>=0.3.12` to `>=0.3.13,<0.4.0` for pre-commit hooks
- Updated `pandas-stubs` from `>=2.2.3.241009` to `>=2.3.3.260113,<3` for type stubs
- Updated `hypothesis` from `>=6.122.3` to `>=6.152.4,<7` for property-based testing

### Configuration

New configuration options in `.tif1rc` or environment variables:

```python
# CDN configuration
cdns = [
    "https://cdn.staticdelivr.com/gh/TracingInsights",
    "https://cdn.jsdelivr.net/gh/TracingInsights"
]

# Enable JSON minification (optional, reduces file sizes)
cdn_use_minification = false
```

### Migration Notes

- No breaking changes to public APIs.
- Existing code will automatically use the new multi-CDN system.
- Users experiencing CDN issues can configure custom CDN sources via the `cdns` configuration option.
- The CDN system now automatically falls back to jsDelivr if StaticDelivr is unavailable.

### Compatibility Notes

- All public APIs remain backward compatible with v0.2.0.
- CDN configuration is optional; defaults provide optimal reliability.
- Minification is disabled by default to maintain compatibility with existing workflows.

### Known Issues

- None reported at release time.

## [0.2.0] - 2026-04-17

### Summary

`0.2.0` is a major feature release that introduces significant improvements to data fetching, plotting capabilities, session handling, and overall performance. This release includes enhanced FastF1 compatibility, optimized data workflows, comprehensive plotting APIs, and improved qualifying session support.

### Added

#### Plotting And Visualization
- **Full FastF1-compatible plotting API** with season-aware team and compound colors (2018–2026).
- Added `get_team_color()`, `get_driver_style()`, and `add_sorted_driver_legend()` helper functions for consistent chart styling.
- Integrated `timple` library for professional F1-style timedelta axis formatting.
- Added fuzzy matching for driver and team lookups with warnings for near-matches.
- Added comprehensive plotting constants including 2026 team colors (Audi, Cadillac).
- Added `plotting_colors_demo.py` example demonstrating the new plotting capabilities.

#### Data Fetching And Performance
- **Session-level laptime fetching** using `session_laptimes.json` instead of per-driver `driver/laptimes.json` files, significantly reducing network requests.
- Added synthetic payload generation to derive driver-specific laptime data from session-wide payloads.
- Added `prefer_session_payload` option for controlling laptime data source preference.
- Added baseline benchmarking script (`baseline_benchmark.py`) with detailed timing and profiling for async and sync workflows.
- Added benchmark results tracking in `baseline_results.json`.

#### Session Handling
- Added qualifying session handling with proper `QualifyingSession` type support.
- Enhanced `split_qualifying_sessions()` method to correctly handle qualifying session markers.
- Added validation for qualifying session types in async fetch and validation modules.

#### Testing And Quality
- Added `conftest.py` with shared pytest fixtures for global state reset between tests.
- Added fixtures to prevent cross-test contamination by resetting async session, CDN manager, and circuit breaker state.
- Added comprehensive unit tests for qualifying session handling, session laptime fetching, and plotting APIs.
- Added property-based tests using Hypothesis for improved test coverage.

#### Documentation
- Added comprehensive tutorial documentation with charts and examples for telemetry visualization.
- Added documentation deployment workflows and CI configuration.
- Added `TOOLS.md` with CLI tools documentation.
- Deployed versioned documentation for v0.1.0 and v0.2.0.

### Changed

#### Core Improvements
- **Migrated from `nest-asyncio` to `nest-asyncio2`** for improved async handling in Jupyter environments.
- Enhanced ultra-cold mode handling to avoid unnecessary refetches when data is already cached.
- Improved error handling in `_validate_json_payload()` to skip validation for patched CDN payloads.
- Added `_get_callable_code()` and `_is_patched_callable()` functions for better callable validation.
- Introduced `_build_session_laptime_payload_from_driver_payloads()` to combine driver payloads into session-wide payloads.
- Updated session data fetching logic to handle `prefer_session_payload` option while maintaining backward compatibility.

#### Data And Schedules
- Updated 2026 F1 schedule to remove cancelled races and reflect current calendar.
- Fixed telemetry functions including `DriverAhead`, `DistanceToDriverAhead`, and related calculations.

#### Code Quality
- Applied code formatting and type hinting improvements across the codebase for enhanced maintainability.
- Improved linting compliance with stricter Ruff rules.
- Removed hardcoded user agent string from `config.py` and HTTP session management.

#### Documentation
- Updated all documentation examples and chart generation scripts to use the new session-backed color API.
- Fixed markdown formatting and broken documentation links across all documentation files.
- Streamlined documentation navigation by removing advanced guides section.
- Updated API reference documentation with improved formatting and terminology.

### Fixed

- Fixed ultra-cold mode to avoid unnecessary refetches when session data is already available.
- Fixed qualifying session splitting logic to correctly identify and handle qualifying markers.
- Fixed telemetry calculations for driver-ahead and distance-to-driver-ahead metrics.
- Fixed validation logic to properly handle synthetic and patched payloads.
- Removed unnecessary path checks for `session_laptimes.json` in telemetry cache lookup tests.

### Build And Packaging

- Standardized the published PyPI package name as `tif1`.
- Added explicit Hatch wheel package configuration for `src/tif1`, improving build clarity and package discovery.
- Updated installation guidance so the canonical command is `pip install tif1`.
- Updated repository metadata, project links, and release-facing configuration to reference `tif1` consistently.
- Refreshed lockfile and package metadata to match current distribution naming.

### Dependencies

- Added `timple==0.1.8` for timedelta plotting support.
- Updated to `nest-asyncio2>=1.7.2,<2` (migrated from `nest-asyncio`).
- Updated `pytest` to `>=9.0.2,<10`.
- Updated `pillow` in the uv group.
- Updated `actions/github-script` from 7 to 8 in CI workflows.
- Updated `actions/checkout` from 4 to 6 in CI workflows.
- Updated `actions/upload-artifact` from 4 to 7 in CI workflows.
- Updated `actions/download-artifact` from 4 to 8 in CI workflows.
- Updated `astral-sh/setup-uv` from 5 to 7 in CI workflows.
- Bumped 16 packages in the python-dependencies group via Dependabot.

### Performance

- **Significant performance improvement** through session-level laptime fetching, reducing the number of HTTP requests from N (per driver) to 1 (per session).
- Optimized async and sync workflows with measurable improvements documented in benchmark results.
- Improved cache efficiency by storing session-wide data and deriving driver-specific views on demand.

### Migration Notes

- The supported package name for this release is `tif1`.
- The supported import path remains `import tif1`.
- The supported CLI command remains `tif1`.
- Some intermediate repository history referenced `tifone` during post-`0.1.0` packaging work. For `0.2.0` and later, treat `tif1` as the canonical package name.
- Code using the old `nest-asyncio` package will automatically use `nest-asyncio2` after upgrading.
- Existing plotting code will continue to work, but new season-aware color APIs are recommended for better accuracy.

### Compatibility Notes

- **Breaking Change**: The internal data fetching mechanism now prefers `session_laptimes.json` over per-driver laptime files. This is transparent to most users but may affect custom cache implementations.
- The `prefer_session_payload` option allows reverting to legacy behavior if needed.
- All public APIs remain backward compatible with v0.1.0.
- Qualifying session handling is now more robust and correctly identifies qualifying markers.

### Known Issues

- None reported at release time.

## [0.1.0] - 2026-02-13

### Summary

`0.1.0` is the initial public release of `tif1`, a high-performance Formula 1 timing-data library built around fast access, structured domain models, and a focused API for session, lap, driver, and telemetry analysis.

### Added

- Initial PyPI release of `tif1` for Python 3.10 and newer.
- Core Formula 1 data access APIs centered around sessions, drivers, laps, and telemetry.
- Public convenience entry points including `get_events`, `get_sessions`, `get_session`, `get_event`, `get_event_by_name`, `get_event_by_round`, and `get_event_schedule`.
- Lazy top-level exports in `tif1.__init__`, exposing the primary public API without forcing all heavy modules to import eagerly.
- Structured domain objects and data access patterns for working with event schedules, session data, driver views, lap views, and telemetry datasets.

### Data Access And Performance

- Direct access to TracingInsights timing data covering the 2018-current seasons.
- CDN-backed fetching via jsDelivr for remote session data retrieval.
- Asynchronous and parallel fetching designed to reduce cold-load latency for session and telemetry access.
- SQLite-backed local caching to avoid repeated downloads and accelerate repeated analysis workflows.
- HTTP networking based on `niquests`, aligned with the project's performance-focused design goals.
- Fast-path workflows for retrieving fastest laps and telemetry for individual drivers or full sessions.
- On-demand loading behavior so users can work with the parts of a session they need instead of always loading everything up front.

### Analysis Features

- Access to lap times, sector times, tire compounds, stint-related data, and telemetry channels such as speed, throttle, brake, RPM, gear, and DRS.
- Access to position, acceleration, distance, and related derived session datasets used in race and lap analysis workflows.
- Support for standard Formula 1 session types including practice sessions, qualifying, sprint formats, and races.
- Optional backend flexibility with pandas as the primary backend and polars support for users who prefer an alternate DataFrame engine.

### Reliability And Developer Experience

- Built-in retry and circuit-breaker infrastructure for more resilient network access.
- Custom exception hierarchy rooted at `TIF1Error`, including dedicated errors for missing data, invalid data, cache failures, network problems, and unloaded-session access.
- Configuration support for runtime behavior through the project's config layer and user configuration file support.
- Optional validation capabilities powered by Pydantic.
- JSON parsing based on `orjson` for faster serialization and parsing paths.
- Comprehensive type hints to improve IDE support and typed usage.
- Jupyter-friendly usage patterns, including async-friendly dependencies for notebook environments.

### CLI

- Initial `tif1` CLI with commands for listing events, listing sessions, inspecting drivers, viewing fastest laps, checking cache information, clearing cache, printing version information, and running debug-oriented session loads.
- Rich-formatted terminal output for common exploration tasks.

### Documentation

- Initial public documentation and project metadata for the first release.
- Versioned documentation deployment for `0.1.0`.

### Notes

- `0.1.0` established the public API and packaging baseline for the project.
- Later releases may refine packaging, naming, documentation, and release automation without changing the core goal of providing fast, canonical Formula 1 timing-data access.
