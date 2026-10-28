# Ergast data in tif1: Stats repo first, Jolpica fallback

> **Status (2026-10-28): implemented in 0.10.0.** Every phase of section 15
> shipped in one change set: `tif1/ergast/` (sections 6-7), the `Session`
> merge and extras (section 8), the schedule backends, the packaged
> 1950-2026 Ergast schedules and pre-2018 sessions (section 9), the CLI and
> Jupyter surfaces (section 10), tests (section 13) and docs (section 14).
> One deviation: the `compliance` extra of section 13 cannot exist because
> fastf1 pins pandas below 3 while tif1 requires pandas 3; the compliance
> tests skip unless fastf1 is installed by hand (`uv pip install fastf1
> --no-deps`). The open questions of section 18 were resolved with their
> recommended answers.

Implementation plan, written 2026-10-28 after a gap analysis of fastf1 3.8.3
against tif1 0.9.0 and an inspection of `TracingInsights-Archive/Stats` and
the Jolpica API. Decisions below were settled in the design interview; the
"Assumptions" and "Open questions" sections at the end list what was decided
by the author and what still needs an answer.

## 1. Goals

1. Close every fastf1 API gap that Ergast data can close: the whole
   `fastf1.ergast` package, the `backend=`/`force_ergast=` keyword family,
   `get_events_remaining`, `get_testing_event`, `get_testing_session`,
   `Event.is_testing`, `EventSchedule.is_testing`, and the official
   classification columns of `Session.results`.
2. Serve that data from the Stats repo (no rate limits, one JSON per
   race-level table) and fall back to Jolpica only when Stats cannot answer
   correctly.
3. Expose the extra Ergast data tif1 can carry beyond fastf1 (gaps, fastest
   lap facts, pit stops, standings, circuit geometry), keeping fastf1's
   columns first and identical.
4. Extend the Session and schedule APIs back to 1950.
5. Keep the performance budget: nothing new on the laps/telemetry hot path,
   one extra parallel request when `results`/`drivers` are read, zero network
   for past-season schedules.

## 2. Findings that shape the design

### 2.1 fastf1 surface missing in tif1 (introspected, fastf1 3.8.3)

| Area | Missing in tif1 |
| --- | --- |
| `fastf1.ergast` | `Ergast` (13 endpoint methods), `ErgastResultFrame`, `ErgastResultSeries`, `ErgastSimpleResponse`, `ErgastMultiResponse`, `ErgastRawResponse`, `ErgastResponseMixin` (`total_results`, `is_complete`, `get_next_result_page`, `get_prev_result_page`), `structure` tables, `legacy.fetch_results/fetch_season/fetch_day`, `BASE_URL`, `TIMEOUT`, `HEADERS` |
| `fastf1.exceptions` | `ErgastError`, `ErgastJsonError`, `ErgastInvalidRequestError`, `RateLimitExceededError` (also `DataNotLoadedError`, `NoLapDataError`, `InvalidSessionError`, `FastF1CriticalError`, outside this plan's data scope) |
| top level | `get_events_remaining`, `get_testing_event`, `get_testing_session`; kwargs `backend=`, `force_ergast=` on `get_session`/`get_event`/`get_event_schedule`; `strict_search=` on `get_event` |
| events | `Event.is_testing()`, `EventSchedule.is_testing()`; schedules before 2018 (vendored f1schedule files are 2018-2026; `get_event_schedule(2015)` is empty, `get_session(2015, ...)` raises) |
| `Session.results` | `Points`, `GridPosition`, `Time`, `Q1`/`Q2`/`Q3`, `DriverId`, `TeamId`, `BroadcastName`, `CountryCode` absent or NaN; `Position`/`ClassifiedPosition`/`Status` are derived from lap counts, not the official classification; `total_laps` is a `None` placeholder |
| minor | `Laps.QUICKLAP_THRESHOLD`, `Telemetry.TELEMETRY_FREQUENCY`, `Telemetry.register_new_channel`, `Cache.api_request_wrapper` |

### 2.2 The Stats repo (`TracingInsights-Archive/Stats`, branch `main`)

- Layout: `{year}/events.json` (Jolpica `/{year}/races/` response) and
  `{year}/{slug}/` with raw Jolpica `MRData` payloads:
  `event_info.json` (one `Race` object), `results.json`, `quali_results.json`,
  `sprint_results.json` (sprint rounds only), `laptimes.json` (1996 onward,
  all pages merged into one `Laps` list), `pitstops.json` (2011 onward),
  `driverPoints.json` and `teamPoints.json` (standings after that round).
- `slug = raceName.lower().replace(" ", "-")`; it must be resolved through
  `events.json` by round, never guessed from tif1 event names (2026 has
  `bahrain-grand-prix-in-malaysia` next to a stale `bahrain-grand-prix`
  folder; `são-paulo-grand-prix` needs URL encoding).
- Season-level `driverPoints.json`/`teamPoints.json` are stale or empty and
  are never read. Per-round files are correct.
- The merged `laptimes.json` keeps the first page's `limit: 100` header;
  paging headers in Stats files are meaningless and must be recomputed.
- Updated manually per round, one to two days after a race (latest: 2026
  round 18 on 2026-10-26). The repository cron is disabled.
- jsDelivr serves single files (twelve never-touched files answered 200 on
  first request); a transient 403 "Package size exceeded the configured limit
  of 50 MB" appears right after a branch update, and StaticDelivr showed one
  transient 404. `cdn.py` already treats 403 as a per-file refusal; the risk
  is the "4xx everywhere = missing for 7 days" verdict. No Hugging Face
  bucket exists for Stats.

### 2.3 Jolpica (`https://api.jolpi.ca/ergast/f1`)

- Unauthenticated: 4 requests/second burst, 500 requests/hour sustained,
  HTTP 429 "Request was throttled" when exceeded; `limit` caps at 100; a
  custom `User-Agent` is required; CORS is open (`*`); `cache-control:
  max-age=600`.
- Differences from Ergast: no XML; 2023 `SprintShootout` and 2024+
  `SprintQualifying` keys in race schedules (Ergast called them
  `SecondPractice`); `positionText` uses `R` instead of `N`; `Time.time` always
  has three decimals; from 2025 all DNF causes collapse to `Retired`; status
  table ordered by `count`; standings queries require a season.
- List endpoints are sorted by id (`driverId`, `constructorId`, `circuitId`).
  `/{season}/drivers` includes practice-only drivers (2024 lists 25 drivers,
  Antonelli among them), so a driver list cannot be derived from result
  files. `total` for season-wide results counts result rows (2024: 479).

## 3. Decisions (from the interview)

| # | Decision |
| --- | --- |
| D1 | Full fastf1 parity: new `tif1.ergast` package, `from tif1.ergast import Ergast` is a drop-in, identical kwargs, flattened column names and response classes. |
| D2 | Derive as much as possible from Stats. Jolpica only for what Stats cannot answer correctly (see routing matrix). |
| D3 | Paging: Stats-derived answers are complete by default (`limit=None` means all); Jolpica-backed calls keep fastf1's default of 30. An explicit `limit`/`offset` is honoured on both. |
| D4 | `Session.results` merges Ergast data lazily on access, fastf1-style; `session.drivers`/`drivers_df` follow the official classification when Ergast data exists. |
| D5 | Schedules and sessions from 1950: `MIN_YEAR = 1950`; pre-2018 sessions carry results, Ergast lap timings (1996+), pit stops (2011+), no telemetry. |
| D6 | Stats is served by the existing jsDelivr -> StaticDelivr chain through a repo-aware URL layout (Hugging Face skipped for Stats). A "known missing" verdict for a Stats file is recorded only when every source answered 404, never on a 403, with one ~1 s in-process retry after a 403 from the last source. |
| D7 | `Session.results.Time` is the true total race time from `Time.millis`; a `TimeGap` extra carries the gap to the winner. |
| D8 | Extras: extra result/driver columns; `Session.pit_stops`, `Session.driver_standings`, `Session.constructor_standings`, top-level `get_driver_standings`/`get_constructor_standings`; circuit columns on `Event`/`EventSchedule`; CLI `results`/`standings` commands and Jupyter classification display. |
| D9 | `tif1.ergast` frames stay pandas (fastf1 parity) with a `result_type="polars"` option; `Session.pit_stops`/standings follow the session `lib`; `Session.results` stays pandas. |
| D10 | Freshness by season age: past seasons cached permanently; current season Stats payloads and all Jolpica responses expire after `ergast_ttl_hours` (default 6); `current`/`last` resolution expires after 1 hour; a missing current-season file is re-probed on each TTL expiry, not held for 7 days. |

## 4. Architecture

```
tif1.ergast.Ergast.get_*()            Session.results / drivers / pit_stops / standings
        |                                       |
        v                                       v
tif1/ergast/interface.py  ---------->  tif1/ergast/router.py (selectors -> source plan)
                                          |                      |
                                 StatsSource (stats.py)   JolpicaSource (jolpica.py)
                                   |  slug/round index        |  URL builder, rate limiter,
                                   |  per-round files          |  pagination, 429 handling
                                   v                           v
                        tif1/ergast/payloads.py: ErgastPayloadLoader
                        (memo -> SQLite json tier with fetched_at wrapper -> fetch)
                                   |                           |
                    cdn.CDNManager.try_sources_repo      HttpTransport.get_json(url)
                    (sync; fanned out in threads)        (niquests / Pyodide)
                                   |
                   jsDelivr -> StaticDelivr (Hugging Face skipped for Stats)
```

Flattening (`structure.py`) and response classes (`interface.py`) are
source-agnostic: both sources hand back the same `MRData`-shaped dict, so the
flattening tables are applied once, after routing.

## 5. Routing matrix

`S` = Stats (per-round files, season-wide answers assembled from them),
`J` = Jolpica. "Local filter" means the filter is applied to the Stats-derived
rows in memory. Any query without a season, any `status=` filter
(needs `statusId`), and any `season="current"`/`round="last"` resolution goes to
Jolpica (the resolution result is cached for one hour and the resolved integers
are routed normally).

| Method | Season + optional round | Extra selectors | Source |
| --- | --- | --- | --- |
| `get_seasons` | n/a | any | J |
| `get_race_schedule` | `events.json` rows, round filter local | `circuit` local (circuitId); `driver`/`constructor`/`grid_position`/`results_position`/`fastest_rank` filter rounds through the season results assembly | S; `status` -> J |
| `get_driver_info` | any | any | J (practice-only drivers are not derivable; see 2.3) |
| `get_constructor_info` | derived from `results.json` Constructor objects, sorted by `constructorId` | `driver`/`circuit`/`grid_position`/`results_position`/`fastest_rank` local | S; `status` -> J |
| `get_circuits` | derived from `events.json`, sorted by `circuitId` | `driver`/`constructor`/... through season results assembly | S; `status` -> J |
| `get_finishing_status` | any | any | J (`statusId` not in Stats) |
| `get_race_results` | `results.json` | `driver`/`constructor`/`circuit`/`grid_position`/`results_position`/`fastest_rank` local | S; `status` -> J; no season -> J |
| `get_qualifying_results` | `quali_results.json` | same, local | S |
| `get_sprint_results` | `sprint_results.json`; rounds without a `Sprint` key in `events.json` contribute nothing (no fallback) | same, local | S |
| `get_driver_standings` | `driverPoints.json` of the round; season only -> last completed round (algorithm 7.4) | `driver`, `standings_position` local | S; no season -> `ErgastInvalidRequestError` (Jolpica requires a season) |
| `get_constructor_standings` | `teamPoints.json`, same rule | `constructor`, `standings_position` local | S |
| `get_lap_times` | `laptimes.json` | `lap_number`, `driver` local | S |
| `get_pit_stops` | `pitstops.json` | `stop_number`, `lap_number`, `driver` local | S |

Fallback rule for every `S` cell: a Stats file that is 404 on every CDN or
has `total == "0"` falls through to Jolpica for the same (season, round,
endpoint); the Jolpica answer is cached with the TTL of section 7.5. A
sprint file is only "expected" when `events.json` lists a `Sprint` for that
round; a laps file before 1996 or a pit-stop file before 2011 is expected to be
absent and still falls through once (the verdict is then cached), so later
additions upstream are picked up.

## 6. New package `src/tif1/ergast/`

### 6.1 `__init__.py`

Exports `Ergast`, `fetch_results`, `fetch_season`, `fetch_day` (fastf1 names)
plus `ErgastResultFrame`, `ErgastResultSeries`, `ErgastSimpleResponse`,
`ErgastMultiResponse`, `ErgastRawResponse`, `ErgastResponseMixin`,
`BASE_URL`, `TIMEOUT`, `HEADERS`, `interface`, `legacy`, `structure`.
Lazy `__getattr__` so `import tif1.ergast` does not import pandas (same
pattern as `tif1/__init__.py`). `tif1.__init__` gains `"ergast"` in
`_LAZY_EXPORTS` and `__all__`.

### 6.2 `structure.py` (flattening tables)

- Port fastf1's category dicts verbatim (`Seasons`, `Races_Schedule`,
  `Races_RaceResults`, `Races_QualifyingResults`, `Races_SprintResults`,
  `Races_Laps`, `Races_PitStops`, `Drivers`, `Constructors`, `Circuits`,
  `Status`, `StandingsLists_Driver`, `StandingsLists_Constructor`, sub-tables
  `Driver`, `Constructor`, `ConstructorsInline`, `Circuit`, `Location`,
  `FastestLap`, `FastestLapTime`, `FastestLapAvgSpeed`, `TotalRaceTime`,
  `Timings`, `Laps`, `PitStops`, practice/qualifying/sprint date tables) with
  the same output names and the same `date_from_ergast`, `time_from_ergast`,
  `timedelta_from_ergast`, `save_int`, `save_float` casting helpers (shared
  regex, `-1`/`nan` sentinels kept for parity).
- Additions (appended after fastf1's names, never reordering them):
  - `Races_Schedule`: `SprintQualifying` -> `sprintQualifyingDate`/`sprintQualifyingTime`,
    `SprintShootout` -> `sprintShootoutDate`/`sprintShootoutTime`.
  - `TotalRaceTime`: `totalRaceTime` is built from `millis` when present
    (D7); `totalRaceTimeGap` holds the parsed `Time.time` for P2 onward and
    `0` for the winner.
- Flattening is implemented without `copy.deepcopy` (fastf1's TODO): one pass
  that builds column lists directly, then `pd.DataFrame` from dict-of-lists
  with explicit dtypes (object for str, float64, `timedelta64[ns]`,
  `datetime64[ns]`), which also removes fastf1's per-row dict churn.

### 6.3 `interface.py` (public classes)

- `ErgastResponseMixin`: same attributes (`_response_headers`,
  `_query_filters`, `_query_metadata`, `_selectors`), `total_results`,
  `is_complete`, `get_next_result_page`, `get_prev_result_page`. New read-only
  `source` property (`"stats"` or `"jolpica"`) and `fetched_at`.
- `ErgastResultFrame(pd.DataFrame)` / `ErgastResultSeries(pd.Series)` using
  tif1's `_constructor`/`_metadata` pattern from `models.py` (no
  `BaseDataFrame` import from fastf1).
- `ErgastSimpleResponse`, `ErgastMultiResponse` (`description`, `content`),
  `ErgastRawResponse(list)` with `auto_cast` recursion, exactly as fastf1.
- `Ergast(result_type="pandas"|"raw"|"polars", auto_cast=True, limit=None,
  *, source="auto"|"stats"|"jolpica")`. `limit=None` resolves per source
  (D3): Stats -> all rows, Jolpica -> API default 30. `source` is a tif1
  extra for tests and debugging.
- The 13 methods keep fastf1's signatures and docstrings. Each builds a
  `QueryPlan` (`endpoint`, `table`, `category`, `subcategory`, selectors,
  limit, offset) and hands it to `router.execute(plan)`.
- Paging over Stats answers: leaf rows (result rows, lap timings, pit
  stops, standings rows, races, circuits, constructors) are counted to form
  `total`; `offset`/`limit` slice the leaf rows across descriptions, then rows
  are regrouped into `description`/`content`. Synthetic headers:
  `{"limit": limit or total, "offset": offset, "total": total}`, so
  `is_complete`/`get_next_result_page` behave identically on both sources.
- `result_type="polars"`: convert through `pandas_to_polars` from
  `core_utils/backend_conversion.py` after flattening; multi responses return
  `content` as a list of `pl.DataFrame` and `description` as `pl.DataFrame`.

### 6.4 `router.py`

- `execute(plan) -> (payload_dict, source, headers)`; decides per the matrix,
  resolves `current`/`last` through Jolpica (`/current.json?limit=1`,
  `/current/last.json?limit=1`, cached 1 h), applies local filters, assembles
  season-wide answers (one async fan-out wave of per-round files through the
  existing `fetch_multiple_async` machinery, concurrency from
  `max_concurrent_requests`), and emits a canonical `MRData` dict so the
  flattening code never knows the source.
- Local filters are applied on the raw nested dicts before flattening
  (cheaper than filtering frames, and `raw` responses see the same subset).
- `status` filters and anything without a season bypass Stats entirely.

### 6.5 `stats.py` (`StatsSource`)

- Config: `stats_repo` (default `TracingInsights-Archive/Stats`),
  `stats_branch` (`main`).
- Round index: `events.json` per season -> `{round: (raceName, slug,
  sprint: bool, date, time, circuit)}`; slug URL-encoded with
  `urllib.parse.quote`. Memoised per process; cached permanently for past
  seasons, TTL for the current season.
- File accessors: `results(season, round)`, `qualifying`, `sprint`, `laps`,
  `pitstops`, `driver_standings`, `constructor_standings`, `event_info`,
  each returning the parsed dict or `None` (missing/empty) so the router can
  fall through.
- Validation: light pydantic models in `validation.py` (`MRDataEnvelope`,
  `RaceTablePayload`, `StandingsTablePayload`) that check the envelope keys
  and list types only; `validate_data` gating as for the other payloads.
- Season-wide assembly: `assemble(endpoint, season, rounds)` fetches all
  files in one wave, keeps round order, concatenates the `Races`
  (or `StandingsLists`) entries, recomputes `total`.
- Empty detection: `int(MRData.total) == 0` or empty `Races`.

### 6.6 `jolpica.py` (`JolpicaSource`)

- URL builder ported from fastf1 `_build_url` (same selector precedence
  rules) against `jolpica_base_url` (default `https://api.jolpi.ca/ergast/f1`).
- Headers: `User-Agent: tif1/{__version__} (+https://github.com/TracingInsights/tif1)`.
  Timeout from `ergast_timeout` (default 5.0 s, fastf1's `TIMEOUT`).
- Rate limiter (process-global, thread-safe): token bucket 4/s and a
  sliding one-hour window of 500 (`ergast_rate_limit_burst`,
  `ergast_rate_limit_hourly`). Calls block until a token is free; a 429
  honours `Retry-After` (or backs off 2 s, 4 s, 8 s) for up to three attempts
  and then raises `RateLimitExceededError`.
- Auto-pagination when a caller passes an explicit `limit` above 100
  (API cap): fetch 100-row pages until `limit` rows or `total` reached,
  then stitch into one `MRData` dict (headers rewritten to the requested
  limit/offset).
- Errors: non-JSON body -> `ErgastJsonError`; non-200 -> `ErgastInvalidRequestError`
  (fastf1 parity), both subclasses of `ErgastError(TIF1Error)`; transport
  errors -> `NetworkError`; `offline_mode` -> `NetworkError` before any
  request.
- Async twin (`get_async`) over `AsyncHttpTransport` for Pyodide and for
  `Session.load_async`; the sync path uses the same `HttpTransport` seam
  (`PyodideTransport` in the browser).
- Config switch `ergast_fallback` (`TIF1_ERGAST_FALLBACK`, default true).
  When false, a Stats miss raises `DataNotFoundError` and Jolpica-only
  endpoints raise `ErgastInvalidRequestError` explaining the switch.

### 6.7 `payloads.py` (`ErgastPayloadLoader`)

- Built on `PayloadLoader` with injected `fetch`, `cache_get`, `cache_set`,
  `memo_get`, `memo_set` (the existing override seams), so memo -> SQLite ->
  fetch ordering and coercion are reused, not duplicated.
- Cache keys: `stats/{year}/{slug}/{file}` and `jolpica/{path}?{query}` in
  the SQLite `json` tier, stored as `{"fetched_at": t, "payload": {...}}`
  (the wrapper `events.py` already uses for `f1schedule/`).
- TTL policy (D10) implemented in `cache_get`: past season -> never expires;
  current season -> `ergast_ttl_hours`; `jolpica/current*` -> 1 h.
- Known-empty verdicts: an empty Stats file (or empty Jolpica answer) is
  stored as the empty payload with `fetched_at`; for past seasons it is
  re-probed after `missing_payloads_ttl_days`, for the current season after
  `ergast_ttl_hours`.
- `Cache.invalidate("ergast")` scope added to `InvalidateScope` so
  `tif1 cache-clear --ergast` can drop only this tier.

### 6.8 `legacy.py`

`fetch_results(year, gp, session)`, `fetch_season(year)`, `fetch_day(year,
gp, day)` with fastf1 semantics, implemented over the router (so they also
read Stats first).

## 7. Shared infrastructure changes

### 7.1 `cdn.py`: repo-aware layout and the verdict guard (D6)

- `CDNSource.format_repo_url(owner_repo: str, branch: str, path: str)`:
  jsDelivr `gh/{owner_repo}@{branch}/{path}`, StaticDelivr
  `gh/{owner_repo}/{branch}/{path}`, Hugging Face
  `buckets/{owner}/{repo}/resolve/{path}`. The existing `format_url(year, gp,
  session, path)` becomes a thin wrapper (`owner_repo=f"TracingInsights/{year}"`),
  so the 45 current call sites keep working.
- `CDNSource.serves(owner_repo)`: Hugging Face answers `False` for any owner
  other than the year buckets' org (no Stats bucket exists); the router
  skips it rather than paying a 401.
- `try_sources_repo(owner_repo, branch, path, fetch_func)`: the same loop
  and latency bookkeeping as `try_sources`, parameterised by repo (an async
  twin was written and removed in review: nothing called it; the Stats
  fan-out runs the sync loop in a thread pool). The outcome
  carries the list of statuses so the loader can apply the guard: a missing
  verdict is persisted only when every attempted source answered 404; a 403
  from the last source triggers one in-process retry after ~1 s before the
  caller sees `DataNotFoundError` (never persisted).
- Unit tests: `tests/unit/test_cdn.py` gains repo-layout URL tests, the
  403-then-200 retry, the "403 anywhere never persists" rule, and the HF skip.

### 7.2 `async_fetch.py`

- Not done as planned: the Stats results file rides alongside
  `drivers.json` on a daemon thread (`Session._start_ergast_prefetch`) and
  season-wide assembly fans the sync loop out in `payloads.stats_payloads`,
  so `fetch_multiple_async` needed no repo-aware request type.

### 7.3 `exceptions.py`, `config.py`, `types.py`

- Exceptions: `ErgastError(TIF1Error)`, `ErgastJsonError(ErgastError)`,
  `ErgastInvalidRequestError(ErgastError)`, `RateLimitExceededError(NetworkError)`;
  exported from `tif1` and `tif1.exceptions`.
- Config keys and env vars: `ergast_results` (`TIF1_ERGAST_RESULTS`, bool,
  true), `ergast_fallback` (`TIF1_ERGAST_FALLBACK`, bool, true),
  `jolpica_base_url` (`TIF1_JOLPICA_BASE_URL`), `ergast_ttl_hours`
  (`TIF1_ERGAST_TTL_HOURS`, float, 6), `ergast_timeout`
  (`TIF1_ERGAST_TIMEOUT`, float, 5), `ergast_rate_limit_burst`
  (`TIF1_ERGAST_RATE_LIMIT_BURST`, int, 4), `ergast_rate_limit_hourly`
  (`TIF1_ERGAST_RATE_LIMIT_HOURLY`, int, 500), `stats_repo`
  (`TIF1_STATS_REPO`), `stats_branch` (`TIF1_STATS_BRANCH`).
- `types.py`: `ErgastResultType = Literal["pandas", "raw", "polars"]`,
  `ScheduleBackend = Literal["fastf1", "f1timing", "ergast"]`, TypedDicts for
  the `MRData` envelope used by validation.
- `constants.py`: `MIN_YEAR = 1950`; `ERGAST_SESSION_FILES` mapping session
  name -> Stats file; `RESULTS_COLUMN_ORDER` (fastf1's 22 names, then extras).

### 7.4 Last completed round (standings without a round)

1. From `events.json`, take rounds whose race `date`+`time` (UTC) is in the
   past (all rounds for past seasons).
2. Fetch `driverPoints.json`/`teamPoints.json` of the highest such round; if
   missing or empty, step back one round, at most twice.
3. Otherwise fall through to Jolpica `/{season}/driverstandings`.

### 7.5 TTL summary

| Payload | Past season | Current season |
| --- | --- | --- |
| Stats files (`events.json`, per-round files) | permanent | `ergast_ttl_hours` (6 h) |
| Jolpica responses | `ergast_ttl_hours` | `ergast_ttl_hours` |
| `current`/`last` resolution | n/a | 1 h |
| Empty / missing verdicts | `missing_payloads_ttl_days` (7 d) | `ergast_ttl_hours` |

## 8. `Session` integration (`core.py`, `models.py`)

### 8.1 Lazy fetch and prefetch wave

- New `Session._ergast_payload()` resolves the Stats file for the session:
  Race -> `results.json`, Qualifying -> `quali_results.json`, Sprint ->
  `sprint_results.json`, Practice 1-3 / Sprint Qualifying / Sprint Shootout ->
  `results.json` (identity columns only, fastf1 parity for practice, an extra
  for the sprint quali formats). Round comes from `self.event.RoundNumber`.
- `_prefetch_session_tables` / `_prefetch_session_tables_async` add the
  `RepoPayloadRequest` for that file whenever `drivers.json` is requested, so
  `drivers`, `drivers_df` and `results` cost one parallel request, not a
  serial one. Failure of that request never fails the wave.
- Fallback chain inside the merge: Stats -> Jolpica (`ergast_fallback`) ->
  today's lap-derived classification with a warning (fastf1's
  `soft_exceptions` behaviour). `offline_mode` uses the cached payload or
  skips the merge silently (info log). `results` never raises because of
  Ergast.
- Opt-out: `get_session(..., ergast=None | bool)` and `ergast_results`
  config; `False` restores the current behaviour exactly.

### 8.2 Merge rules (fastf1 parity + D7)

- Join key: `DriverNumber` (TracingInsights `dn`) = Ergast `number` (string).
- TracingInsights rows are primary. For Race, Qualifying and Sprint the join
  is outer: drivers present only in Ergast (DNS, withdrawals) are added with
  `FirstName`, `LastName`, `FullName`, `Abbreviation` (= `code`), `TeamName`
  (= constructor name), `TeamColor` resolved from the session's team colour
  map when the team exists in `drivers.json`, else `""`, and `Position`/
  `GridPosition` set to NaN (fastf1 parity). For the other sessions the join
  is left.
- Race / Sprint columns from Ergast: `Position` (float), `ClassifiedPosition`
  (`positionText`), `GridPosition` (float, 0.0 = pit-lane start, as fastf1),
  `Status`, `Points` (float), `Time` (`millis` -> `Timedelta`, NaT when
  absent), `Laps` (float). Qualifying: `Position`, `Q1`, `Q2`, `Q3`
  (`Timedelta`, NaT when absent). All sessions: `DriverId`, `TeamId`.
- `BroadcastName` derived as `f"{FirstName[0]} {LastName.upper()}"`;
  `CountryCode` stays `""` (not in Ergast; see open question 2).
- Extras appended after fastf1's 22 columns (D8): `TimeGap`,
  `FastestLapRank`, `FastestLapNumber`, `FastestLapTime`,
  `FastestLapAvgSpeed`, `PermanentNumber`, `DriverNationality`,
  `TeamNationality`, `DateOfBirth`, `DriverUrl`, `TeamUrl`.
- dtype contract (fastf1 `_COLUMNS`): str columns object dtype, `Position`/
  `GridPosition`/`Points`/`Laps` float64, `Q1`/`Q2`/`Q3`/`Time`/`TimeGap`/
  `FastestLapTime` `timedelta64[ns]`, `DateOfBirth` `datetime64[ns]`.
  `SessionResults` gains `_force_default_cols` behaviour: every column always
  exists.
- Order: by `Position` ascending, NaN last, stable. Duplicate driver numbers
  (shared drives in the 1950s) are kept with a warning, as fastf1 does.
- `session.drivers` / `drivers_df` / `_driver_rows_info` take the merged
  order when Ergast data exists (D4). `charts._common.finishing_order` and
  the CLI `drivers` command therefore show the official classification.
- `Session.total_laps` returns the maximum Ergast `laps` for Race/Sprint
  (today a `None` placeholder); unchanged elsewhere.
- `DriverResult.dnf`: classify by regex `^(finished|\+\d+ laps?)$`
  (case-insensitive) instead of the current four-string list, which treats
  "+3 Laps" as a DNF.
- `Driver` Series (`models.Driver`) gains `DriverId`, `TeamId`,
  `PermanentNumber`, `DriverNationality`, `DateOfBirth` when the merge ran.

### 8.3 New Session surfaces (D8, D9)

- `Session.pit_stops` (2011+; Race only, else empty): columns
  `DriverNumber`, `Driver` (abbreviation), `DriverId`, `Stop`, `Lap`,
  `TimeOfDay` (`Timedelta` since midnight local, parsed from Ergast `time`),
  `Duration` (`Timedelta`). pandas or polars per `lib`.
- `Session.driver_standings` / `Session.constructor_standings`: standings
  after this event's round, columns `Position`, `PositionText`, `Points`,
  `Wins`, `DriverNumber`, `Abbreviation`, `DriverId`, `FirstName`,
  `LastName`, `DateOfBirth`, `DriverNationality`, `TeamIds`, `TeamNames`
  (lists, fastf1's inline-constructor shape) for drivers; `Position`,
  `PositionText`, `Points`, `Wins`, `TeamId`, `TeamName`, `TeamNationality`,
  `TeamUrl` for constructors. pandas or polars per `lib`.
- Top-level `tif1.get_driver_standings(year, round=None, *, lib=None)` and
  `tif1.get_constructor_standings(year, round=None, *, lib=None)` returning
  the same frames (season-only means last completed round, 7.4).

## 9. Schedules and history (`events.py`, `event_frames.py`)

### 9.1 Ergast schedule backend

- `get_event_schedule(year, include_testing=True, *, backend=None,
  force_ergast=False)`. `backend` in `{"fastf1", "f1timing", "ergast", None}`.
  `None`: f1schedule (vendored/live) for 2018 onward, Ergast before 2018,
  and Ergast as fallback when f1schedule has no season. `"fastf1"` and
  `"f1timing"` both mean the f1schedule source (tif1 has no F1 live-timing
  backend; `"f1timing"` logs at debug level). `"ergast"` always builds from
  Stats `events.json` (Jolpica fallback). `force_ergast=True` warns and maps
  to `backend="ergast"` (fastf1 deprecation parity).
- Ergast-backed rows: `RoundNumber`, `Country`, `Location`,
  `OfficialEventName` (`""`), `EventDate` (date + time, tz-naive UTC),
  `EventName` (`raceName`), `EventFormat` (`conventional`, `sprint` for
  2021-2022, `sprint_shootout` for 2023, `sprint_qualifying` from 2024, by the
  presence of `Sprint`), `Session1..5` names as fastf1 orders them,
  `Session{n}DateUtc` from the real Jolpica keys (`FirstPractice`,
  `SecondPractice`, `ThirdPractice`, `Qualifying`, `Sprint`,
  `SprintQualifying`, `SprintShootout`) when present and fastf1's day-offset
  estimates otherwise, `Session{n}Date` NaT (no local offset in Ergast),
  `F1ApiSupport = year >= 2018`.
- Circuit extras on every schedule (D8), appended after fastf1's 23 columns:
  `CircuitId`, `CircuitName`, `CircuitLatitude`, `CircuitLongitude`,
  `EventUrl`, `CircuitUrl`. For 2018 onward they are joined by `RoundNumber`
  from the vendored compact table (9.2), so schedule calls stay offline.
- `Event.is_testing()` and `EventSchedule.is_testing()` (`EventFormat ==
  "testing"`); `get_events_remaining(dt=None, *, include_testing=True,
  backend=None, force_ergast=False)`; `get_testing_event(year, test_number,
  *, backend=None)` and `get_testing_session(year, test_number,
  session_number, *, backend=None)` built on the f1schedule testing rows
  (`RoundNumber == 0`, ordered by `EventDate`); `backend="ergast"` raises
  `ValueError` for testing, as fastf1 does.
- `get_event(year, gp, *, backend=None, force_ergast=False,
  strict_search=False, exact_match=False)`; `strict_search` maps to the
  existing exact-name path.
- `get_session(year, gp, identifier, *, backend=None, force_ergast=False,
  enable_cache=None, lib=None, ergast=None)`.

### 9.2 Vendored compact schedule data

- `tools/vendor_ergast_schedules.py` reads every `{year}/events.json` from
  Stats (1950 to the previous season) and writes
  `src/tif1/data/schedules/ergast/schedule_{year}.json` holding only
  `round`, `raceName`, `date`, `time`, session date/time keys, and the
  `Circuit` block (about 10 KB per season, roughly 800 KB for 76 seasons,
  under 100 KB in the wheel after compression). `MANIFEST.in` already includes
  `src/tif1/data/**/*.json`.
- The current season is fetched live from Stats with the schedule TTL and
  falls back to the vendored file; past seasons are vendored only, exactly
  like the f1schedule tier. `_YearSchedule` memo and the SQLite
  `f1schedule/` wrapper pattern are reused with an `ergast/` key prefix.
- `tests/unit/test_schedule_schema.py` gains a schema check for the compact
  files; a `compliance_online` test diffs one vendored season against the
  live Stats file.

### 9.3 Pre-2018 sessions (D5)

- `get_sessions(year < 2018, event)` returns `["Qualifying", "Race"]`
  (plus `"Sprint"` when the round has one; none before 2021).
- `Session` for those years resolves the event through the Ergast schedule;
  `session_info` and `date` come from the vendored schedule row.
- `results`: Ergast-only merge path (no `drivers.json`): identity columns
  from the Ergast `Driver`/`Constructor` blocks, `TeamColor` from
  `plotting_constants` team colours when the constructor is known, else `""`,
  `HeadshotUrl = ""`.
- `laps` (Race, 1996 onward): the router's `laptimes.json` plus the results
  and pit-stop files are converted into the TracingInsights `laptimes`
  payload schema (`time`, `lap`, `pos`, `drv`, `dNum`, `team`, `stint`,
  `sesT` as the cumulative race time, `lST`, booleans false, remaining keys
  `None`) and fed through the existing laps constructor, so both backends,
  dtypes, column order and every `Laps` method come for free. `Stint` is
  derived from pit-stop lap numbers (2011 onward), `PitInTime`/`PitOutTime`
  stay NaT (Ergast has no session clock). The `Laps.session` link works for
  `pick_*` helpers; `get_telemetry` raises `DataNotFoundError`.
- `telemetry`, `car_data`, `pos_data`, `weather`, `race_control_messages`,
  `track_status`, `session_status`, `get_circuit_info` raise
  `DataNotFoundError("... not available before 2018")` with session context.
- `pit_stops`, `driver_standings`, `constructor_standings` work as in 8.3.

## 10. CLI and Jupyter (D8)

- `tif1 results YEAR GP SESSION [--lib pandas|polars] [--no-ergast]`: prints
  the classification table (Position, Driver, Team, Grid, Status, Points,
  Time/TimeGap or Q1-Q3).
- `tif1 standings YEAR [ROUND] [--constructors]`.
- `tif1 cache-clear --ergast` (section 6.7).
- `jupyter.display_session_info` shows "Official classification: yes/no
  (source)" and the top three when the merge ran.

## 11. Pyodide and offline

- Stats fetches go through the same `HttpTransport`/`AsyncHttpTransport`
  seams, so `PyodideTransport` covers them; Jolpica has open CORS. The
  rate limiter uses `time.monotonic` and `asyncio.sleep` on the async path
  (no threads). `tools/pyodide_checks.py` gains one `Ergast().get_race_results(2024, 1)`
  check and one `Session.results` merge check.
- `offline_mode`: cached Stats/Jolpica payloads are served; anything else
  raises `NetworkError` in `tif1.ergast` and degrades silently in
  `Session.results`.

## 12. Performance budget and measurements

| Path | Budget | Measured by |
| --- | --- | --- |
| `import tif1` | unchanged (lazy package) | existing import benchmark |
| `session.laps` | unchanged (no Ergast on the laps path) | `tests/benchmarks` |
| `session.results` / `drivers` cold | +1 parallel request in the existing wave (target < 80 ms added on a warm edge) | new benchmark |
| `session.results` warm (SQLite) | < 5 ms added | new benchmark |
| `Ergast().get_race_results(season=2024)` cold | one `events.json` RTT + one 24-file wave (target < 1.5 s) | new benchmark |
| same, warm | < 30 ms (memo) | new benchmark |
| `get_event_schedule(1995)` | offline, < 10 ms | new benchmark |
| flattening 2024 season results | < 20 ms (dict-of-lists build, no deepcopy) | microbenchmark |

## 13. Tests

- Fixtures: copies of real Stats files under `tests/fixtures/ergast/stats/`
  (2025 `events.json`; 2025 `chinese-grand-prix` all eight files, a sprint
  round; 1950 `british-grand-prix` with its empty qualifying file; 2026
  `events.json` for the Bahrain-in-Malaysia slug; 2024 `dutch-grand-prix`
  without a sprint) and Jolpica fixtures under `tests/fixtures/ergast/jolpica/`
  (seasons page, 2024 drivers page, 2024 status, `current/last`, a 429 body).
  Served through `InMemoryTransport` keyed by URL, so unit tests never touch
  sockets (the autouse blocker stays).
- Unit (`tests/unit/test_ergast_*.py`): structure flattening per category,
  casting helpers, paging slices and headers, router matrix (every cell),
  Stats slug resolution and encoding, empty/missing fall-through, last
  completed round, TTL policy by season age, verdict guard, rate limiter
  (fake clock), 429 handling, auto-pagination, `Session.results` merge
  (outer/left joins, DNS rows, duplicates, dtypes, ordering, `Time` from
  millis, `TimeGap`), `drivers` reorder, opt-out, offline, `total_laps`,
  `dnf`, pit stops, standings, schedule backends, `is_testing`,
  `get_events_remaining`, testing helpers, pre-2018 sessions and laps
  conversion, CLI commands, polars result type.
- Property (`tests/property`): time-string parsers round-trip, paging
  invariants (`offset + len(page) <= total`, pages partition the rows),
  slug function idempotence over observed race names.
- Compliance: `tests/unit/test_ergast_fastf1_compliance.py` with
  `pytest.importorskip("fastf1")` compares tif1's flattened frames with
  `fastf1.ergast.interface.ErgastResultFrame(response=..., category=...)` on
  the same fixture payloads (column names, order of fastf1's columns, dtypes,
  values except the documented `totalRaceTime` change); fastf1 is added to a
  `compliance` dev extra so CI runs it.
- Integration (`integration` marker): one Stats file through the real CDN
  chain, one Jolpica call with the rate limiter, one pre-2018 session.
- Benchmarks (`benchmark` marker): the rows of section 12.
- Coverage stays above the 80 % threshold; `ruff check`, `ruff format`,
  `ty check` clean.

## 14. Documentation (Mintlify, via `.agents/skills/mintlify/SKILL.md`)

- New `docs/api-reference/ergast.mdx` (classes, methods, routing, paging,
  sources, config) and `docs/guides/historical-data.mdx` (standings, pit
  stops, 1950-2017 sessions, Stats vs Jolpica).
- Update `docs/api-reference/session.mdx` (results columns, `pit_stops`,
  standings, `ergast` kwarg), `events.mdx` (`backend`, circuit columns,
  testing helpers), `cli.mdx`, `config.mdx`, `exceptions.mdx`,
  `fastf1-compat.mdx`, `reference/data-reference.mdx` (Stats files),
  `reference/faq.mdx`, `migration-from-fastf1.mdx`, `why-tif1.mdx`;
  `docs/docs.json` navigation (Core API and Compatibility groups).
- Repo files: `COMPATIBILITY.md` (tick the Ergast rows, note `Time`
  semantics), `DATA_REFERENCE.md` (Stats layout), `AGENTS.md` (package
  summary, routing rule, CDN repo layout), `CHANGELOG.md` entry, version
  bump to 0.10.0 in `pyproject.toml` (open question 3).

## 15. Delivery phases

| Phase | Scope | Acceptance |
| --- | --- | --- |
| P1 Foundations | 7.1-7.3, 6.1-6.8 (`tif1.ergast` complete), fixtures, unit/property/compliance tests, `ergast.mdx` | `from tif1.ergast import Ergast` passes the compliance suite; every routing cell has a test; no change to existing benchmarks |
| P2 Session merge | 8.1-8.3, CLI `results`/`standings`, Jupyter, docs updates | `session.results` matches fastf1's columns and values on 2024 fixtures; `drivers` order official; opt-out restores old output byte-for-byte |
| P3 History | 9.1-9.3, vendored schedules, `MIN_YEAR = 1950`, testing helpers, `get_events_remaining`, `is_testing` | `get_session(1995, "Monaco", "Race").results` and `.laps` (1996+) work offline after one fetch; `get_event_schedule(2024, backend="ergast")` matches fastf1's Ergast schedule columns |
| P4 Surfaces and release | `result_type="polars"`, top-level standings helpers, `cache-clear --ergast`, Pyodide checks, CHANGELOG, version | benchmarks within budget; docs built; CHANGELOG and COMPATIBILITY updated |

Each phase is one PR; P2 and P3 can start once P1's router and loader APIs
are merged.

## 16. Risks and mitigations

- Stats lag after a race weekend: TTL re-probe (D10) plus Jolpica fallback
  for a missing round; `source` attribution makes it visible.
- Slug drift (renamed races, stale folders): slugs always come from
  `events.json`; a 404 on a resolved slug falls through to Jolpica.
- jsDelivr transient 403 / StaticDelivr transient 404: verdict guard and the
  single retry (D6); never persisted.
- Jolpica limits for heavy historical use: the routing keeps Jolpica traffic
  to list endpoints and genuine misses; the limiter blocks instead of
  bursting; `RateLimitExceededError` is explicit.
- `drivers` reorder changes chart output for post-race penalties: intended
  (official order); documented in CHANGELOG with the `ergast=False` opt-out.
- Duplicate car numbers in early seasons: duplicates kept with a warning;
  `get_driver` resolves by abbreviation first.
- Wheel size from vendored schedules: compact format, measured in P3 (target
  under 100 KB compressed).
- Pyodide has no threads: the rate limiter and fan-out use asyncio only on
  the async path; the sync path blocks through the existing bridge.

## 17. Assumptions accepted (decided by the author, veto in review)

1. `get_driver_info` always goes to Jolpica, a correction to D2, because
   Jolpica lists practice-only drivers that no Stats file contains.
2. The `totalRaceTime` rule of D7 also applies inside `tif1.ergast` frames
   (`totalRaceTime` from `millis`, new `totalRaceTimeGap`); `totalRaceTimeMillis`
   is unchanged.
3. Sprint Qualifying / Sprint Shootout sessions receive `DriverId`/`TeamId`
   from the race results file (left join) although fastf1 leaves them empty.
4. `"f1timing"` is accepted as an alias of the f1schedule backend rather than
   raising.
5. Jolpica fallback is on by default; `TIF1_ERGAST_FALLBACK=0` disables it.
6. Jolpica responses are cached with the same 6 h TTL for past seasons too
   (past-season Jolpica answers are rare: list endpoints and misses).
7. The Ergast tier lives in the SQLite `json` tier with the `fetched_at`
   wrapper; no schema version bump is needed (`_FRAME_TIER_SCHEMA_VERSION`
   only covers telemetry frames).
8. The four minor parity items (`QUICKLAP_THRESHOLD`, `TELEMETRY_FREQUENCY`,
   `register_new_channel`, `api_request_wrapper`) are added as constants/thin
   shims in P4 since they are one-liners.
9. `Session.results` stays pandas on polars sessions (D9); charts and the
   CLI read it as pandas.

## 18. Open questions

1. Abbreviation for drivers without an Ergast `code` (most drivers before the
   1990s): leave `Abbreviation` empty and key `Laps.Driver` by `DriverId`
   (recommended, nothing invented), or synthesise a three-letter code from the
   family name with collision handling.
2. `CountryCode` (fastf1 has "FRA"-style codes from the F1 API): leave `""`
   (recommended for P2) or vendor a nationality-to-ISO3 map as an extra later.
3. Version: 0.10.0 (recommended; additive, one behaviour change behind an
   opt-out) or 1.0.0.
4. Fill `Q1`/`Q2`/`Q3` for Sprint Qualifying / Sprint Shootout from the laps'
   `QualifyingSession` segment (tif1-only derivation, closes a COMPATIBILITY
   gap)? Recommended yes, as an extra in P2.
5. Historical event-name aliases (e.g. "Indianapolis 500", "Pescara",
   "Swiss") in `EVENT_NAME_ALIASES`: rely on fuzzy matching first and add
   aliases only when a test shows a wrong resolution (recommended).
6. Whether the Stats repository's cron should be re-enabled so the TTL
   re-probe has something to find; this is a data-ops decision outside tif1.
