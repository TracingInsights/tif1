# tif1 Performance Experiments — Baseline & Experiment 1 (2026-09-04)

Harness: `tools/perf_validation_experiment.py` (offline, deterministic, no network).
Payload set models a full race session: `drivers.json` + `session_laptimes.json`
(1140 rows x 40 fields) + `weather.json` (130 rows) + `rcm.json` (400 rows) +
1140 telemetry payloads (400 samples x 18 channels each). Numbers are
best-of-5 `time.perf_counter` runs on this thread's sandbox (1-core burst);
absolute values vary run to run, relative deltas are stable.

## Key context finding

Shipped config defaults are `validate_data/lap_times/telemetry = False`
(`src/tif1/config.py`) — default users already skip pydantic validation
entirely (dispatch-only cost measured at ~0.0006 ms/payload). The experiment
therefore matters for opt-in validation configs and for removing the pydantic
cost ceiling entirely.

## Baseline (pydantic validation enabled via TIF1_VALIDATE_*=true)

Micro, per payload:

| payload                    | pydantic (ms) | no-pydantic (ms) | delta  |
|----------------------------|--------------:|-----------------:|--------|
| drivers.json (20)          |        0.035  |          0.0007  | -98%   |
| session_laptimes (1140x40) |        4.83   |          2.87    | -41%   |
| weather.json (130x8)       |        0.117  |          0.064   | -45%   |
| rcm.json (400x9)           |        0.476  |          0.314   | -34%   |
| {drv}/{lap}_tel (400x18)   |        0.775  |          0.471   | -39%   |

Macro, full-session fetch pipeline (`fetch_multiple_async`, 1144 payloads,
in-memory fake HTTP session, no cache):

| configuration                    | best (ms) |
|----------------------------------|----------:|
| validation ON, pydantic          | 1971      |
| validation ON, pydantic removed  | 1500      |
| validation OFF (shipped default) |  960      |

## Experiment 1: remove pydantic from the fetch pipeline — RESULT

-25% end-to-end on the validation-enabled full-session fetch (1971 -> 1500 ms);
34-98% per payload. Default-config users: unchanged (validation already off).
All 1164 unit + 94 property/integration tests pass; output parity verified
key-by-key against the pydantic path on all six payload shapes (including
wrapped `tel` and PascalCase weather inputs).

Post-change profile: the fast dump itself is free (0.003 ms); ~95% of the
remaining "validation" time is `_normalize_payload_lists` scanning every
element of every channel for null-like strings (0.449 of 0.471 ms per tel
payload; 2.91 of 2.87 ms for laptimes). That is now the validation cost
ceiling and the target of H2.

Files changed: `src/tif1/validation.py` (pydantic-free
`validate_{lap,telemetry,race_control,weather}_data` built on
`_fast_list_dump`/`_fast_telemetry_dump`, alias->field mapping, default
filling, length checks, identical non-strict fallbacks), `src/tif1/async_fetch.py`
(removed the `validate_drivers().model_dump()` round-trip for drivers.json).

Known semantic deltas (documented): no per-element type coercion (numeric
strings -> floats) and no driver-code pattern check on the fetch path; the
pydantic entry points (`validate_laps`, `validate_telemetry`, ...) remain for
API compatibility.

## Experiment 2 (H2): defer null-like normalization to vectorized DataFrame cleanup — RESULT

`_normalize_payload_lists` scanned every element of every channel (~95% of the
post-H1 validation cost). The laps DataFrame path already normalizes null-like
strings vectorized (`helpers._replace_null_like_strings` / `_pl` variant), and
the default no-validation path never scanned at all — so the fetch-path scan
was redundant work for laps/telemetry/rcm.

Change: `validate_{lap,telemetry,race_control}_data` gained `normalize=True`
keyword (public contract unchanged); the fetch pipeline passes
`normalize=False` for laps/telemetry/rcm. Weather keeps inline coercion
(tiny payload; explicit contract in
`test_coverage_core2::test_weather_json_none_strings_coerced`).

Micro (validation on, per payload):

| payload                    | pydantic | H1     | H2      |
|----------------------------|---------:|-------:|--------:|
| session_laptimes (1140x40) | 4.83 ms  | 2.87   | 0.0075  |
| rcm.json (400x9)           | 0.476    | 0.314  | 0.0023  |
| {drv}/{lap}_tel (400x18)   | 0.775    | 0.471  | 0.0061  |
| weather.json (130x8)       | 0.117    | 0.064  | 0.065 (kept) |

Macro (validation on, full session): 1971 -> 1500 (H1) -> **987 ms** (H2) —
within ~3% of the validation-off floor (960 ms). Cumulative: validation-enabled
full-session fetch is **2.0x faster**; per-payload validation cost is now
~0.006-0.008 ms (640x faster than pydantic for laptimes).

Verification: 1164 unit + property/integration suites pass (one unreproduced
flake on a single combined run, green on two subsequent identical runs); ruff
and ty unchanged from baseline. Files: `src/tif1/validation.py`,
`src/tif1/async_fetch.py`.

## Experiment 3 (H5): cache write path — raw blobs + SQLite-tier compression — RESULT

Reframed by measurement: the original hypothesis (skip re-serialization) was
only ~4% of the cold-write cost (orjson dumps 0.13 ms/payload). The real cost
is SQLite insert **volume**: 1140 telemetry payloads x 81 KB = ~92 MB of rows
(~0.96 ms/insert) plus WAL commit flushes. Profiling: 1144 `cache.set` calls
= 1879 ms, of which dumps = 143 ms and LRU = 1 ms.

Fix (two parts):

1. `Cache.set_raw(key, blob)` — the async fetch pipeline now persists the
   original response bytes when validation left the payload untouched
   (duck-typed like the existing `_get_from_memory` probe, so stub caches in
   tests keep working). Skips dumps + str decode entirely.
2. zlib-3 compression at the SQLite boundary only (`_encode_sqlite_value` /
   `_decode_sqlite_value`): blobs >= 4 KB compress ~12x (81 KB -> 7 KB). The
   memory LRU keeps plain blobs; small rows stay plain TEXT (legacy shape);
   legacy TEXT rows and corrupt rows degrade to miss/refetch, verified.

Macro (default config, full session, cache enabled, cold write):

| metric | before | after | delta |
|---|---:|---:|---|
| cold write | 3750 ms | 2262-2288 ms | **-39%** |
| warm memory | 852 ms | 921 ms | noise |
| warm disk | 661-720 ms | 720-752 ms | noise |

Correctness: round-trips verified for big (compressed) / small (plain TEXT) /
raw-bytes / telemetry-table / batch reads / legacy TEXT rows / corrupt rows;
1164 unit tests pass; ruff clean; ty unchanged. Two environmental test
artifacts on this sandbox, not regressions:
`property/test_async_properties.py::...time_is_max_not_sum` (passes in
isolation) and `benchmarks/...::test_concurrent_reads_dont_block` (asserts
>1.5x GIL-bound thread speedup; fails identically on pre-change code here).

Files: `src/tif1/cache.py`, `src/tif1/async_fetch.py`.

Remaining observation for H4/H6: warm loads still re-parse every payload
(`json_loads` 0.19 ms x 1144 ~ 220 ms from memory tier, ~0.36 ms from SQLite);
memory LRU is 1024 items so a full session (1144 payloads) partially evicts.

## Experiment 4 (H9): polars-first telemetry assembly for the pandas backend — REJECTED after implementation attempt

The initial micro-benchmark (`_create_telemetry_df` x1140: polars 423 ms vs
pandas 2598 ms, 6.1x) measured the **legacy per-frame fallback path**, not the
primary batch path. The real batch path (`_assemble_telemetry_batch` ->
`_merge_telemetry_payloads` + `_telemetry_frame_from_merged`) already builds
one merged dict-of-lists with a single dtype pass (prior optimization, same
family as H8). Re-measured on the real paths (1140 payloads, 456k rows):

| construction route | from-merged | notes |
|---|---:|---|
| current: `pd.DataFrame(dict)` + dtype pass | 725-790 ms | baseline |
| merged -> polars -> `to_pandas` + dtype pass | 294 ms (2.7x) | **requires pyarrow** (dev-only dep; verified `to_pandas` hard-fails without it) |
| explicit numpy / nullable arrays | 559 ms (1.4x) | dtype-identical on test payloads, but changes inference semantics for null/mixed int columns (pandas infers object for `[1, None]`, this route yields Int64) |
| strict-safe numpy (only null-free homogeneous columns) | 705 ms (3%) | provably semantics-identical — and worthless |

Verdict: the pandas batch path is already at its dependency-free floor. The
2.7x route needs pyarrow as a new ~40 MB runtime dependency and produces
arrow-influenced dtypes on non-canonical columns (RPM-style int-with-null
columns become Int64 instead of object/float64), making output dtypes depend
on whether pyarrow happens to be installed. The 1.4x route silently changes
pandas inference semantics. The 3% route is free but pointless. None meet the
bar; `lib="polars"` users already have the native fast path. No code change.

## Experiments 4-10 (H3, H4, H6, H7, H8, H9, H10) — RESULTS

Environment: 2 cores; CDN reachable but live payload layout not discoverable
(404s on candidate paths; jsDelivr listing API blocked), so H7 numbers are
**simulated-latency** measurements (30 ms RTT), not real-CDN numbers.

| # | Outcome | Evidence |
|---|---------|----------|
| H3 parse off event loop | **REJECTED by measurement** — thread offload of telemetry parse gained nothing under 30 ms latency (726 vs 706 ms) and regressed ~8% without latency (1048 vs ~960 ms): GIL serializes orjson anyway and the executor hop costs ~0.08 ms/payload. Matches the existing design comment. Change reverted. |
| H4 parsed-object tier | **KEPT** — read-through parsed LRU (128 json / 256 telemetry entries) in front of the blob tiers; any write drops the parsed entry. Repeat hit: 0.4 us vs 190 us parse (~460x); one-driver warm telemetry load 12.9 -> 0.028 ms. Full-session warm loads unaffected by design (1144 payloads > 128 cap; larger caps would cost ~250 KB/payload in RAM). |
| H6 commit interval | **KEPT (small)** — 25 -> 100: 1144 compressed writes 532 -> 427 ms (noisy ~20% of the write path). Durability window widens to 100 regenerable cache writes; `close()` force-commits. |
| H7 concurrency cap | **KEPT, flagged** — default `max_concurrent_requests` 20 -> 64: simulated 30 ms RTT batch load 720 -> 450 ms (-37%), saturating at 64 (128: 447 ms). Rides the multiplexed HTTP/2/3 session. Needs real-CDN validation before release. |
| H8 single-shot laps frame | **Already implemented** — pandas path already builds one DataFrame from `_merge_lap_payloads` dict-of-lists (pd.concat noted as ~2x regression in pandas 3.0); polars path uses `pl.concat(vertical_relaxed)`. No further work found. |
| H9 polars telemetry assembly | **REJECTED after implementation attempt** — the real batch path is already merged-dict optimized (the 6.1x micro number was against the legacy fallback); faster routes require pyarrow (~40 MB new dep, environment-dependent dtypes) or change inference semantics; the semantics-safe route is 3%. See the H9 deep-dive above. |
| H10 lazy pandas import | **MEASURED, rejected** — `import tif1` ~50 ms warm / pandas dominates (331 ms cold); pydantic confirmed NOT imported (lazy already). Lazy-pandas is a large refactor for CLI-startup-only wins. |

## Final state after all experiments

Canonical macro runs (1144-payload full session, best of 5):

| configuration | baseline | final | delta |
|---|---:|---:|---|
| validation ON, no cache | 1971 ms | 1024 ms | **1.9x** |
| default config, no cache | 960 ms | 921 ms | parity (no regression) |
| default config, cold write-cache | 3750 ms | 2086 ms | **-44%** |

Plus: repeat warm hits ~460x faster (H4), network-bound batch loads -37%
simulated (H7), one-driver warm telemetry loads ~460x (H4).

Code changed: `src/tif1/validation.py`, `src/tif1/async_fetch.py`,
`src/tif1/cache.py`, `src/tif1/config.py` (max_concurrent_requests 64,
cache_commit_interval 100), `tests/unit/test_cache.py` (interval-pinned
batching test), `tools/perf_validation_experiment.py` (harness).
Verification: 1164 unit + 94 property/integration pass; ruff clean; ty
diagnostics identical to the pre-change baseline (40, all pre-existing).

## Ten hypotheses

| # | Hypothesis | Predicted impact | Status |
|---|------------|------------------|--------|
| H1 | Remove pydantic validation from the fetch pipeline | -25% full-session fetch (validation on); -34..98% per payload | MEASURED (this run) |
| H2 | Type-aware null-like normalization: normalize only lists containing strings, or defer to a vectorized pandas `replace` on object columns after DataFrame construction | Removes ~0.45 ms x 1140 tel + ~2.9 ms laptimes; validation cost -> near zero (another ~-500 ms on validation-on telemetry load) | MEASURED (this run): fetch pipeline now defers to the existing vectorized DataFrame cleanup; validation-on macro 1500 -> 987 ms |
| H3 | Telemetry parse+validate blocks the event loop (`json_loads` inline for `_tel.json`, `_validate_json_payload` inline in the coroutine) while up to 128 executor threads idle | Wall-clock cut of the CPU-bound segment by ~min(cores, payload count) factor on multi-core hosts; needs multi-core box to measure | MEASURED: rejected — GIL serializes parse; offload adds ~8% overhead; reverted |
| H4 | Warm-start loads re-parse every cached payload from SQLite (`cache.get` -> orjson parse ~0.85 ms/tel payload); cache parsed objects or a parsed-frame tier | Large warm telemetry-load win (~1 s of parse per full session) | MEASURED (this run): read-through parsed tier; repeat hits 0.4 us vs 190 us (~460x) |
| H5 | `cache.set` re-serializes every fetched payload (`json_dumps`, cache.py:592); store the raw response bytes instead and parse on read | ~-0.5 s on cold write-cache full-session loads | MEASURED (this run): re-serialization was only 4%; real cost was SQLite insert volume. Raw blobs + zlib-3 at the SQLite boundary (12x smaller rows): cold write 3750 -> ~2270 ms (-39%) |
| H6 | Batch SQLite writes: one transaction per `fetch_multiple_async` batch instead of per-payload `cache.set` | Moderate cold-start win when `write_cache` on; measure with `test_cache_serialization_benchmark` | MEASURED: interval 25->100, ~20% of the write path; kept |
| H7 | Raise `max_concurrent_requests`/telemetry prefetch caps (20/32) for multiplexed HTTP/2 CDN fetches | Network-bound cold-start wall-clock win; requires online measurement | MEASURED (simulated 30 ms RTT): 20->64 concurrency, -37% wall; kept, needs real-CDN check |
| H8 | Laps DataFrame assembly from per-driver frames + concat; construct once from `session_laptimes.json` columns | Moderate laps-load win; verify current construction path first | Already implemented (`_merge_lap_payloads`); no further work |
| H9 | Polars-first assembly for telemetry batches (polars is a hard dep) or zero-copy dict-of-lists construction | Assembly speedup for 1140 x 400-sample frames; extend `tests/benchmarks/test_dataframe_performance.py` | MEASURED + implementation attempted: rejected — real batch path already merged-dict optimized; faster routes need pyarrow or change dtype semantics (see H9 deep-dive) |
| H10 | `import tif1` costs ~50 ms (pandas eager; pydantic already lazy — verified not in `sys.modules`) | Minor CLI/notebook startup win via lazy pandas; low priority | MEASURED: rejected — pandas dominates import, CLI-only win |

Rejected during analysis (measured, not worth pursuing): dispatch overhead of
`_validate_json_payload` with toggles off (~0.0006 ms/payload).
