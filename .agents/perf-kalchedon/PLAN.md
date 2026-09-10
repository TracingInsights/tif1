# K-series performance experiments — 2026-09-10 — PLAN

Prior series (NOT re-run): H1–H11 (`.agents/perf/RESULTS.md`), E1–E10
(`.agents/perf-monaco/RESULTS.md`), F1–F10 + G1–G10 (`.agents/perf-fab5/RESULTS.md`).
The planned-but-never-run hypotheses in `.agents/perf-kleitor/PLAN.md` (that thread
pivoted to restore work) are folded into this series where still relevant:
K-G1 dedupe (moot — background prefetch disabled by default), K-G2 → K7,
K-G3 → part of K5 measurement, K-G4 → K3, K-G5 → K10, K-G6 → folded into K1/K4
design, K-G7 → K6, K-G8 (polars laps) → covered by K7 scope note, K-G9 → moot
(table waves are tiny and serial), K-G10 → K4.

## Baselines (this sandbox, 2026-09-10)

Cold Monaco 2026 Race, default config, warm CDN edge, 5 fresh-process runs
(`tools/monaco_telemetry_cold_benchmark.py`):

| metric | value |
|---|---|
| total_s | median **9.84** (min 9.18, max 14.27) |
| telemetry_s | median 9.05 |
| get_session_s | ~0.35–0.53 (≈0.46 s lazy `tif1.core`→pandas import chain; H10 territory, not re-run) |
| laps_s | ~0.39–0.66 |

Warm-cache loads (persistent cache primed via `TIF1_ULTRA_COLD_START=false` run,
then default config; `tools/warm_cache_benchmark.py`): total 4.3–8.4 s,
telemetry 3.7–8.0 s (noisy; see K2/K9 for the variance sources).

Key phase measurements:

- Warm telemetry e2e (profiled): `_create_telemetry_df` ×1452 = **3.2 s**,
  `get_telemetry_batch` (SQL + serial zlib decompress + orjson parse) = **1.3 s**,
  ~2.2 s network tail from 3 all-CDN-missing files.
- **Default config never persists the cache**: `ultra_cold_start=True` (default)
  resolves to ultra-cold whenever the session is not already cached, and
  `write_cache = enable_cache and not ultra_cold` skips writes on every path;
  `ultra_cold_background_cache_fill` defaults to False. A default-config user
  re-downloads the full ~84 MB on every fresh process (verified: primed-by-default
  cache dir contains 0 rows after a full load; with ultra-cold off the same flow
  writes 1454 JSON rows + 1452 telemetry rows).
- Missing-file tail: jsDelivr returns 403 (rate-limit) for files that 404 on the
  fallback CDNs; the 403 turns the attempt into a retryable NetworkError → 3
  attempts × (3 CDN probes + 1–2 s backoff sleeps) ≈ 5 s tail on every load that
  includes an everywhere-missing file (warm trace: 18 probes over 5.7→12.5 s).
- Telemetry bulk fetch writes BOTH cache tiers per payload (set_raw in-fetch +
  set_telemetry after) when writes are enabled.

## Ten hypotheses

| # | Hypothesis | Measured with |
|---|-----------|---------------|
| K1 | Default-config cold sessions should persist fetched payloads (deferred background write-back off the fetch semaphore) so the second load is warm — currently unreachable | Live: run-1 cold parity + run-2 warm vs control re-download; offline write macro |
| K2 | Warm batch cache read (`get_telemetry_batch`) decompresses+parses ~1452 blobs serially in one executor thread; a small thread pool parallelizes (zlib/orjson release the GIL) — distinct from rejected F10 (GIL-bound pandas assembly) and H3 (parse during live fetch) | Offline on primed cache; e2e warm |
| K3 | SQLite-tier codec zlib-3 → zstd (optional `zstandard` dep): faster compress on cold writes AND faster decompress on warm reads; zstd magic-byte detection keeps legacy zlib rows readable | Offline codec bench on 1452 real blobs; cache macro |
| K4 | Telemetry bulk fetch double-writes two cache tiers with duplicate dumps/compress/insert; write one tier from the original raw blob post-gather | Offline cache-on macro; warm-read correctness |
| K5 | Per-payload cache writes take the SQLite lock + commit check per row; batched executemany writes for bulk paths (also `synchronous`/commit-interval probe — kleitor K-G3) | Offline write macro |
| K6 | `_typed_telemetry_frame` builds Int64 columns from Python lists via `pd.array(v)`; numpy-first construction is faster and dtype-identical for canonical columns (kleitor K-G7, unrun) | Offline assembly micro on 1452 real payloads, parity-gated |
| K7 | Laps assembly `_process_lap_df` runs to_numeric+to_timedelta+astype+categorical+reorder; pre-typed merged-dict construction (kleitor K-G2, unrun) | Offline laps assembly micro; live A/B |
| K8 | Warm disk loads re-parse JSON per payload; a binary (msgpack) SQLite-tier encoding skips orjson on warm reads | Offline decode bench on real blobs; warm macro |
| K9 | All-CDN-4xx (403+404 mix) missing files trigger the full retry storm (3 attempts × backoff sleeps); treat exhausted-CDNs-with-only-4xx as DataNotFound like the all-404 case | Deterministic offline harness; live tail trace |
| K10 | uvloop for the fetch event loop (kleitor K-G5, unrun) — optional extra, graceful fallback | Live A/B |

Rust feasibility (user-opened option, evaluated with measurement inside K6/K7
analysis): quantify the remaining Python-level fraction of the assembly phase;
prototype only if a Rust-owned slice could beat the C-dominated paths.

## Method

- Offline: deterministic harnesses on the dumped 1452-payload set
  (`/tmp/monaco_all_tel.pkl`) and the primed warm cache; parity-gated where
  output dtypes/values must not change.
- Live: `tools/monaco_telemetry_cold_benchmark.py` + interleaved A/B via
  `tools/monaco_ab_suite.py`; warm flows via `tools/warm_cache_benchmark.py`.
- Keep = improvement beyond noise; rejected experiments reverted and documented.
- ONE combined PR for all kept experiments (per this thread's instructions).
