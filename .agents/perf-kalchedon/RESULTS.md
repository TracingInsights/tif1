# K-series performance experiments — 2026-09-10 — FINAL

Ten hypotheses, all run to completion. Prior series were NOT re-run: H1–H11
(`.agents/perf/RESULTS.md`), E1–E10 (`.agents/perf-monaco/RESULTS.md`),
F1–F10 + G1–G10 (`.agents/perf-fab5/RESULTS.md`), and the planned-but-never-run
`.agents/perf-kleitor/PLAN.md` set is folded in where still relevant (its G1
dedupe is moot — the background prefetch is disabled by default config; G2→K7,
G3→K5-pragma-probe, G4→K3, G5→K10, G6→K1's deferred-write design, G7→K6,
G10→K4).

Benchmark: full telemetry of the 2026 Monaco GP Race (~1452 payloads ≈ 84 MB),
live CDN chain, fresh process per run, `tools/monaco_telemetry_cold_benchmark.py`;
interleaved A/B via `tools/monaco_ab_suite.py` (control = `main`@a554c76 tree at
`/tmp/tif1-control`); warm flows via explicit `TIF1_CACHE_DIR` two-run protocols.

## Baseline (control tree, this vantage, warm edge)

| metric | value |
|---|---|
| cold total_s (5-run median) | **9.84** (min 9.18, max 14.27) |
| cold telemetry_s (median) | 9.05 |
| warm-cache load (properly primed) | 4.3–8.4 s total |
| warm e2e telemetry profile | `_create_telemetry_df`×1452 ≈ 3.2 s + `get_telemetry_batch` (SQL + serial decompress + parse) ≈ 1.3 s + ~2.2 s missing-file network tail |
| get_session | 0.35–0.53 s (≈0.46 s lazy `tif1.core`→pandas import chain; H10 territory, not re-run) |

**Key discovery (K1's premise):** under default config (`ultra_cold_start=True`)
the persistent cache is NEVER written — `write_cache = enable_cache and not
ultra_cold` gates every path and `ultra_cold_background_cache_fill` defaults to
False. A default-config user re-downloads the full ~84 MB on every fresh
process; the G5 "never re-download over a warm cache" contract is unreachable.
Verified: full load with default config leaves a 0-row cache; the same flow with
`TIF1_ULTRA_COLD_START=false` writes 1454 JSON rows + 1452 telemetry rows.

## Verdicts

| # | Hypothesis | Verdict | Evidence |
|---|-----------|---------|----------|
| K1 | Default-config cold sessions persist fetched payloads (write-back decoupled from ultra-cold read-skip; deferred out of the fetch slots) so the second load is warm | **KEPT** | Two-run protocol, default config: candidate run-1 cold 11.26 s, run-2 warm **4.68 s** vs control run-2 **9.31 s** (re-downloads; cache stays 0 rows) → **−50% on every load after the first**. Cold cost: +0.5 s (deferred flush 0.78 s measured, `tools/phase_split_diagnostic.py`), offset in interleaved A/B: final suite B median **−9.2%** (4/5 pairs faster; A median 12.24 vs B 11.11). Cache contents after candidate run-1: json=2 rows + telemetry=1452 — single tier, as designed. |
| K2 | Warm batch read (`get_telemetry_batch`) parallelizes decode+parse (zlib/orjson release the GIL) | **REJECTED (vantage-limited)** | Offline on 1452 primed rows: serial 1092 ms vs pool(2) 1286 ms / pool(4) 1079 ms / pool(8) 1276 ms — 0.85–1.01x on this 1-core sandbox. The mechanism is real (GIL released) but needs ≥2 cores; not shipped since it regresses single-core boxes. |
| K3 | SQLite-tier codec zlib-3 → zstd-1 (optional `zstandard` dep; magic-byte detection keeps legacy rows readable) | **KEPT** | 1452 real blobs: compress **1009 → 226 ms (4.5x)**, decompress **325 → 153 ms (2.1x)**, stored ratio 26.2% → **23.4% (better)**. Round-trip + legacy-zlib-row read verified in tests; live warm read of a legacy zlib cache measured 4.43 s (parity with old-code warm reads on the same cache). |
| K4 | Telemetry bulk fetch double-writes two cache tiers; write one tier from the parsed payload | **KEPT** | Write macro on real payloads: current (zlib-3, 2 tiers, per-row) **3.08 s** → zstd single-tier **0.69 s (4.47x)**. Implemented in `fetch_json_async` (telemetry paths route to `set_telemetry`), `Session._cache_result`, and `_schedule_background_cache_fill` (JSON-tier copy dropped). All readers consult the telemetry table first (verified in code); post-loop `set_telemetry` in the bulk path removed as redundant. |
| K5 | Batched `executemany` SQLite writes for bulk paths (+ `synchronous=OFF` probe, kleitor G3) | **REJECTED** | executemany/100 = 0.81 s vs per-row single-tier 0.69 s (no gain; interleaved dumps+compress pipelines better); `synchronous=OFF` 0.75 s vs 0.81 s (noise — WAL+NORMAL already skips per-commit fsync). |
| K6 | numpy-first Int64/typed construction in `_typed_telemetry_frame` (kleitor G7) | **REJECTED** | 1452 real payloads, parity perfect (1452/1452 frames identical incl. dtypes): current 1.26 ms/frame vs numpy-first **1.51 ms/frame (0.83x)** — the `None not in v` pre-scans + per-column `np.asarray` cost more than pandas' internal sanitize saves. |
| K7 | Laps pre-typed merged-dict construction (kleitor G2) | **REJECTED (negligible)** | Warm-cache laps assembly is **79 ms** (`_process_lap_df`; `_replace_null_like_strings` 46 ms + dtype coercions) of a ~10 s pipeline (~0.8%); below the live noise floor (±1–2 s). |
| K8 | Binary (msgpack) SQLite-tier encoding to skip orjson on warm reads | **REJECTED** | 1452 real payloads: orjson 962 ms vs msgpack unpack 876 ms (1.1x), but msgpack pack 300 ms vs orjson dumps ~190 ms (slower encode), and msgpack+zstd stores 25.6% vs json+zstd 23.4% (bigger). Marginal warm-read win, worse everywhere else. |
| K9 | All-CDN-4xx exhaustion (403+404 mix) raises `DataNotFoundError` like all-404, killing the missing-file retry storm | **KEPT** | Deterministic harness (fake 403-on-primary + 404-on-mirrors, jitter off): current **NetworkError after 3.15 s / 9 CDN probes** (3 attempts × backoff sleeps 1 s + 2 s) → candidate **DataNotFoundError after 0.18 s / 3 probes (−94%)**. The mechanism was observed live earlier this vantage (warm trace: 18 probes over 5.7→12.5 s for 3 everywhere-missing files; jsDelivr was 403-ing while mirrors 404'd). By final A/B time jsDelivr had switched to plain 404s, making the live delta neutral that hour — the win is situational but the storm is real and now structurally impossible. Non-4xx failures (5xx, transport) keep the retryable `NetworkError`. |
| K10 | uvloop for the fetch event loop (kleitor G5) | **REJECTED** | Interleaved 3+3 live: stock 16.01/16.27/11.33 vs uvloop 9.53/40.13/10.70 — mixed pairs, one straggler run each way; no beyond-noise signal. Consistent with F-series micros (loop scheduling ~9 ms per 1452-request batch, ~0.06% of the phase). |

## Rust / native-extension feasibility (explicitly in scope — evaluated, not shipped)

Measured against the actual hot loops on the 1452-payload set: the assembly
phase is pandas-C-bound (`pd.DataFrame` construction ≈ 1.9 s of the 3.2 s;
`sanitize_array` 1.1 s — K6 showed even Python-glue micro-optimization loses,
0.83x), JSON decode is orjson-C (962 ms), and the storage codec is now zstd-C.
The remaining Python-level slice is too small for a Rust rewrite to pay for its
PyO3 boundary costs, and the "Rust JSON→Arrow→pandas" route is the polars/pyarrow
path H9 already rejected (dtype semantics + 40 MB dep). A Rust extension would
also add a maturin build backend to a pure-wheel library for sub-noise gains.
Documented rather than prototyped — the decision is backed by the K6/H9/F10
measurements, not skipped.

## Cumulative outcome (shipped)

- **Second and later loads of a cached session: 9.31 → 4.68 s (−50%)** under
  default config — the persistent cache tier finally works by default.
- Cold loads: median −9.2% in the final interleaved suite (4/5 pairs faster),
  with the K9 storm structurally eliminated for everywhere-missing files.
- Bulk cache writes 4.47x faster (zstd + single tier); warm disk reads ~2.1x
  faster decompress; legacy zlib rows and legacy inner-payload telemetry rows
  stay readable.
- 1183 unit tests pass (3 updated to the new ultra-cold write-back contract),
  +11 new contract tests (`tests/unit/test_k_series_contracts.py`);
  property/integration green except the documented sandbox timing flake
  (`test_parallel_operations_time_is_max_not_sum`, passes in isolation);
  ruff clean; ty at the pre-existing 5-diagnostic baseline.

## Files changed

- `src/tif1/cdn.py` — K9: all-4xx exhaustion → `DataNotFoundError`; statuses
  threaded through the sequential and raced fallback loops.
- `src/tif1/cache.py` — K3: zstd-1 codec with zlib fallback and legacy-row
  magic-byte detection; `json_dumps_bytes` (no str round-trip on writes).
- `src/tif1/async_fetch.py` — K4+K1: telemetry-path cache writes route to the
  telemetry tier; `fetch_multiple_async(defer_telemetry_writes=True)` collects
  writes and flushes them in one serial executor job after the gather.
- `src/tif1/core.py` — K1: write-back decoupled from ultra-cold read-skip on
  every fetch path; `_mark_session_cache_populated` only upgrades an
  unresolved probe (a cold-starting session keeps cold semantics); redundant
  post-loop writes removed; `_cache_telemetry_payload` single-tier routing.
- `src/tif1/core_utils/constants.py` — shared `telemetry_ref_from_path`.
- `src/tif1/core_utils/json_utils.py` — `json_dumps_bytes`.
- `pyproject.toml` — `zstandard>=0.25.0,<0.26` dependency.
- Tests: 3 contract updates + `tests/unit/test_k_series_contracts.py` (11 tests).
- Harnesses: `tools/warm_cache_benchmark.py`, `tools/request_count_diagnostic.py`,
  `tools/prefetch_wave_diagnostic.py`, `tools/phase_split_diagnostic.py`,
  `tools/dump_all_tel.py`; series artifacts under `.agents/perf-kalchedon/`.
