# Sestos-series performance experiments — 2026-09-14 — FINAL

Ten hypotheses, all run to completion. Prior series were NOT re-run: H1–H11
(`.agents/perf/RESULTS.md`), E1–E10 (`.agents/perf-monaco/RESULTS.md`),
F1–F10 + G1–G10 (`.agents/perf-fab5/RESULTS.md`), K1–K10
(`.agents/perf-kalchedon/RESULTS.md`). Anything those measured (CDN
order/sharding/racing/hedging, concurrency caps, uvloop, GC, msgpack,
executemany, numpy-first frames, merged-dict frames, lazy imports, keepalive,
per-request micros, parse offload, validation pipeline, cache codecs,
write-back semantics, 4xx exhaustion) is out of scope here.

Benchmark: full telemetry of the 2026 Monaco GP Race (~1452 payloads ≈ 84 MB
raw, ~20 MB stored zstd-1), live CDN chain, fresh process per run.
Cold: `tools/monaco_telemetry_cold_benchmark.py` (throwaway
`TIF1_CACHE_DIR`). Warm: `tools/warm_cache_benchmark.py` (persistent primed
cache). Interleaved A/B via `tools/monaco_ab_suite.py`; control = main @
0c074a0 at `/tmp/tif1-control`.

## Baseline (this vantage, 2026-09-14)

| metric | value |
|---|---|
| cold total_s (5-run median, warm edge) | **11.39** (runs 11.39 / 10.08 / 9.69; first two suite runs hit the truly-cold edge at 190.4 / 204.7 s and re-warmed it) |
| warm total_s (3-run median, primed cache) | **6.09** (telemetry phase median **5.13 s**) |
| warm telemetry phase split (cProfile) | batch cache read ≈ 1.1 s (SQL 46 ms + zstd 67 ms + **orjson 840 ms**) + per-frame assembly (`_create_telemetry_df` × 1452) ≈ 3.8 s + missing-file network tail ≈ 1.0 s |
| missing-everywhere files | LEC/66, STR/58, SAI/72 `_tel.json` 404 on **every** CDN (verified again this series) — re-probed on every load |
| repeat `fetch_all_laps_telemetry()` in one process | full re-read + re-assembly (~2.9 s) |

## Verdicts

| # | Hypothesis | Verdict | Evidence |
|---|-----------|---------|----------|
| L1 | Brotli transfer encoding (install `brotli` so niquests advertises `br`) | **REJECTED (falsified by probe)** | niquests already advertises `gzip, deflate, zstd` and jsDelivr serves gzip: VER/1_tel.json = 49,553 B (gzip) vs 48,544 B (br) — a 2% delta, not the 60% of the uncompressed 119,773 B. (Probe also showed `Accept-Encoding: zstd` alone gets served **uncompressed** — gzip-first ordering is load-bearing.) No A/B warranted. |
| L2 | Persistent negative-result cache: everywhere-missing (all-CDN 4xx) payloads are remembered (TTL `missing_payloads_ttl_days`, default 7) so later loads skip the CDN walk | **KEPT** | Paired protocol, primed cache (3 rounds): no-verdicts telemetry 4.566 / 5.666 / 4.905 s vs verdicts 4.087 / 4.031 / 4.007 s — faster in 3/3 pairs, median −17.8%, HTTP GETs 9 → **0**. First A/B round exposed a real bug this experiment then fixed: `discard_missing` originally ran a SQLite DELETE on **every successful fetch** (1452 write transactions in the fetch slots; cold +23%), now it lazy-loads the verdict set and no-ops when no verdict exists (contract test added). |
| L3 | `fetch_all_laps_telemetry_async` reuses session-memoized frames on repeat calls (and memoizes frames it builds) | **KEPT** | Same process, primed cache: call 1 = 4.38 s (control 4.65 s), call 2 = **0.128 s vs control 2.896 s (22.6x)**; parity verified (same keys, `.equals` on sampled frames, identity of returned objects). |
| L4 | Multi-connection batch fetch: N parallel niquests sessions (own h3/h2 connections) instead of one multiplexed connection | **REJECTED (probe)** | 300 known-good telemetry files, interleaved 3 rounds: 1×22 median 1.04 s vs 2×11 1.08 s, 4×6 1.10 s, 2×22 1.02 s — parity within noise. The single multiplexed connection already saturates the edge; confirms the F-series "fetch phase client-side exhausted" from a new angle. |
| L5 | SQLite `PRAGMA mmap_size` for warm batch reads | **REJECTED (negligible)** | Primed DB, interleaved 5-rep: stock read median 46 ms vs mmap 34 ms — the SQL read is only 46 ms of a 1.1 s read phase (orjson parse dominates at 840 ms); the ~12 ms delta is 0.2% of the warm telemetry phase. |
| L6 | zstd trained dictionary for telemetry-tier blobs | **REJECTED (negligible)** | Trained on 200 payloads, measured on the other 1252: ratio 0.234 → 0.232, decompress 67 → 62 ms, compress slower (0.198 → 0.243 s). Dictionaries only pay off for small blobs; these are 50–120 KB. |
| L7 | Materialized-frame tier: warm loads persist assembled per-(driver,lap) DataFrames (pickle protocol 5 + zstd-1, `telemetry_frames` table); subsequent warm loads read frames directly (skip SQL blob read + zstd + orjson + pandas construction) | **KEPT** | Offline probe: per-frame pickle round-trip 1.085 s (one-blob variant slower, 1.291 s) vs 4.9 s read+assembly. Protocol (fresh process per step): purge → warm1 4.74 s (assembles from payloads + materializes 1452 frames, ~25 MB stored) → warm2 **1.08 s**, warm2-parity **1.06 s with 0/1452 mismatched** (columns, dtypes, values), warm3 1.04 s. Warm telemetry phase 5.13 → ~1.05 s (**−80%**). Frames materialize only from cache/memo-sourced payloads — the network fresh-results path never writes the tier, so cold loads pay zero. Corrupt rows degrade to tier misses (contract test). |
| L8 | Overlap the deferred telemetry cache-write flush (GIL-releasing zstd/SQLite) with post-fetch frame assembly (GIL-bound), via `fetch_multiple_async(flush_future_out=...)` | **REJECTED** | Two interleaved 5+5 cold A/Bs. First: B slower 5/5 pairs, median +23% (confounded by the L2 DELETE bug, since fixed). Second (clean): A median 9.95 s vs B median 13.57 s, **B slower in 5/5 pairs (+36%)**. On 1-core vantages, interleaving the CPU-bound flush with GIL-bound pandas assembly loses — same physics as F2 (+13.7%) and K2. Reverted; the inline deferred flush (K1) stands. |
| L9 | Skip the per-request JSON-tier `cache.get` probe for `_tel.json` paths (telemetry lives in its own table) | **REJECTED (falsified for the default path)** | Instrumented run: the default-config cold bulk path passes `use_cache=False` (ultra-cold) — **0 probes**; warm loads have zero requests to probe. Only off-default (`ultra_cold_start=false`) cold loads pay ~1 probe/payload at ~0.1–0.4 ms each, hidden under network — sub-noise, consistent with the F-series micro floor. The probes the hypothesis targets don't exist on the benchmark path. |
| L10 | `get_session` schedule path: vendored schedule payload parsed+validated per process — trim it | **REJECTED (falsified by measurement)** | `events._load_schedule_payload` (vendored parse + validate, 9 years) = **5.2 ms**; full `get_session` in-process 18.9 ms, repeat 0.6 ms. The benchmark's 0.42–0.65 s `get_session_s` is the lazy `tif1.core`→pandas import chain — already adjudicated and rejected by G9/H10. Nothing to trim. |

## Cumulative outcome (shipped)

Combined L2 + L3 + L7, final live verification:

- **Warm loads** (primed persistent cache, fresh process, benchmark protocol):
  - baseline (control): total median 6.09 s, telemetry 5.13 s
  - after L2 (verdicts active): telemetry 4.03–4.09 s
  - after L2+L7 first warm load (materializes frames): 4.74 s
  - after L2+L7 subsequent warm loads: telemetry **1.04–1.08 s**, total ≈ 1.6–1.8 s
    → **−70% vs baseline** (−80% on the telemetry phase)
- **Repeat per-lap telemetry access in one process** (L3): 2.90 → 0.13 s (22.6x).
- **Cold loads**: final interleaved A/B (5+5) at parity with control — by design
  (none of the kept experiments add work to the cold fetch path; the negative
  cache turns the 3 everywhere-missing files' walk into a one-time cost per
  cache, recorded once). Full numbers in `.agents/perf-sestos/final_ab.json`.
- Cache semantics: `missing_payloads` verdicts are TTL-scoped (7 days
  default, `TIF1_MISSING_PAYLOADS_TTL_DAYS`), dropped on successful writes,
  cleared by `invalidate()`/`clear()`; the `telemetry_frames` tier is derived
  data cleared with the telemetry scope; legacy zlib rows and pre-tier caches
  keep working (frames materialize on the first warm load).

### Rust / native-extension feasibility (in scope — evaluated, not shipped)

Re-affirmed with this series' measurements rather than re-litigated: the
three biggest remaining warm-path costs are now **orjson (C)**, **zstd (C)**
and **pandas frame construction (C)** — and L7 removes all three from the warm
path by persisting the assembled product, which is the same win a Rust
assembly pipeline would target without PyO3 boundary costs. Cold-side, the
fetch wave remains edge-bound (L4 parity at 1×22 vs 4 connections) and the
assembly phase is pandas-C-bound (K6: even Python-glue micro-optimization
lost, 0.83x; F10: threads 2.5x slower; F3: merged construction changes
dtypes). A Rust rewrite of the remaining Python slice (~0.4 s of dict
iteration/padding in a ~10 s pipeline) cannot pay for itself; documented
rather than prototyped, now with the L-series numbers as evidence.

## Verification

- 13 new contract tests (`tests/unit/test_l_series_contracts.py`): negative
  cache (record/short-circuit/TTL/discard-lazy-guard/invalidate/clear/
  cross-instance), memoized frame reuse, frames tier (parity, corrupt-row
  miss, invalidation, read-only, cold-read-skip), all passing.
- Full unit/property/integration suites + ruff + ty run on the final tree
  (see PR verification notes).
- Live parity: warm2 frames tier 0/1452 mismatched (columns, dtypes, values);
  1452/1452 frames returned on every measured run.

## Files changed

- `src/tif1/cache.py` — L2: `missing_payloads` table, TTL-scoped
  `is_known_missing`/`record_missing`/`discard_missing`, invalidate/clear
  wiring; L7: `telemetry_frames` table, `get_telemetry_frames_batch` /
  `set_telemetry_frames_batch`.
- `src/tif1/async_fetch.py` — L2: negative-verdict check before the CDN walk
  (executor-safe), verdict recording on all-4xx `DataNotFoundError`, verdict
  discard on successful fetch.
- `src/tif1/core.py` — L3: frame-memo reuse in `fetch_all_laps_telemetry_async`;
  L7: frames-tier read pre-filter + one bulk materialization write.
- `src/tif1/config.py` — `missing_payloads_ttl_days` (default 7.0, env
  `TIF1_MISSING_PAYLOADS_TTL_DAYS`).
- Tests: `tests/unit/test_l_series_contracts.py` (13 tests).
- Harnesses: `tools/sestos_profile_warm.py`, `sestos_debug_batch.py`,
  `sestos_probe_get_session.py`, `sestos_probe_storage.py`,
  `sestos_probe_multiconn.py`, `sestos_probe_frames.py`,
  `sestos_measure_repeat_fetch.py`, `sestos_measure_negative_cache.py`,
  `sestos_measure_frames_tier.py`, `sestos_probe_json_tier_probes.py`;
  series artifacts under `.agents/perf-sestos/`.
