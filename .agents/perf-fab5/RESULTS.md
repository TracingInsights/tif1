# Live cold-fetch performance experiments, series F — 2026 Monaco GP Race (2026-09-08) — FINAL

Follow-up series to `.agents/perf/RESULTS.md` (H1–H11) and
`.agents/perf-monaco/RESULTS.md` (E1–E10). Those hypotheses are NOT re-run here:
pydantic removal (H1/H2), cache tiers/writes/interval (H4/H5/H6), concurrency
cap (H7, 22; E8, 44 rejected), merged single-frame assembly (H8/H9), lazy
imports (H10, rejected), CDN racing (Exp11, rejected), decode+parse off the
event loop (E1, merged), pre-typed frames (E2, #60), jitter default fix
(E4, #61), vectorized refs (E6, #62), StaticDelivr-first (E9, #63), keepalive
max (E10, #64), GC freeze (E3, rejected), config memoize (E5, rejected), h3
disable (E7, rejected).

Benchmark: same as the E-series — full cold telemetry of the 2026 Monaco GP
Race (fresh process + throwaway `TIF1_CACHE_DIR`, ~1452 payloads ≈ 84 MB),
`tools/monaco_telemetry_cold_benchmark.py`, A/B via `tools/monaco_ab_suite.py`
(interleaved run-by-run so both variants see identical edge conditions).
Baseline tree: `main` at 44064e1 (E1 merged; E-series stack still open).

## Baseline (5 cold runs, main + E1, this sandbox vantage)

| run | get_session_s | laps_s | telemetry_s | total_s |
|-----|-------------:|-------:|------------:|--------:|
| 1 | 1.506 | 0.904 | 21.106 | 23.761 |
| 2 | 1.807 | 1.353 | 33.560 | 36.316 |
| 3 | 1.673 | 1.081 | 22.468 | 25.121 |
| 4 | 1.403 | 1.288 | 22.914 | 25.697 |
| 5 | 1.674 | 0.929 | 22.981 | 25.697 |

Median total **25.70 s**, telemetry **22.91 s** (this vantage's edge is
colder/slower than the E-series sandbox; absolute values are not comparable
across vantages — only interleaved A/B deltas are).

Phase split (live probe, cold): telemetry ≈ fetch 15.7 s (60%) +
post-processing 10.6 s (40%, refs ~0.5 s + per-lap frame assembly ~7-10 s).
Request-level diagnostics: 22-wide concurrency on ONE CDN host; ~60-75 req/s
steady (edge-fill latency bound, ~350 ms in-flight), h3 dominates (279/300).
get_session 1.67 s is ~95% the lazy `tif1.core`→pandas import chain (H10
territory, not re-run). Laps phase is network-edge-bound (drivers wave, then
laptimes wave; already concurrent).

## Ten hypotheses — final outcomes

| # | Hypothesis | Verdict | Evidence |
|---|------------|---------|----------|
| F1 | Shard large batches across the two fast CDN hosts (round-robin, fallback intact) | **REJECTED** | Interleaved A/B 5+5 (`.agents/perf-fab5/f1.json`): A median 25.12 s vs B 33.98 s → **+35.3% total / +38.8% telemetry regression**; B slower in 5/5 adjacent pairs. StaticDelivr's edge is far slower at batch scale from this vantage (~700-file bursts); the shard probe's apparent parity was a run-order artifact. Confirms E9's vantage-dependence caveat from the other direction. |
| F2 | Stream telemetry assembly: build each lap frame as its fetch completes | **REJECTED** | Interleaved A/B 5+5 (`.agents/perf-fab5/f2.json`): A median 28.40 s vs B 32.29 s → **+13.7% total / +14.8% telemetry regression**; B worse at every quantile. GIL contention: assembly (GIL-bound pandas construction) in worker threads fights the event loop and niquests response handling during the fetch; the serial post-fetch phase is exactly where the GIL is otherwise free. Extends the H3 finding from parse to assembly. |
| F3 | Merged-dict assembly + per-lap slice views for the result map | **REJECTED** | Offline parity run on 400 real payloads: merged inference leaks cross-payload dtype pollution — one dirty sibling payload forces `DistanceToDriverAhead` to object for EVERY lap's slice, while per-frame inference keeps clean laps float64. User-visible dtype semantics change (same wall H9 hit); no code change shipped. |
| F4 | Fast float-seconds → timedelta64[ns] cast (multiply+view vs `pd.to_timedelta`) | **REJECTED** | Offline, 400 real frames: current dtype pass 0.487 s (1.22 ms/frame) vs fast-cast 0.505 s → **0.96x (no gain)**; `to_timedelta` on float columns is already vectorized C; plus parity mismatches on None-padded frames. |
| F5 | Fuse get+decode+parse into one executor hop per request | **REJECTED (negligible)** | Deterministic echo micro-benchmark, 1452 requests: two hops 177.4 ms vs fused 110.1 ms → **~67 ms/batch (~0.3% of 23 s)**, below the A/B noise floor (±1-2 s); also undoes E1's deliberate get/decode separation. |
| F6 | Remove per-request `_track_request` (lock + config.get + monotonic) | **REJECTED (negligible)** | Micro-benchmark: 2.9 ms/batch uncontended, 6.6 ms under 22-thread contention → **~3-7 ms/batch (~0.03%)**. |
| F7 | jsDelivr `.min.json` minified payloads (`cdn_use_minification`) | **FALSIFIED BY PROBE** | `VER/1_tel.json` → 200 (119,773 B); `VER/1_tel.min.json` → **404**. jsDelivr does not minify JSON; enabling the knob would send every telemetry file through a 404 fall-through. No A/B needed. |
| F8 | Hoist per-request function-level imports off the fetch path | **REJECTED (negligible)** | Micro-benchmark: 3 `sys.modules`-hit imports × 1452 = **0.67 ms/batch (~0.003%)**. |
| F9 | Bounded worker pool instead of semaphore+gather in `fetch_multiple_async` | **REJECTED (negligible)** | Micro-benchmark, 1452 coroutines: semaphore+gather 9.1 ms vs worker pool 1.5 ms → **~7.6 ms/batch (~0.03%)**. |
| F10 | Thread-parallel per-lap frame assembly (post-fetch) | **REJECTED** | Offline, 400 real payloads: serial 1.72 s; 2/4/8 threads 4.2-4.5 s → **0.38-0.41x (2.5x slower)**. GIL + thread contention beats pandas' C-level construction time. |

## Series conclusion

All ten hypotheses were run to completion; none improved the benchmark. Zero
PRs from this series (one per *successful* experiment was the rule).

What the rejections collectively establish:

1. **The fetch phase (~60% of wall) is exhausted client-side.** It is
   edge-fill-latency bound at 22-wide on one CDN host; every remaining lever
   (order, keepalive, h3, concurrency cap, sharding, racing) has now been
   measured — the open E-series stack already carries the ones that won
   (E9/E10 on the E-series vantage).
2. **The assembly phase (~40%) only yields to *faster construction*, not to
   scheduling.** Overlap (F2), parallelism (F10), merged builds (F3), and
   dtype-pass micro-optimization (F4) all lose to GIL physics or dtype-parity
   walls. The known win there is the open E2 PR (#60, pre-typed frames,
   ~1.7x per frame) — this series found nothing beyond it.
3. **Per-request Python overhead is already at the floor**: the four
   measured micros (F5/F6/F8/F9) sum to ~75 ms on a ~23 s batch (~0.3%).

Highest-value pending performance work in this repo is therefore **merging
the open E-series stack (#60-#64)**, not new experiments.

## Artifacts

- `.agents/perf-fab5/baseline.json`, `f1.json`, `f2.json` — A/B suite reports
- `.agents/perf-fab5/profile_assembly.py`, `profile_get_session.py`,
  `profile_first_get_session.py`, `profile_schedule.py` — phase/profile probes
- `.agents/perf-fab5/proto_f3_f4.py`, `proto_f4.py`, `proto_f10.py`,
  `proto_micros.py` — offline measurement harnesses (rejections F3-F6, F8-F10)
- `tools/monaco_shard_probe.py` — F1 single-/sharded-CDN probe (kept for
  re-measurement from other vantage points, like `tools/cdn_parallel_experiment.py`)
- `.agents/perf-fab5/normalize_crlf.py` — internal tooling (note: repo files
  have per-file mixed LF/CRLF conventions; the F2 A/B "B" tree contained
  line-ending churn that is functionally identical and did not affect
  measurement semantics)

Method notes: A/B suites interleave control/candidate run-by-run (fresh
process + throwaway cache each) so CDN edge-state drift hits both variants;
micro-hypotheses (F5/F6/F8/F9) were measured with deterministic offline
harnesses because their predicted effects (0.003-0.3%) are below the live
suite's noise floor (±1-2 s), which is itself the honest verdict for them.

---

# G-series — 10 more experiments (2026-09-09) — FINAL

Triggered after the F-series stack PR (#65). Same benchmark and methodology;
H/E/F territory skipped. Recon probes: gcore.jsdelivr.net matches cdn
steady-state (41 ms vs 40 ms warm; fastly 7x slower); 3 Monaco telemetry files
404 on every CDN (LEC/66, STR/58, SAI/72; warm-edge 404-walk 0.09-0.92 s);
helpers.py eagerly imports polars (258 ms of the cold import chain);
`_prefetch_session_tables` drags weather.json (~10 KB) + rcm.json (~38 KB)
into every laps load over a dedicated extra HTTP session; jsDelivr serves
brotli (119,773 B -> 48,544 B); default `ultra_cold_start=True` skips ALL
cache reads (a warm-cache second process re-downloads everything).

Baseline for this series: the G-experiments' control sides (G1 suite A-side
get_session median 0.733 s, total ~11.4-11.8 s on the re-warmed edge; the
vantage had cooled to a 121 s batch immediately post-restart, then re-warmed).

## Outcomes

| # | Hypothesis | Verdict | Evidence |
|---|------------|---------|----------|
| G1 | Lazy polars import (helpers/models/types/backend_conversion) | **KEPT** | polars is optional but eagerly imported — 258 ms of every pandas-default cold start. Deterministic subprocess: first get_session 0.542 -> 0.425 s. A/B 5+5: get_session_s median 0.733 -> 0.447 s (**-39%**); total/telemetry within noise. Polars backend verified lazily loading. Commit f13e595. |
| G2 | Race remaining CDNs after a 404 (happy path untouched) | **KEPT** | Sequential 404-walk pays every CDN's latency (HF redirect ~0.4 s) in series. Raced: production walk for LEC/66_tel.json, 6 reps: median 0.09 -> 0.05 s (**-40%** warm edge; cool edges save the smaller remaining CDN latency, hundreds of ms). DataNotFound-still-only-after-all-404 preserved; 1183 tests. Commit a0d09d2. |
| G3 | Shard large batches across cdn + gcore mirrors | **REJECTED (probe)** | 300-file probe: steady-state shard 1.01 s vs cdn-only 1.34 s (-25%), BUT gcore's own edge was cold for these files — first gcore round 10.94 s. First-touch mirror-edge cost dominates for real users (confirms F1's +35% at mechanism level); mirrors are also vantage-risky. No code change. |
| G4 | Prefetch only requested session tables (weather/rcm gated on intent) | **KEPT** | laps-only flows fetched weather+rcm (~48 KB) + a dedicated extra HTTP session they never used; load()'s docstring already promises fetch-only-what's-required. Behavior verified: laps-only now fetches drivers only; load(all) unchanged. A/B 5+5: total median 11.38 -> 10.67 s (-6.3%), laps parity on this warm edge (win is cold-edge material). Commit 27ddc19. |
| G5 | `ultra_cold_start` auto-detects a warm cache | **KEPT** | Default config NEVER read the persistent cache (config short-circuit made the auto-detect unreachable): a warm-cache second process re-downloaded all 84 MB. Now ultra-cold only skips cache when the session isn't cached. Warm full-telemetry load: 10.72 -> **6.29 s (-41%)**; cold path unchanged (one memoized availability probe). 1183 tests. Commit 8a2446a. |
| G6 | Telemetry-tier cache write: orjson bytes direct | **REJECTED (negligible)** | 7.8 us/write (3.1 ms per 400 payloads) — ~11 ms per full-session write pass; zlib compression dominates the write, not the str round-trip. |
| G7 | Laps reorder: fuse insert(0)+reorder double copy | **REJECTED** | Measured on a 1455-row frame: current path 0.60 ms; the fused alternative 0.87 ms — the current code is already faster; sub-ms either way. |
| G8 | drivers.json sync fetch off the event loop | **REJECTED (analysis)** | Pure serial dependency — laptimes URLs cannot exist before drivers resolve, and nothing else is scheduled on the loop during the blocking call; G4's A/B laps-phase parity (0.412 vs 0.435 s medians) confirms no inflation. |
| G9 | `import tif1` trim (fuzzy/init eager imports) | **REJECTED (negligible)** | Import breakdown shows no deferrable module of consequence left after G1 (fuzzy ~11 ms at most, tif1 self is its own definitions); nothing worth the churn. |
| G10 | Drop Connection/Keep-Alive headers on h2/h3 | **REJECTED (negligible)** | 44 bytes/request uplink (~64 KB per full batch); no latency effect measurable. Protocol-hygiene note kept here: these headers are RFC-noncompliant on h2/h3 (harmless today); removal is a correctness nicety, not a perf change. |

## Series conclusion

Four of ten kept — the wins were all in cold-start and cache-semantics
territory the earlier series hadn't mapped: import-chain fat (G1), over-fetch
(G4), a config default that silently disabled the persistent cache (G5), and
serial 404-walks (G2). The fetch phase's steady state remains edge-bound and
client-side exhausted (G3 confirms F1 at the mechanism level); per-request
micros (G6-G10) are at the floor, consistent with the F-series.

Stack: G1 -> G4 -> G5 -> G2 (each commit's tree passed the full 1183-test
unit suite; A/B or deterministic measurements per the table). PR publication
from this thread is blocked while #65 (the stack root, docs-only) is open:
the platform's stack namespace (`<thread-branch>/<slug>`) collides with
#65's head ref until that branch is merged/deleted or the PR closed; the
commits are staged on the branch chain ready to publish.
