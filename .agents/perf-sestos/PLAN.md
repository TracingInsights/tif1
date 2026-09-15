# Sestos-series performance experiments — 2026-09-14 — PLAN

Tenth-series follow-up. Prior series are NOT re-run: H1–H11
(`.agents/perf/RESULTS.md`), E1–E10 (`.agents/perf-monaco/RESULTS.md`),
F1–F10 + G1–G10 (`.agents/perf-fab5/RESULTS.md`), K1–K10
(`.agents/perf-kalchedon/RESULTS.md`). Anything measured there (CDN order/
sharding/racing/hedging, concurrency caps, uvloop, GC, msgpack, executemany,
numpy-first frames, merged-dict frames, lazy imports, keepalive, pool
micros, parse offload, validation pipeline, cache codecs zstd-1/zlib,
write-back semantics, 4xx exhaustion) is out of scope here.

Benchmark: 2026 Monaco GP Race full telemetry (~1452 payloads ≈ 84 MB raw),
live CDN chain. Cold: `tools/monaco_telemetry_cold_benchmark.py` (fresh
process + throwaway cache, 5-run suites). Warm: `tools/warm_cache_benchmark.py`
(persistent primed cache, fresh process per run). Interleaved A/B via
`tools/monaco_ab_suite.py` with control = main @ 0c074a0 at /tmp/tif1-control.

## Baseline (this vantage, 2026-09-14)

- Cold (warm-edge runs): total 11.39 / 10.08 / 9.69 s → median **11.39 s**
  (telemetry median 10.32 s). First two suite runs hit the truly-cold edge
  (190.4 / 204.7 s) and re-warmed it — excluded as edge-cold, kept in the JSON.
- Warm (primed persistent cache): total 6.10 / 5.34 / 6.09 s → median **6.09 s**
  (telemetry median 5.13 s).
- Warm phase split (cProfile on the warm telemetry phase): batch cache read
  (SQL + zstd + orjson) ≈ 1.1 s, per-frame assembly (`_create_telemetry_df` ×
  1452) ≈ 3.8 s, missing-file network tail ≈ 1.0 s (LEC/66, STR/58, SAI/72
  404 on every CDN — re-probed on EVERY load).
- `fetch_all_laps_telemetry()` called twice in one process re-reads and
  re-assembles every frame (no frame memo reuse).

## Ten hypotheses

| # | Hypothesis | Target | Measurement |
|---|-----------|--------|-------------|
| L1 | Brotli transfer encoding (install brotli so niquests advertises `br`) | cold fetch bytes | PROBE: response sizes per Accept-Encoding |
| L2 | Persistent negative-result cache for everywhere-missing payloads (all-CDN 4xx verdicts remembered; TTL) | warm+cold missing-file tail (~1.0 s) | warm A/B 3+3 + unit tests |
| L3 | `fetch_all_laps_telemetry_async` reuses memoized frames (session-memo `telemetry_df` tier) on repeat calls | repeat per-lap telemetry calls in one process | deterministic double-call harness |
| L4 | Multi-connection batch fetch: N parallel niquests sessions (own h3/h2 connections) instead of 1 multiplexed connection at 22 streams | cold fetch wave (flow-control bound?) | steady-state probe (300 files) then live A/B if ≥10% |
| L5 | SQLite `PRAGMA mmap_size` for warm reads | warm batch read (~1.1 s) | offline, primed cache DB |
| L6 | zstd trained dictionary for telemetry-tier blobs | warm read decompress + ratio | offline, 1452 real blobs |
| L7 | Persistent materialized-frame tier: pickle assembled per-(driver,lap) frames (zstd) on warm loads; subsequent warm loads skip read+assembly | warm read+assembly (~4.9 s) | parity 1452/1452 + warm two-run protocol |
| L8 | Overlap the deferred telemetry cache-write flush (GIL-releasing zstd/SQLite) with post-fetch frame assembly (GIL-bound) | cold total (flush ~0.8 s serial today) | live cold A/B 5+5 |
| L9 | Skip the per-request JSON-tier `cache.get` probe for `_tel.json` paths (telemetry lives in its own table; batch-check both tiers up front) | cold fetch wave executor overhead | offline + live A/B |
| L10 | `get_session` schedule path: vendored schedule payload is parsed+validated on every process — measure and trim (per-year lazy validation) | get_session (~0.4–0.65 s) | deterministic subprocess timing |

Everything is implemented and measured to completion; only measured wins are
kept and combined into a single PR.
