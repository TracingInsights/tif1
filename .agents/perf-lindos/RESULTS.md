# Lindos-Series Performance Experiments — Final

**Benchmark:** 2026 Monaco GP Race telemetry, ~1,452 payloads / 84.4 MB raw, live CDN,
fresh process per run, 1-core sandbox vantage (same class as the K/L-series vantages).

Prior series since v0.7.0 were read and are NOT repeated: E (1-10), F (1-10), G (1-10),
H-series (perf), K (1-10), L (1-10). The N-series below explores territory those series
had not measured: cold-path frame materialization, session-table prefetch persistence,
brotli negotiation, numpy-first Int64 construction, Arrow-native pipelines, a real
pyo3/Rust implementation, and a concurrency ramp.

## Baseline (this vantage, 2026-09-15)

- Cold, steady edge: total median ~9.1-12.1 s (first runs 103-228 s while the CDN edge
  re-warmed; suite methodology interleaves A/B on the re-warmed edge).
- Warm #1 (first load after cold fetch, payload tier + materialize-on-read):
  total 5.37 s — get_session 0.40 / laps 0.32 / telemetry 4.66
- Warm #2+ (frame tier): total 1.41-1.48 s — get_session 0.40 / laps 0.07 /
  telemetry 0.94-1.02
- Component profiles (warm cache, offline):
  - Frame-tier batch read 1.12 s: pickle.loads 0.92 (82%), zstd 0.14, SQLite 0.05
  - Payload-tier batch read 1.13 s: orjson 0.84 (75%), zstd 0.10, SQLite 0.04
  - Per-frame assembly (`_create_telemetry_df` × 1452): 2.55-2.65 s
  - Frame materialize write: pickle+zstd ~0.45 s + per-row SQL 0.36 s
  - `import tif1` 52 ms; `get_session` fresh-process 0.40 s is pandas-import bound

## Hypotheses

| ID | Hypothesis | Verdict | Key evidence |
|----|------------|---------|--------------|
| N1 | Materialize telemetry frames on the cold fetch path (frames already in memory; written in the post-gather bulk write) | **KEPT** | 2nd load (warm #1) telemetry **4.66 → 1.08 s (-77%)**, total 5.37 → 1.97 s. Cold A/B: no median regression (B median 10.81 s vs A 13.28 s; A suffered straggler runs, B consistently 10.4-10.8 s). Frames now reach the frame tier one load earlier. |
| N2 | Brotli Accept-Encoding negotiation (add `brotli`; niquests then negotiates `br` with jsDelivr) | Rejected | Wire size only ~5% smaller than gzip (e.g. 9,389 vs 9,941 B per payload). Interleaved A/B (4+4): median total 11.95 vs 12.37 s (-3.4%), pairs split 2-2 — within noise. Not shipped as a dependency. |
| N3 | `_prefetch_session_tables` persists fetched tables + consults the persistent cache first | **KEPT** | Root cause of the warm-#1 laps anomaly (0.46 s vs 0.07 s): prefetch fetched drivers/weather/rcm over a raw HTTP session, kept them memo-only, and never checked the persistent cache — every fresh process re-downloaded them after a cold `load()`. After fix: warm #1 laps **0.456 → 0.068 s (-85%)**, total 1.98 → 1.67 s; persistent-cache hits now skip the network entirely. |
| N4 | numpy-based Int64/masked-array construction in the typed frame path | Rejected | 4.06 s vs 2.55 s baseline loop on the same corpus (slower), and column/dtype parity breaks without the rename map — consistent with K6's rejection of numpy-first construction. |
| N5 | `executemany` for the frame-tier bulk write | **KEPT** (folded into N1) | 1452-row write pass 0.36 → 0.21 s; same round-trip parity contract (exact frame equality, corrupt-row fallback). |
| N6 | Concurrency slow-start ramp (window 6 → 22, +2 per completion) | Rejected | Interleaved A/B (5+5): B median +8.96% total (11.90 vs 10.92 s); telemetry -2.19% (noise). Ramping delays the TLS warm-up the E-series tuned; no compensating win. Reverted. |
| N7 | Pool sizing alignment (pool_maxsize vs concurrency) | Non-issue | `pool_connections` is auto-sized `max(256, target_concurrency)` with 4x maxsize; no mismatch to fix. |
| N8 | Custom Rust/pyo3 columnar parser + numpy-native assembly | Rejected (measured) | Built a real pyo3 extension (`/tmp/telparse`, hand-rolled columnar JSON scanner emitting numpy arrays, "None"-string channels preserved as lists for parity). All 1452 payloads parse; **pipeline parity 0/1452 mismatches**; but parse 0.90 s vs orjson 0.99 s and full pipeline **2.38 s vs 2.39 s** — no measurable speedup. Remaining costs are pandas construction (parity-bound) and number parsing (orjson already near-optimal). A compiled dependency is not justified by ~0%. |
| N9 | pyarrow.json native parse (`pyarrow.json.read_json`) | Rejected | 1.70 s vs orjson 0.84 s on the same corpus — the generic row-oriented Arrow reader is ~2x slower than orjson on this columnar-dict-of-lists payload shape. |
| N10 | Alternative codecs round: Arrow IPC frame tier / msgspec / zstd -3 | Rejected | Arrow IPC roundtrip 3.05 s vs pickle+zstd 1.42 s (per-frame `from_pandas`+IPC overhead); msgspec decode 1.04 s vs orjson 0.97 s; zstd level -3 compresses 33% faster but blobs are 81% larger (35.8 vs 19.8 MB per session). Also: merged-dict assembly + slicing (1.9 s vs 2.55 s) breaks per-lap dtype parity 1452/1452 (session-wide int/float inference) — confirms F3's rejection at the mechanism level. |

## Shipped outcome (N1 + N3 + N5 combined)

Second-load (warm #1) user experience, Monaco full telemetry:

| Phase | Before | After |
|---|---:|---:|
| get_session | 0.40 s | 0.40 s (unchanged; pandas-import bound) |
| laps | 0.32-0.46 s | **0.07 s** |
| telemetry | 4.66 s | **1.08-1.19 s** |
| total | 5.37 s | **1.91 s (-64%)** |

Third+ loads (frame tier) remain 1.46-1.56 s total (laps 0.07 s, telemetry
0.95-0.96 s). Cold steady-edge stays at parity-or-better: final interleaved
A/B of the combined stack, B median **-2.06% total / -2.90% telemetry**
(A suffered a 43 s straggler run; B's worst was 17.9 s; B never lost the
median on any suite this series). `load()`-only sessions now also warm
drivers/weather/rcm into the persistent cache instead of re-downloading them
in every later process.

## Verification

- Full unit + property suites: **1,293 passed**, coverage 84.52% (threshold 80%).
- Ruff lint + format clean (`src/`, `tests/`, `tools/`); `ty` diagnostics identical to HEAD.
- 5 new contract tests in `tests/unit/test_n_series_contracts.py`
  (N1 network materialization + cache-disable gating, N3 persist + cache-first, N5 bulk-write parity).
- Rust probe artifacts: crate built with pyo3 0.26/numpy 0.26, wheel installed, parity
  verified against the full 1452-payload corpus; probe kept at `tools/lindos_probe_rust.py`.

## Main changed files

- `src/tif1/core.py` — N1 (cold-path frame materialization), N3 (prefetch persist + cache-first)
- `src/tif1/cache.py` — N5 (executemany bulk frame writes)
- `tests/unit/test_n_series_contracts.py` — contracts for N1/N3/N5
- `tools/lindos_*.py` — probes and measurement harnesses (this series)
- `.agents/perf-lindos/*.json` — baseline, N1/N2/N6/final A/B reports
