# Argos series — performance experiments (2026-10-14)

Prior series read and NOT re-run: H1–H11 (`.agents/perf/RESULTS.md`), E1–E10
(`.agents/perf-monaco/RESULTS.md`), F1–F10 + G1–G10 (`.agents/perf-fab5/RESULTS.md`),
K1–K10 (`.agents/perf-kalchedon/RESULTS.md`), L1–L10 (`.agents/perf-sestos/RESULTS.md`),
N1–N10 (`.agents/perf-lindos/RESULTS.md`), T1–T10 (`.agents/perf-tyria/RESULTS.md`),
R1–R10 (`.agents/perf-rhodes/RESULTS.md`), U1–U11 (`.agents/perf-ithome/RESULTS.md`),
V1–V11 (`.agents/perf-halikarnassos/RESULTS.md`), plus the never-run kleitor plan.

## Vantage

Local workstation: 32 cores / 30 GB, Python 3.14.4, pandas 3.0.6, polars 2.0.0,
niquests 3.21.1. Every prior series ran on 1-core sandboxes, so multi-core-only
mechanisms were never measurable before. Network: jsDelivr edge (FRA → BOM) is
cold and rate-limits bursts with 403s from here; a truly-cold Monaco 2026
telemetry fetch takes >1 h with 30 s read timeouts. Live cold A/B is therefore
treated as low-confidence; deterministic offline harnesses (`tools/argos_*.py`)
and the warm-cache benchmark carry the verdicts. An unrelated user workload
(video rendering, load ≈ 28) ran during parts of this series; all offline
numbers are interleaved best-of-N for that reason.

## Pre-hypothesis probes (documented so no future series retries them)

- `pandas` import pulls `pyarrow` + `pyarrow.compute` eagerly on pandas 3.0.6
  (~0.1–0.2 s of the 0.23 s warm import) — pandas-side, not actionable.
- `lz4.frame.decompress` holds the GIL (8 threads 26 ms vs serial 14 ms);
  `compression.zstd.decompress` releases it (50 → 28 ms). Confirms V8 on a real
  multi-core box; lz4 stays single-threaded.
- `pysimdjson` (simdjson `as_buffer` → ndarray without Python floats) has no
  Python 3.14 wheel and fails to build without headers — not a default-dependency
  candidate; not measured further.
- `pd.DataFrame(dict, copy=False)` skips block consolidation: every telemetry
  frame leaves the builder with 19 single-column blocks, every laps frame with
  one block per column (46 blocks on the 2021 Belgian race).
- `pd.to_timedelta(float_ndarray, unit="s")` runs `sequence_to_td64ns` twice per
  call (listlike convert + `TimedeltaIndex.__new__`).
- `DataFrame.query(..., engine="python")` on the driver-laps path tokenises the
  expression per call (~1.7 ms vs 0.4 ms boolean mask on a 1455-row frame).
- First request on a fresh niquests session costs 160–800 ms here (DNS + TLS),
  the second 3–10 ms.

## Ten hypotheses

| # | Hypothesis | Target | Measured with |
|---|-----------|--------|---------------|
| A1 | Build typed telemetry frames with `DataFrame._from_arrays` (consolidated blocks, no per-column sanitize) and `array.array`-based list→ndarray conversion | cold assembly; frame-tier pickle/unpickle; every later frame op | `tools/argos_assembly_bench.py` parity 1452/1452 + interleaved best-of-5 |
| A2 | Vectorized exact replica of pandas' float-seconds→`timedelta64[ns]` cast (`seconds_to_td64ns`) for telemetry `Time` and laps timedelta columns | per-frame `to_timedelta` (~90 µs → ~8 µs); laps `_numeric_seconds_to_timedelta` | bit-exact int64 parity on the corpus + laps columns |
| A3 | Post-return background flush of the deferred payload-tier writes (same writer registry as the U5 frame write) | cold `fetch_all_laps_telemetry` critical path | deterministic flush timing; rows verified at exit |
| A4 | Consolidate the laps frame once after `_process_lap_df`; `Driver.laps` via boolean mask instead of `query(engine="python")` | every `pick_*`, `Driver.laps`, `get_lap`, `iterrows` | interleaved micro on real laps; `.equals` + dtypes parity |
| A5 | `iterlaps`: one vectorized `isna` pass instead of per-row `isna` + `drop` | FastF1-idiom lap iteration | interleaved micro; yielded sequence parity |
| A6 | `Telemetry` time helpers (`slice_by_time`, `get_first_non_zero_time_index`, `integrate_distance`): skip redundant `to_timedelta` coercions on already-typed columns, numpy masks | per-call model ops | interleaved micro on real frames; `.equals` parity |
| A7 | Fix the `http_disable_http3` knob (the replacement adapter drops it) and measure h2 vs h3 on the live bulk fetch | fetch phase CPU/latency; 403/timeout regime | interleaved live batches, 3+3, with status counts |
| A8 | Pre-connect the shared CDN session during `get_session()` (daemon thread) so the first `laps` request skips DNS/TLS | cold laps phase | interleaved first-request latency ×6 |
| A9 | Multi-core process-pool parse+assembly for the cold bulk path (≥4 cores, ≥200 payloads), frames pickled back | post-gather assembly | offline from raw bytes, parity-gated |
| A10 | Alternative bulk transport: niquests vs `curl_cffi` (libcurl h2) vs `aiohttp` (h1.1 × 22) on 300 telemetry files | fetch phase | interleaved live, 3 rounds; keep only if ≥ 1.5x |

Keep rule: deterministic win beyond noise with full parity; live-only wins must
be consistent across interleaved pairs. All kept experiments ship in ONE PR.
