# Thebes series — plan (2026-10-15)

Vantage: 32-core workstation, 30 GB RAM, Python 3.13.15 (uv venv), pandas
3.0.6, polars 2.0.0, numpy 2.5.x. Benchmark corpus: 2026 Monaco Grand Prix
Race (1452 telemetry payloads, 84.4 MB raw JSON, 614 258 rows), the standard
set of every prior series. Parity corpus: every telemetry payload in the
developer's cache for seasons 2018-2025 (8883 payloads, 1.33 GB raw JSON,
read-only copy) plus the 2026 set.

Prior series read and NOT re-run: H1-H11 (`.agents/perf/RESULTS.md`), E1-E10
(`perf-monaco`), F1-F10 + G1-G10 (`perf-fab5`), K1-K10 (`perf-kalchedon`),
L1-L10 (`perf-sestos`), N1-N10 (`perf-lindos`), T1-T10 (`perf-tyria`), R1-R10
(`perf-rhodes`), U1-U11 (`perf-ithome`), V1-V11 (`perf-halikarnassos`),
KR-1..10 (`perf-kroton`), A1-A12 (`perf-argos`). In particular: pydantic
removal, cache tiers/codecs (zlib, zstd, lz4, msgpack, Arrow IPC for *pandas*
frames, columnar arrays rebuilt through the public constructor, raw blobs,
single-blob session pickles, trained dictionaries, mmap, page cache), lazy
imports (polars, pandas rejected), concurrency caps on a *warm* edge (22 vs
44/64), CDN order bake-offs, sharding, full hedging of every request,
uvloop, GC freeze during fetch / GC suspension in loops, thread-parallel and
streamed assembly, process-pool assembly (A9, measured not shipped), aiohttp
(A10, measured not shipped), merged-dict assembly, numpy-first construction
through `pd.DataFrame`, `_from_arrays` assembly, `TimedeltaIndex` casts,
pre-typed laps, executemany, write deferral, frame memoization, negative
cache, 403/404 semantics, preconnect, HTTP/2 default, exact-name event
resolution, laps frame tier, polars laps single-shot assembly.

## Baseline (this vantage, measured 2026-10-15)

| Metric | Value |
|---|---:|
| `import tif1.core` | ~280 ms (pandas 247 ms of it; pyarrow 46 ms inside pandas) |
| Warm pandas (`tools/warm_cache_benchmark.py`, 5 runs) | total 0.51-0.55 s: get_session 0.25-0.29, laps 0.004, telemetry 0.249-0.258 |
| Warm pandas telemetry split | SQL 19 ms + lz4 ~40 ms + `pickle.loads` ~127 us/frame (~80 us of it is the DataFrame skeleton: Index/BlockManager reconstruction) + map/memo overhead |
| Warm polars (frames primed, 5 runs) | see RESULTS.md baseline table |
| Cold Monaco 2026, warm edge (3 runs) | total 7.9 / 30.9 / 43.4 s (telemetry 6.2 / 28.5 / 40.5): two of three runs hit jsDelivr 503s and 30 s read timeouts |
| Cold-edge live probe (2026 Canadian GP Race, 400 files, cap 22) | p50 1.67 s, p90 3.4 s, p99 6.9 s, max 30 s (ReadTimeout); 7.3 files/s |
| Cold-edge live probe, disjoint 150-file slices | jsDelivr cap 22: 2.5 files/s (p50 6.2 s); cap 44: 3.1 files/s with 9 timeouts; after the circuit breaker disabled jsDelivr, Hugging Face served the same session at 25-27 files/s (p50 0.5 s, no errors) at cap 44 and 88 |
| Offline cold assembly (`_create_telemetry_df` x 1452, parsed payloads) | 877-963 ms; `_from_arrays` ~250 us/frame of the ~640 us/frame, sentinel comprehension ~180 ms, DriverAhead inference ~110 ms |
| orjson parse of the 2026 corpus | 228 ms (84 MB); pysimdjson typed decode 228 ms (parity; -46 % only on the sentinel-free 2024 corpus) |

## Hypotheses

| # | Hypothesis | Target | Measurement |
|---|-----------|--------|-------------|
| TH1 | Typed-columnar frame-tier encoding: per-frame rows store the block arrays as raw bytes + a tiny header and are rebuilt through `BlockManager`/`_from_mgr` directly (8 us) instead of `pickle.loads` (127 us, 80 us of it skeleton). Legacy pickled rows stay readable. | warm pandas telemetry (0.25 s) and the background frame write | offline per-frame read/write, fresh-process warm benchmark A/B, parity on the 2026 set |
| TH2 | Direct-block typed assembly: `_typed_telemetry_frame` builds blocks itself (one float64 block via `vstack`, EAs as their own blocks, shared column `Index`) instead of `DataFrame._from_arrays` (which re-sanitizes and re-forms every column). | cold assembly (~0.9 s post-gather), payload-tier warm loads | offline assembly A/B, parity on 2026 + cross-season corpus |
| TH3 | Latency-aware CDN routing + straggler hedging: per-source EWMA latency re-ranks the mirrors per process; a request that exceeds the hedge delay (2x the best source's EWMA, clamped) is re-issued on the next source and the first answer wins. Full hedging of every request (Exp11) stays rejected; this only touches stragglers. | cold-edge loads (minutes on this vantage), 30 s timeouts | deterministic replay simulation of the observed latency distributions; live disjoint-slice A/B on a cold session |
| TH4 | Rust/pyo3 telemetry decoder (`tif1_native`, optional): parses one `_tel.json` into typed numpy arrays with the sentinel rule applied, releasing the GIL so the 22 fetch threads parse in parallel on multi-core hosts. Pure-Python fallback unchanged. | cold parse + list->array conversion (~0.4-0.5 s of GIL time) | offline single-/multi-thread decode A/B vs orjson, pipeline parity |
| TH5 | pysimdjson typed decode (library swap for telemetry payloads). | cold parse | already measured: parity on the 2026 corpus (228 vs 228 ms) — REJECTED, numbers in RESULTS.md |
| TH6 | Cold laps phase: fetch `session_laptimes.json` in the same parallel wave as `drivers.json`/`weather.json`/`rcm.json` (its path does not depend on the driver list) so the laps phase costs one CDN round trip instead of two. | cold `session.laps` / `load(telemetry=False)` (1.1-2.1 s on this vantage) | request-level trace (serial RTTs before/after), live interleaved A/B of the laps phase |
| TH7 | Stale-while-revalidate for the live f1schedule calendar: serve the stale SQLite row immediately and refresh it in a background thread once the TTL expired, instead of a blocking GitHub fetch inside `get_session`. | first `get_session` after 6 h idle | deterministic: injected slow transport, `get_session` latency before/after |
| TH8 | Polars frame tier as Arrow IPC session blobs: one lz4 IPC frame per (session, schema) read with `pl.read_ipc` and sliced per lap, instead of 1452 pickled frames. | warm polars telemetry | offline read/write A/B (probe: 68 -> 23 ms read, 112 -> 30 ms write), fresh-process polars warm A/B, `DataFrame.equals` parity |
| TH9 | Sentinel and string-channel fast paths in assembly: the only sentinel seen in 8883 cross-season payloads is the literal `"None"`; swap it for `null` in the raw bytes before parsing so numeric channels convert with one C call (`np.array(..., dtype=float64)` maps None -> NaN) and `DriverAhead` skips the token scan. | cold assembly (sentinel comprehension ~180 ms, DriverAhead ~110 ms) | offline A/B stacked on TH2, parity on both corpora |
| TH10 | Cold-edge adaptive concurrency: raise the per-batch cap above 22 only while the observed median latency says the edge is cold and no source is failing. | cold-edge throughput | same simulation harness as TH3 plus live disjoint slices |

Rules: every hypothesis runs to a verdict with numbers; parity gates are
frame-by-frame (columns, dtypes, values, row count); cold live measurements
are interleaved or disjoint-slice paired so edge drift hits both arms;
anything rejected is documented with its numbers in RESULTS.md.
