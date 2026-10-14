# Argos series — results (2026-10-14)

See `PLAN.md` for the hypothesis list, the vantage description and the
pre-hypothesis probes. Numbers below are filled in as each experiment
completes.

## Verdict table

| # | Hypothesis | Verdict | Key evidence |
|---|-----------|---------|--------------|
| A1 | `_from_arrays` typed telemetry frames + `array.array` channel conversion + one-pass sentinel coercion | **KEPT** | real corpus (1452 frames, 675 rows, 5 sentinel channels on 1324 frames), in-process interleaved 5 pairs: candidate faster 5/5, −7…−16 % per pair, best 1145 → 1036 ms (−9.6 %); **1452/1452 frames identical** (cols, dtypes, rows, values); blocks 27 588 → 10 286 (2.7x fewer, so copy/slice/pickle of every frame is cheaper: synthetic copy() 150 → 41 µs) |
| A2 | vectorized seconds→timedelta64[ns] (telemetry `Time` + laps timedelta columns) | **KEPT** | 91 → 8 µs per 400-sample channel; bit-exact (int64 view) on all 1452 corpus frames (inside the A1 parity gate), 200 random frames incl. NaN/negatives, and the laps `_numeric_seconds_to_timedelta` contract (NaN→NaT, name kept) |
| A3 | post-return deferred payload flush | **KEPT** | flush of the 1452 real payloads (orjson dumps + zstd + SQLite) = 277-293 ms on this box (0.78 s on the K-series sandbox) now leaves the cold critical path; joined by close()/invalidate()/clear()/atexit; contract test verifies rows after the join |
| A4 | laps consolidation + mask-based `Driver.laps` | **KEPT** | 2021 Belgian race laps (60 rows, 46 → 17 blocks): pick_driver 1.27 → 0.66 ms, pick_quicklaps 1.04 → 0.66 ms, iloc slice 5.8 → 3.1 ms; `.equals` + dtypes parity |
| A5 | copy-free `pick_fastest` / `_filter_valid_laptimes`, vectorized `iterlaps` | **KEPT** | pick_fastest 2.74 → 1.19 ms (with A4), filter 2.03 → 0.74 ms, iterlaps 18.3 → 1.9 ms (60 laps); yielded sequence identical (index, values, name, dtype) |
| A6 | Telemetry slicing helpers | **KEPT (small)** | real 1989-row frame: get_first_non_zero_time_index 0.15 → 0.05 ms, slice_by_mask 0.52 → 0.33 ms, slice_by_time 0.80 → 0.54 ms; `.equals` parity incl. pad |
| A7 | HTTP/3 knob fix + h2 vs h3 live A/B | knob fixed (**KEPT**); default decision below | before: `disable_http3=True` still negotiated h3 (version 30) from the 2nd request; after: stays h2. 150-file batches (3 interleaved pairs): h2 faster 3/3 (0.66-0.70 vs 0.74-1.12 s steady state; first pair 3.95 vs 16.98 s with edge warm-up). Full session, control code (3 pairs): h3 143.1 / 10.0 / 12.1 s vs h2 11.6 / 10.2 / 7.8 s — h2 faster 2/3, and the only collapse (143 s) was h3; On the candidate code (A12 in place, quiet machine): h3 4.37 / 5.24 / 6.18 s vs h2 4.19 / 4.60 / 6.27 s total — h2 faster 2/3 pairs. **Default flipped to HTTP/2-only** (`http_disable_http3=True`, reversible via `TIF1_HTTP_DISABLE_HTTP3=false`): 5/6 full-session pairs and 3/3 small-batch pairs at parity-to-faster, and every collapse regime observed this series (143 s, 614 s, the hours-long first run) was over h3 |
| A8 | CDN pre-connect during `get_session()` (cold sessions only) | **KEPT (small)** | live interleaved 6+6: laps_s median 0.404 → 0.358 s (−12 %), on faster 4/6; 8+8: 0.673 → 0.631 s (−6 %), on faster 6/8. Warm-load check found the thread cost +0.1-0.2 s of GIL time when nothing needs the network, so the hook is skipped when the session is already cached |
| A9 | process-pool parse+assembly | **MEASURED, NOT SHIPPED** | raw-bytes corpus, parity 0/1452 mismatches: serial parse+build 1294 ms → pool(8) 414 ms incl. unpickle (−68 %), pool(16) 359 ms; worker start-up 340-580 ms (would overlap the fetch). Not shipped: needs raw-bytes plumbing through the fetch path and N pandas worker processes (~1 GB RSS at 8 workers) for ~0.6 s of a network-bound cold load; documented as the next cold-path lever for multi-core users |
| A10 | alternative bulk transports (niquests vs curl_cffi vs aiohttp) | **MEASURED, NOT SHIPPED** | 150 files warm edge, 3 interleaved rounds: niquests 0.77 s, curl_cffi 0.80 s, aiohttp 0.47 s medians; full session (1455 files / 84 MB, 2 rounds): niquests 3.37-3.47 s, aiohttp 2.66-2.76 s (−21 %), curl_cffi 2.47 / 17.1 s (erratic, 12 non-200). The raw transport is not the bottleneck once A12 lands (the loop-side blocking was); a client swap would touch DoH resolvers, h2/h3, retries, the `InMemoryTransport` test seam and every fetch path for ~0.7 s of a 3.4 s transfer — documented, not shipped |
| A12 | Resolve niquests' lazy multiplexed response inside the worker hop (first `status_code` access was on the event-loop thread) | **KEPT** | instrumented production batch: 1461 lazy resolutions, **9.7 s of a 10.2 s wall on the loop thread**, CPU 3.6 s; interleaved parse-phase A/B (1455 payloads, warm edge, load 15-39): control 21.0 / 65.2 / 66.7 s vs candidate 5.8 / 42.7 / 31.9 s — candidate faster 3/3 (−72 / −34 / −52 %); clean-load numbers below |
| A11 | 403 = per-file refusal (no CDN health penalty) | **KEPT** | deterministic sim (`tools/argos_403_sim.py`), 300 files / 22-wide, 20 % cold on jsDelivr, 5 seeds: control median 1.33 s (min 1.11, max 2.02; 3.39 s in the first run — jsDelivr disabled in 2/5 seeds, every later file through the 300 ms mirror) → candidate median 0.68 s (max 0.76, never disabled); 5 % cold: 0.49-0.55 → 0.34-0.38 s |

## Live finding that motivated A11

Every truly-cold run from this vantage logged
`403 Client Error: Forbidden` for jsDelivr followed by
`CDN source 'jsDelivr' disabled after 3 failures`; the 403 body is
"Package size exceeded the configured limit of 50 MB" — jsDelivr refuses to
fetch files it has not cached from the (over-limit) `2026` data repository,
intermittently and per file (a 40-file burst: 34 × 200, 1 × 403, 5 × 404
"Failed to fetch from GitHub"). The old client treated each 403 as a health
failure and sent the rest of the session through the Hugging Face mirror
(~300 ms+ per file plus 30 s read timeouts here). Structural fix is on the
data side (keep per-year repos under 50 MB or put a limit-free mirror first).

## End-to-end cold A/B (control = main @ 93fd74f, candidate = this branch)

`tools/monaco_ab_suite.py`, 3 interleaved pairs, fresh process + throwaway
cache per run, edge already warm for the Monaco 2026 files, machine load
15-20 from an unrelated render job (both arms equally affected):

| pair | control total / telemetry | candidate total / telemetry |
|---|---:|---:|
| 1 | 86.5 / 83.1 s | 53.0 / 49.9 s |
| 2 | **614.4 / 610.5 s** (collapse: h3 + 403 → jsDelivr disabled → timeouts) | 10.7 / 9.8 s |
| 3 | 13.2 / 12.4 s | **10.65 / 9.84 s** (−20 %) |

Candidate faster in 3/3 pairs; medians 86.5 → 10.7 s total (−88 %), best case
13.2 → 10.65 s (−20 %). The candidate never entered the collapse regime; its
53 s first run was the edge re-warm that both arms pay once (A ran first).
Raw transfer of the same 84 MB with the production session is 3.4 s on this
link (`tools/argos_phase_split.py --mode raw`), so the remaining telemetry
phase is ~3.4 s network + ~1 s assembly + the in-fetch parse/decode that
A12 no longer serialises on the loop.

## Warm-cache A/B (frame tier primed by each tree, 6 interleaved pairs, load 2-12)

`tools/warm_cache_benchmark.py`, fresh process per run, control cache
`/tmp/tif1-warm-ctl`, candidate cache `/tmp/tif1-warm-cand`:

| phase | control median / min | candidate median / min | candidate faster |
|---|---:|---:|---|
| get_session | 0.314 / 0.180 s | 0.314 / 0.203 s | 1/6 (parity) |
| laps | 0.062 / 0.036 s | 0.045 / 0.032 s | **6/6** |
| telemetry | 0.370 / 0.236 s | 0.252 / 0.239 s | 3/6 (median −32 %, min parity) |
| total | 0.747 / 0.461 s | 0.670 / 0.478 s | 3/6 |

An earlier 7-pair run with the ungated A8 hook showed the candidate *slower*
(laps +31 ms, telemetry +65 ms): the pre-connect thread's session set-up
competed for the GIL on a load that needs no network. Gating the hook on
`_session_cache_available()` removed that regression; the consolidated
frames (A1) unpickle 22 % faster (206 → 160 ms for 1452 frames, measured
directly on the two tiers).
