## Summary

Built and measured on top of the `polars-2-upgrade` commit (93fd74f), which this branch therefore includes.

Argos performance series: 12 hypotheses measured on a 32-core workstation (every prior series ran on 1-core sandboxes), 9 shipped in this PR, 3 measured and documented. Full log: `.agents/perf-argos/RESULTS.md`; plan and vantage notes: `.agents/perf-argos/PLAN.md`; harnesses: `tools/argos_*.py`.

### Shipped

| # | Change | Measured effect |
|---|--------|-----------------|
| A12 | Resolve niquests' lazy multiplexed response inside the worker hop instead of on the event loop | 1461 resolutions = 9.7 s of a 10.2 s batch were spent blocking the loop; parse-phase A/B faster 3/3 pairs (−34…−72 %). Cold end-to-end A/B (3 interleaved pairs): candidate faster 3/3, steady-state 13.2 → 10.65 s (−20 %), medians 86.5 → 10.7 s (control collapsed once to 614 s). |
| A11 | 403 is a per-file refusal (like 404), never a CDN health failure | jsDelivr 403s ("Package size exceeded the configured limit of 50 MB") used to disable jsDelivr for the process after 3 files; sim median 1.33 → 0.68 s per 300-file batch |
| A1 | `DataFrame._from_arrays` typed telemetry frames, `array.array` channel conversion, one-pass sentinel coercion | assembly −7…−16 % per pair, 1452/1452 frames identical; 2.7x fewer blocks → frame tier unpickle −22 % |
| A2 | Vectorized bit-exact float-seconds → `timedelta64[ns]` | 91 → 8 µs per channel; laps timedelta columns too |
| A3 | Deferred payload-tier flush runs after `fetch_all_laps_telemetry` returns (joined writer) | 277–293 ms off the cold critical path here (0.78 s on the K-series sandbox) |
| A4 | Consolidated laps frame; mask-based `Driver.laps` | 46 → 17 blocks; `pick_driver` −48 %, slices ~2x |
| A5 | Copy-free `pick_fastest`/`_filter_valid_laptimes`; vectorized `iterlaps` | `pick_fastest` 2.74 → 1.19 ms; `iterlaps` 18.3 → 1.9 ms per 60 laps |
| A6 | `Telemetry.slice_by_time` / `slice_by_mask` / `get_first_non_zero_time_index` | −32 % / −36 % / −67 % |
| A7 | `http_disable_http3` actually disables HTTP/3 (adapter dropped the switch) | With the switch working, HTTP/2 was at parity or faster in 5/6 full-session and 3/3 small-batch interleaved pairs, and every stalled fetch seen this series ran over HTTP/3, so **HTTP/2 is now the default** (`TIF1_HTTP_DISABLE_HTTP3=false` re-enables QUIC). |
| A8 | CDN pre-connect during `get_session()` for uncached sessions (`cdn_preconnect`) | cold laps phase −6…−12 %, faster in 10/14 interleaved pairs; skipped on cached sessions |

### Measured, not shipped

- A9 process-pool parse+assembly: −68 % on the post-gather phase with 8 workers (parity 0/1452 mismatches), but needs raw-byte plumbing through the fetch path and ~1 GB of worker RSS.
- A10 alternative transports: aiohttp −21 % on the raw 84 MB transfer, curl_cffi erratic; the transport is not the bottleneck once A12 lands.

### Live finding

Every truly-cold fetch from this vantage hit jsDelivr 403s (the per-year data repositories exceed jsDelivr's 50 MB package limit, so uncached files are refused intermittently). The structural fix is on the data side (keep per-year repos under 50 MB or make a limit-free mirror primary); A11 stops the client from amplifying it.

### Verification

- `uv run pytest tests/unit/` — 1440 passed (1425 existing + 15 new contract tests)
- `uv run pytest tests/property/` — 80 passed, 1 pre-existing timing failure (`test_fetch_driver_laps_parallel_timing`, fails identically on the untouched control tree)
- `uv run ruff check src/ tests/ tools/`, `uv run ruff format --check`, `uv run ty check src/tif1` — clean
- `tools/ste_check.py docs/ --baseline tools/ste_baseline.json` — ok
- New contract tests: `tests/unit/test_argos_series_contracts.py` (15 tests)
