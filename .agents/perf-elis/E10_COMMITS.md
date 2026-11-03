# E10: intended commits (`git commit` is denied in this sandbox; everything is staged)

The staged tree holds two logical commits. To split them, commit E1's files first
(the inherited base), then the rest.

## 1. `perf(elis-E10): base: E1`

The E1 work this worktree started from (lazy telemetry in `load()`), unchanged
except for one test fixture line noted in commit 2: every file listed in
`.agents/perf-elis/E1_COMMITS.md`, plus E1's reports and raw data under
`.agents/perf-elis/E1*`.

## 2. `perf(elis-E10): pipeline cold lazy telemetry waves through one prioritised stream`

Cold `load(telemetry="lazy")` waves run on one background event loop
(`core_utils/telemetry_stream.PriorityFetchStream`): the laps a caller waits for
go first, each lap is fetched once whatever its priority, and once the waves
follow laps-row order the next driver's laps fill the free slots (the tail of a
wave, the time spent on its frames). A cold loop over every lap of the 2025
Bahrain race after a lazy load drops from ~11.3 s (E1: 20 driver waves) to
~8.5-9.0 s, below the eager load; a four-driver notebook in another order is
unchanged (no background traffic). `fetch_all_laps_telemetry()` closes the
stream and memoizes its payloads. Interpreter exit cancels the stream's
requests in flight; `CDNManager`'s hedged race now cancels its child tasks
when it is cancelled. Config keys `lazy_telemetry_lookahead` (default 1; -1
every pending driver; 0 E1's per-driver waves) and
`lazy_telemetry_background_requests` (default 22).

Files:
- `src/tif1/core_utils/telemetry_stream.py` (new)
- `src/tif1/core.py`, `src/tif1/async_fetch.py`, `src/tif1/cdn.py`, `src/tif1/config.py`
- `tests/unit/test_lazy_telemetry_stream.py` (new),
  `tests/unit/test_lazy_telemetry_load.py` (fixture pins lookahead 0: those
  tests count one batch per wave)
- `AGENTS.md` (the new module)
- `docs/concepts/sessions.mdx`, `docs/api-reference/core.mdx`,
  `docs/api-reference/config.mdx`, `docs/reference/environment-variables.mdx`
  (`telemetry="lazy"`, the three config keys / env vars)
- `tools/perf/elis_e10_cold_bench.py`, `tools/perf/elis_e10_lazy_parity.py`,
  `tools/perf/elis_e10_stream_overhead.py`, `tools/perf/elis_e10_stream_check.py`,
  `tools/perf/elis_e10_campaign.sh`
- `.agents/perf-elis/E10.md`, `.agents/perf-elis/E10_*.json`, this file
