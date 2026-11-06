# N9 intended commits (git commit was denied in the sandbox; everything is staged)

1. `perf(naxos-N9): write a bulk wave's payload-tier rows while the wave fetches`
   Files: `src/tif1/async_fetch.py`, `tests/unit/test_naxos_n9_streamed_writes.py`.

   A cold fetch_all_laps_telemetry() is link-bound (~10 MB/s here) and leaves
   the GIL mostly idle during the wave, but its payload-tier flush (orjson
   dumps + zstd + SQLite, ~1.35 s for a race) only started after the gather,
   where it fought the GIL-bound frame build (1.9 s instead of 0.75 s) and
   was joined again at exit. fetch_multiple_async(defer_telemetry_writes=True)
   now hands the rows to _StreamedTelemetryWrites, whose registered writer
   thread persists them as they arrive (it exits when idle or closed, so it
   cannot hold the exit join). Without threads (Pyodide) the post-gather flush
   remains. The rows written are byte-identical.

2. `perf(naxos-N9): link, trace and cold A/B probes`
   Files: `tools/perf/naxos_n9_link.py` (curl link test),
   `tools/perf/naxos_n9_trace.py` + `naxos_n9_analyze.py` (cold timeline:
   bodies, writers, per-thread CPU, stack samples), `naxos_n9_micro.py`
   (post-wave work costs), `naxos_n9_cold_bench.py` (copy of
   delphi_cold_bench with p90 and raw runs), `naxos_n9_cache_diff.py`
   (row-level diff of two cold-primed caches).

3. `docs(naxos-N9): report, progress and A/B results`
   Files: `.agents/perf-naxos/N9*.md`, `.agents/perf-naxos/N9_cold_*.json`.
