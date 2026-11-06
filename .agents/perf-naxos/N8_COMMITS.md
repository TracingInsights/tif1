# N8: intended commits (`git commit` is denied in this sandbox; everything is staged)

Apply in this order (one commit each):

1. `perf(naxos-N8): start the niquests warm-up only past one driver's wave on the bridge`

   Files: `src/tif1/first_wave.py`, `src/tif1/async_fetch.py` (docstring),
   `src/tif1/payload_loader.py` (docstring), `tests/unit/test_first_wave.py`,
   `AGENTS.md` (the bridge paragraph).

   A cold one-driver telemetry wave (57 files, ~0.5 s) ran entirely on the
   stdlib bridge in every traced run, while the niquests warm-up it started
   (the import, ~250 modules and ~120-145 ms of CPU under the GIL, then the
   session and its HEADs) finished only after the wave and slowed the wave's
   receive threads. The bridge now counts the requests it serves and starts
   the warm-up after `BRIDGE_WARM_AFTER` (80, more than one driver's laps of
   any session); longer waves move to niquests as before.

2. `perf(naxos-N8): DriverAhead codes to strings without a polars query plan`

   Files: `src/tif1/core_utils/helpers.py`, `tests/unit/test_native_decoder.py`.

   `Series.gather` runs as a query plan in polars 2 (~0.2 ms per lap, ~20 ms
   for the first in a process). A lap's codes (~1k) are now looked up in
   Python and longer arrays index the Series with the codes; the same Series
   (values, dtype, flags, one chunk). A cold polars one-driver frame build
   (57 laps, fresh process) 82 -> 63 ms.

3. `perf(naxos-N8): cold one-driver wave tracing tools and the N8 report`

   Files: `tools/perf/naxos_n8_*.py`, `.agents/perf-naxos/N8*.{md,json,log}`,
   the raw traces in `.agents/perf-naxos/`.
