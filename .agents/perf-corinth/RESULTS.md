# Corinth round: results

Date: 2026-11-06. Control: `main` after the Naxos round (PR #126, commit
552c357). Box: 32 cores, 30 GB, Linux, CPython 3.13 (3.14.8 for C5),
pandas 3.1.0, polars 2.1.0, tif1-native 0.2.1. Agents: Opus 5.5 (high; C8
and C10 xhigh), headless, two per account (contact: C1, C8; hi: C2, C5;
madhu: C3, C6; info: C4, C9; ceo: C10, C7). Same method as Naxos: sparse
`git clone --shared` trees, no worktrees, one cache copy per agent, staged
patches integrated on `perf/corinth`.

The round targeted user flows earlier rounds never measured (the benchmarks
so far were one 2025 race): season loops, pre-2018 sessions, the Ergast
interface, current-season cache refreshes, Python 3.14, plotting, the CLI,
the tel.v2 decoder, the schedule APIs and a per-session bundle file.

## Hypotheses and verdicts

| ID | hypothesis | verdict | effect (agent's own A/B) |
|---|---|---|---|
| C1 | season loop, cold | SHIP | `get_session` of round r right after r-1 prefetches round r+1's session-level files and Stats classification over the first wave's connections (`session_lookahead`, default on). Season loop wall -41% pandas (4236 -> 2519 ms), -35% polars (5/5); a `load()` loop -27%, a telemetry loop -18%; single sessions and warm paths unchanged |
| C2 | pre-2018 sessions, cold | SHIP | no lap-timing requests before 1996, no pit stops before 2011, the round's Stats files in one first wave, parallel rounds: 1998 Monaco 2.06 -> 0.72 s pandas, 2.81 -> 0.52 polars; cold `laps` ~1 s -> 3-38 ms; fuzzy event names on polars -28% warm |
| C3 | `tif1.ergast`, cold | SHIP | Stats files over the stdlib client while pandas imports, `events.json` and the predicted round files in one wave: cold queries -34..-46% pandas (`get_race_results(season=2024)` 1084 -> 638 ms), -28..-39% polars; `from tif1.ergast import Ergast` no longer imports pandas |
| C4 | current-season freshness | SHIP | single requests before niquests exists go over the stdlib client (no niquests import, no 250 KB CRL under the GIL); a current-season results/quali/sprint file fetched 14+ days after its race is kept permanently (every 2026 file was committed once, never rewritten); stored season index for finished rounds. Expired-TTL scripts -14..-44%, cold -29..-48% |
| C5 | Python 3.14 | SHIP | GC promotion gate by version (3.14.5+ and 3.15 betas reverted the incremental GC: discuss.python.org/t/107014) and the per-lap frame build under `suspend_gc(promote=True)`: on 3.14 pandas all_tel -5%, notebook -6.6%, polars laps -11%; 7-35 ms of GC gone from every `get_session` |
| C6 | plotting | SHIP | lazy matplotlib/cycler in `tif1.plotting`, rapidfuzz only for fuzzy lookups, `plot_driver_laptimes` without seaborn (artists and Agg pixel SHA-1 identical; a test compares with seaborn every run): `import tif1.plotting` 0.5-0.8 s -> 0.02 s, a first chart 2.4 -> 1.3 s |
| C7 | CLI | SHIP | session commands on polars when installed (unless the user set `lib`), pandas-free standings, dist-info version: warm 0.31-0.56x, cold 0.56-0.70x; 209 command lines identical; fixes `tif1 fastest` on polars-configured installs |
| C8 | tel.v2 decode | SHIP (needs tif1-native 0.3.0 + published files) | whole-file decode in one native call, GIL released: race 472 -> 298 ms one thread, 276 -> 74 ms with 8 threads; 100 MB/s cold `fetch_all` 3.07 -> 2.48 s pandas; bit-identical |
| C9 | schedule without pandas | SHIP | `get_event_schedule(..., lib="polars")` / `get_events_remaining`: -53..-55% on the schedule calls, polars season loop 726 -> 368 ms, never imports pandas; pandas schedule build -33% in process (wall within noise) |
| C10 | session bundle file | SHIP (off until published) | `session.v1.bin`: a session's small files in one request; six-race loop -15..-24%, `load()` + results -15% pandas / -40% polars, 80-200 ms per session at 40 ms RTT; off: identical requests |

## Integration notes

- Order on `perf/corinth`: C2, C9, C4, C1, C10, C6, C7, C3, C5, docs and
  the CLI fix, C8, then results and artifacts.
- C3 and C4 each added an on-demand stdlib client. C4's set the wave's
  `_client`, which opens the telemetry bridge, whose niquests hand-over needs
  a first wave's URLs. Unified: one process-wide client (`_build_client`,
  `_shared`) for single requests and the wave; only a first wave sets
  `_client`. Tests assert single requests never open the bridge.
- C1 + C4: the wave builds its client through C4's shared builder and still
  signals C1's prefetch (`_client_settled`). C10 + C2: the bundle choice
  lives in C2's modern-season branch of the first wave.
- CLI (integrator): `_session_lib()` keeps C7's polars default but honours a
  `lib` the user set in `TIF1_LIB` or a `.tif1rc` (`Config.is_user_set`,
  recorded from the file and the environment at load, not from `set()`).
- `test_thread_gates.test_frame_writer_runs_inline` compared the global
  writer list with `[]`; on a shared pytest worker another test's writer can
  still be listed. It now checks that the inline path adds none.
- C8's Rust kernels are tested by the CI `native` job (the main venv's
  0.2.1 wheel skips them). The agents' 2 MB parity dumps were left out.

## Combined tree vs control

Cold, live CDN, fresh empty cache per run, 3 interleaved reps
(`flows_ab.log`): season loop pandas 3673 -> 2225 ms (-39%), polars 2844 ->
2017 (-29%); 1998 Monaco pandas 2023 -> 559 (-72%), polars 2304 -> 478
(-79%); Ergast queries 1358 -> 630 (-54%); current-season race pandas 1203
-> 585 (-51%), polars 960 -> 367 (-62%); `get_event_schedule` 447 -> 419
(2/3). All other rows 3/3.

Warm (8 pairs, then a 12-pair recheck of the rows that looked worse while
C8 was benchmarking): every workload within noise on both backends
(`integ_warm_ab_*.json`, `integ_warm_recheck_*.json`).

## Final tree

Unit suite 4377 passed, 26 skipped; ruff (whole repository), ruff format,
ty, pre-commit hooks clean; parity 0/133 (pandas), 0/131 (polars); perf gate
0/120 regressed (no baseline rewrite: the gate's warm phases did not move);
docs STE check 0 violations, no broken links.
