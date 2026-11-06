# Naxos round: results

Date: 2026-11-06. Control: `main` after the Gortyn round plus PR #125
(commit d28ce70). Box: 32 cores (i9-13900HX), 30 GB, Linux, CPython 3.13,
pandas 3.1.0, polars 2.1.0, numpy 2.5.4, tif1-native 0.2.1. Reference
session: 2025 Bahrain Grand Prix race and qualifying. Agents: Opus 5.5
(high; N4 and N7 xhigh), headless, spread over five accounts (contact: N2,
N5, N6, N10, N11; hi: N1, N8; madhu: N3, N9; info: N4; ceo: N7).

Method: as Delphi/Elis/Gortyn (`BRIEF.md`), with one change: no git
worktrees (disk was 96% full). Each agent worked in a sparse
`git clone --shared` (`src/`, `tests/`, `tools/`, `native/` sources, ~25 MB)
with its own warm cache copy, and ran both A/B arms on that one copy. The
integration clone (`perf/naxos`) has the docs checked out. Agents' `git
commit` was denied in the sandbox: every tree was staged and integrated by
patch; the integrator's commits are built from recorded trees.

## Baseline (control, fresh process, ms)

Warm pandas: import+get_session 470 (`import pandas` 341 of it), laps 482,
all_tel 560 (103), notebook 744; warm polars: 210, 216, 362 (117), 583 (load
116, analysis 158). Cold (live CDN): pandas laps 590, notebook 10-12 s,
all_tel 9.3-10.4 s; polars laps 355, notebook 9.2-9.9 s. The link caps at
~10 MB/s (N9: 22 parallel curl downloads of a race's 1128 files, 69 MB, in
6.8-9.0 s).

## Hypotheses and verdicts

| ID | hypothesis | verdict | effect |
|---|---|---|---|
| N1 | numpy's import without OpenBLAS's thread-pool start on the critical path | DROPPED (maintainer decision) | one-thread OpenBLAS start, pool restored inside numpy's import, workers pinned to 4 CPUs and unpinned by a daemon thread: pandas import_get_session -10% (10/10), laps -5..-10%, all_tel -2..-9%, polars notebook -7%; parity exact. Dropped: tif1 must not change other libraries' thread placement or the process environment (failure mode: BLAS single-threaded for the process). Report `N1.md`; branch kept |
| N2 | warm polars eager `load()` keeps driver rows lazy | SHIP | eager `load()` 125-157 -> 9-14 ms; load_only wall -34% (7/8), load + one driver -32% (8/8); notebook even; frames and `fetch_all` order identical. With the lazy default this only affects `telemetry="eager"` / `load_telemetry_lazy=False` |
| N3 | polars telemetry arithmetic without numpy or query plans | SHIP | warm polars notebook 716 -> 596 ms (13/16), analysis 162 -> 18 ms; numpy not imported; parity 0/131. Found the control's polars resample non-deterministic at 1 ulp on some inputs (111/763 values); the new kernels are deterministic |
| N4 | defer heavy submodules pandas/polars import | REJECT | pandas/polars bind almost every heavy module by name at import; the deferrable remainder is ~5-10 ms, not measurable on whole-process walls |
| N5 | interpreter exit | REJECT | exit is 15-34 ms and at its floor (CPython module teardown, kernel page freeing); every knob moved the cost or made it slower |
| N6 | pandas per-lap telemetry overhead | SHIP (small) | notebook lap loop -7..-10 ms of ~140 (6/8 in three runs); car_data_loop neutral; ~110 of the loop's 140 ms is row reads/decodes, timedelta boxing and pandas CoW bookkeeping, not per-lap Python |
| N7 | tel.v2 columnar format with predicted channels | SHIP (reader off until the files are published; needs tif1-native 0.3.0) | derived channels stored as ULP residuals against bit-exact predictions (interp, spline, integration, acceleration; Rust kernels + numpy twins): race 52.5 -> 10.8 MB (gzipped JSON 71.3), qualifying 17.8 -> 4.5; 0 mismatches on 1405 laps. Local CDN at 8 MB/s: pandas cold all_tel 9.5 -> 4.3 s, notebook4 2.75 -> 1.72, driver_tel 1.14 -> 0.89 (8/8 each); polars all_tel 9.4 -> 4.2. 100 MB/s link: all_tel 3.96 -> 3.70 (7/8). Decode 4.1 ms per file (v1 2.3) |
| N8 | cold one-driver wave tail | SHIP | no edge misses or hedges in the timelines: the niquests warm-up started by the stdlib bridge competed with the wave and never served it, plus a polars query plan per lap. Cold pandas driver_tel 1926 -> 1582 ms (18/24), p90 3563 -> 2397; polars -13% (15/24) |
| N9 | is the cold full load bandwidth-bound? | SHIP | the wave is link-bound (~10 MB/s), but 1.9 s of serial CPU followed it (frame build + payload-tier flush fighting for the GIL) plus ~0.6 s at exit. The flush now runs in a writer thread during the wave: post-download tail -1.0..-2.2 s (8/8, 5/8, 7/8, 8/8); polars cold notebook -8.8% (8/8), pandas all_tel -10% (7/8) |
| N10 | warm `fetch_all` and frame builds | SHIP | `gc.freeze(); gc.unfreeze()` after GC-suspended bursts and the first heavy imports: GC in fetch_all 9.7 -> 0.2 ms, in get_session 26-33 -> 0. pandas all_tel 866 -> 759 (10/12), polars all_tel -10% (9/12), laps -7%, notebook -5% |
| N11 | the cold lazy path (the new default) | PARTIAL -> SHIP with the integrator's loop rule | a driver's lone first touch fetches one lap: one driver's fastest-lap telemetry 2947 -> 1051 ms pandas, 2774 -> 915 polars; four drivers 4236 -> 1124, 4439 -> 1493 (8/8 each). Cost: a loop over one driver's laps +6..+21%, fixed at integration (below). Deferring the stream's payload flush: REJECT |
| (lazy default) | `load_telemetry_lazy=True` | SHIP (maintainer decision) | cold four-driver notebook ~10.0 -> ~3.0 s, laps-only load 8.5 s -> 0.13 s, every-lap loop 10.3 -> 9.2 s; frames identical; documented as a fastf1 difference; `load(telemetry="eager")` added |

## Integration notes

- Order on `perf/naxos`: lazy default, N10, N3, N2, N6, N8, N11, N9, the
  loop rule, N7, then docs/changelog/baseline.
- N2 and the lazy-default branch fixed the same lap-order bug of
  `fetch_all_laps_telemetry()` after a warm polars lazy load; one fix kept.
- N11 + N2: both add a method at the same place (kept both). Tests that
  assumed `load()` is eager now pass `telemetry="eager"`; tests that reach
  single laps use `get_driver(d).get_lap(n)` instead of an `iterlaps()`
  search.
- Loop rule (integrator, N11's sketch): `Session._note_lazy_telemetry_walk`,
  called when `iterlaps()` starts on a lazy session, marks drivers with two
  or more laps in the loop as touched, so the loop starts with the driver's
  wave; one lap per driver keeps the lone fetch. Cold A/B against the same
  tree with the rule off (8 reps): pandas driver_loop 1251 -> 1182 ms (5/8,
  loop phase 620 -> 512), polars 1093 -> 1043 (6/8); fastest4 unchanged
  (5/8, 4/8). `integ_walk_cold_{pandas,polars}.json`.
- N7's Rust kernels are not in tif1-native 0.2.1: the CI `native` job builds
  the extension from the commit and runs the kernel tests the main venv
  skips. Release tif1-native 0.3.0 before turning the reader on.
- The agents' 2 MB parity digest dumps were left out of the branch.

## Combined tree vs control (warm, fresh process, one cache copy, wall median ms, pairs won)

pandas (10 pairs, load average ~2-7): import_get_session 450 -> 425 (9/10),
laps 453 -> 426 (9/10), load_only 471 -> 443 (10/10), results 469 -> 443
(9/10), driver_tel 479 -> 464 (5/10), car_data_loop 485 -> 459 (8/10),
all_tel 658 -> 557 (9/10; phase 198 -> 130), notebook 762 -> 711 (8/10),
quali_split 470 -> 444 (9/10); `get_session` -25 ms in every workload.

polars (8 pairs): import_get_session 208 -> 196 (7/8), laps 212 -> 202
(6/8), load_only 365 -> 212 (8/8; load 116 -> 9), results 220 -> 206 (8/8),
state 217 -> 207 (8/8), fastest_lap_tel 219 -> 210 (6/8), driver_fastest_tel
220 -> 208 (8/8), driver_tel 232 -> 220 (8/8), car_data_loop 236 -> 228
(6/8), all_tel 336 -> 322 (7/8), notebook 557 -> 403 (8/8; load 110 -> 9,
analysis 155 -> 16, the lap loop 41 -> 142 because the lazy load moves the
row reads there), quali_split 212 -> 202 (8/8).

Files: `integ_warm_ab_pandas_quiet.json`, `integ_warm_ab_polars.json`
(the first pandas run, `integ_warm_ab_pandas.json`, ran beside N7's
benchmarks and is noise-dominated).

## Final tree

Unit suite 4121 passed, 9 skipped; ruff, ruff format, ty clean; parity
0/133 (pandas) and 0/131 (polars) against the control digests; perf gate
0/120 regressed (machine factor 0.97), then the baseline rewritten on the
combined tree (digests unchanged).
