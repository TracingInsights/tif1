# perf: Delphi round — nine shipped hypotheses, one PR

Control: `main` at 255759b (0.11.2 plus the session_info work). Reference
session: 2025 Bahrain Grand Prix race (1128 laps, 20 drivers) and
qualifying, warm cache, fresh process per measurement, offline; cold runs
use an empty cache against the live CDN. Box: 32 cores, CPython 3.13.15,
pandas 3.0.6, polars 2, tif1-native 0.2. Method, baseline and every
per-hypothesis report: `.agents/perf-delphi/` (`RESULTS.md`, `D<k>.md`,
A/B JSONs). Harness: `tools/perf/delphi_warm_bench.py`, `delphi_warm_ab.py`,
`delphi_cold_bench.py`, `delphi_parity.py`.

## Where the time went (control)

A warm pandas script of 545 ms wall: interpreter start ~30, `import pandas`
~342, tif1's own startup ~60, the workload 3-115, interpreter teardown ~80.
A warm polars script of 371 ms: polars ~180, numpy ~54, tif1 ~30, teardown
~50. Two polars workloads leaked a pandas import (t0_date,
`get_driver().get_fastest_lap_tel()`): +230-270 ms. Warm pandas `load()` read
and decoded 70 MB of driver rows (95 ms) to learn which laps the tier held;
`fetch_all_laps_telemetry()` 114 ms, of which the single-cursor SQLite read
was 57; the notebook flow's analysis step spent 60 ms in per-column pandas
ffill/bfill chains, its four `get_driver().get_fastest_lap()` 35 ms, its four
drivers' merged telemetry 76 ms re-reading rows the memo already held.

## Shipped (D1, D2, D3, D5, D6, D7, D8, D9, D10)

| ID | change | effect (control -> candidate, ms) |
|---|---|---|
| D1 | `gc.freeze()` in an exit hook (`exit_gc_freeze`, `TIF1_EXIT_GC_FREEZE`) | every script's teardown -40..-80: pandas laps wall 566 -> 493, notebook 984 -> 874, polars get_session 383 -> 329, cold pandas laps 992 -> 876 |
| D2 | pandas startup trim: lazy block probe, lazy async/CDN/types imports, pathlib schedule read, lazy regexes | tif1's own startup with pandas imported 34 -> 20 (12/12); fresh-interpreter test pins the module set |
| D3 | polars sessions import neither pandas nor pyarrow on warm paths (`t0_date`/`session_start_time` are stdlib types on polars) | polars state 685 -> 392, `get_driver().get_fastest_lap_tel()` 693 -> 380, driver tel 454 -> 413, fetch_all 580 -> 505 |
| D5 | warm pandas `load()` keeps the driver rows lazy (lap lists from the rows' `laps` column) | `load()` 85 -> 8; `load()` + laps/results wall 652 -> 566; `load()` + one driver 715 -> 575; a loop over every lap after `load()` pays the batch plus one lone row read (~15) |
| D6 | `resample_channels` / `fill_missing` on numpy source-index maps | 4 laps in process 39.5 -> 6.1 and 7.5 -> 2.5; notebook analysis step 60 -> 21 (14/14) |
| D7 | `Driver` fast paths (results row by position, sliced `Driver.laps`, argmin fastest lap) | `get_driver(d).get_fastest_lap()` 6.5 -> 1.0 per driver; notebook step 36 -> 15 (10/10) |
| D8 | a driver's merged telemetry gathered from the memoized decoded row | notebook `driver_merged_x4` 76 -> 40 (8/8); after `load()` four drivers 61 -> 26 in process; without `load()` unchanged |
| D9 | frame-tier driver rows read inside the lz4 workers through `blobopen` on up to 4 reader connections | `fetch_all_laps_telemetry()` 111-120 -> 94 (12/12); eager row read 83-88 -> 58-61 (12/12); the raw 70 MB read 57 -> 17-20 |
| D10 | polars sessions import numpy only when an array is built | polars import + get_session 351 -> 240, laps 378 -> 250, results 368 -> 260 (6/6 each) |

Rejected with measurements (reports kept): D4 (decoding driver rows in the
decompression workers: the decode is GIL-bound and any Python-heavy thread
beside the SQLite cursor loop slows the 70 MB read 2-3x; the GIL-free `str`
take alone is worth ~2 ms). `DO_NOT_RETRY.md` records it with the earlier
rounds' list.

## Combined tree, final measurements

Warm, fresh process, 6 interleaved pairs per workload and backend,
whole-process wall median (ms); the candidate was faster in 6/6 pairs
unless noted.

| workload | pandas | polars |
|---|---|---|
| import + get_session | 521 -> 457 | 344 -> 191 |
| laps | 531 -> 456 | 358 -> 197 |
| load() then laps + results | 627 -> 470 | 502 -> 334 (5/6) |
| results | 531 -> 464 (5/6) | 347 -> 204 |
| state tables + t0_date | 515 -> 455 | 639 -> 200 |
| get_fastest_lap_tel | 534 -> 471 | 366 -> 280 |
| get_driver("VER").get_fastest_lap_tel | 552 -> 471 | 643 -> 288 |
| laps.pick_driver("VER").telemetry | 550 -> 486 | 414 -> 342 |
| per-lap get_car_data loop | 553 -> 478 | 664 -> 348 |
| fetch_all_laps_telemetry | 669 -> 551 | 494 -> 320 |
| notebook flow | 918 -> 751 | 601 -> 534 |
| qualifying: laps, split, results | 535 -> 473 | 360 -> 321 |

Cold (empty cache, live CDN, 3 interleaved reps, 3/3 faster every time):
pandas laps 881 -> 753, fastest-lap telemetry 936 -> 836, a driver's
telemetry 1551 -> 1396, the notebook flow 10724 -> 10251; polars 758 -> 588,
847 -> 795, 1804 -> 1380, 10652 -> 10461.

Trade-offs, all documented in the CHANGELOG: the first telemetry frame of a
pandas process picks up ~6 ms of pyarrow string initialisation the startup
probe used to pay for every script (D2); the first array-building call of a
polars script (telemetry, `split_qualifying_sessions`, fuzzy event
matching) carries the numpy import D10 took out of startup; a loop over
every lap of a session right after `load()` pays the row batch `load()`
used to pay plus one lone row read of ~15 ms (D5), while `load()` itself
is 84 -> 12 ms.

## Parity

`tools/perf/delphi_parity.py` digests 133 pandas and 131 polars surfaces
(laps, results, drivers, state tables, fastest-lap and driver telemetry,
every `pick_*`, the qualifying split, resample/fill, car/pos data, every
frame of `fetch_all_laps_telemetry`). Final tree against control: pandas 0
differing surfaces; polars 4, all the documented D3 type change
(`t0_date`/`session_start_time` as `datetime`/`timedelta` on polars
sessions, same values).

## Checks

Unit suite 3412 passed (control 3181; the rest are the round's tests),
`ruff check`/`ruff format` and `ty check src/tif1` clean on the combined
tree. CHANGELOG "Performance — Delphi series" carries one paragraph per
shipped change; `docs/api-reference/config.mdx` documents `exit_gc_freeze`,
`docs/api-reference/core.mdx` and `docs/migration-from-fastf1.mdx` the
polars scalar types.

## Not a library change: the data side

The cold bulk telemetry fetch is bound by bytes on the wire (1128 lap files
of ~61 KB gzipped JSON). A typed columnar encoding of the same channels
(int32 ms clocks, int16/float32 channels, dictionary `DriverAhead`, zstd
-19) measured half the bytes per lap, and one file per driver instead of one
per lap would remove the per-request overhead; both need the data
repositories to publish the files (`RESULTS.md`, "Data-side recommendation").
