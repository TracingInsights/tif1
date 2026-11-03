# Delphi performance round: results

Date: 2026-11-03. Control: `main` at 255759b (0.11.2 plus the session_info
work). Box: 32 cores, 30 GB, Linux, CPython 3.13.15, pandas 3.0.6, polars 2,
pyarrow, tif1-native 0.2. Reference session: 2025 Bahrain Grand Prix race
(1128 laps, 20 drivers) and qualifying.

## Method

- Warm: `tools/perf/delphi_warm_bench.py` runs each workload in a fresh
  interpreter against a primed cache, offline, and reports in-process phase
  times plus the whole-process wall (interpreter start to exit).
  `tools/perf/delphi_warm_ab.py` interleaves control and candidate trees
  (ABAB..., each arm on its own cache copy) and reports medians and pairs won.
- Cold: `tools/perf/delphi_cold_bench.py`, empty cache per run, live CDN,
  interleaved when comparing trees.
- Parity: `tools/perf/delphi_parity.py` digests 130+ user-visible surfaces
  (laps, results, drivers, the state tables, fastest-lap and driver
  telemetry, pick_*, split, resample, fill_missing, car/pos data, every frame
  of `fetch_all_laps_telemetry`) per backend; control and candidate digests
  are compared surface by surface.
- One git worktree per hypothesis (`/tmp/tif1-delphi/D<k>`, branch
  `perf/delphi-D<k>`), one agent each, a shared brief
  (`BRIEF.md`, `HYPOTHESES.md`, `DO_NOT_RETRY.md`); reports in `D<k>.md`.
  The box ran up to eight agents at once, so absolute numbers drift between
  runs; ratios and pairs won are the evidence.

## Baseline (control, fresh process, warm cache, offline, median of 5, ms)

| workload | pandas wall | pandas phase | polars wall | polars phase |
|---|---:|---:|---:|---:|
| import + get_session | 545 | get_session 411 | 371 | 262 |
| laps | 553 | 3 | 364 | 6 |
| results | 569 | 10 | 366 | 17 |
| state (weather, rcm, track, session status, t0_date) | 545 | 1 / 1 / 2 / 3 / 0 | 662 | 1 / 1 / 1 / 1 / 231 |
| get_fastest_lap_tel | 567 | 8 | 389 | 12 |
| get_driver("VER").get_fastest_lap_tel | 571 | 20 | 685 | 268 |
| laps.pick_driver("VER").telemetry | 570 | 23 | 446 | 48 |
| per-lap get_car_data loop (VER) | 594 | 25 | | |
| fetch_all_laps_telemetry | 715 | 114 | | |
| notebook flow | 1048 | load 111, picks 11, iterlaps tel 77, iloc 27, merged x4 76, analysis x4 65, driver fastest x4 35 | | |
| qualifying: laps, split, results | 672 | 2 / 9 / 17 | | |

Where the time goes in a warm pandas script (545 ms wall): interpreter start
~30, `import pandas` ~342 (numpy 80, pyarrow 75 inside it), tif1's own
startup ~60, the workload 3-115, interpreter teardown ~80. A warm polars
script (371 ms): polars ~180, numpy ~54, tif1 ~30, teardown ~50.

Cold (empty cache, live CDN, median of 5, whole process, ms): pandas laps
906 (get_session 592 of which the HTTP stack import ~150, laps 149),
fastest-lap telemetry 974, a driver's `Laps.telemetry` 1568, the notebook
flow 11010 (`load()` fetches the 1128 telemetry files: 9.2 s); polars 799 /
813 / 1764 / 11257. Link measurements: RTT 16 ms, TLS done at ~50 ms, TTFB
~70 ms, 5-8 MB/s per connection; the bulk telemetry fetch (~73 MB on the
wire as gzipped JSON) is bandwidth-bound here, not latency-bound.

## Hypotheses and verdicts

(filled in as the reports land; see each `D<k>.md`)

| ID | hypothesis | verdict | effect |
|---|---|---|---|
| D1 | `gc.freeze()` at interpreter exit | SHIP | pandas laps 566 -> 493 ms wall (6/6), notebook 984 -> 874 (6/6); polars get_session 383 -> 329 (6/6); cold pandas laps 992 -> 876 (3/3) |
| D2 | pandas-session startup trim | SHIP | tif1's own startup with pandas imported 33-37 -> 19-21 ms (12/12 pairs in every mode; lazy block probe, lazy async/CDN/types imports, pathlib schedule read, lazy regexes); whole-process wall -4..-13 ms (10-17/20); the first telemetry frame of a process picks up ~6 ms of deferred one-time pyarrow string setup |
| D3 | polars sessions without pandas/pyarrow on warm paths | SHIP | polars state 685 -> 392 (6/6), driver fastest tel 693 -> 380 (6/6), driver tel 454 -> 413 (6/6), fetch_all 580 -> 505 (6/6) |
| D4 | decode driver rows in the decompression workers | REJECT (measured) | the decode is GIL-bound (20 rows: 1 worker 24 ms, 8 workers 33-43 ms) and any Python-heavy thread beside the cursor loop slows the 70 MB read 2-3x (the loop re-acquires the GIL once per row); the only GIL-free part, the `str` take, moves for ~2 ms. Variant kept on the branch, not merged |
| D5 | `load()` keeps driver rows lazy | SHIP (minimax trade) | warm pandas `load()` 85 -> 8 ms (8/8); `load()` then laps/results wall 652 -> 566 (8/8); `load()` then one driver's car data 715 -> 575 (6/6); a loop over every lap after `load()` pays the batch `load()` paid plus one lone row read (~15 ms): notebook wall 1028 -> 1030 (3/8, iterlaps loop 75 -> 179 with load 86 -> 8); all_tel/driver_tel/car_data_loop untouched; parity 0/133. Integrated with a bridge to D8: a lazy row at hand (decoded or decompressed) serves the merged frame, an unread one takes D8's throwaway decode |
| D6 | numpy ffill/bfill in resample_channels / fill_missing | SHIP | 4 laps in process: `resample_channels("0.1s")` 39.5 -> 6.1 ms, `fill_missing()` 7.5 -> 2.5 ms; notebook analysis phase 60 -> 21 ms (14/14 pairs), wall 1057 -> 983 (5/6) / 960 -> 941 (4/8); parity 0/133 |
| D7 | Driver fast paths | SHIP | `get_driver(d).get_fastest_lap()` 6.5 -> 1.0 ms per driver (`get_driver` 1.9 -> 0.3, `.laps` 1.3 -> 0.25, `.get_fastest_lap()` 2.9 -> 0.35); notebook driver_fastest_lap_x4 35.6 -> 14.7 ms (10/10), wall 1079 -> 1040 (7/10) |
| D8 | driver merged telemetry from the memoized row | SHIP | notebook driver_merged_x4 76 -> 40 ms (8/8), wall 982 -> 935 (7/8); in process after `load()` four drivers 61 -> 26 ms; without `load()` unchanged (23.1 -> 22.7, 6/12: the earlier +1-2 ms was the page faults of a forced copy); car_data_loop 25 -> 21 ms (7/8); parity 0/133 and 0/131 |
| D9 | faster frame-tier row read | SHIP | the integrator's probe found nothing in parallel cursors (29-34 vs 27 ms); the agent's variant reads each driver row inside the lz4 worker through `blobopen` on up to 4 read-only connections (GIL released, no cursor loop, a read transaction re-checks the row against the main index, main connection as fallback): `fetch_all` phase 111-120 -> 94 ms (12/12, 6/8), notebook `load` 83-88 -> 58-61 ms (12/12, 8/8), notebook wall 942 -> 923 (10/12); driver_tel and polars unchanged; parity 0/133 and 0/131. The raw read is 17-20 ms; what is left of `fetch_all` is ~40k page faults of the decompressed output and the serial decode |
| D10 | polars sessions without numpy until needed | SHIP | polars get_session 351 -> 240 (6/6), laps 378 -> 250 (6/6), results 368 -> 260 (6/6) |

## Combined tree, stage 1 (D1 + D3 + D10), polars, 6 interleaved pairs

Whole-process wall, control -> candidate median (ms), pairs won; in-process
phase in brackets.

| workload | control | candidate | pairs | phase |
|---|---:|---:|---|---|
| import + get_session | 352 | 225 | 6/6 | get_session 247 -> 146 |
| laps | 366 | 216 | 6/6 | |
| results | 385 | 218 | 6/6 | |
| state | 648 | 211 | 6/6 | t0_date 225 -> 0 |
| get_fastest_lap_tel | 384 | 341 | 5/6 | phase 12 -> 87 (numpy + pyarrow now load here) |
| get_driver("VER").get_fastest_lap_tel | 664 | 323 | 6/6 | 245 -> 94 |
| laps.pick_driver("VER").telemetry | 422 | 355 | 6/6 | 43 -> 133 (numpy loads here) |
| fetch_all_laps_telemetry | 514 | 342 | 6/6 | 117 -> 106 |
| notebook flow | 629 | 575 | 5/6 | picks 4 -> 112 (numpy loads here) |

## Combined tree, final (D1 + D2 + D3 + D5 + D6 + D7 + D8 + D9 + D10)

Warm, fresh process, 6 interleaved pairs per workload and backend,
whole-process wall median (ms), candidate faster in N/6 pairs; in-process
phases that moved in brackets. Quiet box (load ~4).

| workload | pandas | polars |
|---|---|---|
| import + get_session | 521 -> 457 (6/6) | 344 -> 191 (6/6) [get_session 245 -> 126] |
| laps | 531 -> 456 (6/6) | 358 -> 197 (6/6) |
| load() then laps + results | 627 -> 470 (6/6) [load 84 -> 12] | 502 -> 334 (5/6) |
| results | 531 -> 464 (5/6) [results 7 -> 11] | 347 -> 204 (6/6) |
| state (weather, rcm, track/session status, t0_date) | 515 -> 455 (6/6) | 639 -> 200 (6/6) [t0_date 224 -> 0] |
| get_fastest_lap_tel | 534 -> 471 (6/6) [phase 8 -> 14] | 366 -> 280 (6/6) [12 -> 83, numpy loads here] |
| get_driver("VER").get_fastest_lap_tel | 552 -> 471 (6/6) [18 -> 22] | 643 -> 288 (6/6) [245 -> 76] |
| laps.pick_driver("VER").telemetry | 550 -> 486 (6/6) [22 -> 28] | 414 -> 342 (6/6) [44 -> 139, numpy loads here] |
| per-lap get_car_data loop (VER) | 553 -> 478 (6/6) [22 -> 26] | 664 -> 348 (6/6) [251 -> 71] |
| fetch_all_laps_telemetry | 669 -> 551 (6/6) [107 -> 89] | 494 -> 320 (6/6) [111 -> 103] |
| notebook flow | 918 -> 751 (6/6) [load 84 -> 12, iterlaps loop 73 -> 141, merged x4 74 -> 39, analysis x4 57 -> 21, driver fastest x4 34 -> 16] | 601 -> 534 (6/6) [picks 4 -> 69, numpy loads here] |
| qualifying: laps, split, results | 535 -> 473 (6/6) | 360 -> 321 (6/6) [split 2 -> 105, numpy loads here] |

The pandas phases that grew by 4-6 ms (`results`, the first telemetry
frame of a process) carry D2's deferred one-time pyarrow string
initialisation, which the startup probe used to pay for every script; the
polars phases that grew carry the numpy (and, for telemetry, pyarrow-free
IPC) import D10/D3 took out of startup. Every wall is lower.

Cold, empty cache, live CDN, 3 interleaved reps, whole-process wall median
(ms), candidate faster in 3/3 every time:

| workload | pandas | polars |
|---|---|---|
| laps | 881 -> 753 | 758 -> 588 |
| get_fastest_lap_tel | 936 -> 836 | 847 -> 795 |
| laps.pick_driver("VER").telemetry | 1551 -> 1396 | 1804 -> 1380 |
| notebook flow (load() fetches 1128 telemetry files) | 10724 -> 10251 | 10652 -> 10461 |

Parity of the final tree: pandas 0 differing surfaces out of 133; polars 4
out of 131, all the documented D3 type change (`t0_date` and
`session_start_time` as `datetime`/`timedelta` on polars sessions, same
values). Unit suite 3412 passed; ruff, ruff format and ty clean.

## Data-side recommendation (not a library change)

The cold bulk telemetry fetch (`load()` with telemetry, `fetch_all_laps_telemetry`)
is bound by bytes on the wire: 1128 lap files of ~61 KB gzipped JSON each.
A typed columnar encoding of the same channels (int32 milliseconds for
`time`, int16 for the integer channels, float32 for the float channels,
dictionary codes for `DriverAhead`, zstd -19) measured 30 KB per lap on 20
laps of VER (JSON 145 KB, gzip 61 KB, zstd on JSON 53 KB), i.e. half the
bytes, so a race's cold load would drop from ~9 s to ~5 s on this link, and
one file per driver (57 laps) instead of one per lap would remove the
per-request overhead too. Both need the data repositories to publish the new
files; the library can read them alongside the JSON once they exist.
