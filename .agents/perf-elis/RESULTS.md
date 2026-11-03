# Elis performance round: results

Date: 2026-11-03. Control: `origin/main` at b5b91ed (the Delphi round
merged, PR #119). Box: 32 cores, 30 GB, Linux, CPython 3.13.15, pandas
3.0.6, polars 2, pyarrow, tif1-native 0.2. Reference session: 2025 Bahrain
Grand Prix race and qualifying. Agents: Opus 5.5 at high effort, two per
account (ceo, madhu, info, hi), headless with a scoped allowlist.

Method: the Delphi harness (`tools/perf/delphi_warm_bench.py`,
`delphi_warm_ab.py`, `delphi_cold_bench.py`, `delphi_parity.py`), one
worktree per hypothesis (`/tmp/tif1-elis/E<k>`, branch `perf/elis-E<k>`),
control digests `/tmp/elis_parity_{pandas,polars}.json`; reports in
`E<k>.md`. Integration branch `perf/elis`.

## Baseline (control, fresh process, ms)

See `BRIEF.md`: warm pandas import+get_session 457, laps 456, all_tel 551
(89), notebook 751; warm polars 191, 197, 320 (103), 534; cold pandas laps
753, fastest_lap_tel 836, driver_tel 1396, notebook 10251; cold polars 588,
795, 1380, 10461.

## Hypotheses and verdicts

| ID | hypothesis | verdict | effect |
|---|---|---|---|
| E1 | lazy telemetry in `load()` | SHIP (opt-in) | `load(telemetry="lazy")` / `load_telemetry_lazy`: a cold four-driver notebook 9811 -> 2985 ms pandas, 9944 -> 3014 polars (-70%, 5/5 each; `load()` 8.4 s -> 0.13 s); warm polars `load()` 179 -> 8 ms; every frame identical to the eager load (1143 cold + 1133 cache read-back + 2262 warm surfaces, 0 differ); default (eager) path unchanged (parity 0/133, 0/131, warm/cold A/Bs within noise). Worst case, every lap of a cold session: +6.5% (20 driver waves vs one batch), so the default stays eager; follow-up E10 pipelines the waves |
| E2 | cold first wave on the stdlib while niquests imports | PARTIAL -> integrated with E3 | the first wave (`drivers.json`, laptimes) and single requests before a niquests session exists go over `http.client` + `ssl` (same trust store via wassima, gzip, keep-alive); niquests imports only when a bulk wave needs it. Cold `laps` 876 -> 586 ms pandas, 645 -> 430 polars (5/5 each); `fastest_lap_tel` 871 -> 609 (5/5), 826 -> 649 (4/5); cold `driver_tel` +2..12% (0/5) because the OCSP round trip no longer hides behind the pandas import. Background `import niquests` (variants a/b) convoys with the pandas import under the GIL: 0/7. Warm unchanged (identical module sets); parity 0/133, 0/131, cold frames identical. Follow-up E9: deferred niquests warm-up once the first wave lands, docs and a knob for the stdlib wave |
| E3 | persist the OCSP verdict across processes | SHIP | cold `laps` pandas 863 -> 793 ms (6/6; rerun 991 -> 819, 6/6), fastest_lap_tel 1005 -> 900 (4/6); polars laps 685 -> 575 (6/6), fastest_lap_tel 867 -> 742 (6/6) for a returning user; a first-ever process gains the qh3 preload alone (32 -> 2 ms of import). Revocation check stays on; only SUCCESSFUL+GOOD answers with a verified signature persist, 8 entries max, until `nextUpdate`/7 days; `http_revocation_cache=False` opts out. Warm paths unchanged; parity 0/133, 0/131 |
| E4 | huge-page decompress buffers in tif1-native | PARTIAL (merge after this release, with the tif1-native 0.2.1 wheel; branch kept) | mechanism exact: decompression page faults 31k -> 1.4k, whole `fetch_all` 57k -> 26.5k, system time 141 -> 92 ms, parity 0/133; walls only -1..-3% (all_tel phase 90 -> 86 at 9/12, car_data_loop 29 -> 26 at 8/12, notebook lap loop -3..-24 ms at 9-10/12) because the saving spreads over 8 workers and lz4_flex decodes 1.6x slower than liblz4; needs a tif1-native 0.2.1 wheel release and a new Rust dependency. Remaining fault sources: `blobopen().read()` bytes (15.5k) and the main-thread decode (9.2k) |
| E5 | first lazy row access prefetches the rest through the D9 readers | SHIP (partial) | the second lazy driver touched out of laps order reads its own row and starts reading the other rows ahead on two pool lanes (D9 readers); `load()` then two drivers' fastest-lap car data 71-78 -> 32 ms in process, wall 598 -> 525 (8/8) / 572 -> 533 (7/8); four drivers 78-89 -> 52-74 ms, wall -6..-8%; one driver, `load_only`, `driver_tel`, `car_data_loop`, `all_tel` run the control's code (neutral). Rejected part: reading ahead beside a loop over every lap is 4-50 ms slower than the batch at every lane count (page faults of the fresh buffers contend with the lap builds), so an in-order walk keeps D5's batch and the notebook is unchanged. Queued reads are cancelled at close and before the exit pool join; parity 0/133, 0/131 plus 1128 + 4 frames through the new path identical |
| E6 | polars telemetry without numpy; faster pyarrow-free IPC read | SHIP | the common polars telemetry reads run on eager Series kernels and Python lists (no numpy import, ~55 ms): `driver_tel` wall 373 -> 264 ms (phase 153 -> 24), `fastest_lap_tel` 284 -> 236 (71 -> 13), `car_data_loop` 380 -> 252 (79 -> 26), `quali_split` 300 -> 231 (58 -> 1), all 6/6; 1-3 driver IPC rows through polars' eager stream reader (9 vs 16 ms per row; the pooled whole-session read keeps `pl.read_ipc`, the stream reader stalled the cursor loop there); `all_tel`/`notebook` unchanged (the notebook's other `pick_*` still load numpy); pandas within noise; parity 0/131, 0/133, plus 121 numpy-blocked frames and 2810 list-built payload frames identical |
| E7 | small certain wins on warm pandas paths | SHIP | `laps.iloc[i]` x200 29.0 -> 19.5 ms (10/10; one `ints_to_pytimedelta` call for the 12 timedelta columns), `pick_driver` 0.77 -> 0.21 ms in process and ~5 -> ~1 ms as a script's first selection (contiguous-run slice when `Driver` is the session table's category column), `split_qualifying_sessions` 11.6 -> 7.3 ms (6/8; labels factorized once, no pyarrow string kernels), merged telemetry x4 39.5 -> 35.2 (10/10; ns clocks as int64 views), first `results` 11.5 -> 9.7 ms (6/8; Ergast query classes as plain slotted classes), `_lap_start_lookup` no longer imports `pyarrow.compute` in the per-lap loop; parity 0/133 and 0/131 |
| E10 | E1 follow-up: pipelined lazy waves | SHIP | one prioritised background stream per lazy session (`PriorityFetchStream`, a daemon asyncio loop that exits when idle): foreground laps first, each key fetched once, a sequential lookahead (`lazy_telemetry_lookahead`, default 1) fills the free slots with the next driver only when the script reads drivers in laps-row order (an unconditional lookahead made a four-driver notebook +24%). Cold lazy loop over every lap: pandas 11388 -> 8918 ms (4/5), polars 10288 -> 8466 (5/5), now 10-14% faster than the eager load (E1 was +6.5%); four-driver notebook unchanged (2966 vs eager 10000, 5/5); `fetch_all` takes the stream's payloads; exit cancels requests in flight (hedged CDN requests now cancel their backup); warm unchanged; parity 0/133, 0/131 plus 1590 cold + 1133 read-back + 2262 warm surfaces identical. Recommended flipping the default to lazy; decided against: `load()` must keep filling the cache for offline use, so `"lazy"` stays opt-in |
| E9 | E2 follow-up: hide the bulk wave's niquests start-up; finish E2 | SHIP (bridge), REJECT (early-import triggers) | starting the niquests import at the first-wave handover convoys with the laps build (+20-30 ms on `laps`, no `driver_tel` gain) and at the first telemetry access has no lead: both rejected. Shipped instead: the first telemetry wave runs on the stdlib connections the first wave opened while niquests imports and connects in a daemon thread, then every later request runs on niquests (HTTP/2, hedging, OCSP). Cold `driver_tel`, returning user: pandas 1515 -> 1233 ms (4/5; variant run 1358 -> 1228, 5/5), polars 1412 -> 1301 (5/5); first-ever process 1410 (3/5) / 1292 (4/5); `laps`/`fastest_lap_tel`/`notebook` unchanged. `cdn_first_wave` knob (default True; `False` = every request through niquests and its revocation check), `close_session` no longer waits for a session a warm-up thread is building (+83 ms at exit otherwise), docs and COMPATIBILITY updated; warm unchanged (identical module set); parity 0/133, 0/131, cold frames identical |
| E8 | CI performance regression gate | SHIP | `perf-gate` CI job (`tests/perf_gate`, marker `perf_gate`, Linux/3.13): a 7 MB vendored payload bundle (2025 Bahrain race + qualifying, four drivers' telemetry) seeded through the tree's own `Cache` API, 10 fresh-process warm workloads x 2 backends x 3 reps compared with a min/median baseline scaled by a per-runner factor (median ratio over ~120 phases, clamped 1/3..3), 1.5x + 5 ms limits, 5 confirmation reruns; cold and warm parity digests of 89 surfaces per backend. ~47 s of tests here; 0 false positives in 6 loaded runs; an injected 50 ms `Session.laps` sleep flags 21 phases. Found a pre-existing cold/warm class difference in pandas `fetch_all_laps_telemetry()` (plain `DataFrame` from payloads, `Telemetry` from the tier), frozen in `parity_pandas_cold_delta.json` for a follow-up fix |

## Integration notes

- E10 is integrated on top of E1 (its agent's tree carried E1 as a base);
  the `load_telemetry_lazy` docs the integrator had written for E1 were
  replaced by E10's fuller pages (`sessions.mdx` "Lazy telemetry" section,
  `core.mdx`, `config.mdx`, `environment-variables.mdx`).
- E8's cold digest found that pandas `fetch_all_laps_telemetry()` returned
  plain `DataFrame` values for payload-built frames and `Telemetry` for
  tier-read ones; fixed on the branch by wrapping through
  `_session_telemetry` in `_frames_from_payloads` and the async network
  branch (no copy). The gate's baseline and digests were then regenerated
  on the final tree (`--write-baseline`: factor 1.00, 0/120; the cold delta
  file is empty) and the gate passes (factor 1.01, 0/120, 6 tests).
- E7's and E9's 2 MB parity digest dumps were dropped from the branch
  (reproducible with `delphi_parity.py`).
- Final tree: unit suite 3628 passed; ruff, ruff format, ty and the docs
  STE gate clean; parity 0/133 (pandas) and 0/131 (polars) against the
  Elis control digests.

## Combined tree (E1 + E2 + E3 + E5 + E6 + E7 + E8 + E9), warm, final (measured before E10 and the class fix, which touch no warm path)

Fresh process, primed cache, offline, 6 interleaved pairs per workload and
backend (control cache vs the candidate's copy), whole-process wall median
(ms), candidate faster in N/6; phases that moved in brackets.

| workload | pandas | polars |
|---|---|---|
| import + get_session | 478 -> 454 (3/6) | 210 -> 211 (3/6) |
| laps | 432 -> 468 (1/6)* | 213 -> 232 (1/6)* |
| load() then laps + results | 491 -> 499 (2/6) | 377 -> 356 (4/6) [load 129 -> 117] |
| results | 492 -> 473 (3/6) [14 -> 12] | 209 -> 215 (3/6) |
| state | 454 -> 438 (5/6) | 206 -> 206 (3/6) |
| get_fastest_lap_tel | 485 -> 483 (4/6) | 288 -> 217 (6/6) [79 -> 12] |
| get_driver("VER").get_fastest_lap_tel | 496 -> 468 (4/6) [24 -> 21] | 302 -> 219 (6/6) [91 -> 17] |
| laps.pick_driver("VER").telemetry | 448 -> 434 (3/6) | 350 -> 228 (6/6) [135 -> 23] |
| per-lap get_car_data loop | 467 -> 497 (0/6)* | 340 -> 224 (6/6) [82 -> 24] |
| fetch_all_laps_telemetry | 575 -> 588 (3/6) [95 -> 98] | 328 -> 328 (3/6) [111 -> 106] |
| notebook flow | 788 -> 757 (5/6) [iloc 29 -> 19, merged x4 42 -> 38] | 589 -> 584 (5/6) [picks 115 -> 100, merged x4 13 -> 8] |
| qualifying: laps, split, results | 498 -> 473 (5/6) [split 12 -> 6] | 309 -> 221 (6/6) [split 69 -> 1] |

\* The three walls the candidate lost moved only in the untouched
`get_session` phase (+12..+30 ms between the two cache files); re-run with
both arms on one cache, 8 pairs: pandas `car_data_loop` 501 -> 485 (6/8,
phase 27 -> 26), `import_get_session` 445 -> 436 (5/8), `laps` 450 -> 471
(2/8, phase 4 -> 4, minima 442 vs 435), `all_tel` 594 -> 594 (phase 96 ->
98): no regression. The warm pandas path was not this round's target; its
gains are E7's phases. The polars telemetry workloads are E6.

## Combined tree (E1 + E2 + E3 + E5 + E6 + E7 + E8 + E9 + E10 + the class fix), cold, final

Empty cache per run, live CDN, 5 interleaved reps, whole-process wall
median/min (ms). `laps` and `fastest_lap_tel`: the plain cold bench.
`driver_tel`: the three-arm bench (control; a first-ever process; a
returning user whose cache holds only the OCSP verdict). The four-driver
notebook: control eager `load()` vs the candidate's `load(telemetry="lazy")`.

| workload | pandas | polars |
|---|---|---|
| laps | 863/788 -> 578/563 (5/5) | 728/633 -> 481/432 (5/5) |
| get_fastest_lap_tel | 837/799 -> 585/568 (5/5) | 1438/1002 -> 904/596 (5/5) |
| laps.pick_driver("VER").telemetry, first-ever process | 1426 -> 1238 (4/5) | 2464 -> 2256 (4/5) |
| same, returning user | 1426 -> 1200 (5/5) | 2464 -> 2193 (4/5) |
| four-driver notebook, eager -> lazy | 11062/10200 -> 3183/2892 (5/5) | 10037/9954 -> 2716/2606 (5/5) |

The polars `fastest_lap_tel` and `driver_tel` controls ran on a slow link
spell (1438 and 2464 against ~800 and ~1400 earlier in the round); the pairs
are interleaved, so the ratios hold. The eager `notebook` (every lap) and
`all_tel` are unchanged by design (E1/E10 only change the lazy mode).

## Combined E2 + E3, cold (empty cache, live CDN, 5 interleaved reps, whole-process wall median ms)

`laps` and `fastest_lap_tel` with the plain cold bench (control vs the
E2+E3 tree); `driver_tel` with E3's three-arm bench (control; a first-ever
process; a returning user whose cache holds only the OCSP verdict).

| backend | laps | fastest_lap_tel | driver_tel first-ever | driver_tel returning |
|---|---|---|---|---|
| pandas | 998 -> 741 (5/5) | 841 -> 669 (5/5) | 1456 -> 1444 (1/5, neutral) | 1456 -> 1356 (5/5) |
| polars | 584 -> 436 (5/5) | 799 -> 586 (5/5) | 1686 -> 1650 (3/5) | 1686 -> 1649 (3/5) |

E2 alone lost 2-12% on `driver_tel` (niquests' OCSP round trip moved onto
the bulk wave's critical path); with E3's persisted verdict the pair is
neutral for a first-ever process and wins for a returning user. E9 targets
the remaining import + handshake of the bulk wave.
