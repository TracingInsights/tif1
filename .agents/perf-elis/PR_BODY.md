# perf: Elis round — cold path, lazy telemetry, polars without numpy, a CI perf gate

Control: `main` at b5b91ed (the Delphi round merged, PR #119). Reference
session: 2025 Bahrain Grand Prix race and qualifying; warm = primed cache,
fresh process, offline; cold = empty cache, live CDN. Box: 32 cores,
CPython 3.13, pandas 3.0.6, polars 2, tif1-native 0.2. Agents: Opus 5.5 at
high effort, one worktree per hypothesis against a frozen control; every
report, A/B JSON and the method are under `.agents/perf-elis/`
(`RESULTS.md`, `E<k>.md`).

## Where the time went (control, after Delphi)

A warm pandas script: `import pandas` ~342 of ~457 ms; a warm polars script
~191 ms, but its first telemetry read carried the numpy import (55 ms) and
a slower pyarrow-free IPC read. Cold: `import niquests` 144 ms plus a TLS
handshake and an OCSP round trip (120-240 ms) in every process before the
first request; a cold `load()` fetched all 1128 telemetry files (~9 s)
before a notebook touched its first lap. Warm `fetch_all` was bound by
page faults of the decompressed rows.

## Shipped

| ID | change | effect (control -> candidate, ms, fresh process) |
|---|---|---|
| E1 | `load(telemetry="lazy")` / `load_telemetry_lazy` (opt-in; default stays eager) | cold four-driver notebook 9811 -> 2985 pandas, 9944 -> 3014 polars (5/5); `load()` 8.4 s -> 0.13 s; warm polars `load()` 179 -> 8; every frame identical to the eager load |
| E2 | cold first wave (`drivers.json`, laptimes) on `http.client` + `ssl`; niquests imported only for a bulk wave | cold `laps` 876 -> 586 pandas, 645 -> 430 polars; `fastest_lap_tel` 871 -> 609 / 826 -> 649 (5/5) |
| E3 | OCSP verdicts persisted in the SQLite cache (GOOD only, signature re-verified, <= 8, until `nextUpdate`/7 d); qh3's QUIC stack no longer imported for the check | returning user: cold `laps` 863 -> 793 pandas, 685 -> 575 polars; `fastest_lap_tel` 1005 -> 900 / 867 -> 742 (6/6) |
| E9 | first telemetry wave on the stdlib connections while niquests imports and connects in a thread; `cdn_first_wave` knob; exit no longer waits for a warm-up | cold `driver_tel` 1515 -> 1233 pandas (returning user), 1412 -> 1301 polars (5/5); E2's wins kept |
| E5 | second lazy driver reads its row and prefetches the rest on two pool lanes (D9 readers) | `load()` then two drivers' car data 71-78 -> 32 ms (wall 598 -> 525, 8/8); four drivers 78-89 -> 52-74; loops and one-driver scripts unchanged |
| E6 | polars telemetry reads without numpy; eager IPC stream reader for 1-3 driver rows | polars `driver_tel` 373 -> 264, `fastest_lap_tel` 284 -> 236, `car_data_loop` 380 -> 252, `quali_split` 300 -> 231 (6/6) |
| E7 | small warm pandas paths | `iloc` x200 29 -> 20, `pick_driver` 0.8 -> 0.2 (5 -> 1 as a first selection), split 12 -> 7, merged x4 40 -> 35, first `results` 11.5 -> 9.7 |
| E8 | CI performance regression gate (`perf-gate` job, 7 MB vendored bundle, phase baseline with a per-runner factor, cold + warm parity digests) | ~47 s of tests; 0 false positives in 6 loaded runs; an injected 50 ms `Session.laps` sleep flags 21 phases |

Held, branch kept: E4 (lz4 decompression into huge-page buffers through
`tif1-native`): the mechanism is exact (decompression page faults 31k ->
1.4k, system time 141 -> 92 ms, parity 0/133) but the wall gain is 1-3%
and it needs a new Rust dependency and a `tif1-native` 0.2.1 wheel
release. Rejected with numbers inside E9: starting the niquests import at
the first-wave handover or at the first telemetry access (GIL convoy with
the laps build / no lead); inside E5: reading rows ahead beside a loop
over every lap (page-fault contention beats the batch).

## Combined tree, final measurements

Cold, empty cache per run, live CDN, 5 interleaved reps, whole-process
wall median (ms), candidate faster in N/5:

| workload | pandas | polars |
|---|---|---|
| laps | 863 -> 578 (5/5) | 728 -> 481 (5/5) |
| get_fastest_lap_tel | 837 -> 585 (5/5) | 1438 -> 904 (5/5) |
| laps.pick_driver("VER").telemetry, first-ever process | 1426 -> 1238 (4/5) | 2464 -> 2256 (4/5) |
| same, returning user (OCSP verdict kept) | 1426 -> 1200 (5/5) | 2464 -> 2193 (4/5) |
| four-driver notebook, eager -> `load(telemetry="lazy")` | 11062 -> 3183 (5/5) | 10037 -> 2716 (5/5) |

Warm, primed cache, offline, 6 interleaved pairs, whole-process wall
median (ms); phases in brackets:

| workload | pandas | polars |
|---|---|---|
| get_fastest_lap_tel | 485 -> 483 | 288 -> 217 (6/6) [79 -> 12] |
| get_driver("VER").get_fastest_lap_tel | 496 -> 468 [24 -> 21] | 302 -> 219 (6/6) [91 -> 17] |
| laps.pick_driver("VER").telemetry | 448 -> 434 | 350 -> 228 (6/6) [135 -> 23] |
| per-lap get_car_data loop | 501 -> 485 (6/8, same cache) | 340 -> 224 (6/6) [82 -> 24] |
| notebook flow | 788 -> 757 (5/6) [iloc 29 -> 19, merged x4 42 -> 38] | 589 -> 584 (5/6) [picks 115 -> 100, merged 13 -> 8] |
| qualifying: laps, split, results | 498 -> 473 (5/6) [split 12 -> 6] | 309 -> 221 (6/6) [split 69 -> 1] |
| load() then laps + results | 491 -> 499 | 377 -> 356 (4/6) [load 129 -> 117] |
| import + get_session, laps, results, state, fetch_all | within noise (phases equal; the untouched `get_session` phase moved +/-30 ms between cache files, re-checked on one cache) | within noise |

Rounds of the Delphi harness, every integration step re-digested: see
`.agents/perf-elis/RESULTS.md`.

## Trade-offs (all documented in the CHANGELOG and the docs)

- The stdlib first wave and the first telemetry wave (until niquests has
  connected) verify the certificate chain and host name but skip niquests'
  OCSP check and are not hedged; `cdn_first_wave=False` sends every request
  through niquests. Persisted OCSP answers are used until their own
  `nextUpdate` (at most 7 days); `http_revocation_cache=False` checks in
  every process.
- `telemetry="lazy"` surfaces a telemetry fetch failure at the first lap
  access instead of inside `load()`, and a script that touches every lap of
  a cold session pays ~6% more (20 driver waves instead of one batch);
  that is why the default stays eager.
- E7 and E6 depend on private pandas (`ints_to_pytimedelta`) and polars
  (`PyDataFrame.read_ipc_stream`) entry points behind probes with the
  generic path as the fallback; E3 on niquests' documented pickling
  contract of `InMemoryRevocationStatus` (a contract test fails loudly on
  drift).

## Parity

`tools/perf/delphi_parity.py` (133 pandas and 131 polars surfaces) against
the control digests after every integration step: 0 differing surfaces on
both backends. Cold parity by the agents' own tools: E1 (1143 + 1133 + 2262
frames), E2/E9 (cold frames identical), E5 (1128 + 4 frames through the
prefetch path), E6 (121 numpy-blocked frames, 2810 list-built payload
frames). The new CI gate's four digests pass on the integrated tree.

## Checks

Unit suite 3628 passed (control 3436), `ruff check`/`ruff format`,
`ty check src/tif1` and the docs STE gate clean on the combined tree; the
`perf-gate` job passes on it (baseline regenerated on the final tree:
factor 1.00; check: factor 1.01, 0 of 120 phases regressed, four parity
digests identical). The gate's cold digest found and this PR fixes a
pre-existing inconsistency: pandas `fetch_all_laps_telemetry()` returned
plain `DataFrame` values for payload-built frames and `Telemetry` for
tier-read ones; both are `Telemetry` now (no copy).

## Decisions

1. `telemetry=True` stays eager: `load()` must keep filling the cache so a
   session loaded online works offline afterwards. `"lazy"` (and
   `load_telemetry_lazy`) stay opt-in; E10 makes the lazy mode faster than
   eager in every cold shape measured, so the opt-in costs nothing when
   chosen.
2. `cdn_first_wave` defaults to on: the first wave and the first telemetry
   wave skip niquests' OCSP check until it has connected (chain and host
   name verified). Off sends every request through niquests.
3. E4 (huge-page lz4 buffers through `tif1-native`) merges after this
   release, together with the `tif1-native` 0.2.1 wheel it needs; the
   branch `perf/elis-E4` and its report stay.
