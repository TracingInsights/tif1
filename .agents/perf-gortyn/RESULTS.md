# Gortyn round: results

Date: 2026-11-03. Control: `origin/main` at f692ee7 (the 0.11.3 release).
Same box, harness, method and accounts as Elis (`BRIEF.md`); integration
branch `perf/gortyn`.

## Hypotheses and verdicts

| ID | hypothesis | verdict | effect |
|---|---|---|---|
| G1 | E4 on 0.11.3: huge-page lz4 buffers through tif1-native 0.2.1 | PARTIAL, merged (user decision: ship with the 0.2.1 wheel) | on 0.11.3 every driver-row read (batch, per-row, E5 lanes, serial) lands on the huge-page path: `fetch_all` faults 56.7k -> 26.5k, system time 139-152 -> 95 ms; phases `car_data_loop` 28.5 -> 25.9 ms (10/12 in three runs), `driver_tel` 27.5 -> 26.0 (11/12); `all_tel` and the lap loop within this box's noise (a same-code A/A on two cache copies moved `all_tel` by 23 ms: frame-tier A/Bs need one shared cache); no wall moves. Chunked `blobopen` reads into a huge-page buffer halve the remaining faults but cost GIL round trips: REJECT. Parity 0/133, 0/131 on the 0.2.1 overlay; suite 3527 passed there, 3508 on the fallback. Needs the tif1-native 0.2.1 wheel release (`native.yml` dispatch testpypi -> pypi, then `uv lock --upgrade-package tif1-native`) |
| G2 | upstream pre-release canary (hardening job) | SHIP | `tests/unit/test_private_seams.py`: 10 private seams (pandas direct blocks, lap frames, Arrow strings, lap rows, constructor hooks; polars IPC stream reader, `_from_pydf`; niquests OCSP pickling; qh3 `_hazmat` preload; `threading._register_atexit`) with the verified version each, asserting the fast path is the one taken (12 simulated drifts each fail the named test); `.github/workflows/upstream-canary.yml` weekly + dispatch, installs the newest releases/pre-releases of 8 packages, runs the seam tests, the unit suite and the perf gate, files/updates/closes one `upstream-drift` issue; `tools/upstream_prereleases.py`, `tools/upstream_canary_report.py`. No library code changed. Found: pandas 3.1.0 (released; the lock pins 3.0.6) breaks two Q18 probes by a static reading -> G5 |
| G3 | per-driver typed columnar telemetry files (format, encoder, reader) | PARTIAL -> merged with the reader off by default | `tel.v1`: `{driver}/tel.v1.{first}.bin`, ten laps per file, typed byte-shuffled columns in one zstd-19 frame, decoded laps bit-identical to the native JSON decode (`--verify` on 1405 laps: 0 differences; one file per driver was slower, one connection at ~4.7 MB/s). Bytes: race 71.3 -> 52.5 MB (-26%), qualifying 24.3 -> 17.8 (-27%); float64 `distance`/`rel_distance`/`DistanceToDriverAhead`/`acc_*` are ~65% of a file. Local throttled CDN with files (8 MB/s): pandas `all_tel` 12532 -> 9981 (5/5), notebook4 3406 -> 3085 (5/5), `driver_tel` 1433 -> 1324 (5/5); polars `all_tel` -24%, notebook4 -8%, `driver_tel` neutral. Without files a session's first cold wave pays one 404 (~100 ms, then a 7-day verdict): live `driver_tel` 1181 -> 1295 (1/5), so `telemetry_columnar` defaults to False until the data repositories publish the files (`tools/data/encode_driver_telemetry.py --json-dir ... --verify`). Parity 0/133, 0/131; frames from files equal frames from JSON on both backends |
| G4 | polars `pick_*` and `iterlaps(require=)` without numpy | SHIP | every polars selection (`pick_drivers/lap/laps/team(s)/tyre/compounds/quicklaps/fastest/track_status/wo_box/box_laps/not_deleted/accurate`, `iterlaps(require=)`, `get_driver`, `PolarsDriver.get_lap`) on eager Series masks; a lap-time notebook (`lap_analysis`: laps, eight picks, iterlaps, every driver's fastest lap, results) wall 354 -> 269 ms (-24%, 8/8; 363 -> 275 at 10/10), its picks 61 -> 4 ms; the general notebook's picks 71-114 -> 3 ms but its wall is neutral (the telemetry arithmetic still imports numpy, now at that step); iterlaps telemetry loop 56 -> 38 ms; 238 real-session method surfaces identical with numpy loaded and blocked; parity 0/131; suite green. The first agent was cut off by the /tmp quota; a finishing agent completed the gates and report |
| G5 | lock bump to the newest stables; pandas 3.1.0 probe drift fixes | SHIP (fixes) + lock bump done by the integrator | on the newest stables the suite had 28 failures, four drifts: (1) pandas 3.1 gives Arrow slices an empty `_cache` and its `TimedeltaArray` no `_freq`, so both Q18 probes returned False (the lap-frame route silently fell back): `frame_codec._array_layout()` now mirrors the installed layout and the probes stay the guard; (2) `TimedeltaArray.total_seconds()` rounds differently on 3.1 and two fast paths had copied the old arithmetic: `Distance`/`DifferentialDistance` and `LapTimeSeconds` now use `total_seconds()` itself (equal to pandas and fastf1 on every version; on 3.1 the values move by <=1.5 ulp, 32 analysis surfaces); (3) `Laps.to_sql(index=True)` raised on 3.1 (`reset_index` dropped pandas' `level_0`): fixed, a user's own `level_0` column is no longer dropped either; (4) two test premises. On 3.1 the restored Q18 route is worth 8% of the decode (pandas' own slicing got faster), on 3.0.6 24%; the locked-version A/B is noise; pandas 3.1 is wall-neutral, polars 2.1 imports ~8 ms slower (upstream). Lock bumped here: pandas 3.1.0, polars 2.1.0, numpy 2.5.4, pyarrow 26.0.1, niquests 3.21.2, urllib3-future 2.25.905, qh3 2.1.0; seam table re-verified |
| G6 | liblz4 in tif1-native instead of lz4_flex | SHIP | `lz4-sys` (liblz4 1.10, C compiled by `cc` in the wheel build) decodes the blocks (`LZ4_decompress_safe` / `_usingDict` with the previous 64 KB as an in-place prefix dictionary; unsized frames decode the last block through a scratch buffer so nothing past the content is written): serial decode of both sessions' 40 rows 67.2 (lz4_flex) -> 40.0 ms, 2.5% faster than pyarrow's codec; the fault saving kept (56.7k -> 26.5k, -54..-62 ms system) and the user CPU 17-19 ms below lz4_flex, at or below pyarrow's; vs the 0.11.3 control (same cache, 12 pairs) `car_data_loop` 27.2 -> 23.1 ms (10/12), `driver_tel` 26.1 -> 24.1 (11/12), iterlaps loop 148 -> 142 (8/12), `all_tel` even; wheel +6.6 KB; every wheel-matrix leg has a C compiler (documented in `native.yml`); `native/tests/smoke.py` exercises the prefix-dictionary path on every built wheel; parity 0/133, 0/131 on the overlay; 3537 passed there |

## Integration notes

- Order on `perf/gortyn`: G2, G1, G4, G3 (reader default flipped to False
  by the integrator, docs updated), G6 (its `native/` files supersede G1's),
  G5 (library fixes), then the lock bump the G5 sandbox could not run
  (`uv lock --upgrade-package` for the eight seam packages; seam table
  re-verified against the new pins).
- The shared control cache `/tmp/tif1-delphi-warm` was destroyed (77 KB)
  when `/tmp` hit its quota mid-round; restored from the `E10` copy. Later
  agents measured both arms on one cache copy (G1 showed a same-code A/A
  moving `fetch_all` by 23 ms between two copies).
- G5's and the other agents' 2 MB parity digest dumps were dropped from
  the branch (reproducible).
- The perf gate's baseline and digests were rewritten on the new versions
  (`--write-baseline`: factor 1.00, 0/120; check 6 passed); its pandas
  digest carries the `total_seconds()` ulp change, the cold/warm delta
  stays empty.

## Final tree

- Unit suite on the new versions: 3944 passed, 37 skipped (the native
  0.2.1 tests, which the agents ran on the wheel overlay: 3537 passed
  there). ruff, ruff format, ty, the docs STE gate and the perf gate clean.
  19 warnings: seven `ResourceWarning: unclosed database` from one
  `test_cache.py` test's own connections and pandas 3.1's `Pandas4Warning`
  when a test passes `inplace=True` to `Laps.reset_index` (forwarded as
  fastf1 does); neither is a library change.
- Parity against the 0.11.3 control digests (made on pandas 3.0.6):
  polars 0/131; pandas 32/133, exactly the `Distance`/`DifferentialDistance`
  analysis surfaces G5 moved to pandas' own `total_seconds()` (at most 1.5
  ulp; on the locked versions 0/133).
- Final warm A/B vs the 0.11.3 control, both arms on the new versions and
  one shared cache, main venv (tif1-native 0.2.0, so G1/G6's path is
  inactive here; their wins were measured on the 0.2.1 overlay), 6 pairs,
  whole-process wall median (ms): every phase equal within noise. pandas:
  notebook 737 -> 712 (5/6; iterlaps loop 142 -> 134, the restored Q18
  route on pandas 3.1), all_tel 588 -> 573 (5/6; phase 95 -> 94),
  load_only 495 -> 474 (5/6), quali_split 464 -> 459 (5/6); polars: picks
  118 -> 3 ms with the numpy import moving to the telemetry step (notebook
  wall 598 -> 595), all_tel 339 -> 336 (5/6; phase 106 -> 100), quali_split
  224 -> 220 (5/6). G4's lap-analysis workload (-24%) and G1/G6's phases
  are in their reports.
