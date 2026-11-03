# perf: Gortyn round — E4 with liblz4, polars selections without numpy, columnar telemetry format, upstream canary, lock bump

Control: `main` at f692ee7 (the 0.11.3 release). Reference session: 2025
Bahrain Grand Prix race and qualifying; warm = primed cache, fresh process,
offline; cold = empty cache. Agents: Opus 5.5 at high effort, one worktree
per hypothesis against the frozen control; every report, A/B JSON and the
method are under `.agents/perf-gortyn/` (`RESULTS.md`, `G<k>.md`).

## Shipped

| ID | change | effect |
|---|---|---|
| G1 + G6 | E4 (huge-page lz4 buffers) on 0.11.3, with liblz4 (`lz4-sys`) instead of `lz4_flex` in `tif1-native` 0.2.1 | warm race `fetch_all`: 56.7k -> 26.5k minor faults, system time 147-155 -> 93 ms, decoder 2.5% faster than pyarrow's codec (1.7x faster than lz4_flex); `car_data_loop` 27.2 -> 23.1 ms (10/12), `driver_tel` 26.1 -> 24.1 (11/12), iterlaps loop 148 -> 142 (8/12); `fetch_all` wall even. Chunked blob reads into huge pages: rejected with numbers |
| G4 | every polars selection (`pick_*`, `iterlaps(require=)`, `get_driver`, `get_lap`) on eager Series masks | no numpy import for lap-time work: a warm lap-time notebook 354 -> 269 ms (-24%, 8/8 and 10/10), the benchmark notebook's eight picks 71-114 -> 3 ms; 238 real-session method surfaces identical |
| G3 | per-driver typed columnar telemetry format `tel.v1` (spec, pipeline encoder with bit-exact `--verify`, reader with JSON fallback), **off by default** until the data repositories publish the files | race 71.3 -> 52.5 MB (-26%), a driver's laps 6 requests instead of 57; on a throttled local CDN with files: cold `fetch_all` -20..-24%, four-driver notebook -8..-9%, pandas `driver_tel` -8% (5/5); frames identical |
| G2 | upstream pre-release canary: `tests/unit/test_private_seams.py` (10 private seams, verified versions, fast path asserted active) and a weekly `upstream-canary.yml` that files one `upstream-drift` issue | 12 simulated drifts each fail the named test; found that pandas 3.1.0 (already released) breaks two Q18 probes -> G5 |
| G5 | pandas 3.1 fixes and the lock bump to the newest stables | 28 failures on pandas 3.1 before: the Q18 lap-frame route silently fell back (array layout probes), `Distance`/`LapTimeSeconds` disagreed with `total_seconds()`, `Laps.to_sql(index=True)` raised; all fixed, suite green on both version sets; lock now pandas 3.1.0 / polars 2.1.0 / numpy 2.5.4 / pyarrow 26.0.1 / niquests 3.21.2 / qh3 2.1.0, seam table re-verified; pandas 3.1 wall-neutral, polars 2.1 imports ~8 ms slower (upstream) |

Also fixed on the way: the shared benchmark control cache is corrupted
when `/tmp` fills; frame-tier A/Bs must run both arms on one cache copy
(a same-code A/A moved `fetch_all` by 23 ms between two copies).

## Release steps this PR needs

1. Merge; `native.yml` then builds and tests the 0.2.1 crate on every leg
   (each leg has a C compiler for `lz4-sys`; the `native` CI job asserts
   `NATIVE_LZ4_DECOMPRESS_INTO` is active).
2. Publish the wheel: `gh workflow run native.yml --ref main -f environment=testpypi`,
   then `-f environment=pypi`.
3. `uv lock --upgrade-package tif1-native` once 0.2.1 is on PyPI (only the
   `tif1-native` block changes; the `>=0.2,<0.3` marker already allows it),
   and release tif1 0.11.4 so installed users get the huge-page path.
4. Run the `Upstream canary` workflow once by hand (`workflow_dispatch`)
   before relying on its schedule.
5. Data side: run `tools/data/encode_driver_telemetry.py --json-dir <session> --verify`
   in the data pipeline for the 2025/2026 repositories; then flip
   `telemetry_columnar` to `True` in `config.py` and the two docs pages.

## Parity and checks

`tools/perf/delphi_parity.py` (133 pandas, 131 polars surfaces) against the
0.11.3 control after every integration step: 0 differing surfaces on both
backends on the locked versions; on the bumped versions polars 0/131 and
pandas 32/133, exactly the `Distance`/`DifferentialDistance` analysis
surfaces that now follow pandas 3.1's `total_seconds()` (at most 1.5 ulp).
Unit suite on the final tree and the new versions: 3944 passed, 37 skipped
(the native 0.2.1 tests, which ran on the wheel overlay in the agents'
runs: 3537 passed there). ruff, ruff format, ty and the docs STE gate
clean; the perf gate's baseline and digests were rewritten on the new
versions and the gate passes (factor 1.00, 0 of 120).

Final warm A/B against the 0.11.3 control, both arms on the new versions
and one shared cache (main venv, so without the 0.2.1 wheel): every phase
equal within noise; pandas notebook 737 -> 712 ms (5/6, the lap loop 142
-> 134 from the restored Q18 route), `all_tel` 588 -> 573 (5/6), polars
picks 118 -> 3 ms (the numpy import moves to the telemetry step; wall
neutral). No regression on either backend.
