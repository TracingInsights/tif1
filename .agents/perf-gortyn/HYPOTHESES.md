# Gortyn hypotheses (one agent each; the control is origin/main at f692ee7, the 0.11.3 release)

## G1. E4 on 0.11.3: huge-page lz4 buffers through tif1-native 0.2.1

Evidence: Elis E4 (`/tmp/tif1-elis/E4/.agents/perf-elis/E4.md`, branch
`perf/elis-E4`) decompresses the pandas frame-tier driver rows into
`MADV_HUGEPAGE` anonymous mappings through a new
`tif1_native.lz4_frame_decompress_into` (pure-Rust `lz4_flex`, GIL released):
decompression page faults 31k -> 1.4k, the whole `fetch_all` 57k -> 26.5k,
system time 141 -> 92 ms, parity exact, but only 1-3% of wall on the Delphi
tree. It was held for after the 0.11.3 release because it needs a
`tif1-native` 0.2.1 wheel. Your worktree already holds E4 applied on
0.11.3 (staged, no conflicts); read the E4 report first.
Approach: (1) commit the inherited base as `perf(gortyn-G1): base: E4 on
0.11.3`; (2) rebuild the wheel from this tree's `native/` (`uvx maturin
build --release -m native/Cargo.toml -o /tmp/tif1-gortyn/G1/wheels`), make
the private venv `/tmp/tif1-gortyn-venv-G1` (`uv venv --python 3.13`, then
`uv pip install --python /tmp/tif1-gortyn-venv-G1/bin/python -e
/tmp/tif1-gortyn/G1 --all-extras` plus the wheel; never touch the main
venv), and check `tests/unit/test_elis_e4_huge_page_rows.py` and
`test_native_decoder.py` pass there; (3) re-measure against the 0.11.3
control with E4's `tools/perf/elis_e4_warm_ab.py` (`--candidate-python`)
on `all_tel`, `notebook`, `car_data_loop`, `driver_tel`, `load_only`, 12
pairs, and the no-regression run with `TIF1_DISABLE_NATIVE=1` on both arms;
since Elis E5 and E9 changed the row reads (reads ahead on two lanes, the
readers), check that the huge-page path still covers every row read
(`_huge_page_decompress` from `_read_frame_row` and the lanes) and report
the fault counts (`elis_e4_alltel_probe.py`); (4) the next fault source E4
named, the `blobopen().read()` bytes (15.5k faults): try reading the blob
in fixed-size chunks into a reusable per-worker huge-page buffer and
decompressing from it, measure, keep it only if it pays; (5) the wheel
release: `native/Cargo.toml` 0.2.1 (already), `pyproject.toml` keeps
`tif1-native>=0.2,<0.3`; write in the report the exact `native.yml`
dispatch the integrator must run and what `uv lock --upgrade-package
tif1-native` will change once 0.2.1 is on PyPI. Parity: identical digests
on the private venv (`--lib pandas` and `polars`), every `all_tel/frames`
surface. Verdict bar: the in-process phases (walls will not move 3%).

## G2. Upstream pre-release canary (hardening job)

Evidence: two rounds put fast paths on private seams: pandas internals
(`core_utils/pandas_blocks.py` probes, `models._interleaved_row`'s
`ints_to_pytimedelta`, `fast_xs`), polars `_plr.PyDataFrame.read_ipc_stream`
(`cache._read_polars_ipc_row`), niquests' `_ocsp_cache` /
`InMemoryRevocationStatus` pickling and the `qh3._hazmat` load
(`ocsp_cache.py`), `threading._register_atexit` (`cache.py`,
`core_utils/telemetry_stream.py`). Each has a fallback and a contract test,
so drift cannot break results, but it can silently drop a path to the slow
route, and today that is found by users after an upgrade.
Approach: a scheduled workflow `.github/workflows/upstream-canary.yml`
(weekly + `workflow_dispatch`, Linux, Python 3.13, never a PR gate) that
installs the pre-releases of pandas, polars, numpy, pyarrow and niquests
(`uv pip install --pre --upgrade ...` into the synced env; print the
versions), runs the unit suite with `-x` off, the contract tests, and the
perf gate (`tests/perf_gate`, which flags a fallback as a phase
regression), and on failure opens or updates one issue (label
`upstream-drift`) with the versions and the failing tests
(`peter-evans/create-issue-from-file` or `gh issue` in the job; keep it
idempotent); add a `tests/unit/test_private_seams.py` that lists every
private seam with the upstream version it was verified against and asserts
the fast path is active on the pinned versions (so the canary's failure
names the seam). Document the job in `AGENTS.md`'s build/test section and
`CONTRIBUTING.md`. Measure: the job's runtime (target < 10 min) by running
its steps locally in a throwaway venv against today's pre-releases (report
which pre-releases exist and whether anything drifts already). No parity
concerns. Do not change library code except to add the seam list.

## G3. Per-driver typed columnar telemetry files (the data-side format), with the reader

Evidence: a race's telemetry is 1128 lap files of ~61 KB gzipped JSON (~69
MB on the wire); the eager `load()` is bandwidth-bound at ~9 s and a
driver's wave (57 requests) ~0.5 s. The Delphi report's data-side
recommendation measured a typed columnar encoding (int32 ms clocks,
int16/float32 channels, dictionary `DriverAhead`, zstd -19) at ~30 KB per
lap on 20 of VER's laps: half the bytes, and one file per driver would make
a wave one request. The data repositories (TracingInsights/{year}) are
published by a separate pipeline the integrator owns; this hypothesis
produces the format, the encoder the pipeline will run, and the reader.
Approach: (1) the format: compare, on every lap of the 2025 Bahrain race
and qualifying from the warm cache, (a) Arrow IPC file with zstd
compression (`pyarrow` writer, dictionary `DriverAhead`, the clocks as
int32 ms since lap start plus a per-lap int64 origin), (b) the same columns
as a custom little-endian blob (per-channel typed arrays, one zstd frame),
(c) one file per driver holding all its laps (a laps table of offsets and
origins plus the concatenated channels), on size, decode time to the
pandas frame the JSON path builds (through the existing
`_create_telemetry_df` contract: numpy arrays and a `DictionaryChannel`),
and polars; pick one and write `design/TELEMETRY_COLUMNAR_FORMAT.md`
(layout, dtypes, rounding rules so decoded values equal the JSON's floats
bit for bit or state exactly where they differ, versioning, the CDN path
`{year}/{gp}/{session}/{driver}_tel.v1.bin` or similar, size budget vs
jsDelivr's 50 MB repo limit); (2) `tools/data/encode_driver_telemetry.py`:
from a tif1 cache (or the JSON payload files) to the per-driver files, with
a `--verify` that decodes them back and compares every lap frame to the
JSON-built one (`assert_frame_equal(check_exact=True)`; any intentional
difference documented); (3) the reader: `payload_loader`/`core` try the
per-driver file first on a cold wave (one request), fall back to the JSON
laps on 404 (remember the verdict in the missing-payload tier so a session
without files costs one 404 per driver, never per lap), decode into the
same `_create_telemetry_df` inputs so every downstream path (frame tier
writer, memo, lazy waves, `fetch_all`) is unchanged; config
`telemetry_columnar` (default True; `False` = JSON only); (4) measure: bytes
per session (JSON gzipped vs the files), decode time per driver (fresh
process, both backends), and a cold A/B of `driver_tel`, `fastest_lap_tel`
and the four-driver notebook with the files served by a local static HTTP
server for both arms (JSON for control, files for the candidate) at a
throttled bandwidth (`tc`-free: a tiny server that sleeps per byte, or a
`--bandwidth` option in `tools/perf/`) so the bytes saving shows; the live
CDN has no files yet, so also show the fallback costs nothing measurable on
the live CDN (cold `laps`, `driver_tel` vs control within noise, one 404
per driver). Parity: identical digests; the `--verify` of the encoder on
both sessions. Tests: offline, the encoder/decoder round trip on synthetic
and fixture payloads, the 404 fallback through `InMemoryTransport`.

## G4. The rest of E6: polars `pick_*` and `iterlaps(require=...)` without numpy

Evidence: Elis E6 moved `pick_driver`, the single-driver join, the split,
the fastest-lap selection and the lone-lap decode off numpy on polars
sessions, but the notebook still imports numpy in its `pick_*` step
(polars `picks_x8` 100 ms): `pick_drivers` with more than 4 codes,
`pick_quicklaps`, `pick_fastest`, `pick_track_status`, `pick_laps` lists,
`pick_wo_box`/`pick_box_laps` (`_present_values`), `pick_compounds`,
`pick_team(s)`, `pick_accurate`, `pick_not_deleted`, `iterlaps(require=...)`
run on numpy masks (Olympia OH10). E6's report, section 6, gives the recipe:
`_filter_rows(frame, series_mask)` with eager Series kernels (no `pl.col`
expressions, no `LazyFrame`: comparisons, `&`/`|`, `not_`, `is_in` on a
Series, slices), `x == x` as the present-rows mask (null where null, and
`filter` drops nulls), masks rechunked to one chunk before filtering (10x
on the ~60-chunk joined columns).
Approach: convert the family method by method, measuring each against the
numpy mask in process (20 repetitions on the 1128-row race laps; keep
numpy where it is clearly faster and say which), keep `PolarsTelemetry`
methods on numpy (searchsorted/cumsum have no plan-free kernel), extend
`tests/unit/test_polars_without_pandas.py` (numpy mode) to assert the
notebook's eight picks and `iterlaps` leave numpy unloaded, and add
per-method parity tests against the numpy implementation (ties, nulls,
empty results, the `strict`/threshold semantics of `pick_quicklaps`, the
`IsPersonalBest` rule of `pick_fastest`, case handling of codes). Measure
polars `notebook`, `laps`, `fastest_lap_tel`, `quali_split`, 6 pairs (the
notebook's `picks_x8` is the phase; the wall gains the numpy import, ~55
ms); pandas untouched. Parity: `--lib polars` identical.

## G5. Lock bump to the newest stables, and the pandas 3.1.0 probe drift

Evidence (G2's report, `/tmp/tif1-gortyn/G2/.agents/perf-gortyn/G2.md`,
sections 3 and 6): every seam package has a newer stable than `uv.lock`
(pandas 3.0.6 -> 3.1.0, polars 2.0.0 -> 2.1.0, numpy 2.5.3 -> 2.5.4, pyarrow
26.0.0 -> 26.0.1, niquests 3.21.1 -> 3.21.2, urllib3-future 2.24.900 ->
2.25.905, qh3 1.9.4 -> 2.1.0), and a static reading of pandas 3.1.0 says two
Pergamon Q18 probes fail there: `ArrowExtensionArray.__getitem__` now
answers slices through `_from_pyarrow_array(...)` and sets `obj._cache = {}`
(so `_probe_str_slice`'s `vars()` comparison returns False) and
`TimedeltaArray._simple_new` dropped `freq`/`_freq` (so `_probe_timedelta`
returns False); the lap-frame fast route then silently falls back for every
lap. `pyproject.toml` allows pandas 3.1.0, so fresh installs are on that
slow path today. `tests/unit/test_private_seams.py` (G2, on `perf/gortyn`
in the main repo: copy it into your worktree first, with
`tests/unit/test_upstream_canary_report.py`, `tools/upstream_prereleases.py`,
`tools/upstream_canary_report.py` and `.github/workflows/upstream-canary.yml`,
so the seam tests run here) names each seam and its verified version.
Approach: (1) in a throwaway venv (`uv venv /tmp/tif1-gortyn-venv-G5
--python 3.13`, `uv pip install --python ... -e /tmp/tif1-gortyn/G5
--all-extras`, then `uv pip install --python ... --upgrade pandas polars
polars-runtime-32 numpy pyarrow niquests urllib3-future qh3`; `/tmp` has
13 GB free now) run the seam tests and the unit suite; record every
failure; (2) fix the drifts in the library so the fast paths are taken on
both the locked and the newest versions (`_str_slice` mirrors the
reference slice's attribute set, `_timedelta_array` skips `_freq` when the
reference lacks it; anything else the run shows), keeping the probes as the
guard; (3) bump `uv.lock` to the newest stables (`uv lock --upgrade-package
...` for those packages only, in the worktree with
`UV_PROJECT_ENVIRONMENT=/tmp/tif1-gortyn-venv-G5` so the main venv is not
touched), update the seam table's `verified` versions, run the whole unit
suite, ruff, ty and the perf gate on the new versions (the gate's parity
digests may need `--write-baseline` if a pandas/polars repr changed: report
every changed surface); (4) measure: the warm A/B of your tree on the new
versions vs the control on the locked versions is a versions comparison,
not a code one; report it (6 pairs, `laps,all_tel,notebook,quali_split`,
both backends, with `--candidate-python /tmp/tif1-gortyn-venv-G5/bin/python`
on a copy of `delphi_warm_ab.py`) and, separately, your tree vs the control
both on the new versions (same python, 6 pairs) to show the probe fixes
restore the fast paths (Q18: the laps `iloc`/`iterlaps` phases and the
frame-tier decode). Parity: identical digests on the locked versions;
on the new versions explain every difference (repr/dtype drift vs a value
change). Note `qh3` 2.x may conflict with urllib3-future's pin: if the
resolver refuses it, keep qh3 at the newest version it accepts and say so.

## G6. liblz4 in tif1-native: keep E4's fault saving without lz4_flex's decode cost

Evidence (Elis E4 and Gortyn G1 reports, `/tmp/tif1-elis/E4/.agents/perf-elis/E4.md`
and `/tmp/tif1-gortyn/G1/.agents/perf-gortyn/G1.md`): decompressing the
frame-tier driver rows into huge-page buffers removes ~30k page faults and
45-54 ms of system time per race `fetch_all`, but the pure-Rust `lz4_flex`
decoder is ~1.6x slower than pyarrow's liblz4 (E4: 66-68 vs 40.8 ms serial
over both sessions' 40 rows; G1: 0-33 ms of user CPU handed back per race),
which is why the phases barely move. Vendoring liblz4 (the `lz4-sys` crate,
C source built by `cc` inside the wheel build) would keep the fault saving
and the liblz4 speed. The cost is a C toolchain in every wheel build of
`.github/workflows/native.yml` (Linux glibc/musl x86_64/aarch64, macOS
universal2, Windows x64/arm64; abi3) and the sdist.
Your worktree holds G1 (E4 on 0.11.3) applied and staged; commit it as
`perf(gortyn-G6): base: G1` first. Approach: (1) in `native/`, replace the
`lz4_flex` block decode of `lz4_frame_decompress_into` with
`lz4-sys`'s `LZ4_decompress_safe_usingDict` (linked blocks need the
previous 64 KB as the dictionary; stored blocks are copies; keep the frame
header parsing and every `ValueError` the E4 tests pin), the GIL released as
now; keep `lz4_flex` only if a fallback for a target without a C toolchain
is needed (say which); (2) build the wheel here (`uvx maturin build
--release -m native/Cargo.toml -o /tmp/tif1-gortyn/G6/wheels`) and overlay
it as G1 did (`native/target/site` first on `PYTHONPATH`; no private venv
needed); (3) measure the serial decode over both sessions' 40 rows (E4's
`tools/perf/elis_e4_decode_speed.py`: pyarrow vs the new decoder, best of
7), the fault probe (`tools/perf/gortyn_g1_fault_probe.py`: faults, sys,
user), and the warm A/B against the G1 tree (`/tmp/tif1-gortyn/G1/src`,
both arms on one shared cache and the same overlay python, 12 pairs:
`all_tel,notebook,car_data_loop,driver_tel`), plus native-off both arms
(no regression); (4) the wheel matrix: update `native.yml` for the C
toolchain where needed (manylinux images carry gcc; musllinux via the
maturin container; macOS universal2 needs both archs' objects from `cc`;
Windows arm64 cross-compiles with MSVC: check `lz4-sys` supports each and
document any target you cannot verify locally; `cargo build --release`
must succeed here for x86_64 Linux at least), bump the crate to 0.2.1 (keep
G1's version; this is the same release) and update `native/README.md`;
(5) parity identical on the overlay (`--lib pandas`, every `all_tel/frames`
surface); `tests/unit/test_elis_e4_huge_page_rows.py` and
`test_native_decoder.py` green on the overlay. Verdict bar: the decoder
speed must match pyarrow's within 10%, and the warm phases must improve on
G1 (the fault saving no longer paid back as CPU).
