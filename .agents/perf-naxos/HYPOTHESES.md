# Naxos hypotheses (one agent each; the control is d28ce70 = main after Gortyn + PR #125)

Numbers below are from `BRIEF.md` (this box, fresh process, median of 5).
Accounts: the coordinator assigns each ID to one Claude account; the ID
alone identifies your work.

## N1. Numpy's import without OpenBLAS's thread-pool start on the critical path

Evidence: `import numpy` takes 97-115 ms in a fresh process on this 32-core
box and 62 ms with `OPENBLAS_NUM_THREADS=1` (numpy 2.5.4 wheels bundle
`scipy-openblas64` 0.3.34, `MAX_THREADS=64`, pthreads). Every pandas session
imports numpy (through pandas), and a polars session imports it at its first
telemetry arithmetic (the notebook's analysis phase). A whole warm pandas
`laps` script measured 430 -> ~395 ms with the env var (noisy, 6 runs).
OpenBLAS starts its worker threads and allocates their buffers in its
library constructor (`blas_thread_init` / `gotoblas_init`), on the importing
thread.
Approach: (1) research and measure exactly where the 35-45 ms goes (perf
record / `strace -f -c` / `ltrace` of the import with and without the env var;
OpenBLAS source: `driver/others/blas_server.c`, `memory.c`; the
scipy-openblas64 symbol names, e.g. `scipy_openblas_set_num_threads64_`;
`threadpoolctl`'s OpenBLAS controller). (2) Design a mechanism that keeps the
user-visible behaviour: when tif1 is the first to import numpy (the common
`import tif1` / `get_session` order) and the user has not set
`OPENBLAS_NUM_THREADS`/`OMP_NUM_THREADS`/`GOTO_NUM_THREADS`, import numpy with
a small thread count and restore OpenBLAS's default count afterwards, off the
critical path (a daemon thread calling the set-threads entry point through
ctypes, or lazily before the first BLAS call if OpenBLAS supports growing the
pool on demand). Afterwards `threadpoolctl.threadpool_info()` and
`numpy` matrix products must report and use the same thread count as an
untouched process; the env var must not leak into `os.environ` for child
processes. If the user already imported numpy, or set any of those env vars,
do nothing. (3) Cover where tif1 imports numpy first: the pandas session's
`import pandas` (`_load_pandas_models`), the polars lazy numpy binding
(`core_utils/lazy_import.py`), `tif1_native`. (4) Measure warm pandas
`laps`, `notebook`, `all_tel` and polars `notebook` walls (8 pairs) and the
cost on a numpy-heavy follow-up (a 2000x2000 `@` right after the import, to
show the deferred pool start does not land on the user's first BLAS call).
Also measure on a smaller core count (`taskset -c 0-7`) and report both.
Risks to address in the report: Windows/macOS (Accelerate on macOS arm64
wheels has no OpenBLAS: no-op), MKL builds (no-op), free-threaded builds,
`fork` after import. Parity: digests identical (BLAS is not on tif1's paths).
Research online: OpenBLAS issues on thread-pool startup cost, numpy's
`_distributor_init`, threadpoolctl.

## N2. Polars eager `load()` keeps driver rows lazy (Delphi D5 for polars)

Evidence: warm polars `load_only` wall 349 ms with `load` 116 ms of it,
versus pandas `load` 11 ms: Delphi D5 made the pandas `load()` keep the
frame-tier driver rows lazy (each driver's row is read on first access, E5
reads the others ahead), while the polars `load()` still reads every
driver's IPC row (`_get_polars_ipc_frames`) eagerly. The notebook's polars
`load` is 120 ms of its 583 ms wall.
Approach: read Delphi D5 (`.agents/perf-delphi/D5.md`), Elis E5/E6
(`.agents/perf-elis/E5.md`, `E6.md`) and the polars IPC tier code in
`cache.py` (`_get_polars_ipc_frames`, `_read_polars_ipc_row`) and `core.py`
(the eager load path for polars, `SessionMemo`). Make the warm polars
`load(telemetry=True)` record which drivers have rows and read each row on
first access (one driver: the eager stream reader path; the reads-ahead
lanes when the script walks several drivers), with exactly the frames the
eager load gives. Measure warm polars `load_only`, `notebook`, `all_tel`,
`driver_tel`, `car_data_loop` and a "load then one driver's fastest-lap car
data" script (8 pairs); the minimax rule of D5 applies (a loop over every
lap after `load()` must not get materially slower). Parity 0/131 polars
(and 0/133 pandas if you touch shared code). Check the cold eager load is
unchanged (it builds frames from payloads, not the tier).

## N3. Polars telemetry arithmetic without numpy and without query plans

Evidence: in the warm polars notebook, `telemetry_analysis_4` (four laps:
`add_distance`, `add_differential_distance`, `resample_channels("0.1s")`,
`fill_missing`, `slice_by_time`, `merge_channels`) is 158 ms, versus 20 ms
on pandas. A cProfile: the numpy import inside `integrate_distance` ~100 ms
(the first numpy use of the process), and 53 polars `LazyFrame.collect`
query plans in `_resample_by_rule` (~27 ms). Gortyn G4 and Elis E6 moved the
selections and the common telemetry reads to eager Series kernels and Python
lists; the arithmetic still uses numpy (AGENTS.md: "the `PolarsTelemetry`
arithmetic ... keeps numpy").
Approach: profile each method (`models_polars.py`: `integrate_distance`,
`add_distance`, `add_differential_distance`, `add_relative_distance`,
`add_driver_ahead`, `resample_channels` / `_resample_by_rule`,
`fill_missing`, `slice_by_time` / `slice_by_lap`, `merge_channels`). Rewrite
the hot ones on eager polars Series kernels (arithmetic, comparisons,
`filter`, gathers, `Series.cum_sum()` and friends are allowed here if a
single call costs less than numpy's import amortised: measure each kernel;
a per-call query plan inside a loop is not), Python lists, or the
`tif1_native` extension (a small Rust kernel for cumulative distance /
resampling interpolation is in scope; new functions go in `native/src`, with
a Python fallback; build the wheel into a private venv only if you need to,
`/tmp/naxos/venv-N3`, and say so). Values must be identical to the numpy
versions bit for bit (float summation order matters: numpy's `cumsum` is
sequential; check `np.interp`-style interpolation results exactly), dtypes
and columns unchanged. Keep numpy as the fallback for rare inputs. Measure
the polars `notebook` (8 pairs, its analysis phase and wall) and a script
doing only `get_driver(d).get_fastest_lap().get_telemetry().add_distance()`
for 4 drivers. Extend `test_polars_without_pandas.py` (numpy absent after
the analysis). Parity 0/131 polars.

## N4. Defer heavy modules that pandas/polars import but tif1's paths never use

Evidence: `python -X importtime -c "import pandas"`: 341 ms; `pyarrow.compute`
40 ms (27 ms self) is imported by `pandas.core.arrays.arrow.accessors` at
import time; `pandas.core.groupby` 55 ms; `pandas.io` and `pandas.plotting`
are imported eagerly; `numpy._core._add_newdocs` 25 ms of docstring work.
Polars: `polars.io` 40 ms, `polars.functions` 47 ms. A warm pandas script is
~470 ms of which ~340 is `import pandas`; DO_NOT_RETRY says lazy pandas for a
pandas session is impossible, which this is not: pandas is imported, but
some of its submodules (and pyarrow's) could load on first use.
Approach: list, with `-X importtime` and an attribute-access audit (which
names of each module are touched at import time and on tif1's warm paths),
the modules imported during `import pandas` / `import polars` that tif1's
warm workloads never use. For each candidate, try deferring it with
`importlib.util.LazyLoader` (a stdlib, documented API: the module object is
created but executed on first attribute access) installed by tif1 just
before it imports pandas (only when the module is not yet in
`sys.modules`), or with a narrower mechanism if LazyLoader breaks something
(`from x import name` at import time forces the load). Measure what each
deferral saves (fresh process, 10 runs), and what it costs the first real
use. Hard constraints: a user who imports pandas or pyarrow themselves after
tif1 must get fully working modules (`import pyarrow.compute as pc;
pc.sum(...)` works, `pd.DataFrame.plot` works, isinstance checks work,
pickling works); nothing must differ for a user who imported pandas before
tif1. Any reliance on pandas/pyarrow internals is a private seam
(`tests/unit/test_private_seams.py` entry + test). Research online:
CPython LazyLoader caveats, PEP 810 (explicit lazy imports, 3.15), how
scientific-python's `lazy_loader` does it, pandas issues on import time.
Measure warm pandas `laps`, `notebook`, `all_tel` and polars `laps`,
`driver_tel` walls (8 pairs). Parity 0/133 and 0/131. Report every module
tried with its saving, even the ones rejected.

## N5. Interpreter exit after a tif1 workload

Evidence: the interpreter's exit after the workload costs ~20 ms after a
pandas `laps` script and ~50 ms after `fetch_all_laps_telemetry()` (eight
`tif1-frame-decompress` pool threads alive, 100+ MB of decompressed
buffers, pyarrow memory pools, the SQLite connection, the module teardown
of pandas/pyarrow/polars). Delphi D1 shipped `gc.freeze()` at exit.
Approach: measure where the exit time goes (`perf record -g` across the
exit, `strace -T`, timestamps in `atexit` hooks and
`threading._register_atexit`, `sys.audit` hooks, `-X importtime` is not
enough): thread-pool joins, `munmap` of huge buffers, allocator teardown
(pyarrow's mimalloc/jemalloc pool), SQLite close (WAL checkpoint?), module
dict clearing, `gc` passes. Then cut what can be cut without changing any
user-visible semantics: user `atexit` handlers still run, buffered output is
flushed, the cache is consistent (a WAL checkpoint may be skipped only if
the next open recovers it at no cost), no warnings printed. `os._exit` is not
allowed. Ideas: release the decompressed row buffers and memos before exit
in one go, shut the pool down without a join when its queue is empty,
`sqlite3` close order, avoiding a final full `gc`. Measure the
whole-process wall of warm pandas `laps`, `all_tel`, `notebook` and polars
`all_tel`, `notebook` (8 pairs); the in-process phases must be unchanged.

## N6. Pandas per-lap telemetry: `lap.telemetry` / `get_car_data()` per-lap overhead

Evidence: the pandas notebook's `iterlaps` telemetry loop (228 laps of four
drivers, `lap.telemetry` each) is 148 ms (~0.65 ms per lap; polars 41 ms for
the same loop) and `car_data_loop` (VER's 57 laps, `get_car_data()`) 28 ms.
A cProfile of the loop: `models.Lap.telemetry` -> `core._get_telemetry_df_for_ref`
-> `cache.SessionMemo.get` -> `_materialize_telemetry` -> `frame()` -> `_load`
-> `frame_codec ... build`, plus `iterlaps` / `_lap_from_row` 17 ms and
a `DataFrame.to_numpy` interleave of 7 ms; the driver-row decompress and
widen (`_expand_narrow_row`) ~13 ms.
Approach: read Delphi D5/D8, Elis E5/E7 and the current per-lap path; find
the per-lap constant costs (Python call depth, per-lap pandas constructions,
the merged-to-car/pos split, index building, `_metadata` propagation,
`iterlaps`' per-row Series) and cut them, e.g. by building each lap's frame
from pre-sliced column arrays of the driver's decoded row through the
direct block assembly (`core_utils/pandas_blocks.py`), caching per-driver
slice offsets, and building `iterlaps`' rows from one `to_numpy` per dtype
block. Frames must be identical (dtypes, index, columns, `_metadata`, class)
and independent (a user mutating one lap's frame must not change
another's, or the memo). Measure warm pandas `notebook` (its loop phase and
wall), `car_data_loop`, `driver_tel` (8 pairs). Parity 0/133.

## N7. A smaller columnar telemetry format (tel.v2): lossless float codecs

Evidence: Gortyn G3 built `tel.v1` (`{driver}/tel.v1.{first}.bin`, ten laps
per file, typed byte-shuffled columns in one zstd-19 frame): race 71.3 ->
52.5 MB (-26%), and on a throttled local CDN the cold pandas `all_tel` went
12.5 -> 10.0 s. The float64 channels (`distance`, `rel_distance`,
`DistanceToDriverAhead`, `acc_x/y/z`) are ~65% of a file. A cold eager load
or `fetch_all` of a race is ~10 s here, bounded by bytes on the wire. The
data repositories have not published v1 yet, so a better v2 can replace it
before anything ships.
Approach: read G3 (`.agents/perf-gortyn/G3.md`, `design/TELEMETRY_COLUMNAR_FORMAT.md`,
`core_utils/columnar_telemetry.py`, `tools/data/encode_driver_telemetry.py`).
Study each channel's real values (are `distance`/`rel_distance` exact
functions of other channels? are the floats decimal-rounded values in
disguise, e.g. 3 decimals, which a scaled-integer or ALP-style encoding stores
losslessly? deltas, run lengths, dictionary channels). Evaluate lossless
codecs on the real 2025 Bahrain race and qualifying laps from the warm cache
(`/tmp/naxos/warm-N7`; the payload tier holds the lap JSON): per-channel
choices among zstd levels with byte shuffle, delta/xor (Gorilla/Chimp), ALP
(decimal floats), pcodec (`pcodec` Rust crate / `pcodec` Python package for
the study), FSST for strings. Decoded laps must equal the native JSON
decoder's mapping bit for bit (the G3 `--verify` rule). If a codec wins,
implement the decoder in `tif1-native` (Rust, GIL released; a private venv
`/tmp/naxos/venv-N7` with the wheel built from your tree, never the main
venv) with a pure-Python fallback only if one is reasonably fast (otherwise
the reader asks for v2 only when the native decoder is present), the encoder
in `tools/data/encode_driver_telemetry.py`, and the format doc. Measure:
bytes per session (race, qualifying), decode time per file, and the cold
`all_tel` / `driver_tel` / notebook on the G3 throttled local CDN harness
(`.agents/perf-gortyn/G3.md` names it) v1 vs v2. Allowed: a new Rust crate
dependency in `native/`. Not allowed: lossy encodings. Research online:
pcodec, ALP (SIGMOD 2024), Chimp, BtrBlocks, FastLanes.

## N8. Cold one-driver telemetry: the wave's tail

Evidence: cold `driver_tel` (fresh process, empty cache,
`laps.pick_driver("VER").telemetry`, 57 lap files ~3.5 MB) is 1.6-3.9 s
pandas and ~1.0 s polars (median of 3), while the same process's `laps`
is 590/355 ms: the one-driver wave costs 0.65-3.3 s for 3.5 MB, far from
the link's bandwidth. The variance (3.9 s outliers) suggests tail latency
(jsDelivr edge misses fetching from GitHub, slow connections, the
hedging/backoff policy, the stdlib bridge -> niquests handover of E9).
Approach: instrument a cold `driver_tel` run (per-request start, TTFB, end,
connection, CDN, hedge fired or not, bytes; the niquests import and warm-up
on the timeline; `TIF1_LOG_LEVEL`/a local trace hook; do not commit the
instrumentation unless it is cheap and off by default). Run 10+ cold runs on
each backend to find what the slow runs have in common. Then fix what the
timeline shows (e.g. hedge earlier on the edge-miss signature, the right
connection count for a 57-file wave, request ordering, connection reuse
across the bridge handover, the pandas-side frame build serialised behind
the wave). Respect DO_NOT_RETRY (full hedging, sharding across hosts,
concurrency above 22, etc. were measured); if your fix resembles one,
explain what is different. Measure cold `driver_tel`, `fastest_lap_tel`,
`laps` on both backends (interleaved, 8 reps, report medians AND p90),
and say what else was using the network.

## N9. Cold eager load and `fetch_all`: is it really bandwidth-bound?

Evidence: a cold pandas notebook (eager `load()`, 1128 lap files ~69 MB) is
10-12 s, `all_tel` 9.3-10.4 s; polars notebook 9.2-9.9 s. Earlier rounds
called it bandwidth-bound at 5-8 MB/s per connection with 22 connections, but
22 connections at 5-8 MB/s each would move 69 MB in well under 2 s, so
either the link (or jsDelivr per-client throttling) caps the aggregate, or
the pipeline does. Priming the round's cache (cold race load + fetch_all +
four drivers, pandas) took 45.8 s.
Approach: (1) measure the link: aggregate throughput of 22 parallel
downloads of the same lap files with `curl --parallel` and with a plain
asyncio + niquests script outside tif1, versus tif1's own wave; one large file
on one connection; jsDelivr vs Hugging Face vs StaticDelivr. (2) Instrument
tif1's eager wave: bytes per second over time, CPU per thread (py-spy
`--native` / `perf`), GIL contention between the fetch loop, gzip decode,
the native decoder, the frame build and the SQLite writes; where time goes
after the last byte arrives (frame build, tier writes, the `fetch_all`
second pass). (3) Explain the 45.8 s priming run (which step, is a second
pass re-fetching or re-building?). (4) Fix what the evidence shows (and only
that), within DO_NOT_RETRY's limits. Measure cold `notebook` and `all_tel` on
both backends (5 interleaved reps, medians and p90). If the link is the cap,
report it with numbers and stop there: that is a valid result.

## N10. Warm pandas `fetch_all_laps_telemetry()` and the merged-telemetry build

Evidence: warm pandas `all_tel` phase 103 ms (wall 560), polars 117 ms
(after PR #125: 150 ms on the perf-gate fixture); the pandas notebook's
`driver_merged_x4` 36 ms; G1/G6 cut the decompression's page faults
(tif1-native 0.2.1 is now installed in the main venv, so the huge-page path
is live in this round's measurements). What remains per driver row: the
`blobopen` read, the lz4 decode, the widen (`frame_narrow`), the TF1 decode
into blocks, the per-lap split into 1128 frames, the dict build.
Approach: profile the whole `fetch_all` (py-spy with `--native`, perf, a
per-stage timer), on both backends, and find the largest remaining stage
(per-lap frame construction is the usual suspect: 1128 DataFrames; on polars
1128 slices). Try: building the per-lap frames as zero-copy views of the
driver's blocks (frames must stay independent if a user mutates one:
copy-on-write semantics of pandas 3 may already give this; prove it with a
test), skipping per-lap work the dict's consumer never needs, a native
split. Measure warm `all_tel` (both backends), `notebook` (pandas, its
merged and loop phases), `car_data_loop` (8 pairs). Parity 0/133 and
0/131, including every `all_tel/frames` surface.

## N11. The cold lazy path, which becomes the default (`load_telemetry_lazy=True`)

Context: the user decided to flip `load_telemetry_lazy` to True in this
round (a separate branch does the flip and the docs; do not touch config
defaults or docs). Your control and candidate both run lazy: set
`TIF1_LOAD_TELEMETRY_LAZY=true` for both arms (copy the cold harness into
your tree's `tools/perf/` and add the env var and the workloads below).
Evidence: Elis E1/E10 (`.agents/perf-elis/E1.md`, `E10.md`): a cold
four-driver notebook lazy ~3.0 s (eager ~10 s); a lazy loop over every lap
~8.9 s; `load()` itself ~0.13 s. The link here caps at ~10 MB/s (N9's
measurement: 22 parallel curl downloads of a race's 1128 files, 69 MB, 6.8-9.0 s);
one driver's laps are ~3.5 MB (~0.35 s of link time), so a four-driver
notebook moves ~14 MB (~1.4 s): the other ~1.6 s is not bytes.
Approach: instrument cold lazy runs (both backends): the timeline of
`get_session`, `load()`, each driver's first access (foreground wave start,
first/last body, frame build), the background stream's lookahead, the
niquests import/warm-up and the stdlib bridge, the exit. Workloads: (a) the
four-driver notebook (`load()` then `pick_driver(d).telemetry` and
`get_driver(d).get_fastest_lap().get_telemetry()` for VER, LEC, NOR, PIA),
(b) `load()` then one driver's fastest-lap telemetry, (c) `load()` then
`fetch_all_laps_telemetry()`, (d) `load()` then a loop over every lap.
Find what is not link time and cut it (e.g. a first foreground wave that
waits for a niquests session, waves that do not overlap the previous
driver's frame build, the lookahead rule for scripts that read drivers out
of laps order, the per-driver ramp-up, the frame-tier writes at exit).
Respect DO_NOT_RETRY (an unconditional lookahead was +24%: E10). Measure
interleaved cold A/Bs (5-8 reps, medians and p90) on both backends; say what
else was using the network (other agents run cold benches too; N8 and N9
measure cold paths: keep your runs short). Parity: the frames equal the
eager load's (E10's lazy parity script `tools/perf/elis_e10_lazy_parity.py`
if present in the tree) and 0/133, 0/131 on the warm digests.
