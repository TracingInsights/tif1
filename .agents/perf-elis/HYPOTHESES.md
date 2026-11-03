# Elis hypotheses (one agent each; the control is origin/main after Delphi, b5b91ed)

## E1. Lazy telemetry in `load()` (cold notebooks: 10 s -> ~2 s)

Evidence: a cold `session.load()` with telemetry fetches all 1128 lap files of
a race (~9 s on this link, bandwidth-bound) before the script touches a
single lap; the notebook flow then uses four drivers. Olympia's cold
`car_data` path already fetches one driver's laps in one parallel wave
(0.4-0.8 s per driver: `core.py`, the whole-driver wave used by
`get_car_data`/`Laps.telemetry` on a cold cache; grep "one parallel wave" /
`_fetch_driver_telemetry_wave` or the OH-era names in `core.py`).
Approach: `Session.load(telemetry=...)` keeps `True` (eager, fastf1's
behaviour) and `False`; add `"lazy"`: the telemetry refs are registered as
pending (as the warm path's lazy sources are, Delphi D5) and a driver's
laps are fetched in one wave on the first access of any of its laps
(`lap.telemetry`, `get_car_data`, `Laps.telemetry`, `get_fastest_lap_tel`);
`fetch_all_laps_telemetry()` after a lazy load fetches every pending driver
in one batch exactly as the eager load did (same concurrency, same cache
writes, same frame tier writes). A config key `load_telemetry_lazy`
(env `TIF1_LOAD_TELEMETRY_LAZY`, default False) makes `"lazy"` the meaning
of `telemetry=True`; keep the default False in this round and recommend in
the report whether it should flip (fastf1 parity of results is unchanged
either way; only when the bytes move changes). Warm sessions must be
unaffected (the frame tier serves them; D5's lazy rows stay). Measure: copy
`delphi_cold_bench.py` into your worktree and add a `notebook_4drivers`
workload (load(weather, messages) then the four drivers' merged telemetry,
their fastest laps' car data and the 4-lap analysis, plus results) and a
`notebook_lazy` variant that calls `load(telemetry="lazy")`; A/B cold
`notebook` (must be unchanged: it touches every lap), `notebook_4drivers`
(eager vs lazy: the win), `laps`, `fastest_lap_tel`, 3-5 reps, one cold
bench at a time. Warm: `notebook,load_only,all_tel` no regression. Parity:
identical digests; plus a parity check of your own that a lazy-loaded
session's `fetch_all_laps_telemetry()` and per-driver frames equal the
eager load's (same cache copy, compare frame by frame).

## E2. Cold first wave on the stdlib while niquests imports in the background

Evidence: `import niquests` costs 144 ms in every cold process (urllib3-future
~57, asyncio ~31, niquests' own modules the rest; qh3/HTTP-3 is not
imported). A cold `laps` script is 753 ms wall: pandas 342, niquests 144,
TLS + TTFB of the first wave ~150-250, the laps work ~150. Olympia OH11
imports niquests and warms one connection per first-wave path in daemon
threads during `get_session` (`core._preconnect_cdn`, `warm_connections`,
`wait_for_warm_connections`); the first wave (drivers.json, laptimes,
session_info, the Ergast Stats file) then rides those connections. The
import itself is CPU work under the GIL, so threads only hide it behind
network waits.
Approach, measure both: (a) start `import niquests` in a daemon thread at
`get_session` (if OH11 does not already) and run the first wave on the
stdlib (`http.client` over `ssl` with the same certificate store niquests
uses: `wassima`/certifi, same headers, same CDN fallback order, gzip), so
the TLS handshake and the TTFB of the first files overlap the import; the
bulk wave (telemetry) stays on niquests, which by then is imported and its
connections warmed; (b) the smaller step: keep niquests for everything but
make sure its import fully overlaps the pandas import or the schedule
resolution (probably impossible under the GIL; measure and say so).
Constraints: `http_session.py`/`async_fetch.py`/`cdn.py` own the fallback
loops and the hedging (TH3) and the 403/404 semantics (A11, K9); the stdlib
path must keep those semantics for the first wave or defer to niquests on
any error; `InMemoryTransport` tests must still cover the first wave;
`TIF1_OFFLINE_MODE` unchanged; Pyodide path untouched (`browser.py`). No
new dependency. Measure cold `laps,fastest_lap_tel,driver_tel` (5 reps,
interleaved) on both backends; warm `laps,import_get_session` no
regression (nothing should import in a warm process). Parity: identical.

## E3. Persist the OCSP verdict across processes

Evidence: niquests' OCSP revocation check costs 120-240 ms per cold process
(`http_revocation_check`, on by default; `COMPATIBILITY.md`/docs explain the
trade-off). OCSP responses carry `thisUpdate`/`nextUpdate` validity windows
(days); browsers cache them.
Approach: find where niquests performs the check (`niquests/extensions/_ocsp*`
or similar in `/home/harish/Documents/tif1/.venv/lib/python3.13/site-packages/niquests`)
and whether it keeps an in-process cache keyed by certificate (issuer,
serial, fingerprint) that can be pre-seeded, or exposes a hook. If so,
persist the (key, DER response bytes, nextUpdate) in the SQLite `json`
tier (`cache.py`, kind-based entries; see how the schedule TTL rows are
stored) when a check completes, and seed niquests' cache from it before the
first connection of the next process, only while `nextUpdate` is in the
future and the response says GOOD (never cache REVOKED/UNKNOWN; a seeded
entry must be ignored when its certificate differs). If niquests has no
such seam, measure what a pre-warmed check would save anyway (a probe) and
write a REJECT with the exact reason and the smallest upstream change that
would make it possible. Security notes go in the report and the docs
(`docs/reference/environment-variables.mdx` `TIF1_HTTP_REVOCATION_CHECK`,
`docs/api-reference/config.mdx`). Measure cold `laps` and
`fastest_lap_tel` (5 reps, interleaved; the second process of each arm is
the one that gains: run the candidate twice per rep against a cache that
keeps the OCSP row, i.e. do not wipe the cache between the two). Parity:
identical. Tests: offline, with a fake OCSP response.

## E4. `fetch_all` output page faults: decompress into huge-page buffers (tif1-native)

Evidence (Delphi D9 report, section 6): after the worker-side reads, a warm
`fetch_all_laps_telemetry()` (89 ms, 1128 laps) spends its time on ~40 000
minor page faults for the 123 MB of lz4 output (110-130 ms of system time
across the 8 workers) and a 30 ms serial decode. jemalloc did not change the
fault count; transparent huge pages are `madvise` on this box, so only an
explicit `MADV_HUGEPAGE` on the output buffers (2 MB pages: ~60 faults
instead of 40 000) or buffer reuse would help, and pyarrow's lz4 codec
(`_pa_lz4_codec`, `cache.py`) allocates fresh memory per row with no
"decompress into" API; python-lz4 has none either.
Approach: (1) measure the ceiling first: fault count and system time of the
decompression alone (`resource.getrusage` before/after, `perf stat` if
available) and a prototype of the same bytes landing in an anonymous
`mmap.mmap(-1, n)` buffer with `madvise(mmap.MADV_HUGEPAGE)`; (2) add to
`native/` (Rust, pyo3, `lz4_flex` crate, no C deps) a
`lz4_block_decompress_into(src: bytes, dst: memoryview) -> int` that
releases the GIL, and a small Python-side allocator in `cache.py` that hands
out huge-page `mmap` buffers (sized to the row's stored uncompressed length,
rounded up to 2 MB; the frame tier rows know their size, see
`frame_codec`/`_decompress_row`) so the narrow-row widening and the
`DriverFrameSource` views sit on huge pages; fall back to today's path when
the extension is missing, `MADV_HUGEPAGE` is unavailable (Windows, macOS,
Pyodide) or the row is small; (3) measure `all_tel`, `notebook` (load and
the iterlaps loop) and `car_data_loop`, both with the extension (your
private venv, below) and without (`TIF1_DISABLE_NATIVE=1`: no regression).
Building: this hypothesis may run `uvx maturin build --release -m
native/Cargo.toml -o /tmp/tif1-elis/E4/wheels`, create a private venv
`uv venv /tmp/tif1-elis-venv-E4 --python 3.13` and populate it with
`uv pip install --python /tmp/tif1-elis-venv-E4/bin/python -e /tmp/tif1-elis/E4 --all-extras`
(or the dependency list of `pyproject.toml`) plus the built wheel; never
install anything into `/home/harish/Documents/tif1/.venv`. Copy
`delphi_warm_ab.py` into your worktree and add `--candidate-python` so the
candidate arm runs on the private venv. Keep `tests/unit/test_native_decoder.py`
green and add tests for the new entry point (decompress-into equals the
pyarrow codec's bytes; wrong-size destinations raise; the fallback path).
The wheel matrix (`.github/workflows/native.yml`) and the pyproject marker
test must still pass (no new platform assumptions in the Rust code: the
`madvise` lives on the Python side, guarded by `hasattr(mmap, "MADV_HUGEPAGE")`).
Parity: identical digests (every `all_tel/frames` surface).

## E5. First lazy row access prefetches the rest through the D9 readers (pandas)

Evidence: Delphi D5 (lazy `load()` rows) keeps the first driver's row read on
its own (~15 ms) and batches every remaining row on the second driver;
Delphi D9 moved row reads into the lz4 workers through `blobopen` on
reader connections (GIL released, no cursor loop). D5's report, section 6,
modelled a background prefetch of the remaining rows started by the first
lone read at 20-40 ms better on the two/four-driver shapes and the loop
over every lap (notebook `iterlaps_tel_loop` 141 ms vs 73 before D5).
Approach: on the first lazy access, submit every pending lazy row of the
session to the pool (D9's `_submit_frame_row_reads`), wait only for the
touched driver's row, and keep the other futures on the lazy sources
(`LazyDriverFrameSource.hydrate_raw` when they complete; the decode still
waits for each driver's first lap). Needs: a closing flag so
`Cache.close()`/exit never waits on or races the readers (D9 closes idle
readers; a lent reader closes on return), memory bounded (at most
`_reader_cap` rows in flight, as D9), and the single-driver shape not worse
(a script touching one driver must not pay for 19 reads: start the prefetch
only when the session has more pending drivers than N, or throttle it to
the pool's spare capacity; measure N). Measure warm pandas `notebook`,
`load_only`, `driver_tel`, `car_data_loop`, and a copied-bench workload
`load_then_2_drivers` / `load_then_4_drivers` (load() then those drivers'
fastest-lap car data). Parity: identical. Tests: the prefetch fires once,
never for one driver, and a cache closed mid-prefetch leaks no reader.

## E6. Polars telemetry reads without numpy and a faster pyarrow-free IPC read

Evidence: on polars the first telemetry read of a process carries the numpy
import (55 ms: `driver_tel` phase 44 -> 139 ms, `fastest_lap_tel` 12 -> 83,
`quali_split` 2 -> 105 after Delphi D10 moved numpy out of startup) and the
pyarrow-free IPC row read (Delphi D3: `pl.read_ipc` on the bytes, ~13 ms
per race row vs pyarrow's ~6 ms; polars 2 has no eager in-memory IPC reader
and `read_ipc` lost `rechunk`/`memory_map`). Olympia OH10 put the polars
hot paths (`pick_*`, slices, `Laps.telemetry`, `merge_channels`,
`add_distance`, `fill_missing`, `split_qualifying_sessions`) on numpy masks
because a polars query plan costs ~0.5 ms (the first of a process ~7 ms).
Approach: (1) measure what the 55 ms buys: which of the first-read paths
actually need numpy (the masks, the IPC row slicing, `DriverAhead` gather,
the Date/SessionTime arithmetic) and whether eager `pl.Series` operations
(no `LazyFrame`, no `pl.col` expressions: `Series.filter`, `Series.gather`,
comparisons, `arg_true`) are within a factor of 2 of the numpy masks in
process; if so, make the telemetry read and the common `pick_*` numpy-free
and keep numpy only where it is clearly faster (report the per-method
numbers); (2) the IPC read: try `pl.read_ipc_stream`, `memory_map` through
a temporary file or `mmap` of the row, `pl.read_ipc(..., rechunk=False)`
equivalents in polars 2, or splitting the row into per-driver IPC messages
read with `pl.from_arrow` on a `pyarrow`-free path (polars' own
`_from_pydf`); pick the fastest that keeps the frame identical (dtypes
including `String` vs `string_view`, chunking, the `DriverAhead` codes).
Measure polars `driver_tel`, `fastest_lap_tel`, `car_data_loop`, `all_tel`,
`quali_split`, `notebook` (6 pairs); pandas untouched. Parity: `--lib
polars` identical (the 4 `t0_date`/`session_start_time` type surfaces
already differ from pandas by design and are identical between control and
candidate). `tests/unit/test_polars_without_pandas.py` gains a numpy-free
telemetry read if you get there.

## E7. Small certain wins on the warm pandas paths

Evidence (Delphi D7/D8 notes and the final A/B): `pick_driver` ~2 ms per
call inside `Laps.telemetry` (a 1128-row categorical compare plus the `Laps`
wrap; `Driver.laps` does the same in 0.25 ms by slicing a run of rows),
`_join_lap_telemetry` ~2 ms per merged frame (the `Time` rebase: a shallow
copy and a column set), `results` 11 ms (7 ms of Ergast merge in
`core_utils/ergast_results.py`, 1 ms of the lazy `ergast.structure`
regexes, the rest the first `ArrowStringArray` of the process), the
qualifying `split` 12 ms, `laps.iloc[i]` 0.14 ms (`iloc_200` 29 ms).
Approach: profile each on the warm cache (cProfile plus manual timers, 20
repetitions in process), take the ones with a mechanism (a per-session
driver -> row-range map for `pick_driver` when the laps are stored driver
by driver, as `Driver.laps` already assumes; building the rebased `Time`
without the copy; memoizing the results frame's column arrays; the split's
boundaries from the laps' `Time` array once), and leave the rest with
numbers. Each change must keep values, dtypes, index, name and class
identical. Measure warm pandas `driver_tel`, `car_data_loop`, `results`,
`quali_split`, `notebook` (iloc_200, merged x4), 6 pairs; the bar is the
in-process phase (these are 1-5 ms items; the wall will not move 3%).
Parity: identical.

## E8. A performance regression gate in CI

Evidence: every round's wins are only protected by the next round's
baseline. The Delphi harness runs offline against a primed cache; CI has no
such cache, and the `benchmarks` job (`tests/benchmarks/`, `-m benchmark`)
is not a gate.
Approach: (1) a small vendored fixture cache: build, with the control tree,
a SQLite cache holding one short session (a sprint or a few drivers' laps
of a small race, both backends; keep the file under ~10 MB: strip the
telemetry to 3 drivers, keep laps/results/state) under `tests/fixtures/perf/`
with a script in `tools/perf/` that regenerates it; (2) a pytest module
(marker `perf_gate`, run in a dedicated CI job on Linux only, `-p
no:xdist`) that runs the fresh-process workloads of
`delphi_warm_bench.py` against that cache (`import_get_session`, `laps`,
`results`, `state`, `driver_tel`, `all_tel`, `notebook`-like, on both
backends) and compares in-process phases (not walls: CI machines are
noisy) against a committed baseline JSON with a tolerance (e.g. 1.5x per
phase, best of 3), failing with a table of the phases that regressed; a
`--write-baseline` mode regenerates the JSON; (3) the parity digest as a
test: `delphi_parity.py`'s digest of the fixture cache committed as a JSON
fixture and compared in the same job (surface by surface) so dtype/value
drift is caught without the network. Keep the existing test layout
(`tests/unit`, `tests/integration`, ...) and `pyproject.toml` markers;
document the job in `AGENTS.md`'s build/test section. Measure: the job's
own runtime (target < 2 min) and that it passes on the control tree and
fails on a deliberate slowdown (a `time.sleep(0.05)` in `Session.laps`
behind an env var used only by your test). No parity concerns.

## E9. E2 follow-up: hide the bulk wave's niquests start-up, and finish E2

Evidence (E2's report, `/tmp/tif1-elis/E2/.agents/perf-elis/E2.md`, and its
integration with E3 on `perf/elis`): the stdlib first wave makes cold
`laps` and `fastest_lap_tel` 21-33% faster, but a cold script that starts a
bulk telemetry wave (`driver_tel`: `laps.pick_driver("VER").telemetry`) is
2-12% slower because niquests' import (~120 ms of CPU), its first TLS
handshakes and the OCSP round trip used to overlap the pandas import and
now sit on the critical path when the wave starts. E3 (now integrated)
persists the OCSP verdict, so a returning user skips the responder round
trip; the import and the handshakes remain. E2 measured that a background
`import niquests` started at `get_session` convoys with the pandas import
(0/7), but never tried starting it later.
Approach: (1) start the niquests import and connection warm-up
(`http_session.warm_connections`, with E3's seeded verdict) in a daemon
thread at the moment the first-wave bodies are handed over (the laps build
starts then: Python work that releases the GIL less than an import does,
but short), or at the first telemetry access, so that by the time the bulk
wave begins niquests is imported and connected; measure both trigger
points and the GIL cost they impose on the laps build (the `laps` phase
must not lose more than it gains elsewhere: cold `laps` must stay within
noise of E2's 586/430 ms). (2) Finish E2's loose ends: a config knob
(`cdn_first_wave`, env `TIF1_CDN_FIRST_WAVE`, default True) that turns the
stdlib wave off independently of `cdn_preconnect`; when the user set
`http_revocation_check` explicitly to True in their config/env, skip the
stdlib wave or document that those two small files are fetched with chain
and host-name verification but without the OCSP check (choose, justify in
the report); docs in `docs/reference/environment-variables.mdx`,
`docs/api-reference/config.mdx`, `COMPATIBILITY.md` (STE checker clean:
`python tools/ste_check.py docs/ --baseline tools/ste_baseline.json`), and
the `AGENTS.md` architecture notes for `first_wave.py`. (3) Measure cold
`laps,fastest_lap_tel,driver_tel,notebook` on both backends with
`tools/perf/elis_e3_cold_bench.py` (three arms: control, first-ever,
returning user; your candidate is the E9 tree, the control is the
E2+E3 tree at `/tmp/tif1-elis-control-e9/src`), 5 reps, one cold bench at
a time; warm `laps,driver_tel` no regression; parity identical.

## E10. E1 follow-up: pipelined lazy waves so the lazy mode has no worst case

Evidence (E1's report): `load(telemetry="lazy")` cuts a cold four-driver
notebook from 9.8 s to 3.0 s, but a script that touches every lap of a cold
session pays 20 sequential per-driver waves (~0.5-0.6 s each, each with its
own ramp-up and tail) instead of one 1128-file batch: +6.5% (11.3 vs 10.6
s). That worst case is the reason the default stays eager.
Approach, measure both: (1) pipelining: when a driver's wave completes and
its frames are being built, start the next pending driver's wave (laps-row
order, or the driver whose lap was just touched first) in the background
through the same `fetch_multiple_async` machinery, bounded so a foreground
wave is never starved (the pool and the concurrency cap are shared: measure
the foreground wave's latency with and without a background wave in
flight); (2) continuation: after the first lazily fetched wave, keep
fetching the remaining pending drivers in the background at reduced
concurrency while the script works, so a script that ends up touching
everything approaches the eager batch's time and one that touches four
drivers is not slowed (its waves must preempt or join the background
fetch); check `DO_NOT_RETRY.md`'s speculative-prefetch entries (they were
about warm tiers, not this) and E1's note that `load()` must not surface
network failures later than it does today (document how a background wave
failure is reported). Keep every frame identical to the eager load's
(E1's `elis_e1_lazy_parity.py`, all three modes), the cache writes the same,
and the eager default untouched. Measure cold `iterlaps_all_lazy` vs
`iterlaps_all` (eager), `notebook_lazy` vs `notebook_4drivers` (eager),
`notebook` (eager, no regression), 5 reps, one cold bench at a time, both
backends; warm `notebook,load_only` no regression. Recommend, with numbers,
whether `telemetry=True` can default to lazy; if yes, include the docs
changes the flip needs (`docs/concepts/sessions.mdx`,
`docs/api-reference/core.mdx`, `docs/migration-from-fastf1.mdx`, the
STE checker clean) but leave the default itself to the integrator.
