# Naxos performance round: shared brief for the hypothesis agents

tif1 (https://github.com/TracingInsights/tif1) is a Python library serving
Formula 1 timing and telemetry data, a faster drop-in for fastf1. Its whole
reason to exist is speed. This round ("Naxos") runs ten hypotheses, one per
agent, each in its own sparse git clone, against a frozen control tree:
`main` after the Gortyn round plus PR #125 (commit d28ce70). Read the
repository's `AGENTS.md` (architecture, constraints, style) before writing
code; it is authoritative. Then read your section of `HYPOTHESES.md` (same
folder as this file).

Eighteen rounds already ran (Monaco ... Delphi, Elis, Gortyn). Their rejected
ideas are listed in `DO_NOT_RETRY.md` next to this file: do not spend time on
them. The round reports in `/home/harish/Documents/tif1/.agents/perf-*/`
(`RESULTS.md` and the per-hypothesis `<ID>.md`) describe the current state of
every hot path; read the ones your section names.

## Where things are

- Main repository (READ ONLY for you, never edit, never run git there):
  `/home/harish/Documents/tif1`. Its virtualenv is the one interpreter you use:
  `/home/harish/Documents/tif1/.venv/bin/python` (3.13, pandas 3.1, polars 2.1,
  numpy 2.5, pyarrow 26, tif1-native 0.2.1).
- Disk is short: no git worktrees, no second virtualenv unless your section
  says so, no copies of the warm cache beyond the ones named here. Delete
  scratch files over 50 MB when you are done with them.
- Frozen control tree (READ ONLY): `/tmp/naxos/control` (commit d28ce70, a
  sparse clone: `src/`, `tests/`, `tools/`, `native/` sources, no `docs/`).
  The A/B harness runs it with `PYTHONPATH=/tmp/naxos/control/src`.
- Your tree: `/tmp/naxos/<ID>` on branch `perf/naxos-<ID>` (the ID is in your
  prompt, e.g. `N3`), a `git clone --shared` of the main repository with the
  same sparse checkout. Only files in this tree may be edited or committed.
  Do not touch other agents' trees. If you must edit a doc page, run
  `git sparse-checkout add /docs/` in your tree first.
- Your warm cache: `/tmp/naxos/warm-<ID>` (290 MB SQLite, a copy of the
  round's primed cache: 2025 Bahrain Grand Prix race and qualifying on both
  backends, laps/telemetry/results/state and every frame tier). Run BOTH
  arms of your A/Bs on this one copy (two copies of the same cache differ by
  up to 23 ms on `all_tel`: Gortyn G1), unless your change alters what the
  cache stores; then make one more copy `/tmp/naxos/warm-<ID>-cand` with
  `cp -a` and delete it when done. Never write to `/tmp/naxos/warm` (the
  master copy).
- Control parity digests: `/tmp/naxos/parity_pandas.json`,
  `/tmp/naxos/parity_polars.json`. Baseline numbers:
  `/tmp/naxos/baseline_warm_{pandas,polars}.json`,
  `/tmp/naxos/baseline_cold_{pandas,polars}.json`.
- Harness (in the main repo; run it from your tree with the paths shown).
  You may copy a harness file into your tree's `tools/perf/` and extend it
  there (a new workload, a `--candidate-python` option, an env var per arm)
  when your section needs it; then run your copy and say so in the report.
  - `/home/harish/Documents/tif1/tools/perf/delphi_warm_bench.py`: fresh-process
    warm workloads (`--child <workload>` is the per-process entry).
  - `/home/harish/Documents/tif1/tools/perf/delphi_warm_ab.py`: interleaved
    control-vs-candidate A/B over those workloads (whole-process wall time plus
    in-process phases, medians and pairs won).
  - `/home/harish/Documents/tif1/tools/perf/delphi_cold_bench.py`: cold (empty
    cache per run, live CDN) workloads `laps`, `fastest_lap_tel`, `driver_tel`,
    `notebook`, `all_tel`; `--control-src` interleaves control and candidate.
  - `/home/harish/Documents/tif1/tools/perf/delphi_parity.py`: a digest of 130+
    user-visible surfaces of the two sessions, and `--compare a.json b.json`.
  - `/home/harish/Documents/tif1/tools/perf/perf_gate.py` and
    `tests/perf_gate/`: the CI regression gate (run it before you finish if
    you changed a measured path; rewrite the baseline only if your change is
    meant to move a phase, and say so).

## How to run things (no `uv run`, no `uv sync`: they would build a second
## environment)

All commands from inside your tree (`cd /tmp/naxos/<ID>`):

```bash
PY=/home/harish/Documents/tif1/.venv/bin/python
export PYTHONPATH=/tmp/naxos/<ID>/src HOME=/tmp/naxos/home-<ID>
mkdir -p $HOME

# Unit tests (sockets are blocked by a fixture; at most 6 workers, other
# agents share this 32-core box). Keep --no-cov, coverage is checked at merge.
$PY -m pytest tests/unit -q -x --no-cov -p no:cacheprovider -n 6

# Lint, format, types (all three must be clean before you commit):
/home/harish/Documents/tif1/.venv/bin/ruff check src/ tests/
/home/harish/Documents/tif1/.venv/bin/ruff format src/ tests/
/home/harish/Documents/tif1/.venv/bin/ty check --python /home/harish/Documents/tif1/.venv src/tif1

# Warm A/B (control vs your tree, interleaved, both arms on your cache copy):
$PY /home/harish/Documents/tif1/tools/perf/delphi_warm_ab.py \
    --control-src /tmp/naxos/control/src --control-cache /tmp/naxos/warm-<ID> \
    --candidate-src /tmp/naxos/<ID>/src --candidate-cache /tmp/naxos/warm-<ID> \
    --lib pandas --pairs 8 --workloads laps,all_tel,notebook \
    --out .agents/perf-naxos/<ID>_ab_pandas.json

# Cold A/B (empty cache per run, live CDN; 5 reps; the network is shared, so
# keep the reps modest and say in the report what else was running):
$PY /home/harish/Documents/tif1/tools/perf/delphi_cold_bench.py \
    --lib pandas --reps 5 --control-src /tmp/naxos/control/src \
    --candidate-src /tmp/naxos/<ID>/src --workloads laps,fastest_lap_tel \
    --out .agents/perf-naxos/<ID>_cold_pandas.json

# Parity (0 differing surfaces unless your section allows a difference; then
# explain every one in the report):
TIF1_CACHE_DIR=/tmp/naxos/warm-<ID> TIF1_OFFLINE_MODE=true $PY \
    /home/harish/Documents/tif1/tools/perf/delphi_parity.py --lib pandas --out /tmp/naxos/<ID>_parity_pandas.json
$PY /home/harish/Documents/tif1/tools/perf/delphi_parity.py --compare /tmp/naxos/parity_pandas.json /tmp/naxos/<ID>_parity_pandas.json
# (and --lib polars against /tmp/naxos/parity_polars.json when you touch polars code)
```

Workloads of `delphi_warm_bench.py` (2025 Bahrain race unless noted; each a
fresh interpreter, offline, primed cache): `import_get_session`, `laps`,
`load_only` (load() then laps and results), `results`, `state`,
`fastest_lap_tel`, `driver_fastest_tel`, `driver_tel`
(`laps.pick_driver("VER").telemetry`), `car_data_loop` (per-lap
`get_car_data()` over VER's laps), `all_tel` (`fetch_all_laps_telemetry()`),
`notebook` (load(weather, messages) + 8 pick_* + iterlaps lap.telemetry loop
over 4 drivers + 200 iloc + 4 drivers' merged telemetry + a 4-lap analysis
(add_distance, add_differential_distance, resample_channels("0.1s"),
fill_missing, slice_by_time, merge_channels) + fastest laps + results +
weather), `quali_split`.

## Baseline (control tree d28ce70, this box, fresh process; ms; median/min of 5;
## `wall` is interpreter start to exit, phases in brackets)

(Measured with tif1-native 0.2.0; the main venv has 0.2.1 since, which moves
the pandas frame-tier phases by a few ms: G1/G6. Your A/Bs run both arms on
0.2.1, so compare arms, not against these numbers.)

pandas warm: import+get_session 470 (get_session 392); laps 482; load_only
497 (load 11, results 10); results 459; state 466; fastest_lap_tel 517 (13);
driver_fastest_tel 482 (21); driver_tel 499 (27); car_data_loop 479 (28);
all_tel 560 (103); notebook 744 (load 11, picks 12, iterlaps telemetry loop
148, iloc 19, merged x4 36, analysis x4 20, driver fastest x4 13);
quali_split 470 (split 6).

polars warm: import+get_session 210 (get_session 141); laps 216; load_only
349 (load 116: the eager load reads every driver's IPC row; pandas keeps them
lazy since Delphi D5); results 220; state 219; fastest_lap_tel 218 (12);
driver_fastest_tel 222 (16); driver_tel 238 (21); car_data_loop 240 (24);
all_tel 362 (117); notebook 583 (load 120, iterlaps loop 41, analysis x4 158:
the numpy import ~100 ms of it plus 53 polars query plans in
`resample_channels`); quali_split 239.

Fixed costs measured for this round: bare interpreter 30 ms; `import pandas`
341 ms in process (numpy 79 of it, OpenBLAS thread-pool start ~35-45 ms of
numpy's on this 32-core box: `OPENBLAS_NUM_THREADS=1` makes numpy's import
62 ms instead of 97-115; `pyarrow.compute` 40 ms imported by pandas' Arrow
accessors; `pyarrow.lib` 31 ms); `import polars` 145 ms; interpreter exit
20 ms after `laps`, ~50 ms after `fetch_all_laps_telemetry()` on pandas
(eight decompression threads, big buffers), ~40 ms polars.

Cold (empty cache, live CDN, median of 3, whole process; see
`/tmp/naxos/baseline_cold_*.json`): pandas laps 590, fastest_lap_tel 600,
driver_tel 1600-3900 (high variance), notebook (eager load) 10000-12000,
all_tel 9300-10400; polars laps 355, fastest_lap_tel 400, driver_tel 1015.
A race's telemetry is 1128 lap files of ~61 KB gzipped JSON (~69 MB on the
wire).

The box: 32 cores, 30 GB, Linux, Python 3.13. Up to ten agents run at once,
so absolute numbers drift; read ratios and pairs won, and always interleave.

## Rules

1. Performance is the deliverable, parity is the constraint. A change that
   alters a user-visible value, dtype, column order, index or class fails
   unless your section explicitly allows it. fastf1 parity matters: when in
   doubt, keep fastf1's behaviour.
2. Follow `AGENTS.md`: a polars session never imports pandas (nor pyarrow on
   its hot paths); no query plans on polars hot paths; orjson, not json;
   custom exceptions; Windows rules (explicit `encoding="utf-8"`,
   `.as_posix()`); type hints; Google docstrings; ruff (line length 100).
   Keep `tests/unit/test_polars_without_pandas.py` green and extend it when
   you remove an import from a polars path. Any new private upstream API
   needs an entry and a test in `tests/unit/test_private_seams.py`.
3. No new runtime dependency unless your section allows it.
4. Measure before and after, interleaved, with the harness above. Report
   medians and pairs won. A win is a candidate median at least 3% better on
   the whole-process wall of the named workload, or a clearly larger
   in-process phase win with the wall not worse, and at least 6 of 8 pairs
   won. Reject honestly: a measured negative result with numbers is a valid
   deliverable.
5. Add or adjust unit tests for new code paths (offline, no sockets). Do not
   weaken existing tests to make them pass.
6. Keep commits on your branch small and descriptive (`perf(naxos-<ID>): ...`).
   Do not merge, rebase, push, or touch other branches. If `git commit` is
   denied by your sandbox, leave everything staged (`git add -A`) and put the
   intended commit message in `.agents/perf-naxos/<ID>_COMMITS.md`.
7. Research online when your section calls for it (upstream docs, issues,
   source of numpy/OpenBLAS/polars/pandas/CPython): cite what you used.
8. Time box: about 3 hours of wall time. If the hypothesis is falsified
   early, write the report and stop. Write progress notes to
   `.agents/perf-naxos/<ID>_PROGRESS.md` in your tree as you go (one line per
   milestone with the time): the coordinator reads them every 10 minutes.

## Deliverable: `.agents/perf-naxos/<ID>.md` in your tree, committed (or staged)

Sections: (1) Hypothesis (one paragraph); (2) What changed (files,
mechanism); (3) Measurements: the A/B table(s) (workload, phase, control
median, candidate median, pairs won), the parity result (`N differing
surfaces`, with every allowed difference explained), unit tests / ruff / ty
status; (4) Verdict: SHIP, PARTIAL (what to keep) or REJECT, with the reason;
(5) A CHANGELOG paragraph in the voice of the repository's `CHANGELOG.md`
"Performance" entries (plain engineer English, numbers, no marketing); (6)
Notes for the integrator (risks, follow-ups, anything surprising). End your
run with a short summary of the verdict and the key numbers.
