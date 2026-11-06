# Corinth performance round: shared brief for the hypothesis agents

tif1 (https://github.com/TracingInsights/tif1) is a Python library serving
Formula 1 timing and telemetry data, a faster drop-in for fastf1. Its whole
reason to exist is speed. This round ("Corinth") runs ten hypotheses, one per
agent, each in its own sparse git clone, against a frozen control tree:
`main` after the Naxos round (PR #126, commit 552c357). Read the
repository's `AGENTS.md` (architecture, constraints, style) before writing
code; it is authoritative. Then read your section of `HYPOTHESES.md` (same
folder as this file).

Nineteen rounds already ran (Monaco ... Elis, Gortyn, Naxos). Their rejected
ideas are listed in `DO_NOT_RETRY.md` next to this file: do not spend time on
them. The round reports in `/home/harish/Documents/tif1/.agents/perf-*/`
(`RESULTS.md` and the per-hypothesis `<ID>.md`) describe the current state of
every hot path; read the ones your section names.

## Where things are

- Main repository (READ ONLY for you, never edit, never run git there):
  `/home/harish/Documents/tif1`. Its virtualenv is the one interpreter you use:
  `/home/harish/Documents/tif1/.venv/bin/python` (3.13, pandas 3.1, polars 2.1,
  numpy 2.5, pyarrow 26, tif1-native 0.2.1). A Python 3.14 env with the same
  locked dependencies: `/tmp/corinth/venv314/bin/python` (C5 only).
- Disk is short: no git worktrees, no second virtualenv unless your section
  says so, no copies of the warm cache beyond the ones named here. Delete
  scratch files over 50 MB when you are done with them.
- Frozen control tree (READ ONLY): `/tmp/corinth/control` (commit 552c357, a
  sparse clone: `src/`, `tests/`, `tools/`, `native/` sources, no `docs/`).
  The A/B harness runs it with `PYTHONPATH=/tmp/corinth/control/src`.
- Your tree: `/tmp/corinth/<ID>` on branch `perf/corinth-<ID>` (the ID is in your
  prompt, e.g. `C3`), a `git clone --shared` of the main repository with the
  same sparse checkout. Only files in this tree may be edited or committed.
  Do not touch other agents' trees. If you must edit a doc page, run
  `git sparse-checkout add /docs/` in your tree first.
- Your warm cache: `/tmp/corinth/warm-<ID>` (290 MB SQLite, a copy of the
  round's primed cache: 2025 Bahrain Grand Prix race and qualifying on both
  backends, laps/telemetry/results/state and every frame tier). Run BOTH
  arms of your A/Bs on this one copy (two copies of the same cache differ by
  up to 23 ms on `all_tel`: Gortyn G1), unless your change alters what the
  cache stores; then make one more copy `/tmp/corinth/warm-<ID>-cand` with
  `cp -a` and delete it when done. Never write to `/tmp/corinth/warm` (the
  master copy). A season cache (every 2025 race's laps, results and
  schedule, no telemetry) is at `/tmp/corinth/season` (READ ONLY: copy it).
- Control parity digests: `/tmp/corinth/parity_pandas.json`,
  `/tmp/corinth/parity_polars.json`. Survey scripts (the numbers in
  `HYPOTHESES.md`): `/tmp/corinth/scripts/survey.py <flow> <lib>` (flows
  season, schedule, ergast, old, plot) and `/tmp/corinth/scripts/current.py
  <lib> <label>` (a 2026 race).
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

All commands from inside your tree (`cd /tmp/corinth/<ID>`):

```bash
PY=/home/harish/Documents/tif1/.venv/bin/python
export PYTHONPATH=/tmp/corinth/<ID>/src HOME=/tmp/corinth/home-<ID>
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
    --control-src /tmp/corinth/control/src --control-cache /tmp/corinth/warm-<ID> \
    --candidate-src /tmp/corinth/<ID>/src --candidate-cache /tmp/corinth/warm-<ID> \
    --lib pandas --pairs 8 --workloads laps,all_tel,notebook \
    --out .agents/perf-corinth/<ID>_ab_pandas.json

# Cold A/B (empty cache per run, live CDN; 5 reps; the network is shared, so
# keep the reps modest and say in the report what else was running):
$PY /home/harish/Documents/tif1/tools/perf/delphi_cold_bench.py \
    --lib pandas --reps 5 --control-src /tmp/corinth/control/src \
    --candidate-src /tmp/corinth/<ID>/src --workloads laps,fastest_lap_tel \
    --out .agents/perf-corinth/<ID>_cold_pandas.json

# Parity (0 differing surfaces unless your section allows a difference; then
# explain every one in the report):
TIF1_CACHE_DIR=/tmp/corinth/warm-<ID> TIF1_OFFLINE_MODE=true $PY \
    /home/harish/Documents/tif1/tools/perf/delphi_parity.py --lib pandas --out /tmp/corinth/<ID>_parity_pandas.json
$PY /home/harish/Documents/tif1/tools/perf/delphi_parity.py --compare /tmp/corinth/parity_pandas.json /tmp/corinth/<ID>_parity_pandas.json
# (and --lib polars against /tmp/corinth/parity_polars.json when you touch polars code)
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

## Baseline (control tree 552c357, this box, fresh process; ms)

Warm (2025 Bahrain race, `delphi_warm_bench.py`, Python 3.13 / 3.14):
pandas laps wall 444 / 476, all_tel 629 / 680 (phase 102-152 / 120),
notebook lap loop 126 / 140; polars laps 204 / 226, all_tel 356 / 369,
notebook lap loop 144-165 (the lazy default reads rows there). `import
pandas` is 341 ms of every pandas script; `import polars` ~110 in process.

Flows no earlier round measured (`/tmp/corinth/scripts/survey.py`, fresh
process, live CDN, cold = empty cache):
- season loop, 2025, 24 races, `get_session(2025, r, "R").laps` +
  `.results` per race: cold pandas 15.5 s (loop), polars 0.56 s on the
  pandas-primed cache; warm pandas 122 ms, polars 195 ms;
  `get_event_schedule(2025)` 394-469 ms (the pandas import, on polars too).
- current season (2026 Mexico City race, `current.py`): cold pandas
  get_session 1094, laps 442, results 1242 (total 2.8 s); warm 507 total
  (polars 163); warm with the schedule and Ergast TTLs expired (every script
  6 h after the last refresh): pandas 722, polars 463.
- Ergast interface: cold `get_race_results(season=2024)` 1185 ms,
  `get_driver_standings(season=2024)` 520, `get_qualifying_results(2023, 5)`
  672; warm 54 / 2 / 2.
- pre-2018 session (1998 Monaco race): cold get_session 442, laps 6524 ms,
  results 6; warm laps 23 (polars 26).
- plotting: `import tif1.plotting` 539 ms (matplotlib 253, pyplot 127);
  a warm speed trace to PNG 89-103 ms.
- CLI: `python -m tif1.cli --help` / `version` 200-230 ms.

The link here caps at ~10 MB/s (Naxos N9). The network is shared by every
agent: interleave control and candidate (ABAB), keep cold reps modest, and
say in the report what else was running.

The box: 32 cores, 30 GB, Linux. Up to ten agents run at once, so absolute
numbers drift; read ratios and pairs won, and always interleave.

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
6. Keep commits on your branch small and descriptive (`perf(corinth-<ID>): ...`).
   Do not merge, rebase, push, or touch other branches. If `git commit` is
   denied by your sandbox, leave everything staged (`git add -A`) and put the
   intended commit message in `.agents/perf-corinth/<ID>_COMMITS.md`.
7. Research online when your section calls for it (upstream docs, issues,
   source of numpy/OpenBLAS/polars/pandas/CPython): cite what you used.
8. Time box: about 3 hours of wall time. If the hypothesis is falsified
   early, write the report and stop. Write progress notes to
   `.agents/perf-corinth/<ID>_PROGRESS.md` in your tree as you go (one line per
   milestone with the time): the coordinator reads them every 10 minutes.

## Deliverable: `.agents/perf-corinth/<ID>.md` in your tree, committed (or staged)

Sections: (1) Hypothesis (one paragraph); (2) What changed (files,
mechanism); (3) Measurements: the A/B table(s) (workload, phase, control
median, candidate median, pairs won), the parity result (`N differing
surfaces`, with every allowed difference explained), unit tests / ruff / ty
status; (4) Verdict: SHIP, PARTIAL (what to keep) or REJECT, with the reason;
(5) A CHANGELOG paragraph in the voice of the repository's `CHANGELOG.md`
"Performance" entries (plain engineer English, numbers, no marketing); (6)
Notes for the integrator (risks, follow-ups, anything surprising). End your
run with a short summary of the verdict and the key numbers.
