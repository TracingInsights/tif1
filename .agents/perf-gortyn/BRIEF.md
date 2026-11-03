# Gortyn performance round: shared brief for the hypothesis agents

tif1 (https://github.com/TracingInsights/tif1) is a Python library serving
Formula 1 timing and telemetry data, a faster drop-in for fastf1. Its whole
reason to exist is speed. This round ("Gortyn") runs four hypotheses, one per
agent, each in its own git worktree, against a frozen control tree: the
Delphi round's result (branch `perf/delphi`, commit b5b91ed, PR #119). Read
the repository's `AGENTS.md` (architecture, constraints, style) before
writing code; it is authoritative. Then read your section of
`HYPOTHESES.md` (same folder as this file).

Fifteen rounds already ran (Monaco ... Olympia, Delphi). Their rejected ideas
are listed in `DO_NOT_RETRY.md` next to this file: do not spend time on
them. The Delphi reports (`/home/harish/Documents/tif1/.agents/perf-delphi/`,
`RESULTS.md` and `D<k>.md`) describe the current state of every hot path
your hypothesis touches; read the ones your section names.

## Where things are

- Main repository (READ ONLY for you, never edit, never run git here):
  `/home/harish/Documents/tif1`. Its virtualenv is the one interpreter you use:
  `/home/harish/Documents/tif1/.venv/bin/python` (3.13, pandas 3.0.6, polars 2,
  pyarrow, tif1-native 0.2 installed).
- Frozen control tree (READ ONLY): `/tmp/tif1-gortyn-control` (commit f692ee7,
  `main`). The A/B harness runs it with
  `PYTHONPATH=/tmp/tif1-gortyn-control/src`.
- Your worktree: `/tmp/tif1-gortyn/<ID>` on branch `perf/gortyn-<ID>` (the ID is
  in your prompt, e.g. `G3`). Only files in this worktree may be edited or
  committed. Do not touch other agents' worktrees.
- Your private warm cache (290 MB SQLite, primed with the 2025 Bahrain Grand
  Prix race and qualifying on both backends, laps/telemetry/results/state):
  `/tmp/tif1-gortyn-warm-<ID>`. Never point anything at the control cache
  `/tmp/tif1-delphi-warm` (shared, read-only by convention) other than
  through the A/B harness, which reads it.
- Harness (in the main repo; run it from your worktree with the paths shown).
  You may copy a harness file into your worktree's `tools/perf/` and extend
  it there (a new workload, a `--candidate-python` option) when your section
  needs it; then run your copy and say so in the report.
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
  - Control digests for this round: `/tmp/gortyn_parity_pandas.json` and
    `/tmp/gortyn_parity_polars.json`.

## How to run things (no `uv run`, no `uv sync` in the worktree unless your
## section says so: they would build a second environment)

All commands from inside your worktree (`cd /tmp/tif1-gortyn/<ID>`):

```bash
PY=/home/harish/Documents/tif1/.venv/bin/python
export PYTHONPATH=/tmp/tif1-gortyn/<ID>/src

# Unit tests (sockets are blocked by a fixture; at most 6 workers, other
# agents share this 32-core box). Keep --no-cov, coverage is checked at merge.
$PY -m pytest tests/unit -q -x --no-cov -p no:cacheprovider -n 6

# Lint, format, types (all three must be clean before you commit):
/home/harish/Documents/tif1/.venv/bin/ruff check src/ tests/
/home/harish/Documents/tif1/.venv/bin/ruff format src/ tests/
/home/harish/Documents/tif1/.venv/bin/ty check --python /home/harish/Documents/tif1/.venv src/tif1

# Warm A/B (control vs your tree, interleaved, each arm on its own cache):
$PY /home/harish/Documents/tif1/tools/perf/delphi_warm_ab.py \
    --control-src /tmp/tif1-gortyn-control/src --control-cache /tmp/tif1-delphi-warm \
    --candidate-src /tmp/tif1-gortyn/<ID>/src --candidate-cache /tmp/tif1-gortyn-warm-<ID> \
    --lib pandas --pairs 6 --workloads laps,all_tel,notebook \
    --out .agents/perf-gortyn/<ID>_ab_pandas.json

# Cold A/B (empty cache per run, live CDN; 5 reps; the network is shared, so
# never run two cold benches at once and keep the reps modest):
$PY /home/harish/Documents/tif1/tools/perf/delphi_cold_bench.py \
    --lib pandas --reps 5 --control-src /tmp/tif1-gortyn-control/src \
    --candidate-src /tmp/tif1-gortyn/<ID>/src --workloads laps,fastest_lap_tel \
    --out .agents/perf-gortyn/<ID>_cold_pandas.json

# Parity (0 differing surfaces unless your section allows a difference; then
# explain every one in the report):
TIF1_CACHE_DIR=/tmp/tif1-gortyn-warm-<ID> TIF1_OFFLINE_MODE=true $PY \
    /home/harish/Documents/tif1/tools/perf/delphi_parity.py --lib pandas --out /tmp/<ID>_parity_pandas.json
$PY /home/harish/Documents/tif1/tools/perf/delphi_parity.py --compare /tmp/gortyn_parity_pandas.json /tmp/<ID>_parity_pandas.json
# (and --lib polars against /tmp/gortyn_parity_polars.json when you touch polars code)
```

Workloads of `delphi_warm_bench.py` (2025 Bahrain race unless noted; each a
fresh interpreter, offline, primed cache): `import_get_session`, `laps`,
`load_only` (load() then laps and results), `results`, `state`,
`fastest_lap_tel`, `driver_fastest_tel`, `driver_tel`
(`laps.pick_driver("VER").telemetry`), `car_data_loop` (per-lap
`get_car_data()` over VER's laps), `all_tel` (`fetch_all_laps_telemetry()`),
`notebook` (load(weather, messages) + 8 pick_* + iterlaps lap.telemetry loop
+ 200 iloc + 4 drivers' merged telemetry + a 4-lap analysis + fastest laps +
results + weather), `quali_split`.

## Baseline (control tree = 0.11.3, this box, fresh process; ms, median of 6;
## `wall` is interpreter start to exit, phases in brackets)

pandas warm: import+get_session 454; laps 456; load_only 499 (load 12);
results 473 (12); state 438; fastest_lap_tel 483 (15); driver_fastest_tel
468 (21); driver_tel 434 (26); car_data_loop 485 (26); all_tel 588 (98);
notebook 757 (load 12, picks 10, iterlaps loop 157, iloc 19, merged x4 38,
analysis x4 21, driver fastest x4 14); quali_split 473 (split 6). The pandas
import is ~342 of every wall, tif1's own startup ~20, teardown ~30.

polars warm: import+get_session 211; laps 232; load_only 356 (load 117);
results 215; state 206; fastest_lap_tel 217 (12); driver_fastest_tel 219
(17); driver_tel 228 (23); car_data_loop 224 (24); all_tel 328 (106);
notebook 584 (picks 100: the `pick_*` family other than `pick_driver` still
imports numpy there); quali_split 221 (split 1).

Cold (empty cache, live CDN, median of 5, whole process): pandas laps 578,
fastest_lap_tel 585, driver_tel 1238 (first-ever) / 1200 (returning user),
four-driver notebook eager 11062 / `load(telemetry="lazy")` 3183; polars
481, 904, 2256 / 2193, 10037 / 2716. A race's telemetry is 1128 lap files
of ~61 KB gzipped JSON (~69 MB on the wire, 5-8 MB/s per connection, 22
connections): the eager `load()` is bandwidth-bound at ~9 s.

The box: 32 cores, 30 GB, Linux, Python 3.13. Several agents run at once, so
absolute numbers drift; read ratios and pairs won, and always interleave.

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
   you remove an import from a polars path.
3. No new runtime dependency unless the section allows it.
4. Measure before and after, interleaved, with the harness above. Report
   medians and pairs won. A win is a candidate median at least 3% better on
   the whole-process wall of the named workload, or a clearly larger
   in-process phase win with the wall not worse, and at least 4 of 6 pairs
   won. Reject honestly: a measured negative result with numbers is a valid
   deliverable.
5. Add or adjust unit tests for new code paths (offline, no sockets). Do not
   weaken existing tests to make them pass.
6. Keep commits on your branch small and descriptive (`perf(gortyn-<ID>): ...`).
   Do not merge, rebase, push, or touch other branches. If `git commit` is
   denied by your sandbox, leave everything staged (`git add -A`) and put the
   intended commit message in `.agents/perf-gortyn/<ID>_COMMITS.md`.
7. Time box: about 2.5 hours of wall time. If the hypothesis is falsified
   early, write the report and stop.

## Deliverable: `.agents/perf-gortyn/<ID>.md` in your worktree, committed (or staged)

Sections: (1) Hypothesis (one paragraph); (2) What changed (files,
mechanism); (3) Measurements: the A/B table(s) (workload, phase, control
median, candidate median, pairs won), the parity result (`N differing
surfaces`, with every allowed difference explained), unit tests / ruff / ty
status; (4) Verdict: SHIP, PARTIAL (what to keep) or REJECT, with the reason;
(5) A CHANGELOG paragraph in the voice of the repository's `CHANGELOG.md`
"Performance" entries (plain engineer English, numbers, no marketing); (6)
Notes for the integrator (risks, follow-ups, anything surprising). End your
run with a short summary of the verdict and the key numbers.
