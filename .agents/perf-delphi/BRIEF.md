# Delphi performance round: shared brief for the hypothesis agents

tif1 (https://github.com/TracingInsights/tif1) is a Python library serving
Formula 1 timing and telemetry data, a faster drop-in for fastf1. Its whole
reason to exist is speed. This round ("Delphi") runs ten hypotheses, one per
agent, each in its own git worktree, against a frozen control tree. Read the
repository's `AGENTS.md` (architecture, constraints, style) before writing
code; it is authoritative. Then read your section of `HYPOTHESES.md` (same
folder as this file).

Fourteen rounds already ran (Monaco, Lindos, Kalchedon, Sestos, Tyria, Rhodes,
Ithome, Halikarnassos, Kroton, Argos, Thebes, Pergamon, Olympia, ...). Their
rejected ideas are listed in `DO_NOT_RETRY.md` next to this file: do not spend
time on them. Your hypothesis section tells you exactly what was measured and
why we believe there is a win.

## Where things are

- Main repository (READ ONLY for you, never edit, never run git here):
  `/home/harish/Documents/tif1`. Its virtualenv is the one interpreter you use:
  `/home/harish/Documents/tif1/.venv/bin/python` (3.13, pandas 3.0.6, polars 2,
  pyarrow, tif1-native installed).
- Frozen control tree (READ ONLY): `/tmp/tif1-delphi-control` (commit 255759b,
  `main`). The A/B harness runs it with `PYTHONPATH=/tmp/tif1-delphi-control/src`.
- Your worktree: `/tmp/tif1-delphi/<ID>` on branch `perf/delphi-<ID>` (the ID
  is in your prompt, e.g. `D4`). Only files in this worktree may be edited or
  committed. Do not touch other agents' worktrees.
- Your private warm cache (300 MB SQLite, primed with the 2025 Bahrain Grand
  Prix race and qualifying on both backends, laps/telemetry/results/state):
  `/tmp/tif1-delphi-warm-<ID>`. Never point anything at the control cache
  `/tmp/tif1-delphi-warm` (it is shared and read-only by convention) other
  than through the A/B harness, which reads it.
- Delphi tools (in the main repo, run them from your worktree with the paths
  shown; do not copy or edit them):
  - `/home/harish/Documents/tif1/tools/perf/delphi_warm_bench.py`: fresh-process
    warm workloads (`--child <workload>` is the per-process entry).
  - `/home/harish/Documents/tif1/tools/perf/delphi_warm_ab.py`: interleaved
    control-vs-candidate A/B over those workloads (whole-process wall time plus
    in-process phases, medians and pairs won).
  - `/home/harish/Documents/tif1/tools/perf/delphi_parity.py`: a digest of 130+
    user-visible surfaces of the two sessions (laps, results, drivers, state
    tables, fastest-lap telemetry, driver telemetry, pick_*, split, resample,
    fill_missing, car/pos data, every frame of `fetch_all_laps_telemetry`),
    and `--compare a.json b.json`.
  - `/home/harish/Documents/tif1/tools/perf/delphi_cold_bench.py`: cold (empty
    cache, live CDN) workloads, for hypotheses that touch the cold path only.
  - Control digests already written: `/tmp/delphi_parity_pandas.json` and
    `/tmp/delphi_parity_polars.json`.

## How to run things (no `uv run`, no `uv sync` in the worktree: they would
## build a second environment)

All commands from inside your worktree (`cd /tmp/tif1-delphi/<ID>`):

```bash
PY=/home/harish/Documents/tif1/.venv/bin/python
export PYTHONPATH=/tmp/tif1-delphi/<ID>/src

# Unit tests (sockets are blocked by a fixture; use at most 6 workers, other
# agents share this 32-core box). Keep --no-cov, coverage is checked at merge.
$PY -m pytest tests/unit -q -x --no-cov -p no:cacheprovider -n 6
# One file / one test:
$PY -m pytest tests/unit/test_core.py -q --no-cov -p no:cacheprovider -k name

# Lint, format, types (all three must be clean before you commit):
/home/harish/Documents/tif1/.venv/bin/ruff check src/ tests/
/home/harish/Documents/tif1/.venv/bin/ruff format src/ tests/
/home/harish/Documents/tif1/.venv/bin/ty check --python /home/harish/Documents/tif1/.venv src/tif1

# Warm workload of your tree alone (fresh process per rep, offline):
TIF1_CACHE_DIR=/tmp/tif1-delphi-warm-<ID> $PY /home/harish/Documents/tif1/tools/perf/delphi_warm_bench.py \
    --lib pandas --reps 5 --workloads laps,all_tel

# The A/B that decides your verdict (control vs your tree, interleaved,
# each arm on its own cache; 6 pairs; pick the workloads your section names):
$PY /home/harish/Documents/tif1/tools/perf/delphi_warm_ab.py \
    --control-src /tmp/tif1-delphi-control/src --control-cache /tmp/tif1-delphi-warm \
    --candidate-src /tmp/tif1-delphi/<ID>/src --candidate-cache /tmp/tif1-delphi-warm-<ID> \
    --lib pandas --pairs 6 --workloads laps,all_tel,notebook \
    --out .agents/perf-delphi/<ID>_ab_pandas.json

# Parity (must be 0 differing surfaces unless your section says a surface
# changes on purpose; then explain every difference in your report):
TIF1_CACHE_DIR=/tmp/tif1-delphi-warm-<ID> TIF1_OFFLINE_MODE=true $PY \
    /home/harish/Documents/tif1/tools/perf/delphi_parity.py --lib pandas --out /tmp/<ID>_parity_pandas.json
$PY /home/harish/Documents/tif1/tools/perf/delphi_parity.py --compare /tmp/delphi_parity_pandas.json /tmp/<ID>_parity_pandas.json
# (and --lib polars against /tmp/delphi_parity_polars.json when you touch polars code)
```

Workloads of `delphi_warm_bench.py` (2025 Bahrain race unless noted; each a
fresh interpreter, offline, primed cache): `import_get_session`, `laps`,
`results`, `state` (weather, rcm, track_status, session_status, t0_date),
`fastest_lap_tel`, `driver_fastest_tel` (`get_driver("VER").get_fastest_lap_tel()`),
`driver_tel` (`laps.pick_driver("VER").telemetry`), `car_data_loop` (per-lap
`get_car_data()` over VER's laps), `all_tel` (`fetch_all_laps_telemetry()`),
`notebook` (load(weather, messages) + 8 pick_* + iterlaps lap.telemetry loop +
200 iloc + 4 drivers' merged telemetry + a 4-lap analysis (add_distance,
add_differential_distance, resample_channels("0.1s"), fill_missing,
slice_by_time, merge_channels) + get_fastest_lap_tel + 4x
get_driver(d).get_fastest_lap() + results + weather), `quali_split`
(Qualifying: laps, split_qualifying_sessions, results).

## Baseline (control tree, this box, fresh process, warm cache, offline; ms,
## median of 5; `wall` is interpreter start to exit)

pandas: import+get_session wall 545 (in-process 424: the pandas import is
~342 of it, tif1's own startup ~60, interpreter teardown ~80); laps 553 (laps
phase 3); results 569 (10); state 545 (weather 1, rcm 1, track_status 2,
session_status 3); fastest_lap_tel 567 (8); driver_fastest_tel 571 (20);
driver_tel 570 (23); car_data_loop 594 (25); all_tel 715 (114); notebook 1048
(load 111, picks_x8 11, iterlaps_tel_loop 77, iloc_200 27, driver_merged_x4 76,
telemetry_analysis_4 65, fastest_lap_tel 4, driver_fastest_lap_x4 35);
quali_split 672 (split 9, results 17).

polars: import+get_session wall 371 (in-process 275: polars import ~180,
numpy ~54 on top, teardown ~50); laps 364 (6); results 366 (17); state 662
(t0_date 231: it imports pandas); fastest_lap_tel 389 (12); driver_fastest_tel
685 (268: it imports pandas and pyarrow); driver_tel 446 (48, of which the
pyarrow import ~27).

Cold (empty cache, live CDN, median of 5, ms): pandas laps 906 (get_session
592, laps 149), fastest_lap_tel 974, driver_tel 1568, notebook 11010 (load()
fetches all 1128 telemetry files: 9.2 s, bandwidth-bound on this link);
polars 799 / 813 / 1764 / 11257.

The box: 32 cores, 30 GB, Linux, Python 3.13. Several agents run at once, so
absolute numbers drift; read ratios and pairs won, and always interleave.

## Rules

1. Performance is the deliverable, parity is the constraint. A change that
   alters a user-visible value, dtype, column order, index or class fails
   unless your section explicitly allows it. fastf1 parity matters: when in
   doubt, keep fastf1's behaviour.
2. Follow `AGENTS.md`: a polars session never imports pandas (and should not
   import pyarrow on its hot paths either); no query plans on polars hot
   paths; orjson, not json; custom exceptions; Windows rules (explicit
   `encoding="utf-8"`, `.as_posix()`); type hints; Google docstrings; ruff
   (line length 100). Keep `tests/unit/test_polars_without_pandas.py` green
   and extend it when you remove a pandas/pyarrow import from a polars path.
3. No new runtime dependency unless the section allows it.
4. Measure before and after, interleaved, with the harness above. Report
   medians and pairs won. A win is a candidate median at least 3% better on
   the whole-process wall of the named workload, or a clearly larger in-process
   phase win with the wall not worse, and at least 4 of 6 pairs won. Reject
   honestly: a measured negative result with numbers is a valid deliverable.
5. Add or adjust unit tests for new code paths (offline, no sockets). Do not
   weaken existing tests to make them pass.
6. Keep commits on your branch small and descriptive (`perf(delphi-<ID>): ...`).
   Do not merge, rebase, push, or touch other branches.
7. Time box: about 2 hours of wall time. If the hypothesis is falsified
   early, write the report and stop.

## Deliverable: `.agents/perf-delphi/<ID>.md` in your worktree, committed

Sections: (1) Hypothesis (one paragraph); (2) What changed (files, mechanism);
(3) Measurements: the A/B table(s) (workload, phase, control median,
candidate median, pairs won), the parity result (`N differing surfaces`,
with every allowed difference explained), unit tests / ruff / ty status;
(4) Verdict: SHIP, PARTIAL (what to keep) or REJECT, with the reason;
(5) A CHANGELOG paragraph in the voice of the repository's `CHANGELOG.md`
"Performance" entries (plain engineer English, numbers, no marketing);
(6) Notes for the integrator (risks, follow-ups, anything surprising).
