# Pergamon wave 2 — shared brief for every hypothesis agent

You are running ONE performance hypothesis for the tif1 library (F1 timing data,
pandas/polars). Prior work you must not repeat: `.agents/perf*/RESULTS.md`
(13 series, ~130 hypotheses) — read `.agents/perf-pergamon/RESULTS.md` and
`PLAN.md` first (the warm path was just cut to ~0.28 s: import 0.22 s, laps
1 ms, telemetry 48 ms). Read `AGENTS.md` for build/test/lint commands.

## Environment
- Your checkout is an isolated git worktree on its own branch; commit your
  change there when done (one focused commit; do not reformat unrelated code,
  do not touch files you do not need — other agents edit the same modules and
  conflicts cost integration time).
- Control tree (frozen, read-only): `/tmp/tif1-pergamon-control/src`.
- Primed warm cache snapshot (2026 Monaco GP Race, pandas driver rows + polars
  IPC rows + laps/weather/rcm): `/tmp/tif1-warm-q-base/cache.sqlite`. COPY it
  to your own directory before use (`mkdir -p /tmp/tif1-q-<ID> && cp ...`);
  never write to the snapshot or to `~/.cache/tif1`. Set `TIF1_CACHE_DIR`
  (or `TIF1_WARM_CACHE_DIR` for the benchmark tool) BEFORE importing tif1.
- Other agents benchmark on this 32-core box at the same time: always use
  interleaved A/B pairs (control vs candidate, fresh process per run) and
  report medians + per-pair win counts, never a single run. Use
  `uv run python tools/pergamon_warm_ab.py --control-src /tmp/tif1-pergamon-control/src --control-cache <copy1> --candidate-cache <copy2> --pairs 6`
  for whole-load numbers and `tools/pergamon_idioms_bench.py --src-dir ...`
  for per-lap idioms; write your own fresh-process micro-harness when the
  hypothesis is narrower.
- The network is reachable but slow and irrelevant: everything is warm-cache.
- Parity is mandatory for anything that touches frames: columns, dtypes,
  values (`DataFrame.equals`), block count — against the control tree's
  output on the full 1452-frame session (and polars schema + `equals`).

## Deliverables
1. Verdict KEPT / REJECTED with the measured numbers (before/after, pairs).
2. If KEPT: the change committed in your worktree with a contract test added
   to `tests/unit/test_pergamon_contracts.py` (append a section) or a new
   test file; `uv run ruff check src/ tests/ tools/`, `uv run ruff format`,
   `uv run ty check src/tif1`, and the relevant unit tests must pass
   (`uv run pytest tests/unit -q -x -n 8` at least once at the end).
3. A report file `.agents/perf-pergamon/q/<ID>.md` (committed) with:
   hypothesis, mechanism, protocol, numbers, verdict, files changed, risks.
4. Your final message: the verdict line, the numbers, the commit hash and
   branch name, and anything the integrator must know (conflicts, caveats).
If the hypothesis is falsified early by a probe, stop there, write the report
with the probe numbers, and say so — a clean rejection is a valid outcome.
Do not pad: if the win is below the noise floor, say REJECTED (noise).
