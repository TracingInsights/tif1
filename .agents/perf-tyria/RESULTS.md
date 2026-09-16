# Tyria Series — Results (10 hypotheses, all run)

Benchmark corpus: 2026 Monaco GP Race telemetry (~1452 payloads / 84MB), same as prior series.
Sandbox: Modal (burstable CPU), 8 GiB. Prior shipped work (H/E/F/G/K/L/N) not repeated.
Rule "no format-guessing change may alter parsed values" enforced throughout:
`format="ISO8601"` fast paths apply only where real payloads are proven uniform
(all-9-digit-fraction; verified live on CDN across 2021–2025 sessions + unit
fixtures).
Full suite: 1304 passed + 2 flaky timing-sensitive property tests (pass in isolation),
coverage 84.55% (≥80% bar).

## Kept (shipped in this PR)

| ID | Change | Measured result | Parity |
|----|--------|-----------------|--------|
| T1 | Fix inverted async cache-enable condition (`core.py:3213`): `not self.enable_cache or ...` → `self.enable_cache and ...`. Old code opened the SQLite cache exactly when caching was disabled. | Correctness fix; no-regression on warm path | full suite green |
| T2 | LapTime subset-fallback parse (`helpers._process_lap_df`): run `pd.to_numeric` once; run `pd.to_timedelta` only on the non-numeric subset instead of the full column + `.where`. | LapTime block 12.97 → 9.79 ms (−25%) on 20k-row realistic input (numeric + 2% None) | exact dtype+values on the real payload shape (see note) |
| T4 | Explicit datetime formats on uniform-shape columns: `format="ISO8601"` for `LapStartDate` (`helpers`) and pandas RCM `Time` (`core`); explicit `"%Y-%m-%dT%H:%M:%S%.f"` for polars RCM `Time` (`core`). | LapStartDate 1.82 → 1.55 ms (−15%); pandas RCM 1.35 → 1.10 ms (−19%); polars RCM: **no measurable win** on the locked polars 1.44.1 (warm min-of-30 ≈ parity, 0.58 → 0.64 ms on 3k rows; the explicit format pays a one-time chrono-compile on first call) — kept as a determinism change, not a speed claim | exact on uniform real shapes |
| T10 | Move `_NULL_LIKE_STRINGS`/`_coerce_null_like_string_list` from `tif1.validation` to `tif1.exceptions`; `helpers` imports from `exceptions`, `validation` re-exports. | `tif1.validation` + pydantic fully out of the `import tif1.core` tree (`-X importtime`: `tif1.core_utils.helpers` self-cost ~78 → ~8 ms cumulative); `import tif1` ~47-50 ms | identity-same re-export objects; 157 targeted tests green |

## Rejected / excluded (measured, not shipped)

| ID | Hypothesis | Numbers | Reason |
|----|------------|---------|--------|
| T3 | Single-C-call `_numeric_seconds_to_timedelta` (drop NaT-Series + masked `.loc` assign) | candidate 6.2 vs control 6.5 ms (~−5%) standalone | Breaks the `test_process_lap_df_never_passes_nan_to_seconds_timedelta` contract (NaN must never reach `pd.to_timedelta(unit="s")`); NaN-safe variants measured slower; reverted |
| T5 | Exact-`isin` probe before strip/lowercase null-like normalization | no measurable delta | REJECT |
| T6 | Single-blob session frames tier (1 SELECT + 1 decompress vs 1452) | ~9.35x slower | REJECT |
| T7 | Batch JSON-tier cache reads for laptime waves (one `IN` SELECT vs ~24) | ~37% cold-SQLite-only, negligible warm-path benefit | EXCLUDED (marginal) |
| T8 | Merged-dict assembly for `fetch_all_laps_telemetry_async` cold path | already in tree (`_assemble_telemetry_batch` everywhere) | REJECT (already shipped) |
| T9 | Per-request fetch overhead (circuit-breaker locks, semaphore-per-task, retry setup) | ~0.007-0.008 ms/request | REJECT (negligible) |

## Correctness notes that constrained the kept set

- **T4 mixed-shape hazard (found by measurement, kept out of the PR):** pandas'
  inference path parses only the first-seen format in a mixed column and coerces
  the rest to NaT (`["...01.000000000", "...26(no frac)", "...00.000"]` → middle
  becomes NaT), while `format="ISO8601"` parses every row. So on hypothetical
  mixed-shape input the two paths *differ by design*. Live CDN probes plus all
  unit fixtures show real payloads are uniform, which is why the explicit format
  is safe — and strictly more correct — on these columns. Re-verification probes
  (9 sessions across 2021–2025) found zero exceptions: RCM `time` and `lSD` all
  `frac-len-9`.
- **T2 string-time entries:** the subset fallback preserves old behavior exactly
  on the real payload shape — numeric seconds plus `"None"` sentinels
  (live-verified: `session_laptimes.json` across 2021–2025 sessions carries no
  other string form). On hypothetical mixed float+`"H:MM:SS.fff"` columns the
  two paths *differ by design*: pandas mis-parses such strings inside a
  numeric-dominated mixed column (e.g. `"0:02:05.500000"` → 125.5 ms), and the
  subset parse fixes that (→ 2 m 5.5 s). Pure-string columns parse identically.
- **T3:** the all-NaN/empty/nullable-mask edge cases that silently infer
  `timedelta64[s]` under a single-call form are exactly what the existing masked
  implementation (and its guard test) protects; kept the masked form.
