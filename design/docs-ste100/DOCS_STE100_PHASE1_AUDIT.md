# Phase 1 Correctness Audit — tif1 Documentation

**Date:** 2026-09-02
**Scope:** Plan Section 5, passes 1–10 (DOCS_STE100_PLAN.md). Factual and configuration correctness only; the STE rewrite (Phases 2–5) has not started. All pages remain at their current voice.
**Source baseline:** `src/tif1` at version 0.6.0, CHANGELOG through `[Unreleased]`.

---

## Pass 1 — Version drift

- `docs/docs.json` said `version: "0.4.0"` while `pyproject.toml` says `0.6.0`; the `versions` array was missing `0.5.0`, `0.5.1`, and `0.6.0`.
- **Fixed:** `version` is now `0.6.0`; `versions` lists all released versions (`latest`, `0.6.0`, `0.5.1`, `0.5.0`, `0.4.0`, `0.3.1`, `0.3.0`, `0.2.0`, `0.1.0`).

## Pass 2 — Chart count

- `src/tif1/charts/` defines **22** `plot_*` functions (21 shipped at 0.4.0; `plot_race_launch_ratings` was added in 0.5.0). `docs/api-reference/charts.mdx` already documented all 22, but 13 stale "21" claims existed.
- **Fixed** in: `README.md`, `docs/introduction.mdx` (3), `docs/quickstart.mdx` (2), `docs/why-tif1.mdx` (4), `docs/api-reference/charts.mdx` (2). Verified by recount: `grep -rh "^def plot_" src/tif1/charts/ | wc -l` → 22, and no `\b21 (one-call|native|optional|built-in)` claim remains.

## Pass 3 — CDN description drift

- 0.6.0 introduced a three-tier chain (StaticDelivr primary → jsDelivr fallback → Hugging Face buckets backup) but only 5 pages described it; the rest described two sources or jsDelivr only.
- **Fixed** 19 pages that describe the source chain or its defaults: introduction, why-tif1 (2 places), architecture (2 diagrams + prose), troubleshooting, api-reference/overview, cli, async-fetch, exceptions, events (2), concepts/sessions, guides/error-handling, reference/faq, and `config.mdx` (the `cdns` default value in 3 places — the documented default was the pre-0.6.0 single-source list).
- Also corrected a wrong source attribution: schedule data comes from the `theOehrly/f1schedule` repository via jsDelivr (plus vendored JSON), not from the TracingInsights data repos (`docs/api-reference/events.mdx`, 2 places).
- Incidental jsDelivr URL examples (http-session, core-utils, concepts/cli) were left as examples, not chain descriptions.

## Pass 4 — Module coverage gaps (decision D2)

Five new API-reference pages, all signatures extracted from source introspection and all code examples executed headless before publication:

| Page | Module | Notes |
| :--- | :--- | :--- |
| `api-reference/payload-loader.mdx` | `payload_loader.py` | Central loading pipeline: `HttpTransport` protocol, `NiquestsTransport`, `InMemoryTransport`, `PayloadLoader.get()` (memo → SQLite → fetch → validate → write-back), `fetch_from_cdn(fast=)`, `get_url`, `get_url_loader()`. |
| `api-reference/session.mdx` | `session.py` | Documented honestly as the thin re-export shim (`Session`, `get_session`). |
| `api-reference/schedule-schema.mdx` | `schedule_schema.py` | `validate_schedule_payload` — manual `isinstance` validation raising `InvalidDataError` (not pydantic, contrary to earlier assumptions). |
| `api-reference/plotting-constants.mdx` | `plotting_constants.py` | `YEAR_CONSTANTS`, `DEFAULT_COMPOUND_COLORS`, `TEAM_COLORS`, `TEAM_CODES`; consumption and override paths. |
| `api-reference/assets.mdx` | `assets.py` | Bundled offline car/tyre/font assets and the matplotlib annotation helpers. |

All five are wired into `docs.json` navigation (Core API, Data Pipeline, Visualization & Tools groups).

Source facts found during this pass that contradict earlier assumptions: `fetch_from_cdn` and `get_url` are `PayloadLoader` methods, not module-level functions; `schedule_schema.py` does not use pydantic; `plotting_constants.py` has no `__all__`.

## Pass 5 — Overlapping pages (decision D3: keep separate, fix content)

`utils`/`utilities`/`core-utils` and `http`/`http-session` pages stay separate. Content correctness was enforced through the pass-6 signature sweep (no mismatches found in these pages) and the pass-7 example runs. No structural merges were made, per D3.

## Pass 6 — API signature sweep

- New tool `tools/sig_audit.py`: introspects the public API of every `tif1.*` module (tif1-defined members only; inherited pandas members excluded) and compares every documented `def` in `docs/api-reference/` code fences against source, with page-to-module disambiguation. 334 source symbols, 29 pages audited.
- **Result: no true signature drift.** Two flags were investigated and resolved as false positives: `core.mdx` documents `Driver.get_fastest_lap_tel()` (no params; correct — distinct from `Session.get_fastest_lap_tel(ultra_cold=)`), and `exceptions.mdx` defines an example-local `validate_drivers` helper (not the `tif1.validation` function).
- Coverage gaps fixed: `cdn.mdx` now documents `try_sources_async` and the 0.6.0 rule that Hugging Face sources never use minification; `cache.mdx` gained a "Module-Level Cache API" section (`LRUCache`, `get_backend_lap_cache`, `clear_lap_cache`, `Cache.get_entry`/`set_entry`, `SessionMemo`); `models.mdx` now documents `LazyTelemetryDict` and the `TelemetryProvider` protocol.
- Accepted exceptions (documented here, not in docs): `async_fetch.get_http_session`/`close_http_session` (private re-export aliases of `http_session.get_session`/`close_session`), `SessionMemo` internals, `validation.*.validate_consistent_lengths` classmethods, `models.Telemetry` differential-distance internals, `charts.get_chart_save_config`, `jupyter.JupyterDisplayMixin` — internal-facing surface, scheduled for review during the Phase 5 API prose rewrite.

## Pass 7 — Code example execution

- New tool `tools/run_doc_examples.py`: extracts every fenced Python block from tutorials, guides, and get-started pages, executes each page's blocks sequentially in an isolated subprocess (Agg backend, per-page temp `$HOME` so `config.set()` examples cannot pollute the environment, shared data cache, pinned default backend), classifies error-demonstration and network-dependent blocks, and reports per-block results.
- First sweep (36 pages, 299 blocks) found real documentation bugs, all fixed:
  - `guides/data-visualization.mdx` imported `plot_speed_comparison` and `plot_lap_times` from `tif1.plotting` — these functions do not exist; examples rewritten against the real `tif1.charts` API.
  - `guides/error-handling.mdx` read `CircuitBreaker.failure_count` — attribute does not exist (real attribute: `failures`).
  - `getting-started.mdx` used `session.session_type` — attribute does not exist.
  - Wrong column names (`Session`, `TeamColor`, `Team`, `Compound`, `SessionTime`, seaborn `Driver`/`LapTime(s)`) in qualifying-analysis, telemetry-comparison, weather-impact, race-analysis, laptimes-distribution.
  - `Timedelta` objects formatted with `:.3f` or plotted against float axes in 6 tutorials (fixed with `.total_seconds()`).
  - `get_lap()` called with float64 indices (fixed with `int()`).
  - `Axes.boxplot(labels=...)` — removed in current matplotlib (now `tick_labels`).
  - `to_sql` example wrote a frame with duplicate column names; a `Series.to_parquet` call; boolean masks with mismatched lengths.
  - Missing imports (`tif1`, `pandas as pd`) in first blocks of best-practices, data-visualization.
  - Optional dependencies (`openpyxl`, `sqlalchemy`, `pytables`, `plotly`, `scikit-learn`) required by some examples now carry explicit install notes.
- Final verification: **all 36 pages, 299 blocks, zero failures** (blocks the harness classifies as intentional error demonstrations excepted). (See "Reproduce" below.)
- Additional root cause found during verification: `examples.mdx` and `getting-started.mdx` each contained a polars-backend demo that rebound the page's `session`/`laps` variables, so every later pandas-style block operated on polars objects (`'DataFrame' object has no attribute 'groupby'`, boolean-mask length errors). Fixed by giving the polars demos their own variable names. `examples.mdx` also demonstrated `config.set("lib", "polars"); config.save()` mid-page, which persists the backend switch globally — the example now states the persistence effect and pins the default back.
- Environment note: one sweep was invalidated mid-run because a documented `config.set()` example persisted `~/.tif1rc` with `lib: "polars"`, flipping the default backend for later pages. The runner now isolates `$HOME` per page. This is also a docs finding: pages demonstrating `config.set()` should state that changes persist to `~/.tif1rc` — noted for the Phase 2+ rewrite.

## Pass 8 — Claims audit (decision D5: re-run benchmarks)

Benchmark suite re-run 2026-09-02 (`uv run pytest tests/benchmarks/ -m benchmark`): **119 passed, 1 failed, 1 xpassed**. The failure (`test_cache_performance.py::TestLockFreeReadPerformance::test_concurrent_reads_dont_block`, asserts >1.5x thread-parallelism for pure-Python cache reads) measures 1.0x in this sandbox — GIL-bound reads with no overlapping I/O; flagged to the owner as environment-sensitive, not a docs claim, and out of D4 scope (no code changes).

Claims actioned:

- **Benchmark-validated and kept/reworded:** parallel fetch is more than 3x faster than sequential (asserted and passed by `test_async_parallelism_validation`); cache-hit read path ~20x vs the legacy path (109 ms → 5.3 ms median); fastest-laps cold path ~2.4x (17 ms → 7 ms median). Pages now cite the suite and the run date: introduction (with a reproducible-results note), getting-started, best-practices, examples, faq (4 places), migration-from-fastf1, core.mdx (2).
- **Reworded to neutral:** fastf1-compat's "2-5x faster cold starts / 10-20x faster warm starts" (vs fastf1; unverifiable here) became factual statements about lazy fetching and cache tiers; "20-30% faster network requests" (HTTP/2) became "multiplexed, connection-efficient".
- **Kept, labeled as author-measured:** the introduction fastf1-relative table (4.8x/28x/20x). fastf1 is not a dependency and its API is rate-limited, so these numbers cannot be reproduced by the suite; each row is now marked "author-measured; not reproduced by the benchmark suite" and the table is accompanied by the suite-validated numbers.
- **Kept as cited:** the jolpica-f1 500 requests/hour rate-limit claim (why-tif1 carries verified sources). "2018–current" data coverage matches the vendored schedule years (2018–2026) and `core_utils` supported range.
- **Not actioned (external library comparisons, qualified in place):** orjson "2-5x faster than stdlib" parsing, polars "2-5x/3-5x" backend claims — these describe third-party libraries rather than tif1 measurements; recorded here for the Phase 2+ STE rewrite to revisit.

## Pass 9 — Links, media, build

- `docs/style-ste100.md` used an angle-bracket autolink (`<https://…>`), which MDX cannot parse — it broke `mintlify validate` and `broken-links` for the whole site. **Fixed** to a plain URL.
- Systemic fence malformation found and repaired in three layers:
  1. **2,781 closing fences carried a language tag** (e.g. ` ```python ` used to close a block, sometimes with trailing prose/JSX swallowed onto the fence line) across 77 files. A one-off repair converted them to bare fences and restored swallowed trailing content to its own line.
  2. **78 opening fences had no language tag** (19 files). Mintlify's MDX pipeline needs fenced code to carry an info string for reliable code-block parsing; the untagged opens were tagged (`python`/`text`) by content inspection.
  3. **`api-reference/types.mdx` had a pre-existing stray fence line** that split one example into two half-blocks and inverted the open/close pairing of every fence after it in the file. The stray divider was removed and the tail fences rebuilt to content truth (tagged opens, bare closes).
- A dead internal link was removed: `guides/best-practices.mdx` linked to the nonexistent `/guides/production-deployment` page.
- All referenced images (`/assets/*.png`, logo, favicon) verified present in the repo.
- `npx mintlify broken-links` → **no broken links**; `npx mintlify validate` → **build validation passed** (both re-run after all Phase 1 edits, including the fence repairs).

## Pass 10 — CHANGELOG cross-check

- 0.6.0 (Hugging Face fallback): now documented on every chain-describing page (pass 3); the "minification never applies to Hugging Face sources" rule is now in `cdn.mdx` (was missing).
- 0.5.1 (data-dictionary/schema reference removal): verified clean — no stale references remain.
- 0.5.0: plot styles, `get_plot_style`, team lookups, `tif1.assets`, `plot_race_launch_ratings` all documented (assets via the new page). Finding: the "new `extract_telemetry_sectors` utility" entry refers to a repository script (`scripts/extract_telemetry_sectors.py`, deleted in 0.5.1, never part of the published package), not a library API — no docs change needed; recorded so future readers do not hunt for a public function.
- `[Unreleased]`: empty.

## Tooling and regression gates (added this phase)

- `tools/sig_audit.py` — signature/coverage audit (above).
- `tools/run_doc_examples.py` — example execution harness (above).
- `tools/ste_check.py` gained `--baseline`: CI now fails only when violations grow beyond the recorded baseline. Wired as a step in the "Validate Documentation" job (`.github/workflows/docs-preview.yml`).
- `tools/ste_baseline.json` regenerated for the current tree (85 files; the five new pages and repaired fences changed file coverage).

## Reproduce

```bash
uv run pytest tests/benchmarks/ -m benchmark --no-cov --override-ini="addopts="
uv run python tools/sig_audit.py
uv run python tools/run_doc_examples.py --timeout 420
uv run python tools/ste_check.py docs/ --baseline tools/ste_baseline.json
cd docs && npx -y mintlify@latest validate && npx -y mintlify@latest broken-links
```

## Open items for later phases

- The owner should request the official ASD-STE100 Issue 9 copy (pending since Phase 0) to pin exact rule IDs.
- The concurrent-reads benchmark failure needs an owner decision (test expectation vs. environment).
- `config.set()` persistence behavior deserves a callout on the config page during the Phase 5 rewrite.
- Accepted coverage exceptions from pass 6 to revisit in Phase 5.
