# Changelog

All notable changes to this project are documented in this file.

The project uses semantic versioning. Release dates are listed in `YYYY-MM-DD` format.

## [Unreleased]

## [0.7.0] - 2026-09-04

### Summary

`0.7.0` is a performance release. Fetch-path validation no longer uses pydantic
(validation-on full-session fetch 1971 → 1024 ms, 1.9x), the SQLite cache compresses
large blobs and keeps parsed objects for warm hits, and the default concurrency and
cache-commit settings are retuned from live CDN measurements. The docs site is converted
to Simplified Technical English, with five new API reference pages. No public APIs are
removed; `Cache.set_raw()` is added. Default-config users (validation off) still see the
cache and concurrency gains.

### Added

- **`Cache.set_raw()`** — persist already-serialized JSON blobs without re-encoding.
  The async fetch pipeline uses this when validation left the payload byte-identical
  to the HTTP body. Blobs ≥4 KB are zlib-compressed at the SQLite boundary.

### Changed

- **Pydantic removed from the fetch-pipeline validation path** (`validation.py`): the four
  `validate_*_data` functions now use a pydantic-free fast path (alias→field mapping, default
  filling, length checks) with identical output on well-typed payloads. Validation-enabled
  full-session fetch: 1971 → 1024 ms (1.9x). The pydantic entry points (`validate_laps`,
  `validate_telemetry`, ...) remain for API compatibility. Note: no per-element type coercion
  or driver-code pattern check on the fetch path.
- **Null-like normalization deferred to DataFrame construction** for laps/telemetry/rcm on the
  fetch path (`normalize=False`), relying on the existing vectorized
  `helpers._replace_null_like_strings`; weather keeps inline coercion. Per-payload validation
  cost drops to ~0.006-0.008 ms.
- **SQLite cache tier compresses large blobs** (zlib-3, ≥4 KB; telemetry payloads compress ~12x).
  Default-config cold write-cache full-session load: 3750 → ~2100 ms (-44%). Legacy TEXT rows
  still read; corrupt rows degrade to a cache miss. Caches written by 0.7.0 are not readable
  by 0.6.x — clear the cache if you downgrade.
- **Parsed-object read-through tiers** in `Cache` (128 json / 256 telemetry entries): repeat warm
  hits skip orjson parsing (190 µs → 0.4 µs); writes drop parsed entries so stale objects cannot
  survive.
- Default `max_concurrent_requests` 20 → 22 (live CDN, 2025 Abu Dhabi Practice 1, 96 telemetry
  payloads: 22 beat 20 in 5/5 interleaved pairs, median -26%; cap 64 was slower than 20).
- Default `cache_commit_interval` 25 → 100 (~20% of the compressed-write path; `close()` still
  force-commits).
- Runtime dependency lower bounds raised: niquests 3.21.1, pydantic 2.13.5, typer 0.27.2,
  typing-extensions 4.16.0, matplotlib 3.11.1, scipy 1.17.1, orjson 3.12.0, polars 1.44.1.

### Performance

- Full experiment log with baselines, methodology, and per-hypothesis verdicts:
  `.agents/perf/RESULTS.md`; reproducible offline harness: `tools/perf_validation_experiment.py`.
- Accepted: pydantic removal (H1), deferred normalization (H2), raw+compressed cache writes (H5),
  parsed-object tiers (H4), commit interval (H6), concurrency cap (H7: 22 after live CDN).
- Rejected by measurement: event-loop parse offload (H3, GIL-bound), lazy pandas import (H10),
  polars-first assembly for the pandas backend (H9 — the merged-dict batch path is already at its
  dependency-free floor; faster routes require pyarrow or change dtype inference semantics).
- Already implemented previously: single-shot laps/telemetry frame assembly (H8).

### Documentation

- Converted the docs site (guides, concepts, tutorials, API reference) to Simplified Technical
  English (ASD-STE100).
- Added API reference pages for `session`, `payload-loader`, `schedule-schema`,
  `plotting-constants`, and `assets`.
- Corrected factual drift from 0.6.0 (three-source CDN chain, config defaults, Hugging Face
  minification rule).

### Known Issues

- None reported at release time.

## [0.6.0] - 2026-08-26

### Summary

`0.6.0` adds Hugging Face storage buckets as a third, last-resort CDN source. The fetch chain is now StaticDelivr (primary) → jsDelivr (fallback) → Hugging Face buckets (backup), keeping data available when the GitHub-backed CDNs are down. No public API changes.

### Added

- **Hugging Face buckets as backup CDN source** — `cdn.py` now defaults to three sources in priority order:
  - StaticDelivr: `https://cdn.staticdelivr.com/gh/TracingInsights` (`{year}/main/{gp}/{session}/{path}`)
  - jsDelivr: `https://cdn.jsdelivr.net/gh/TracingInsights` (`{year}@main/{gp}/{session}/{path}`)
  - Hugging Face: `https://huggingface.co/buckets/tracinginsights` (`{year}/resolve/{gp}/{session}/{path}`)
- The Hugging Face buckets mirror the TracingInsights GitHub data repos (`TracingInsights/{year}`, seasons 2018–2026) with an identical directory layout, served through the `/resolve/` endpoint with no branch segment.
- `CDNManager._name_for_url()` identifies Hugging Face URLs as `"HuggingFace"`; both sync (`try_sources`) and async (`try_sources_async`) fallback loops transparently include it.

### Changed

- Default `cdns` config value now lists all three sources; existing custom `cdns` configurations are unaffected.
- Minification (`cdn_use_minification`) is never applied to Hugging Face sources — they do not serve `.min.json`, and a 404 would otherwise abort the fallback chain as a fatal `DataNotFoundError`.
- Documentation updated across `README.md`, `AGENTS.md`, and the docs site (`cdn.mdx`, `http.mdx`, `data-flow.mdx`, `utilities.mdx`, `environment-variables.mdx`) to describe the three-source chain.

### Fixed

- Repaired duplicate node IDs and conflicting style declarations in the data-flow architecture mermaid diagram (`docs/concepts/data-flow.mdx`).

### Known Issues

- None reported at release time.

## [0.5.1] - 2026-08-12

### Summary

`0.5.1` is a patch release focused on documentation cleanup, test reliability, and removing unused repository scripts. No public API changes.

### Changed

- Streamlined documentation by removing references to the machine-readable `data_dictionary.json` and `telemetry.schema.json` from `README.md`, `DATA_REFERENCE.md`, and `docs/reference/data-reference.mdx`.
- Minor docs/branding consistency updates (`docs/docs.json`, guides, and related pages).
- Added `from __future__ import annotations` in `cache.py` for clearer type hints.

### Fixed

- Hardened `test_models_module_does_not_import_core` so it dynamically reloads `tif1.models` without polluting `sys.modules` for later tests (prevents import-cycle false positives/side effects).
- Updated cold-path fastest-lap tests to use current event/session naming (`Pre-Season Testing` / `Practice 1`).

### Removed

- Deleted unused repository scripts and baselines (`scripts/baseline_benchmark.py`, `scripts/download_assets.sh`, `scripts/extract_telemetry_sectors.py`, and related baseline results). These were not part of the published package.

### Known Issues

- None reported at release time.

## [0.5.0] - 2026-08-11

### Summary

`0.5.0` ships bundled chart assets (car/tyre images, brand fonts), two new curated plot styles (`default-light`, `default-dark`), and a new `plot_race_launch_ratings` native chart. It also includes the `tif1.assets` module for offline asset loading and rendering, plus pixel-perfect matching with the reference v2 analysis scripts.

### Added

- **New plot styles: `default-light` and `default-dark`** — `tif1.plotting` now ships two curated matplotlib themes that work with every chart's `color_scheme` parameter and with `setup_mpl(color_scheme=...)`:
  - `default-light` is extracted from the v2 `Fastest_Lap.py` analysis script (lightblue background, black text, Tenada heading font, hidden top/right spines, 32pt ticks, 20×20 @ 300 dpi, bar height 0.1, tyre/car image offsets).
  - `default-dark` is extracted from the v2 `Race_Launch_Performance_Ratings.py` script (TracingInsights dark brand `#011627` background, lime text, black grid, white bar/ytick labels, Coolvetica/Azonix/GreatVibes fonts, subplot margins, `car_threshold: 2.5`).
  - New `get_plot_style(name)` registry returns deep copies of the style configs, and `get_team_code()` / `team_code_mapping()` / `team_color_mapping()` expose session/year-aware team lookups.
- **Bundled chart assets** — new `tif1.assets` module ships car images for every team 2018–2026 (91 PNGs), 7 tyre compound images, and 4 brand fonts directly inside the package (no network access at plot time). Includes cached `load_car_image` / `load_tyre_image` loaders, path/discovery helpers, and matplotlib `AnnotationBbox` helpers (`add_car_images`, `add_tyre_images`, `add_car_at_position`, `add_tyre_at_position`) that mirror the v2 scripts' rendering.
- **New native chart: `plot_race_launch_ratings`** — added to `tif1.charts` (and exported at the top level). It loads each driver's first race lap, interpolates 50/100/150/200 km/h crossing times from telemetry, derives 0–10 launch ratings, and renders a barh chart in the `default-dark` style with bundled car/tyre images and the 2.5 rating threshold.

### Changed

- **Performance: telemetry sector extraction** — new `extract_telemetry_sectors` utility splits telemetry into per-sector snapshots for downstream analysis.
- **Documentation restructuring** — reorganized docs navigation, removed deprecated `io-pipeline` and `lap-operations` reference pages, added `data-reference` and `why-tif1` pages.
- **Cache directory handling** — improved cache path resolution and configuration.

### Fixed

- **`plot_race_launch_ratings` output now matches the v2 script pixel-for-pixel** (verified against `F1-analysis/v2/Race_Launch_Performance_Ratings.py` on 2024 Bahrain: identical 6000×6000 renders, mean-abs pixel diff 0.000):
  - The chart now exports the full canvas exactly like the v2 script (no `tight_layout`, no `bbox_inches` cropping; the manual `subplots_adjust` margins are preserved). `charts._common.finalize_figure` gained optional `tight_layout`/`bbox_inches` kwargs (defaults unchanged for all other charts).
  - Crossing times are rounded to 3 decimals **before** the 0-10 rating normalization (matching the script's `.round(3)` on each `Time_XX` column), removing 0.01 rating discrepancies.
  - The driver sort replicates the script's exact pipeline (`groupby("Driver").first()` + pandas' default unstable `sort_values`), preserving its tie-breaking for identical ratings.

## [0.4.0] - 2026-08-08

### Summary

`0.4.0` is a major release that introduces the native `tif1.charts` module (21 one-call chart functions), migrates the library to **pandas 3.0**, and raises the minimum supported Python version to **3.11**. The pandas 3.0 migration fixes a critical timedelta unit-resolution bug and removes deprecated `pd.concat(copy=False)` calls ahead of pandas 4.

### Breaking Changes

- **Python 3.10 support dropped** — minimum supported version is now **3.11** (required by pandas 3.0).
- **pandas 2.x support dropped** — `tif1` now requires **pandas >=3.0.5,<4**.

### Migration Notes

- Upgrade to Python 3.11+ and pandas 3.0+ before installing `tif1` 0.4.0.
- No public API changes; the pandas 3.0 `str`-dtype default is transparent to users (telemetry `Driver` is kept as `object` for FastF1 compatibility).

### Added

- **Native chart functions** — new `tif1.charts` module with 21 one-call chart functions, also re-exported at the top level (`tif1.plot_<name>`):
  - Track maps: `plot_track_speed_map`, `plot_track_throttle_map`, `plot_track_brake_zones`, `plot_track_acceleration_map`, `plot_gear_shifts`, `plot_multi_driver_speed_comparison`
  - Telemetry: `plot_speed_traces`, `plot_annotated_speed_trace`, `plot_telemetry_comparison`, `plot_gg_diagram`
  - Lap times: `plot_driver_laptimes`, `plot_laptimes_distribution`, `plot_laptime_heatmap`, `plot_qualifying_grid`, `plot_lap_delta`, `plot_position_changes`, `plot_track_temperature`
  - Performance: `plot_downforce_levels`, `plot_throttle_distance`, `plot_tire_degradation`
  - Top speeds: `plot_top_speeds`
- Every chart takes `(year, event, session)`, loads its own data, and returns a `(fig, ax)` pair (`plot_telemetry_comparison` returns the 4 panel axes as a numpy array). Optional `save_path`, `dpi`, `figsize`, `facecolor`, `color_scheme`, `enable_cache`, and `lib` parameters are shared across all charts, plus the shared filters `drivers`, `teams`, `n_drivers`, `laps`, `laptime_cutoff`, `include_deleted`, and `include_pit_laps`.
- Added `scipy>=1.14,<2` dependency (used by `plot_gg_diagram`'s convex-hull performance envelope).

### Changed

- Rewrote all 21 `docs/assets/generate_*.py` scripts as thin wrappers around the native chart functions; `generate_all_charts.py` now calls `tif1.charts` as the canonical "regenerate all docs charts" entrypoint (standalone-owned outputs like `race_position_changes.png` are no longer overwritten).
- Updated 21 tutorials to teach the native chart API and added the `api-reference/charts` reference page.
- **Performance: telemetry batch assembly via merged dict-of-lists** — `get_fastest_laps_tels` / `get_fastest_laps_tels_async` now assemble telemetry from raw payloads into a single DataFrame (one dtype-conversion pass) instead of building a per-driver DataFrame and calling `pd.concat`. Measured **~2.5-3x faster** on pandas 3.0 (and ~2x faster on pandas 2.3) with byte-identical output, mitigating the pandas 3.0 regression in repeated per-frame `pd.to_timedelta`/`astype` machinery. The polars backend keeps the per-driver `_create_telemetry_df` + `pl.concat` path unchanged.
- **Performance: `Laps.telemetry` / `get_car_data` via merged dict-of-lists** — these accessors now collect raw telemetry payloads through a new `Session._get_telemetry_payload_for_ref` (payload-only version of the per-lap source chain) and build a single merged DataFrame instead of building a per-lap DataFrame and calling `pd.concat`. Measured **~2x faster** with identical output. Falls back to the legacy per-lap path for stand-in session objects and on malformed payloads; the polars backend is unaffected.

### Fixed

- Fixed crash on 2026 lap data where missing values are encoded as the string `"None"` (e.g. in the `Deleted` column): `_apply_laps_dtypes` now normalizes null-like string sentinels before dtype coercion, so `astype("boolean")` no longer raises and bool columns no longer turn missing values into `True`.
- Applied the same null-like string normalization to the polars laps path: `_create_lap_df` normalizes payload lists before DataFrame construction (so polars infers proper `Boolean`/`Float` dtypes instead of stringifying mixed columns), and `_process_lap_df` normalizes String columns as a defensive boundary.
- Non-strict `validate_lap_data` now returns the normalized payload (null-like strings converted to `None`) even when validation fails, instead of returning raw un-normalized data.

### Dependencies

Updated all dependencies to their latest stable versions, one at a time, with breaking changes resolved:

- **`pandas` 2.3.3 → 3.0.5** (major) and **`pandas-stubs` 2.3.3.260113 → 3.0.5.260730**: migrated the codebase for pandas 3.0 breaking changes:
  - Timedelta unit inference (`timedelta64[us]` is now produced for sub-second inputs): all `is_timedelta64_ns_dtype` guards were widened to `is_timedelta64_dtype` and `_numeric_seconds_to_timedelta` no longer reinterprets already-timedelta columns, preventing a 1e6× lap-time scaling bug in `pick_fastest`/`slice_by_time`.
  - `str` dtype is now the default for string columns: telemetry `Driver` is explicitly kept as `object` dtype for FastF1 compatibility, matching the laps dtype contract.
- **Python minimum raised to 3.11** (required by pandas 3.0): updated `requires-python`, classifiers, CI matrix, ty config, and documentation.
- **Fastest-lap selection optimized**: `_select_fastest_laps` now uses `sort_values + drop_duplicates` instead of `groupby(...).idxmin()` — measurably faster on both pandas 2.x and 3.x (pandas 3.0 made `groupby.idxmin` ~2× slower), recovering most of the pandas-3.0 regression in `get_fastest_laps(by_driver=True)`.
- **Lap-table assembly optimized**: the pandas laps path now builds one DataFrame from a single merged dict-of-lists (`_merge_lap_payloads`) instead of concatenating per-driver frames with `pd.concat`, which regressed ~2× in pandas 3.0. Measured ~20× faster DataFrame assembly with identical output (verified against the concat path, including drivers with differing column sets).
- `typer` 0.26.7 → 0.27.1 (metavar help-printing change only)
- `niquests` 3.19.1 → 3.21.0
- `matplotlib` 3.10.9 → 3.11.1
- `polars` 1.41.2 → 1.43.2
- `typing-extensions` 4.15.0 → 4.16.0
- `pyarrow` 24.0.0 → 25.0.0
- `hypothesis` 6.155.3 → 6.165.2
- `prek` 0.4.5 → 0.4.12
- `ty` 0.0.49 → 0.0.69
- `ruff` 0.15.17 → 0.16.2 (new `PLR0917` rule added to the ignore list alongside the existing `PLR09xx` rules; fixed a `B033` duplicate set item in tests)
- Transitive upgrades via lockfile: `numpy` 2.3.5 → 2.5.1, plus `certifi`, `charset-normalizer`, `idna`, `click`, `requests`, `pygments`, `pillow`, and others.

## [0.3.1] - 2026-05-15

### Summary

`0.3.1` is a patch release containing CDN URL formatting fixes, CI workflow improvements, pre-commit hook infrastructure, documentation corrections, and dependency updates.

### Added

- Added release discussion workflow (`release-discussion.yml`) for automated release announcements.
- Added pre-commit hook configuration with git hooks for automated linting before commits.
- Added Mintlify documentation validation to pre-commit hooks.
- Added explicit read permissions to GitHub Actions workflow for improved security posture.
- Added benchmark test support for real cold-start CDN scenarios.

### Changed

- Updated CDN URL formatting in `cdn.py` to use `/branch/` path format for StaticDelivr compatibility.
- Improved pre-commit hook cross-platform compatibility and robustness.
- Updated `AGENTS.md` with corrected documentation references.
- Expanded benchmark test file with cold-start CDN test scenario.

### Fixed

- Fixed CDN references across documentation and code to correctly reflect StaticDelivr as primary CDN.
- Fixed documentation links and HTML entity encoding in code examples.
- Fixed trailing whitespace in environment variables documentation.
- Fixed Template Injection security issue in GitHub Actions workflow.

### Dependencies

Updated dependencies to latest stable versions:

- Updated `niquests` from `>=3.18.7,<4` to `>=3.18.8,<4`
- Updated `hypothesis` from `>=6.152.4,<7` to `>=6.152.7,<7`
- Updated `ty` from `>=0.0.34,<0.1.0` to `>=0.0.35,<0.1.0`

### Known Issues

- None reported at release time.

## [0.3.0] - 2026-05-08

### Summary

`0.3.0` introduces StaticDelivr as the primary CDN provider with improved fallback handling, enhanced API reference documentation, and updated F1 2026 schedule data. This release focuses on reliability improvements and better documentation coverage.

### Added

#### CDN Infrastructure
- **StaticDelivr as primary CDN** with jsDelivr as fallback for improved reliability and performance.
- Added configurable CDN sources via `cdns` configuration option.
- Added optional JSON minification support via `cdn_use_minification` configuration flag (20-40% file size reduction).
- Added intelligent CDN fallback system with automatic source switching on failures.
- Added per-source failure tracking with automatic disabling after 3 consecutive failures.
- Added `CDNSource` dataclass for structured CDN configuration.
- Added `CDNManager.try_sources()` method for robust multi-CDN fetching with fallback.

#### Documentation
- Expanded API reference documentation with comprehensive coverage of all modules.
- Added detailed documentation for CDN system, configuration options, and utilities.
- Updated tooling guidelines in `AGENTS.md` for improved developer experience.

#### Data
- Updated F1 2026 schedule with current calendar reflecting cancelled races.

### Changed

#### CDN System
- Migrated from jsDelivr-only to multi-CDN architecture with StaticDelivr as primary source.
- Enhanced URL formatting to support both jsDelivr and StaticDelivr URL patterns.
- Improved CDN source validation to reject invalid or unsupported URLs (e.g., raw.githubusercontent.com).
- Enhanced logging for CDN operations with detailed source tracking and failure reporting.

#### Code Quality
- Fixed pre-commit checks for CI pipeline.
- Improved code formatting and linting compliance.

### Fixed

- Fixed CDN fallback handling to properly propagate `DataNotFoundError` (404) without retrying other sources.
- Fixed CDN source naming to avoid duplicates when multiple sources are configured.
- Fixed JSON file path handling for minification support.

### Performance

- **Improved data fetching reliability** through multi-CDN architecture with automatic fallback.
- Optional JSON minification can reduce bandwidth usage by 20-40% for JSON payloads.
- Reduced latency through StaticDelivr's optimized edge network.

### Dependencies

Updated core dependencies to latest stable versions:

- Updated `pandas` from `>=2.2.0` to `>=2.3.3,<3` for improved performance and bug fixes
- Updated `niquests` from `>=3.10.2` to `>=3.18.7,<4` for enhanced HTTP/2 support
- Updated `pydantic` from `>=2.10.6` to `>=2.13.4,<3` for better validation performance
- Updated `typer` from `>=0.15.1` to `>=0.25.1,<0.26.0` for CLI improvements
- Updated `rich` from `>=13.9.4` to `>=15.0.0,<16` for enhanced terminal output
- Updated `matplotlib` from `>=3.9.4` to `>=3.10.9,<4` for plotting improvements
- Updated `seaborn` from `>=0.13.2` to `>=0.13.2,<1` (maintained)
- Updated `polars` from `>=1.36.0` to `>=1.40.1,<2` for better DataFrame performance
- Updated `orjson` from `>=3.10.14` to `>=3.11.9,<4` for faster JSON parsing
- Updated `rapidfuzz` from `>=3.10.2` to `>=3.14.5,<4` for improved fuzzy matching

Development dependencies:
- Updated `pytest` from `>=8.3.4` to `>=9.0.3,<10` for better test framework features
- Updated `pytest-xdist` from `>=3.6.1` to `>=3.8.0,<4` for parallel test execution
- Updated `pytest-cov` from `>=6.0.0` to `>=7.1.0,<8` for coverage reporting
- Updated `pytest-asyncio` from `>=0.25.2` to `>=1.3.0,<2` for async test support
- Updated `pytest-benchmark` from `>=5.1.0` to `>=5.2.3,<6` for performance benchmarking
- Updated `pytest-mock` from `>=3.14.0` to `>=3.15.1,<4` for mocking utilities
- Updated `pyarrow` from `>=18.1.0` to `>=24.0.0,<25` for Arrow format support
- Updated `ruff` from `>=0.8.6` to `>=0.15.12,<0.16.0` for linting and formatting
- Updated `ty` from `>=0.0.33` to `>=0.0.34,<0.1.0` for type checking
- Updated `prek` from `>=0.3.12` to `>=0.3.13,<0.4.0` for pre-commit hooks
- Updated `pandas-stubs` from `>=2.2.3.241009` to `>=2.3.3.260113,<3` for type stubs
- Updated `hypothesis` from `>=6.122.3` to `>=6.152.4,<7` for property-based testing

### Configuration

New configuration options in `.tif1rc` or environment variables:

```python
# CDN configuration
cdns = [
    "https://cdn.staticdelivr.com/gh/TracingInsights",
    "https://cdn.jsdelivr.net/gh/TracingInsights"
]

# Enable JSON minification (optional, reduces file sizes)
cdn_use_minification = false
```

### Migration Notes

- No breaking changes to public APIs.
- Existing code will automatically use the new multi-CDN system.
- Users experiencing CDN issues can configure custom CDN sources via the `cdns` configuration option.
- The CDN system now automatically falls back to jsDelivr if StaticDelivr is unavailable.

### Compatibility Notes

- All public APIs remain backward compatible with v0.2.0.
- CDN configuration is optional; defaults provide optimal reliability.
- Minification is disabled by default to maintain compatibility with existing workflows.

### Known Issues

- None reported at release time.

## [0.2.0] - 2026-04-17

### Summary

`0.2.0` is a major feature release that introduces significant improvements to data fetching, plotting capabilities, session handling, and overall performance. This release includes enhanced FastF1 compatibility, optimized data workflows, comprehensive plotting APIs, and improved qualifying session support.

### Added

#### Plotting And Visualization
- **Full FastF1-compatible plotting API** with season-aware team and compound colors (2018–2026).
- Added `get_team_color()`, `get_driver_style()`, and `add_sorted_driver_legend()` helper functions for consistent chart styling.
- Integrated `timple` library for professional F1-style timedelta axis formatting.
- Added fuzzy matching for driver and team lookups with warnings for near-matches.
- Added comprehensive plotting constants including 2026 team colors (Audi, Cadillac).
- Added `plotting_colors_demo.py` example demonstrating the new plotting capabilities.

#### Data Fetching And Performance
- **Session-level laptime fetching** using `session_laptimes.json` instead of per-driver `driver/laptimes.json` files, significantly reducing network requests.
- Added synthetic payload generation to derive driver-specific laptime data from session-wide payloads.
- Added `prefer_session_payload` option for controlling laptime data source preference.
- Added baseline benchmarking script (`baseline_benchmark.py`) with detailed timing and profiling for async and sync workflows.
- Added benchmark results tracking in `baseline_results.json`.

#### Session Handling
- Added qualifying session handling with proper `QualifyingSession` type support.
- Enhanced `split_qualifying_sessions()` method to correctly handle qualifying session markers.
- Added validation for qualifying session types in async fetch and validation modules.

#### Testing And Quality
- Added `conftest.py` with shared pytest fixtures for global state reset between tests.
- Added fixtures to prevent cross-test contamination by resetting async session, CDN manager, and circuit breaker state.
- Added comprehensive unit tests for qualifying session handling, session laptime fetching, and plotting APIs.
- Added property-based tests using Hypothesis for improved test coverage.

#### Documentation
- Added comprehensive tutorial documentation with charts and examples for telemetry visualization.
- Added documentation deployment workflows and CI configuration.
- Added `TOOLS.md` with CLI tools documentation.
- Deployed versioned documentation for v0.1.0 and v0.2.0.

### Changed

#### Core Improvements
- **Migrated from `nest-asyncio` to `nest-asyncio2`** for improved async handling in Jupyter environments.
- Enhanced ultra-cold mode handling to avoid unnecessary refetches when data is already cached.
- Improved error handling in `_validate_json_payload()` to skip validation for patched CDN payloads.
- Added `_get_callable_code()` and `_is_patched_callable()` functions for better callable validation.
- Introduced `_build_session_laptime_payload_from_driver_payloads()` to combine driver payloads into session-wide payloads.
- Updated session data fetching logic to handle `prefer_session_payload` option while maintaining backward compatibility.

#### Data And Schedules
- Updated 2026 F1 schedule to remove cancelled races and reflect current calendar.
- Fixed telemetry functions including `DriverAhead`, `DistanceToDriverAhead`, and related calculations.

#### Code Quality
- Applied code formatting and type hinting improvements across the codebase for enhanced maintainability.
- Improved linting compliance with stricter Ruff rules.
- Removed hardcoded user agent string from `config.py` and HTTP session management.

#### Documentation
- Updated all documentation examples and chart generation scripts to use the new session-backed color API.
- Fixed markdown formatting and broken documentation links across all documentation files.
- Streamlined documentation navigation by removing advanced guides section.
- Updated API reference documentation with improved formatting and terminology.

### Fixed

- Fixed ultra-cold mode to avoid unnecessary refetches when session data is already available.
- Fixed qualifying session splitting logic to correctly identify and handle qualifying markers.
- Fixed telemetry calculations for driver-ahead and distance-to-driver-ahead metrics.
- Fixed validation logic to properly handle synthetic and patched payloads.
- Removed unnecessary path checks for `session_laptimes.json` in telemetry cache lookup tests.

### Build And Packaging

- Standardized the published PyPI package name as `tif1`.
- Added explicit Hatch wheel package configuration for `src/tif1`, improving build clarity and package discovery.
- Updated installation guidance so the canonical command is `pip install tif1`.
- Updated repository metadata, project links, and release-facing configuration to reference `tif1` consistently.
- Refreshed lockfile and package metadata to match current distribution naming.

### Dependencies

- Added `timple==0.1.8` for timedelta plotting support.
- Updated to `nest-asyncio2>=1.7.2,<2` (migrated from `nest-asyncio`).
- Updated `pytest` to `>=9.0.2,<10`.
- Updated `pillow` in the uv group.
- Updated `actions/github-script` from 7 to 8 in CI workflows.
- Updated `actions/checkout` from 4 to 6 in CI workflows.
- Updated `actions/upload-artifact` from 4 to 7 in CI workflows.
- Updated `actions/download-artifact` from 4 to 8 in CI workflows.
- Updated `astral-sh/setup-uv` from 5 to 7 in CI workflows.
- Bumped 16 packages in the python-dependencies group via Dependabot.

### Performance

- **Significant performance improvement** through session-level laptime fetching, reducing the number of HTTP requests from N (per driver) to 1 (per session).
- Optimized async and sync workflows with measurable improvements documented in benchmark results.
- Improved cache efficiency by storing session-wide data and deriving driver-specific views on demand.

### Migration Notes

- The supported package name for this release is `tif1`.
- The supported import path remains `import tif1`.
- The supported CLI command remains `tif1`.
- Some intermediate repository history referenced `tifone` during post-`0.1.0` packaging work. For `0.2.0` and later, treat `tif1` as the canonical package name.
- Code using the old `nest-asyncio` package will automatically use `nest-asyncio2` after upgrading.
- Existing plotting code will continue to work, but new season-aware color APIs are recommended for better accuracy.

### Compatibility Notes

- **Breaking Change**: The internal data fetching mechanism now prefers `session_laptimes.json` over per-driver laptime files. This is transparent to most users but may affect custom cache implementations.
- The `prefer_session_payload` option allows reverting to legacy behavior if needed.
- All public APIs remain backward compatible with v0.1.0.
- Qualifying session handling is now more robust and correctly identifies qualifying markers.

### Known Issues

- None reported at release time.

## [0.1.0] - 2026-02-13

### Summary

`0.1.0` is the initial public release of `tif1`, a high-performance Formula 1 timing-data library built around fast access, structured domain models, and a focused API for session, lap, driver, and telemetry analysis.

### Added

- Initial PyPI release of `tif1` for Python 3.10 and newer.
- Core Formula 1 data access APIs centered around sessions, drivers, laps, and telemetry.
- Public convenience entry points including `get_events`, `get_sessions`, `get_session`, `get_event`, `get_event_by_name`, `get_event_by_round`, and `get_event_schedule`.
- Lazy top-level exports in `tif1.__init__`, exposing the primary public API without forcing all heavy modules to import eagerly.
- Structured domain objects and data access patterns for working with event schedules, session data, driver views, lap views, and telemetry datasets.

### Data Access And Performance

- Direct access to TracingInsights timing data covering the 2018-current seasons.
- CDN-backed fetching via jsDelivr for remote session data retrieval.
- Asynchronous and parallel fetching designed to reduce cold-load latency for session and telemetry access.
- SQLite-backed local caching to avoid repeated downloads and accelerate repeated analysis workflows.
- HTTP networking based on `niquests`, aligned with the project's performance-focused design goals.
- Fast-path workflows for retrieving fastest laps and telemetry for individual drivers or full sessions.
- On-demand loading behavior so users can work with the parts of a session they need instead of always loading everything up front.

### Analysis Features

- Access to lap times, sector times, tire compounds, stint-related data, and telemetry channels such as speed, throttle, brake, RPM, gear, and DRS.
- Access to position, acceleration, distance, and related derived session datasets used in race and lap analysis workflows.
- Support for standard Formula 1 session types including practice sessions, qualifying, sprint formats, and races.
- Optional backend flexibility with pandas as the primary backend and polars support for users who prefer an alternate DataFrame engine.

### Reliability And Developer Experience

- Built-in retry and circuit-breaker infrastructure for more resilient network access.
- Custom exception hierarchy rooted at `TIF1Error`, including dedicated errors for missing data, invalid data, cache failures, network problems, and unloaded-session access.
- Configuration support for runtime behavior through the project's config layer and user configuration file support.
- Optional validation capabilities powered by Pydantic.
- JSON parsing based on `orjson` for faster serialization and parsing paths.
- Comprehensive type hints to improve IDE support and typed usage.
- Jupyter-friendly usage patterns, including async-friendly dependencies for notebook environments.

### CLI

- Initial `tif1` CLI with commands for listing events, listing sessions, inspecting drivers, viewing fastest laps, checking cache information, clearing cache, printing version information, and running debug-oriented session loads.
- Rich-formatted terminal output for common exploration tasks.

### Documentation

- Initial public documentation and project metadata for the first release.
- Versioned documentation deployment for `0.1.0`.

### Notes

- `0.1.0` established the public API and packaging baseline for the project.
- Later releases may refine packaging, naming, documentation, and release automation without changing the core goal of providing fast, canonical Formula 1 timing-data access.
