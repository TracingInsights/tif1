# Changelog

All notable changes to this project are documented in this file.

The project uses semantic versioning. Release dates are listed in `YYYY-MM-DD` format.

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
