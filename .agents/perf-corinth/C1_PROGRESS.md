# C1 progress

- 2026-11-06 start: read brief/hypothesis/do-not-retry; no handoff.
- traced cold season loop (control tree): per race = drivers+laptimes wave (parallel) -> laps build -> Stats results.json (sequential, MainThread). Hot jsDelivr edge ~15-30 ms per file, edge miss 150-800 ms per file (1.5 s for weather.json). load() loops also fetch weather/rcm. Hot-edge control ~70 ms/race.
- implemented `tif1/session_lookahead.py` + `first_wave.prefetch`: round r after r-1 prefetches round r+1's files (what r-1 fetched) + Stats classification into first-wave slots. First trace: ~30 ms/race on hot edge, results 22 -> 0.2-6 ms.
- cold A/B (5 pairs): pandas season wall 5740 -> 4366 (5/5), loop 2805 -> 1513; polars season wall 4679 -> 3034 (5/5), loop 3142 -> 1698; load() loop 8 rounds wall 3166 -> 2327 (4/4); digests equal. Single-race cold: noise (lookahead bookkeeping 9.5 us/get_session).
- unit suite 4121 passed; new tests/unit/test_corinth_c1_session_lookahead.py (34); ruff/ty clean.
- digest switched to row hashes (to_csv was 1-1.8 s inside the wall): pandas season wall 4236 -> 2519 (5/5); polars rerun 6406 -> 4233 (5/5, erratic network); season_tel 4/4; season_laps loop 1278 -> 884; single-race laps 10 pairs 631 -> 592 (6/10, noise). Warm A/B noise both ways (load 30-36). perf gate 6 passed. Parity 0/133, 0/131.
- report written (.agents/perf-corinth/C1.md), verdict SHIP; commit denied -> staged + C1_COMMITS.md.
