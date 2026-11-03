# E3 intended commits (`git commit` was denied in the sandbox; everything is staged)

1. `perf(elis-E3): persist niquests' GOOD OCSP verdicts across processes and load qh3._hazmat without the QUIC stack`
   - `src/tif1/ocsp_cache.py`, `src/tif1/http_session.py`, `src/tif1/config.py`,
     `tests/unit/test_ocsp_cache.py`
   - The revocation check stays on. `_create_session` loads `qh3._hazmat` on its own
     (2 ms instead of the 32 ms `qh3` package `__init__`) and restores the GOOD verdicts
     earlier processes wrote to the SQLite json tier (row `tif1/ocsp_verdicts`) into
     niquests' `InMemoryRevocationStatus` through its pickling contract. New verdicts
     are re-authenticated against their issuer before they are written (after each
     connection warm-up and at `close_session`). `http_revocation_cache`
     (`TIF1_HTTP_REVOCATION_CACHE`) turns the persistence off.

2. `docs(elis-E3): document http_revocation_cache and the kept OCSP answers`
   - `docs/reference/environment-variables.mdx`, `docs/api-reference/config.mdx`,
     `docs/api-reference/http-session.mdx`, `AGENTS.md`

3. `perf(elis-E3): three-arm cold bench, OCSP probe and the E3 report`
   - `tools/perf/elis_e3_cold_bench.py`, `tools/perf/elis_e3_ocsp_probe.py`,
     `.agents/perf-elis/E3.md`, `.agents/perf-elis/E3_*.json`, this file
