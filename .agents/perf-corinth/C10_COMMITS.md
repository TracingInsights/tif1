# C10 intended commits (`git commit` was denied by the sandbox; everything is staged)

1. `perf(corinth-C10): session bundle (session.v1.bin): format, encoder, reader (off by default)`

   Files: `src/tif1/core_utils/session_bundle.py`, `src/tif1/session_bundle.py`,
   `src/tif1/async_fetch.py`, `src/tif1/payload_loader.py`,
   `src/tif1/ergast/payloads.py`, `src/tif1/first_wave.py`, `src/tif1/core.py`,
   `src/tif1/config.py`, `tests/unit/test_session_bundle.py`,
   `tools/data/encode_session_bundle.py`, `design/SESSION_BUNDLE_FORMAT.md`,
   `AGENTS.md`.

   Body: One zstd file per session carries drivers.json, the session
   laptimes, session_info.json, weather, race control and the Stats
   classification with its season index, byte for byte. With session_bundle
   on, the first fetch of any member downloads it once per process (the cold
   first wave asks for it instead of the two first-wave files); members are
   validated and cached as fetched files, absent ones get the all-404
   verdict, the Ergast tier ages its entries from the bundle's built_at, and
   a 404 is remembered in the missing tier before the files follow. Encoder
   tools/data/encode_session_bundle.py --verify.

2. `perf(corinth-C10): local HTTPS CDN harness, measurements and report`

   Files: `tools/perf/corinth_c10_cold_local.py`, `.agents/perf-corinth/C10*.{md,json}`.
