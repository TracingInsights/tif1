# G3 intended commits (git commit is denied in this sandbox; everything is staged)

1. `perf(gortyn-G3): per-driver columnar telemetry files (tel.v1, 10 laps each) and the reader`

   A cold wave of two or more laps of one driver (or a lazy-stream request)
   asks for the driver's `{driver}/tel.v1.{first}.bin` files (typed columns
   in one zstd frame, ~26% fewer bytes than the gzipped lap JSON, decoded to
   exactly the native decoder's mapping) before the per-lap JSON. A 404 on
   the primary CDN is remembered in the missing-payload tier and ends
   probing for that session in-process. Unreadable or failing files fall
   back to the JSON. Config `telemetry_columnar` (`TIF1_TELEMETRY_COLUMNAR`),
   default True. `HttpTransport.get_bytes` on `NiquestsTransport` and
   `InMemoryTransport`.
   Files: src/tif1/core_utils/columnar_telemetry.py (new), async_fetch.py,
   payload_loader.py, first_wave.py (status-aware stdlib fetch: a bridge 404
   is final), config.py, core.py, core_utils/constants.py,
   tests/unit/test_first_wave.py,
   tests/unit/test_columnar_telemetry.py (new).

2. `docs(gortyn-G3): tel.v1 format, encoder for the data pipeline, harnesses, report`

   design/TELEMETRY_COLUMNAR_FORMAT.md, tools/data/encode_driver_telemetry.py,
   tools/perf/gortyn_g3_format_compare.py, tools/perf/gortyn_g3_cold_local.py,
   AGENTS.md, docs/api-reference/config.mdx, .agents/perf-gortyn/G3*.
