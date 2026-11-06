# G1 intended commits (`git commit` was denied in the sandbox; everything is staged)

Split the staged tree into these commits, in order:

1. `perf(gortyn-G1): base: E4 on 0.11.3`
   - `native/Cargo.lock`, `native/Cargo.toml`, `native/README.md`,
     `native/src/lib.rs`, `native/src/lz4.rs`
   - `src/tif1/cache.py`, `src/tif1/core_utils/json_utils.py`
   - `tests/unit/test_elis_e4_huge_page_rows.py`
   - `tools/perf/elis_e4_alltel_probe.py`, `tools/perf/elis_e4_ceiling.py`,
     `tools/perf/elis_e4_decode_speed.py`, `tools/perf/elis_e4_warm_ab.py`
   - `.agents/perf-elis/E4.md`, `.agents/perf-elis/E4_*.json`,
     `.agents/perf-elis/E4_venv_requirements.txt`

2. `perf(gortyn-G1): A/B harness with a wheel overlay, fault and blob-read probes`
   - `tools/perf/gortyn_g1_warm_ab.py`, `tools/perf/gortyn_g1_fault_probe.py`,
     `tools/perf/gortyn_g1_blob_read.py`

3. `perf(gortyn-G1): report: E4 re-measured on 0.11.3 (PARTIAL), chunked blob reads rejected`
   - `.agents/perf-gortyn/G1.md`, `.agents/perf-gortyn/G1_COMMITS.md`,
     `.agents/perf-gortyn/G1_ab_pandas_*.json`,
     `.agents/perf-gortyn/G1_step4_chunked_blob_read.patch`
