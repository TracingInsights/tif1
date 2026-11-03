# G6 intended commits (`git commit` is denied in this sandbox; everything is staged)

1. `perf(gortyn-G6): base: G1`

   The inherited G1 tree (Elis E4 on 0.11.3: `tif1_native.lz4_frame_decompress_into`
   on lz4_flex, huge-page driver rows in `cache.py`, E4's tests and probes, G1's
   report, artefacts and probes). Unchanged from what the worktree held.

2. `perf(gortyn-G6): decode lz4 frame blocks with liblz4 (lz4-sys) instead of lz4_flex`

   `native/src/lz4.rs` keeps the frame parsing and every error, and decodes the
   blocks with `LZ4_decompress_safe` / `LZ4_decompress_safe_usingDict` (the
   previous 64 KB of output as the in-place prefix). An unsized frame's last
   block goes through a scratch buffer, so nothing lands past the content.
   `native/Cargo.toml`/`Cargo.lock`: `lz4-sys 1.11` replaces `lz4_flex`.
   Serial decode of the 40 Bahrain rows: 67.1 -> 40.0 ms (pyarrow 41.0).
   `native/tests/smoke.py` decodes a hand-built linked frame on every wheel;
   `tests/unit/test_gortyn_g6_native_lz4.py`; `native/README.md`, the C
   toolchain notes in `native.yml`, and the CI `native` job asserts the lz4
   entry point.

3. `perf(gortyn-G6): probes and report`

   `tools/perf/gortyn_g6_warm_ab.py` (per-arm wheel overlays),
   `tools/perf/gortyn_g6_fault_probe.py` (per-arm `SITE=`),
   `tools/perf/gortyn_g6_decode_speed.py`, `.agents/perf-gortyn/G6.md` and the
   A/B JSON files.
