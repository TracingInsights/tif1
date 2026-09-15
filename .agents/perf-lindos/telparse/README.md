# telparse — N8 Rust/pyo3 probe (rejected)

Parity-exact columnar telemetry JSON parser (`bytes` -> per-channel numpy
arrays; `"None"`-string channels preserved as lists) built with pyo3 0.26 +
numpy 0.26 to test whether a native parser beats the orjson+pandas pipeline.

Measured on the full 2026 Monaco 1452-payload corpus (84.4 MB):
parse 0.90 s vs orjson 0.99 s; full pipeline 2.38 s vs 2.39 s — no speedup
(remaining cost is pandas construction, which is parity-bound). Rejected;
kept for reproducibility.

Build: `maturin build --release` from this directory, install the wheel,
then run `tools/lindos_probe_rust.py`.
