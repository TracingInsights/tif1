# tif1-native

Optional native accelerators for [tif1](https://github.com/TracingInsights/tif1).
`tif1` works without this package; when it is importable, `tif1` decodes
telemetry payloads with `tif1_native.decode_telemetry`, which parses a
`{driver}/{lap}_tel.json` payload straight into typed numpy arrays while
releasing the GIL, so the fetch threads parse in parallel on multi-core hosts.

Build locally (needs a Rust toolchain):

    uvx maturin develop --release -m native/pyproject.toml

or build a wheel with `uvx maturin build --release -m native/pyproject.toml`.
