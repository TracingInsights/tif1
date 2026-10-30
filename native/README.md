# tif1-native

Optional native accelerators for [tif1](https://github.com/TracingInsights/tif1).
`tif1` works without this package; when it is importable, `tif1` decodes
telemetry payloads with `tif1_native.decode_telemetry`, which parses a
`{driver}/{lap}_tel.json` payload straight into typed numpy arrays while
releasing the GIL, so the fetch and cache threads parse in parallel on
multi-core hosts. From 0.2 the `DriverAhead` channel is returned
dictionary-encoded (int32 codes into a short category list, built by the
`dict_factory` tif1 passes) instead of one Python `str` per sample; tif1
builds the string column from the codes. Frames are identical either way
(`tests/unit/test_native_decoder.py`).

`TIF1_DISABLE_NATIVE=1` makes tif1 ignore an installed extension.

## Build locally

Needs a Rust toolchain:

    uvx maturin develop --release -m native/Cargo.toml

or build a wheel with `uvx maturin build --release -m native/Cargo.toml`.

## Wheels

`.github/workflows/native.yml` builds and tests the distribution on every
change under `native/`, and uploads it to TestPyPI or PyPI from a manual
dispatch only:

- one `cp311-abi3` wheel per platform, used by every GIL CPython from 3.11
  on (3.15 included) without a rebuild;
- no free-threaded wheels for now: pyo3 0.29 builds only 3.14t and tif1 does
  not run on free-threaded CPython yet (orjson and polars ship no such
  wheels); the module declares `gil_used = false`, so a local build on a
  free-threaded interpreter keeps the GIL disabled;
- Linux manylinux2014 and musllinux_1_2 (x86_64, aarch64), macOS universal2,
  Windows x64 and arm64, plus an sdist for other platforms (needs Rust).

Pyodide is not built; tif1 uses orjson there.
