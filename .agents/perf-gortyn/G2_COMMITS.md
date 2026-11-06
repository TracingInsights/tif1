# G2 intended commits (git commit was denied in the agent sandbox; everything is staged)

## 1. `perf(gortyn-G2): private-seam tests and a weekly upstream canary`

Files: `tests/unit/test_private_seams.py`, `tests/unit/test_upstream_canary_report.py`,
`tools/upstream_canary_report.py`, `tools/upstream_prereleases.py`,
`.github/workflows/upstream-canary.yml`, `AGENTS.md`, `CONTRIBUTING.md`.

```
perf(gortyn-G2): private-seam tests and a weekly upstream canary

tests/unit/test_private_seams.py lists every private upstream API a fast
path relies on (pandas block/array internals and constructor hooks, polars
read_ipc_stream and _from_pydf, niquests' OCSP pickling, the qh3._hazmat
preload, threading._register_atexit) with the version it was verified
against, and asserts each fast path is the one taken, so a drift fails
under the seam's name instead of silently falling back.

.github/workflows/upstream-canary.yml (weekly + dispatch, never a PR gate)
installs the newest releases and pre-releases of those packages over the
locked env, runs the seam tests, the unit suite and the perf gate, and
opens or updates one upstream-drift issue (tools/upstream_canary_report.py).
```

## 2. `docs(gortyn-G2): report and the in-memory wheel drift check`

Files: `.agents/perf-gortyn/G2.md`, `.agents/perf-gortyn/G2_wheel_drift.py`,
`.agents/perf-gortyn/G2_COMMITS.md`.
