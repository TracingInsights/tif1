# C3 intended commits (git commit is denied in this sandbox; everything is staged)

## 1. perf(corinth-C3): cold tif1.ergast queries overlap the pandas import, fetch over the stdlib client, one wave per cold season

- pandas response classes move to `tif1.ergast.frames`, resolved through
  `interface.__getattr__` (same public path, `__module__`, pickling):
  importing `tif1.ergast` no longer imports pandas.
- A Stats-routed query runs in a daemon thread while the frame modules
  (pandas; polars and pyarrow for `result_type="polars"`) import through
  `import_promoted`; Jolpica-routed queries import first, then fetch inline.
- Stats files go over `first_wave.standalone_client()` (the wave's client,
  else one of its own that never opens the telemetry bridge) while niquests
  has no session: no niquests import, no OCSP-checked handshakes; 404/403 are
  final there, anything else goes to niquests.
- A cold season fetches `events.json` and the round files the query reads
  (predicted from the packaged Ergast schedule) in one `stats_payloads` wave.
- `tools/perf/corinth_c3_ergast_ab.py`: interleaved cold/warm A/B of ten
  Ergast queries with a per-answer digest; `tools/perf/corinth_c3_trace.py`:
  per-request timeline of one cold query.
- `tests/unit/test_ergast_cold_path.py`: lazy pandas, overlap, routing,
  predicted files, the cold wave, the stdlib statuses, the standalone client.
- AGENTS.md: Ergast tier and HTTP bullets.

Cold wall, 5 interleaved pairs: pandas -34..-46% on the nine Stats-routed
queries (5/5 each; race results 2024 1084 -> 638 ms), polars -28..-39%;
Jolpica query even; warm neutral to -8%; parity exact (answer digests,
0/133 and 0/131 session surfaces, fastf1 compliance 8/8).
