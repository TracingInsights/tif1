## Wave 2 (20 more hypotheses, 15 shipped)

Run by subagents in isolated worktrees against the wave-1 tree and integrated by hand; one report per hypothesis in `.agents/perf-pergamon/q/`, summary in `.agents/perf-pergamon/q/RESULTS.md`.

Analysis-notebook benchmark (warm 2026 Monaco race, fresh process, offline, 8 interleaved rounds):

| step | Thebes base | wave 1 | final |
|---|---:|---:|---:|
| `load()` | 288 ms | 114 ms | 54 ms |
| `iterlaps` + `lap.telemetry` | 371 ms | 207 ms | 87 ms |
| 4-driver telemetry analysis | 70 ms | 68 ms | 41 ms |
| 4 × `get_driver(d).get_fastest_lap()` | 33 ms | 47 ms | 20 ms |
| new session, `get_driver(d).laps` | 40 ms | 39 ms | 7 ms |
| whole process | 1350 ms | 988 ms | 705 ms |

Warm telemetry phase vs wave 1: pandas 98.7 → 74.6 ms (10/10), polars 90.6 → 58.8 ms (8/8). Interactive sessions: `get_session` after `import tif1` 0.22-0.30 s → 0.02-0.03 s (background preload). First chart to PNG 1.84 → 1.05 s; `tif1 cache-info` / `events` / `sessions` ~3.5x faster.

Kept: Q1, Q2, Q4, Q6, Q7, Q8, Q9 (memory retention), Q10, Q11, Q13, Q14, Q15, Q16 (Jupyter crash fix + sync known-missing reads), Q17, Q18, Q19, Q20. Rejected: Q3, Q5, Q12, and the Rust lap builder probed in Q18.

Fixed along the way: a Jupyter `asyncio.run()` crash introduced by wave 1's asyncio-free path; missing laps re-walking the CDN (up to a 30 s timeout) in every process; a zstd decompressor shared across worker threads; the process-wide laps cache keeping every loaded session alive.

Verification: parity 1452/1452 pandas and polars frames vs wave 1 (also after a full re-materialization by the new writers), 9859/9859 frames of a 2018-2025 cache; unit + property + integration 1875 passed (86.9 %); ruff, ty and the docs STE gate clean.
