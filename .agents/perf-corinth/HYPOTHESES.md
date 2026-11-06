# Corinth hypotheses (one agent each; the control is 552c357 = main after Naxos)

Numbers from `BRIEF.md` (this box, fresh process). Most of this round is user
flows no earlier round measured: the benchmarks so far were one 2025 race.
For every cold measurement: interleave control and candidate, 5-8 reps, report
medians and p90, and name what else used the network.

## C1. A season loop, cold

Evidence: `survey.py season pandas` (24 races of 2025:
`get_session(2025, r, "R")`, `.laps`, `.results`) takes 15.5 s cold, ~650 ms
per race, while one race's cold `laps` is ~590 ms *including* the pandas
import: each race pays a chain of sequential round trips (schedule
resolution, the first wave `drivers.json` + laptimes, `session_info.json`,
the Ergast Stats classification, the circuit/event tables...). Warm, the
same loop is 122 ms.
Approach: (1) trace one cold race's requests in order (start, TTFB, end,
which call waited on which; the stdlib bridge, niquests, the Ergast prefetch
thread, `_preconnect_cdn`) and the season loop's timeline. (2) Cut the
per-session critical path: fetch a session's independent files in one
parallel wave as soon as the session is known (the classification, session
info, laps and drivers do not depend on each other), keep connections warm
across sessions in one process (no re-handshake, no re-import), avoid
per-session work that only the first session needs. (3) Sequential access
across sessions: when a script asks for round r and then r+1 of the same
season and session type, the next round's first-wave files can be fetched
in the background (an idle-slot lookahead like Elis E10's, at most one
session ahead, cancelled at exit; measure it against the risk of wasted
requests and the E10 +24% lesson about background traffic sharing the link
with a foreground wave). Workloads: the season loop (pandas and polars),
one cold race (`laps`, `laps+results`), a cold `fastest_lap_tel`, each A/B'd
(5 reps of the season loop is ~80 s per arm: keep it to 4-5 pairs). Parity:
0/133, 0/131 warm digests; the loop's frames equal the control's (write a
small digest of every race's laps/results).

## C2. A pre-2018 (Ergast-only) session, cold

Evidence: `survey.py old pandas` (1998 Monaco race): cold `laps` 6524 ms,
warm 23 ms; `get_session` 442. Laps of Ergast-only seasons are synthesized
from Ergast lap timings (`core_utils/ergast_results.py`, `ergast/` tier:
Stats `laptimes.json` per round from `TracingInsights-Archive/Stats`, Jolpica
fallback limited to 4 requests per second and 200 per hour).
Approach: trace where the 6.5 s goes (which files, which source, how many
requests, the rate limiter's sleeps, page loops, the merge of split laps,
the frame build). Fix what the trace shows: e.g. the Stats file instead of
Jolpica paging when it exists, parallel fetches of independent files, no
redundant requests (the classification and lap timings of one round fetched
once), a cheaper laps synthesis. Measure cold and warm 1998 Monaco, 2010
Monza, 1975 British GP, 1955 Monaco (laps, results, drivers) on both
backends (5 reps interleaved). Parity: the frames equal the control's on
those sessions (write a digest) and 0/133, 0/131 on the warm digests.

## C3. The Ergast interface (`tif1.ergast`), cold

Evidence: `survey.py ergast pandas`: cold `get_race_results(season=2024)`
1185 ms, `get_driver_standings(season=2024)` 520, `get_qualifying_results
(season=2023, round=5)` 672; warm 54 / 2 / 2. Season-wide answers are
assembled from per-round Stats files (`ergast/router.py`, `payloads.py`,
`payloads.stats_payloads` fans out through a thread pool), with the
CDN mirror walk of `CDNManager.try_sources_repo`.
Approach: trace the requests of each query (count, parallelism, mirror
order, 404 walks, the Jolpica fallbacks, TTLs), then cut: e.g. parallelism
and connection reuse for the per-round fan-out, no mirror walk on the happy
path, a season-level Stats file where one exists, skipping files the query
does not need, cheaper response construction. Measure a set of 10 common
queries (results, qualifying, sprint, standings, schedule, laps, pit stops,
driver info; past and current season) cold and warm, both result types
(pandas, polars), 5 reps interleaved. Parity: the response frames equal the
control's (digest of every query's frames and attributes); keep
`tests/unit/test_ergast_fastf1_compliance.py` green if you can install fastf1
into a private venv (`/tmp/corinth/venv-C3`), otherwise say so.

## C4. Current-season freshness: schedule and Ergast TTL refreshes

Evidence: `current.py` (2026 Mexico City race): cold pandas get_session 1094
ms, laps 442, results 1242; warm 507 total (polars 163); warm with the
schedule and Ergast TTLs expired (`TIF1_SCHEDULE_TTL_HOURS=0
TIF1_ERGAST_TTL_HOURS=0`, what every script pays 6 h after the last refresh):
pandas 722, polars 463 (nearly 3x warm). The current/future season's schedule
is fetched from `theOehrly/f1schedule` (raw GitHub, cached
`schedule_ttl_hours`); the current season's Ergast answers have
`ergast_ttl_hours`.
Approach: trace the refresh requests (when, how many, sequential or not, on
the critical path or not). Ideas: conditional requests (`If-None-Match` /
`If-Modified-Since` with the stored ETag/Last-Modified, a 304 refreshes the
TTL without a body), overlapping the refresh with the pandas/polars import
or the laps load instead of blocking `get_session`, refreshing only what the
call needs (a past round of the current season does not change once its
classification is final: can its Ergast data be cached permanently after a
finished race's results stop changing? Prove it with the data, and keep
fresh results for the latest round), stale-while-revalidate only where the
staleness cannot change an answer (say exactly which). Cold `results` 1.2 s:
trace and cut. Measure the four states (cold, warm, warm expired, warm
expired offline) on both backends, 6 reps interleaved, for a past round of
2026 and the latest round. Parity: the answers equal the control's.

## C5. Python 3.14 warm paths

Evidence: on Python 3.14 (`/tmp/corinth/venv314/bin/python`, same locked
dependencies) the warm workloads are 7-10% slower than on 3.13: pandas laps
444 -> 476 ms wall, get_session 345 -> 374 (in process), all_tel 629 -> 680;
polars laps 204 -> 226, notebook lap loop similar. Naxos N10's GC
promotion (`gc.freeze(); gc.unfreeze()`) is off on 3.14 by design (the
incremental collector re-marks the moved lists). Python 3.14 is the current
stable release; CI tests 3.11-3.15.
Approach: profile the 3.14 warm paths against 3.13 (`-X importtime`, perf,
cProfile, `gc.callbacks` counting collections and their durations), find the
3.14-specific costs (incremental GC passes during imports and frame bursts,
`gc.disable` semantics under 3.14, slower paths in pandas/polars imports on
3.14, interpreter exit), and fix what tif1 controls without hurting 3.13
(e.g. a 3.14-specific GC strategy for the bursts: thresholds, `gc.disable`
spans, `gc.collect(1)` placement; research CPython 3.14's incremental GC
design, `gc.set_threshold` semantics there, and what the 3.15 changes are).
Measure warm `laps`, `all_tel`, `notebook`, `load_only` on 3.14 (8 pairs) and
check 3.13 is unchanged (8 pairs). If you can, also run 3.15 (`uv python
install 3.15`, a private venv `/tmp/corinth/venv315`: report only). Parity
0/133, 0/131 on 3.14 and 3.13.

## C6. Plotting

Evidence: `import tif1.plotting` 539 ms (matplotlib 253, pyplot 127, numpy
119 of it); a warm speed trace to PNG 89-103 ms (`survey.py plot`). Charts
always draw from a pandas session (AGENTS.md). Plotting is how many users
end a script.
Approach: list what `tif1.plotting` (and `tif1.charts` if present) imports
at import time and what each public function needs. Defer what the common
helpers do not need (team/driver colors, `setup_mpl`, `get_driver_style`
lookups that need no matplotlib), keeping fastf1's import-time behaviour
where fastf1 has one (does `fastf1.plotting` import matplotlib at import?
check its source). Then profile the chart functions tif1 provides on a warm
session (every function in `docs/` and the examples; the data preparation
part: telemetry selection, resampling, color lookups) and cut what is
tif1's. Measure: `import tif1.plotting`; a script that gets a team color;
three typical chart scripts (speed trace, lap-time scatter of a race, tyre
strategy) end to end, warm, 8 pairs. Rendering inside matplotlib is out of
scope. Parity: the figures' data (line x/y arrays, colors, labels) equal the
control's.

## C7. The CLI

Evidence: `python -m tif1.cli --help` and `version` take 200-230 ms; the CLI
uses typer + rich (`cli.py`). Commands: events, sessions, drivers, fastest,
results, standings, cache-info, cache-clear, version, debug.
Approach: measure each command warm (and `events`/`results` cold) with
`-X importtime`; defer imports a command does not need (rich tables only
where printed, pandas only for commands that need a session, typer's
completion machinery), keep output identical. Check the installed console
script entry point too (`tif1 ...` via `pyproject.toml`). Measure every
command warm, 10 pairs (they are short). Parity: identical stdout/stderr
and exit codes for every command (capture with `COLUMNS=120`).

## C8. tel.v2 decode CPU

Evidence: Naxos N7 (`.agents/perf-naxos/N7.md`): a race's tel.v2 files
decode in ~472 ms of CPU (4.1 ms per file) versus 273 ms for tel.v1 and
the native JSON decoder; on a 100 MB/s link `all_tel` with v2 tied v1 at
first and won 7% after the decoder was sped up: decode CPU decides fast-link
cold loads. The remaining cost is "mostly numpy orchestration around the
kernels" (N7 section 6). The kernels are in `native/src/telcols.rs`
(tif1-native 0.3.0, unreleased).
Approach: move the whole file decode into one native call that returns the
columns per lap (zstd frame, byte unshuffle, value encodings, predictions,
residual application), GIL released, with the numpy decoder kept as the
fallback; then parallel decode across files if the GIL is released. Build
the wheel into a private venv `/tmp/corinth/venv-C8` (`uvx maturin build
--release -m native/Cargo.toml`, then `uv venv` + `uv pip install` the tree
and the wheel; never touch the main venv). You need tel.v2 files: encode
the race and qualifying from the warm cache's payloads with
`tools/data/encode_driver_telemetry.py` into a scratch directory and serve
them with N7's local CDN harness (`tools/perf/naxos_n7_cold_local.py`).
Measure decode ms per race (both sessions), and cold `all_tel`, notebook4,
`driver_tel` on the local CDN at 8 MB/s and 100 MB/s, v2-before vs v2-after
(8 pairs). Exactness: decoded laps bit-identical (`--verify`); frame digests
equal.

## C9. Schedule APIs without the pandas import

Evidence: `get_event_schedule(2025)` 394-469 ms, nearly all of it `import
pandas`, on polars sessions too (`survey.py schedule`, `season`); `get_event`
and `get_session` resolution are already pandas-free (9-13 ms). fastf1
returns a pandas `EventSchedule`, so `tif1.get_event_schedule` must keep
returning one by default.
Approach: (1) what polars users and scripts actually do with a schedule
(round numbers, event names, dates, session names: iterate rows, filter);
(2) make those reachable without pandas: e.g. a `lib="polars"` (or
`backend=`) option on `get_event_schedule` / `get_events_remaining` returning
a polars frame with the same columns and dtypes mapped (document the
mapping), pandas-free `Event`-like access, or a lazily materialized
`EventSchedule` (check whether any fastf1 behaviour forbids it); (3) cut what
is left of the pandas `EventSchedule` construction itself (after the
import). Measure `get_event_schedule(2025)` both ways, a polars season loop
that reads the schedule, and `get_events_remaining`, 10 pairs warm and 5
cold. Parity: the pandas `EventSchedule` is unchanged (digest every column,
dtype, index, attrs); the polars schedule matches it value for value.

## C10. One bundle file per session (data format + reader)

Evidence: a cold session pays one request per file it reads (`drivers.json`,
laptimes, `session_info.json`, weather, race control, the Ergast
classification from another repository); the season loop is 24 x those round
trips (C1 attacks the code side; this is the data side). Naxos N7 / Gortyn
G3 set the pattern: a new file in the data repositories, an encoder in
`tools/data/`, a reader that asks for it first and falls back to the
per-file JSON, off by default until the files are published.
Approach: design `session.v1.bin` (or `.json.zst`): the small per-session
JSON files in one compressed container (zstd, a directory of entries with
their bytes; decide which files belong: the first wave, session info, weather,
race control, the classification; not telemetry), byte-identical payloads
after decode (so every parser downstream is unchanged). Reader: on a cold
session, one request for the bundle; on 404 (remembered in the missing tier
like the columnar files) the per-file path; each entry fills the payload
tier as if fetched alone. Encoder `tools/data/encode_session_bundle.py`
with `--verify`. Measure on a local CDN (copy N7's harness, `--link` 8 MB/s
and a 40 ms RTT if you can add latency) the cold single session (`laps`,
`laps+results`, `load()`) and a cold 6-race loop, bundle vs no bundle (8
pairs), and the bytes. Parity: payloads byte-identical; frames equal. Write
`design/SESSION_BUNDLE_FORMAT.md`. Config key off by default.
