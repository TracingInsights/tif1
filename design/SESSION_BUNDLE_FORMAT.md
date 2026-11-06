# Session bundles (`session.v1.bin`)

Status: format v1 (Corinth C10), reader in tif1 behind `session_bundle`
(default `False`, `TIF1_SESSION_BUNDLE`); the data repositories do not publish
the files yet. Codec: `src/tif1/core_utils/session_bundle.py`; reader:
`src/tif1/session_bundle.py`; encoder for the publishing pipeline:
`tools/data/encode_session_bundle.py`.

## Why

A cold session reads its small files one request each, partly one after the
other:

| file | repository | read by |
|---|---|---|
| `drivers.json`, `session_laptimes.json` | `TracingInsights/{year}` | the first wave (`get_session`, during the pandas/polars import), `laps` |
| `weather.json`, `rcm.json` | `{year}` | `load()` (weather, messages) |
| `session_info.json` | `{year}` | `t0_date`, `session_start_time`, `track_status`, `session_status`, `session_info`, `total_laps` |
| `{year}/events.json` (season index), `{year}/{slug}/results.json` | `TracingInsights-Archive/Stats` | `results`, `drivers` (the Ergast classification: the index first, then the round's file) |

After the first wave, `load()` + `results` + `t0_date` pays four more round
trips in sequence (weather and race control together, then the season index,
the classification, `session_info.json`); a season loop pays the laps wave
and the classification per race. A bundle carries all of them, unchanged, in
one file next to the session's files, so one request answers the session.

Sizes (2025 Bahrain; the files as served, gzip -6, against the bundle):

| session | files (gzip -6) | `session.v1.bin` |
|---|---|---|
| Race (7 entries) | 66.1 KB | 48.9 KB |
| Qualifying (7 entries) | 23.1 KB | 20.2 KB |
| 2025 races 1-6 (drivers, laptimes, results, index) | 331 KB | 254 KB |

## Path

`{year}/{gp}/{session}/session.v1.bin` in the `{year}` repository, next to the
session's JSON files (`gp`/`session` as in every other path: the event and
session folder names). Not published for the Ergast-only seasons (before
2018), which have no session files.

## Layout (little-endian)

```
offset  size  field
0       8     magic b"TIF1SES\x01"
8       8     u64: size of the decompressed body
16      ...   one zstd frame (any level; content size and content checksum set)
```

The decompressed body:

```
0       4     u32: H, the size of the head
4       H     head: a JSON object (UTF-8)
4+H     ...   the entries' bytes, concatenated in the head's order
```

The head:

```json
{
  "format": 1,
  "built_at": 1762430000.0,
  "entries": [["drivers.json", 4965], ["session_laptimes.json", 343445], ...],
  "absent": ["session_info.json"]
}
```

- `format`: 1. A reader rejects any other value (and falls back to the files).
- `built_at`: epoch seconds at which the encoder read its inputs.
- `entries`: `[name, size]` pairs; the sizes add up to the body after the head
  exactly (else the bundle is rejected).
- `absent`: names of member files the repository did not hold at `built_at`.

Entry names:

- per-year repository files: the session-relative path, one of
  `drivers.json`, `session_laptimes.json`, `session_info.json`,
  `weather.json`, `rcm.json` (`SESSION_MEMBERS`);
- Stats repository files: `stats/{year}/{slug}/{file}`, the key of tif1's
  Ergast cache tier (`slug` decoded, as `race_slug(raceName)` of the round in
  `events.json`): the season index `stats/{year}/events.json` and the session's
  classification file, `results.json` (Race, Sprint Shootout, practice: the
  race results, as tif1 reads them), `quali_results.json` (Qualifying) or
  `sprint_results.json` (Sprint), when it exists at `built_at`.

A reader ignores names it does not know. A member's bytes are the file's bytes
as the CDN serves them after content decoding: byte-identical, so every parser
downstream is unchanged (`--verify` checks it).

## Reader contract (tif1)

- On (`session_bundle=True`, threads available, not offline, not Pyodide), the
  first fetch of any per-year member of a session downloads the bundle once
  per process (a future shared by the async fan-out, the sync loader and the
  Ergast thread); a cold `get_session` asks for it in the stdlib first wave
  instead of `drivers.json` and the laptimes.
- A member the bundle carries is parsed, validated and returned exactly as the
  fetched file would be. A member listed in `absent` raises
  `DataNotFoundError` and gets the missing-payload verdict, as an all-CDN 404
  would. A member the bundle neither carries nor lists is fetched alone.
- After decoding, a background writer (joined like the frame-tier writers)
  fills the payload tier with every member the cache does not hold, as a fetch
  of that file would store it (its bytes, or the validated object when a
  validator changed it), records the absent ones as missing, and writes the
  Stats entries as `{"fetched_at": built_at, "payload": ...}` unless the tier
  holds a newer entry.
- The Ergast tier reads a Stats entry from a decoded bundle when the memo and
  SQLite tiers hold nothing newer, aged from `built_at`: past seasons never
  expire, the current season's entries expire `ergast_ttl_hours` after
  `built_at` (an older bundle copy is not used; the file is fetched). The
  Ergast prefetch thread waits for the session's bundle download before its
  lookups.
- A 404 for the bundle on the primary CDN is remembered in the
  missing-payload tier (key `{year}/{gp}/{session}/session.v1.bin`, TTL
  `missing_payloads_ttl_days`) and for the process; the files follow, at the
  cost of that one request. A transport failure on every mirror or an
  unreadable bundle falls back to the files without a verdict.

## Publishing contract (data pipeline)

- Build a session's bundle after its files and rebuild it whenever one of its
  members changes upstream (a file added or corrected, the classification
  amended, the season index updated): the reader trusts `absent` and treats
  the members as current as of `built_at`.
- For a practice session, the race results exist only after the race: rebuild
  the practice bundles of an event after its race (until then they carry the
  season index and list no classification; the reader fetches it alone).
- `PYTHONPATH=src python tools/data/encode_session_bundle.py --year Y
  --repo-dir <{year} checkout> --stats-dir <Stats checkout> --verify` encodes
  every session folder; `--session-dir` one folder. Encoding is ~0.2 s per
  session at level 19.

## Versioning

A change of the layout, the head's fields or the meaning of an entry is a new
`format` and a new file name (`session.v2.bin`); readers of v1 never ask for
it. New member names may be added to v1 bundles (old readers ignore them).
