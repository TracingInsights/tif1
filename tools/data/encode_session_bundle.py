"""Encode a session's small JSON files into its bundle (``session.v1.bin``, Corinth C10).

The data repositories (``TracingInsights/{year}``) publish a session's small
files one by one (``drivers.json``, ``session_laptimes.json``,
``session_info.json``, ``weather.json``, ``rcm.json``), and its Ergast
classification lives in ``TracingInsights-Archive/Stats``
(``{year}/{slug}/results.json`` and the season index ``{year}/events.json``).
This tool writes, next to the session's files, ``session.v1.bin``: their bytes,
unchanged, in one zstd container that tif1 reads first on a cold session
(format: ``design/SESSION_BUNDLE_FORMAT.md``, codec:
``tif1.core_utils.session_bundle``).

Sources:

- ``--session-dir DIR`` (repeatable) or ``--repo-dir DIR``: session folders of a
  ``{year}`` repository checkout (``DIR`` = ``{repo}/{GP}/{Session}``; with
  ``--repo-dir`` every ``{GP}/{Session}`` folder holding a ``drivers.json``).
  The member files the folder lacks are listed as absent. ``--stats-dir`` is a
  checkout of the Stats repository: the session's classification file (the
  race results for practice, as tif1 reads them) and the season index are
  added when they exist. The round is resolved with tif1's schedule, else by
  the race name in ``events.json``.
- ``--cache DIR``: every session in a tif1 cache's JSON tier, with the Stats
  entries of its Ergast tier (re-serialized by orjson) and the members its
  missing-payload tier records as absent; the benchmark harnesses use it.

Bundles are written to the session folder (or under ``--out-dir`` as
``{year}/{GP}/{Session}/session.v1.bin``). ``--verify`` decodes every bundle
written and checks that each entry is byte-identical to its input file and
parses to the same object. Rebuild a session's bundle whenever one of its
members changes upstream (the reader trusts ``absent`` and ages the Stats
entries from ``built_at``).

Usage (from the repo root)::

    PYTHONPATH=src python tools/data/encode_session_bundle.py --year 2025 \\
        --session-dir "2025/Bahrain Grand Prix/Race" --stats-dir Stats --verify
"""

from __future__ import annotations

import argparse
import gzip
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import unquote

import orjson

from tif1.cache import _decode_sqlite_value
from tif1.core_utils.ergast_results import ergast_endpoint
from tif1.core_utils.json_utils import json_loads
from tif1.core_utils.session_bundle import (
    BUNDLE_FILE,
    SESSION_MEMBERS,
    STATS_PREFIX,
    decode_bundle,
    encode_bundle,
)
from tif1.ergast.stats import EVENTS_FILE, STATS_FILES, race_slug

SessionKey = tuple[int, str, str]


def stats_names(year: int, gp: str, session: str, races: list[dict] | None) -> list[str]:
    """Stats entry names of a session: the season index and its classification file."""
    if not races:
        return []
    round_number = None
    try:
        from tif1.events import _event_round_number

        round_number = int(_event_round_number(year, gp, None))
    except Exception:  # testing events, unknown names: match the race name instead
        round_number = None
    race = None
    for candidate in races:
        if round_number is not None and str(candidate.get("round")) == str(round_number):
            race = candidate
            break
        if round_number is None and candidate.get("raceName") == gp:
            race = candidate
            break
    names = [f"{STATS_PREFIX}{year}/{EVENTS_FILE}"]
    if race is not None:
        slug = race_slug(str(race.get("raceName", "")))
        names.append(f"{STATS_PREFIX}{year}/{slug}/{STATS_FILES[ergast_endpoint(session)]}")
    return names


def _races(events: bytes | None) -> list[dict] | None:
    if events is None:
        return None
    races = json_loads(events).get("MRData", {}).get("RaceTable", {}).get("Races")
    return races if isinstance(races, list) else None


def _read_file(path: Path) -> bytes | None:
    if path.is_file():
        return path.read_bytes()
    gz = path.with_name(path.name + ".gz")
    if gz.is_file():
        return gzip.decompress(gz.read_bytes())
    return None


def from_session_dir(
    year: int, folder: Path, stats_dir: Path | None
) -> tuple[dict[str, bytes], list[str]]:
    """``(entries, absent)`` of a session folder of a ``{year}`` repository checkout."""
    gp, session = folder.parent.name, folder.name
    entries: dict[str, bytes] = {}
    absent: list[str] = []
    for name in SESSION_MEMBERS:
        body = _read_file(folder / name)
        if body is None:
            absent.append(name)
        else:
            entries[name] = body
    if stats_dir is not None:
        races = _races(_read_file(stats_dir / str(year) / EVENTS_FILE))
        for name in stats_names(year, gp, session, races):
            body = _read_file(stats_dir / name.removeprefix(STATS_PREFIX))
            if body is not None:
                entries[name] = body
    return entries, absent


def from_cache(cache_dir: Path) -> dict[SessionKey, tuple[dict[str, bytes], list[str]]]:
    """``{(year, gp, session): (entries, absent)}`` of every session in a tif1 cache.

    ``gp``/``session`` are the URL-encoded names of the cache keys.
    """
    conn = sqlite3.connect(f"file:{(cache_dir / 'cache.sqlite').as_posix()}?mode=ro", uri=True)
    files: dict[str, bytes] = {}
    for key, data in conn.execute("SELECT key, data FROM cache"):
        raw = _decode_sqlite_value(data)
        files[key] = raw if isinstance(raw, bytes) else raw.encode()
    missing = {key for (key,) in conn.execute("SELECT key FROM missing_payloads")}
    conn.close()
    stats: dict[str, bytes] = {}
    for key, raw in files.items():
        if key.startswith(STATS_PREFIX):
            payload = json_loads(raw).get("payload")
            if isinstance(payload, dict):
                stats[key] = orjson.dumps(payload)
    sessions: dict[SessionKey, tuple[dict[str, bytes], list[str]]] = {}
    for key in files:
        parts = key.split("/")
        if len(parts) != 4 or parts[3] != "drivers.json" or not parts[0].isdigit():
            continue
        year, gp, session = int(parts[0]), parts[1], parts[2]
        prefix = f"{year}/{gp}/{session}/"
        entries = {name: files[prefix + name] for name in SESSION_MEMBERS if prefix + name in files}
        absent = [name for name in SESSION_MEMBERS if prefix + name in missing]
        races = _races(stats.get(f"{STATS_PREFIX}{year}/{EVENTS_FILE}"))
        for name in stats_names(year, unquote(gp), unquote(session), races):
            if name in stats:
                entries[name] = stats[name]
        sessions[(year, gp, session)] = (entries, absent)
    return sessions


def verify(blob: bytes, entries: dict[str, bytes], absent: list[str]) -> None:
    """Decode ``blob`` and check every entry against its input (raises AssertionError)."""
    bundle = decode_bundle(blob)
    assert list(bundle.entries) == list(entries), "entry names or order differ"
    assert bundle.absent == frozenset(absent), "absent names differ"
    for name, body in entries.items():
        assert bundle.entries[name] == body, f"{name}: bytes differ"
        assert json_loads(bundle.entries[name]) == json_loads(body), f"{name}: parse differs"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--year", type=int, help="season of --session-dir/--repo-dir")
    parser.add_argument("--session-dir", action="append", default=[], type=Path)
    parser.add_argument("--repo-dir", type=Path, help="a {year} repository checkout")
    parser.add_argument("--stats-dir", type=Path, help="a Stats repository checkout")
    parser.add_argument("--cache", type=Path, help="a tif1 cache directory")
    parser.add_argument("--out-dir", type=Path, help="write {year}/{GP}/{Session}/ here")
    parser.add_argument("--level", type=int, default=19)
    parser.add_argument("--built-at", type=float, default=None, help="epoch seconds (now)")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    built_at = time.time() if args.built_at is None else args.built_at

    jobs: list[tuple[Path, dict[str, bytes], list[str]]] = []
    if args.cache is not None:
        if args.out_dir is None:
            parser.error("--cache needs --out-dir")
        for (year, gp, session), (entries, absent) in sorted(from_cache(args.cache).items()):
            folder = args.out_dir / str(year) / unquote(gp) / unquote(session)
            jobs.append((folder, entries, absent))
    folders = list(args.session_dir)
    if args.repo_dir is not None:
        folders += sorted(p.parent for p in args.repo_dir.glob("*/*/drivers.json*"))
    if folders and args.year is None:
        parser.error("--session-dir/--repo-dir need --year")
    for folder in folders:
        entries, absent = from_session_dir(args.year, folder, args.stats_dir)
        out = folder if args.out_dir is None else args.out_dir / folder.parent.name / folder.name
        jobs.append((out, entries, absent))
    if not jobs:
        parser.error("nothing to encode")

    total_in = total_out = 0
    for folder, entries, absent in jobs:
        blob = encode_bundle(entries, absent=absent, built_at=built_at, level=args.level)
        if args.verify:
            verify(blob, entries, absent)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / BUNDLE_FILE).write_bytes(blob)
        per_file = sum(len(gzip.compress(body, 6)) for body in entries.values())
        total_in += per_file
        total_out += len(blob)
        print(
            f"{folder.as_posix()}: {len(entries)} entries, {len(absent)} absent, "
            f"{len(blob) / 1e3:.1f} KB (files gzip-6: {per_file / 1e3:.1f} KB)"
            + (", verified" if args.verify else "")
        )
    print(
        f"{len(jobs)} bundles, {total_out / 1e3:.1f} KB (files gzip-6: {total_in / 1e3:.1f} KB)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
