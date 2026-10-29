"""Vendor the MultiViewer circuit table keyed by Ergast ``CircuitId``.

Fetches ``https://api.multiviewer.app/api/v1/circuits`` and joins every
circuit of the packaged Ergast schedules (``src/tif1/data/schedules/ergast/
schedule_{year}.json``, 2018 to the newest file) to a MultiViewer entry by
name, locality and country (``tif1.core_utils.circuit_info.match_circuit_index``,
the matcher the live fallback uses) with a hand-maintained override map for
the mismatches. Writes ``src/tif1/data/circuits/mv_circuits.json``::

    {"bahrain": {"circuitKey": 63, "countryKey": 36, "iocCountryCode": "BRN",
                 "name": "Sakhir", "country": "Bahrain", "years": [2019, ...]}, ...}

sorted by circuit id, and reports the circuit ids MultiViewer does not list.

Usage::

    uv run python tools/vendor_mv_circuits.py [--start 2018] [--end 2026] [--check]
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tif1.core_utils.circuit_info import match_circuit_index  # noqa: E402

INDEX_URL = "https://api.multiviewer.app/api/v1/circuits"
SCHEDULES_DIR = ROOT / "src" / "tif1" / "data" / "schedules" / "ergast"
OUT_PATH = ROOT / "src" / "tif1" / "data" / "circuits" / "mv_circuits.json"

#: Ergast ``CircuitId`` -> MultiViewer ``circuitKey`` where name matching is
#: not enough (or must be pinned): Mugello is 146 at MultiViewer (149 is the
#: F1 live-timing key, which MultiViewer uses for Jeddah); Singapore's entry
#: carries the country as its name; Portimao is "Algarve International Circuit".
OVERRIDES: dict[str, int] = {
    "marina_bay": 61,
    "mugello": 146,
    "portimao": 147,
}

#: Circuit ids MultiViewer is known not to list (reported, never an error).
KNOWN_UNLISTED: frozenset[str] = frozenset({"madring", "sepang"})


def fetch_index() -> dict[str, dict]:
    request = urllib.request.Request(INDEX_URL, headers={"User-Agent": "tif1-vendor/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        msg = f"unexpected index payload: {type(payload).__name__}"
        raise SystemExit(msg)
    return payload


def schedule_circuits(start: int, end: int) -> dict[str, dict]:
    """``circuit_id -> {name, location, country, years}`` over the packaged schedules."""
    circuits: dict[str, dict] = {}
    for year in range(start, end + 1):
        path = SCHEDULES_DIR / f"schedule_{year}.json"
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            races = json.load(handle).get("races", [])
        for race in races:
            circuit = race.get("Circuit") or {}
            location = circuit.get("Location") or {}
            circuit_id = circuit.get("circuitId")
            if not circuit_id:
                continue
            entry = circuits.setdefault(
                circuit_id,
                {
                    "name": circuit.get("circuitName", ""),
                    "location": location.get("locality", ""),
                    "country": location.get("country", ""),
                    "years": set(),
                },
            )
            entry["years"].add(year)
    return circuits


def build_table(index: dict[str, dict], circuits: dict[str, dict]) -> tuple[dict, list[str]]:
    by_key = {int(entry["circuitKey"]): entry for entry in index.values()}
    table: dict[str, dict] = {}
    unmapped: list[str] = []
    for circuit_id in sorted(circuits):
        meta = circuits[circuit_id]
        key = OVERRIDES.get(circuit_id)
        if key is None:
            key = match_circuit_index(
                index,
                circuit_id=circuit_id,
                circuit_name=meta["name"],
                country=meta["country"],
                location=meta["location"],
            )
        entry = by_key.get(key) if key is not None else None
        if entry is None:
            unmapped.append(circuit_id)
            continue
        table[circuit_id] = {
            "circuitKey": int(entry["circuitKey"]),
            "countryKey": int(entry["countryKey"]),
            "iocCountryCode": str(entry.get("iocCountryCode", "")),
            "name": str(entry.get("name", "")),
            "country": str(entry.get("country", "")),
            "years": sorted(int(year) for year in entry.get("years", [])),
        }
    return table, unmapped


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=int, default=2018)
    parser.add_argument("--end", type=int, default=2100)
    parser.add_argument(
        "--check", action="store_true", help="compare with the packaged file, write nothing"
    )
    args = parser.parse_args()

    index = fetch_index()
    circuits = schedule_circuits(args.start, args.end)
    table, unmapped = build_table(index, circuits)

    unused_overrides = sorted(set(OVERRIDES) - set(circuits))
    if unused_overrides:
        print(f"overrides without a schedule circuit: {', '.join(unused_overrides)}")
    listed_keys = {entry["circuitKey"] for entry in table.values()}
    unused_index = sorted(
        f"{key} {entry['name']} ({entry['country']})"
        for key, entry in index.items()
        if int(entry["circuitKey"]) not in listed_keys
    )
    print(f"mapped {len(table)} of {len(circuits)} circuit ids to {len(listed_keys)} keys")
    if unused_index:
        print("MultiViewer entries not used by any schedule circuit:")
        for line in unused_index:
            print(f"  {line}")
    if unmapped:
        print("schedule circuit ids without a MultiViewer entry:")
        for circuit_id in unmapped:
            meta = circuits[circuit_id]
            tag = "known" if circuit_id in KNOWN_UNLISTED else "UNEXPECTED"
            print(f"  {circuit_id}: {meta['name']}, {meta['location']}, {meta['country']} [{tag}]")

    text = json.dumps(table, indent=2, ensure_ascii=False) + "\n"
    if args.check:
        current = OUT_PATH.read_text(encoding="utf-8") if OUT_PATH.exists() else ""
        if current != text:
            print(f"{OUT_PATH} is out of date")
            return 1
        print(f"{OUT_PATH} is up to date")
        return 0
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(text, encoding="utf-8")
    print(f"wrote {OUT_PATH}")
    return 1 if any(circuit_id not in KNOWN_UNLISTED for circuit_id in unmapped) else 0


if __name__ == "__main__":
    raise SystemExit(main())
