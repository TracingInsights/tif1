"""Property-based tests for the Ergast tier: parsers, paging, slugs, page merging."""

from __future__ import annotations

import datetime

from hypothesis import given, settings
from hypothesis import strategies as st

from tif1.ergast import structure
from tif1.ergast.jolpica import merge_pages, merge_split_laps
from tif1.ergast.router import apply_paging
from tif1.ergast.stats import race_slug


@given(
    hours=st.integers(min_value=0, max_value=23),
    minutes=st.integers(min_value=0, max_value=59),
    seconds=st.integers(min_value=0, max_value=59),
    millis=st.integers(min_value=0, max_value=999),
)
@settings(max_examples=200)
def test_timedelta_parser_round_trips(hours, minutes, seconds, millis):
    text = (
        f"{hours}:{minutes:02d}:{seconds:02d}.{millis:03d}"
        if hours
        else f"{minutes}:{seconds:02d}.{millis:03d}"
    )
    parsed = structure.timedelta_from_ergast(text)
    assert parsed == datetime.timedelta(
        hours=hours, minutes=minutes, seconds=seconds, milliseconds=millis
    )
    assert structure.timedelta_from_ergast("+" + text) == parsed
    assert structure.timedelta_from_ergast("-" + text) == -parsed


@given(st.integers(min_value=-(10**9), max_value=10**9))
def test_save_int_round_trips_integers(value):
    assert structure.save_int(str(value)) == value


@given(st.text(max_size=12))
def test_save_int_and_float_never_raise(text):
    assert isinstance(structure.save_int(text), int)
    assert isinstance(structure.save_float(text), float)


@given(st.text(alphabet=st.characters(blacklist_categories=("Cs",)), min_size=1, max_size=40))
def test_race_slug_is_idempotent_and_lowercase(name):
    slug = race_slug(name)
    assert race_slug(slug) == slug
    assert " " not in slug
    assert slug == slug.lower()


def _results_payload(counts: list[int]) -> dict:
    races = [
        {
            "season": "2024",
            "round": str(index + 1),
            "Results": [{"position": str(position + 1)} for position in range(count)],
        }
        for index, count in enumerate(counts)
    ]
    total = sum(counts)
    return {
        "MRData": {
            "limit": str(total),
            "offset": "0",
            "total": str(total),
            "RaceTable": {"season": "2024", "Races": races},
        }
    }


@given(
    counts=st.lists(st.integers(min_value=0, max_value=25), min_size=1, max_size=6),
    limit=st.integers(min_value=1, max_value=40),
    offset=st.integers(min_value=0, max_value=60),
)
@settings(max_examples=150)
def test_paging_partitions_leaf_rows(counts, limit, offset):
    payload = _results_payload(counts)
    total = sum(counts)
    page = apply_paging(payload, "RaceTable", limit=limit, offset=offset)
    rows = [row for race in page["MRData"]["RaceTable"]["Races"] for row in race["Results"]]
    expected = [row for race in payload["MRData"]["RaceTable"]["Races"] for row in race["Results"]][
        offset : offset + limit
    ]
    assert rows == expected
    assert int(page["MRData"]["total"]) == total
    assert int(page["MRData"]["limit"]) == limit
    assert int(page["MRData"]["offset"]) == offset
    assert all(race["Results"] for race in page["MRData"]["RaceTable"]["Races"])


@given(
    counts=st.lists(st.integers(min_value=0, max_value=25), min_size=1, max_size=6),
    page_size=st.integers(min_value=1, max_value=30),
)
@settings(max_examples=100)
def test_consecutive_pages_cover_every_row_once(counts, page_size):
    payload = _results_payload(counts)
    total = sum(counts)
    seen: list[dict] = []
    offset = 0
    while offset < total:
        page = apply_paging(payload, "RaceTable", limit=page_size, offset=offset)
        seen.extend(row for race in page["MRData"]["RaceTable"]["Races"] for row in race["Results"])
        offset += page_size
    expected = [row for race in payload["MRData"]["RaceTable"]["Races"] for row in race["Results"]]
    assert seen == expected


@given(
    counts=st.lists(st.integers(min_value=0, max_value=12), min_size=1, max_size=5),
    page_size=st.integers(min_value=1, max_value=10),
)
@settings(max_examples=100)
def test_merging_pages_restores_the_complete_payload(counts, page_size):
    payload = _results_payload(counts)
    total = sum(counts)
    pages = [
        apply_paging(payload, "RaceTable", limit=page_size, offset=offset)
        for offset in range(0, max(total, 1), page_size)
    ]
    merged = merge_pages(pages)
    rows = [row for race in merged["MRData"]["RaceTable"]["Races"] for row in race["Results"]]
    expected = [row for race in payload["MRData"]["RaceTable"]["Races"] for row in race["Results"]]
    assert rows == expected
    assert int(merged["MRData"]["limit"]) == total


@given(
    laps=st.lists(
        st.tuples(st.integers(min_value=1, max_value=8), st.integers(min_value=1, max_value=4)),
        min_size=0,
        max_size=20,
    )
)
def test_merge_split_laps_keeps_every_timing_in_order(laps):
    entries = [
        {
            "number": str(number),
            "Timings": [{"driverId": f"{number}-{index}-{i}"} for i in range(count)],
        }
        for index, (number, count) in enumerate(laps)
    ]
    merged = merge_split_laps(entries)
    assert [lap["number"] for lap in merged] == list(dict.fromkeys(str(n) for n, _ in laps))
    flat_before = [t["driverId"] for lap in entries for t in lap["Timings"]]
    flat_after = [t["driverId"] for lap in merged for t in lap["Timings"]]
    assert sorted(flat_before) == sorted(flat_after)
    for lap in merged:
        ids = [t["driverId"] for t in lap["Timings"]]
        assert ids == [i for i in flat_before if i.startswith(f"{lap['number']}-")]
