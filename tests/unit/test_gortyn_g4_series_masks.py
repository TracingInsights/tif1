"""Gortyn G4: the polars selections run on Boolean Series masks, not numpy.

Every converted method is compared with the numpy-mask implementation it
replaced (kept here as the reference, built from the module's numpy helpers),
and with the polars expression where one states the semantics, on seeded
random laps frames with nulls, NaN, tied lap times, empty frames, float lap
numbers and many-chunk columns.
"""

from __future__ import annotations

import datetime as dt
import random
from collections.abc import Callable
from typing import Any

import numpy as np
import pytest

pl = pytest.importorskip("polars")
from polars.testing import assert_frame_equal  # noqa: E402

from tif1 import models_polars as mp  # noqa: E402
from tif1.exceptions import LapNotFoundError  # noqa: E402

SEEDS = range(40)
CODES = ["VER", "LEC", "NOR", "PIA", "HAM", None]
NUMBERS = {"VER": "1", "LEC": "16", "NOR": "4", "PIA": "81", "HAM": "44", None: None}
TEAMS = ["Red Bull Racing", "Ferrari", "McLaren", None]
COMPOUNDS = ["SOFT", "MEDIUM", "HARD", None]
STATUSES = ["1", "12", "4", "26", "671", "", None]


def _maybe(rng: random.Random, value: Any, p: float = 0.15) -> Any:
    return None if rng.random() < p else value


def _laps_frame(seed: int, *, float_laps: bool = False, chunks: int = 1) -> pl.DataFrame:
    rng = random.Random(seed)
    n = rng.choice([0, 1, 2, 7, 25, 60])
    codes = [rng.choice(CODES) for _ in range(n)]
    # Lap times on a coarse grid so ties are common.
    times = [_maybe(rng, dt.timedelta(seconds=90 + rng.randrange(6) * 0.5)) for _ in range(n)]
    pits = [_maybe(rng, dt.timedelta(seconds=rng.randrange(5000)), 0.8) for _ in range(n)]
    lap_numbers: list[Any] = [_maybe(rng, rng.randrange(1, 12)) for _ in range(n)]
    if float_laps:
        lap_numbers = [
            float("nan")
            if v is not None and rng.random() < 0.1
            else (None if v is None else v * 1.0)
            for v in lap_numbers
        ]
    data = {
        "Driver": pl.Series(codes, dtype=pl.String),
        "DriverNumber": pl.Series([NUMBERS[c] for c in codes], dtype=pl.String),
        "LapNumber": pl.Series(lap_numbers, dtype=pl.Float64 if float_laps else pl.Int64),
        "LapTime": pl.Series(times, dtype=pl.Duration("ns")),
        "IsPersonalBest": pl.Series([_maybe(rng, rng.random() < 0.4) for _ in range(n)]),
        "Deleted": pl.Series([_maybe(rng, rng.random() < 0.2) for _ in range(n)]),
        "IsAccurate": pl.Series([_maybe(rng, rng.random() < 0.7) for _ in range(n)]),
        "Compound": pl.Series([rng.choice(COMPOUNDS) for _ in range(n)], dtype=pl.String),
        "Team": pl.Series([rng.choice(TEAMS) for _ in range(n)], dtype=pl.String),
        "TrackStatus": pl.Series([rng.choice(STATUSES) for _ in range(n)], dtype=pl.String),
        "PitInTime": pl.Series(pits, dtype=pl.Duration("ns")),
        "PitOutTime": pl.Series(pits[::-1], dtype=pl.Duration("ns")),
    }
    for name in ("IsPersonalBest", "Deleted", "IsAccurate"):
        data[name] = data[name].cast(pl.Boolean)
    frame = pl.DataFrame(data)
    if chunks > 1 and n:
        step = max(1, n // chunks)
        parts = [frame.slice(i, step) for i in range(0, n, step)]
        frame = pl.concat(parts, rechunk=False)
    return frame


def _laps(frame: pl.DataFrame) -> mp.PolarsLaps:
    return mp.PolarsLaps._from_frame(frame)


FRAMES = [
    pytest.param(seed, float_laps, chunks, id=f"s{seed}-{'f' if float_laps else 'i'}-c{chunks}")
    for seed in SEEDS
    for float_laps, chunks in ((False, 1), (True, 1), (False, 5))
]


# --------------------------------------------------------------------------- #
# The numpy-mask implementations G4 replaced (reference)
# --------------------------------------------------------------------------- #


def _ref_text_test_values(series: pl.Series, test: Callable[[str], bool]) -> np.ndarray:
    """The removed ``models_polars._text_test_values``."""
    values = series.to_list()
    outcome = {value: value is not None and test(value) for value in set(values)}
    return np.fromiter(map(outcome.__getitem__, values), dtype=bool, count=len(values))


def _ref_filter(laps: mp.PolarsLaps, keep: np.ndarray) -> pl.DataFrame:
    return mp._filter_rows(laps, keep)


def _ref_driver_numbers(laps: mp.PolarsLaps, numbers: list[str]) -> np.ndarray:
    return mp._isin_values(laps.get_column("DriverNumber").cast(pl.String), numbers)


def _ref_pick_drivers(laps: mp.PolarsLaps, identifiers: list[Any]) -> pl.DataFrame:
    codes = [mp._driver_identifier(identifier) for identifier in identifiers]
    names = [code.upper() for code in codes if not code.isdigit()]
    numbers = [code for code in codes if code.isdigit()]
    keep = mp._isin_values(laps.get_column("Driver").cast(pl.String), names)
    if numbers:
        keep = keep | _ref_driver_numbers(laps, numbers)
    return _ref_filter(laps, keep)


def _ref_pick_quicklaps(laps: mp.PolarsLaps, threshold: float | None) -> pl.DataFrame:
    threshold = mp.PolarsLaps.QUICKLAP_THRESHOLD if threshold is None else threshold
    if laps.height == 0:
        return laps
    lap_time_ns = mp._ns_series(laps, "LapTime")
    best = lap_time_ns.min()
    if best is None:
        return laps
    return _ref_filter(laps, lap_time_ns.to_numpy() < best * threshold)


def _ref_fastest_position(laps: mp.PolarsLaps, rule: str) -> int | None:
    if laps.height == 0:
        return None
    lap_time = laps.get_column("LapTime")
    valid = mp._valid_values(lap_time)
    if rule == "personal_best":
        valid = valid & mp._mask_values(laps.get_column("IsPersonalBest"))
    elif rule == "not_deleted":
        valid = valid & ~mp._mask_values(laps.get_column("Deleted"))
    if valid.all():
        return int(lap_time.arg_min())
    positions = np.flatnonzero(valid)
    if positions.size == 0:
        return None
    times = lap_time.filter(pl.Series(valid))
    return int(positions[int(times.arg_min())])


def _ref_pick_laps(laps: mp.PolarsLaps, numbers: list[int]) -> pl.DataFrame:
    return _ref_filter(laps, np.isin(laps.get_column("LapNumber").to_numpy(), numbers))


def _ref_track_status(laps: mp.PolarsLaps, status: str, how: str) -> pl.DataFrame:
    values = laps.get_column("TrackStatus").cast(pl.String)
    if how == "equals":
        keep = mp._mask_values(values == status)
    elif how in ("contains", "excludes"):
        keep = _ref_text_test_values(values, lambda value: status in value)
    else:
        keep = _ref_text_test_values(values, lambda value: any(ch in value for ch in status))
    if how in ("excludes", "none"):
        keep = ~keep
    return _ref_filter(laps, keep)


def _ref_iterlaps_positions(laps: mp.PolarsLaps, required: list[str]) -> list[int]:
    if not laps.height:
        return []
    keep = mp._valid_values(laps.get_column(required[0]))
    for column in required[1:]:
        keep = keep & mp._valid_values(laps.get_column(column))
    return np.flatnonzero(keep).tolist()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "values",
    [
        [True, None, False, True, None],
        [None, None],
        [True, False],
        [],
        [None],
    ],
)
@pytest.mark.parametrize("fill", [False, True])
def test_filled_matches_fill_null(values, fill):
    mask = pl.Series(values, dtype=pl.Boolean)
    out = mp._filled(mask, fill)
    assert out.null_count() == 0
    assert out.to_list() == mask.fill_null(fill).to_list()


@pytest.mark.parametrize(
    "series",
    [
        pl.Series([1.0, float("nan"), None, float("inf"), -float("inf")]),
        pl.Series([1, None, 3]),
        pl.Series(["a", None, ""], dtype=pl.String),
        pl.Series(["a", None, "b"], dtype=pl.Categorical),
        pl.Series([dt.timedelta(1), None], dtype=pl.Duration("ns")),
        pl.Series([None, None], dtype=pl.Float64),
        pl.Series([], dtype=pl.Float64),
        pl.Series([[1], None, []], dtype=pl.List(pl.Int64)),
        pl.Series([True, None, False]),
    ],
    ids=lambda s: str(s.dtype),
)
def test_present_and_valid_series_match_numpy_helpers(series):
    assert mp._present_series(series).to_list() == mp._present_values(series).tolist()
    assert mp._valid_series(series).to_list() == mp._valid_values(series).tolist()


@pytest.mark.parametrize("wanted", [[], ["a"], ["a", "c"], ["a", "b", "c", "d", "e"], ["x"] * 6])
def test_isin_and_text_test_series_match_numpy_helpers(wanted):
    series = pl.Series(["a", None, "b", "c", "e", "a", ""], dtype=pl.String)
    assert mp._isin_series(series, wanted).to_list() == mp._isin_values(series, wanted).tolist()
    test = lambda value: value in wanted  # noqa: E731
    assert (
        mp._text_test_series(series, test).to_list() == _ref_text_test_values(series, test).tolist()
    )


def test_isin_series_numbers_on_float_column_match_np_isin():
    series = pl.Series([1.0, float("nan"), None, 5.0, 5.5, 7.0])
    for numbers in ([5], [1, 5, 7], [1, 2, 3, 5, 7]):
        expected = np.isin(series.to_numpy(), numbers).tolist()
        assert mp._isin_series(series, numbers).to_list() == expected


def test_first_true_and_true_positions():
    assert mp._first_true(pl.Series([None, False, True, True])) == 2
    assert mp._first_true(pl.Series([False, None], dtype=pl.Boolean)) is None
    assert mp._first_true(pl.Series([], dtype=pl.Boolean)) is None
    assert mp._true_positions(pl.Series([True, None, False, True])) == [0, 3]


# --------------------------------------------------------------------------- #
# Methods against the numpy implementation
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(("seed", "float_laps", "chunks"), FRAMES)
def test_selections_match_numpy_masks(seed, float_laps, chunks):
    frame = _laps_frame(seed, float_laps=float_laps, chunks=chunks)
    laps = _laps(frame)
    checks: list[tuple[str, Any, Any]] = [
        ("pick_driver", laps.pick_driver("VER"), _ref_pick_drivers(laps, ["VER"])),
        ("pick_driver_num", laps.pick_driver("16"), _ref_pick_drivers(laps, ["16"])),
        (
            "pick_drivers",
            laps.pick_drivers(["ver", "LEC"]),
            _ref_pick_drivers(laps, ["ver", "LEC"]),
        ),
        (
            "pick_drivers_6",
            laps.pick_drivers(["VER", "lec", "NOR", "PIA", "HAM", "44", 81]),
            _ref_pick_drivers(laps, ["VER", "lec", "NOR", "PIA", "HAM", "44", 81]),
        ),
        ("pick_drivers_none", laps.pick_drivers([]), _ref_pick_drivers(laps, [])),
        ("pick_quicklaps", laps.pick_quicklaps(), _ref_pick_quicklaps(laps, None)),
        ("pick_quicklaps_1", laps.pick_quicklaps(1.0), _ref_pick_quicklaps(laps, 1.0)),
        ("pick_quicklaps_int", laps.pick_quicklaps(2), _ref_pick_quicklaps(laps, 2)),
        (
            "pick_quicklaps_np",
            laps.pick_quicklaps(np.float64(1.01)),
            _ref_pick_quicklaps(laps, 1.01),
        ),
        ("pick_laps", laps.pick_laps([2, 5]), _ref_pick_laps(laps, [2, 5])),
        ("pick_laps_6", laps.pick_laps(range(1, 7)), _ref_pick_laps(laps, list(range(1, 7)))),
        ("pick_laps_int", laps.pick_laps(3), _ref_pick_laps(laps, [3])),
        (
            "pick_compounds",
            laps.pick_compounds(["soft", "HARD"]),
            frame.filter(pl.col("Compound").is_in(["SOFT", "HARD"]).fill_null(False)),
        ),
        (
            "pick_compounds_5",
            laps.pick_compounds(["SOFT", "HARD", "MEDIUM", "INTERMEDIATE", "WET"]),
            frame.filter(pl.col("Compound").is_not_null()),
        ),
        ("pick_tyre", laps.pick_tyre("medium"), frame.filter(pl.col("Compound") == "MEDIUM")),
        ("pick_team", laps.pick_team("McLaren"), frame.filter(pl.col("Team") == "McLaren")),
        (
            "pick_teams",
            laps.pick_teams(["McLaren", "Ferrari"]),
            frame.filter(pl.col("Team").is_in(["McLaren", "Ferrari"]).fill_null(False)),
        ),
        (
            "pick_wo_box",
            laps.pick_wo_box(),
            frame.filter(pl.col("PitInTime").is_null() & pl.col("PitOutTime").is_null()),
        ),
        (
            "pick_box_laps",
            laps.pick_box_laps(),
            frame.filter(pl.col("PitInTime").is_not_null() | pl.col("PitOutTime").is_not_null()),
        ),
        ("pick_box_in", laps.pick_box_laps("in"), frame.filter(pl.col("PitInTime").is_not_null())),
        (
            "pick_box_out",
            laps.pick_box_laps("out"),
            frame.filter(pl.col("PitOutTime").is_not_null()),
        ),
        (
            "pick_not_deleted",
            laps.pick_not_deleted(),
            frame.filter(~pl.col("Deleted").fill_null(False)),
        ),
        (
            "pick_accurate",
            laps.pick_accurate(),
            frame.filter(pl.col("IsAccurate").fill_null(False)),
        ),
    ]
    if not float_laps:
        checks += [
            ("pick_lap", laps.pick_lap(3), frame.filter(pl.col("LapNumber") == 3)),
            (
                "pick_laps_slice",
                laps.pick_laps(slice(3, 7)),
                frame.filter((pl.col("LapNumber") >= 3) & (pl.col("LapNumber") <= 7)),
            ),
            (
                "pick_laps_open_slice",
                laps.pick_laps(slice(None, None)),
                frame.filter(pl.col("LapNumber") >= 1),
            ),
        ]
    for status in ("1", "2", "26", "6", "14"):
        for how in ("equals", "contains", "excludes", "any", "none"):
            checks.append(
                (
                    f"pick_track_status[{status},{how}]",
                    laps.pick_track_status(status, how),
                    _ref_track_status(laps, status, how),
                )
            )
    for name, got, expected in checks:
        assert isinstance(got, mp.PolarsLaps), name
        try:
            assert_frame_equal(got, expected, check_exact=True)
        except AssertionError as error:
            raise AssertionError(f"{name}: {error}") from error


@pytest.mark.parametrize(("seed", "float_laps", "chunks"), FRAMES)
def test_fastest_lap_matches_numpy_masks(seed, float_laps, chunks):
    laps = _laps(_laps_frame(seed, float_laps=float_laps, chunks=chunks))
    for rule, method in (
        ("personal_best", lambda: laps.pick_fastest()),
        ("time", lambda: laps.pick_fastest(only_by_time=True)),
        ("not_deleted", lambda: laps._fastest_lap("not_deleted")),
    ):
        lap = method()
        expected = _ref_fastest_position(laps, rule)
        if expected is None:
            assert lap is None, rule
        else:
            assert lap is not None, rule
            assert lap.name == expected, rule
            assert_frame_equal(lap.frame, laps.slice(expected, 1))


def test_fastest_lap_float_times_skip_nan():
    laps = _laps(
        pl.DataFrame(
            {
                "LapTime": [float("nan"), 91.0, None, 90.5, 90.5],
                "IsPersonalBest": [True, True, True, False, True],
            }
        )
    )
    assert laps.pick_fastest().name == 4  # 90.5 personal best, NaN and null never count
    assert laps.pick_fastest(only_by_time=True).name == 3  # first of the tied minima
    assert _laps(pl.DataFrame({"LapTime": [float("nan"), None]})).pick_fastest() is None


def test_fastest_lap_without_flag_columns_uses_every_timed_lap():
    laps = _laps(pl.DataFrame({"LapTime": pl.Series([None, 92, 91, 91]).cast(pl.Duration("ns"))}))
    assert laps.pick_fastest().name == 2


@pytest.mark.parametrize(("seed", "float_laps", "chunks"), FRAMES)
def test_iterlaps_require_matches_numpy_masks(seed, float_laps, chunks):
    laps = _laps(_laps_frame(seed, float_laps=float_laps, chunks=chunks))
    for required in (
        ["LapTime"],
        ["LapTime", "PitInTime"],
        ["LapNumber"],
        ["IsPersonalBest", "Driver", "LapNumber"],
    ):
        got = [position for position, _ in laps.iterlaps(require=required)]
        assert got == _ref_iterlaps_positions(laps, required), required
    assert [p for p, _ in laps.iterlaps(require=["Missing"])] == []
    assert [p for p, _ in laps.iterlaps()] == list(range(laps.height))


# --------------------------------------------------------------------------- #
# Results and driver lookups
# --------------------------------------------------------------------------- #


def _ref_position_for(table: mp.PolarsSessionResults, identifier: Any) -> int | None:
    key = mp._driver_identifier(identifier).strip()
    if not key:
        return None
    matches = None
    upper, lower = key.upper(), key.lower()
    for column in table._IDENTIFIER_COLUMNS:
        if column not in table.columns:
            continue
        text = table.get_column(column).cast(pl.String)
        if column == "Abbreviation":
            hits = _ref_text_test_values(text, lambda value: value.upper() == upper)
        elif column == "DriverNumber":
            hits = mp._mask_values(text == key)
        else:
            hits = _ref_text_test_values(text, lambda value: value.lower() == lower)
        matches = hits if matches is None else matches | hits
    if matches is None:
        return None
    positions = np.flatnonzero(matches)
    if positions.size:
        return int(positions[0])
    number, separator, code = key.partition("/")
    if separator and number.isdigit() and code.strip():
        wanted = code.strip().upper()
        label = mp._mask_values(table.get_column("DriverNumber").cast(pl.String) == number)
        label = label & _ref_text_test_values(
            table.get_column("Abbreviation").cast(pl.String),
            lambda value: value.upper() == wanted,
        )
        positions = np.flatnonzero(label)
        if positions.size:
            return int(positions[0])
    return None


def test_results_position_for_matches_numpy_masks():
    table = mp.PolarsSessionResults._from_frame(
        pl.DataFrame(
            {
                "DriverNumber": ["2", "2", "1", None, "44", "7"],
                "Abbreviation": ["MOS", "Pal", "VER", "XXX", None, "ver"],
                "FullName": [
                    "Stirling Moss",
                    None,
                    "Max Verstappen",
                    "Nobody",
                    "Lewis Hamilton",
                    "",
                ],
                "BroadcastName": ["S MOSS", "P PAL", "M VERSTAPPEN", None, "L HAMILTON", "X"],
            }
        )
    )
    identifiers = [
        "VER", "ver", "1", 1, "2", "2/PAL", "2/pal", "2/XYZ", "3/MOS", "lewis hamilton",
        "L HAMILTON", "nobody", "", "  ", "44", "7", "Max Verstappen", "x", "unknown",
        {"Abbreviation": "MOS"},
    ]  # fmt: skip
    for identifier in identifiers:
        assert table._position_for(identifier) == _ref_position_for(table, identifier), identifier


def test_driver_get_lap_finds_the_first_matching_lap():
    laps = _laps(pl.DataFrame({"Driver": ["VER"] * 4, "LapNumber": pl.Series([1, None, 3, 3])}))
    driver = mp.PolarsDriver(pl.DataFrame({"Abbreviation": ["VER"]}), driver="VER")
    driver._laps = laps
    lap = driver.get_lap(3)
    assert lap.name == 2
    assert lap["LapNumber"] == 3
    with pytest.raises(LapNotFoundError):
        driver.get_lap(2)
    driver._laps = _laps(pl.DataFrame({"Driver": ["VER"]}))
    with pytest.raises(LapNotFoundError):
        driver.get_lap(1)


def test_distinct_present_with_nulls():
    assert mp._distinct_present(pl.Series([None, "VER", None, "VER"])) == (1, "VER")
    assert mp._distinct_present(pl.Series([None, "VER", "LEC"])) == (2, None)
    assert mp._distinct_present(pl.Series([None, None], dtype=pl.String)) == (0, None)
