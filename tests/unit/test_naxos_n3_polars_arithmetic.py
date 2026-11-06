"""Naxos N3: the polars telemetry arithmetic without numpy and without query plans.

Each eager kernel is checked against the path it replaces, value for value
(``repr`` of every float, so 1 ulp and the sign of a zero count): the numpy
distance arithmetic, numpy's searches and fills, polars' ``interpolate`` /
``forward_fill`` / ``backward_fill`` and the resampler's query plan. The
plan's bin means are compared on frames whose bins hold at most two rows:
their sum does not depend on the order of the rows, which the plan does not
fix (see ``_bin_mean``).
"""

from __future__ import annotations

import math
import random
from typing import Any

import numpy as np
import polars as pl
import pytest

from tif1 import models_polars as mp
from tif1.models_polars import PolarsTelemetry

_T0 = 1_741_240_800_000_000_000  # 2025-03-06 06:00 UTC in ns
_SESSION_T0 = 3_600_000_000_000  # 1 h into the session


def _values(series: pl.Series) -> list[str]:
    return [repr(value) for value in series.to_list()]


def assert_frames_identical(result: pl.DataFrame, reference: pl.DataFrame) -> None:
    assert result.columns == reference.columns
    assert result.dtypes == reference.dtypes
    assert result.height == reference.height
    for column in reference.columns:
        assert _values(result[column]) == _values(reference[column]), column


def build_telemetry(
    rng: random.Random,
    n: int,
    *,
    max_gap_ms: int = 300,
    duplicates: bool = False,
    gaps: bool = False,
    zeros: bool = False,
) -> PolarsTelemetry:
    """A lap-like telemetry frame with tif1's schema, optionally with null/NaN gaps."""
    offsets = [0]
    for _ in range(n - 1):
        step = 0 if duplicates and rng.random() < 0.05 else rng.randint(1, max_gap_ms) * 1_000_000
        offsets.append(offsets[-1] + step)

    def floats(scale: float) -> list[float | None]:
        values: list[float | None] = [rng.uniform(-scale, scale) for _ in range(n)]
        if zeros:
            for _ in range(max(1, n // 10)):
                values[rng.randrange(n)] = rng.choice([0.0, -0.0])
        if gaps:
            for _ in range(max(1, n // 12)):
                values[rng.randrange(n)] = None
            for _ in range(max(1, n // 15)):
                values[rng.randrange(n)] = math.nan
        return values

    gears: list[int | None] = [rng.randint(1, 8) for _ in range(n)]
    ahead: list[str | None] = [rng.choice(["1", "16", "81"]) for _ in range(n)]
    if gaps:
        gears[0] = None
        ahead[rng.randrange(n)] = None
    frame = pl.DataFrame(
        {
            "Date": pl.Series([_T0 + o for o in offsets], dtype=pl.Int64).cast(pl.Datetime("ns")),
            "SessionTime": pl.Series([_SESSION_T0 + o for o in offsets]).cast(pl.Duration("ns")),
            "DriverAhead": pl.Series(ahead, dtype=pl.String),
            "Time": pl.Series(offsets, dtype=pl.Int64).cast(pl.Duration("ns")),
            "RPM": pl.Series(floats(12_000.0), dtype=pl.Float64),
            "Speed": pl.Series(floats(340.0), dtype=pl.Float64),
            "nGear": pl.Series(gears, dtype=pl.Int64),
            "Brake": pl.Series([rng.random() < 0.2 for _ in range(n)], dtype=pl.Boolean),
            "Source": pl.Series([rng.choice(["car", "pos"]) for _ in range(n)]).cast(
                pl.Categorical
            ),
            "X": pl.Series(floats(5_000.0), dtype=pl.Float64),
            "Driver": pl.Series(["VER"] * n, dtype=pl.String),
            "LapNumber": pl.Series([1 + i * 3 // n for i in range(n)], dtype=pl.Int64),
        }
    )
    return PolarsTelemetry(frame, driver="VER")


def _plan_resample(frame: pl.DataFrame, step: int, monkeypatch: pytest.MonkeyPatch) -> Any:
    with monkeypatch.context() as patch:
        patch.setattr(mp, "_resample_eager", lambda *_args: None)
        return mp._resample_by_rule(frame, step)


# --------------------------------------------------------------------------- #
# Distances
# --------------------------------------------------------------------------- #


def _numpy_differential(tel: pl.DataFrame) -> np.ndarray:
    """The numpy arithmetic ``_differential_distance_values`` replaced (control d28ce70)."""
    speed = tel.get_column("Speed").cast(pl.Float64, strict=False).to_numpy()
    speed_ms = np.where(np.isnan(speed), 0.0, speed) / 3.6
    if "Time" not in tel.columns:
        return speed_ms * 0.0
    nanoseconds = mp._series_ns(tel.get_column("Time")).cast(pl.Float64).to_numpy()
    seconds = nanoseconds / float(1_000_000_000)
    seconds[np.isnan(seconds)] = 0.0
    delta = np.zeros(seconds.size)
    np.maximum(np.diff(seconds), 0.0, out=delta[1:])
    return speed_ms * delta


@pytest.mark.parametrize("seed", range(6))
def test_distances_match_the_numpy_arithmetic(seed: int) -> None:
    rng = random.Random(seed)
    tel = build_telemetry(rng, rng.randint(1, 900), gaps=seed % 2 == 1, zeros=True)
    if seed == 4:  # a clock that steps back and has a gap
        times = tel["Time"].cast(pl.Int64).to_list()
        times[len(times) // 2] = None
        if len(times) > 3:
            times[1], times[2] = times[2], times[1]
        tel = tel._rewrap(tel.with_columns(pl.Series("Time", times).cast(pl.Duration("ns"))))
    if seed == 5:
        tel = tel._rewrap(tel.drop("Time"))
    reference = _numpy_differential(tel)
    differential = tel.calculate_differential_distance()
    assert differential.name == "DifferentialDistance"
    assert _values(differential) == [repr(v) for v in reference.tolist()]
    distance = tel.integrate_distance()
    assert distance.name == "Distance"
    assert distance.dtype == pl.Float64
    assert _values(distance) == [repr(v) for v in np.cumsum(reference).tolist()]
    added = tel.add_distance().add_differential_distance().add_relative_distance()
    last = np.cumsum(reference)[-1]
    expected_relative = np.cumsum(reference) / np.full(reference.size, last)
    assert _values(added["RelativeDistance"]) == [repr(v) for v in expected_relative.tolist()]


def test_cumulative_sum_lists_and_numpy_agree(monkeypatch: pytest.MonkeyPatch) -> None:
    rng = random.Random(3)
    values = pl.Series("d", [rng.uniform(0.0, 90.0) for _ in range(5_000)] + [math.nan, 1.0])
    with monkeypatch.context() as patch:
        patch.setattr(mp, "_numpy_kernels", lambda _rows: False)
        lists = mp._cumulative_sum(values)
    with monkeypatch.context() as patch:
        patch.setattr(mp, "_numpy_kernels", lambda _rows: True)
        numpy = mp._cumulative_sum(values)
    assert lists.dtype == numpy.dtype == pl.Float64
    assert _values(lists) == _values(numpy)


def test_numpy_kernels_switch() -> None:
    assert mp._numpy_kernels(mp._LIST_KERNEL_MAX_ROWS + 1) is True
    assert mp._numpy_kernels(100) is False  # a lap never needs numpy
    # numpy.ma is loaded in this process (pandas imports it): big frames use numpy.
    assert mp._numpy_kernels(mp._LIST_KERNEL_NUMPY_LOADED_ROWS + 1) is True


# --------------------------------------------------------------------------- #
# Fills
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("seed", range(8))
def test_linear_fill_matches_polars_interpolate(seed: int) -> None:
    rng = random.Random(seed)
    length = rng.randint(1, 3_000)
    positions = sorted(rng.sample(range(length), rng.randint(1, length)))
    if seed == 0:
        positions = [length - 1]
    present = set(positions)
    values = [rng.uniform(-1e6, 1e6) for _ in positions]
    it = iter(values)
    full = pl.Series("v", [next(it) if i in present else None for i in range(length)], pl.Float64)
    reference = full.interpolate().forward_fill().backward_fill()
    plan = mp._linear_fill_plan(positions, length)
    result = mp._linear_fill(pl.Series("v", values, dtype=pl.Float64), plan)
    assert _values(result) == _values(reference)


@pytest.mark.parametrize("seed", range(6))
def test_fill_missing_matches_the_expression_path(
    seed: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    rng = random.Random(seed)
    tel = build_telemetry(rng, rng.randint(2, 600), gaps=True)
    if seed == 3:  # a channel without any value: all NaN stays NaN
        tel = tel._rewrap(tel.with_columns(pl.lit(math.nan).alias("X")))
    if seed == 4:  # all null stays null
        tel = tel._rewrap(tel.with_columns(pl.lit(None, dtype=pl.Float64).alias("X")))
    result = tel.fill_missing()
    with monkeypatch.context() as patch:
        patch.setattr(mp, "_interpolated", lambda *_args: None)
        patch.setattr(mp, "_numpy_kernels", lambda _rows: True)
        reference = tel.fill_missing()
    assert_frames_identical(result, reference)
    assert isinstance(result, PolarsTelemetry)


# --------------------------------------------------------------------------- #
# Resampling
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("seed", "rule", "gaps"),
    [(0, "100ms", False), (1, "100ms", True), (2, "250ms", False), (3, "50ms", True)],
)
def test_resample_matches_the_query_plan(
    seed: int, rule: str, gaps: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bins of at most two rows: the plan's means do not depend on its row order."""
    rng = random.Random(seed)
    step = mp._rule_to_ns(rule)
    # Samples at least half a bin apart (and some long gaps): never three in one bin.
    tel = build_telemetry(rng, 400, max_gap_ms=step // 1_000_000 * 6, gaps=gaps, zeros=True)
    offsets = tel["Time"].cast(pl.Int64).to_list()
    spaced = [offsets[0]]
    for value in offsets[1:]:
        spaced.append(max(value, spaced[-1] + step // 2 + 1))
    tel = tel._rewrap(
        tel.with_columns(
            pl.Series("Date", [_T0 + o for o in spaced]).cast(pl.Datetime("ns")),
            pl.Series("SessionTime", [_SESSION_T0 + o for o in spaced]).cast(pl.Duration("ns")),
            pl.Series("Time", spaced).cast(pl.Duration("ns")),
        )
    )
    eager = mp._resample_eager(tel, "Date", step)
    assert eager is not None
    assert_frames_identical(eager, _plan_resample(tel, step, monkeypatch))
    resampled = tel.resample_channels(rule=rule)
    assert isinstance(resampled, PolarsTelemetry)
    assert_frames_identical(resampled, eager)


def test_resample_held_columns_take_the_last_of_equal_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ms = 1_000_000
    tel = PolarsTelemetry(
        pl.DataFrame(
            {
                "Date": pl.Series([_T0, _T0 + 100 * ms, _T0 + 100 * ms, _T0 + 350 * ms]).cast(
                    pl.Datetime("ns")
                ),
                "nGear": pl.Series([None, 3, 4, None], dtype=pl.Int64),
                "Status": pl.Series(["a", None, "c", "d"]).cast(pl.Categorical),
                "Speed": [1.0, 2.0, 4.0, 8.0],
            }
        )
    )
    eager = mp._resample_eager(tel, "Date", 100 * ms)
    assert eager is not None
    # Bin starts 0, 100, 200, 300 ms: rows 0, 2, 2, 2 are in force (row 3 is at 350 ms);
    # the leading null takes the next bin's value.
    assert eager["nGear"].to_list() == [4, 4, 4, 4]
    assert eager["Status"].to_list() == ["a", "c", "c", "c"]
    assert eager["Speed"].to_list() == [1.0, 3.0, 5.5, 8.0]  # bin 2 interpolated
    assert_frames_identical(eager, _plan_resample(tel, 100 * ms, monkeypatch))


def test_bin_mean() -> None:
    values = [1.0, None, math.nan, 2.0, -0.0]
    assert mp._bin_mean(values, 0, 1) == 1.0
    assert mp._bin_mean(values, 1, 2) is None
    assert mp._bin_mean(values, 2, 3) is None  # NaN is missing
    assert repr(mp._bin_mean(values, 4, 5)) == "-0.0"  # a one-row bin keeps its value
    assert mp._bin_mean(values, 1, 3) is None
    assert mp._bin_mean(values, 0, 4) == 1.5  # null and NaN skipped
    assert repr(mp._bin_mean([-0.0, -0.0], 0, 2)) == "0.0"  # the sum starts at 0.0
    # Kahan: the small values a plain sum loses next to 1.0 are carried.
    tiny = [1.0] + [1e-16] * 10
    assert mp._bin_mean(tiny, 0, 11) == (1.0 + 1e-15) / 11
    plain = 0.0
    for value in tiny:  # (``sum`` compensates floats itself from Python 3.12)
        plain += value
    assert mp._bin_mean(tiny, 0, 11) != plain / 11
    total = compensation = 0.0
    sample = [random.Random(5).uniform(-1e3, 1e4) for _ in range(40)]
    for value in sample:
        adjusted = value - compensation
        running = total + adjusted
        compensation = (running - total) - adjusted
        total = running
    assert mp._bin_mean(sample, 0, 40) == total / 40


def test_resample_eager_declines_rare_inputs() -> None:
    rng = random.Random(1)
    tel = build_telemetry(rng, 50)
    step = mp._rule_to_ns("100ms")
    unsorted = tel._rewrap(tel.reverse())
    assert mp._resample_eager(unsorted, "Date", step) is None
    with_null = tel._rewrap(
        tel.with_columns(pl.Series("Date", [None] * 50, dtype=pl.Datetime("ns")))
    )
    assert mp._resample_eager(with_null, "Date", step) is None
    micro = tel._rewrap(tel.with_columns(pl.col("Time").cast(pl.Duration("us"))))
    assert mp._resample_eager(micro, "Date", step) is None
    assert mp._resample_eager(tel, "Date", 1) is None  # too many bins
    # The plan answers them, as before.
    assert unsorted.resample_channels(rule="100ms").height > 0


# --------------------------------------------------------------------------- #
# Searches, slices and merges
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("seed", range(4))
def test_nearest_rows_lists_and_numpy_agree(seed: int, monkeypatch: pytest.MonkeyPatch) -> None:
    rng = random.Random(seed)
    keys = sorted(rng.randrange(0, 10_000) for _ in range(rng.randint(1, 400)))
    clocks = sorted(rng.randrange(-500, 10_500) for _ in range(rng.randint(1, 400)))
    if seed == 0:
        clocks = [keys[0], keys[-1], (keys[0] + keys[-1]) // 2]  # exact matches and a midpoint
    key_series = pl.Series(keys, dtype=pl.Int64)
    clock_series = pl.Series(clocks, dtype=pl.Int64)
    with monkeypatch.context() as patch:
        patch.setattr(mp, "_numpy_kernels", lambda _rows: False)
        lists = mp._nearest_rows(clock_series, key_series)
    with monkeypatch.context() as patch:
        patch.setattr(mp, "_numpy_kernels", lambda _rows: True)
        numpy = mp._nearest_rows(clock_series, key_series)
    assert isinstance(lists, pl.Series)
    assert lists.to_list() == numpy.tolist()


@pytest.mark.parametrize("seed", range(4))
def test_merge_channels_lists_and_numpy_agree(seed: int, monkeypatch: pytest.MonkeyPatch) -> None:
    rng = random.Random(seed)
    left = build_telemetry(rng, rng.randint(5, 500), duplicates=True)
    right = build_telemetry(rng, rng.randint(5, 500), duplicates=True, gaps=True)
    with monkeypatch.context() as patch:
        patch.setattr(mp, "_numpy_kernels", lambda _rows: True)
        reference = left.merge_channels(right)
    assert_frames_identical(left.merge_channels(right), reference)


def test_held_lists_and_numpy_agree(monkeypatch: pytest.MonkeyPatch) -> None:
    rng = random.Random(2)
    values = [None if rng.random() < 0.3 else rng.randint(1, 8) for _ in range(500)]
    values[0] = None
    series = pl.Series("nGear", values, dtype=pl.Int64)
    floats = pl.Series("x", [math.nan, 1.0, None, math.nan, 2.0, None], dtype=pl.Float64)
    for data, has_nan in ((series, False), (floats, True)):
        with monkeypatch.context() as patch:
            patch.setattr(mp, "_numpy_kernels", lambda _rows: True)
            reference = mp._held(data, has_nan)
        assert _values(mp._held(data, has_nan)) == _values(reference)


@pytest.mark.parametrize(
    ("pad", "side", "interpolate"),
    [(0, "both", False), (2, "both", False), (3, "before", False), (1, "after", True)],
)
def test_slices(pad: int, side: str, interpolate: bool) -> None:
    rng = random.Random(7)
    tel = build_telemetry(rng, 300)
    session_time = tel["SessionTime"].cast(pl.Int64).to_list()
    start = session_time[40] + 1  # between samples: the edges are interpolated
    end = session_time[200] - 1
    result = tel.slice_by_time(
        start / 1e9, end / 1e9, pad=pad, pad_side=side, interpolate_edges=interpolate
    )
    times = result["SessionTime"].cast(pl.Int64).to_list()
    inside = [t for t in session_time if start <= t <= end]
    before = pad if side in ("both", "before") else 0
    after = pad if side in ("both", "after") else 0
    assert len(times) == len(inside) + before + after + (2 if interpolate else 0)
    assert result["Time"].cast(pl.Int64)[before + (1 if interpolate else 0)] == inside[0] - start


def test_slice_by_mask_pads_inside_the_frame() -> None:
    tel = build_telemetry(random.Random(4), 10)
    mask = [False, False, True, False, True, False, False, False, False, False]
    padded = tel.slice_by_mask(mask, pad=3)
    assert padded["Time"].to_list() == tel["Time"].to_list()[0:8]
    assert tel.slice_by_mask(pl.Series([None] * 10, dtype=pl.Boolean)).height == 0


def test_lap_numbers_and_first_non_zero_time() -> None:
    tel = build_telemetry(random.Random(4), 30)
    assert tel._get_lap_numbers() == [1, 2, 3]
    assert tel.get_first_non_zero_time_index() == 1
    mask = tel._lap_number_mask([2, 3])
    assert mask.to_list() == [n in (2, 3) for n in tel["LapNumber"].to_list()]
    floats = tel._rewrap(tel.with_columns(pl.col("LapNumber").cast(pl.Float64)))
    many = floats._lap_number_mask([1, 2, 3, 4, 5, 6])
    assert many.to_list() == [True] * 30
