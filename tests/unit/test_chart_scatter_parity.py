"""The driver lap-time scatter draws seaborn's figure without seaborn (Corinth C6).

``tif1.charts.lap_times._scatter_by_compound`` makes the matplotlib calls that
``sns.scatterplot(data=laps, x="LapNumber", y="LapTime", hue="Compound",
palette=..., s=80, linewidth=0, legend="auto")`` makes (seaborn 0.13), so
``plot_driver_laptimes`` no longer imports seaborn and scipy.stats (~0.85 s).
These tests draw both on the same frames and compare every artist, the axis
state and the rendered pixels, with and without timple's timedelta converter.
"""

from __future__ import annotations

import datetime as dt
import hashlib

import matplotlib as mpl

mpl.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.units as munits
import numpy as np
import pandas as pd
import pytest

from tif1.charts import lap_times

sns = pytest.importorskip("seaborn")

PALETTE = {
    "SOFT": "#da291c",
    "MEDIUM": "#ffd12e",
    "HARD": "#f0f0ec",
    "INTERMEDIATE": "#43b02a",
    "WET": "#0067ad",
    "UNKNOWN": "#00ffff",
    "TEST_UNKNOWN": "#434649",
}


def _laps(n: int = 30, seed: int = 6) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "Driver": rng.choice(["VER", "LEC", "NOR"], n),
            "LapNumber": np.arange(1, n + 1, dtype="float64"),
            "LapTime": pd.to_timedelta(rng.uniform(91, 99, n).round(3), unit="s"),
            "Compound": rng.choice(["MEDIUM", "HARD", "SOFT"], n),
        }
    )


def _with_gaps() -> pd.DataFrame:
    laps = _laps(40, seed=7)
    laps.loc[[0, 5], "LapTime"] = pd.NaT
    laps.loc[[3, 9], "Compound"] = None
    laps.loc[12, "LapNumber"] = np.nan
    return laps


def _categorical() -> pd.DataFrame:
    laps = _laps(20, seed=8)
    laps["Compound"] = pd.Categorical(
        laps["Compound"], categories=["SOFT", "MEDIUM", "HARD", "INTERMEDIATE"]
    )
    return laps


def _one_compound() -> pd.DataFrame:
    laps = _laps(12, seed=9)
    laps["Compound"] = "SOFT"
    return laps


def _no_compound() -> pd.DataFrame:
    laps = _laps(8, seed=10)
    laps["Compound"] = None
    return laps


FRAMES = {
    "plain": _laps,
    "gaps": _with_gaps,
    "categorical": _categorical,
    "one_compound": _one_compound,
    "no_compound": _no_compound,
    "empty": lambda: _laps().iloc[0:0],
    "all_nat": lambda: _laps().assign(
        LapTime=pd.Series(pd.NaT, index=range(30), dtype="timedelta64[ns]")
    ),
}


def _hash(values) -> str:
    array = np.asarray(values)
    if array.dtype == object:
        return repr(array.tolist())
    return f"{array.dtype}{array.shape}" + hashlib.sha1(np.ascontiguousarray(array)).hexdigest()


def _digest(fig) -> dict:
    ax = fig.axes[0]
    legend = ax.get_legend()
    record = {
        "children": [type(artist).__name__ for artist in ax.get_children()],
        "lines": [
            (
                _hash(line.get_xdata()),
                _hash(line.get_ydata()),
                str(line.get_color()),
                line.get_label(),
                str(line.get_linestyle()),
                str(line.get_marker()),
                float(line.get_markersize()),
                str(line.get_markerfacecolor()),
                str(line.get_markeredgecolor()),
                float(line.get_markeredgewidth()),
                line.get_visible(),
            )
            for line in ax.get_lines()
        ],
        "collections": [
            (
                type(c).__name__,
                _hash(c.get_offsets()),
                _hash(c.get_facecolors()),
                _hash(c.get_edgecolors()),
                _hash(c.get_sizes()),
                _hash(c.get_linewidths()),
                c.get_label(),
                c.get_zorder(),
            )
            for c in ax.collections
        ],
        "labels": [
            (label.get_text(), label.get_visible()) for label in (ax.xaxis.label, ax.yaxis.label)
        ],
        "limits": (ax.get_xlim(), ax.get_ylim(), tuple(ax.dataLim.bounds)),
        "inverted": (ax.xaxis_inverted(), ax.yaxis_inverted()),
        "units": [type(axis.get_converter()).__name__ for axis in (ax.xaxis, ax.yaxis)],
        "tickers": [
            type(t).__name__
            for axis in (ax.xaxis, ax.yaxis)
            for t in (axis.get_major_locator(), axis.get_major_formatter())
        ],
        "spines": {side: spine.get_visible() for side, spine in ax.spines.items()},
        "legend": None
        if legend is None
        else (
            legend.get_title().get_text(),
            [text.get_text() for text in legend.get_texts()],
            [type(handle).__name__ for handle in legend.legend_handles],
        ),
        "next_color": ax._get_lines.get_next_color(),
    }
    fig.canvas.draw()
    record["ticks"] = [t.get_text() for t in ax.get_yticklabels() + ax.get_xticklabels()]
    record["pixels"] = hashlib.sha1(bytes(fig.canvas.buffer_rgba())).hexdigest()
    return record


def _draw_seaborn(laps: pd.DataFrame) -> dict:
    fig, ax = plt.subplots(figsize=(4, 3), dpi=50)
    try:
        sns.scatterplot(
            data=laps,
            x="LapNumber",
            y="LapTime",
            ax=ax,
            hue="Compound",
            palette=PALETTE,
            s=80,
            linewidth=0,
            legend="auto",
        )
        sns.despine(left=True, bottom=True)
        return _digest(fig)
    finally:
        plt.close(fig)


def _draw_tif1(laps: pd.DataFrame) -> dict:
    fig, ax = plt.subplots(figsize=(4, 3), dpi=50)
    try:
        lap_times._scatter_by_compound(ax, laps, PALETTE)
        lap_times._despine_all_sides()
        return _digest(fig)
    finally:
        plt.close(fig)


@pytest.fixture(params=["timple", "no_timedelta_converter"])
def converters(request, monkeypatch):
    """Run with timple's timedelta converter registered, and with none."""
    from tif1 import plotting

    plotting._enable_timple()
    if request.param == "no_timedelta_converter":
        for key in list(munits.registry):
            if isinstance(key, type) and issubclass(key, (dt.timedelta, np.timedelta64)):
                monkeypatch.delitem(munits.registry, key)
    return request.param


@pytest.mark.parametrize("frame", sorted(FRAMES))
@pytest.mark.filterwarnings("ignore:Ignoring `palette`:UserWarning")
def test_scatter_matches_seaborn(frame, converters):
    laps = FRAMES[frame]()
    assert _draw_tif1(laps.copy()) == _draw_seaborn(laps.copy())


@pytest.mark.parametrize("frame", ["empty", "no_compound"])
def test_no_compound_warns_like_seaborn(frame):
    laps = FRAMES[frame]()
    with pytest.warns(UserWarning, match="Ignoring `palette`") as seaborn_warnings:
        _draw_seaborn(laps.copy())
    with pytest.warns(UserWarning, match="Ignoring `palette`") as tif1_warnings:
        _draw_tif1(laps.copy())
    assert [str(w.message) for w in tif1_warnings] == [str(w.message) for w in seaborn_warnings]


def test_scatter_matches_seaborn_under_the_fastf1_theme(converters):
    from tif1 import plotting

    with mpl.rc_context():
        plotting.setup_mpl(mpl_timedelta_support=False, color_scheme="fastf1")
        laps = _with_gaps()
        assert _draw_tif1(laps.copy()) == _draw_seaborn(laps.copy())


def test_missing_palette_key_raises_like_seaborn():
    laps = _laps()
    laps.loc[2, "Compound"] = "HYPERSOFT"
    with pytest.raises(ValueError, match="missing keys") as seaborn_error:
        _draw_seaborn(laps)
    with pytest.raises(ValueError, match="missing keys") as tif1_error:
        _draw_tif1(laps)
    assert str(tif1_error.value) == str(seaborn_error.value)


@pytest.mark.parametrize(
    "vector",
    [
        pd.Series([1.0, 2.0, np.nan]),
        pd.Series([0, 1, 1]),
        pd.Series(["a", None, "b"]),
        pd.Series(pd.to_timedelta([1, 2], unit="s")),
        pd.Series(pd.to_datetime(["2025-01-01", "2025-01-02"])),
        pd.Series([np.nan, np.nan]),
        pd.Series(pd.Categorical(["b", "a"], categories=["b", "a", "c"])),
        pd.Series([1, "a"], dtype=object),
        pd.Series([], dtype=float),
    ],
)
def test_variable_type_and_order_match_seaborn(vector):
    from seaborn._base import categorical_order, variable_type

    assert lap_times._seaborn_variable_type(vector) == variable_type(vector)
    assert lap_times._seaborn_categorical_order(vector) == categorical_order(vector)
