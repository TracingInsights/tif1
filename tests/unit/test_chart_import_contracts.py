"""Contracts for the chart fast paths (Pergamon Q14).

* ``import tif1.charts`` must not import seaborn or scipy (~0.75 s of the
  first chart in a process); only the seaborn-backed lap-time charts and the
  G-G diagram import them, at call time.
* seaborn's named colormaps stay usable on every chart (they are registered
  on demand by :func:`tif1.charts._common.ensure_colormap`).
* :func:`tif1.charts._common.apply_common_filters` takes the lap frame once;
  its result must equal the step-by-step filter chain it replaced.
"""

from __future__ import annotations

import os
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

from tif1.charts import _common


def _run(code: str) -> str:
    env = dict(os.environ, MPLBACKEND="Agg")
    result = subprocess.run(
        [sys.executable, "-c", code], check=True, capture_output=True, text=True, env=env
    )
    return result.stdout.strip()


def test_importing_charts_does_not_import_seaborn_or_scipy():
    out = _run(
        "import sys, tif1.charts, tif1.plotting; "
        "print(sorted(m for m in ('seaborn', 'scipy', 'scipy.stats', 'scipy.spatial') "
        "if m in sys.modules))"
    )
    assert out == "[]"


def test_importing_plotting_does_not_import_matplotlib():
    # Corinth C6: the color/name/style lookups need no matplotlib (~0.4 s) and
    # an exact match needs no rapidfuzz; the helpers that draw import them.
    out = _run(
        "import sys, tif1.plotting as p; "
        "p.get_compound_mapping(None); p.team_color_mapping(2025); p.get_plot_style('dark'); "
        "print(sorted(m for m in ('matplotlib', 'matplotlib.pyplot', 'rapidfuzz', 'seaborn') "
        "if m in sys.modules))"
    )
    assert out == "[]"


def test_plotting_matplotlib_attributes_resolve_lazily():
    out = _run(
        "import sys, tif1.plotting as p; before = 'matplotlib' in sys.modules; "
        "import matplotlib, matplotlib.pyplot; "
        "print(before, p.mpl is matplotlib, p.plt is matplotlib.pyplot, "
        "p.cycler is matplotlib.cycler)"
    )
    assert out == "False True True True"


def test_setup_mpl_writes_rcparams_without_pyplot():
    out = _run(
        "import sys, tif1.plotting as p; p.setup_mpl(mpl_timedelta_support=False, "
        "color_scheme='fastf1'); import matplotlib; "
        "print(matplotlib.rcParams['figure.facecolor'], 'matplotlib.pyplot' in sys.modules)"
    )
    assert out == "#292625 False"


def test_seaborn_colormap_names_are_registered_on_demand():
    out = _run(
        "import sys; from tif1.charts import _common; "
        "_common.ensure_colormap('viridis'); before = 'seaborn' in sys.modules; "
        "_common.ensure_colormap('rocket'); from matplotlib import colormaps; "
        "print(before, 'seaborn' in sys.modules, 'rocket' in colormaps)"
    )
    assert out == "False True True"


def _chained_filters(
    laps_df,
    *,
    drivers=None,
    n_drivers=None,
    laps=None,
    include_deleted=False,
    include_pit_laps=False,
    finish_order=None,
):
    """The pre-Q14 implementation: one take per filter."""
    df = laps_df.copy()
    if laps is not None:
        df = df[df["LapNumber"].isin(laps)]
    if drivers is not None:
        df = df[df["Driver"].isin(drivers)]
    if n_drivers is not None and finish_order:
        df = df[df["Driver"].isin(finish_order[:n_drivers])]
    if not include_deleted and "Deleted" in df.columns:
        df = df[~df["Deleted"].fillna(False).astype(bool)]
    if not include_pit_laps:
        keep = pd.Series(True, index=df.index)
        if "PitInTime" in df.columns:
            keep &= df["PitInTime"].isna()
        if "PitOutTime" in df.columns:
            keep &= df["PitOutTime"].isna()
        df = df[keep]
    return df


def _lap_frame() -> pd.DataFrame:
    rng = np.random.default_rng(14)
    n = 120
    drivers = np.array(["VER", "LEC", "NOR", "HAM", "PIA", "SAI"])[rng.integers(0, 6, n)]
    pit_in = pd.to_timedelta(rng.uniform(0, 5000, n), unit="s").to_series().reset_index(drop=True)
    pit_in[rng.random(n) < 0.9] = pd.NaT
    pit_out = pit_in.shift(1)
    deleted = pd.array(rng.random(n) < 0.1, dtype="boolean")
    deleted[rng.random(n) < 0.05] = pd.NA
    frame = pd.DataFrame(
        {
            "Driver": drivers,
            "LapNumber": rng.integers(1, 30, n).astype("float64"),
            "LapTime": pd.to_timedelta(rng.uniform(72, 90, n), unit="s"),
            "Compound": pd.Categorical(rng.choice(["SOFT", "MEDIUM", "HARD"], n)),
            "PitInTime": pit_in,
            "PitOutTime": pit_out,
            "Deleted": deleted,
            "Position": rng.integers(1, 21, n).astype("float64"),
        },
        index=pd.RangeIndex(5, 5 + n),
    )
    return frame


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"include_deleted": True},
        {"include_pit_laps": True},
        {"include_deleted": True, "include_pit_laps": True},
        {"laps": [1, 2, 3, 10, 11]},
        {"drivers": ["VER", "NOR"]},
        {"n_drivers": 3, "finish_order": ["LEC", "PIA", "HAM", "VER"]},
        {"laps": [99]},
    ],
)
def test_single_take_filters_equal_the_chained_filters(kwargs):
    frame = _lap_frame()
    expected = _chained_filters(frame, **kwargs)
    actual = _common.apply_common_filters(frame, **kwargs)
    assert type(actual) is type(expected)
    assert actual.equals(expected)
    assert actual.index.equals(expected.index)
    assert list(actual.dtypes) == list(expected.dtypes)
    assert actual._mgr.nblocks == expected._mgr.nblocks
    # The result never aliases the caller's frame.
    actual.iloc[:, 1] = -1.0
    assert not (frame["LapNumber"] == -1.0).any()
