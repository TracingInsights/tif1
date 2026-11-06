"""Corinth C7: the CLI's session tables print the same text from either backend.

- ``results``, ``drivers`` and ``fastest`` load polars sessions when polars is
  installed (no pandas import); their tables equal the pandas sessions'
  tables, tied fastest laps included (pandas' quicksort order).
- ``standings`` prints the standings rows without building a frame.
- ``tif1.__version__`` and the User-Agent read the dist-info header without
  importing :mod:`importlib.metadata`.
"""

from __future__ import annotations

import importlib.metadata
import os
import random
import subprocess
import sys
import textwrap
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

import tif1
from tif1 import cli, core
from tif1.cache import clear_lap_cache, get_cache
from tif1.core_utils import dist_version
from tif1.payload_loader import InMemoryTransport

pl = pytest.importorskip("polars")

# An event no other unit test caches (test_q10_fastest_warm uses Monaco).
GP = "Hungarian%20Grand%20Prix"
PREFIX = f"2026/{GP}/Race/"
CODES = [f"D{i:02d}" for i in range(22)]


def _laptimes() -> dict:
    """22 drivers, three laps each; several drivers share their best time."""
    rng = random.Random(7)
    drivers, laps, times, deleted, teams = [], [], [], [], []
    for index, code in enumerate(CODES):
        best = 80.0 + (index // 3) * 0.123  # three drivers per tied best time
        for lap in (1, 2, 3):
            drivers.append(code)
            laps.append(lap)
            times.append(best if lap == 2 else best + rng.uniform(0.2, 2.0))
            deleted.append(False)
            teams.append(f"Team {index // 2}")
    times[4] = None  # a lap without a time
    return {"drv": drivers, "lap": laps, "time": times, "del": deleted, "team": teams}


def _drivers() -> dict:
    return {
        "drivers": [
            {"driver": code, "team": f"Team {i // 2}", "dn": str(i + 1)}
            for i, code in enumerate(CODES)
        ]
    }


@pytest.fixture
def cached_session(monkeypatch):
    clear_lap_cache()
    cache = get_cache()
    cache.set(PREFIX + "drivers.json", _drivers())
    cache.set(PREFIX + "session_laptimes.json", _laptimes())
    cache.conn.commit()

    def get_session(*_args, lib: str = "pandas", **_kwargs):
        session = core.Session(2026, "Hungarian Grand Prix", "Race", True, lib)
        session._memo.has_session_data = True
        session._payload_loader.transport = InMemoryTransport({})
        monkeypatch.setattr(session, "_fetch_json", _missing_json)
        return session

    monkeypatch.setattr(tif1, "get_session", get_session)
    yield
    clear_lap_cache()


def _missing_json(path: str, *_args, **_kwargs):
    if path.endswith(("drivers.json", "session_laptimes.json")):
        return get_cache().get(PREFIX + path.rsplit("/", 1)[-1])
    raise tif1.DataNotFoundError(path)


def _invoke(monkeypatch, lib: str, args: list[str]) -> str:
    monkeypatch.setattr(cli, "_session_lib", lambda: lib)
    clear_lap_cache()
    result = CliRunner().invoke(cli.app, args)
    assert result.exit_code == 0, result.output
    # stdout only: the first schedule lookup of a worker may log (stderr) that
    # the live 2026 schedule is unreachable (sockets are blocked).
    return result.stdout


@pytest.mark.usefixtures("cached_session")
@pytest.mark.parametrize(
    "args",
    [
        ["fastest", "2026", "Hungary", "R"],
        ["fastest", "2026", "Hungary", "R", "-d", "D05"],
        ["drivers", "2026", "Hungary", "R"],
        ["results", "2026", "Hungary", "R", "--no-ergast"],
    ],
)
def test_tables_match_across_backends(monkeypatch, args):
    pandas_output = _invoke(monkeypatch, "pandas", args)
    polars_output = _invoke(monkeypatch, "polars", args)
    assert polars_output == pandas_output
    assert "D05" in polars_output


@pytest.mark.usefixtures("cached_session")
def test_fastest_ties_keep_pandas_quicksort_order(monkeypatch):
    output = _invoke(monkeypatch, "polars", ["fastest", "2026", "Hungary", "R"])
    printed = [line.split("│")[2].strip() for line in output.splitlines() if "│ D" in line]
    assert sorted(printed) == sorted(CODES)
    # pandas' quicksort reorders the tied drivers; the polars table follows it.
    assert printed == [
        line.split("│")[2].strip()
        for line in _invoke(monkeypatch, "pandas", ["fastest", "2026", "Hungary", "R"]).splitlines()
        if "│ D" in line
    ]


def test_session_lib_prefers_polars(monkeypatch):
    assert cli._session_lib() == "polars"
    monkeypatch.setattr("importlib.util.find_spec", lambda _name: None)
    assert cli._session_lib() == "pandas"


@pytest.mark.parametrize("seed", range(40))
def test_sort_order_is_pandas_sort_values(seed):
    rng = random.Random(seed)
    size = rng.randint(0, 40)
    values: list = [rng.choice([None, *range(6)]) for _ in range(size)]
    ns = [None if v is None else 80_000_000_000 + v * 1_000_000 for v in values]
    frame = pd.DataFrame({"LapTime": pd.to_timedelta(pd.Series(ns, dtype="float64"), unit="ns")})
    expected = frame.sort_values("LapTime").index.tolist()
    assert cli._sort_order(ns) == expected
    floats = [None if v is None else 80.0 + v / 1000 for v in values]
    float_frame = pd.DataFrame({"time": pd.Series(floats, dtype="float64")})
    assert cli._sort_order(floats) == float_frame.sort_values("time").index.tolist()


def test_polars_rows_return_nanoseconds():
    frame = pl.DataFrame(
        {
            "Driver": ["VER", None],
            "LapTime": pl.Series([95_140_000_001, None], dtype=pl.Int64).cast(pl.Duration("ns")),
            "Points": [25.0, None],
        }
    )
    assert cli._polars_rows(frame, ("Driver", "LapTime", "Points", "Absent")) == [
        {"Driver": "VER", "LapTime": 95_140_000_001, "Points": 25.0},
        {"Driver": None, "LapTime": None, "Points": None},
    ]


@pytest.mark.parametrize("seed", range(5))
def test_nanosecond_formatting_equals_timedelta(seed):
    rng = random.Random(seed)
    for _ in range(2000):
        ns = rng.randint(-(10**13), 10**13)
        assert cli._seconds(ns) == pd.Timedelta(ns).total_seconds()
        assert cli._fmt_td(ns) == cli._fmt_td(pd.Timedelta(ns))


def test_formatting_without_pandas_values():
    assert cli._fmt_num(None) == ""
    assert cli._fmt_num(float("nan"), 1) == ""
    assert cli._fmt_num(7.0) == "7"
    assert cli._fmt_td(None) == ""
    assert cli._fmt_td(5_739_435_000_000) == "1:35:39.435"


@pytest.mark.usefixtures("ergast_offline")
@pytest.mark.parametrize("kind", ["driver", "constructor"])
def test_standings_rows_equal_the_frame(kind):
    from tif1.ergast.standings import (
        get_constructor_standings,
        get_driver_standings,
        standings_rows,
    )

    getter = get_driver_standings if kind == "driver" else get_constructor_standings
    frame = getter(2025, 2, lib="pandas")
    rows = standings_rows(kind, 2025, 2)
    assert rows
    assert list(rows[0]) == list(frame.columns)
    for row, expected in zip(rows, frame.to_dict("records"), strict=True):
        for column, value in expected.items():
            if column == "DateOfBirth":
                assert pd.Timestamp(row[column]) == value
            elif isinstance(value, float) and value != value:  # noqa: PLR0124
                assert row[column] != row[column]
            else:
                assert row[column] == value


def test_tif1_version_matches_importlib_metadata(monkeypatch):
    monkeypatch.setattr(dist_version, "_version_value", None)
    assert dist_version.tif1_version() == importlib.metadata.version("tif1")
    assert dist_version._find_dist_info("tif1") is not None


def _fake_path(tmp_path: Path, monkeypatch, *entries: str) -> None:
    monkeypatch.setattr(sys, "path", [str(tmp_path / entry) for entry in entries])


def test_find_dist_info_follows_sys_path_order(tmp_path, monkeypatch):
    for folder, version in (("first", "1.2.3"), ("second", "9.9.9")):
        info = tmp_path / folder / f"tif1-{version}.dist-info"
        info.mkdir(parents=True)
        info.joinpath("METADATA").write_text(
            f"Metadata-Version: 2.4\nName: tif1\nVersion: {version}\n\nVersion: 0\n",
            encoding="utf-8",
        )
    _fake_path(tmp_path, monkeypatch, "missing", "first", "second")
    found = dist_version._find_dist_info("tif1")
    assert found is not None
    assert dist_version._metadata_version(found) == "1.2.3"
    assert importlib.metadata.version("tif1") == "1.2.3"


def test_find_dist_info_defers_to_importlib(tmp_path, monkeypatch):
    egg = tmp_path / "eggs" / "TIF1.egg-info"
    egg.mkdir(parents=True)
    _fake_path(tmp_path, monkeypatch, "eggs")
    assert dist_version._find_dist_info("tif1") is None

    archive = tmp_path / "bundle.zip"
    archive.write_bytes(b"")
    _fake_path(tmp_path, monkeypatch, "bundle.zip")
    assert dist_version._find_dist_info("tif1") is None

    class _Finder:
        @staticmethod
        def find_distributions(*_args, **_kwargs):
            return iter(())

    monkeypatch.setattr(sys, "meta_path", [_Finder, *sys.meta_path])
    assert dist_version._find_dist_info("tif1") is None


def test_metadata_without_version_falls_back(tmp_path, monkeypatch):
    info = tmp_path / "site" / "tif1-1.0.dist-info"
    info.mkdir(parents=True)
    info.joinpath("METADATA").write_text("Name: tif1\n\nVersion: 2\n", encoding="utf-8")
    assert dist_version._metadata_version(str(info)) is None
    _fake_path(tmp_path, monkeypatch, "site")
    monkeypatch.setattr(dist_version, "_version_value", None)
    calls: list[str] = []
    monkeypatch.setattr(importlib.metadata, "version", lambda name: calls.append(name) or "7.7")
    assert dist_version.tif1_version() == "7.7"
    assert calls == ["tif1"]


def test_user_agent_uses_the_dist_info_version(monkeypatch):
    from tif1 import http_session

    monkeypatch.setattr(http_session, "_user_agent_value", None)
    assert http_session.user_agent() == (
        f"tif1/{importlib.metadata.version('tif1')} (+https://github.com/TracingInsights/tif1)"
    )


def test_version_and_standings_rows_import_no_frame_library(tmp_path):
    code = """
        import sys
        import tif1
        from tif1.core_utils.ergast_results import standings_records

        version = tif1.__version__
        row = {
            "position": "1", "positionText": "1", "points": "25", "wins": "1",
            "Driver": {"code": "PIA", "givenName": "Oscar", "familyName": "Piastri",
                       "dateOfBirth": "2001-04-06"},
            "Constructors": [{"name": "McLaren"}],
        }
        table = {"StandingsLists": [{"DriverStandings": [row]}]}
        rows = standings_records({"MRData": {"StandingsTable": table}}, "driver", native=True)
        loaded = [m for m in ("pandas", "polars", "numpy", "importlib.metadata") if m in sys.modules]
        print(version, rows[0]["Abbreviation"], rows[0]["DateOfBirth"].year, len(loaded))
    """
    env = dict(os.environ, TIF1_CACHE_DIR=str(tmp_path), PYTHONDONTWRITEBYTECODE="1")
    out = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    ).stdout.split()
    assert out == [importlib.metadata.version("tif1"), "PIA", "2001", "0"]
