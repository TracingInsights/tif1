"""CLI commands backed by Ergast data: ``results``, ``standings``, ``cache-clear --ergast``."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from typer.testing import CliRunner

import tif1
from tif1.cli import _fmt_num, _fmt_td, app
from tif1.payload_loader import InMemoryTransport

FIXTURES = Path(__file__).parent.parent / "fixtures" / "ergast" / "stats"


def _drivers_payload() -> dict:
    payload = json.loads((FIXTURES / "2025/chinese-grand-prix/results.json").read_text())
    rows = payload["MRData"]["RaceTable"]["Races"][0]["Results"]
    return {
        "drivers": [
            {
                "driver": r["Driver"]["code"],
                "team": r["Constructor"]["name"],
                "dn": r["number"],
                "fn": r["Driver"]["givenName"],
                "ln": r["Driver"]["familyName"],
                "tc": "",
                "url": "",
            }
            for r in rows
        ]
    }


@pytest.fixture
def offline_sessions(monkeypatch, ergast_offline):
    original = tif1.get_session

    def get_session(*args, **kwargs):
        session = original(*args, enable_cache=False, **kwargs)
        session._payload_loader.transport = InMemoryTransport({"drivers.json": _drivers_payload()})
        return session

    monkeypatch.setattr(tif1, "get_session", get_session)
    return get_session


class TestFormatting:
    def test_fmt_td(self):
        assert _fmt_td(pd.Timedelta(seconds=95.123)) == "1:35.123"
        assert _fmt_td(pd.Timedelta(hours=1, minutes=30, seconds=55.026)) == "1:30:55.026"
        assert _fmt_td(pd.NaT) == ""
        assert _fmt_td(None) == ""
        assert _fmt_td(pd.Timedelta(seconds=-5)) == "-0:05.000"

    def test_fmt_num(self):
        assert _fmt_num(25.0) == "25"
        assert _fmt_num(18.0, 1) == "18.0"
        assert _fmt_num(float("nan")) == ""
        assert _fmt_num(None) == ""


@pytest.mark.usefixtures("offline_sessions")
class TestResultsCommand:
    def test_race_results_table(self):
        result = CliRunner().invoke(app, ["results", "2025", "Chinese Grand Prix", "Race"])
        assert result.exit_code == 0, result.output
        assert "PIA" in result.output
        assert "McLaren" in result.output
        assert "1:30:55.026" in result.output
        assert "+0:09.748" in result.output
        assert "Finished" in result.output

    def test_qualifying_results_table_by_round(self):
        result = CliRunner().invoke(app, ["results", "2025", "2", "Qualifying"])
        assert result.exit_code == 0, result.output
        assert "Q3" in result.output
        assert "1:30.641" in result.output

    def test_no_ergast_flag(self):
        result = CliRunner().invoke(app, ["results", "2025", "2", "Race", "--no-ergast"])
        assert result.exit_code == 0, result.output
        assert "1:30:55.026" not in result.output


@pytest.mark.usefixtures("ergast_offline")
class TestStandingsCommand:
    def test_driver_standings(self):
        result = CliRunner().invoke(app, ["standings", "2025", "2"])
        assert result.exit_code == 0, result.output
        assert "NOR" in result.output
        assert "44.0" in result.output
        assert "after round 2" in result.output

    def test_constructor_standings(self):
        result = CliRunner().invoke(app, ["standings", "2025", "2", "--constructors"])
        assert result.exit_code == 0, result.output
        assert "McLaren" in result.output
        assert "78.0" in result.output

    def test_polars_config_still_prints(self):
        pytest.importorskip("polars")
        from tif1.config import get_config

        previous = get_config().get("lib")
        get_config().set("lib", "polars")
        try:
            result = CliRunner().invoke(app, ["standings", "2025", "2"])
        finally:
            get_config().set("lib", previous)
        assert result.exit_code == 0, result.output
        assert "NOR" in result.output

    def test_missing_standings_exit_code(self, monkeypatch):
        monkeypatch.setattr(tif1, "get_driver_standings", lambda *_a, **_k: pd.DataFrame())
        result = CliRunner().invoke(app, ["standings", "1900"])
        assert result.exit_code == 1


class TestCacheClear:
    def test_ergast_scope(self, monkeypatch):
        calls: list[str] = []
        fake = SimpleNamespace(
            invalidate=lambda scope: calls.append(scope), clear=lambda: calls.append("clear")
        )
        monkeypatch.setattr(tif1, "get_cache", lambda: fake)
        result = CliRunner().invoke(app, ["cache-clear", "--yes", "--ergast"])
        assert result.exit_code == 0, result.output
        assert calls == ["ergast"]
        result = CliRunner().invoke(app, ["cache-clear", "--yes"])
        assert calls == ["ergast", "clear"]

    def test_cache_invalidate_ergast_scope_drops_only_ergast_rows(self):
        from tif1.cache import get_cache
        from tif1.ergast.payloads import clear_memo

        cache = get_cache()
        cache.set("stats/2025/x/results.json", {"fetched_at": 1, "payload": {}})
        cache.set("jolpica/seasons.json", {"fetched_at": 1, "payload": {}})
        cache.set("2025/GP/Race/drivers.json", {"drivers": []})
        assert cache.get("2025/GP/Race/drivers.json") == {"drivers": []}  # memory tier warm
        clear_memo()
        cache.invalidate("ergast")
        assert cache.get("stats/2025/x/results.json") is None
        assert cache.get("jolpica/seasons.json") is None
        assert cache._memory_cache.get("2025/GP/Race/drivers.json") is not None
        assert cache.get("2025/GP/Race/drivers.json") == {"drivers": []}


def test_jupyter_display_reports_classification(ergast_offline):
    from tif1.jupyter import display_session_info

    session = tif1.get_session(2025, "Chinese Grand Prix", "Race", enable_cache=False)
    session._payload_loader.transport = InMemoryTransport({"drivers.json": _drivers_payload()})
    html = display_session_info(session)
    assert "Not loaded" in html
    _ = session.results
    html = display_session_info(session)
    assert "official (Ergast)" in html
    session_off = tif1.get_session(
        2025, "Chinese Grand Prix", "Race", enable_cache=False, ergast=False
    )
    assert ">off<" in display_session_info(session_off)
