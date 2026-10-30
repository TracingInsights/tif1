"""A polars session never imports pandas (H3: ~250 ms of the polars startup).

The session runs in a fresh interpreter (this process imported pandas long
ago) on in-memory payloads: ``drivers.json``, two drivers' laps and
telemetry, weather, race control, and the Ergast classification fixture, so
the results merge runs too. Every public surface the polars notebook flow
touches is read, then the child reports whether ``pandas`` (or the pandas
model module) was imported.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import tif1

pytest.importorskip("polars")

FIXTURES = Path(__file__).parent.parent / "fixtures" / "ergast"

_CHILD = r"""
import json
import socket
import sys
from pathlib import Path


def _no_network(*_args, **_kwargs):
    raise OSError("network is blocked in this test")


# Name resolution precedes every remote connection; asyncio's loopback
# socketpair does not resolve names, so it keeps working.
socket.getaddrinfo = _no_network

from tif1 import core
from tif1.config import get_config
from tif1.ergast import jolpica, payloads as ergast_payloads
from tif1.payload_loader import InMemoryTransport

fixtures = Path(sys.argv[1])
stats_dir = fixtures / "stats"
ergast = InMemoryTransport(
    {
        InMemoryTransport.key_for(path, stats_dir): json.loads(path.read_text(encoding="utf-8"))
        for path in stats_dir.rglob("*.json")
    }
)
ergast_payloads.default_transport = lambda: ergast
jolpica.default_transport = lambda: ergast
config = get_config()
config.set("ergast_fallback", False)
config.set("prefetch_all_telemetry_after_laps_load", False)

results = json.loads(
    (stats_dir / "2025" / "chinese-grand-prix" / "results.json").read_text(encoding="utf-8")
)["MRData"]["RaceTable"]["Races"][0]["Results"]
drivers = [
    {
        "driver": row["Driver"]["code"],
        "team": row["Constructor"]["name"],
        "dn": row["number"],
        "fn": row["Driver"]["givenName"],
        "ln": row["Driver"]["familyName"],
        "tc": "",
        "url": "",
    }
    for row in results
]
starts = {"VER": [300.0, 395.0, 490.0], "PIA": [300.5, 396.0, 491.5]}


def laptimes(code, number, team):
    lap_starts = starts[code]
    n = len(lap_starts)
    return {
        "time": [95.0, 94.0, 96.0],
        "lap": [1, 2, 3],
        "compound": ["SOFT"] * n,
        "stint": [1] * n,
        "s1": [30.0] * n,
        "s2": [30.0] * n,
        "s3": [35.0] * n,
        "life": [1, 2, 3],
        "pos": [1.0 if code == "PIA" else 4.0] * n,
        "status": ["1", "14", "1"],
        "pb": [False, None, True],
        "sesT": [start + 95.0 for start in lap_starts],
        "drv": [code] * n,
        "dNum": [number] * n,
        "pout": [None] * n,
        "pin": [None] * n,
        "s1T": [start + 30.0 for start in lap_starts],
        "s2T": [start + 60.0 for start in lap_starts],
        "s3T": [start + 95.0 for start in lap_starts],
        "vi1": [300.0] * n,
        "vi2": [300.0] * n,
        "vfl": [300.0] * n,
        "vst": [300.0] * n,
        "fresh": [True, None, False],
        "team": [team] * n,
        "lST": lap_starts,
        "lSD": [f"2025-03-23T07:{5 + int(s) // 60:02d}:{int(s) % 60:02d}" for s in lap_starts],
        "del": [False] * n,
        "delR": [None] * n,
        "ff1G": [False] * n,
        "iacc": [True] * n,
        "wT": [start + 10.0 for start in lap_starts],
        "wAT": [27.0, 28.0, 29.0],
        "wH": [45.0] * n,
        "wP": [1017.0] * n,
        "wR": [False] * n,
        "wTT": [35.0] * n,
        "wWD": [200] * n,
        "wWS": [1.0] * n,
    }


def telemetry(ahead):
    n = 9
    return {
        "tel": {
            "time": [i * 0.25 for i in range(n)],
            "rpm": [10000.0 + 100 * i for i in range(n)],
            "speed": [200.0 + i for i in range(n)],
            "gear": [7, 7, 7, 8, 8, 8, 8, 8, 8],
            "throttle": [100.0] * n,
            "brake": [False] * n,
            "drs": [0] * n,
            "distance": [i * 14.0 for i in range(n)],
            "rel_distance": [i / (n - 1) for i in range(n)],
            "driver_ahead": [ahead] * n,
            "distance_to_driver_ahead": [12.0] * n,
            "x": [float(i) for i in range(n)],
            "y": [0.0] * n,
            "z": [0.0] * n,
        }
    }


session_payloads = {
    "drivers.json": {"drivers": drivers},
    "VER/laptimes.json": laptimes("VER", "1", "Red Bull Racing"),
    "PIA/laptimes.json": laptimes("PIA", "81", "McLaren"),
    "weather.json": {
        "wT": [10.0, 70.0, 130.0],
        "wAT": [27.0, 27.5, 28.0],
        "wH": [45.0, 46.0, 47.0],
        "wP": [1017.0, 1017.0, 1016.5],
        "wR": [False, False, False],
        "wTT": [35.0, 35.5, 36.0],
        "wWD": [200, 210, 220],
        "wWS": [1.0, 1.2, 1.4],
    },
    "rcm.json": {
        "time": ["2025-03-23T07:00:00", "2025-03-23T07:10:00", "2025-03-23T08:40:00"],
        "cat": ["Flag", "Flag", "Flag"],
        "msg": ["GREEN LIGHT - PIT EXIT OPEN", "YELLOW IN TRACK SECTOR 4", "CHEQUERED FLAG"],
        "status": [None, None, None],
        "flag": ["GREEN", "YELLOW", "CHEQUERED"],
        "scope": ["Track", "Sector", "Track"],
        "sector": [None, 4.0, None],
        "dNum": [None, None, None],
        "lap": [1, 2, 3],
    },
}
for code, ahead in (("VER", "81"), ("PIA", "1")):
    for lap in (1, 2, 3):
        session_payloads[f"{code}/{lap}_tel.json"] = telemetry(ahead)
transport = InMemoryTransport(session_payloads)


async def fetch_multiple(requests, **_kwargs):
    return [session_payloads.get(request[3]) for request in requests]


core.fetch_multiple_async = fetch_multiple
session = core.Session(2025, "Chinese Grand Prix", "Race", enable_cache=False, lib="polars")
session._payload_loader.transport = transport

seen = {}
surfaces = {
    "laps": lambda: session.laps,
    "results": lambda: session.results,
    "drivers": lambda: session.drivers,
    "drivers_df": lambda: session.drivers_df,
    "get_driver": lambda: session.get_driver("VER"),
    "driver_laps": lambda: session.get_driver("PIA").laps,
    "fastest_lap_tel": lambda: session.get_fastest_lap_tel(),
    "driver_telemetry": lambda: session.laps.pick_drivers("VER").telemetry,
    "fastest_telemetry": lambda: session.laps.pick_drivers("VER").pick_fastest().get_telemetry(),
    "fetch_all": lambda: session.fetch_all_laps_telemetry(),
    "car_data": lambda: session.car_data["1"],
    "weather_data": lambda: session.weather_data,
    "race_control_messages": lambda: session.race_control_messages,
    "track_status": lambda: session.track_status,
    "session_status": lambda: session.session_status,
    "fastest_laps": lambda: session.get_fastest_laps(),
}
for name, read in surfaces.items():
    value = read()
    seen[name] = type(value).__name__
    if "pandas" in sys.modules:
        break
points = session.results.filter(session.results["Abbreviation"] == "PIA")["Points"].to_list()
print(
    json.dumps(
        {
            "pandas": "pandas" in sys.modules,
            "models": "tif1.models" in sys.modules,
            "seen": seen,
            "points": points,
            "time_dtype": str(session.results.schema["Time"]),
        }
    )
)
"""


def test_polars_session_never_imports_pandas(tmp_path):
    package_root = str(Path(tif1.__file__).resolve().parent.parent)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([package_root, *filter(None, [env.get("PYTHONPATH")])])
    env["HOME"] = str(tmp_path)
    env["USERPROFILE"] = str(tmp_path)
    env["TIF1_CACHE_DIR"] = str(tmp_path / "cache")
    env["TIF1_PRELOAD_IMPORTS"] = "0"
    completed = subprocess.run(
        [sys.executable, "-c", _CHILD, str(FIXTURES)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        cwd=tmp_path,
        timeout=300,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout.strip().splitlines()[-1])
    assert report["pandas"] is False, report["seen"]
    assert report["models"] is False
    assert len(report["seen"]) == 16
    assert report["seen"]["results"] == "PolarsSessionResults"
    # The Ergast classification was merged (natively, without pandas scalars).
    assert report["points"] == [25.0]
    assert report["time_dtype"] == "Duration(time_unit='ns')"
