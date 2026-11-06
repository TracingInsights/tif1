"""A polars session never imports pandas (H3: ~250 ms of the polars startup).

The session runs in a fresh interpreter (this process imported pandas long
ago) on in-memory payloads: ``drivers.json``, two drivers' laps and
telemetry, weather, race control, and the Ergast classification fixture, so
the results merge runs too. Every public surface the polars notebook flow
touches is read, then the child reports whether ``pandas`` (or the pandas
model module) or ``pyarrow`` was imported (Delphi D3: one lap's telemetry is
a native-decoder payload, whose ``DriverAhead`` codes used to go through a
pyarrow ``take``; ``t0_date``/``session_start_time`` used to be pandas
scalars). The child disarms the frame-tier writer a load schedules in its
daemon thread: it writes the polars rows with pyarrow's IPC writer (4-5x
faster than polars' own) and is not a read path.

The ``numpy`` mode of the child (Delphi D10: numpy is ~55 ms on top of
polars) reads the laps, results, drivers and state surfaces with a
``session_info.json`` present and reports whether numpy was imported, then
the lap telemetry reads (Elis E6), the ``pick_*`` family, ``iterlaps(require=)``
and the driver lookups (Gortyn G4), and the telemetry arithmetic of a notebook's
analysis (Naxos N3: distances, resampling, fills, slices, merges).

A third child reads the schedules with ``lib="polars"`` (Corinth C9: the
pandas ``EventSchedule`` imported pandas into polars scripts, ~400 ms) and
reports whether pandas, numpy or pyarrow was imported.
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
mode = sys.argv[2] if len(sys.argv) > 2 else "pandas"
if mode == "numpy":
    session_payloads["session_info.json"] = {
        "t0_date": "2025-03-23T06:55:00.000000",
        "session_start_time": 300.0,
        "total_laps": 3,
        "track_status": {"Time": [0.0, 120.5], "Status": ["1", "2"], "Message": ["AllClear", "Yellow"]},
        "session_status": {"Time": [0.0, 300.0], "Status": ["Inactive", "Started"]},
    }
else:
    # VER's fastest lap (lap 2) as the native decoder returns it: numpy
    # channels and dictionary-encoded DriverAhead codes (a missing sample is
    # -1). The numpy mode keeps the list payloads: numpy must stay unloaded.
    import numpy as np

    from tif1.core_utils.json_utils import DictionaryChannel

    native = session_payloads["VER/2_tel.json"]["tel"]
    for channel, values in list(native.items()):
        if channel == "driver_ahead":
            codes = np.array([0, 0, -1, 0, 0, 0, 0, 0, 0], dtype=np.int32)
            native[channel] = DictionaryChannel(codes, ["81"])
        elif channel != "brake":
            native[channel] = np.asarray(values)
transport = InMemoryTransport(session_payloads)


async def fetch_multiple(requests, **_kwargs):
    return [session_payloads.get(request[3]) for request in requests]


core.fetch_multiple_async = fetch_multiple
core.register_frame_writer = lambda _write: None  # the tier writer is pyarrow's, off the read paths
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
    "driver_fastest_tel": lambda: session.get_driver("VER").get_fastest_lap_tel(),
    "driver_pick_telemetry": lambda: session.laps.pick_driver("VER").telemetry,
    "t0_date": lambda: session.t0_date,
    "session_start_time": lambda: session.session_start_time,
    "session_info": lambda: session.session_info,
    "total_laps": lambda: session.total_laps,
}
if mode == "numpy":
    numpy_free = (
        "laps",
        "results",
        "drivers",
        "drivers_df",
        "get_driver",
        "weather_data",
        "race_control_messages",
        "track_status",
        "session_status",
    )
    for name in numpy_free:
        seen[name] = type(surfaces[name]()).__name__
        if "numpy" in sys.modules:
            break
    numpy_before = "numpy" in sys.modules
    # Elis E6: the common telemetry reads build their frames from the list
    # payloads with Series kernels, so numpy stays unloaded through them too.
    lap_telemetry = {
        "fastest_lap_tel": lambda: [session.get_fastest_lap_tel()],
        "driver_telemetry": lambda: [session.laps.pick_driver("VER").telemetry],
        "lap_car_data": lambda: [
            lap.get_car_data() for _, lap in session.laps.pick_driver("PIA").iterlaps()
        ],
    }
    lap_telemetry_seen = {}
    for name, read in lap_telemetry.items():
        frames = read()
        lap_telemetry_seen[name] = [
            [type(frame).__name__, frame.height, frame["Time"].cast(int).to_list()[:2]]
            for frame in frames
        ]
        if "numpy" in sys.modules:
            break
    numpy_after_lap_telemetry = "numpy" in sys.modules
    # Gortyn G4: the pick_* family (the notebook's eight picks first),
    # iterlaps(require=...) and the driver lookups select with Series masks.
    laps = session.laps

    def lap_keys(frame):
        return list(zip(frame["Driver"].to_list(), frame["LapNumber"].to_list()))

    picks = {
        "pick_driver": lambda: lap_keys(laps.pick_driver("VER")),
        "pick_drivers": lambda: lap_keys(laps.pick_drivers(["VER", "LEC"])),
        "pick_quicklaps": lambda: lap_keys(laps.pick_quicklaps()),
        "pick_quicklaps_strict": lambda: lap_keys(laps.pick_quicklaps(1.0)),
        "pick_fastest": lambda: [laps.pick_fastest()["Driver"], laps.pick_fastest()["LapNumber"]],
        "pick_laps": lambda: lap_keys(laps.pick_laps([2, 3])),
        "pick_compounds": lambda: lap_keys(laps.pick_compounds(["SOFT"])),
        "pick_wo_box": lambda: lap_keys(laps.pick_wo_box()),
        "pick_track_status": lambda: lap_keys(laps.pick_track_status("1")),
        "pick_drivers_6": lambda: lap_keys(
            laps.pick_drivers(["ver", "pia", "nor", "lec", "ham", "81"])
        ),
        "pick_track_status_contains": lambda: lap_keys(laps.pick_track_status("4", "contains")),
        "pick_track_status_none": lambda: lap_keys(laps.pick_track_status("4", "none")),
        "pick_team": lambda: lap_keys(laps.pick_team("McLaren")),
        "pick_teams": lambda: lap_keys(laps.pick_teams(["McLaren", "Ferrari"])),
        "pick_tyre": lambda: lap_keys(laps.pick_tyre("soft")),
        "pick_box_laps": lambda: lap_keys(laps.pick_box_laps()),
        "pick_not_deleted": lambda: lap_keys(laps.pick_not_deleted()),
        "pick_accurate": lambda: lap_keys(laps.pick_accurate()),
        "pick_lap": lambda: lap_keys(laps.pick_lap(2)),
        "pick_laps_slice": lambda: lap_keys(laps.pick_laps(slice(2, None))),
        "iterlaps_require": lambda: [i for i, _ in laps.iterlaps(require=["IsPersonalBest"])],
        "iterlaps_require_none": lambda: [i for i, _ in laps.iterlaps(require=["PitInTime"])],
        "driver_fastest_lap": lambda: session.get_driver("VER").get_fastest_lap()[
            "LapNumber"
        ].to_list(),
        "driver_get_lap": lambda: session.get_driver("PIA").get_lap(3)["LapNumber"],
    }
    picks_seen = {}
    for name, read in picks.items():
        picks_seen[name] = read()
        if "numpy" in sys.modules:
            break
    numpy_after_picks = "numpy" in sys.modules
    fastest_schema = {k: str(v) for k, v in session.get_fastest_lap_tel().schema.items()}
    fastest_telemetry = session.laps.pick_drivers("VER").pick_fastest().get_telemetry()
    telemetry_rows = fastest_telemetry.height
    numpy_after_get_telemetry = "numpy" in sys.modules
    distance = fastest_telemetry.add_distance()["Distance"].to_list()
    # Naxos N3: the telemetry arithmetic of the notebook's analysis runs on
    # Series kernels and lists too (numpy and its ``numpy.ma`` were ~120 ms).
    tel = fastest_telemetry.add_distance().add_differential_distance()
    other = session.laps.pick_drivers("PIA").pick_fastest().get_telemetry()
    start, end = tel["SessionTime"][1], tel["SessionTime"][6]
    lap = session.laps.pick_drivers("VER").pick_fastest()
    analysis = {
        "relative": tel.add_relative_distance()["RelativeDistance"].to_list()[-1],
        "differential": tel["DifferentialDistance"].to_list()[:3],
        "resample": tel.resample_channels(rule="0.1s").height,
        "resample_1s": tel.resample_channels()["Speed"].to_list(),
        "fill": tel.fill_missing().height,
        "slice": tel.slice_by_time(start, end).height,
        "slice_pad": tel.slice_by_time(start, end, pad=1).height,
        "slice_edges": tel.slice_by_time(start, end, interpolate_edges=True).height,
        "slice_lap": tel.slice_by_lap(lap).height,
        "merge": tel.merge_channels(other).width,
        "first_non_zero": tel.get_first_non_zero_time_index(),
    }
    print(
        json.dumps(
            {
                "numpy_before_telemetry": numpy_before,
                "numpy_after_lap_telemetry": numpy_after_lap_telemetry,
                "lap_telemetry_seen": lap_telemetry_seen,
                "numpy_after_picks": numpy_after_picks,
                "picks_seen": picks_seen,
                "numpy_after_get_telemetry": numpy_after_get_telemetry,
                "distance": distance,
                "analysis": analysis,
                "fastest_schema": fastest_schema,
                "numpy_after_telemetry": "numpy" in sys.modules,
                "pandas": "pandas" in sys.modules,
                "models_polars": "tif1.models_polars" in sys.modules,
                "seen": seen,
                "telemetry_rows": telemetry_rows,
                "points": session.results.filter(session.results["Abbreviation"] == "PIA")[
                    "Points"
                ].to_list(),
                "track_status": session.track_status["Status"].to_list(),
                "weather_schema": {k: str(v) for k, v in session.weather_data.schema.items()},
                "wind": session.weather_data["WindDirection"].to_list(),
            }
        )
    )
    sys.exit(0)
for name, read in surfaces.items():
    value = read()
    seen[name] = type(value).__name__
    if "pandas" in sys.modules or "pyarrow" in sys.modules:
        break
driver_ahead = session.get_driver("VER").get_fastest_lap_tel()["DriverAhead"].to_list()
points = session.results.filter(session.results["Abbreviation"] == "PIA")["Points"].to_list()
print(
    json.dumps(
        {
            "pandas": "pandas" in sys.modules,
            "pyarrow": "pyarrow" in sys.modules,
            "models": "tif1.models" in sys.modules,
            "seen": seen,
            "driver_ahead": driver_ahead,
            "t0_date": str(session.t0_date),
            "session_start_time": str(session.session_start_time),
            "points": points,
            "time_dtype": str(session.results.schema["Time"]),
        }
    )
)
"""


def _run_child(tmp_path: Path, mode: str) -> dict:
    package_root = str(Path(tif1.__file__).resolve().parent.parent)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([package_root, *filter(None, [env.get("PYTHONPATH")])])
    env["HOME"] = str(tmp_path)
    env["USERPROFILE"] = str(tmp_path)
    env["TIF1_CACHE_DIR"] = str(tmp_path / "cache")
    env["TIF1_PRELOAD_IMPORTS"] = "0"
    completed = subprocess.run(
        [sys.executable, "-c", _CHILD, str(FIXTURES), mode],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        cwd=tmp_path,
        timeout=300,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_polars_session_never_imports_pandas(tmp_path):
    report = _run_child(tmp_path, "pandas")
    assert report["pandas"] is False, report["seen"]
    assert report["pyarrow"] is False, report["seen"]
    assert report["models"] is False
    assert len(report["seen"]) == 22
    assert report["seen"]["driver_fastest_tel"] == "PolarsTelemetry"
    assert report["driver_ahead"] == ["81", "81", None, "81", "81", "81", "81", "81", "81"]
    # polars sessions answer with stdlib scalars (datetime / timedelta), not pandas'.
    assert report["seen"]["t0_date"] == "datetime"
    assert report["t0_date"] == "2025-03-23 07:05:00"  # LapStartDate - LapStartTime
    assert report["seen"]["session_start_time"] == "timedelta"
    assert report["seen"]["session_info"] == "dict"
    assert report["seen"]["total_laps"] == "int"  # the leader's laps of the Ergast fixture
    assert report["seen"]["results"] == "PolarsSessionResults"
    # The Ergast classification was merged (natively, without pandas scalars).
    assert report["points"] == [25.0]
    assert report["time_dtype"] == "Duration(time_unit='ns')"


def test_polars_laps_results_and_state_never_import_numpy(tmp_path):
    """The laps, results, drivers and state reads of a polars session leave numpy unloaded."""
    report = _run_child(tmp_path, "numpy")
    assert report["numpy_before_telemetry"] is False, report["seen"]
    assert len(report["seen"]) == 9
    assert report["pandas"] is False
    assert report["models_polars"] is True  # the lazy binding of its hot paths was exercised
    # Elis E6: the fastest lap's telemetry, a driver's merged laps and each
    # lap's car data are built from the list payloads without numpy.
    assert report["numpy_after_lap_telemetry"] is False, report["lap_telemetry_seen"]
    seen_tel = report["lap_telemetry_seen"]
    assert seen_tel["fastest_lap_tel"] == [["PolarsTelemetry", 9, [0, 250_000_000]]]
    # Three 9-sample laps joined into one slice, ``Time`` from the first lap start.
    assert seen_tel["driver_telemetry"] == [["PolarsTelemetry", 27, [0, 250_000_000]]]
    assert seen_tel["lap_car_data"] == [["PolarsTelemetry", 9, [0, 250_000_000]]] * 3
    assert report["fastest_schema"]["Date"] == "Datetime(time_unit='ns', time_zone=None)"
    assert report["fastest_schema"]["SessionTime"] == "Duration(time_unit='ns')"
    assert report["fastest_schema"]["nGear"] == "Int64"
    assert report["fastest_schema"]["Brake"] == "Boolean"
    assert report["fastest_schema"]["DriverAhead"] == "String"
    # Gortyn G4: the pick_* family, iterlaps(require=...) and the driver
    # lookups leave numpy unloaded, and so does the fastest lap's merged
    # get_telemetry(); Naxos N3: so does the telemetry arithmetic after it.
    assert report["numpy_after_picks"] is False, report["picks_seen"]
    picks = report["picks_seen"]
    assert len(picks) == 24
    both = [["PIA", 1], ["PIA", 2], ["PIA", 3], ["VER", 1], ["VER", 2], ["VER", 3]]
    ver, pia = both[3:], both[:3]
    assert picks["pick_driver"] == ver
    assert picks["pick_drivers"] == ver
    assert picks["pick_quicklaps"] == both  # 94 s best, 96 s < 94 * 1.07
    assert picks["pick_quicklaps_strict"] == []  # strictly below the best
    assert picks["pick_fastest"] == ["PIA", 3]  # personal bests tie at 96 s: the first row
    assert picks["pick_laps"] == [["PIA", 2], ["PIA", 3], ["VER", 2], ["VER", 3]]
    assert picks["pick_compounds"] == both
    assert picks["pick_wo_box"] == both
    assert picks["pick_track_status"] == [["PIA", 1], ["PIA", 3], ["VER", 1], ["VER", 3]]
    assert picks["pick_drivers_6"] == both  # codes upper-cased, "81" is PIA's number
    assert picks["pick_track_status_contains"] == [["PIA", 2], ["VER", 2]]
    assert picks["pick_track_status_none"] == [["PIA", 1], ["PIA", 3], ["VER", 1], ["VER", 3]]
    assert picks["pick_team"] == pia
    assert picks["pick_teams"] == pia
    assert picks["pick_tyre"] == both
    assert picks["pick_box_laps"] == []
    assert picks["pick_not_deleted"] == both
    assert picks["pick_accurate"] == both
    assert picks["pick_lap"] == [["PIA", 2], ["VER", 2]]
    assert picks["pick_laps_slice"] == [["PIA", 2], ["PIA", 3], ["VER", 2], ["VER", 3]]
    assert picks["iterlaps_require"] == [0, 1, 2, 3, 4, 5]
    assert picks["iterlaps_require_none"] == []  # PitInTime is all null
    assert picks["driver_fastest_lap"] == [2]  # not deleted, 94 s
    assert picks["driver_get_lap"] == 3
    assert report["numpy_after_get_telemetry"] is False
    assert report["numpy_after_telemetry"] is False, report["analysis"]
    assert report["telemetry_rows"] == 9
    assert report["distance"][0] == 0.0
    # 0.25 s samples at 200 + i km/h: the first sample drove nothing.
    assert report["distance"][1] == 201.0 / 3.6 * 0.25
    assert report["distance"][-1] > 0.0
    analysis = report["analysis"]
    assert analysis["relative"] == 1.0
    assert analysis["differential"] == [0.0, 201.0 / 3.6 * 0.25, 202.0 / 3.6 * 0.25]
    assert analysis["resample"] == 21  # 2 s of samples in 0.1 s bins
    assert analysis["resample_1s"] == [201.5, 205.5, 208.0]  # means of 4, 4 and 1 samples
    assert analysis["fill"] == 9
    assert analysis["slice"] == 6
    assert analysis["slice_pad"] == 8
    assert analysis["slice_edges"] == 6  # both edges are sampled instants
    assert analysis["slice_lap"] == 9
    assert analysis["merge"] > 23
    assert analysis["first_non_zero"] == 1
    # The frames themselves are the typed ones.
    assert report["points"] == [25.0]
    assert report["track_status"] == ["1", "2"]
    assert report["weather_schema"] == {
        "Time": "Duration(time_unit='ns')",
        "AirTemp": "Float64",
        "Humidity": "Float64",
        "Pressure": "Float64",
        "Rainfall": "Boolean",
        "TrackTemp": "Float64",
        "WindDirection": "Int64",
        "WindSpeed": "Float64",
    }
    assert report["wind"] == [200, 210, 220]


_SCHEDULE_CHILD = r"""
import json
import sys
from datetime import datetime

import tif1

schedule = tif1.get_event_schedule(2025, lib="polars")
remaining = tif1.get_events_remaining(datetime(2025, 7, 1), lib="polars")
old = tif1.get_event_schedule(1998, include_testing=False, lib="polars")
print(
    json.dumps(
        {
            "modules": {m: m in sys.modules for m in ("pandas", "numpy", "pyarrow")},
            "frames": [type(f).__name__ for f in (schedule, remaining, old)],
            "rounds": schedule["RoundNumber"].to_list(),
            "remaining": remaining["EventName"].to_list()[:2],
            "old": old.height,
        }
    )
)
"""


def test_polars_schedule_never_imports_pandas_numpy_or_pyarrow(tmp_path):
    """``lib="polars"`` schedules (vendored seasons, offline) leave pandas/numpy/pyarrow unloaded."""
    package_root = str(Path(tif1.__file__).resolve().parent.parent)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([package_root, *filter(None, [env.get("PYTHONPATH")])])
    env["HOME"] = str(tmp_path)
    env["USERPROFILE"] = str(tmp_path)
    env["TIF1_CACHE_DIR"] = str(tmp_path / "cache")
    env["TIF1_OFFLINE_MODE"] = "true"
    env["TIF1_PRELOAD_IMPORTS"] = "0"
    completed = subprocess.run(
        [sys.executable, "-c", _SCHEDULE_CHILD],
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
    assert report["modules"] == {"pandas": False, "numpy": False, "pyarrow": False}
    assert report["frames"] == ["DataFrame", "DataFrame", "DataFrame"]
    assert report["rounds"] == [*range(1, 25), 0]
    assert report["remaining"] == ["British Grand Prix", "Belgian Grand Prix"]
    assert report["old"] == 16
