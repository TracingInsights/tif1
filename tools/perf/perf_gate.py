"""CI performance regression gate: fresh-process warm workloads against a vendored cache.

The Delphi harness (``delphi_warm_bench.py``) measures warm sessions against a
primed 290 MB cache that CI does not have. This gate runs the same kind of
fresh-interpreter workloads against a small cache seeded from
``tests/fixtures/perf/payloads.bin.xz`` (the 2025 Bahrain race and qualifying,
telemetry of the race's VER and the qualifying's PIA, VER and LEC; built by
``tools/perf/perf_gate_fixture.py``) and
compares the in-process phase times with a committed baseline
(``tests/fixtures/perf/baseline.json``). Wall times are not compared: a CI
machine's interpreter start and teardown are too noisy.

Steps of one run (``run_gate``):

1. Seed a new cache directory from the bundle through the ``Cache`` API of
   the tree under test (a child process), so the cache schema always matches.
2. Build the parity digest of both sessions on each backend in a fresh
   process (``--digest``). This first pass reads the payload tier and writes
   the frame tiers (the cold build path); a second digest after the timed
   runs reads them back (the warm path). Each must equal its committed
   digest surface by surface (``parity_<lib>.json``, warm, plus
   ``parity_<lib>_cold_delta.json`` for the cold pass; see ``DIGEST_KINDS``).
3. Run every workload ``reps`` times per backend, each in a fresh process,
   offline, and keep the best (minimum) time of each phase.
4. Re-run ``confirm_reps`` more times every workload with a phase over its
   limit (a descheduled run is noise; a regression is slow on every run).

Comparison (``compare``): CI runners are slower or faster than the machine
that wrote the baseline, so every phase is first scaled by the machine factor,
the median of best/baseline-best over the phases of at least
``_FACTOR_MIN_MS`` (one slow phase does not move a median of ~120). The
baseline keeps the min and the median of ``BASELINE_REPS`` runs per phase; a
phase regresses when its best time exceeds ``median * factor * tolerance``
and the excess is at least ``abs_floor_ms`` (sub-millisecond phases jitter
by more than 50%). A slowdown of every phase at once reads as a slower
machine: the factor is printed in the report so a reviewer can see it, and
it is clamped to ``[1 / _FACTOR_CLAMP, _FACTOR_CLAMP]``.

Usage (from the repository root, with the project's interpreter):
    python tools/perf/perf_gate.py                    # check, print the table
    python tools/perf/perf_gate.py --write-baseline   # rewrite baseline + digests
    python tools/perf/perf_gate.py --libs pandas --workloads laps,results --reps 1
The pytest wrapper is ``tests/perf_gate/test_perf_gate.py`` (marker ``perf_gate``).

``TIF1_PERF_GATE_INJECT_SLEEP=<seconds>`` (read by the workload child only)
wraps ``Session.laps`` with a sleep: the gate's self-test proves that a
deliberate slowdown fails the comparison.
"""

from __future__ import annotations

import argparse
import hashlib
import lzma
import math
import os
import platform
import statistics
import struct
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPO / "tests" / "fixtures" / "perf"
BUNDLE = FIXTURE_DIR / "payloads.bin.xz"
BASELINE = FIXTURE_DIR / "baseline.json"
LIBS = ("pandas", "polars")

YEAR, GP = 2025, "Bahrain Grand Prix"
#: Drivers whose telemetry the bundle holds (``perf_gate_fixture.TELEMETRY_DRIVERS``).
RACE_TEL = ("VER",)
QUALI_TEL = ("PIA", "VER", "LEC")
WORKLOADS = (
    "import_get_session",
    "laps",
    "results",
    "state",
    "driver_tel",
    "car_data_loop",
    "fastest_lap_tel",
    "quali_split",
    "all_tel",
    "notebook",
)
DEFAULT_REPS = 3
BASELINE_REPS = 7
DEFAULT_CONFIRM_REPS = 5
DEFAULT_TOLERANCE = 1.5
DEFAULT_ABS_FLOOR_MS = 5.0
_FACTOR_MIN_MS = 2.0
_FACTOR_CLAMP = 3.0
INJECT_ENV = "TIF1_PERF_GATE_INJECT_SLEEP"


#: ``cold``: the first pass, frames built from the payload tier; ``warm``: the
#: frames read back from the frame tiers. ``parity_<lib>.json`` is the warm
#: digest; ``parity_<lib>_cold_delta.json`` lists only the surfaces where the
#: cold pass differs from it (``null``: absent on the cold pass). The paths do
#: not agree everywhere yet: pandas ``fetch_all_laps_telemetry()`` returns
#: plain ``DataFrame`` objects when it builds the frames and ``Telemetry``
#: objects from the frame tier. Each stays as it is until a reviewed change
#: rewrites the files.
DIGEST_KINDS = ("cold", "warm")


def parity_path(lib: str) -> Path:
    """Committed (warm) parity digest of one backend."""
    return FIXTURE_DIR / f"parity_{lib}.json"


def cold_delta_path(lib: str) -> Path:
    """Committed surfaces where the cold pass differs from the warm digest."""
    return FIXTURE_DIR / f"parity_{lib}_cold_delta.json"


def expected_digest(lib: str, kind: str) -> dict[str, Any]:
    """The committed digest of one backend and read path."""
    import orjson

    digest = orjson.loads(parity_path(lib).read_bytes())
    if kind == "cold":
        for key, value in orjson.loads(cold_delta_path(lib).read_bytes()).items():
            if value is None:
                digest.pop(key, None)
            else:
                digest[key] = value
    return digest


def _dump_digest(digest: dict[str, Any]) -> bytes:
    """One surface per line: a changed surface is a one-line diff."""
    import orjson

    if not digest:
        return b"{}\n"
    lines = [
        orjson.dumps(key) + b": " + orjson.dumps(value, option=orjson.OPT_SORT_KEYS)
        for key, value in sorted(digest.items())
    ]
    return b"{\n" + b",\n".join(lines) + b"\n}\n"


# -- fixture bundle -------------------------------------------------------------


def read_bundle(path: Path = BUNDLE) -> tuple[dict[str, Any], memoryview]:
    """Return the bundle manifest and the payload area its offsets point into."""
    import orjson

    raw = lzma.decompress(path.read_bytes())
    (head_len,) = struct.unpack_from("<Q", raw, 0)
    manifest = orjson.loads(raw[8 : 8 + head_len])
    return manifest, memoryview(raw)[8 + head_len :]


def _seed(cache_dir: Path) -> None:
    """Child: write the bundle's payloads into a new cache at ``cache_dir``."""
    import orjson

    from tif1.cache import Cache

    manifest, area = read_bundle()
    cache = Cache(cache_dir=cache_dir)
    for key, offset, length in manifest["json"]:
        cache.set_raw(key, area[offset : offset + length])
    for year, gp, session, driver, lap, offset, length in manifest["telemetry"]:
        payload = orjson.loads(area[offset : offset + length])
        cache.set_telemetry(year, gp, session, driver, lap, payload)
    for key in manifest["missing"]:
        cache.record_missing(key)
    cache.close()
    print(
        f"seeded {len(manifest['json'])} payloads, {len(manifest['telemetry'])} laps, "
        f"{len(manifest['missing'])} missing verdicts"
    )


# -- workload child ---------------------------------------------------------------


def _inject_slowdown(seconds: float) -> None:
    from tif1.core import Session

    original = Session.laps

    def slow_laps(self):
        time.sleep(seconds)
        return original.fget(self)

    Session.laps = property(slow_laps)


def _child(workload: str, lib: str) -> dict[str, float]:
    out: dict[str, float] = {}
    t_proc = time.perf_counter()

    def step(name: str, fn: Callable[[], Any]) -> Any:
        t0 = time.perf_counter()
        result = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 3)
        return result

    import tif1

    out["import"] = round((time.perf_counter() - t_proc) * 1000, 3)
    inject = os.environ.get(INJECT_ENV)
    if inject:
        _inject_slowdown(float(inject))
    quali = workload in {"fastest_lap_tel", "quali_split", "all_tel", "notebook"}
    name = "Qualifying" if quali else "Race"
    session = step("get_session", lambda: tif1.get_session(YEAR, GP, name, lib=lib))
    if workload == "laps":
        step("laps", lambda: session.laps)
    elif workload == "results":
        step("laps", lambda: session.laps)
        step("results", lambda: session.results)
    elif workload == "state":
        step("laps", lambda: session.laps)
        step("weather", lambda: session.weather_data)
        step("rcm", lambda: session.race_control_messages)
        step("track_status", lambda: session.track_status)
        step("session_status", lambda: session.session_status)
        step("t0_date", lambda: session.t0_date)
    elif workload == "driver_tel":
        step("laps", lambda: session.laps)
        step("driver_tel", lambda: session.laps.pick_driver("VER").telemetry)
    elif workload == "car_data_loop":
        step("laps", lambda: session.laps)
        drv = session.laps.pick_driver("VER")
        step("car_data_loop", lambda: [lap.get_car_data() for _, lap in drv.iterlaps()])
    elif workload == "fastest_lap_tel":
        step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
        step("driver_fastest_tel", lambda: session.get_driver("VER").get_fastest_lap_tel())
    elif workload == "quali_split":
        step("laps", lambda: session.laps)
        step("split", lambda: session.laps.split_qualifying_sessions())
        step("results", lambda: session.results)
    elif workload == "all_tel":
        step("laps", lambda: session.laps)
        step("all_tel", lambda: session.fetch_all_laps_telemetry())
    elif workload == "notebook":
        _notebook(session, lib, step)
    elif workload != "import_get_session":
        raise SystemExit(f"unknown workload {workload!r}")
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 3)
    return out


def _notebook(session: Any, lib: str, step: Callable[[str, Callable[[], Any]], Any]) -> None:
    """The Delphi ``notebook`` workload on the drivers the bundle has telemetry for."""
    # The eager load (fastf1's load()): the baseline was measured on it, and
    # it keeps the batch reader of every driver row under the gate. The lazy
    # default moves that cost into the first lap loop (iterlaps_tel_loop).
    step("load", lambda: session.load(telemetry="eager", weather=True, messages=True))
    laps = session.laps
    step(
        "picks_x8",
        lambda: [
            laps.pick_driver("VER"),
            laps.pick_drivers(["VER", "LEC"]),
            laps.pick_quicklaps(),
            laps.pick_fastest(),
            laps.pick_laps([5, 6]),
            laps.pick_compounds(["SOFT"]),
            laps.pick_wo_box(),
            laps.pick_track_status("1"),
        ],
    )
    tel_laps = laps.pick_drivers(list(QUALI_TEL))
    step("iterlaps_tel_loop", lambda: [lap.telemetry for _, lap in tel_laps.iterlaps()])
    if lib == "polars":
        step("iloc_200", lambda: [laps.row(i, named=True) for i in range(200)])
    else:
        step("iloc_200", lambda: [laps.iloc[i] for i in range(200)])
    step("driver_merged_x3", lambda: [laps.pick_driver(d).telemetry for d in QUALI_TEL])
    fastest = [laps.pick_driver(d).pick_fastest() for d in QUALI_TEL]

    def analysis() -> list[Any]:
        tels = []
        for lap in fastest:
            tel = lap.get_telemetry().add_distance().add_differential_distance()
            tel.resample_channels(rule="0.1s")
            tel.fill_missing()
            if lib == "polars":
                tel.slice_by_time(tel["Time"][0], tel["Time"][len(tel) // 2])
            else:
                tel.slice_by_time(tel["Time"].iloc[0], tel["Time"].iloc[len(tel) // 2])
            tels.append(tel)
        tels[0].merge_channels(tels[1])
        return tels

    step("telemetry_analysis_3", analysis)
    step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
    step(
        "driver_fastest_lap_x3",
        lambda: [session.get_driver(d).get_fastest_lap() for d in QUALI_TEL],
    )
    step("results", lambda: session.results)
    step("weather", lambda: session.weather_data)


# -- parity digest child (after tools/perf/delphi_parity.py) ----------------------


def _hash(text: str) -> str:
    return hashlib.blake2b(text.encode("utf-8"), digest_size=12).hexdigest()


def _hash_array(values: Any) -> str:
    import numpy as np

    arr = np.asarray(values)
    if arr.dtype == object:
        return _hash(repr(list(arr)))
    h = hashlib.blake2b(digest_size=12)
    h.update(str(arr.dtype).encode("utf-8"))
    h.update(np.ascontiguousarray(arr).tobytes())
    return h.hexdigest()


def frame_digest(frame: Any, lib: str) -> dict[str, Any]:
    """Shape, class, index and per-column dtype + value hash of a frame, series or row."""
    if frame is None:
        return {"none": True}
    if lib == "polars":
        import polars as pl

        if isinstance(frame, pl.Series):
            frame = frame.to_frame()
        if not hasattr(frame, "columns"):
            if hasattr(frame, "to_frame"):
                frame = frame.to_frame()
            else:
                return {"row": _hash(repr(frame.to_dict() if hasattr(frame, "to_dict") else frame))}
        cols = {
            name: [str(frame.get_column(name).dtype), _hash(repr(frame.get_column(name).to_list()))]
            for name in frame.columns
        }
        return {"shape": list(frame.shape), "columns": cols, "class": type(frame).__name__}
    import pandas as pd

    cls = type(frame).__name__
    if isinstance(frame, pd.Series):
        frame = frame.to_frame().T if frame.index.dtype == object else frame.to_frame()
    cols = {}
    for i, name in enumerate(frame.columns):
        column = frame.iloc[:, i]
        raw = column.to_numpy()
        if raw.dtype == object:
            cleaned = [
                None
                if x is None
                or (isinstance(x, float) and math.isnan(x))
                or x is pd.NaT
                or x is pd.NA
                else x
                for x in raw
            ]
            digest = _hash(repr(cleaned))
        else:
            digest = _hash_array(raw)
        cols[str(name)] = [str(column.dtype), digest]
    return {
        "shape": list(frame.shape),
        "index": _hash(repr(list(frame.index))),
        "columns": cols,
        "class": cls,
    }


def _scalar(value: Any) -> str:
    return f"{type(value).__name__}:{value!r}"


def build_digest(lib: str) -> dict[str, Any]:
    """Digest every user-visible surface the bundle can serve, for one backend."""
    import tif1

    out: dict[str, Any] = {}
    for sess_name, tel_drivers in (("Race", RACE_TEL), ("Qualifying", QUALI_TEL)):
        s = tif1.get_session(YEAR, GP, sess_name, lib=lib)
        key = sess_name
        laps = s.laps
        out[f"{key}/laps"] = frame_digest(laps, lib)
        out[f"{key}/results"] = frame_digest(s.results, lib)
        out[f"{key}/drivers"] = list(s.drivers)
        out[f"{key}/weather"] = frame_digest(s.weather_data, lib)
        out[f"{key}/rcm"] = frame_digest(s.race_control_messages, lib)
        out[f"{key}/track_status"] = frame_digest(s.track_status, lib)
        out[f"{key}/session_status"] = frame_digest(s.session_status, lib)
        out[f"{key}/t0_date"] = _scalar(s.t0_date)
        out[f"{key}/session_start_time"] = _scalar(s.session_start_time)
        out[f"{key}/total_laps"] = _scalar(s.total_laps)
        out[f"{key}/pick_quicklaps"] = frame_digest(laps.pick_quicklaps(), lib)
        out[f"{key}/pick_fastest"] = frame_digest(laps.pick_fastest(), lib)
        out[f"{key}/pick_compounds"] = frame_digest(laps.pick_compounds(["SOFT"]), lib)
        out[f"{key}/pick_wo_box"] = frame_digest(laps.pick_wo_box(), lib)
        out[f"{key}/pick_track_status"] = frame_digest(laps.pick_track_status("1"), lib)
        if sess_name == "Qualifying":
            out[f"{key}/split"] = [frame_digest(q, lib) for q in laps.split_qualifying_sessions()]
            out[f"{key}/fastest_lap_tel"] = frame_digest(s.get_fastest_lap_tel(), lib)
            all_tel = s.fetch_all_laps_telemetry()
            out[f"{key}/all_tel"] = {
                f"{k[0]}/{k[1]}": frame_digest(v, lib)
                for k, v in sorted(all_tel.items(), key=lambda kv: (str(kv[0][0]), int(kv[0][1])))
            }
        for d in tel_drivers:
            drv = s.get_driver(d)
            out[f"{key}/{d}/driver_tel"] = frame_digest(laps.pick_driver(d).telemetry, lib)
            out[f"{key}/{d}/driver_laps"] = frame_digest(drv.laps, lib)
            out[f"{key}/{d}/driver_row"] = frame_digest(
                drv if lib == "pandas" else drv.to_frame() if hasattr(drv, "to_frame") else None,
                lib,
            )
            out[f"{key}/{d}/driver_fastest_lap"] = frame_digest(drv.get_fastest_lap(), lib)
            out[f"{key}/{d}/driver_fastest_tel"] = frame_digest(drv.get_fastest_lap_tel(), lib)
            lap = laps.pick_driver(d).pick_fastest()
            tel = lap.get_telemetry()
            out[f"{key}/{d}/analysis/tel"] = frame_digest(tel, lib)
            tel = tel.add_distance().add_differential_distance()
            out[f"{key}/{d}/analysis/dist"] = frame_digest(tel, lib)
            out[f"{key}/{d}/analysis/resample"] = frame_digest(
                tel.resample_channels(rule="0.1s"), lib
            )
            out[f"{key}/{d}/analysis/resample1s"] = frame_digest(tel.resample_channels(), lib)
            out[f"{key}/{d}/analysis/fill"] = frame_digest(tel.fill_missing(), lib)
            out[f"{key}/{d}/analysis/car"] = frame_digest(lap.get_car_data(), lib)
            out[f"{key}/{d}/analysis/pos"] = frame_digest(lap.get_pos_data(), lib)
            out[f"{key}/{d}/analysis/car_pad"] = frame_digest(
                lap.get_car_data(pad=1, pad_side="both"), lib
            )
            out[f"{key}/{d}/car_loop"] = [
                frame_digest(lap.get_car_data(), lib) for _, lap in laps.pick_driver(d).iterlaps()
            ]
    return out


def compare_digests(expected: dict[str, Any], actual: dict[str, Any]) -> list[str]:
    """Surface-by-surface differences (one line per differing column/value)."""
    lines: list[str] = []
    for key in sorted(set(expected) | set(actual)):
        if key not in expected or key not in actual:
            side = "the committed digest" if key in expected else "this tree"
            lines.append(f"{key}: only in {side}")
            continue
        a, b = expected[key], actual[key]
        if a == b:
            continue
        if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
            for i, (x, y) in enumerate(zip(a, b)):
                if x != y:
                    lines.extend(_frame_diff(f"{key}[{i}]", x, y))
        elif isinstance(a, dict) and isinstance(b, dict) and "columns" not in a:
            # A collection of frames (all_tel): recurse per member.
            lines.extend(f"{key}/{line}" for line in compare_digests(a, b))
        else:
            lines.extend(_frame_diff(key, a, b))
    return lines


def _frame_diff(key: str, a: Any, b: Any) -> list[str]:
    if not (isinstance(a, dict) and isinstance(b, dict) and "columns" in a and "columns" in b):
        return [f"{key}: {str(a)[:100]} -> {str(b)[:100]}"]
    lines = [
        f"{key}: {part} {a.get(part)} -> {b.get(part)}"
        for part in ("shape", "class", "index")
        if a.get(part) != b.get(part)
    ]
    ca, cb = a["columns"], b["columns"]
    lines.extend(
        f"{key}: column {col}: {ca.get(col)} -> {cb.get(col)}"
        for col in dict.fromkeys([*ca, *cb])
        if ca.get(col) != cb.get(col)
    )
    if list(ca) != list(cb) and not lines:
        lines.append(f"{key}: column order {list(a['columns'])} -> {list(b['columns'])}")
    return lines or [f"{key}: differs"]


# -- orchestration (parent) ------------------------------------------------------


def child_env(cache_dir: Path, home: Path, src: Path | None) -> dict[str, str]:
    """A clean child environment: no TIF1_* from the caller, no user config, offline."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("TIF1_", "PYTEST_"))}
    env["TIF1_CACHE_DIR"] = str(cache_dir)
    env["TIF1_OFFLINE_MODE"] = "true"
    env["TIF1_CDN_PRECONNECT"] = "false"
    # A developer's ~/.tif1rc must not change what the gate measures.
    env["HOME"] = str(home)
    env["USERPROFILE"] = str(home)
    if src is not None:
        env["PYTHONPATH"] = str(src)
    return env


def _run(args: list[str], env: dict[str, str]) -> str:
    cmd = [sys.executable, str(Path(__file__).resolve()), *args]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    if proc.returncode != 0:
        msg = f"{' '.join(args)} failed (exit {proc.returncode}):\n{proc.stderr[-3000:]}"
        raise RuntimeError(msg)
    return proc.stdout


@dataclass
class GateRun:
    """Everything one gate run measured."""

    phases: dict[str, dict[str, dict[str, float]]] = field(default_factory=dict)
    digests: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)
    #: Every run's time of every phase (``phases`` holds their minimum).
    samples: dict[str, dict[str, dict[str, list[float]]]] = field(default_factory=dict)
    #: ``(lib, workload)`` pairs re-measured because a phase looked regressed.
    confirmed: list[tuple[str, str]] = field(default_factory=list)
    seconds: float = 0.0


def run_gate(
    libs: tuple[str, ...] = LIBS,
    workloads: tuple[str, ...] = WORKLOADS,
    reps: int = DEFAULT_REPS,
    src: Path | None = REPO / "src",
    digests: bool = True,
    inject_sleep: float | None = None,
    work_dir: Path | None = None,
    baseline: dict[str, Any] | None = None,
    confirm_reps: int = DEFAULT_CONFIRM_REPS,
    tolerance: float = DEFAULT_TOLERANCE,
    abs_floor_ms: float = DEFAULT_ABS_FLOOR_MS,
) -> GateRun:
    """Seed a cache, build the digests (cold, then warm) and time every workload.

    Returns the best time of each phase per backend and workload, and the
    digests per backend (``"cold"``: first pass over the payload tier,
    ``"warm"``: after the frame tiers were written). With a ``baseline``, a
    workload with a phase over its limit is run ``confirm_reps`` more times
    (its best time can only drop): one descheduled run must not fail the
    gate, a real regression is slow on every run.
    """
    import orjson

    t0 = time.perf_counter()
    run = GateRun()
    with tempfile.TemporaryDirectory(prefix="tif1-perf-gate-", dir=work_dir) as tmp:
        root = Path(tmp)
        home = root / "home"
        home.mkdir()
        cache_dir = root / "cache"
        env = child_env(cache_dir, home, src)
        _run(["--seed", str(cache_dir)], env)
        if inject_sleep is not None:
            env[INJECT_ENV] = str(inject_sleep)

        def measure(lib: str, workload: str) -> None:
            rec = orjson.loads(_run(["--child", workload, "--lib", lib], env))
            slot = run.phases.setdefault(lib, {}).setdefault(workload, {})
            samples = run.samples.setdefault(lib, {}).setdefault(workload, {})
            for phase, ms in rec.items():
                slot[phase] = min(ms, slot.get(phase, math.inf))
                samples.setdefault(phase, []).append(ms)

        for lib in libs:
            if digests:
                out = root / f"digest_{lib}_cold.json"
                _run(["--digest", lib, "--out", str(out)], env)
                run.digests.setdefault(lib, {})["cold"] = orjson.loads(out.read_bytes())
            else:
                # Prime the frame tiers: the timed runs must read them warm.
                for workload in workloads:
                    _run(["--child", workload, "--lib", lib], env)
            for _ in range(reps):
                for workload in workloads:
                    measure(lib, workload)
        if baseline is not None and confirm_reps > 0:
            suspects = sorted(
                {
                    (r.lib, r.workload)
                    for r in compare(baseline, run.phases, tolerance, abs_floor_ms).regressions
                }
            )
            for _ in range(confirm_reps):
                for lib, workload in suspects:
                    measure(lib, workload)
            run.confirmed = suspects
        if digests:
            for lib in libs:
                out = root / f"digest_{lib}_warm.json"
                _run(["--digest", lib, "--out", str(out)], env)
                run.digests[lib]["warm"] = orjson.loads(out.read_bytes())
    run.seconds = time.perf_counter() - t0
    return run


# -- comparison --------------------------------------------------------------------


@dataclass
class PhaseResult:
    """One phase compared with its baseline."""

    lib: str
    workload: str
    phase: str
    baseline_ms: float
    measured_ms: float
    expected_ms: float
    limit_ms: float

    @property
    def regressed(self) -> bool:
        return self.measured_ms > self.limit_ms

    @property
    def ratio(self) -> float:
        return self.measured_ms / self.expected_ms if self.expected_ms > 0 else math.inf


@dataclass
class Comparison:
    """The phases of one run against the baseline."""

    factor: float
    results: list[PhaseResult]
    missing: list[str]

    @property
    def regressions(self) -> list[PhaseResult]:
        return [r for r in self.results if r.regressed]

    @property
    def ok(self) -> bool:
        return not self.regressions and not self.missing


def _base_stats(entry: Any) -> tuple[float, float]:
    """``(min, median)`` of a baseline phase (a bare number stands for both)."""
    if isinstance(entry, dict):
        return float(entry["min"]), float(entry["median"])
    return float(entry), float(entry)


def machine_factor(baseline: dict[str, Any], measured: dict[str, Any]) -> float:
    """Median measured/baseline ratio (best times) over the phases long enough to time."""
    ratios = [
        ms / base_min
        for lib, workloads in measured.items()
        for workload, phases in workloads.items()
        for phase, ms in phases.items()
        if (entry := baseline.get(lib, {}).get(workload, {}).get(phase)) is not None
        and (base_min := _base_stats(entry)[0]) >= _FACTOR_MIN_MS
    ]
    if not ratios:
        return 1.0
    return min(max(statistics.median(ratios), 1 / _FACTOR_CLAMP), _FACTOR_CLAMP)


def compare(
    baseline: dict[str, Any],
    measured: dict[str, Any],
    tolerance: float = DEFAULT_TOLERANCE,
    abs_floor_ms: float = DEFAULT_ABS_FLOOR_MS,
    factor: float | None = None,
) -> Comparison:
    """Compare measured best times (``{lib: {workload: {phase: ms}}}``) with the baseline.

    Baseline phases are ``{"min", "median"}`` over the baseline's runs. The
    factor compares best with best; the limit starts from the baseline
    median: a phase regresses when its best time is above ``median * factor
    * tolerance`` and more than ``abs_floor_ms`` above ``median * factor``.
    A phase whose runs spread widely (polars' first numpy use) thus gets the
    room its own spread needs, a steady one (median close to min) keeps the
    tight limit. A phase that the baseline has but the run lacks is reported
    as missing (a workload lost a step); a new phase without a baseline is
    ignored until the baseline is rewritten.
    """
    if factor is None:
        factor = machine_factor(baseline, measured)
    results: list[PhaseResult] = []
    missing: list[str] = []
    for lib, workloads in measured.items():
        for workload, phases in workloads.items():
            base_phases = baseline.get(lib, {}).get(workload)
            if base_phases is None:
                continue
            missing.extend(
                f"{lib}/{workload}/{phase}" for phase in base_phases if phase not in phases
            )
            for phase, ms in phases.items():
                entry = base_phases.get(phase)
                if entry is None:
                    continue
                median = _base_stats(entry)[1]
                expected = median * factor
                limit = max(expected * tolerance, expected + abs_floor_ms)
                results.append(PhaseResult(lib, workload, phase, median, ms, expected, limit))
    return Comparison(factor, results, missing)


def format_table(comparison: Comparison, only_regressions: bool = False) -> str:
    """A fixed-width table of the compared phases (regressions marked)."""
    rows = comparison.regressions if only_regressions else comparison.results
    lines = [
        (
            f"machine factor {comparison.factor:.2f} (median measured/baseline); "
            f"{len(comparison.regressions)} of {len(comparison.results)} phases regressed"
        ),
        (
            f"{'backend':8s} {'workload':20s} {'phase':22s} {'base':>8s} {'expect':>8s} "
            f"{'limit':>8s} {'now':>8s} {'ratio':>6s}"
        ),
    ]
    for r in rows:
        mark = "  REGRESSED" if r.regressed else ""
        lines.append(
            f"{r.lib:8s} {r.workload:20s} {r.phase:22s} {r.baseline_ms:8.2f} "
            f"{r.expected_ms:8.2f} {r.limit_ms:8.2f} {r.measured_ms:8.2f} {r.ratio:6.2f}{mark}"
        )
    lines.extend(f"MISSING phase {name}" for name in comparison.missing)
    return "\n".join(lines)


def baseline_document(run: GateRun, reps: int) -> dict[str, Any]:
    """The JSON written by ``--write-baseline``: min and median of every phase."""
    phases = {
        lib: {
            workload: {
                phase: {"min": round(min(vals), 3), "median": round(statistics.median(vals), 3)}
                for phase, vals in by_phase.items()
            }
            for workload, by_phase in workloads.items()
        }
        for lib, workloads in run.samples.items()
    }
    return {
        "version": 1,
        "meta": {
            "python": platform.python_version(),
            "machine": platform.machine(),
            "system": platform.system(),
            "reps": reps,
            "statistic": "min and median over reps, ms, in-process phases",
        },
        "phases": phases,
    }


def load_baseline(path: Path | None = None) -> dict[str, Any]:
    """The committed baseline (``{lib: {workload: {phase: {"min", "median"}}}}``)."""
    import orjson

    return orjson.loads((path or BASELINE).read_bytes())["phases"]


def write_outputs(run: GateRun, reps: int) -> None:
    """Rewrite the committed baseline and the parity digests from ``run``.

    The workloads ``run`` measured replace their baseline entries; the others
    (a run restricted with ``--libs``/``--workloads``) are kept.
    """
    import orjson

    opts = orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS
    document = baseline_document(run, reps)
    if BASELINE.exists():
        phases = orjson.loads(BASELINE.read_bytes())["phases"]
        for lib, workloads in document["phases"].items():
            phases.setdefault(lib, {}).update(workloads)
        document["phases"] = phases
    BASELINE.write_bytes(orjson.dumps(document, option=opts) + b"\n")
    for lib, pair in run.digests.items():
        warm, cold = pair["warm"], pair["cold"]
        delta = {
            k: cold.get(k) for k in sorted(set(warm) | set(cold)) if cold.get(k) != warm.get(k)
        }
        parity_path(lib).write_bytes(_dump_digest(warm))
        cold_delta_path(lib).write_bytes(_dump_digest(delta))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--child", help=argparse.SUPPRESS)
    parser.add_argument("--seed", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--digest", choices=LIBS, help=argparse.SUPPRESS)
    parser.add_argument("--out", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--lib", default="pandas", choices=LIBS, help=argparse.SUPPRESS)
    parser.add_argument("--libs", default=",".join(LIBS))
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument(
        "--reps", type=int, default=None, help=f"default {DEFAULT_REPS}, {BASELINE_REPS} to write"
    )
    parser.add_argument("--confirm-reps", type=int, default=DEFAULT_CONFIRM_REPS)
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE)
    parser.add_argument("--abs-floor-ms", type=float, default=DEFAULT_ABS_FLOOR_MS)
    parser.add_argument("--no-digest", action="store_true", help="skip the parity digests")
    parser.add_argument("--write-baseline", action="store_true")
    parser.add_argument("--json", type=Path, help="also write the measured phases here")
    args = parser.parse_args()
    if args.reps is None:
        args.reps = BASELINE_REPS if args.write_baseline else DEFAULT_REPS

    import orjson

    if args.seed:
        _seed(args.seed)
        return
    if args.child:
        print(orjson.dumps(_child(args.child, args.lib)).decode("utf-8"))
        return
    if args.digest:
        digest = build_digest(args.digest)
        args.out.write_bytes(orjson.dumps(digest, option=orjson.OPT_SORT_KEYS))
        return

    libs = tuple(args.libs.split(","))
    workloads = tuple(args.workloads.split(","))
    run = run_gate(
        libs,
        workloads,
        args.reps,
        digests=not args.no_digest,
        baseline=None if args.write_baseline else load_baseline(),
        confirm_reps=args.confirm_reps,
        tolerance=args.tolerance,
        abs_floor_ms=args.abs_floor_ms,
    )
    if args.json:
        args.json.write_bytes(
            orjson.dumps(baseline_document(run, args.reps), option=orjson.OPT_INDENT_2)
        )
    if args.write_baseline:
        write_outputs(run, args.reps)
        print(f"wrote {BASELINE} and the parity digests ({run.seconds:.0f} s)")
        return
    comparison = compare(load_baseline(), run.phases, args.tolerance, args.abs_floor_ms)
    print(format_table(comparison))
    if run.confirmed:
        print("re-measured: " + ", ".join(f"{lib}/{w}" for lib, w in run.confirmed))
    failed = not comparison.ok
    for lib, pair in run.digests.items():
        for kind, digest in pair.items():
            expected = expected_digest(lib, kind)
            diffs = compare_digests(expected, digest)
            print(f"parity {lib} ({kind}): {len(diffs)} differing surfaces of {len(expected)}")
            for line in diffs[:40]:
                print(f"  {line}")
            failed = failed or bool(diffs)
    print(f"gate run took {run.seconds:.0f} s")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
