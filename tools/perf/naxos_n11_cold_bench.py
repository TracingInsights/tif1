"""Naxos N11: fresh-process cold (empty cache, live CDN) lazy-telemetry workloads.

A copy of Elis E10's ``elis_e10_cold_bench.py`` for the lazy default: both arms
run with ``TIF1_LOAD_TELEMETRY_LAZY=true`` (so ``load()`` is lazy), plus
per-arm ``--control-env`` / ``--candidate-env K=V``.

Workloads (2025 Bahrain race, ``load()`` first in each):

- ``nb4``: for VER, LEC, NOR, PIA: ``laps.pick_driver(d).telemetry`` and
  ``get_driver(d).get_fastest_lap().get_telemetry()``.
- ``fastest1``: one driver's (VER) fastest-lap telemetry.
- ``fetch_all``: ``fetch_all_laps_telemetry()``.
- ``iterlaps``: ``lap.telemetry`` for every lap (``iterlaps()``, laps-row order).
- ``fastest4``: ``laps.pick_driver(d).pick_fastest().get_telemetry()`` for the four.
- ``driver_loop``: ``lap.telemetry`` for every lap of VER.

``--trace DIR`` (single-arm mode, or both arms) makes each child write an
in-process timeline (``N11_TRACE``): stream requests (key, priority, start,
end), niquests GETs, stdlib bridge GETs, the niquests warm-up, each lazy wave
(drivers, wait, frame build, frame-tier scheduling). Summarise it with
``naxos_n11_timeline.py``.

Usage:
    python tools/perf/naxos_n11_cold_bench.py --lib pandas --reps 5 \
        --workloads nb4,fastest1 --control-src /tmp/naxos/control/src \
        --candidate-src src [--out FILE]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

T_START = time.perf_counter()
YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
WORKLOADS = ("nb4", "fastest1", "fetch_all", "iterlaps")
FOUR_DRIVERS = ("VER", "LEC", "NOR", "PIA")


def _install_trace(path: str) -> None:
    """Record a timeline of the lazy path (patched seams) and dump it at exit."""
    import threading

    events: list[dict] = []
    lock = threading.Lock()

    def now() -> float:
        return round((time.perf_counter() - T_START) * 1000, 2)

    def add(kind: str, start: float, **extra) -> None:
        rec = {
            "kind": kind,
            "start": start,
            "end": now(),
            "thread": threading.current_thread().name,
        }
        rec.update(extra)
        with lock:
            events.append(rec)

    from tif1 import async_fetch, first_wave, http_session
    from tif1.core import Session
    from tif1.core_utils import telemetry_stream

    def wrap_sync(owner, name, kind, describe=None):
        original = getattr(owner, name)

        def wrapper(*args, **kwargs):
            t0 = now()
            try:
                return original(*args, **kwargs)
            finally:
                add(kind, t0, **(describe(*args, **kwargs) if describe else {}))

        setattr(owner, name, wrapper)

    wrap_sync(async_fetch, "_get_resolved", "niquests_get", lambda _s, url, _t: {"url": url[-40:]})
    wrap_sync(
        async_fetch, "_stdlib_payload", "stdlib_get", lambda url, _t, _tel: {"url": url[-40:]}
    )
    wrap_sync(first_wave, "_run_niquests_warmup", "niquests_warmup")
    wrap_sync(http_session, "warm_connections", "warm_connections")
    wrap_sync(
        Session,
        "_fetch_lazy_telemetry_wave",
        "wave",
        lambda _self, refs: {"drivers": list(dict.fromkeys(r[0] for r in refs)), "laps": len(refs)},
    )
    wrap_sync(Session, "_stream_telemetry_payloads", "wave_wait")
    wrap_sync(Session, "_prefetch_telemetry_payloads", "wave_prefetch")
    wrap_sync(Session, "_frames_from_payloads", "frames_build")
    wrap_sync(Session, "_schedule_frame_materialization", "frames_schedule")
    wrap_sync(Session, "_close_lazy_telemetry_stream", "stream_close")
    if hasattr(Session, "fetch_all_laps_telemetry"):
        wrap_sync(Session, "fetch_all_laps_telemetry", "fetch_all")

    import tif1.cache as cache_module
    import tif1.core as core_module

    original_register = cache_module.register_frame_writer

    def register(write_fn) -> None:
        label = getattr(write_fn, "__name__", None) or getattr(
            getattr(write_fn, "func", None), "__name__", "writer"
        )

        def timed() -> None:
            t0 = now()
            try:
                write_fn()
            finally:
                add("writer", t0, label=label)

        original_register(timed)

    cache_module.register_frame_writer = register
    core_module.register_frame_writer = register

    original_run = telemetry_stream.PriorityFetchStream._run

    async def run(self, key, request, background):
        t0 = now()
        try:
            return await original_run(self, key, request, background)
        finally:
            add("stream_req", t0, key=list(key), bg=bool(background))

    telemetry_stream.PriorityFetchStream._run = run

    def dump() -> None:
        with lock:
            data = {"events": list(events), "exit_hook": now()}
        Path(path).write_text(json.dumps(data), encoding="utf-8")

    import atexit

    atexit.register(dump)


def _child(workload: str, lib: str) -> None:
    out: dict[str, float] = {}

    def step(name, fn):
        t0 = time.perf_counter()
        r = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        out[f"{name}@"] = round((time.perf_counter() - T_START) * 1000, 2)
        return r

    import tif1

    out["import"] = round((time.perf_counter() - T_START) * 1000, 2)
    trace = os.environ.get("N11_TRACE")
    if trace:
        _install_trace(trace)
    session = step("get_session", lambda: tif1.get_session(YEAR, GP, SESSION, lib=lib))
    step("load", lambda: session.load())
    laps = session.laps
    if workload == "nb4":
        for d in FOUR_DRIVERS:
            step(f"merged_{d}", lambda d=d: laps.pick_driver(d).telemetry)
            step(
                f"fastest_{d}", lambda d=d: session.get_driver(d).get_fastest_lap().get_telemetry()
            )
    elif workload == "fastest1":
        step("fastest_VER", lambda: session.get_driver("VER").get_fastest_lap().get_telemetry())
    elif workload == "fastest4":
        for d in FOUR_DRIVERS:
            step(f"fastest_{d}", lambda d=d: laps.pick_driver(d).pick_fastest().get_telemetry())
    elif workload == "driver_loop":
        step(
            "driver_loop",
            lambda: [lap.telemetry for _, lap in laps.pick_driver("VER").iterlaps()],
        )
    elif workload == "fetch_all":
        step("all_tel", lambda: session.fetch_all_laps_telemetry())
    elif workload == "iterlaps":
        step("iterlaps_tel_loop", lambda: [lap.telemetry for _, lap in laps.iterlaps()])
    else:
        raise SystemExit(f"unknown workload {workload!r}")
    out["in_process_total"] = round((time.perf_counter() - T_START) * 1000, 2)
    print(json.dumps(out), flush=True)


def _run_child(
    workload: str, lib: str, src_dir: str | None, extra_env: dict[str, str], trace: str | None
) -> dict:
    cache_dir = tempfile.mkdtemp(prefix="tif1-n11-cold-")
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir, TIF1_LOAD_TELEMETRY_LAZY="true")
    env.pop("TIF1_OFFLINE_MODE", None)
    env.pop("N11_TRACE", None)
    env.update(extra_env)
    if trace:
        env["N11_TRACE"] = trace
    if src_dir:
        env["PYTHONPATH"] = str(Path(src_dir).resolve())
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    shutil.rmtree(cache_dir, ignore_errors=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    # Time from the last in-process step to process exit (atexit joins, teardown).
    rec["exit_tail"] = round(wall - rec["in_process_total"], 2)
    rec["stderr_warnings"] = proc.stderr.count("WARNING")
    return rec


def _summarize(recs: list[dict]) -> dict:
    phases = {}
    for key in recs[0]:
        vals = sorted(r[key] for r in recs if key in r)
        p90 = vals[min(len(vals) - 1, round(0.9 * (len(vals) - 1)))]
        phases[key] = {
            "median": round(statistics.median(vals), 1),
            "min": round(vals[0], 1),
            "p90": round(p90, 1),
        }
    return phases


def _env_pairs(values: list[str]) -> dict[str, str]:
    return dict(value.split("=", 1) for value in values)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--control-src", default=None)
    parser.add_argument("--candidate-src", default=None)
    parser.add_argument("--control-env", action="append", default=[])
    parser.add_argument("--candidate-env", action="append", default=[])
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--trace", default=None, help="directory for per-run timelines")
    parser.add_argument("--out", default=None)
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib)
        return
    control_env = _env_pairs(args.control_env)
    candidate_env = _env_pairs(args.candidate_env)
    if args.trace:
        Path(args.trace).mkdir(parents=True, exist_ok=True)

    def trace_path(arm: str, workload: str, i: int) -> str | None:
        if not args.trace:
            return None
        return str(Path(args.trace) / f"{args.lib}_{workload}_{arm}_{i}.json")

    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        if args.control_src:
            ctl, cand = [], []
            for i in range(args.reps):
                order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
                for arm in order:
                    if arm == "control":
                        ctl.append(
                            _run_child(
                                workload,
                                args.lib,
                                args.control_src,
                                control_env,
                                trace_path(arm, workload, i),
                            )
                        )
                    else:
                        cand.append(
                            _run_child(
                                workload,
                                args.lib,
                                args.candidate_src,
                                candidate_env,
                                trace_path(arm, workload, i),
                            )
                        )
                print(
                    f"  pair {i}: control {ctl[-1]['process_wall']:.0f} "
                    f"candidate {cand[-1]['process_wall']:.0f}",
                    flush=True,
                )
            wins = sum(
                1 for a, b in zip(ctl, cand, strict=True) if b["process_wall"] < a["process_wall"]
            )
            summary[workload] = {
                "control_env": control_env,
                "candidate_env": candidate_env,
                "control": _summarize(ctl),
                "candidate": _summarize(cand),
                "candidate_faster_pairs": f"{wins}/{args.reps}",
                "control_runs": ctl,
                "candidate_runs": cand,
            }
            c, d = (
                summary[workload]["control"]["process_wall"],
                summary[workload]["candidate"]["process_wall"],
            )
            print(
                f"[{args.lib}] {workload:10s} control {c['median']:.0f} (p90 {c['p90']:.0f})"
                f"  candidate {d['median']:.0f} (p90 {d['p90']:.0f})  faster {wins}/{args.reps}",
                flush=True,
            )
        else:
            recs = [
                _run_child(
                    workload,
                    args.lib,
                    args.candidate_src,
                    candidate_env,
                    trace_path("candidate", workload, i),
                )
                for i in range(args.reps)
            ]
            summary[workload] = _summarize(recs)
            summary[workload]["runs"] = recs  # type: ignore[assignment]
            line = "  ".join(
                f"{k}={v['median']:.0f}"
                for k, v in summary[workload].items()
                if k != "runs" and not k.endswith("@")
            )
            print(f"[{args.lib}] {workload:10s} {line}", flush=True)
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {"lib": args.lib, "label": args.label, "reps": args.reps, "summary": summary},
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
