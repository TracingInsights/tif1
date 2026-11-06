"""Naxos N9: timeline of a cold eager load / ``fetch_all_laps_telemetry()``.

Runs one cold workload (empty cache, live CDN) in a fresh interpreter with
light instrumentation patched in from outside the library:

- every response body decoded by the fetch pipeline (time, bytes);
- the fetch batch (``fetch_multiple_async``) start/end;
- every background writer thread (payload tier / frame tier) start/end;
- the atexit frame-writer join and the cache close;
- a 50 ms sampler: per-thread CPU (``/proc/self/task/*/stat``), process
  read/write bytes (``/proc/self/io``).

The parent measures the whole-process wall time and prints a summary; the
full trace is written as JSON with ``--out``.

Usage (from the repo root)::

    python tools/perf/naxos_n9_trace.py --lib pandas --workload all_tel --src src
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
CLK_TCK = os.sysconf("SC_CLK_TCK")


def _thread_cpu() -> dict[str, float]:
    """CPU seconds per thread name (summed over threads that share a name)."""
    out: dict[str, float] = {}
    base = Path("/proc/self/task")
    for task in base.iterdir():
        try:
            stat = (task / "stat").read_text(encoding="utf-8")
            comm = (task / "comm").read_text(encoding="utf-8").strip()
        except OSError:
            continue
        fields = stat[stat.rindex(")") + 2 :].split()
        cpu = (int(fields[11]) + int(fields[12])) / CLK_TCK
        out[comm] = out.get(comm, 0.0) + cpu
    return out


def _io() -> dict[str, int]:
    vals = {}
    for line in Path("/proc/self/io").read_text(encoding="utf-8").splitlines():
        k, _, v = line.partition(":")
        vals[k] = int(v)
    return vals


def _child(workload: str, lib: str, trace_path: str) -> None:
    import atexit

    import orjson

    t0 = time.perf_counter()

    def now() -> float:
        return round((time.perf_counter() - t0) * 1000, 1)

    trace: dict = {"events": [], "responses": [], "samples": [], "writers": []}
    stop = threading.Event()

    main_ident = threading.get_ident()
    trace["stacks"] = []

    def stacks() -> dict[str, list[str]]:
        names = {t.ident: t.name for t in threading.enumerate()}
        out = {}
        for ident, frame in sys._current_frames().items():
            name = "main" if ident == main_ident else names.get(ident, str(ident))
            if name == "n9-sampler":
                continue
            chain = []
            f = frame
            while f is not None and len(chain) < 40:
                code = f.f_code
                chain.append(f"{Path(code.co_filename).name}:{code.co_qualname}:{f.f_lineno}")
                f = f.f_back
            out[name] = chain
        return out

    def sampler() -> None:
        while not stop.is_set():
            trace["samples"].append({"t": now(), "cpu": _thread_cpu(), "io": _io()})
            if os.environ.get("N9_STACKS"):
                trace["stacks"].append((now(), stacks()))
            stop.wait(0.05)

    threading.Thread(target=sampler, name="n9-sampler", daemon=True).start()

    def dump() -> None:
        trace["events"].append(("atexit_last", now()))
        stop.set()
        trace["samples"].append({"t": now(), "cpu": _thread_cpu(), "io": _io()})
        Path(trace_path).write_bytes(orjson.dumps(trace))

    atexit.register(dump)  # registered first: runs last

    import tif1
    import tif1.async_fetch as af
    import tif1.cache as cache_mod

    orig_decode = af._decode_and_parse

    def decode(response, *, telemetry=False):
        t_a = now()
        res = orig_decode(response, telemetry=telemetry)
        content = res[1]
        trace["responses"].append(
            (
                t_a,
                now(),
                len(content) if content is not None else -1,
                threading.current_thread().name,
            )
        )
        return res

    af._decode_and_parse = decode

    orig_stdlib = af._stdlib_payload

    def stdlib_payload(url, bound, telemetry):
        t_a = now()
        res = orig_stdlib(url, bound, telemetry)
        trace["responses"].append(
            (t_a, now(), len(res[1]) if res else -2, "stdlib:" + threading.current_thread().name)
        )
        return res

    af._stdlib_payload = stdlib_payload

    orig_fma = af.fetch_multiple_async

    async def fma(requests, **kw):
        trace["events"].append(("fetch_batch_start", now(), len(requests)))
        try:
            return await orig_fma(requests, **kw)
        finally:
            trace["events"].append(("fetch_batch_end", now(), len(requests)))

    af.fetch_multiple_async = fma
    import tif1.core as core_mod

    if getattr(core_mod, "fetch_multiple_async", None) is orig_fma:
        core_mod.fetch_multiple_async = fma

    orig_reg = cache_mod.register_frame_writer

    def register(write_fn):
        name = getattr(write_fn, "__qualname__", None) or getattr(
            getattr(write_fn, "func", None), "__qualname__", "?"
        )
        rec = {"name": name, "registered": now()}
        trace["writers"].append(rec)

        def wrapped():
            rec["start"] = now()
            try:
                write_fn()
            finally:
                rec["end"] = now()

        orig_reg(wrapped)

    cache_mod.register_frame_writer = register
    af_reg = getattr(af, "register_frame_writer", None)
    if af_reg is orig_reg:
        af.register_frame_writer = register
    if getattr(core_mod, "register_frame_writer", None) is orig_reg:
        core_mod.register_frame_writer = register

    orig_join = cache_mod._join_frame_writers

    def join():
        trace["events"].append(("join_writers_start", now()))
        orig_join()
        trace["events"].append(("join_writers_end", now()))

    cache_mod._join_frame_writers = join

    def step(name, fn):
        trace["events"].append((name + "_start", now()))
        r = fn()
        trace["events"].append((name + "_end", now()))
        return r

    session = step("get_session", lambda: tif1.get_session(YEAR, GP, SESSION, lib=lib))
    if workload == "all_tel":
        step("laps", lambda: session.laps)
        step("all_tel", lambda: session.fetch_all_laps_telemetry())
    elif workload == "notebook":
        step("load", lambda: session.load(weather=True, messages=True))
        laps = session.laps
        step("picks", lambda: [laps.pick_driver("VER"), laps.pick_quicklaps(), laps.pick_fastest()])
        step("driver_tel_x2", lambda: [laps.pick_driver(d).telemetry for d in ("VER", "LEC")])
        step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
        step("results", lambda: session.results)
        step("weather", lambda: session.weather_data)
    elif workload == "prime":
        # The round's cache priming: cold race load + fetch_all + four drivers.
        step("load", lambda: session.load(weather=True, messages=True))
        step("all_tel", lambda: session.fetch_all_laps_telemetry())
        laps = session.laps
        step(
            "drivers_x4",
            lambda: [laps.pick_driver(d).telemetry for d in ("VER", "LEC", "NOR", "PIA")],
        )
    trace["events"].append(("in_process_end", now()))


def _summary(trace: dict, wall: float) -> dict:
    ev = {}
    for e in trace["events"]:
        ev.setdefault(e[0], e[1])
    resp = trace["responses"]
    tel = [r for r in resp if r[2] > 0]
    out = {"process_wall": round(wall, 1)}
    out.update(ev)
    if tel:
        out["first_body"] = min(r[1] for r in tel)
        out["last_body"] = max(r[1] for r in tel)
        out["bodies"] = len(tel)
        out["body_mb"] = round(sum(r[2] for r in tel) / 1e6, 2)
        span = (out["last_body"] - out["first_body"]) / 1000
        out["wave_mb_s"] = round(out["body_mb"] / span, 2) if span > 0 else None
    out["writers"] = [
        {k: w.get(k) for k in ("name", "registered", "start", "end")} for w in trace["writers"]
    ]
    if trace["samples"]:
        last = trace["samples"][-1]["cpu"]
        out["cpu_by_thread_s"] = {k: round(v, 2) for k, v in sorted(last.items()) if v >= 0.05}
    return out


def main() -> None:
    """Run the child (``--child``) or a parent that spawns it and summarizes."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--child", default=None)
    ap.add_argument("--trace", default=None)
    ap.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    ap.add_argument("--workload", default="all_tel")
    ap.add_argument("--src", default="src")
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--out", default=None)
    ap.add_argument("--keep-cache", default=None, help="copy the cache dir here after the run")
    ap.add_argument(
        "--control-src",
        default=None,
        help="interleave this tree (control) with --src (candidate), the order flipping per pair",
    )
    args = ap.parse_args()
    if args.child:
        _child(args.child, args.lib, args.trace)
        return
    import orjson

    if args.control_src:
        arms = []
        for i in range(args.reps):
            pair = [("control", args.control_src), ("candidate", args.src)]
            arms.extend(pair if i % 2 == 0 else pair[::-1])
    else:
        arms = [("candidate", args.src)] * args.reps
    runs = []
    for arm, src in arms:
        args.src = src
        cache_dir = tempfile.mkdtemp(prefix="tif1-n9-cold-")
        trace_path = str(Path(cache_dir).parent / f"n9-trace-{os.getpid()}.json")
        env = dict(os.environ, TIF1_CACHE_DIR=cache_dir)
        env.pop("TIF1_OFFLINE_MODE", None)
        env["PYTHONPATH"] = str(Path(args.src).resolve())
        cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--child",
            args.workload,
            "--lib",
            args.lib,
            "--trace",
            trace_path,
        ]
        t0 = time.perf_counter()
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
        wall = (time.perf_counter() - t0) * 1000
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr[-3000:])
        trace = orjson.loads(Path(trace_path).read_bytes())
        Path(trace_path).unlink()
        db = sum(f.stat().st_size for f in Path(cache_dir).rglob("*") if f.is_file())
        if args.keep_cache:
            shutil.copytree(cache_dir, args.keep_cache, dirs_exist_ok=True)
        shutil.rmtree(cache_dir, ignore_errors=True)
        summ = _summary(trace, wall)
        summ["cache_bytes"] = db
        summ["arm"] = arm
        if "last_body" in summ:
            phase_end = summ.get("all_tel_end", summ.get("load_end"))
            # Network-independent: the CPU work left once the last body arrived.
            summ["after_last_body"] = round(wall - summ["last_body"], 1)
            if phase_end is not None:
                summ["phase_after_last_body"] = round(phase_end - summ["last_body"], 1)
        print(orjson.dumps(summ).decode(), flush=True)
        runs.append({"summary": summ, "trace": trace if args.out else None})
    if args.control_src:
        _print_ab([r["summary"] for r in runs])
    if args.out:
        Path(args.out).write_bytes(orjson.dumps(runs))


def _print_ab(summaries: list[dict]) -> None:
    """Medians, p90 and pairs won per metric for an interleaved control/candidate run."""
    import statistics

    ctl = [s for s in summaries if s["arm"] == "control"]
    cand = [s for s in summaries if s["arm"] == "candidate"]
    for key in ("process_wall", "last_body", "after_last_body", "phase_after_last_body"):
        a = [s.get(key) for s in ctl]
        b = [s.get(key) for s in cand]
        if None in a or None in b:
            continue
        wins = sum(1 for x, y in zip(a, b, strict=True) if y < x)
        p90 = lambda v: sorted(v)[max(0, -(-9 * len(v) // 10) - 1)]  # noqa: E731
        print(
            f"AB {key:22s} control {statistics.median(a):8.0f} (p90 {p90(a):6.0f})"
            f"  candidate {statistics.median(b):8.0f} (p90 {p90(b):6.0f})"
            f"  candidate lower {wins}/{len(a)}",
            flush=True,
        )


if __name__ == "__main__":
    main()
