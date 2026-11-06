"""Naxos N8: request timeline of a cold one-driver telemetry wave.

Runs fresh-process cold workloads (empty cache, live CDN) with tracing hooks
installed from outside the library (nothing in ``src/`` changes): every
stdlib request (connect, TTFB, body, bytes, connection reuse, CDN cache
headers), every niquests request (start, resolved, body parsed, bytes,
connection reuse, HTTP version, cache headers), each ``fetch_json_async``
(path, start, end), hedges and CDN failures (log records), the first-wave
start, the niquests import / session build / warm-up, and the workload
phases. The parent prints a per-run summary and keeps the raw events.

Usage (from the tree root):
    python tools/perf/naxos_n8_trace.py --lib pandas --reps 10 \
        --src /tmp/naxos/control/src --workload driver_tel --out FILE
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
import threading
import time
from pathlib import Path

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
T0 = time.perf_counter()
EVENTS: list[dict] = []
_LOCK = threading.Lock()


def _now() -> float:
    return round((time.perf_counter() - T0) * 1000, 2)


def _ev(kind: str, **fields) -> None:
    fields["kind"] = kind
    fields.setdefault("t", _now())
    fields["thread"] = threading.current_thread().name
    with _LOCK:
        EVENTS.append(fields)


def _short(url: str) -> str:
    for marker in ("/Race/", "/Qualifying/"):
        if marker in url:
            return url.split(marker, 1)[1]
    return url[-60:]


def _host(url: str) -> str:
    from urllib.parse import urlsplit

    return urlsplit(url).hostname or ""


def _install_hooks() -> None:
    import http.client
    import logging

    from tif1 import async_fetch, first_wave, http_session

    # -- stdlib connections ---------------------------------------------------
    class TracedHTTPS(http.client.HTTPSConnection):
        def connect(self):
            t = _now()
            super().connect()
            self._n8_connect = (t, _now())

        def getresponse(self):
            r = super().getresponse()
            self._n8_ttfb = _now()
            self._n8_headers = {
                k.lower(): v
                for k, v in r.getheaders()
                if k.lower()
                in ("x-cache", "age", "content-length", "x-served-by", "cf-cache-status")
            }
            return r

    first_wave._connection_factory = TracedHTTPS
    orig_request = first_wave._StdlibClient._request

    def traced_request(self, key, connection, target, timeout):
        new = getattr(connection, "sock", None) is None
        t = _now()
        connection._n8_connect = None
        status = body = None
        err = None
        try:
            status, body = orig_request(self, key, connection, target, timeout)
            return status, body
        except Exception as e:
            err = type(e).__name__
            raise
        finally:
            _ev(
                "stdlib",
                start=t,
                t=_now(),
                host=key[0],
                path=_short(target),
                new_conn=new,
                connect=getattr(connection, "_n8_connect", None),
                ttfb=getattr(connection, "_n8_ttfb", None),
                headers=getattr(connection, "_n8_headers", None),
                status=status,
                bytes=len(body) if body else 0,
                err=err,
            )

    first_wave._StdlibClient._request = traced_request

    orig_start = first_wave.start

    def traced_start(urls, *, timeout):
        _ev("first_wave_start", urls=[_short(u) for u in urls])
        return orig_start(urls, timeout=timeout)

    first_wave.start = traced_start

    orig_warm_niquests = first_wave.warm_niquests

    def traced_warm_niquests():
        if not first_wave._warm_started:
            _ev("warm_niquests_called")
        return orig_warm_niquests()

    first_wave.warm_niquests = traced_warm_niquests
    if os.environ.get("N8_NO_WARM"):
        # Experiment: the bridge serves the whole wave, niquests never warms up.
        first_wave.warm_niquests = lambda: None

    # -- niquests -------------------------------------------------------------
    orig_create = http_session._create_session

    def traced_create():
        t = _now()
        s = orig_create()
        _ev("niquests_session_built", start=t)
        return s

    http_session._create_session = traced_create

    orig_warm = http_session.warm_connections

    def traced_warm(urls, timeout):
        t = _now()
        orig_warm(urls, timeout)
        events = list(http_session._warmups)
        _ev("warm_connections_started", start=t, n=len(events))

        def watch():
            for ev in events:
                ev.wait(30)
            _ev("warm_connections_done")

        threading.Thread(target=watch, daemon=True).start()

    http_session.warm_connections = traced_warm

    orig_resolved = async_fetch._get_resolved

    def traced_resolved(session_client, url, timeout):
        t = _now()
        try:
            r = orig_resolved(session_client, url, timeout)
        except Exception as e:
            _ev("niq_err", start=t, path=_short(url), host=_host(url), err=repr(e)[:200])
            raise
        info = getattr(r, "conn_info", None)
        _ev(
            "niq_resolved",
            start=t,
            path=_short(url),
            host=_host(url),
            status=getattr(r, "status_code", None),
            version=str(getattr(r, "http_version", "")),
            reused=http_session._response_reused_connection(r),
            established=getattr(info, "established_latency", None)
            and info.established_latency.total_seconds(),
            tls=getattr(info, "tls_handshake_latency", None)
            and info.tls_handshake_latency.total_seconds(),
            headers={
                k.lower(): v
                for k, v in (getattr(r, "headers", None) or {}).items()
                if k.lower()
                in ("x-cache", "age", "content-length", "x-served-by", "cf-cache-status")
            },
        )
        r._n8_path = _short(url)
        return r

    async_fetch._get_resolved = traced_resolved

    orig_decode = async_fetch._decode_and_parse

    def traced_decode(response, *, telemetry=False):
        t = _now()
        parsed, content = orig_decode(response, telemetry=telemetry)
        _ev(
            "niq_body",
            start=t,
            path=getattr(response, "_n8_path", "?"),
            bytes=len(content) if content is not None else None,
        )
        return parsed, content

    async_fetch._decode_and_parse = traced_decode

    orig_fja = async_fetch.fetch_json_async

    async def traced_fja(year, gp, session, path, *args, **kwargs):
        t = _now()
        err = None
        try:
            return await orig_fja(year, gp, session, path, *args, **kwargs)
        except BaseException as e:
            err = type(e).__name__
            raise
        finally:
            _ev("fetch_json", start=t, path=path, err=err)

    async_fetch.fetch_json_async = traced_fja

    orig_fma = async_fetch.fetch_multiple_async

    async def traced_fma(requests, **kwargs):
        t = _now()
        try:
            return await orig_fma(requests, **kwargs)
        finally:
            _ev(
                "fetch_multiple",
                start=t,
                n=len(requests),
                first=requests[0][3] if requests else None,
                max_concurrent=kwargs.get("max_concurrent_requests"),
            )

    async_fetch.fetch_multiple_async = traced_fma

    # -- log records (hedges, CDN failures, warnings) ---------------------------
    class Capture(logging.Handler):
        def emit(self, record):
            msg = record.getMessage()
            if record.levelno >= logging.WARNING or "hedg" in msg or "slow" in msg:
                _ev("log", level=record.levelname, logger=record.name, msg=msg[:300])

    handler = Capture(level=logging.DEBUG)
    for name in ("tif1.cdn", "tif1.async_fetch", "tif1.first_wave", "tif1.http_session"):
        lg = logging.getLogger(name)
        lg.addHandler(handler)
        lg.setLevel(logging.DEBUG)
        lg.propagate = False


def _child(workload: str, lib: str) -> None:
    out: dict[str, float] = {}

    def step(name, fn):
        t0 = time.perf_counter()
        _ev("phase_start", name=name)
        r = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        _ev("phase_end", name=name)
        return r

    import tif1

    _install_hooks()
    out["import"] = _now()
    session = step("get_session", lambda: tif1.get_session(YEAR, GP, SESSION, lib=lib))
    if workload == "laps":
        step("laps", lambda: session.laps)
    elif workload == "fastest_lap_tel":
        step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
    elif workload == "driver_tel":
        step("laps", lambda: session.laps)
        before = set(sys.modules)
        if os.environ.get("N8_SAMPLE"):
            # Main-thread stack sampler (1 ms), recorded as events.
            main_id = threading.main_thread().ident
            stop = threading.Event()

            def sampler():
                while not stop.wait(0.001):
                    frame = sys._current_frames().get(main_id)
                    stack = []
                    while frame is not None and len(stack) < 40:
                        code = frame.f_code
                        stack.append(f"{Path(code.co_filename).name}:{code.co_name}")
                        frame = frame.f_back
                    _ev("sample", stack=stack)

            threading.Thread(target=sampler, daemon=True).start()
            step("driver_tel", lambda: session.laps.pick_driver("VER").telemetry)
            stop.set()
        elif os.environ.get("N8_PROFILE"):
            import cProfile
            import pstats

            prof = cProfile.Profile()
            prof.enable()
            step("driver_tel", lambda: session.laps.pick_driver("VER").telemetry)
            prof.disable()
            with open(os.environ["N8_PROFILE_OUT"], "a", encoding="utf-8") as fh:
                stats = pstats.Stats(prof, stream=fh)
                stats.sort_stats(os.environ["N8_PROFILE"]).print_stats(45)
        else:
            step("driver_tel", lambda: session.laps.pick_driver("VER").telemetry)
        new_modules = sorted(set(sys.modules) - before)
        _ev("modules_imported_in_driver_tel", n=len(new_modules), names=new_modules)
    out["in_process_total"] = _now()
    _ev("child_done")
    print(json.dumps({"phases": out, "events": EVENTS}, default=str))


def _run_child(workload: str, lib: str, src_dir: str, env_extra: dict[str, str]) -> dict:
    cache_dir = tempfile.mkdtemp(prefix="tif1-n8-trace-")
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir, **env_extra)
    env.pop("TIF1_OFFLINE_MODE", None)
    env["PYTHONPATH"] = str(Path(src_dir).resolve())
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    shutil.rmtree(cache_dir, ignore_errors=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} failed: {proc.stderr[-3000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    rec["stderr_tail"] = proc.stderr[-1500:]
    return rec


def summarize(rec: dict) -> str:
    ev = rec["events"]
    ph = rec["phases"]
    tel = [e for e in ev if e["kind"] == "fetch_json" and e["path"].endswith("_tel.json")]
    std = [e for e in ev if e["kind"] == "stdlib" and e["path"].endswith("_tel.json")]
    niq = [e for e in ev if e["kind"] == "niq_resolved" and e["path"].endswith("_tel.json")]
    hedges = [e for e in ev if e["kind"] == "log" and "hedg" in e["msg"]]
    warns = [e for e in ev if e["kind"] == "log" and e["level"] in ("WARNING", "ERROR")]
    ready = [e["t"] for e in ev if e["kind"] == "warm_connections_done"]
    dts = sorted(e["t"] - e["start"] for e in tel)
    ds = next((e for e in ev if e["kind"] == "phase_start" and e["name"] == "driver_tel"), None)
    first_tel = min((e["start"] for e in tel), default=None)
    last_tel = max((e["t"] for e in tel), default=None)
    parts = [
        f"wall={rec['process_wall']:.0f}",
        " ".join(f"{k}={v:.0f}" for k, v in ph.items()),
    ]
    if tel:
        parts.append(
            f"tel n={len(tel)} std={len(std)} niq={len(niq)} "
            f"wave={first_tel - (ds['t'] if ds else 0):.0f}+{last_tel - first_tel:.0f}"
            f" req p50={statistics.median(dts):.0f} max={dts[-1]:.0f}"
        )
    if ready:
        parts.append(f"niq_ready={ready[0]:.0f}")
    if hedges:
        parts.append(f"hedges={len(hedges)}")
    if warns:
        parts.append(f"warn={len(warns)}")
    return "  ".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--src", default="src")
    parser.add_argument("--workload", default="driver_tel")
    parser.add_argument("--env", action="append", default=[], help="KEY=VALUE for the child")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib)
        return
    env_extra = dict(item.split("=", 1) for item in args.env)
    recs = []
    for i in range(args.reps):
        rec = _run_child(args.workload, args.lib, args.src, env_extra)
        recs.append(rec)
        print(f"[{args.lib} {args.workload} {i}] {summarize(rec)}", flush=True)
    if args.out:
        Path(args.out).write_text(json.dumps(recs) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
