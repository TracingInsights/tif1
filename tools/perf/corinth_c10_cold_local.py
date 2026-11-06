"""Corinth C10: cold A/B of the session bundle against a local, throttled HTTPS CDN.

Adapted from Naxos N7's ``naxos_n7_cold_local.py``. The live CDN has no session
bundles, so this harness serves the sessions itself: a small HTTP/1.1
keep-alive server over TLS (a throwaway self-signed certificate made with the
``openssl`` CLI) answers the jsDelivr layout of the per-year repositories
(``/gh/TracingInsights/{year}@main/{gp}/{session}/...``: the session JSON files
of the primed caches, gzip) and of the Stats repository
(``/gh/TracingInsights-Archive/Stats@main/{year}/...``: the Ergast tier's files,
re-serialized by orjson), and, unless ``--no-bundles``, each session's
``session.v1.bin`` encoded by ``tools/data/encode_session_bundle.py`` from the
same caches (so a bundle's members are byte-identical to the files served).

Each run is a fresh interpreter on an empty cache directory. The child
replaces ``wassima.generate_ca_bundle`` with the throwaway certificate before
importing tif1, so the production cold path runs unchanged: the stdlib first
wave during the pandas/polars import, the bridge, then niquests. One CDN
source, no hedging (nothing to hedge to), the Ergast merge on. A body is
paced by a token bucket per connection (``--conn-mbps``) and for the link
(``--link-mbps``) after a first-byte delay per request (``--rtt-ms``);
connection set-up is not delayed. Control runs the control tree (per-file
requests), candidate runs this tree with ``TIF1_SESSION_BUNDLE=true``
(``--candidate-env`` adds more); they alternate (ABAB...). ``--verify`` runs
each workload once per arm and compares a digest of the frames it produced.

Workloads (2025 Bahrain Grand Prix race unless noted):

- ``laps``: ``get_session`` + ``laps``;
- ``laps_results``: + ``results``;
- ``load``: ``get_session`` + ``load()`` (laps, weather, race control; lazy
  telemetry registers only) + ``results`` + ``t0_date`` (``session_info.json``);
- ``quali_load``: ``load`` on the qualifying;
- ``loop6``: rounds 1-6 of 2025, ``get_session(2025, r, "R")`` + ``laps`` +
  ``results`` each, in one process (needs ``--season-cache``).

Usage (from the repo root)::

    python tools/perf/corinth_c10_cold_local.py --cache /tmp/corinth/warm-C10 \\
        --season-cache /tmp/corinth/season --control-src /tmp/corinth/control/src \\
        --candidate-src src --lib pandas --reps 8 --workloads laps,load,loop6 --out out.json
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import ssl
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote

YEAR, GP = 2025, "Bahrain Grand Prix"
WORKLOADS = ("laps", "laps_results", "load", "quali_load", "loop6")
PREFIX = "/gh/TracingInsights/"
STATS_PREFIX = "/gh/TracingInsights-Archive/Stats@main/"


# -- child -----------------------------------------------------------------------


def _frame_digest(frames) -> str:
    digest = hashlib.sha256()
    for frame in frames:
        if frame is None:
            digest.update(b"None")
            continue
        if hasattr(frame, "to_pandas") and not hasattr(frame, "iloc"):
            digest.update(repr(frame.schema).encode())
            for name in frame.columns:
                digest.update(name.encode())
                digest.update(repr(frame[name].to_list()).encode())
            continue
        if not hasattr(frame, "columns"):
            digest.update(repr(frame).encode())
            continue
        import pandas as pd

        digest.update(repr(list(frame.columns)).encode())
        digest.update(repr(list(frame.dtypes.astype(str))).encode())
        digest.update(pd.util.hash_pandas_object(frame, index=True).to_numpy().tobytes())
    return digest.hexdigest()


def _child(workload: str, lib: str, base: str, verify: bool) -> None:
    import atexit

    # Registered first, so it runs last: the epoch at which the exit hooks are done.
    atexit.register(lambda: print(f"C10_EXIT {time.time()}", file=sys.stderr, flush=True))
    pem = Path(os.environ["C10_CA_PEM"]).read_text(encoding="utf-8")
    import wassima

    wassima.generate_ca_bundle = lambda *_args, **_kwargs: pem  # trust the local CDN only

    out: dict[str, object] = {"t_child": time.time()}
    t_proc = time.perf_counter()

    def step(name, fn):
        t0 = time.perf_counter()
        r = fn()
        out[name] = round(out.get(name, 0.0) + (time.perf_counter() - t0) * 1000, 2)
        return r

    import tif1
    from tif1.cdn import CDNSource, get_cdn_manager

    manager = get_cdn_manager()
    manager.sources = [CDNSource(name="local", base_url=base, priority=1)]
    manager._failure_counts = {"local": 0}
    out["import"] = round((time.perf_counter() - t_proc) * 1000, 2)
    frames: list = []
    if workload == "loop6":
        for rnd in range(1, 7):
            session = step("get_session", lambda r=rnd: tif1.get_session(YEAR, r, "R", lib=lib))
            frames.append(step("laps", lambda s=session: s.laps))
            frames.append(step("results", lambda s=session: s.results))
    else:
        name = "Qualifying" if workload == "quali_load" else "Race"
        session = step("get_session", lambda: tif1.get_session(YEAR, GP, name, lib=lib))
        if workload in ("laps", "laps_results"):
            frames.append(step("laps", lambda: session.laps))
            if workload == "laps_results":
                frames.append(step("results", lambda: session.results))
        else:
            step("load", lambda: session.load())
            frames.append(session.laps)
            frames.append(step("results", lambda: session.results))
            frames.append(step("t0_date", lambda: session.t0_date))
            frames.append(session.weather_data)
            frames.append(session.race_control_messages)
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    out["pandas_imported"] = "pandas" in sys.modules
    if verify:
        out["digest"] = _frame_digest(frames)
    print(json.dumps(out))


# -- server ----------------------------------------------------------------------


class _Pacer:
    """Token buckets: one for the link, one per connection (bytes per second)."""

    def __init__(self, link_bps: float) -> None:
        self.link_bps = link_bps
        self.link_free = 0.0
        self.lock = threading.Lock()

    def reserve(self, nbytes: int, conn_state: dict, conn_bps: float) -> float:
        now = time.monotonic()
        with self.lock:
            self.link_free = max(self.link_free, now) + nbytes / self.link_bps
            link_at = self.link_free
        conn_state["free"] = max(conn_state.get("free", 0.0), now) + nbytes / conn_bps
        return max(link_at, conn_state["free"]) - now


class LocalCDN:
    def __init__(self, files: dict[str, bytes], gz: set[str], args, tls: ssl.SSLContext) -> None:
        self.files = files
        self.gz = gz
        self.args = args
        self.tls = tls
        self.pacer = _Pacer(args.link_mbps * 1e6)
        self.requests: dict[str, int] = defaultdict(int)
        self.log: list[str] = []
        self.loop = asyncio.new_event_loop()
        self.port = 0

    def start(self) -> None:
        ready = threading.Event()

        def run() -> None:
            asyncio.set_event_loop(self.loop)
            server = self.loop.run_until_complete(
                asyncio.start_server(self._handle, "127.0.0.1", 0, backlog=256, ssl=self.tls)
            )
            self.port = server.sockets[0].getsockname()[1]
            ready.set()
            self.loop.run_forever()

        threading.Thread(target=run, daemon=True).start()
        ready.wait()

    async def _handle(self, reader, writer) -> None:
        conn: dict = {}
        self.requests["connections"] += 1
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                while True:
                    h = await reader.readline()
                    if h in (b"\r\n", b"\n", b""):
                        break
                parts = line.decode("latin-1").split()
                method = parts[0] if parts else "GET"
                target = parts[1] if len(parts) > 1 else "/"
                key = unquote(target.split("?")[0])
                body = self.files.get(key)
                await asyncio.sleep(self.args.rtt_ms / 1000)
                if self.args.log_paths:
                    self.log.append(f"{method} {key} {'404' if body is None else len(body)}")
                if body is None:
                    self.requests["404"] += 1
                    writer.write(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n"
                        b"Content-Type: text/plain\r\n\r\nnot found"
                    )
                    await writer.drain()
                    continue
                kind = "bin" if key.endswith(".bin") else "json"
                if method == "HEAD":
                    self.requests["head"] += 1
                else:
                    self.requests[kind] += 1
                    self.requests[f"{kind}_bytes"] += len(body)
                head = [
                    "HTTP/1.1 200 OK",
                    f"Content-Length: {len(body)}",
                    "Content-Type: "
                    + ("application/octet-stream" if kind == "bin" else "application/json"),
                ]
                if key in self.gz:
                    head.append("Content-Encoding: gzip")
                writer.write(("\r\n".join(head) + "\r\n\r\n").encode())
                if method == "HEAD":
                    await writer.drain()
                    continue
                chunk = 16384
                for i in range(0, len(body), chunk):
                    piece = body[i : i + chunk]
                    delay = self.pacer.reserve(len(piece), conn, self.args.conn_mbps * 1e6)
                    if delay > 0:
                        await asyncio.sleep(delay)
                    writer.write(piece)
                    await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError, ssl.SSLError):
            return
        finally:
            writer.close()


def _tls_files(workdir: Path) -> tuple[Path, Path]:
    """A throwaway self-signed certificate for 127.0.0.1 (``openssl req -x509``)."""
    cert, key = workdir / "cert.pem", workdir / "key.pem"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
            "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1,DNS:localhost",
            "-keyout", str(key), "-out", str(cert),
        ],
        check=True,
        capture_output=True,
    )  # fmt: skip
    return cert, key


def _cache_files(cache_dir: str) -> dict[str, bytes]:
    """jsDelivr path -> body of every session JSON file and Stats file of a tif1 cache."""
    import orjson

    from tif1.cache import _decode_sqlite_value

    conn = sqlite3.connect(f"file:{Path(cache_dir).as_posix()}/cache.sqlite?mode=ro", uri=True)
    files: dict[str, bytes] = {}
    for key, data in conn.execute("SELECT key, data FROM cache"):
        raw = _decode_sqlite_value(data)
        raw = raw if isinstance(raw, bytes) else raw.encode()
        if key.startswith("stats/"):
            payload = orjson.loads(raw).get("payload")
            if isinstance(payload, dict):
                files[STATS_PREFIX + key.removeprefix("stats/")] = orjson.dumps(payload)
            continue
        year, _, rest = key.partition("/")
        if not year.isdigit():
            continue
        files[f"{PREFIX}{year}@main/{unquote(rest)}"] = raw
    conn.close()
    return files


def _bundle_files(cache_dirs: list[str]) -> dict[str, bytes]:
    """jsDelivr path -> ``session.v1.bin`` of every session of the caches (this tree's encoder)."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data"))
    from encode_session_bundle import from_cache

    from tif1.core_utils.session_bundle import BUNDLE_FILE, encode_bundle

    out: dict[str, bytes] = {}
    built_at = time.time()
    for cache_dir in cache_dirs:
        for (year, gp, session), (entries, absent) in from_cache(Path(cache_dir)).items():
            path = f"{PREFIX}{year}@main/{unquote(gp)}/{unquote(session)}/{BUNDLE_FILE}"
            if path not in out:  # the first cache holding a session wins (as for its files)
                out[path] = encode_bundle(entries, absent=absent, built_at=built_at)
    return out


# -- parent ----------------------------------------------------------------------


def _run_child(workload, lib, src_dir, base, verify, ca_pem, extra_env, keep=None) -> dict:
    cache_dir = keep or tempfile.mkdtemp(prefix="tif1-c10-cold-")
    env = dict(
        os.environ,
        TIF1_CACHE_DIR=cache_dir,
        TIF1_CDN_HEDGE="false",
        C10_CA_PEM=str(ca_pem),
        PYTHONPATH=str(Path(src_dir).resolve()),
        **extra_env,
    )
    for name in ("TIF1_OFFLINE_MODE", "TIF1_SESSION_BUNDLE"):
        if name not in extra_env:
            env.pop(name, None)
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", workload]
    cmd += ["--lib", lib, "--base", base] + (["--verify"] if verify else [])
    t_launch = time.time()
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    if keep is None:
        shutil.rmtree(cache_dir, ignore_errors=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} failed: {proc.stderr[-3000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    # Start-up: launch to the child's first line; exit: the workload's end to the
    # last exit hook (the rest, interpreter teardown, is in neither).
    t_child = rec.pop("t_child")
    rec["startup"] = round((t_child - t_launch) * 1000, 2)
    exits = [line for line in proc.stderr.splitlines() if line.startswith("C10_EXIT ")]
    if exits:
        t_exit = float(exits[-1].split()[1])
        rec["exit_hooks"] = round((t_exit - t_child) * 1000 - rec["in_process_total"], 2)
    return rec


def _summarize(recs: list[dict]) -> dict:
    out = {}
    for key in recs[0]:
        if not isinstance(recs[0][key], int | float):
            continue
        values = sorted(r[key] for r in recs)
        p90 = values[min(len(values) - 1, round(0.9 * (len(values) - 1)))]
        out[key] = {
            "median": round(statistics.median(values), 1),
            "min": round(values[0], 1),
            "p90": round(p90, 1),
        }
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--base", default="")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--reps", type=int, default=8)
    parser.add_argument("--cache", default="/tmp/corinth/warm-C10")
    parser.add_argument("--season-cache", default=None, help="a cache with rounds 1-6 (loop6)")
    parser.add_argument("--control-src", required=False)
    parser.add_argument("--candidate-src", required=False)
    parser.add_argument("--candidate-env", default="TIF1_SESSION_BUNDLE=true")
    parser.add_argument("--control-env", default="")
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--no-bundles", action="store_true", help="serve no session.v1.bin")
    parser.add_argument("--link-mbps", type=float, default=8.0, help="link MB/s")
    parser.add_argument("--conn-mbps", type=float, default=6.0, help="per-connection MB/s")
    parser.add_argument("--rtt-ms", type=float, default=40.0, help="first-byte delay")
    parser.add_argument("--log-paths", action="store_true", help="print each arm's requests")
    parser.add_argument("--out", default=None)
    parser.add_argument(
        "--keep-caches", default=None, help="with --verify: keep each run's cache in DIR/{w}-{arm}"
    )
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib, args.base, args.verify)
        return

    def env_of(text: str) -> dict[str, str]:
        return dict(item.split("=", 1) for item in text.split(",") if item)

    sys.path.insert(0, str(Path(args.candidate_src).resolve()))
    t0 = time.perf_counter()
    caches = [args.cache] + ([args.season_cache] if args.season_cache else [])
    files: dict[str, bytes] = {}
    for cache_dir in caches:
        for path, body in _cache_files(cache_dir).items():
            files.setdefault(path, body)
    gz = set(files)
    files = {path: gzip.compress(body, 6) for path, body in files.items()}
    if not args.no_bundles:
        bundles = _bundle_files(caches)
        print(f"{len(bundles)} bundles, {sum(map(len, bundles.values())) / 1e3:.1f} KB", flush=True)
        files.update(bundles)
    workdir = Path(tempfile.mkdtemp(prefix="tif1-c10-tls-"))
    cert, key = _tls_files(workdir)
    tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    tls.load_cert_chain(cert, key)
    tls.set_alpn_protocols(["http/1.1"])
    server = LocalCDN(files, gz, args, tls)
    server.start()
    base = f"https://127.0.0.1:{server.port}/gh/TracingInsights"
    print(f"serving {len(files)} files on {base} ({time.perf_counter() - t0:.1f}s)", flush=True)

    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        arms: dict[str, list[dict]] = {"control": [], "candidate": []}
        traffic: dict[str, list[dict]] = {"control": [], "candidate": []}
        reps = 1 if args.verify else args.reps
        for i in range(reps):
            order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
            for arm in order:
                src = args.control_src if arm == "control" else args.candidate_src
                extra = env_of(args.control_env if arm == "control" else args.candidate_env)
                server.requests.clear()
                server.log.clear()
                keep = None
                if args.keep_caches and args.verify:
                    keep = str(Path(args.keep_caches) / f"{workload}-{arm}")
                    Path(keep).mkdir(parents=True, exist_ok=True)
                rec = _run_child(workload, args.lib, src, base, args.verify, cert, extra, keep)
                arms[arm].append(rec)
                traffic[arm].append(dict(server.requests))
                if args.log_paths and i == 0:
                    print(f"--- {workload} {arm}:\n  " + "\n  ".join(server.log), flush=True)
        ctl, cand = arms["control"], arms["candidate"]
        wins = sum(b["process_wall"] < a["process_wall"] for a, b in zip(ctl, cand, strict=True))
        summary[workload] = {
            "control": _summarize(ctl),
            "candidate": _summarize(cand),
            "candidate_faster_pairs": f"{wins}/{reps}",
            "traffic_control": traffic["control"][0],
            "traffic_candidate": traffic["candidate"][0],
            "runs_control": ctl,
            "runs_candidate": cand,
        }
        if args.verify:
            summary[workload]["digest_equal"] = ctl[0].get("digest") == cand[0].get("digest")
        c = summary[workload]["control"]["process_wall"]
        d = summary[workload]["candidate"]["process_wall"]
        print(
            f"[{args.lib}] {workload:12s} control {c['median']:.0f}/{c['min']:.0f}"
            f"  candidate {d['median']:.0f}/{d['min']:.0f}  faster {wins}/{reps}"
            + (f"  digest_equal={summary[workload]['digest_equal']}" if args.verify else "")
            + f"  traffic ctl={traffic['control'][0]} cand={traffic['candidate'][0]}",
            flush=True,
        )
    shutil.rmtree(workdir, ignore_errors=True)
    if args.out:
        Path(args.out).write_text(
            json.dumps({"lib": args.lib, "args": vars(args), "summary": summary}, indent=1) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
