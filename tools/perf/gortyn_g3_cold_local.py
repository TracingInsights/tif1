"""Gortyn G3: cold A/B against a local, bandwidth-throttled CDN.

The live CDN has no columnar telemetry files yet, so this harness serves a
session itself: a tiny HTTP/1.1 keep-alive server (in a thread of the parent)
answers the per-year repository layout of jsDelivr
(``/gh/TracingInsights/{year}@main/{gp}/{session}/...``) from a primed tif1
cache: the session JSON files and every lap's ``_tel.json`` (gzip, the bytes
the cache holds wrapped in ``{"tel": ...}``) and, unless ``--no-files``, each
driver's ``{driver}/tel.v1.bin`` encoded at start-up. Bodies are paced by a
token bucket per connection (``--conn-mbps``) and for the whole link
(``--link-mbps``), after a fixed first-byte delay (``--rtt-ms``).

Each run is a fresh interpreter on an empty cache directory pointed at the
server (one CDN source, no hedging, no stdlib first wave, Ergast merge off:
the same settings in both arms); control and candidate alternate (ABAB...).
``--verify`` runs each workload once per arm and compares a digest of the
frames it produced.

Usage (from the repo root)::

    python tools/perf/gortyn_g3_cold_local.py --cache /tmp/tif1-gortyn-warm-G3 \\
        --control-src /tmp/tif1-gortyn-control/src --candidate-src src \\
        --lib pandas --reps 5 --workloads driver_tel,notebook4,all_tel --out out.json
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
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import unquote

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
WORKLOADS = ("laps", "fastest_lap_tel", "driver_tel", "notebook4", "all_tel")
NOTEBOOK_DRIVERS = ("VER", "LEC", "NOR", "PIA")
PREFIX = "/gh/TracingInsights/"


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
        import pandas as pd

        digest.update(repr(list(frame.columns)).encode())
        digest.update(repr(list(frame.dtypes.astype(str))).encode())
        digest.update(pd.util.hash_pandas_object(frame, index=True).to_numpy().tobytes())
    return digest.hexdigest()


def _child(workload: str, lib: str, base: str, verify: bool) -> None:
    out: dict[str, object] = {}
    t_proc = time.perf_counter()

    def step(name, fn):
        t0 = time.perf_counter()
        r = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        return r

    import tif1
    from tif1.cdn import CDNSource, get_cdn_manager

    manager = get_cdn_manager()
    manager.sources = [CDNSource(name="local", base_url=base, priority=1)]
    manager._failure_counts = {"local": 0}
    out["import"] = round((time.perf_counter() - t_proc) * 1000, 2)
    session = step("get_session", lambda: tif1.get_session(YEAR, GP, SESSION, lib=lib))
    frames: list = []
    if workload == "laps":
        frames.append(step("laps", lambda: session.laps))
    elif workload == "fastest_lap_tel":
        frames.append(step("fastest_lap_tel", lambda: session.get_fastest_lap_tel()))
    elif workload == "driver_tel":
        step("laps", lambda: session.laps)
        frames.append(step("driver_tel", lambda: session.laps.pick_driver("VER").telemetry))
    elif workload == "notebook4":
        step("load", lambda: session.load(telemetry="lazy", weather=True, messages=True))
        laps = session.laps
        frames.extend(
            step("driver_tel_x4", lambda: [laps.pick_driver(d).telemetry for d in NOTEBOOK_DRIVERS])
        )
    elif workload == "all_tel":
        step("laps", lambda: session.laps)
        result = step("all_tel", lambda: session.fetch_all_laps_telemetry())
        if verify:
            if isinstance(result, dict):
                frames.extend(result[k] for k in sorted(result))
            else:
                frames.append(result)
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
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
    def __init__(self, files: dict[str, bytes], gz: set[str], args) -> None:
        self.files = files
        self.gz = gz
        self.args = args
        self.pacer = _Pacer(args.link_mbps * 1e6)
        self.requests: dict[str, int] = defaultdict(int)
        self.loop = asyncio.new_event_loop()
        self.port = 0

    def start(self) -> None:
        ready = threading.Event()

        def run() -> None:
            asyncio.set_event_loop(self.loop)
            server = self.loop.run_until_complete(
                asyncio.start_server(self._handle, "127.0.0.1", 0, backlog=256)
            )
            self.port = server.sockets[0].getsockname()[1]
            ready.set()
            self.loop.run_forever()

        threading.Thread(target=run, daemon=True).start()
        ready.wait()

    async def _handle(self, reader, writer) -> None:
        conn: dict = {}
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                headers = {}
                while True:
                    h = await reader.readline()
                    if h in (b"\r\n", b"\n", b""):
                        break
                    k, _, v = h.decode("latin-1").partition(":")
                    headers[k.strip().lower()] = v.strip()
                parts = line.decode("latin-1").split()
                target = parts[1] if len(parts) > 1 else "/"
                key = unquote(target.split("?")[0])
                body = self.files.get(key)
                await asyncio.sleep(self.args.rtt_ms / 1000)
                if body is None:
                    self.requests["404"] += 1
                    payload = b"not found"
                    writer.write(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n"
                        b"Content-Type: text/plain\r\n\r\n" + payload
                    )
                    await writer.drain()
                    continue
                kind = "bin" if key.endswith(".bin") else "json"
                if self.args.log_paths:
                    print("GET", key, len(body), flush=True)
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
                chunk = 16384
                for i in range(0, len(body), chunk):
                    piece = body[i : i + chunk]
                    delay = self.pacer.reserve(len(piece), conn, self.args.conn_mbps * 1e6)
                    if delay > 0:
                        await asyncio.sleep(delay)
                    writer.write(piece)
                    await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            return
        finally:
            writer.close()


def _load_files(cache_dir: str, with_files: bool, level: int) -> tuple[dict[str, bytes], set[str]]:
    from tif1.cache import _decode_sqlite_value
    from tif1.core_utils.columnar_telemetry import encode_driver_files
    from tif1.core_utils.json_utils import decode_telemetry_payload

    conn = sqlite3.connect(f"file:{Path(cache_dir) / 'cache.sqlite'}?mode=ro", uri=True)
    files: dict[str, bytes] = {}
    gz: set[str] = set()
    for key, data in conn.execute("SELECT key, data FROM cache"):
        if key.startswith("stats/"):
            continue
        year, _, rest = key.partition("/")
        raw = _decode_sqlite_value(data)
        raw = raw if isinstance(raw, bytes) else raw.encode()
        path = f"{PREFIX}{year}@main/{unquote(rest)}"
        files[path] = gzip.compress(raw, 6)
        gz.add(path)
    by_driver: dict[tuple, dict[int, bytes]] = defaultdict(dict)
    for year, gp, session, driver, lap, data in conn.execute(
        "SELECT year, gp, session, driver, lap, data FROM telemetry_cache"
    ):
        raw = _decode_sqlite_value(data)
        raw = raw if isinstance(raw, bytes) else raw.encode()
        payload = b'{"tel":' + raw + b"}"
        path = f"{PREFIX}{year}@main/{unquote(gp)}/{session}/{driver}/{lap}_tel.json"
        files[path] = gzip.compress(payload, 6)
        gz.add(path)
        by_driver[(year, unquote(gp), session, driver)][lap] = payload
    if with_files:

        def encode(item):
            (year, gp, session, driver), laps = item
            decoded = {lap: decode_telemetry_payload(p)["tel"] for lap, p in laps.items()}
            return {
                f"{PREFIX}{year}@main/{gp}/{session}/{driver}/{name}": blob
                for name, blob in encode_driver_files(decoded, level=level).items()
            }

        with ThreadPoolExecutor(8) as pool:
            for encoded in pool.map(encode, by_driver.items()):
                files.update(encoded)
    return files, gz


# -- parent ----------------------------------------------------------------------


def _run_child(workload, lib, src_dir, base, verify) -> dict:
    cache_dir = tempfile.mkdtemp(prefix="tif1-g3-cold-")
    env = dict(
        os.environ,
        TIF1_CACHE_DIR=cache_dir,
        TIF1_CDN_FIRST_WAVE="false",
        TIF1_CDN_HEDGE="false",
        TIF1_ERGAST_RESULTS="false",
        PYTHONPATH=str(Path(src_dir).resolve()),
    )
    env.pop("TIF1_OFFLINE_MODE", None)
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", workload, "--lib", lib]
    cmd += ["--base", base] + (["--verify"] if verify else [])
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    shutil.rmtree(cache_dir, ignore_errors=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} failed: {proc.stderr[-3000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def _summarize(recs: list[dict]) -> dict:
    return {
        key: {
            "median": round(statistics.median(r[key] for r in recs), 1),
            "min": round(min(r[key] for r in recs), 1),
        }
        for key in recs[0]
        if isinstance(recs[0][key], int | float)
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--base", default="")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--cache", default="/tmp/tif1-gortyn-warm-G3")
    parser.add_argument("--control-src", required=False)
    parser.add_argument("--candidate-src", required=False)
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--no-files", action="store_true", help="serve no tel.v1.bin (404)")
    parser.add_argument("--link-mbps", type=float, default=8.0, help="link MB/s")
    parser.add_argument("--conn-mbps", type=float, default=6.0, help="per-connection MB/s")
    parser.add_argument("--rtt-ms", type=float, default=30.0, help="first-byte delay")
    parser.add_argument("--level", type=int, default=19)
    parser.add_argument("--log-paths", action="store_true", help="print every 200 served")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib, args.base, args.verify)
        return

    # The encoder comes from the candidate tree.
    sys.path.insert(0, str(Path(args.candidate_src).resolve()))
    t0 = time.perf_counter()
    files, gz = _load_files(args.cache, not args.no_files, args.level)
    server = LocalCDN(files, gz, args)
    server.start()
    base = f"http://127.0.0.1:{server.port}/gh/TracingInsights"
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
                server.requests.clear()
                arms[arm].append(_run_child(workload, args.lib, src, base, args.verify))
                traffic[arm].append(dict(server.requests))
        ctl, cand = arms["control"], arms["candidate"]
        wins = sum(b["process_wall"] < a["process_wall"] for a, b in zip(ctl, cand, strict=True))
        summary[workload] = {
            "control": _summarize(ctl),
            "candidate": _summarize(cand),
            "candidate_faster_pairs": f"{wins}/{reps}",
            "traffic_control": traffic["control"][0],
            "traffic_candidate": traffic["candidate"][0],
        }
        if args.verify:
            summary[workload]["digest_equal"] = ctl[0].get("digest") == cand[0].get("digest")
        c = summary[workload]["control"]["process_wall"]
        d = summary[workload]["candidate"]["process_wall"]
        print(
            f"[{args.lib}] {workload:16s} control {c['median']:.0f}/{c['min']:.0f}"
            f"  candidate {d['median']:.0f}/{d['min']:.0f}  faster {wins}/{reps}"
            + (f"  digest_equal={summary[workload]['digest_equal']}" if args.verify else "")
            + f"  traffic ctl={traffic['control'][0]} cand={traffic['candidate'][0]}",
            flush=True,
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps({"lib": args.lib, "args": vars(args), "summary": summary}, indent=1) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
