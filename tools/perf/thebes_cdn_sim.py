"""Deterministic CDN latency simulation for the bulk telemetry fetch (Thebes TH3/TH10).

Replaces ``tif1.async_fetch._get_resolved`` (the blocking GET inside the
worker thread) with a fake whose latency is drawn, per CDN host, from a
seeded distribution shaped like the live samples measured on 2026-10-15
(``.agents/perf-thebes/RESULTS.md``):

- ``cold``  jsDelivr edge fill: lognormal around p50 ~3 s, p90 ~10 s, 3 % of
            requests hit the 30 s read timeout; Hugging Face p50 0.5 s;
            StaticDelivr p50 1.5 s.
- ``warm``  jsDelivr p50 50 ms (p90 120 ms, 0.5 % stragglers of 3-8 s),
            Hugging Face 300 ms, StaticDelivr 200 ms.
- ``flaky`` warm jsDelivr with 5 % 503s and 2 % 30 s timeouts.

Every scenario runs the production ``fetch_multiple_async`` on the tree
given with ``--src-dir`` so routing, hedging, retries, breaker and cache
code paths are all exercised. Reports wall time, request count per host
and bytes "transferred".

Usage:
    uv run python tools/perf/thebes_cdn_sim.py --src-dir /tmp/tif1-control/src --scenario cold
    uv run python tools/perf/thebes_cdn_sim.py --src-dir src --scenario cold
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import tempfile
import threading
import time
from collections import Counter

PAYLOAD = b'{"tel": {"time": [0.0, 0.1], "speed": [1.0, 2.0], "gear": [1, 1]}}'
READ_TIMEOUT = 30.0


class _Resp:
    def __init__(self, status: int, content: bytes = b"") -> None:
        self.status_code = status
        self.content = content
        self.headers = {}
        self.conn_info = None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import niquests

            raise niquests.exceptions.HTTPError(f"{self.status_code} Server Error", response=self)


def _host(url: str) -> str:
    return url.split("/")[2]


class Scenario:
    def __init__(self, name: str, seed: int) -> None:
        self.name = name
        self.rng = random.Random(seed)
        self.lock = threading.Lock()
        self.requests: Counter = Counter()
        self.bytes = 0
        self.timeouts = 0
        self.errors = 0

    def latency(self, host: str) -> tuple[float, int]:
        """(seconds, status) for one request; status 0 = read timeout."""
        r = self.rng
        if self.name == "cold":
            if "jsdelivr" in host:
                if r.random() < 0.03:
                    return READ_TIMEOUT, 0
                return min(READ_TIMEOUT, r.lognormvariate(1.1, 0.8)), 200
            if "huggingface" in host:
                return r.lognormvariate(-0.7, 0.35), 200
            return r.lognormvariate(0.4, 0.5), 200
        if self.name == "clean":
            # Warm edge without stragglers: isolates in-process costs.
            if "jsdelivr" in host:
                return r.lognormvariate(-3.0, 0.3), 200
            return r.lognormvariate(-1.2, 0.3), 200
        if self.name == "warm":
            if "jsdelivr" in host:
                if r.random() < 0.005:
                    return r.uniform(3.0, 8.0), 200
                return r.lognormvariate(-3.0, 0.4), 200
            if "huggingface" in host:
                return r.lognormvariate(-1.2, 0.3), 200
            return r.lognormvariate(-1.6, 0.3), 200
        if self.name == "flaky":
            if "jsdelivr" in host:
                p = r.random()
                if p < 0.02:
                    return READ_TIMEOUT, 0
                if p < 0.07:
                    return r.uniform(0.05, 0.2), 503
                return r.lognormvariate(-3.0, 0.4), 200
            if "huggingface" in host:
                return r.lognormvariate(-1.2, 0.3), 200
            return r.lognormvariate(-1.6, 0.3), 200
        raise ValueError(self.name)

    def get(self, url: str, timeout: float) -> _Resp:
        host = _host(url)
        with self.lock:
            seconds, status = self.latency(host)
            self.requests[host] += 1
        time.sleep(seconds)
        with self.lock:
            if status == 0:
                self.timeouts += 1
            elif status >= 400:
                self.errors += 1
            else:
                self.bytes += len(PAYLOAD)
        if status == 0:
            import niquests

            raise niquests.exceptions.ReadTimeout(f"Read timed out. (read timeout={timeout})")
        return _Resp(status, PAYLOAD if status == 200 else b"")


def _run_laps_mode(args: argparse.Namespace, scenario: Scenario) -> None:
    """Time ``session.laps`` on a cold session: every table request goes through the fake."""
    import sqlite3

    import niquests

    from tif1.cache import _decode_sqlite_value

    conn = sqlite3.connect(f"file:{args.payload_cache}/cache.sqlite?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT key, data FROM cache WHERE key LIKE '2026/Monaco%20Grand%20Prix/Race/%'"
    ).fetchall()
    payloads: dict[str, bytes] = {}
    for key, data in rows:
        blob = _decode_sqlite_value(data)
        payloads[key.rsplit("/", 1)[-1]] = blob.encode() if isinstance(blob, str) else bytes(blob)
    if "drivers.json" not in payloads or "session_laptimes.json" not in payloads:
        raise SystemExit("payload cache lacks drivers.json / session_laptimes.json rows")

    def fake_get(url: str, timeout: float = 30.0, **_kwargs) -> _Resp:
        name = url.rsplit("/", 1)[-1]
        resp = scenario.get(url, timeout)
        if resp.status_code == 200:
            resp.content = payloads.get(name, b"{}")
        return resp

    from tif1 import async_fetch

    async_fetch._get_resolved = lambda _client, url, timeout: fake_get(url, timeout)

    class FakeSession:
        def __init__(self, *_a, **_k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def get(self, url, timeout=30.0, **kwargs):
            return fake_get(url, timeout, **kwargs)

    niquests.Session = FakeSession  # the sync session-table prefetch
    from tif1 import http_session

    http_session.get_session = lambda: FakeSession()  # the sync payload loader
    import tif1

    t0 = time.perf_counter()
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    t1 = time.perf_counter()
    laps = session.laps
    t2 = time.perf_counter()
    print(
        json.dumps(
            {
                "scenario": args.scenario,
                "seed": args.seed,
                "mode": "laps",
                "get_session_s": round(t1 - t0, 2),
                "laps_s": round(t2 - t1, 2),
                "rows": len(laps),
                "requests": dict(scenario.requests),
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", required=True)
    parser.add_argument("--scenario", default="cold")
    parser.add_argument("--files", type=int, default=300)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=0)
    parser.add_argument("--mode", default="fetch", choices=["fetch", "laps"])
    parser.add_argument(
        "--payload-corpus",
        default=None,
        help="Argos corpus pickle; its raw payload bytes are served for _tel.json requests",
    )
    parser.add_argument(
        "--payload-cache",
        default="/tmp/tif1-perf/warm-a",
        help="primed cache dir whose drivers/session_laptimes rows feed the laps mode",
    )
    args = parser.parse_args()

    os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-sim-")
    os.environ["TIF1_CDN_PRECONNECT"] = "false"
    sys.path.insert(0, args.src_dir)
    from tif1 import async_fetch

    scenario = Scenario(args.scenario, args.seed)
    corpus_payloads: list[bytes] = []
    if args.payload_corpus:
        import pickle

        with open(args.payload_corpus, "rb") as f:
            corpus = pickle.load(f)
        corpus_payloads = [corpus["raw"][r] for r in corpus["refs"]]

    def fake_get_resolved(session_client, url, timeout):
        resp = scenario.get(url, timeout)
        if corpus_payloads and resp.status_code == 200:
            index = int(url.rsplit("/", 1)[-1].split("_")[0]) * 20 + int(url.rsplit("/", 2)[-2][1:])
            resp.content = corpus_payloads[index % len(corpus_payloads)]
        return resp

    async_fetch._get_resolved = fake_get_resolved
    async_fetch._get_async_session = lambda: object()
    # The simulated transport never counts as a reused connection.
    from tif1 import http_session

    http_session._response_reused_connection = lambda _r: False

    if args.mode == "laps":
        _run_laps_mode(args, scenario)
        return

    requests = [
        (2026, "Sim%20Grand%20Prix", "Race", f"D{i % 20:02d}/{i // 20 + 1}_tel.json")
        for i in range(args.files)
    ]
    kwargs = {"use_cache": False, "write_cache": False, "validate_payload": False}
    if corpus_payloads:
        # Measure the full in-process cost: fetch + parse in the worker
        # threads, then the frame assembly the session performs post-gather.
        from tif1.core_utils.helpers import _create_telemetry_df

        t0 = time.perf_counter()
        results = asyncio.run(async_fetch.fetch_multiple_async(requests, **kwargs))
        t_fetch = time.perf_counter() - t0
        t0 = time.perf_counter()
        frames = [
            _create_telemetry_df(r.get("tel", r), "D", i, "pandas")
            for i, r in enumerate(results)
            if isinstance(r, dict)
        ]
        t_assembly = time.perf_counter() - t0
        print(
            json.dumps(
                {
                    "scenario": args.scenario,
                    "files": args.files,
                    "frames": sum(f is not None for f in frames),
                    "fetch_parse_s": round(t_fetch, 3),
                    "assembly_s": round(t_assembly, 3),
                    "total_s": round(t_fetch + t_assembly, 3),
                }
            )
        )
        return
    if args.concurrency:
        kwargs["max_concurrent_requests"] = args.concurrency
    t0 = time.perf_counter()
    results = asyncio.run(async_fetch.fetch_multiple_async(requests, **kwargs))
    wall = time.perf_counter() - t0
    ok = sum(1 for r in results if isinstance(r, dict))
    print(
        json.dumps(
            {
                "scenario": args.scenario,
                "seed": args.seed,
                "files": args.files,
                "ok": ok,
                "wall_s": round(wall, 2),
                "requests": dict(scenario.requests),
                "timeouts": scenario.timeouts,
                "errors": scenario.errors,
                "payload_bytes": scenario.bytes,
            }
        )
    )


if __name__ == "__main__":
    main()
