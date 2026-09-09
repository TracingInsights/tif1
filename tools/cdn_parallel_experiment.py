"""Experiment: does fetching from multiple CDNs at once beat sequential fallback?

Compares per-batch wall time of three CDN fetch strategies against the real
CDN fleet (jsDelivr, Hugging Face buckets, StaticDelivr):

- ``sequential``: try sources in priority order; a 404/error falls through to
  the next source (production behavior, :meth:`tif1.cdn.CDNManager.try_sources`).
- ``hedged``: fire one request per CDN simultaneously; first success wins.
- ``staggered``: fire the primary first; if it has not answered within
  ``--stagger-ms``, fire the rest (happy-eyeballs style).

Scenarios:
- ``small``: session-table-sized files that exist on every CDN.
- ``tel``: ``{driver}/{lap}_tel.json`` telemetry files (larger payloads).
- ``primary-404``: jsDelivr genuinely 404s (stale mirror); other CDNs have the
  file — the case where switching CDN on 404 must pay off.
- ``missing``: the file 404s on every CDN.

Usage:
    uv run python tools/cdn_parallel_experiment.py [--rounds 3] [--files-cap 22]

Notes:
- Uses the production HTTP stack (niquests, one shared session, HTTP/2) and a
  file-level concurrency cap matching ``max_concurrent_requests`` (22).
- Hedged loser requests still download to completion (threads cannot be
  cancelled mid-flight), so their bytes/requests are counted; stragglers are
  drained inside the timed batch, mirroring the connection-pool cost.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

import niquests

YEAR = 2026
GP = "Italian%20Grand%20Prix"
SESSION_NAME = "Race"

CDN_ORDER = ["jsDelivr", "HuggingFace", "StaticDelivr"]

DRIVER_CANDIDATES = [
    "VER",
    "HAM",
    "LEC",
    "SAI",
    "NOR",
    "PIA",
    "RUS",
    "ALO",
    "GAS",
    "LAW",
    "TSU",
    "HUL",
    "OCO",
    "STR",
    "BEA",
    "COL",
    "BOR",
    "DOO",
    "HAD",
    "ANT",
]


def format_url(cdn: str, path: str) -> str:
    """Build the CDN URL for a session-relative path (mirrors tif1.cdn rules)."""
    if cdn == "jsDelivr":
        return f"https://cdn.jsdelivr.net/gh/TracingInsights/{YEAR}@main/{GP}/{SESSION_NAME}/{path}"
    if cdn == "HuggingFace":
        return f"https://huggingface.co/buckets/tracinginsights/{YEAR}/resolve/{GP}/{SESSION_NAME}/{path}"
    return f"https://cdn.staticdelivr.com/gh/TracingInsights/{YEAR}/main/{GP}/{SESSION_NAME}/{path}"


class Stats:
    """Per-batch counters shared across worker threads."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.requests = 0
        self.bytes_downloaded = 0
        self.not_found = 0

    def add_request(self, size: int, not_found: bool = False) -> None:
        with self.lock:
            self.requests += 1
            self.bytes_downloaded += size
            if not_found:
                self.not_found += 1


class Fetcher:
    def __init__(self, http: niquests.Session, pool: ThreadPoolExecutor, timeout: float) -> None:
        self.http = http
        self.pool = pool
        self.timeout = timeout

    def _get(self, url: str, stats: Stats) -> tuple[bool, int]:
        """One HTTP GET. Returns (success, bytes)."""
        try:
            resp = self.http.get(url, timeout=self.timeout)
        except Exception:
            stats.add_request(0)
            return False, 0
        size = len(resp.content or b"")
        not_found = resp.status_code == 404
        ok = 200 <= resp.status_code < 300
        stats.add_request(size, not_found=not_found)
        return ok, size

    def sequential(self, urls: list[tuple[str, str]], stats: Stats) -> bool:
        for _cdn, url in urls:
            ok, _ = self._get(url, stats)
            if ok:
                return True
        return False

    def hedged(self, urls: list[tuple[str, str]], stats: Stats) -> bool:
        futures = [self.pool.submit(self._get, url, stats) for _cdn, url in urls]
        return self._first_success(futures)

    def staggered(self, urls: list[tuple[str, str]], stats: Stats, delay_s: float) -> bool:
        primary = self.pool.submit(self._get, urls[0][1], stats)
        done = wait([primary], timeout=delay_s).not_done
        if done or not primary.result()[0]:
            # Primary is slow OR already failed: hedge with the remaining sources.
            futures = [primary] + [
                self.pool.submit(self._get, url, stats) for _cdn, url in urls[1:]
            ]
            return self._first_success(futures)
        return True

    @staticmethod
    def _first_success(futures: list[Future[tuple[bool, int]]]) -> bool:
        pending = set(futures)
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                ok, _ = future.result()
                if ok:
                    return True
        return False


STRATEGIES = ("sequential", "hedged", "staggered")


def discover_driver_files(http: niquests.Session, timeout: float) -> tuple[list[str], list[str]]:
    """Find drivers with laptimes.json, then build telemetry file paths."""
    drivers: list[str] = []
    for driver in DRIVER_CANDIDATES:
        url = format_url("jsDelivr", f"{driver}/laptimes.json")
        try:
            resp = http.get(url, timeout=timeout)
        except Exception:
            continue
        if resp.status_code == 200:
            drivers.append(driver)
        if len(drivers) >= 8:
            break
    if not drivers:
        raise SystemExit("No driver files discovered; aborting experiment")
    tel_files: list[str] = []
    for driver in drivers:
        for lap in (1, 2, 3, 4, 5):
            path = f"{driver}/{lap}_tel.json"
            url = format_url("jsDelivr", path)
            try:
                resp = http.get(url, timeout=timeout)
            except Exception:
                continue
            if resp.status_code == 200:
                tel_files.append(path)
            if len(tel_files) >= 24:
                return tel_files, drivers
    if not tel_files:
        raise SystemExit("No telemetry files discovered; aborting experiment")
    return tel_files, drivers


def run_batch(
    fetcher: Fetcher,
    files: list[str],
    strategy: str,
    files_cap: int,
    stagger_s: float,
    primary_paths: dict[str, str] | None = None,
) -> tuple[float, Stats, int]:
    """Fetch one batch under a file-level concurrency cap. Returns (seconds, stats, failures)."""
    stats = Stats()
    semaphore = threading.Semaphore(files_cap)
    failures = 0
    lock = threading.Lock()

    def one_file(path: str) -> None:
        nonlocal failures
        with semaphore:
            if primary_paths is not None:
                # Simulated stale primary: jsDelivr 404s, the mirrors have the file.
                urls = [
                    (CDN_ORDER[0], format_url(CDN_ORDER[0], primary_paths[path])),
                    *[(cdn, format_url(cdn, path)) for cdn in CDN_ORDER[1:]],
                ]
            else:
                urls = [(cdn, format_url(cdn, path)) for cdn in CDN_ORDER]
            if strategy == "sequential":
                ok = fetcher.sequential(urls, stats)
            elif strategy == "hedged":
                ok = fetcher.hedged(urls, stats)
            else:
                ok = fetcher.staggered(urls, stats, stagger_s)
        if not ok:
            with lock:
                failures += 1

    start = time.perf_counter()
    futures = [fetcher.pool.submit(one_file, path) for path in files]
    for future in futures:
        future.result()
    elapsed = time.perf_counter() - start
    return elapsed, stats, failures


def median_of(values: list[float]) -> float:
    return statistics.median(values)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--files-cap", type=int, default=22)
    parser.add_argument("--stagger-ms", type=int, default=150)
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()

    http = niquests.Session()
    timeout = args.timeout
    stagger_s = args.stagger_ms / 1000.0

    # Warm every CDN once (TLS + connection pool) so no strategy pays handshake tax.
    for cdn in CDN_ORDER:
        fetcher_probe = format_url(cdn, "drivers.json")
        try:
            http.get(fetcher_probe, timeout=timeout)
        except Exception as e:
            print(f"WARN: warmup {cdn} failed: {e}", file=sys.stderr)

    # Per-CDN baseline latency (10 samples, sequential single file).
    print("== Per-CDN single-file baseline (median of 10) ==")
    for cdn in CDN_ORDER:
        url = format_url(cdn, "drivers.json")
        samples = []
        for _ in range(10):
            t0 = time.perf_counter()
            try:
                resp = http.get(url, timeout=timeout)
                _ = resp.content
            except Exception:
                pass
            samples.append(time.perf_counter() - t0)
        print(f"  {cdn:<12} {median_of(samples) * 1000:8.1f} ms")

    tel_files, drivers = discover_driver_files(http, timeout)
    print(f"\nDiscovered drivers: {', '.join(drivers)}")

    small_files = [
        "drivers.json",
        "weather.json",
        "rcm.json",
        "session_laptimes.json",
        "corners.json",
    ] + [f"{driver}/laptimes.json" for driver in drivers[:7]]
    # Simulated stale primary: jsDelivr URL points at a file that genuinely 404s.
    primary_404_paths = {path: f"missing_{path.replace('/', '_')}" for path in small_files}
    missing_files = [f"nope_{i}.json" for i in range(4)]

    scenarios: list[tuple[str, list[str], dict[str, str] | None]] = [
        ("small (healthy)", small_files, None),
        ("telemetry (healthy)", tel_files, None),
        ("primary-404 (stale jsDelivr)", small_files, primary_404_paths),
        ("missing everywhere", missing_files, None),
    ]

    results: dict[tuple[str, str], dict[str, list[float]]] = {}

    for scenario_name, files, primary_paths in scenarios:
        for strategy in STRATEGIES:
            times: list[float] = []
            reqs: list[int] = []
            mbs: list[float] = []
            fails: list[int] = []
            for _round in range(args.rounds):
                pool = ThreadPoolExecutor(max_workers=max(96, args.files_cap * 4))
                fetcher = Fetcher(http, pool, timeout)
                elapsed, stats, failures = run_batch(
                    fetcher,
                    files,
                    strategy,
                    args.files_cap,
                    stagger_s,
                    primary_paths=primary_paths,
                )
                pool.shutdown(wait=True)
                times.append(elapsed)
                reqs.append(stats.requests)
                mbs.append(stats.bytes_downloaded / 1e6)
                fails.append(failures)
                time.sleep(0.5)
            results[(scenario_name, strategy)] = {
                "times": times,
                "requests": reqs,
                "mbs": mbs,
                "failures": fails,
            }

    print(
        f"\n== Batch comparison ({args.rounds} rounds, median; "
        f"{args.files_cap} files in flight; {len(small_files)} small / {len(tel_files)} tel files) =="
    )
    header = f"{'scenario':<28} {'strategy':<10} {'median s':>8} {'min s':>7} {'max s':>7} {'reqs':>6} {'MB':>7} {'fails':>5}"
    print(header)
    print("-" * len(header))
    for (scenario_name, strategy), data in results.items():
        times = data["times"]
        med = median_of(times)
        req_med = int(median_of([float(r) for r in data["requests"]]))
        mb_med = median_of(data["mbs"])
        fail_med = int(median_of([float(f) for f in data["failures"]]))
        print(
            f"{scenario_name:<28} {strategy:<10} {med:8.2f} {min(times):7.2f} "
            f"{max(times):7.2f} {req_med:6d} {mb_med:7.2f} {fail_med:5d}"
        )

    print(
        "\nReading: 'sequential' mirrors the production CDNManager fallback; 'hedged' races "
        "all CDNs per file; 'staggered' fires the primary first and hedges after the delay."
    )


if __name__ == "__main__":
    main()
