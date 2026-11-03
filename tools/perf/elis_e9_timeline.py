"""Elis E9: timeline of a cold (live CDN) session in one fresh process.

E2's ``elis_e2_timeline.py`` plus the first-wave handovers, the niquests
warm-up thread, the first telemetry access, ``--seed DIR`` (start from a
copy of ``DIR/cache.sqlite``, e.g. a cache holding only the OCSP row) and
``--src``. Telemetry GETs of the niquests fan-out are folded into one line
(GETs served by the stdlib bridge are not wrapped). Run from the repository
root:

    python tools/perf/elis_e9_timeline.py --src src --lib pandas --workload driver_tel \
        [--seed DIR]
"""

from __future__ import annotations

import time

T0 = time.perf_counter()

import argparse  # noqa: E402
import os  # noqa: E402
import shutil  # noqa: E402
import sys  # noqa: E402
import tempfile  # noqa: E402
import threading  # noqa: E402

EVENTS: list[tuple[float, float, str, str]] = []


def _ms() -> float:
    return (time.perf_counter() - T0) * 1000


def _wrap(module, name: str, label=None):
    original = getattr(module, name)

    def wrapper(*args, **kwargs):
        start = _ms()
        try:
            return original(*args, **kwargs)
        finally:
            tag = label(*args, **kwargs) if label else name
            EVENTS.append((start, _ms(), threading.current_thread().name, tag))

    setattr(module, name, wrapper)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--workload", default="laps")
    parser.add_argument("--no-ocsp", action="store_true")
    parser.add_argument("--seed", default=None, help="cache dir whose cache.sqlite is copied in")
    parser.add_argument(
        "--trigger", default=None, help="TIF1_E9_TRIGGER (measurement switch, not committed)"
    )
    parser.add_argument("--src", default=None, help="tif1 source tree to import")
    args = parser.parse_args()
    if args.trigger:
        os.environ["TIF1_E9_TRIGGER"] = args.trigger
    if args.src:
        sys.path.insert(0, args.src)
    if args.no_ocsp:
        os.environ["TIF1_HTTP_REVOCATION_CHECK"] = "false"
    cache_dir = tempfile.mkdtemp(prefix="tif1-e9-tl-")
    if args.seed:
        from pathlib import Path

        shutil.copy2(Path(args.seed) / "cache.sqlite", cache_dir)
    os.environ["TIF1_CACHE_DIR"] = cache_dir
    os.environ.pop("TIF1_OFFLINE_MODE", None)
    try:
        t = _ms()
        import tif1
        import tif1.async_fetch as af
        import tif1.http_session as hs
        import tif1.payload_loader as pl
        from tif1 import core

        EVENTS.append((t, _ms(), "MainThread", "import tif1"))
        _wrap(hs, "_create_session", lambda: "niquests import + Session()")
        _wrap(hs, "wait_for_warm_connections", lambda: "wait_for_warm_connections")
        _wrap(af, "wait_for_warm_connections", lambda: "wait_for_warm_connections(af)")
        _wrap(core, "_load_pandas_models", lambda: "import pandas + models")
        _wrap(af, "_get_resolved", lambda _s, url, _t: "GET " + url.rsplit("/", 1)[-1])
        import tif1.first_wave as fw

        _wrap(fw, "take", lambda url: "take " + url.rsplit("/", 1)[-1])
        if hasattr(fw, "_run_niquests_warmup"):
            _wrap(fw, "_run_niquests_warmup", lambda *_a: "niquests warm-up thread")
        _wrap(core.Session, "_resolve_telemetry_ultra_cold_mode", lambda *_a: "first tel access")
        orig_get_json = pl.NiquestsTransport.get_json

        def get_json(self, url, *, timeout=None):
            start = _ms()
            try:
                return orig_get_json(self, url, timeout=timeout)
            finally:
                EVENTS.append(
                    (start, _ms(), threading.current_thread().name, "sync GET " + url[-60:])
                )

        pl.NiquestsTransport.get_json = get_json
        orig_warm = hs.warm_connections

        def warm(urls, timeout):
            start = _ms()
            orig_warm(urls, timeout)
            EVENTS.append((start, _ms(), "MainThread", "warm_connections() call"))
            for done, url in zip(hs._warmups, urls, strict=False):

                def _watch(done=done, url=url):
                    done.wait(10)
                    EVENTS.append((start, _ms(), "preconnect", "HEAD " + url.rsplit("/", 1)[-1]))

                threading.Thread(target=_watch, daemon=True).start()

        hs.warm_connections = warm
        t = _ms()
        session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=args.lib)
        EVENTS.append((t, _ms(), "MainThread", "get_session"))
        t = _ms()
        if args.workload == "laps":
            _ = session.laps
        elif args.workload == "fastest_lap_tel":
            session.get_fastest_lap_tel()
        elif args.workload == "driver_tel":
            t1 = _ms()
            laps = session.laps
            EVENTS.append((t1, _ms(), "MainThread", "session.laps"))
            t1 = _ms()
            _ = laps.pick_driver("VER").telemetry
            EVENTS.append((t1, _ms(), "MainThread", "pick_driver.telemetry"))
        EVENTS.append((t, _ms(), "MainThread", args.workload))
    finally:
        shutil.rmtree(cache_dir, ignore_errors=True)
    tel = [e for e in EVENTS if e[3].startswith("GET ") and e[3].endswith("_tel.json")]
    rest = [e for e in EVENTS if e not in tel and e[3] != "wait_for_warm_connections(af)"]
    if tel:
        rest.append(
            (
                min(e[0] for e in tel),
                max(e[1] for e in tel),
                "pool",
                f"{len(tel)} telemetry GETs (first ends {min(e[1] for e in tel):.1f})",
            )
        )
    for start, end, thread, tag in sorted(rest):
        print(f"{start:8.1f} {end:8.1f} {end - start:7.1f}  {thread[:22]:22s} {tag}")
    print(f"total {_ms():.1f}", file=sys.stderr)


if __name__ == "__main__":
    main()
