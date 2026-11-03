"""Elis E2 probe: cost of one stdlib (http.client + ssl) GET from a fresh process.

Prints the import, CA-store, connect/TLS and body times of a GET of a cold
session's drivers.json and session_laptimes.json over two parallel stdlib
connections, against the niquests import time. Run from the repository root.
"""

from __future__ import annotations

import time

T0 = time.perf_counter()

import sys  # noqa: E402
import threading  # noqa: E402


def ms() -> float:
    return round((time.perf_counter() - T0) * 1000, 1)


BASE = "https://cdn.jsdelivr.net/gh/TracingInsights/2025@main/Bahrain%20Grand%20Prix/Race/"
out: dict[str, float] = {}
t = ms()
import http.client  # noqa: E402
import ssl  # noqa: E402
import zlib  # noqa: E402

out["import ssl+http.client"] = ms() - t
t = ms()
import wassima  # noqa: E402

out["import wassima"] = ms() - t
t = ms()
bundle = wassima.generate_ca_bundle()
out["wassima bundle"] = ms() - t
t = ms()
ctx = ssl.create_default_context(cadata=bundle)
ctx.set_alpn_protocols(["http/1.1"])
out["ssl context"] = ms() - t


def get(path: str) -> None:
    t0 = ms()
    conn = http.client.HTTPSConnection("cdn.jsdelivr.net", context=ctx, timeout=10)
    conn.connect()
    out[f"{path} connect+tls"] = ms() - t0
    conn.request(
        "GET",
        BASE.split("cdn.jsdelivr.net", 1)[1] + path,
        headers={"Accept-Encoding": "gzip", "User-Agent": "tif1-probe"},
    )
    resp = conn.getresponse()
    out[f"{path} ttfb"] = ms() - t0
    body = resp.read()
    if resp.getheader("Content-Encoding") == "gzip":
        body = zlib.decompress(body, 47)
    out[f"{path} done (len {len(body)}, {resp.status})"] = ms() - t0
    out[f"{path} end at"] = ms()


threads = [
    threading.Thread(target=get, args=(p,)) for p in ("drivers.json", "session_laptimes.json")
]
start = ms()
for th in threads:
    th.start()
if "--niquests" in sys.argv:
    t = ms()
    import niquests  # noqa: F401

    out["import niquests (concurrent)"] = ms() - t
for th in threads:
    th.join()
out["wave wall"] = ms() - start
for k, v in out.items():
    print(f"{v:8.1f}  {k}")
