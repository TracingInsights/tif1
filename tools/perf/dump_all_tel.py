"""Dump all real Monaco telemetry payloads (raw bytes + parsed) for offline experiments."""

from __future__ import annotations

import asyncio
import os
import pickle
import tempfile

os.environ.setdefault("TIF1_CACHE_DIR", tempfile.mkdtemp(prefix="tif1-dump-"))

import tif1
from tif1.async_fetch import fetch_multiple_async
from tif1.http_session import get_session

YEAR, GP, SESSION = 2026, "Monaco Grand Prix", "Race"

session = tif1.get_session(YEAR, GP, SESSION)
laps = session.laps
refs: list[tuple[str, int]] = []
for _, row in laps.iterrows():
    driver, lap_num = row.get("Driver"), row.get("LapNumber")
    if driver is not None and lap_num is not None:
        refs.append((str(driver), int(lap_num)))

requests = [(YEAR, session.gp, SESSION, f"{d}/{lap}_tel.json") for d, lap in refs]

# Fetch raw bytes over the production HTTP stack (write_cache=False so the
# throwaway cache stays empty; we keep the bytes ourselves).
results = asyncio.run(
    fetch_multiple_async(requests, use_cache=False, write_cache=False, validate_payload=False)
)

http = get_session()
raw_blobs: dict[tuple[str, int], bytes] = {}
parsed_payloads: dict[tuple[str, int], dict] = {}
for (d, lap), res in zip(refs, results):
    if isinstance(res, dict) and isinstance(res.get("tel"), dict) and res["tel"]:
        parsed_payloads[(d, lap)] = res["tel"]

# Refetch bytes via the same session for the raw-blob set (fast: warm edge).
for d, lap in list(parsed_payloads.keys()):
    url = f"https://cdn.jsdelivr.net/gh/TracingInsights/{YEAR}@main/{session.gp}/{SESSION}/{d}/{lap}_tel.json"
    resp = http.get(url, timeout=30)
    if resp.status_code == 200:
        raw_blobs[(d, lap)] = resp.content

with open("/tmp/monaco_all_tel.pkl", "wb") as f:
    pickle.dump({"raw": raw_blobs, "parsed": parsed_payloads, "gp": session.gp}, f, protocol=5)

total_bytes = sum(len(b) for b in raw_blobs.values())
print(
    f"dumped {len(parsed_payloads)} parsed payloads, {len(raw_blobs)} raw blobs, "
    f"{total_bytes / 1e6:.1f} MB raw"
)
