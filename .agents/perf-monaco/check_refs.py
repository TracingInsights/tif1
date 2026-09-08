"""Inspect session_laptimes.json structure and probe CDN file existence."""

import json

import niquests

LAPTIMES_URL = (
    "https://cdn.jsdelivr.net/gh/TracingInsights/2026@main/"
    "Monaco%20Grand%20Prix/Race/session_laptimes.json"
)

with niquests.Session() as s:
    resp = s.get(LAPTIMES_URL, timeout=30)
    data = resp.json()

print("top-level type:", type(data).__name__)
if isinstance(data, dict):
    keys = list(data.keys())
    print("top keys:", keys[:10])
    first = data[keys[0]]
    print("first value type:", type(first).__name__)
    print(json.dumps(first, indent=1)[:800])
