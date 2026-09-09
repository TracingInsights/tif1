"""G6/G7/G9/G10 offline measurements (deterministic, no network).

G6: telemetry-tier cache write str round-trip vs bytes-native orjson dumps.
G7: laps reorder double-copy (insert(0)+reorder) vs fused single select.
G9: `import tif1` breakdown — eager fuzzy import cost.
G10: Connection/Keep-Alive headers on the h2/h3 session (bytes per request;
latency effect measured separately with warm requests).
"""

from __future__ import annotations

import pickle
import time

import orjson
import pandas as pd

from tif1.core_utils.constants import FASTF1_LAPS_COLUMN_ORDER
from tif1.core_utils.json_utils import json_dumps


def g6() -> None:
    with open("/tmp/monaco_tel_payloads.pkl", "rb") as f:
        entries = pickle.load(f)
    payloads = [e[2] for e in entries[:400]]

    def str_roundtrip() -> None:
        for p in payloads:
            blob = json_dumps(p).encode("utf-8")
            assert isinstance(blob, bytes)

    def bytes_native() -> None:
        for p in payloads:
            blob = orjson.dumps(p)
            assert isinstance(blob, bytes)

    def best(fn, n=5) -> float:
        b = float("inf")
        for _ in range(n):
            t0 = time.perf_counter()
            fn()
            b = min(b, time.perf_counter() - t0)
        return b

    t_str = best(str_roundtrip)
    t_bytes = best(bytes_native)
    print(
        f"G6 dumps+encode: {t_str * 1e3:.1f} ms/400 | orjson bytes: {t_bytes * 1e3:.1f} ms/400 "
        f"| delta {(t_str - t_bytes) * 1e3:.1f} ms total ({(t_str - t_bytes) / len(payloads) * 1e6:.1f} us/write)"
    )


def g7() -> None:
    n = 1455
    df = pd.DataFrame(
        {
            "Driver": [f"D{i % 20}" for i in range(n)],
            "LapNumber": [i % 70 + 1 for i in range(n)],
            "LapTime": pd.to_timedelta([80 + (i % 30) for i in range(n)], unit="s"),
            "SpeedI1": [280.0 + (i % 20) for i in range(n)],
            "Extra": list(range(n)),
        }
    )

    def current() -> None:
        out = df.copy()
        if "index" not in out.columns:
            out.insert(0, "index", range(len(out)))
        current_cols = list(out.columns)
        ordered = [c for c in FASTF1_LAPS_COLUMN_ORDER if c in current_cols]
        ordered.extend(c for c in current_cols if c not in set(FASTF1_LAPS_COLUMN_ORDER))
        _ = out[ordered]

    def fused() -> None:
        current_cols = list(df.columns)
        ordered = ["index"] + [c for c in FASTF1_LAPS_COLUMN_ORDER if c in current_cols]
        ordered.extend(c for c in current_cols if c not in set(FASTF1_LAPS_COLUMN_ORDER))
        _ = df.assign(index=list(range(len(df))))[ordered]

    def best(fn, n=20) -> float:
        b = float("inf")
        for _ in range(n):
            t0 = time.perf_counter()
            fn()
            b = min(b, time.perf_counter() - t0)
        return b

    print(
        f"G7 laps reorder: current {best(current) * 1e3:.2f} ms vs fused {best(fused) * 1e3:.2f} ms"
    )


def g9() -> None:
    import subprocess
    import sys

    out = subprocess.run(
        [sys.executable, "-X", "importtime", "-c", "import tif1"],
        capture_output=True,
        text=True,
        check=False,
        env={"TIF1_CACHE_DIR": "/tmp/g9-import", "PATH": "/usr/bin:/bin"},
    )
    lines = [ln for ln in out.stderr.splitlines() if "tif1" in ln]
    for ln in lines[:8]:
        print(f"G9 {ln}")


def g10() -> None:
    from tif1.http_session import _create_session

    session = _create_session()
    print(f"G10 session headers: {dict(session.headers)}")
    header_bytes = sum(len(k) + len(v) + 4 for k, v in session.headers.items())
    print(f"G10 total default header bytes/request: ~{header_bytes}")


def main() -> None:
    g6()
    g7()
    g9()
    g10()


if __name__ == "__main__":
    main()
