"""V1/V4 full-set parity gate and offline assembly A/B on real Monaco data."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

import numpy as np  # noqa: E402
import orjson  # noqa: E402
import pandas as pd  # noqa: E402
import zstandard  # noqa: E402

from tif1.core_utils import helpers  # noqa: E402


def get_payloads():
    conn = sqlite3.connect("/tmp/perf-v1/cache.sqlite")
    rows = conn.execute(
        "SELECT driver, lap, data FROM telemetry_cache WHERE year=2026 AND gp='Monaco%20Grand%20Prix' AND session='Race'"
    ).fetchall()
    out = []
    for d, lap, blob in rows:
        if isinstance(blob, bytes) and blob[:4] == b"\x28\xb5\x2f\xfd":
            blob = zstandard.ZstdDecompressor().decompress(blob)  # noqa: PLW2901
        out.append((d, lap, orjson.loads(blob)))
    return out


def frames_identical(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    if list(a.columns) != list(b.columns):
        return False
    if not a.dtypes.equals(b.dtypes):
        return False
    if len(a) != len(b):
        return False
    return all(a[c].equals(b[c]) for c in a.columns)


def typed_frame_reference(tel_data, driver, lap_num):
    """The pre-V1 _typed_telemetry_frame semantics, preserved for A/B."""
    from tif1.core_utils.constants import TELEMETRY_RENAME_MAP

    col_data = {}
    for k, v in tel_data.items():
        if not isinstance(v, list):
            continue
        col_data[TELEMETRY_RENAME_MAP.get(k, k)] = v
    if not col_data:
        return None
    n = max(len(v) for v in col_data.values())
    if n == 0:
        return None
    normalized = {k: (v + [None] * (n - len(v))) if len(v) < n else v for k, v in col_data.items()}
    frame_data = {}
    for k, v in normalized.items():
        if k == "Time":
            try:
                frame_data[k] = pd.to_timedelta(np.asarray(v, dtype="float64"), unit="s")
            except (TypeError, ValueError):
                frame_data[k] = pd.to_timedelta(v, unit="s")
        elif k == "Brake" and None not in v:
            frame_data[k] = np.asarray(v, dtype=bool)
        elif k in ("nGear", "DRS"):
            frame_data[k] = pd.array(v, dtype="Int64")
        else:
            frame_data[k] = v
    frame_data["Driver"] = pd.Series(np.full(n, driver, dtype=object), dtype=object)
    frame_data["LapNumber"] = pd.array([lap_num] * n, dtype="Int64")
    frame = pd.DataFrame(frame_data, copy=False)
    for col in ["Time", "Speed", "nGear", "X", "Y", "Z"]:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame


def main() -> None:
    payloads = get_payloads()
    print(f"payloads: {len(payloads)}")

    # Parity gate: reference vs current implementation, all frames.
    mismatches = 0
    for d, lap, tel in payloads:
        ref = typed_frame_reference(tel, d, lap)
        cur = helpers._create_telemetry_df(tel, d, lap, "pandas")
        if ref is None or cur is None:
            if (ref is None) != (cur is None):
                mismatches += 1
            continue
        if not frames_identical(ref, cur):
            mismatches += 1
            if mismatches <= 3:
                print(f"MISMATCH {d}/{lap}")
                for c in ref.columns:
                    if not (ref[c].equals(cur[c])):
                        print(f"  col {c}: ref_dtype={ref[c].dtype} cur_dtype={cur[c].dtype}")
    print(
        f"V1 parity: {len(payloads) - mismatches}/{len(payloads)} identical, mismatches={mismatches}"
    )

    # Merged-path parity (V4): reference merged build vs current.
    ref_merged = pd.concat(
        [
            f
            for f in (typed_frame_reference(t, d, lap) for d, lap, t in payloads[:80])
            if f is not None
        ],
        ignore_index=True,
    )
    merged_cur = helpers._merge_telemetry_payloads([(d, lap, t) for d, lap, t in payloads[:80]])
    cur_df = helpers._telemetry_frame_from_merged(merged_cur)
    merged_parity = (
        list(ref_merged.columns) == list(cur_df.columns)
        and ref_merged.dtypes.equals(cur_df.dtypes)
        and len(ref_merged) == len(cur_df)
        and all(ref_merged[c].equals(cur_df[c]) for c in ref_merged.columns)
    )
    print(f"V4 merged parity: {merged_parity}")

    # Offline A/B timing: full-set assembly current vs reference.
    import time

    def best(fn, repeat=3):
        b = float("inf")
        for _ in range(repeat):
            t0 = time.perf_counter()
            fn()
            b = min(b, time.perf_counter() - t0)
        return b

    def run_current():
        for d, lap, t in payloads:
            helpers._create_telemetry_df(t, d, lap, "pandas")

    def run_reference():
        for d, lap, t in payloads:
            typed_frame_reference(t, d, lap)

    t_cur = best(run_current)
    t_ref = best(run_reference)
    print(
        f"V1 assembly A/B: reference={t_ref * 1e3:.0f}ms current={t_cur * 1e3:.0f}ms "
        f"({(t_cur / t_ref - 1) * 100:+.1f}%) over {len(payloads)} frames"
    )

    # Merged A/B
    entries = [(d, lap, t) for d, lap, t in payloads[:80]]

    def merged_ref():
        pd.concat(
            [
                f
                for f in (typed_frame_reference(t, d, lap) for d, lap, t in entries)
                if f is not None
            ],
            ignore_index=True,
        )

    def merged_cur_build():
        helpers._telemetry_frame_from_merged(helpers._merge_telemetry_payloads(entries))

    t_m_ref = best(merged_ref)
    t_m_cur = best(merged_cur_build)
    print(
        f"V4 merged A/B: concat-of-frames={t_m_ref * 1e3:.0f}ms merged-build={t_m_cur * 1e3:.0f}ms"
    )

    merged_list_build = helpers._merge_telemetry_payloads(entries)

    def merged_with_lists():
        helpers._apply_telemetry_dtypes(pd.DataFrame(merged_list_build, copy=False))

    t_m_lists = best(merged_with_lists)
    print(
        f"V4 merged build: pre-V1 (list)={t_m_lists * 1e3:.0f}ms vs current={t_m_cur * 1e3:.0f}ms"
    )


if __name__ == "__main__":
    main()
