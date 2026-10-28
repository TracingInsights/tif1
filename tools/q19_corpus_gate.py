"""Cross-season parity gate for Pergamon Q19 (driver laps slice + vectorized classification).

For every session with a pandas laps frame in a cache COPY: for every driver,
compare :func:`build_driver_laps_frame` against the pipeline build
``_process_lap_df(_create_lap_df(payload))``
(columns, dtypes, index, values, block layout, object element types) and
count the fallbacks (None); compare the vectorized classification summary
against the row loop. Network access is disabled: sessions whose payloads
are not cached are skipped.

Usage:
    python -I tools/q19_corpus_gate.py --src-dir src --cache-dir /tmp/copy [--out report.json]
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from urllib.parse import unquote


def _problems(a, b) -> list[str]:
    out = []
    if list(a.columns) != list(b.columns):
        return ["columns"]
    if a.columns.dtype != b.columns.dtype or type(a.columns) is not type(b.columns):
        out.append("columns index dtype")
    if not a.dtypes.equals(b.dtypes):
        out.append("dtypes")
    if not a.index.equals(b.index) or type(a.index) is not type(b.index):
        out.append("index")
    if not a.equals(b):
        out.append("values")
    for col in a.columns:
        if isinstance(a[col].dtype, __import__("pandas").CategoricalDtype):
            if a[col].cat.codes.dtype != b[col].cat.codes.dtype:
                out.append(f"codes dtype {col}")
    sig_a = sorted((str(x.dtype), tuple(x.mgr_locs.as_array.tolist())) for x in a._mgr.blocks)
    sig_b = sorted((str(x.dtype), tuple(x.mgr_locs.as_array.tolist())) for x in b._mgr.blocks)
    if sig_a != sig_b:
        out.append("blocks")
    for col in a.columns:
        if a[col].dtype == object:
            if [type(v) for v in a[col].tolist()] != [type(v) for v in b[col].tolist()]:
                out.append(f"object types {col}")
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", required=True)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--out")
    args = parser.parse_args()
    sys.path.insert(0, str(Path(args.src_dir).resolve()))
    os.environ["TIF1_CACHE_DIR"] = args.cache_dir

    from tif1 import core
    from tif1.core_utils.driver_laps import build_driver_laps_frame
    from tif1.core_utils.helpers import _create_lap_df, _process_lap_df

    def _offline(*_a, **_k):
        raise core.NetworkError(url="offline", status_code=None)

    core.Session._run_fetch_multiple = staticmethod(_offline)  # ty: ignore[invalid-assignment]
    core.Session._fetch_json = _offline  # ty: ignore[invalid-assignment]
    core.Session._fetch_json_unvalidated = _offline  # ty: ignore[invalid-assignment]

    conn = sqlite3.connect(Path(args.cache_dir) / "cache.sqlite")
    sessions = conn.execute("SELECT year, gp, session FROM laps_frames ORDER BY 1, 2, 3").fetchall()
    conn.close()

    report: dict = {
        "sessions": 0,
        "skipped": [],
        "drivers": 0,
        "fast": 0,
        "fallback": [],
        "mismatch": [],
        "summary_mismatch": [],
        "summary_fallback": 0,
    }
    for year, gp, sess in sessions:
        try:
            s = core.Session(year, unquote(gp), unquote(sess), enable_cache=True, lib="pandas")
            src = core.get_cache().get_laps_frame(year, gp, sess)
            if src is None:
                report["skipped"].append([year, gp, sess, "no tier frame"])
                continue
            codes = sorted({str(x) for x in src["Driver"].dropna().unique()})
            s.get_driver(codes[0])
        except Exception as exc:
            report["skipped"].append([year, gp, sess, repr(exc)[:120]])
            continue
        report["sessions"] += 1
        for code in codes:
            try:
                payload = s._get_or_derive_driver_laptime_payload(code)
            except Exception as exc:
                report["skipped"].append([year, gp, sess, code, repr(exc)[:120]])
                continue
            if not isinstance(payload, dict):
                report["skipped"].append([year, gp, sess, code, "no payload"])
                continue
            team = s._get_driver_info(code)["team"]
            report["drivers"] += 1
            try:
                ref = _process_lap_df(_create_lap_df(payload, code, team, "pandas"), "pandas")
            except Exception as exc:
                ref = exc
            got = build_driver_laps_frame(payload, code, team)
            if got is None:
                report["fallback"].append([year, gp, sess, code])
                continue
            report["fast"] += 1
            if isinstance(ref, Exception):
                report["mismatch"].append([year, gp, sess, code, f"ref raised {ref!r}"[:160]])
                continue
            issues = _problems(ref, got)
            if issues:
                report["mismatch"].append([year, gp, sess, code, issues])
        # Classification summary: vectorized vs row loop.
        fast = core._summarize_pandas_laps_vectorized(src)
        if fast is None:
            report["summary_fallback"] += 1
        else:
            original = core._summarize_pandas_laps_vectorized
            core._summarize_pandas_laps_vectorized = lambda _laps: None
            try:
                loop = core._summarize_laps_for_classification(src, "pandas")
            finally:
                core._summarize_pandas_laps_vectorized = original
            if list(fast.items()) != list(loop.items()):
                report["summary_mismatch"].append([year, gp, sess])
    print(json.dumps({k: (len(v) if isinstance(v, list) else v) for k, v in report.items()}))
    print("mismatch sample:", report["mismatch"][:10])
    print("fallback sample:", report["fallback"][:10])
    print("skipped sample:", report["skipped"][:5])
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
