"""Interleaved fresh-process A/B of the payload-tier warm load (Pergamon Q15).

Scenario: a warm cache whose frame tiers are empty (pre-frame-tier caches,
interrupted background frame writes, invalidated frame tables, a
``_FRAME_TIER_SCHEMA_VERSION`` bump). Every run gets its own fresh copy of
``--cache`` (the run re-materializes the frame tier), then a fresh process
times ``get_session`` + ``laps`` + ``fetch_all_laps_telemetry()``.

Usage:
    uv run python tools/perf/q15_payload_tier_ab.py --control-src /tmp/tif1-pergamon-control/src \
        --cache /tmp/tif1-q-Q15/noframes.sqlite --pairs 6 [--no-native] [--out x.json]
    uv run python tools/perf/q15_payload_tier_ab.py --child --cache-dir DIR [--src-dir S] \
        [--no-native] [--dump frames.pkl]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PHASES = ("get_session_s", "laps_s", "telemetry_s", "total_s")


def child(args: argparse.Namespace) -> None:
    if args.no_native:
        sys.modules["tif1_native"] = None  # type: ignore[assignment]  # import fails
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    os.environ["TIF1_CACHE_DIR"] = args.cache_dir
    import tif1
    from tif1.core_utils import json_utils

    t0 = time.perf_counter()
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race", lib=args.lib)
    t_get = time.perf_counter() - t0
    t0 = time.perf_counter()
    laps = session.laps
    t_laps = time.perf_counter() - t0
    t0 = time.perf_counter()
    tel = session.fetch_all_laps_telemetry()
    t_tel = time.perf_counter() - t0
    if args.dump:
        import pickle

        with open(args.dump, "wb") as fh:
            import pandas as pd

            # Frame-tier frames are session-bound ``Telemetry`` objects (not
            # picklable); dump the class name plus a plain frame on the same manager.
            plain = {
                k: (
                    type(v).__name__,
                    pd.DataFrame._from_mgr(v._mgr, axes=v._mgr.axes)
                    if isinstance(v, pd.DataFrame)
                    else v,  # polars frames pickle as is
                )
                for k, v in tel.items()
            }
            pickle.dump(plain, fh, protocol=5)
    print(
        json.dumps(
            {
                "get_session_s": round(t_get, 4),
                "laps_s": round(t_laps, 4),
                "telemetry_s": round(t_tel, 4),
                "total_s": round(t_get + t_laps + t_tel, 4),
                "laps_rows": len(laps),
                "telemetry_frames": len(tel),
                "native": json_utils.NATIVE_TELEMETRY_DECODER_AVAILABLE,
            }
        )
    )


def run_arm(src_dir: str | None, cache: str, extra: list[str]) -> dict:
    with tempfile.TemporaryDirectory(prefix="q15-", dir=os.environ.get("Q15_TMP")) as tmp:
        shutil.copyfile(cache, Path(tmp) / "cache.sqlite")
        cmd = [sys.executable, __file__, "--child", "--cache-dir", tmp, *extra]
        if src_dir:
            cmd += ["--src-dir", src_dir]
        out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
        return json.loads(out.strip().splitlines()[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--cache-dir")
    parser.add_argument("--src-dir")
    parser.add_argument("--dump")
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--no-native", action="store_true")
    parser.add_argument("--control-src")
    parser.add_argument("--candidate-src")
    parser.add_argument("--cache")
    parser.add_argument("--pairs", type=int, default=6)
    parser.add_argument("--out")
    args = parser.parse_args()
    if args.child:
        child(args)
        return
    extra = ["--lib", args.lib] + (["--no-native"] if args.no_native else [])
    pairs: list[dict] = []
    for i in range(args.pairs):
        order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
        result: dict = {}
        for arm in order:
            src = args.control_src if arm == "control" else args.candidate_src
            result[arm] = run_arm(src, args.cache, extra)
        pairs.append(result)
        print(
            f"pair {i + 1}: control tel {result['control']['telemetry_s']:.3f} "
            f"total {result['control']['total_s']:.3f} | candidate tel "
            f"{result['candidate']['telemetry_s']:.3f} total {result['candidate']['total_s']:.3f}",
            flush=True,
        )
    summary = {}
    for phase in PHASES:
        ctl = [p["control"][phase] for p in pairs]
        cand = [p["candidate"][phase] for p in pairs]
        wins = sum(1 for p in pairs if p["candidate"][phase] < p["control"][phase])
        summary[phase] = {
            "control_median": round(statistics.median(ctl), 4),
            "control_min": round(min(ctl), 4),
            "candidate_median": round(statistics.median(cand), 4),
            "candidate_min": round(min(cand), 4),
            "candidate_faster_pairs": f"{wins}/{len(pairs)}",
        }
        print(
            f"{phase:14s} control {summary[phase]['control_median']:.4f}"
            f"/{summary[phase]['control_min']:.4f}  candidate "
            f"{summary[phase]['candidate_median']:.4f}/{summary[phase]['candidate_min']:.4f}"
            f"  faster {summary[phase]['candidate_faster_pairs']}"
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {"lib": args.lib, "no_native": args.no_native, "pairs": pairs, "summary": summary},
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
