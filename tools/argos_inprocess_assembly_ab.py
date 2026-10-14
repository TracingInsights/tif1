"""A1/A2: in-process interleaved A/B of the control vs candidate frame builder.

Loads the control tree's ``helpers.py`` as a sibling module of the working
tree's package (relative imports resolve to the same constants/config), then
alternates full-corpus builds control/candidate/control/... so both see the
same machine load. Reports best-of-N per arm and per-pair deltas.

Usage:
    uv run python tools/argos_inprocess_assembly_ab.py --control /tmp/tif1-control/src --pairs 5
"""

from __future__ import annotations

import argparse
import gc
import importlib.util
import pickle
import sys
import time
from pathlib import Path


def _load_control(control_src: str):
    path = Path(control_src) / "tif1" / "core_utils" / "helpers.py"
    spec = importlib.util.spec_from_file_location("tif1.core_utils._helpers_control", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control", default="/tmp/tif1-control/src")
    parser.add_argument("--corpus", default="/tmp/argos_corpus.pkl")
    parser.add_argument("--pairs", type=int, default=5)
    args = parser.parse_args()
    import tif1.core_utils.helpers as cand

    ctl = _load_control(args.control)
    with open(args.corpus, "rb") as f:
        corpus = pickle.load(f)
    payloads = [
        (d, lap, corpus["parsed"][(d, lap)].get("tel", corpus["parsed"][(d, lap)]))
        for d, lap in corpus["refs"]
    ]

    def run(mod) -> float:
        gc.collect()
        gc.disable()
        t0 = time.perf_counter()
        for d, lap, p in payloads:
            mod._create_telemetry_df(p, d, lap, "pandas")
        dt = time.perf_counter() - t0
        gc.enable()
        return dt

    run(ctl)
    run(cand)
    ctl_times, cand_times = [], []
    for i in range(args.pairs):
        if i % 2 == 0:
            ctl_times.append(run(ctl))
            cand_times.append(run(cand))
        else:
            cand_times.append(run(cand))
            ctl_times.append(run(ctl))
        print(
            f"pair {i}: control {ctl_times[-1] * 1000:.0f} ms  candidate {cand_times[-1] * 1000:.0f} ms  "
            f"({(cand_times[-1] / ctl_times[-1] - 1) * 100:+.1f}%)"
        )
    print(
        f"best: control {min(ctl_times) * 1000:.0f} ms vs candidate {min(cand_times) * 1000:.0f} ms "
        f"({(min(cand_times) / min(ctl_times) - 1) * 100:+.1f}%); "
        f"candidate faster in {sum(1 for a, b in zip(ctl_times, cand_times) if b < a)}/{args.pairs} pairs"
    )


if __name__ == "__main__":
    main()
