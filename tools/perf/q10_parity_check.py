"""Q10 parity: compare pickled results of the control and candidate probe runs.

Usage:
    uv run python tools/perf/q10_parity_check.py CONTROL.pkl CANDIDATE.pkl
Prints one JSON line: type, columns, dtypes, ``equals``, block layout, and
(for pandas subclasses) the attached session/driver metadata kind.
"""

from __future__ import annotations

import json
import pickle
import sys


def main() -> None:
    with open(sys.argv[1], "rb") as fh:
        control = pickle.load(fh)
    with open(sys.argv[2], "rb") as fh:
        candidate = pickle.load(fh)
    out: dict = {"type": type(control) is type(candidate)}
    if isinstance(control, dict):  # session-bound subclass dumped by the probe
        out["class"] = control["class"] == candidate["class"]
        out["driver_attr"] = control["driver"] == candidate["driver"]
        control, candidate = control["frame"], candidate["frame"]
    if hasattr(control, "schema"):  # polars
        out["schema"] = control.schema == candidate.schema
        out["equals"] = control.equals(candidate)
    else:
        out["columns"] = list(control.columns) == list(candidate.columns)
        out["dtypes"] = control.dtypes.equals(candidate.dtypes)
        out["index"] = control.index.equals(candidate.index)
        out["equals"] = control.equals(candidate)
        out["blocks"] = [type(b).__name__ + str(b.dtype) for b in control._mgr.blocks] == [
            type(b).__name__ + str(b.dtype) for b in candidate._mgr.blocks
        ]
        out["driver_attr"] = getattr(control, "driver", None) == getattr(candidate, "driver", None)
    out["ok"] = all(out.values())
    print(json.dumps(out))


if __name__ == "__main__":
    main()
