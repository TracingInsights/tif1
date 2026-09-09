"""G1 measurement: import+first-get_session cost, before (HEAD) vs after (lazy polars).

Runs against the CURRENT working tree (candidate) and a control tree (HEAD),
each in a fresh subprocess, best of N. Also verifies polars still works lazily
in the candidate tree.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile

SNIPPET = """
import os, sys, time
sys.path.insert(0, {src!r})
os.environ["TIF1_CACHE_DIR"] = {cache!r}
t0 = time.perf_counter()
import tif1
t1 = time.perf_counter()
s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
t2 = time.perf_counter()
import json
print(json.dumps({{ "import_s": round(t1 - t0, 4), "first_get_session_s": round(t2 - t1, 4) }}))
"""

POLARS_SNIPPET = """
import os, sys, time
sys.path.insert(0, {src!r})
os.environ["TIF1_CACHE_DIR"] = {cache!r}
import tif1
s = tif1.get_session(2026, "Monaco Grand Prix", "Race", lib="polars")
laps = s.laps
import json
print(json.dumps({{"polars_laps_rows": len(laps), "polars_ok": True}}))
"""


def run_snippet(src: str, snippet: str, n: int) -> list[dict]:
    results = []
    for _ in range(n):
        cache = tempfile.mkdtemp(prefix="tif1-g1-")
        code = snippet.format(src=src, cache=cache)
        proc = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=False
        )
        if proc.returncode != 0:
            print(proc.stderr[-800:])
            raise SystemExit(f"snippet failed for {src}")
        results.append(eval(proc.stdout.strip().splitlines()[-1]))
    return results


def main() -> None:
    n = 5
    for label, src in (("control(HEAD)", "/tmp/tif1-control/src"), ("candidate(lazy)", "src")):
        runs = run_snippet(src, SNIPPET, n)
        imports = [r["import_s"] for r in runs]
        gets = [r["first_get_session_s"] for r in runs]
        print(
            f"{label}: import best={min(imports):.3f}s  first_get_session best={min(gets):.3f}s"
            f"  (import+firstcall best={min(i + g for i, g in zip(imports, gets)):.3f}s)"
        )

    # polars still functional on the candidate (lazy load on first use)
    pol = run_snippet("src", POLARS_SNIPPET, 1)[0]
    print(f"candidate polars flow: {pol}")


if __name__ == "__main__":
    main()
