"""KR-1 parity: laps frame via payload path (baseline src) vs tier (main src)."""

import os

import pandas as pd
import pickle
import subprocess

MAIN = "/tmp/hoplite/workspace"
BASE = "/tmp/kroton-baseline"

SCRIPT = """
import os, pickle
os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"
import pandas as pd
import tif1
s = tif1.get_session(2026, "Monaco Grand Prix", "Race")
laps = s.laps
df = pd.DataFrame(laps)
with open(OUT, "wb") as f:
    pickle.dump({"cols": df.columns.tolist(), "dtypes": df.dtypes.astype(str).tolist(), "frame": df}, f)
print("written", len(df))
"""


def run(srcdir, out):
    code = SCRIPT.replace("OUT", repr(out))
    subprocess.run(
        [f"{MAIN}/.venv/bin/python", "-c", code],
        env={**os.environ, "PYTHONPATH": f"{srcdir}"},
        check=True,
        capture_output=True,
    )


run(f"{BASE}/src", "/tmp/kroton-laps-baseline.pkl")
run(f"{MAIN}/src", "/tmp/kroton-laps-tier.pkl")

with open("/tmp/kroton-laps-baseline.pkl", "rb") as f:
    a = pickle.load(f)
with open("/tmp/kroton-laps-tier.pkl", "rb") as f:
    b = pickle.load(f)

fa, fb = a["frame"], b["frame"]
print("cols equal:", a["cols"] == b["cols"])
print("dtypes equal:", a["dtypes"] == b["dtypes"])
print("frames equal:", fa.equals(fb))
if not fa.equals(fb):
    for c in fa.columns:
        if not fa[c].equals(fb[c]):
            print("MISMATCH col:", c, fa[c].dtype, fb[c].dtype)
