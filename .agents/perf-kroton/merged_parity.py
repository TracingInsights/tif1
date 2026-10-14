"""KR-4 parity: merged telemetry frame, baseline src vs main src."""

import os
import pickle
import subprocess

MAIN = "/tmp/hoplite/workspace"
BASE = "/tmp/kroton-baseline"

SCRIPT = """
import os, pickle
os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"
import orjson
from tif1.cache import _decode_sqlite_value, get_cache
from tif1.core import _merge_telemetry_payloads
from tif1.core_utils.helpers import _telemetry_frame_from_merged
c = get_cache()
rows = c.conn.execute(
    "SELECT driver, lap, data FROM telemetry_cache WHERE year=2026"
    " AND gp='Monaco%20Grand%20Prix' AND session='Race'"
).fetchall()
tels = []
for driver, lap, blob in rows:
    data = orjson.loads(_decode_sqlite_value(blob))
    tels.append((driver, lap, data["tel"] if "tel" in data else data))
merged = _merge_telemetry_payloads(tels)
frame = _telemetry_frame_from_merged(merged)
with open(OUT, "wb") as f:
    pickle.dump(frame, f)
print("written", frame.shape)
"""


def run(srcdir, out):
    code = SCRIPT.replace("OUT", repr(out))
    subprocess.run(
        [f"{MAIN}/.venv/bin/python", "-c", code],
        env={**os.environ, "PYTHONPATH": srcdir},
        check=True,
        capture_output=True,
    )


run(f"{BASE}/src", "/tmp/kroton-merged-baseline.pkl")
run(f"{MAIN}/src", "/tmp/kroton-merged-main.pkl")

with open("/tmp/kroton-merged-baseline.pkl", "rb") as f:
    a = pickle.load(f)
with open("/tmp/kroton-merged-main.pkl", "rb") as f:
    b = pickle.load(f)

print("cols equal:", list(a.columns) == list(b.columns))
print("dtypes equal:", a.dtypes.astype(str).tolist() == b.dtypes.astype(str).tolist())
print("frames equal:", a.equals(b))
if not a.equals(b):
    for c in a.columns:
        if not a[c].equals(b[c]):
            print("MISMATCH col:", c, a[c].dtype, b[c].dtype)
