"""Show exact dtype differences between A (current) and C (typed-constructor)."""

import pickle

import numpy as np
import pandas as pd

from tif1.core_utils.constants import TELEMETRY_RENAME_MAP
from tif1.core_utils.helpers import _create_telemetry_df

with open("/tmp/monaco_tel_payloads.pkl", "rb") as f:
    entries: list[tuple[str, int, dict]] = pickle.load(f)

tel = next(v for drv, lap_num, v in entries if drv == "NOR" and lap_num == 1)
a = _create_telemetry_df(tel, "NOR", 1, "pandas")

col_data = {TELEMETRY_RENAME_MAP.get(k, k): v for k, v in tel.items() if isinstance(v, list)}
max_len = max(len(v) for v in col_data.values())
normalized = {
    k: (v + [None] * (max_len - len(v))) if len(v) < max_len else v for k, v in col_data.items()
}
frame_data: dict = {}
for k, v in normalized.items():
    if k == "Time":
        frame_data[k] = pd.to_timedelta(v, unit="s")
    elif k == "Brake" and None not in v:
        frame_data[k] = np.asarray(v, dtype=bool)
    elif k in ("nGear", "DRS"):
        frame_data[k] = pd.array(v, dtype="Int64")
    else:
        frame_data[k] = v
frame_data["Driver"] = np.full(max_len, "NOR", dtype=object)
frame_data["LapNumber"] = pd.array([1] * max_len, dtype="Int64")
c = pd.DataFrame(frame_data, copy=False)

print(f"{'column':12} {'A':>28} {'C':>28}")
for col in a.columns:
    da, dc = a[col].dtype, c[col].dtype
    mark = "" if da == dc else "   <-- DIFF"
    print(f"{col:12} {da!s:>28} {dc!s:>28}{mark}")
print()
print("A values sample:", a.iloc[0].to_dict())
print("C values sample:", c.iloc[0].to_dict())
