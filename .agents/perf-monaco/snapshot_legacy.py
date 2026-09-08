"""Snapshot legacy _create_telemetry_df outputs (columns/dtypes/value hashes)."""

import hashlib
import pickle

from tif1.core_utils.helpers import _create_telemetry_df

with open("/tmp/monaco_tel_payloads.pkl", "rb") as f:
    entries: list[tuple[str, int, dict]] = pickle.load(f)

snapshot: dict[tuple[str, int], tuple] = {}
for driver, lap, tel in entries:
    df = _create_telemetry_df(tel, driver, lap, "pandas")
    if df is None or df.empty:
        snapshot[(driver, lap)] = None
        continue
    h = hashlib.sha256()
    h.update(str(list(df.columns)).encode())
    h.update(str(list(df.dtypes)).encode())
    h.update(str(df.to_numpy()).encode())
    snapshot[(driver, lap)] = h.hexdigest()

with open("/tmp/legacy_frame_hashes.pkl", "wb") as f:
    pickle.dump(snapshot, f)
print(f"snapshot {len(snapshot)} frames, {sum(1 for v in snapshot.values() if v)} non-empty")
