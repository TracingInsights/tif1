"""Verify new _create_telemetry_df outputs match the legacy snapshot exactly."""

import hashlib
import pickle

from tif1.core_utils.helpers import _create_telemetry_df

with open("/tmp/monaco_tel_payloads.pkl", "rb") as f:
    entries: list[tuple[str, int, dict]] = pickle.load(f)
with open("/tmp/legacy_frame_hashes.pkl", "rb") as f:
    legacy: dict[tuple[str, int], str | None] = pickle.load(f)

mismatches = 0
for driver, lap, tel in entries:
    df = _create_telemetry_df(tel, driver, lap, "pandas")
    if df is None or df.empty:
        got = None
    else:
        h = hashlib.sha256()
        h.update(str(list(df.columns)).encode())
        h.update(str(list(df.dtypes)).encode())
        h.update(str(df.to_numpy()).encode())
        got = h.hexdigest()
    if got != legacy.get((driver, lap)):
        mismatches += 1
        if mismatches <= 3:
            print(f"MISMATCH {(driver, lap)}: legacy={legacy.get((driver, lap))} new={got}")
print(f"parity: {len(entries) - mismatches}/{len(entries)} identical, mismatches={mismatches}")
