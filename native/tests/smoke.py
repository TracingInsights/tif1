"""Smoke test for an installed tif1-native wheel (needs only numpy).

Run by ``.github/workflows/native.yml`` on every built wheel: the decoder
types the channels, dictionary-encodes ``DriverAhead``, keeps the 0.1 list
shape without ``dict_channels``, and decodes from several threads (a
free-threaded interpreter must keep the GIL disabled after the import).
"""

from __future__ import annotations

import sys
import sysconfig
import threading
import warnings

import numpy as np

warnings.simplefilter("error", RuntimeWarning)  # a GIL re-enabled on import raises
import tif1_native  # noqa: E402

PAYLOAD = (
    b'{"tel":{"time":[0,0.25,0.5],"gear":[1,2,3],"speed":[1,2,"None"],'
    b'"DriverAhead":["None","44",null],"dataKey":"x"}}'
)


def main() -> None:
    tel = tif1_native.decode_telemetry(PAYLOAD, ["speed"], ["DriverAhead"])["tel"]
    assert tel["time"].dtype == np.float64
    assert tel["gear"].dtype == np.int64
    assert tel["speed"].dtype == np.float64
    assert np.isnan(tel["speed"][2])
    codes, categories = tel["DriverAhead"]
    assert codes.dtype == np.int32
    assert codes.tolist() == [0, 1, -1]
    assert categories == ["None", "44"]
    assert tel["dataKey"] == "x"
    legacy = tif1_native.decode_telemetry(PAYLOAD, ["speed"])["tel"]["DriverAhead"]
    assert legacy == ["None", "44", None]
    try:
        tif1_native.decode_telemetry(b'{"a":[1,"x"]}')
    except ValueError:
        pass
    else:  # pragma: no cover - contract violation
        raise AssertionError("mixed numeric/string channel must raise ValueError")

    results: list[object] = []
    threads = [
        threading.Thread(target=lambda: results.append(tif1_native.decode_telemetry(PAYLOAD)))
        for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(results) == 8

    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    if free_threaded:
        assert not sys._is_gil_enabled(), "importing tif1_native re-enabled the GIL"
    print(
        f"tif1-native {tif1_native.__version__} OK on {sys.version.split()[0]}"
        f"{' (free-threaded)' if free_threaded else ''}"
    )


if __name__ == "__main__":
    main()
