"""Smoke test for an installed tif1-native wheel (needs only numpy).

Run by ``.github/workflows/native.yml`` on every built wheel: the decoder
types the channels, dictionary-encodes ``DriverAhead``, keeps the 0.1 list
shape without ``dict_channels``, and decodes from several threads (a
free-threaded interpreter must keep the GIL disabled after the import); the
liblz4 frame decoder decodes a linked frame into a caller-owned buffer.
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
# Hand-built lz4 frames (python-lz4 accepts both): linked 64 KB blocks, no
# checksums; block 1 is "abcd" + a 12-byte match at offset 4 + "XXXXX", block 2
# a 12-byte match 21 bytes back (into block 1) + "YYYYY".
LZ4_CONTENT = b"abcdabcdabcdabcdXXXXX" + b"abcdabcdabcdYYYYY"
_LZ4_BLOCKS = bytes.fromhex("0d000000486162636404005058585858580900000008150050595959595900000000")
LZ4_FRAME_SIZED = bytes.fromhex("04224d1848402600000000000000" + "3f") + _LZ4_BLOCKS
LZ4_FRAME_UNSIZED = bytes.fromhex("04224d184040" + "c0") + _LZ4_BLOCKS


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

    # liblz4 (built by cc into the wheel): a two-block linked frame, the second
    # block's match reaching into the first (the in-place dictionary), with and
    # without the content size; the destination's tail stays untouched.
    for frame in (LZ4_FRAME_SIZED, LZ4_FRAME_UNSIZED):
        dst = bytearray(b"\xaa" * (len(LZ4_CONTENT) + 16))
        assert tif1_native.lz4_frame_decompress_into(frame, dst) == len(LZ4_CONTENT)
        assert bytes(dst) == LZ4_CONTENT + b"\xaa" * 16
    try:
        tif1_native.lz4_frame_decompress_into(LZ4_FRAME_SIZED, bytearray(len(LZ4_CONTENT) - 1))
    except ValueError:
        pass
    else:  # pragma: no cover - contract violation
        raise AssertionError("a too-small destination must raise ValueError")

    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    if free_threaded:
        assert not sys._is_gil_enabled(), "importing tif1_native re-enabled the GIL"
    print(
        f"tif1-native {tif1_native.__version__} OK on {sys.version.split()[0]}"
        f"{' (free-threaded)' if free_threaded else ''}"
    )


if __name__ == "__main__":
    main()
