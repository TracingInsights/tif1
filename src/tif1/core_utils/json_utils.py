"""JSON utilities with accelerated orjson codec."""

from __future__ import annotations

import importlib
import json
from typing import Any, Protocol, cast

import orjson

from .constants import NUMERIC_TELEMETRY_PAYLOAD_KEYS


class _OrjsonModule(Protocol):
    """Protocol for optional orjson module methods used by tif1."""

    def loads(self, obj: str | bytes | bytearray) -> Any: ...
    def dumps(self, obj: Any) -> bytes: ...


_ORJSON: _OrjsonModule = cast(_OrjsonModule, orjson)
# Telemetry payloads decoded by the native accelerator carry numpy arrays;
# orjson serializes them natively (shortest round-trip float repr).
_DUMPS_OPTIONS = orjson.OPT_SERIALIZE_NUMPY

# Thebes TH4: optional Rust decoder (``tif1-native``). It parses one
# telemetry payload straight into typed numpy arrays with the GIL released,
# so the fetch threads parse in parallel; without it every path uses orjson.
_native_decode_telemetry: Any
try:
    _native_name = "decode_telemetry"  # the extension ships no stubs
    _native_decode_telemetry = getattr(importlib.import_module("tif1_native"), _native_name)
except (ImportError, AttributeError):  # pragma: no cover - depends on the optional wheel
    _native_decode_telemetry = None

NATIVE_TELEMETRY_DECODER_AVAILABLE = _native_decode_telemetry is not None
# Raw payload keys of the numeric-by-contract channels: an all-sentinel
# channel among them decodes to a float64 NaN array, any other all-sentinel
# channel stays a list of tokens (what a generic parser would hand over).
_NATIVE_NUMERIC_KEYS = sorted(NUMERIC_TELEMETRY_PAYLOAD_KEYS)


def json_loads(payload: str | bytes | bytearray | memoryview) -> Any:
    """Deserialize JSON payload with accelerated lib."""
    serialized_payload: str | bytes | bytearray
    if isinstance(payload, memoryview):
        serialized_payload = payload.tobytes()
    else:
        serialized_payload = payload

    try:
        return _ORJSON.loads(serialized_payload)
    except Exception:
        return json.loads(serialized_payload)


def decode_telemetry_payload(payload: str | bytes | bytearray | memoryview) -> Any:
    """Parse a ``_tel.json`` payload, through the native decoder when available.

    With ``tif1-native`` installed every numeric channel comes back as a
    numpy array (int64 for integer channels, float64 with NaN for the
    upstream ``"None"`` sentinel) and string channels as lists; the frame
    builders accept both shapes and produce identical frames. Anything the
    native scanner does not handle falls back to :func:`json_loads`.
    """
    if _native_decode_telemetry is not None and not isinstance(payload, str):
        try:
            return _native_decode_telemetry(
                payload if isinstance(payload, bytes) else bytes(payload), _NATIVE_NUMERIC_KEYS
            )
        except ValueError:
            pass
    return json_loads(payload)


def json_dumps(data: Any) -> str:
    """Serialize JSON payload with accelerated lib."""
    try:
        return orjson.dumps(data, option=_DUMPS_OPTIONS).decode("utf-8")
    except Exception:
        # Same compact shape as orjson so callers see one format.
        return json.dumps(data, separators=(",", ":"))


def json_dumps_bytes(data: Any) -> bytes:
    """Serialize JSON payload to bytes with the accelerated lib.

    orjson already emits bytes; this avoids the str round-trip that
    ``json_dumps(...).encode()`` pays on every cache write.
    """
    try:
        blob = orjson.dumps(data, option=_DUMPS_OPTIONS)
        return blob if isinstance(blob, bytes) else blob.encode("utf-8")
    except Exception:
        return json.dumps(data, separators=(",", ":")).encode("utf-8")


def parse_response_json(response: Any) -> Any:
    """Decode an HTTP response body, preferring raw-byte parsing when available."""
    content = getattr(response, "content", None)
    if isinstance(content, bytes | bytearray | memoryview):
        try:
            return json_loads(content)
        except Exception:
            pass
    return response.json()
