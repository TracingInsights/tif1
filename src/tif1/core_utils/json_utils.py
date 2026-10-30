"""JSON utilities with accelerated orjson codec."""

from __future__ import annotations

import importlib
import json
import os
from collections.abc import Iterator
from typing import Any, Protocol, cast

import orjson

from .constants import DICTIONARY_TELEMETRY_PAYLOAD_KEYS, NUMERIC_TELEMETRY_PAYLOAD_KEYS


class DictionaryChannel:
    """A string telemetry channel decoded as codes into a short category list.

    The native decoder (``tif1-native``) returns ``DriverAhead`` this way
    instead of one Python ``str`` per sample (~860k per race). ``codes`` is an
    int32 numpy array with one entry per sample, an index into ``categories``
    or ``-1`` for a JSON null; ``categories`` holds the distinct strings in
    first-appearance order, the upstream ``"None"`` token included, so
    :meth:`tolist` gives exactly the list a generic JSON parser produces. The
    frame builders read the codes directly; everything else sees a sequence.
    """

    __slots__ = ("categories", "codes")

    def __init__(self, codes: Any, categories: list[str]) -> None:
        self.codes = codes
        self.categories = categories

    def __len__(self) -> int:
        return len(self.codes)

    def tolist(self) -> list[str | None]:
        """The samples as a list of ``str``/None (what orjson would return)."""
        table: list[str | None] = [*self.categories, None]
        return [table[code] for code in self.codes.tolist()]

    def __iter__(self) -> Iterator[str | None]:
        return iter(self.tolist())

    def __getitem__(self, index: Any) -> Any:
        return self.tolist()[index]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DictionaryChannel):
            return self.tolist() == other.tolist()
        if isinstance(other, list):
            return self.tolist() == other
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]  # mutable sequence, like list

    def __repr__(self) -> str:
        return f"DictionaryChannel(n={len(self)}, categories={self.categories!r})"


def _json_default(obj: Any) -> Any:
    """orjson/json ``default`` hook: a dictionary-encoded channel serializes as its list."""
    if isinstance(obj, DictionaryChannel):
        return obj.tolist()
    raise TypeError(f"Type is not JSON serializable: {type(obj).__name__}")


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
# ``TIF1_DISABLE_NATIVE=1`` forces the pure-Python path (read at import).
_native_decode_telemetry: Any = None
if os.environ.get("TIF1_DISABLE_NATIVE", "").strip().lower() not in {"1", "true", "yes", "on"}:
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
_NATIVE_DICT_KEYS = sorted(DICTIONARY_TELEMETRY_PAYLOAD_KEYS)


def _native_dict_args(decode: Any) -> tuple[Any, ...]:
    """Extra decoder arguments for dictionary-encoded channels, if the extension has them.

    ``tif1-native`` 0.2 dictionary-encodes the ``DriverAhead`` channel
    (Olympia H6); an older build rejects the arguments and keeps returning
    lists. The probe checks the round trip on a tiny payload once per process.
    """
    expected = ["1", None, "None", "1"]
    try:
        probe = decode(
            b'{"DriverAhead":["1",null,"None","1"]}', [], _NATIVE_DICT_KEYS, DictionaryChannel
        )
    except (TypeError, ValueError):
        return ()
    value = probe.get("DriverAhead") if isinstance(probe, dict) else None
    if isinstance(value, DictionaryChannel) and value.tolist() == expected:
        return (_NATIVE_DICT_KEYS, DictionaryChannel)
    return ()  # pragma: no cover - a build that disagrees with the contract


# Resolved on the first decode, not at import: the probe builds numpy arrays,
# and importing tif1.cache must not import numpy (CLI startup, Q20).
_NATIVE_DECODE_ARGS: tuple[Any, ...] | None = None


def _native_decode_args() -> tuple[Any, ...]:
    global _NATIVE_DECODE_ARGS
    args = _NATIVE_DECODE_ARGS
    if args is None:
        args = (_NATIVE_NUMERIC_KEYS,)
        if _native_decode_telemetry is not None:
            args = (*args, *_native_dict_args(_native_decode_telemetry))
        _NATIVE_DECODE_ARGS = args
    return args


def native_dictionary_channels() -> bool:
    """Whether natively decoded payloads carry ``DriverAhead`` as a DictionaryChannel."""
    return len(_native_decode_args()) > 1


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
    upstream ``"None"`` sentinel), ``DriverAhead`` as a
    :class:`DictionaryChannel` (``tif1-native`` >= 0.2) and other string
    channels as lists; the frame builders accept every shape and produce
    identical frames. Anything the native scanner does not handle falls back
    to :func:`json_loads`.
    """
    if _native_decode_telemetry is not None and not isinstance(payload, str):
        args = _NATIVE_DECODE_ARGS or _native_decode_args()
        try:
            return _native_decode_telemetry(
                payload if isinstance(payload, bytes) else bytes(payload), *args
            )
        except ValueError:
            pass
    return json_loads(payload)


def json_dumps(data: Any) -> str:
    """Serialize JSON payload with accelerated lib."""
    try:
        return orjson.dumps(data, default=_json_default, option=_DUMPS_OPTIONS).decode("utf-8")
    except Exception:
        # Same compact shape as orjson so callers see one format.
        return json.dumps(data, separators=(",", ":"), default=_json_default)


def json_dumps_bytes(data: Any) -> bytes:
    """Serialize JSON payload to bytes with the accelerated lib.

    orjson already emits bytes; this avoids the str round-trip that
    ``json_dumps(...).encode()`` pays on every cache write.
    """
    try:
        blob = orjson.dumps(data, default=_json_default, option=_DUMPS_OPTIONS)
        return blob if isinstance(blob, bytes) else blob.encode("utf-8")
    except Exception:
        return json.dumps(data, separators=(",", ":"), default=_json_default).encode("utf-8")


def parse_response_json(response: Any) -> Any:
    """Decode an HTTP response body, preferring raw-byte parsing when available."""
    content = getattr(response, "content", None)
    if isinstance(content, bytes | bytearray | memoryview):
        try:
            return json_loads(content)
        except Exception:
            pass
    return response.json()
