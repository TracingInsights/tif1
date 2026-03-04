"""JSON utilities with accelerated orjson codec."""

from __future__ import annotations

import json
from typing import Any, Protocol, cast

import orjson


class _OrjsonModule(Protocol):
    """Protocol for optional orjson module methods used by tif1."""

    def loads(self, obj: str | bytes | bytearray) -> Any: ...
    def dumps(self, obj: Any) -> bytes: ...


_ORJSON: _OrjsonModule = cast(_OrjsonModule, orjson)


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


def json_dumps(data: Any) -> str:
    """Serialize JSON payload with accelerated lib."""
    try:
        return _ORJSON.dumps(data).decode("utf-8")
    except Exception:
        return json.dumps(data)


def parse_response_json(response: Any) -> Any:
    """Decode an HTTP response body, preferring raw-byte parsing when available."""
    content = getattr(response, "content", None)
    if isinstance(content, bytes | bytearray | memoryview):
        try:
            return json_loads(content)
        except Exception:
            pass
    return response.json()
