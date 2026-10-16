"""The fastf1-compatible HTTP helpers fail clearly inside Pyodide (no niquests there)."""

from __future__ import annotations

import pytest

from tif1 import fastf1_compat
from tif1.core_utils import runtime
from tif1.exceptions import TIF1Error


@pytest.mark.parametrize("method", ["requests_get", "requests_post"])
def test_compat_http_helpers_raise_inside_pyodide(monkeypatch, method):
    monkeypatch.setattr(runtime, "IS_EMSCRIPTEN", True)
    monkeypatch.setattr(fastf1_compat.Cache, "_offline_mode", False)
    with pytest.raises(TIF1Error, match="not available inside Pyodide") as info:
        getattr(fastf1_compat.Cache, method)("https://example.com/data.json")
    assert info.value.context["url"] == "https://example.com/data.json"


def test_compat_http_helpers_reach_the_shared_session_natively(monkeypatch):
    seen: dict = {}

    class FakeSession:
        def get(self, url, **kwargs):
            seen["get"] = (url, kwargs)
            return "response"

    monkeypatch.setattr(fastf1_compat.Cache, "_offline_mode", False)
    from tif1 import http_session

    monkeypatch.setattr(http_session, "get_session", lambda: FakeSession())
    assert fastf1_compat.Cache.requests_get("https://example.com/x", timeout=3) == "response"
    assert seen["get"] == ("https://example.com/x", {"timeout": 3})
