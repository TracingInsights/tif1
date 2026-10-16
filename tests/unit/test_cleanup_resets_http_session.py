"""async_fetch.cleanup_resources() must not leave a closed shared HTTP session behind."""

from __future__ import annotations

from tif1 import async_fetch
from tif1 import http_session as http_session_module


class FakeSession:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_cleanup_resources_resets_the_shared_http_session(monkeypatch):
    monkeypatch.setattr(http_session_module, "_create_session", FakeSession)
    monkeypatch.setattr(http_session_module, "_shared_session", None)

    first = async_fetch._get_async_session()
    assert first is http_session_module.get_session()

    async_fetch.cleanup_resources()
    assert first.closed

    second = async_fetch._get_async_session()
    assert second is not first
    assert not second.closed


def test_cleanup_resources_closes_a_foreign_session_directly(monkeypatch):
    foreign = FakeSession()
    monkeypatch.setattr(http_session_module, "_shared_session", None)
    monkeypatch.setattr(async_fetch, "_async_session", foreign)
    async_fetch.cleanup_resources()
    assert foreign.closed
    assert async_fetch._async_session is None
