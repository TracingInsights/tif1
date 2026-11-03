"""Persisted OCSP verdicts (Elis E3, ``tif1.ocsp_cache``), offline with fake OCSP answers."""

from __future__ import annotations

import base64
import subprocess
import sys
import time
import types
from enum import Enum
from typing import Any

import orjson
import pytest

from tif1 import http_session, ocsp_cache
from tif1.cache import Cache


class ResponseStatus(Enum):
    SUCCESSFUL = 0
    TRY_LATER = 3


class CertStatus(Enum):
    GOOD = 0
    REVOKED = 1
    UNKNOWN = 2


class FakeResponse:
    """Mirrors the qh3 ``OCSPResponse`` surface ``ocsp_cache`` uses."""

    def __init__(
        self,
        next_update: int,
        cert_status: CertStatus = CertStatus.GOOD,
        status: ResponseStatus = ResponseStatus.SUCCESSFUL,
        signed_by: bytes = b"issuer-der",
    ) -> None:
        self.next_update = next_update
        self.certificate_status = cert_status
        self.response_status = status
        self.signed_by = signed_by

    def serialize(self) -> bytes:
        return orjson.dumps(
            [
                self.next_update,
                self.certificate_status.value,
                self.response_status.value,
                self.signed_by.decode(),
            ]
        )

    @staticmethod
    def deserialize(src: bytes) -> FakeResponse:
        next_update, cert, status, signed_by = orjson.loads(src)
        return FakeResponse(
            next_update, CertStatus(cert), ResponseStatus(status), signed_by.encode()
        )

    def authenticate_for(self, issuer_der: bytes) -> bool:
        return issuer_der == self.signed_by


class FakeCertificate:
    def __init__(self, der: bytes) -> None:
        self.der = der

    def serialize(self) -> bytes:
        return self.der

    @staticmethod
    def deserialize(src: bytes) -> FakeCertificate:
        return FakeCertificate(src)

    def public_bytes(self) -> bytes:
        return self.der


class FakeRevocationStatus:
    """Mirrors niquests' ``InMemoryRevocationStatus`` pickling contract."""

    def __init__(self) -> None:
        self._store: dict[str, FakeResponse] = {}
        self._issuers_map: dict[str, FakeCertificate] = {}

    @staticmethod
    def support_pickle() -> bool:
        return True

    def __len__(self) -> int:
        return len(self._store)

    def __getstate__(self) -> dict[str, Any]:
        return {
            "_max_size": 2048,
            "_store": {k: v.serialize() for k, v in self._store.items()},
            "_issuers_map": {k: v.serialize() for k, v in self._issuers_map.items()},
            "_failure_count": 0,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._store = {k: FakeResponse.deserialize(v) for k, v in state["_store"].items()}
        self._issuers_map = {
            k: FakeCertificate.deserialize(v) for k, v in state["_issuers_map"].items()
        }


class DictCache:
    """The ``get_entry``/``set_entry`` json-tier surface, in memory."""

    def __init__(self) -> None:
        self.rows: dict[str, Any] = {}
        self.writes = 0

    def get_entry(self, kind: str, key: str) -> Any:
        assert kind == "json"
        row = self.rows.get(key)
        return orjson.loads(row) if row is not None else None

    def set_entry(self, kind: str, key: str, value: Any) -> None:
        assert kind == "json"
        self.rows[key] = orjson.dumps(value)
        self.writes += 1


@pytest.fixture
def fake_ocsp(monkeypatch):
    """Route ``ocsp_cache`` to a fake niquests OCSP module and reset its state."""
    module = types.ModuleType(ocsp_cache._OCSP_MODULE)
    module.OCSPResponse = FakeResponse
    module.Certificate = FakeCertificate
    module.OCSPCertStatus = CertStatus
    module.OCSPResponseStatus = ResponseStatus
    module.InMemoryRevocationStatus = FakeRevocationStatus
    monkeypatch.setitem(sys.modules, ocsp_cache._OCSP_MODULE, module)
    monkeypatch.setattr(ocsp_cache, "_seen", set())
    return module


def _session(revocation_cache: FakeRevocationStatus | None = None) -> types.SimpleNamespace:
    adapter = types.SimpleNamespace(_ocsp_cache=revocation_cache)
    return types.SimpleNamespace(_ocsp_cache=revocation_cache, adapters={"https://": adapter})


def _checked_session(**responses: FakeResponse) -> types.SimpleNamespace:
    """A session whose revocation cache holds ``responses`` keyed by fingerprint."""
    revocation_cache = FakeRevocationStatus()
    for fingerprint, response in responses.items():
        revocation_cache._store[fingerprint] = response
        revocation_cache._issuers_map[fingerprint] = FakeCertificate(b"issuer-der")
    return _session(revocation_cache)


def _in_days(days: float) -> int:
    return int(time.time() + days * 86400)


def test_good_verdict_round_trips_into_the_next_session(fake_ocsp):
    cache = DictCache()
    assert ocsp_cache.persist(_checked_session(fp1=FakeResponse(_in_days(2))), cache) == 1

    ocsp_cache._seen.clear()  # a new process
    session = _session()
    assert ocsp_cache.seed(session, cache) == 1
    restored = session._ocsp_cache
    assert isinstance(restored, FakeRevocationStatus)
    assert session.adapters["https://"]._ocsp_cache is restored
    assert restored._store["fp1"].certificate_status is CertStatus.GOOD
    assert restored._issuers_map["fp1"].der == b"issuer-der"
    # Restored entries are not written back by the same process.
    assert ocsp_cache.persist(session, cache) == 0
    assert cache.writes == 1


def test_row_survives_the_sqlite_json_tier(fake_ocsp, tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        assert ocsp_cache.persist(_checked_session(fp1=FakeResponse(_in_days(1))), cache) == 1
        ocsp_cache._seen.clear()
        session = _session()
        assert ocsp_cache.seed(session, cache) == 1
    finally:
        cache.close()


@pytest.mark.parametrize(
    "response",
    [
        FakeResponse(_in_days(2), cert_status=CertStatus.REVOKED),
        FakeResponse(_in_days(2), cert_status=CertStatus.UNKNOWN),
        FakeResponse(_in_days(2), status=ResponseStatus.TRY_LATER),
        FakeResponse(int(time.time()) + 10),  # expires within MIN_REMAINING_SECONDS
        FakeResponse(0),  # no nextUpdate
        FakeResponse(_in_days(2), signed_by=b"someone-else"),  # signature does not verify
    ],
    ids=["revoked", "unknown", "unsuccessful", "expiring", "no-next-update", "bad-signature"],
)
def test_only_good_signed_live_verdicts_are_persisted(fake_ocsp, response):
    cache = DictCache()
    session = _checked_session(fp1=response)
    assert ocsp_cache.persist(session, cache) == 0
    assert cache.writes == 0
    # Judged once: a later call does not re-examine (or write) it.
    assert ocsp_cache.persist(session, cache) == 0


def test_persist_writes_only_new_verdicts(fake_ocsp):
    cache = DictCache()
    session = _checked_session(fp1=FakeResponse(_in_days(2)))
    assert ocsp_cache.persist(session, cache) == 1
    assert ocsp_cache.persist(session, cache) == 0
    session._ocsp_cache._store["fp2"] = FakeResponse(_in_days(3))
    session._ocsp_cache._issuers_map["fp2"] = FakeCertificate(b"issuer-der")
    assert ocsp_cache.persist(session, cache) == 1
    assert cache.writes == 2
    assert set(cache.get_entry("json", ocsp_cache.CACHE_KEY)["entries"]) == {"fp1", "fp2"}


def test_row_keeps_the_newest_entries(fake_ocsp, monkeypatch):
    cache = DictCache()
    clock = [time.time()]
    monkeypatch.setattr(ocsp_cache.time, "time", lambda: clock[0])
    for i in range(ocsp_cache.MAX_ENTRIES + 3):
        clock[0] += 1
        ocsp_cache.persist(_checked_session(**{f"fp{i}": FakeResponse(_in_days(2))}), cache)
    entries = cache.get_entry("json", ocsp_cache.CACHE_KEY)["entries"]
    assert len(entries) == ocsp_cache.MAX_ENTRIES
    assert "fp0" not in entries
    assert f"fp{ocsp_cache.MAX_ENTRIES + 2}" in entries


def _b64(response: FakeResponse) -> str:
    return base64.b64encode(response.serialize()).decode("ascii")


def _row_with(cache: DictCache, **overrides: Any) -> None:
    ocsp_cache.persist(_checked_session(fp1=FakeResponse(_in_days(2))), cache)
    row = cache.get_entry("json", ocsp_cache.CACHE_KEY)
    row["entries"]["fp1"].update(overrides.pop("entry", {}))
    row.update(overrides)
    cache.set_entry("json", ocsp_cache.CACHE_KEY, row)
    ocsp_cache._seen.clear()


@pytest.mark.parametrize(
    ("overrides", "why"),
    [
        ({"version": "0:old"}, "other row version"),
        ({"entry": {"saved_at": time.time() - ocsp_cache.MAX_AGE_SECONDS - 60}}, "too old"),
        ({"entry": {"next_update": int(time.time()) - 1}}, "expired by metadata"),
        ({"entry": {"response": "not base64!"}}, "undecodable"),
        (
            {"entry": {"response": _b64(FakeResponse(_in_days(2), CertStatus.REVOKED))}},
            "answer re-validates as revoked",
        ),
        (
            {"entry": {"response": _b64(FakeResponse(int(time.time()) - 5))}},
            "answer expired although metadata says live",
        ),
    ],
)
def test_seed_skips_unusable_rows(fake_ocsp, overrides, why):
    cache = DictCache()
    _row_with(cache, **overrides)
    session = _session()
    assert ocsp_cache.seed(session, cache) == 0, why
    assert session._ocsp_cache is None


def test_seed_never_replaces_an_existing_revocation_cache(fake_ocsp):
    cache = DictCache()
    ocsp_cache.persist(_checked_session(fp1=FakeResponse(_in_days(2))), cache)
    existing = FakeRevocationStatus()
    session = _session(existing)
    assert ocsp_cache.seed(session, cache) == 0
    assert session._ocsp_cache is existing


def test_seed_without_a_row_imports_nothing(monkeypatch):
    monkeypatch.setattr(ocsp_cache, "_ocsp_module", lambda: pytest.fail("imported OCSP module"))
    assert ocsp_cache.seed(_session(), DictCache()) == 0


def test_niquests_revocation_cache_keeps_the_pickling_contract():
    """The niquests/qh3 surface ``ocsp_cache`` relies on (catches upstream drift)."""
    module = ocsp_cache._ocsp_module()
    assert module is not None
    for name in ("OCSPResponse", "Certificate", "OCSPCertStatus", "OCSPResponseStatus"):
        assert hasattr(module, name)
    assert hasattr(module.OCSPResponse, "deserialize")
    assert hasattr(module.OCSPResponse, "authenticate_for")
    assert hasattr(module.Certificate, "deserialize")
    assert module.InMemoryRevocationStatus.support_pickle()
    revocation_cache = module.InMemoryRevocationStatus()
    state = revocation_cache.__getstate__()
    assert {"_max_size", "_store", "_issuers_map", "_failure_count"} <= set(state)
    revocation_cache.__setstate__(state)
    assert len(revocation_cache) == 0
    # A restored cache must stay below niquests' "hold" threshold (10 entries).
    assert ocsp_cache.MAX_ENTRIES < 10


def test_preload_hazmat_skips_the_quic_stack():
    code = (
        "import sys\n"
        "from tif1 import ocsp_cache\n"
        "ocsp_cache.preload_hazmat()\n"
        "assert 'qh3._hazmat' in sys.modules and 'qh3' not in sys.modules\n"
        "from niquests.extensions.revocation._ocsp import verify, OCSPResponse\n"
        "assert 'qh3' not in sys.modules, 'qh3 package imported'\n"
        "assert OCSPResponse.__module__ == 'qh3._hazmat'\n"
        "import qh3, qh3.quic.connection\n"  # HTTP/3 still imports the full package
        "from qh3._hazmat import OCSPResponse as again\n"
        "assert again is OCSPResponse\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True, timeout=60)


def test_prepare_revocation_check_respects_the_switch(fake_ocsp, monkeypatch):
    calls: list[Any] = []
    monkeypatch.setattr(ocsp_cache, "seed", lambda session, _cache: calls.append(session) or 0)
    monkeypatch.setattr(ocsp_cache, "preload_hazmat", lambda: None)
    monkeypatch.setattr(http_session, "_ocsp_persist", False)
    monkeypatch.setattr("tif1.cache.get_cache", lambda: DictCache())

    http_session._prepare_revocation_check(_session(), {"http_revocation_cache": False})
    http_session._prepare_revocation_check(_session(), {"offline_mode": True})
    http_session._prepare_revocation_check(_session(), {"enable_cache": False})
    assert calls == []
    assert http_session._ocsp_persist is False

    session = _session()
    http_session._prepare_revocation_check(session, {})
    assert calls == [session]
    assert http_session._ocsp_persist is True


def test_persist_ocsp_verdicts_is_a_no_op_when_off(monkeypatch):
    monkeypatch.setattr(http_session, "_ocsp_persist", False)
    monkeypatch.setattr(ocsp_cache, "persist", lambda *_args: pytest.fail("persisted"))
    http_session.persist_ocsp_verdicts(_session())


def test_persist_ocsp_verdicts_uses_the_open_cache_only(monkeypatch):
    import tif1.cache as cache_module

    seen: list[Any] = []
    monkeypatch.setattr(http_session, "_ocsp_persist", True)
    monkeypatch.setattr(ocsp_cache, "persist", lambda _session, cache: seen.append(cache))
    monkeypatch.setattr(cache_module, "_cache", None)
    http_session.persist_ocsp_verdicts(_session())
    assert seen == []  # never opens a cache (e.g. at exit after the cache closed)

    opened = DictCache()
    monkeypatch.setattr(cache_module, "_cache", opened)
    http_session.persist_ocsp_verdicts(_session())
    assert seen == [opened]


def test_warm_up_threads_persist_after_signalling(monkeypatch):
    order: list[str] = []

    class Session:
        def head(self, _url, timeout):
            del timeout
            order.append("head")

    monkeypatch.setattr(http_session, "get_session", Session)
    monkeypatch.setattr(
        http_session, "persist_ocsp_verdicts", lambda _session=None: order.append("persist")
    )
    http_session.warm_connections(["https://a/x.json"], timeout=1)
    http_session.wait_for_warm_connections()
    deadline = time.monotonic() + 5
    while len(order) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert order == ["head", "persist"]
