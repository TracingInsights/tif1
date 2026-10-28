"""Exercise Jolpica retry exhaustion and season cache finalization without sockets."""

from __future__ import annotations

import datetime as dt
import hashlib
import io
import os
import socket
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import Mock, call, patch

import cota_predictor_improved as model
import orjson
import pytest
import requests
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.response import HTTPResponse
from urllib3.util.retry import Retry


class JolpicaTests(unittest.TestCase):
    """Give every request its own cache and forbid unexpected real connections."""

    def setUp(self) -> None:
        """Isolate cache writes and guard the actual socket boundary."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for active_patch in (
            patch.object(model, "CACHE", self.root),
            patch.object(model, "OFFLINE", False),
            patch.object(socket.socket, "connect", side_effect=AssertionError("Unexpected socket")),
        ):
            active_patch.start()
            self.addCleanup(active_patch.stop)

    def cache_entry(
        self,
        path: str,
        payload: dict[str, Any],
        written: float,
        params: dict[str, Any] | None = None,
    ) -> Path:
        """Write an existing snapshot with a controlled download timestamp."""
        key = hashlib.sha256(
            path.encode() + orjson.dumps(params or {}, option=orjson.OPT_SORT_KEYS)
        ).hexdigest()
        destination = self.root / f"{key}.json"
        destination.write_bytes(orjson.dumps(payload))
        os.utime(destination, (written, written))
        return destination

    def success_response(self, payload: dict[str, Any]) -> requests.Response:
        """Build a requests response containing deterministic API JSON."""
        response = requests.Response()
        response.status_code = 200
        response.url = f"{model.BASE}/2025/results.json"
        response._content = orjson.dumps(payload)
        return response

    def adapter_responses(self, statuses: list[int]) -> tuple[Any, Mock]:
        """Exercise the real adapter/pool retry machinery with fake response bytes."""
        pool = HTTPConnectionPool("api.jolpi.ca")
        self.addCleanup(pool.close)
        # _make_request is the boundary that would send bytes to a real socket.
        responses = [
            HTTPResponse(
                body=io.BytesIO(orjson.dumps({"status": status})),
                status=status,
                headers={"Retry-After": "0"},
                preload_content=False,
            )
            for status in statuses
        ]
        adapter = model.HTTP.get_adapter(f"{model.BASE}/2025/results.json")
        connection_patch = patch.object(
            adapter, "get_connection_with_tls_context", return_value=pool
        )
        request_patch = patch.object(pool, "_make_request", side_effect=responses)
        for active_patch in (connection_patch, request_patch, patch.object(Retry, "sleep")):
            mocked = active_patch.start()
            self.addCleanup(active_patch.stop)
            if active_patch is request_patch:
                request = mocked
        return adapter, request

    def test_exhausted_adapter_429_reaches_bounded_outer_retry(self) -> None:
        """Persistent 429s exhaust both retry layers without hiding in RetryError."""
        adapter = model.HTTP.get_adapter(model.BASE)
        maximum_requests = (adapter.max_retries.total + 1) * 6
        _, request = self.adapter_responses([429] * maximum_requests)
        with patch.object(model.time, "sleep") as sleep:
            with pytest.raises(RuntimeError, match="kept rate-limiting"):
                model.jget("2025/results.json")
        assert request.call_count == maximum_requests
        assert sleep.call_args_list == [call(1), call(2), call(4), call(8), call(16)]
        assert not list(self.root.iterdir())

    def test_last_outer_attempt_can_succeed_after_adapter_exhaustion(self) -> None:
        """The sixth manual attempt still gets a chance to return valid JSON."""
        adapter = model.HTTP.get_adapter(model.BASE)
        rate_limited_requests = (adapter.max_retries.total + 1) * 5
        _, request = self.adapter_responses([429] * rate_limited_requests + [200])
        with patch.object(model.time, "sleep") as sleep:
            actual = model.jget("2025/results.json")
        assert actual == {"status": 200}
        assert request.call_count == rate_limited_requests + 1
        assert sleep.call_args_list == [call(1), call(2), call(4), call(8), call(16), call(0.3)]
        with patch.object(
            model.HTTP, "get", side_effect=AssertionError("Cached request used HTTP")
        ):
            assert model.jget("2025/results.json") == actual

    def test_exhausted_server_error_raises_http_error(self) -> None:
        """Other exhausted statuses still fail explicitly rather than being cached."""
        adapter = model.HTTP.get_adapter(model.BASE)
        _, request = self.adapter_responses([503] * (adapter.max_retries.total + 1))
        with pytest.raises(requests.HTTPError, match="503"):
            model.jget("2025/results.json")
        assert request.call_count == adapter.max_retries.total + 1
        assert not list(self.root.iterdir())

    def test_midseason_snapshot_refreshes_after_season_finalization(self) -> None:
        """A forever-cache TTL cannot freeze a previous year's incomplete snapshot."""
        endpoint = "2025/results.json"
        watermark = dt.datetime(2026, 1, 1, tzinfo=dt.UTC).timestamp()
        cached = self.cache_entry(endpoint, {"rounds": 8}, watermark - 86400)
        with (
            patch.object(
                model.HTTP, "get", return_value=self.success_response({"rounds": 24})
            ) as get,
            patch.object(model.time, "sleep"),
        ):
            assert model.jget(endpoint, ttl=None, valid_after=watermark) == {"rounds": 24}
        get.assert_called_once()
        assert orjson.loads(cached.read_bytes()) == {"rounds": 24}

    def test_finalized_snapshot_is_reused_including_exact_boundary(self) -> None:
        """A snapshot downloaded at or after the watermark can remain cached."""
        endpoint = "2025/results.json"
        watermark = dt.datetime(2026, 1, 1, tzinfo=dt.UTC).timestamp()
        for written in (watermark, watermark + 86400):
            self.cache_entry(endpoint, {"rounds": 24}, written)
            with patch.object(model.HTTP, "get", side_effect=AssertionError("Unexpected HTTP")):
                assert model.jget(endpoint, ttl=None, valid_after=watermark) == {"rounds": 24}

    def test_live_ttl_still_expires_even_after_watermark(self) -> None:
        """The completion watermark does not override an explicit age limit."""
        endpoint = "2025/results.json"
        watermark = dt.datetime(2026, 1, 1, tzinfo=dt.UTC).timestamp()
        self.cache_entry(endpoint, {"old": True}, watermark)
        with (
            patch.object(model.time, "time", return_value=watermark + 3601),
            patch.object(
                model.HTTP, "get", return_value=self.success_response({"new": True})
            ) as get,
            patch.object(model.time, "sleep"),
        ):
            assert model.jget(endpoint, ttl=3600, valid_after=watermark) == {"new": True}
        get.assert_called_once()

    def test_offline_accepts_old_snapshot_without_http(self) -> None:
        """Offline mode deliberately accepts existing stale snapshots reproducibly."""
        endpoint = "2025/results.json"
        watermark = dt.datetime(2026, 1, 1, tzinfo=dt.UTC).timestamp()
        self.cache_entry(endpoint, {"rounds": 8}, watermark - 86400)
        with (
            patch.object(model, "OFFLINE", True),
            patch.object(model.HTTP, "get", side_effect=AssertionError("Offline HTTP")),
        ):
            assert model.jget(endpoint, ttl=1, valid_after=watermark) == {"rounds": 8}
            with pytest.raises(RuntimeError, match="Offline Jolpica cache is missing"):
                model.jget("2025/qualifying.json", valid_after=watermark)

    def test_every_page_receives_the_completion_watermark(self) -> None:
        """Refresh all pagination pages and continue merging split race entries."""
        pages = [
            {
                "MRData": {
                    "total": "101",
                    "RaceTable": {"Races": [{"round": "1", "Results": [{"driver": "first"}]}]},
                }
            },
            {
                "MRData": {
                    "total": "101",
                    "RaceTable": {"Races": [{"round": "1", "Results": [{"driver": "second"}]}]},
                }
            },
        ]
        with patch.object(model, "jget", side_effect=pages) as get:
            races = model.fetch_races(2025, "results", None, valid_after=123.0)
        assert races[1]["Results"] == [{"driver": "first"}, {"driver": "second"}]
        assert get.call_args_list == [
            call("2025/results.json", {"limit": 100, "offset": 0}, None, valid_after=123.0),
            call("2025/results.json", {"limit": 100, "offset": 100}, None, valid_after=123.0),
        ]

    def test_history_assigns_previous_year_and_live_cache_policies(self) -> None:
        """Apply the year-end watermark to both result and qualifying endpoints."""
        now = dt.datetime.now(dt.UTC)
        previous = now.year - 1
        watermark = dt.datetime(now.year, 1, 1, tzinfo=dt.UTC).timestamp()
        with patch.object(model, "fetch_races", return_value={}) as fetch:
            assert model.load_history(previous, now.year).empty
        assert fetch.call_args_list == [
            call(previous, "results", None, valid_after=watermark),
            call(previous, "qualifying", None, valid_after=watermark),
            call(now.year, "results", 3600, valid_after=None),
            call(now.year, "qualifying", 3600, valid_after=None),
        ]


if __name__ == "__main__":
    unittest.main()
