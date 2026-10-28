"""Exercise cache recovery and atomic replacement without network access."""

# ruff: noqa: PT009

from __future__ import annotations

import gzip
import os
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import orjson
import weekend_data as data


class CacheTests(unittest.TestCase):
    """Isolate each request cache and make unexpected network access fail."""

    URL = "https://example.invalid/session.json"

    def setUp(self) -> None:
        """Create an isolated cache directory and replace HTTP with a guarded mock."""
        temporary = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        cache_patch = patch.object(data, "ROOT", self.root)
        cache_patch.start()
        self.addCleanup(cache_patch.stop)
        request_patch = patch.object(
            data.requests, "get", side_effect=AssertionError("Unexpected network call")
        )
        self.request = request_patch.start()
        self.addCleanup(request_patch.stop)
        sleep_patch = patch.object(data.time, "sleep")
        sleep_patch.start()
        self.addCleanup(sleep_patch.stop)

    def cache_path(self, params: dict[str, Any] | None = None) -> Path:
        """Resolve the request key using the production URL-and-parameters scheme."""
        return data.cache_path_for(self.URL, params)

    def write_envelope(self, payload: Any, params: dict[str, Any] | None = None) -> Path:
        """Write a known valid cache envelope and return its path."""
        path = self.cache_path(params)
        with gzip.open(path, "wb") as stream:
            stream.write(orjson.dumps({"data": payload, "status": "ok"}))
        return path

    def allow_download(self, payload: Any) -> None:
        """Provide a deterministic successful HTTP response without opening a socket."""
        response = Mock(status_code=200, url=self.URL, text="")
        response.json.return_value = payload
        self.request.side_effect = None
        self.request.return_value = response

    def make_stale(self, path: Path) -> None:
        """Expire a cache entry so a live request must refresh it."""
        old = time.time() - 7200
        os.utime(path, (old, old))

    def invalid_entries(self) -> dict[str, bytes]:
        """Return representative gzip, JSON, encoding, and envelope corruptions."""
        valid = gzip.compress(orjson.dumps({"data": {"old": True}, "status": "ok"}))
        return {
            "not gzip": b"not a gzip stream",
            "truncated gzip": valid[:-5],
            "invalid JSON": gzip.compress(b'{"data":'),
            "invalid UTF-8": gzip.compress(b"\xff\xfe"),
            "non-object envelope": gzip.compress(b"[]"),
            "missing data": gzip.compress(b'{"status":"ok"}'),
            "missing status": gzip.compress(b'{"data":{}}'),
            "invalid status": gzip.compress(b'{"data":{},"status":false}'),
        }

    def test_valid_cache_read_uses_parameter_key_without_network(self) -> None:
        """Valid matching entries are returned as-is, including stale offline reads."""
        params = {"b": 2, "a": 1}
        payload = {"lap": [1, 2], "drv": ["NOR", "VER"]}
        path = self.write_envelope(payload, params)
        self.assertEqual(data.cached_get(self.URL, {"a": 1, "b": 2}), (payload, "ok"))
        self.make_stale(path)
        self.assertEqual(
            data.cached_get(self.URL, params, live=True, offline=True), (payload, "ok")
        )
        self.request.assert_not_called()

    def test_offline_missing_cache_has_explicit_reason_and_no_network(self) -> None:
        """Missing offline requests return a miss instead of attempting a download."""
        self.assertEqual(data.cached_get(self.URL, offline=True), (None, "not cached"))
        self.request.assert_not_called()

    def test_offline_corruption_is_reported_without_network(self) -> None:
        """Malformed cache bytes and envelopes never crash an offline read."""
        for label, content in self.invalid_entries().items():
            with self.subTest(corruption=label):
                self.cache_path().write_bytes(content)
                payload, status = data.cached_get(self.URL, offline=True)
                self.assertIsNone(payload)
                self.assertIn("corrupt cache", status)
                self.assertEqual(self.cache_path().read_bytes(), content)
        self.request.assert_not_called()

    def test_offline_unreadable_cache_is_reported_without_network(self) -> None:
        """Read permission failures are explicit offline misses rather than exceptions."""
        self.write_envelope({"old": True})
        with patch.object(data, "_read_cache", side_effect=PermissionError("read denied")):
            payload, status = data.cached_get(self.URL, offline=True)
        self.assertIsNone(payload)
        self.assertIn("unreadable cache", status)
        self.assertIn("PermissionError", status)
        self.request.assert_not_called()

    def test_online_corruption_downloads_and_repairs_the_cache(self) -> None:
        """Each invalid cache form is replaced by a complete successful response."""
        fresh = {"lap": [3, 4], "drv": ["LEC", "HAM"]}
        self.allow_download(fresh)
        for label, content in self.invalid_entries().items():
            with self.subTest(corruption=label):
                self.request.reset_mock()
                self.cache_path().write_bytes(content)
                self.assertEqual(data.cached_get(self.URL), (fresh, "ok"))
                self.request.assert_called_once_with(self.URL, params=None, timeout=35)
                self.assertEqual(data.cached_get(self.URL, offline=True), (fresh, "ok"))
                self.assertFalse(list(self.root.glob("*.tmp")))

    def test_online_unreadable_cache_is_a_downloadable_miss(self) -> None:
        """An unreadable old entry does not prevent downloading and caching a fresh response."""
        fresh = {"version": "readable replacement"}
        self.write_envelope({"version": "unreadable old entry"})
        self.allow_download(fresh)
        with patch.object(data, "_read_cache", side_effect=PermissionError("read denied")):
            self.assertEqual(data.cached_get(self.URL), (fresh, "ok"))
        self.request.assert_called_once_with(self.URL, params=None, timeout=35)
        self.assertEqual(data.cached_get(self.URL, offline=True), (fresh, "ok"))
        self.assertFalse(list(self.root.glob("*.tmp")))

    def test_complete_unique_temporary_files_are_atomically_replaced(self) -> None:
        """Atomic replacement receives closed valid gzip files in the destination directory."""
        path = self.write_envelope({"version": 0})
        real_replace = Path.replace
        temporary_names = []

        def inspect_replace(source: Path, destination: Path) -> None:
            """Inspect both complete versions before performing the actual atomic move."""
            source, destination = Path(source), Path(destination)
            self.assertEqual(source.parent, destination.parent)
            self.assertNotEqual(source, destination)
            self.assertEqual(destination, path)
            expected = len(temporary_names)
            self.assertEqual(data._read_cache(destination), ({"version": expected}, "ok"))
            self.assertEqual(data._read_cache(source), ({"version": expected + 1}, "ok"))
            temporary_names.append(source)
            real_replace(source, destination)

        with patch.object(data.Path, "replace", autospec=True, side_effect=inspect_replace):
            for version in (1, 2):
                data._atomic_cache_write(path, {"data": {"version": version}, "status": "ok"})
        self.assertEqual(len(set(temporary_names)), 2)
        self.assertEqual(data._read_cache(path), ({"version": 2}, "ok"))
        self.assertEqual(sorted(self.root.iterdir()), [path])
        self.request.assert_not_called()

    def test_failed_replace_preserves_existing_cache_and_cleans_temporary_file(self) -> None:
        """A replacement failure warns but returns fresh data and keeps the old cache intact."""
        old, fresh = {"version": "old"}, {"version": "new"}
        path = self.write_envelope(old)
        original = path.read_bytes()
        self.make_stale(path)
        self.allow_download(fresh)
        with patch.object(
            data.Path, "replace", autospec=True, side_effect=PermissionError("replace denied")
        ):
            with self.assertWarnsRegex(RuntimeWarning, "Cache write failed"):
                self.assertEqual(data.cached_get(self.URL, live=True), (fresh, "ok"))
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(data.cached_get(self.URL, offline=True), (old, "ok"))
        self.assertEqual(sorted(self.root.iterdir()), [path])

    def test_partial_write_preserves_existing_cache_and_cleans_temporary_file(self) -> None:
        """An interrupted gzip payload is removed without exposing or replacing its old version."""
        old, fresh = {"version": "old"}, {"version": "new"}
        path = self.write_envelope(old)
        original = path.read_bytes()
        self.make_stale(path)
        self.allow_download(fresh)

        real_write = gzip.GzipFile.write

        def interrupted_write(stream: gzip.GzipFile, payload: bytes) -> int:
            """Compress only a payload fragment before simulating a storage failure."""
            real_write(stream, payload[:5])
            raise OSError("storage full")

        with patch.object(
            data.gzip.GzipFile, "write", autospec=True, side_effect=interrupted_write
        ):
            with self.assertWarnsRegex(RuntimeWarning, "Cache write failed"):
                self.assertEqual(data.cached_get(self.URL, live=True), (fresh, "ok"))
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(data.cached_get(self.URL, offline=True), (old, "ok"))
        self.assertEqual(sorted(self.root.iterdir()), [path])

    def test_cache_directory_write_failure_keeps_successful_download_usable(self) -> None:
        """A cache directory creation failure is a warning rather than a failed HTTP result."""
        fresh = {"downloaded": True}
        self.allow_download(fresh)
        with patch.object(data.Path, "mkdir", side_effect=PermissionError("directory denied")):
            with self.assertWarnsRegex(RuntimeWarning, "Cache write failed"):
                self.assertEqual(data.cached_get(self.URL), (fresh, "ok"))
        self.assertEqual(list(self.root.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
