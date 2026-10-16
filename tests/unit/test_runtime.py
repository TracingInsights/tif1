"""tif1.core_utils.runtime: Pyodide detection and the synchronous bridge."""

from __future__ import annotations

import asyncio
import sys
import types

import pytest

from tif1 import core
from tif1.core_utils import runtime
from tif1.exceptions import AsyncRequiredError


def install_fake_pyodide(monkeypatch, *, can_run_sync: bool) -> list:
    """Register a fake ``pyodide.ffi`` whose ``run_sync`` drives coroutines with asyncio."""
    calls: list = []
    ffi = types.ModuleType("pyodide.ffi")
    ffi.can_run_sync = lambda: can_run_sync

    def run_sync(awaitable):
        calls.append(awaitable)
        return asyncio.run(awaitable)

    ffi.run_sync = run_sync
    package = types.ModuleType("pyodide")
    package.ffi = ffi
    monkeypatch.setitem(sys.modules, "pyodide", package)
    monkeypatch.setitem(sys.modules, "pyodide.ffi", ffi)
    return calls


class TestDetection:
    def test_native_runtime_constants(self):
        assert runtime.IS_EMSCRIPTEN is False
        assert runtime.THREADS_AVAILABLE is True
        assert runtime.can_block_on_awaitables() is True

    def test_is_pyodide_needs_emscripten_and_the_module(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "pyodide", types.ModuleType("pyodide"))
        assert runtime.is_pyodide() is False
        monkeypatch.setattr(runtime, "IS_EMSCRIPTEN", True)
        assert runtime.is_pyodide() is True
        monkeypatch.delitem(sys.modules, "pyodide")
        assert runtime.is_pyodide() is False


class TestCanBlockOnAwaitables:
    def test_false_without_the_pyodide_module(self, monkeypatch):
        monkeypatch.setattr(runtime, "IS_EMSCRIPTEN", True)
        monkeypatch.setitem(sys.modules, "pyodide", None)
        monkeypatch.setitem(sys.modules, "pyodide.ffi", None)
        assert runtime.can_block_on_awaitables() is False

    @pytest.mark.parametrize("jspi", [True, False])
    def test_reports_the_jspi_probe(self, monkeypatch, jspi):
        monkeypatch.setattr(runtime, "IS_EMSCRIPTEN", True)
        install_fake_pyodide(monkeypatch, can_run_sync=jspi)
        assert runtime.can_block_on_awaitables() is jspi


class TestRunSyncInBrowser:
    def test_runs_the_coroutine_through_jspi(self, monkeypatch):
        monkeypatch.setattr(runtime, "IS_EMSCRIPTEN", True)
        calls = install_fake_pyodide(monkeypatch, can_run_sync=True)

        async def work():
            return 42

        assert runtime.run_sync_in_browser(work, "work", alternative="session.work_async()") == 42
        assert len(calls) == 1

    def test_without_jspi_raises_before_building_the_coroutine(self, monkeypatch):
        monkeypatch.setattr(runtime, "IS_EMSCRIPTEN", True)
        install_fake_pyodide(monkeypatch, can_run_sync=False)
        built: list = []

        def factory():
            built.append(1)
            return asyncio.sleep(0)

        with pytest.raises(AsyncRequiredError) as info:
            runtime.run_sync_in_browser(factory, "laps", alternative="session.laps_async()")
        assert built == []
        assert "await session.laps_async()" in str(info.value)
        assert info.value.context["operation"] == "laps"


class TestCoreBridge:
    def test_native_path_runs_asyncio(self, monkeypatch):
        seen: list[str] = []
        monkeypatch.setattr(core, "_ensure_nested_loop_support", seen.append)

        async def work():
            return "done"

        assert core._run_coroutine_sync(work, "op") == "done"
        assert seen == ["op"]

    def test_browser_path_uses_jspi(self, monkeypatch):
        monkeypatch.setattr(core, "IS_EMSCRIPTEN", True)
        monkeypatch.setattr(runtime, "IS_EMSCRIPTEN", True)
        calls = install_fake_pyodide(monkeypatch, can_run_sync=True)

        async def work():
            return 7

        assert core._run_coroutine_sync(work, "op") == 7
        assert len(calls) == 1

    def test_browser_path_without_jspi_names_the_async_twin(self, monkeypatch):
        monkeypatch.setattr(core, "IS_EMSCRIPTEN", True)
        monkeypatch.setattr(runtime, "IS_EMSCRIPTEN", True)
        install_fake_pyodide(monkeypatch, can_run_sync=False)

        async def work():  # pragma: no cover - never built
            return None

        with pytest.raises(AsyncRequiredError, match=r"session\.get_fastest_laps_async"):
            core._run_coroutine_sync(work, "get_fastest_laps")

    def test_async_alternative_falls_back_to_laps_async(self):
        assert core._async_alternative("get_fastest_laps_tels") == (
            "session.get_fastest_laps_tels_async()"
        )
        assert core._async_alternative("get_driver") == "session.laps_async()"


class TestImportSupportedPolars:
    def test_missing_polars(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "polars", None)
        assert runtime.import_supported_polars() is None

    def test_rejects_polars_1(self, monkeypatch, caplog):
        fake = types.ModuleType("polars")
        fake.__version__ = "1.33.1"
        monkeypatch.setitem(sys.modules, "polars", fake)
        monkeypatch.setattr(runtime, "_polars_warned", False)
        with caplog.at_level("WARNING", logger="tif1.core_utils.runtime"):
            assert runtime.import_supported_polars() is None
            assert runtime.import_supported_polars() is None
        assert sum("polars 1.33.1" in record.message for record in caplog.records) == 1

    def test_accepts_polars_2(self, monkeypatch):
        fake = types.ModuleType("polars")
        fake.__version__ = "2.1.0"
        monkeypatch.setitem(sys.modules, "polars", fake)
        assert runtime.import_supported_polars() is fake
