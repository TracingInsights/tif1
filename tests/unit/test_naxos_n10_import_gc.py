"""Naxos N10: tif1 imports pandas, polars and its own heavy modules without young GC passes.

The first import of a session's library (``LazyModule`` / ``import_promoted``
for pandas, numpy and ``tif1.models``; ``import_supported_polars``; the
``tif1.__getattr__`` exports) runs under ``suspend_gc(promote=True)``: no
young collection runs during the import, and its ~150 000 long-lived module
objects go straight to the oldest generation instead of being traversed by
the young collections during and after it.
"""

from __future__ import annotations

import gc
import os
import subprocess
import sys
import textwrap

import pytest

from tif1.core_utils import gc_control, lazy_import
from tif1.core_utils.gc_control import gc_promote_ok

# CPython 3.12 starts with interpreter objects in the permanent generation
# (375 on 3.12.14, before any import); tif1 never promotes in a process with
# frozen objects, so the promotion tests only run where the count starts at 0.
_FROZEN_AT_START = gc.get_freeze_count()

needs_promotion = pytest.mark.skipif(
    not gc_promote_ok() or _FROZEN_AT_START != 0,
    reason="no O(1) gc.freeze promotion here, or the interpreter starts with frozen objects",
)


def _in_oldest_generation(obj: object) -> bool:
    return any(item is obj for item in gc.get_objects(generation=2))


@pytest.fixture(autouse=True)
def _clean_gc_state():
    assert gc.isenabled()
    assert gc.get_freeze_count() == _FROZEN_AT_START
    gc.collect()
    yield
    gc.enable()
    if _FROZEN_AT_START == 0:  # never thaw the interpreter's own frozen objects
        gc.unfreeze()


def test_import_promoted_returns_a_loaded_module_untouched(monkeypatch) -> None:
    calls: list[int] = []
    monkeypatch.setattr(gc_control, "promote_young_objects", lambda: calls.append(1))
    assert lazy_import.import_promoted("json") is sys.modules["json"]
    assert calls == []


@needs_promotion
def test_import_promoted_moves_a_new_module_into_the_oldest_generation(
    tmp_path, monkeypatch
) -> None:
    count = gc.get_threshold()[0] * 2 + 100
    source = "OBJECTS = [dict(i=[i]) for i in range(" + str(count) + ")]\n"
    (tmp_path / "n10_heavy_module.py").write_text(source, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "n10_heavy_module", raising=False)
    module = lazy_import.import_promoted("n10_heavy_module")
    try:
        assert gc.isenabled()
        assert _in_oldest_generation(module.OBJECTS[-1])
        assert _in_oldest_generation(module.OBJECTS[-1]["i"])
    finally:
        sys.modules.pop("n10_heavy_module", None)


def test_lazy_module_loads_through_import_promoted(monkeypatch) -> None:
    seen: list[str] = []
    real = lazy_import.import_promoted

    def spy(name: str):
        seen.append(name)
        return real(name)

    monkeypatch.setattr(lazy_import, "import_promoted", spy)
    namespace: dict[str, object] = {}
    proxy = lazy_import.LazyModule("json", namespace, "json")
    namespace["json"] = proxy
    assert proxy.dumps([1]) == "[1]"
    assert seen == ["json"]
    assert namespace["json"] is sys.modules["json"]


_IMPORT_CHILD = textwrap.dedent(
    """
    import gc, sys
    from tif1 import core
    from tif1.core_utils import runtime
    collections = []
    gc.callbacks.append(lambda phase, info: phase == "start" and collections.append(info))
    if sys.argv[1] == "pandas":
        assert "pandas" not in sys.modules
        core._load_pandas_models()
        import pandas
        probe = pandas.DataFrame
    else:
        assert "polars" not in sys.modules
        polars = runtime.import_supported_polars()
        probe = polars.DataFrame
    oldest = gc.get_objects(generation=2)
    print(len(collections), any(obj is probe for obj in oldest), gc.isenabled())
    """
)


@needs_promotion
@pytest.mark.parametrize("library", ["pandas", "polars"])
def test_session_library_import_runs_no_young_collections(tmp_path, library) -> None:
    if library == "polars":
        pytest.importorskip("polars")
    env = {
        **os.environ,
        "HOME": str(tmp_path),
        "TIF1_CACHE_DIR": str(tmp_path / "cache"),
        "TIF1_OFFLINE_MODE": "true",
    }
    proc = subprocess.run(
        [sys.executable, "-c", _IMPORT_CHILD, library],
        check=True,
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )
    collections, in_oldest, enabled = proc.stdout.split()
    assert collections == "0"  # the whole import ran with cyclic GC suspended
    assert in_oldest == "True"  # its objects went straight to the oldest generation
    assert enabled == "True"
