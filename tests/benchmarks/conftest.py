"""CodSpeed benchmark collection filter.

When running with --codspeed, only collect tests that use the ``benchmark``
fixture so that validation / assertion-only helpers in this directory are
not executed under instrumentation.
"""

from __future__ import annotations


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--codspeed", default=False):
        return

    selected = []
    deselected = []
    for item in items:
        if "benchmark" in getattr(item, "fixturenames", ()):
            selected.append(item)
        else:
            deselected.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected
