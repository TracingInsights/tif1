"""Cyclic-GC control for allocation bursts (Rhodes R3, Naxos N10).

:func:`suspend_gc` pauses the cyclic collector around a burst of container
allocations that creates no reference cycles (frame builds, module imports).
With ``promote`` it also hands the burst's survivors to the oldest generation
(:func:`promote_young_objects`), so the first young collection after the
burst does not traverse them all to find nothing to free.

pandas-free and numpy-free: :mod:`tif1.core_utils.lazy_import` uses it
before either is imported.
"""

from __future__ import annotations

import contextlib
import gc
import sys
from collections.abc import Iterator

_GC_PROMOTE_OK: bool | None = None  # resolved by ``gc_promote_ok`` on first use


def gc_promote_ok() -> bool:
    """Whether ``gc.freeze(); gc.unfreeze()`` is an O(1) move into the oldest generation.

    True on GIL builds of CPython 3.11-3.13, whose generational collector
    splices its generation lists into the permanent one and back. The
    free-threaded collector walks the whole heap on both calls and the
    incremental collector of 3.14 re-marks the lists it moves, so neither
    takes this route. Resolved after the first promoted burst, which is the
    pandas or polars import: both import :mod:`sysconfig` (3 ms on its own).
    """
    global _GC_PROMOTE_OK
    ok = _GC_PROMOTE_OK
    if ok is None:
        import sysconfig

        ok = _GC_PROMOTE_OK = bool(
            sys.implementation.name == "cpython"
            and sys.version_info < (3, 14)
            and not sysconfig.get_config_var("Py_GIL_DISABLED")
            and hasattr(gc, "freeze")
        )
    return ok


def promote_young_objects() -> None:
    """Move the objects of the young generations into the oldest one without a traversal.

    Naxos N10: a burst run under :func:`suspend_gc` leaves every container
    it kept in generation 0, and the first collection after GC is back on
    traverses all of them to find no garbage: a race's 1128 lap frames
    (~55 000 blocks, arrays, managers and reference trackers) cost 9 ms of a
    warm pandas ``fetch_all_laps_telemetry()``, and the ~150 000 module
    objects of ``import pandas`` cost ~50 young collections during the
    import and more after it. Those objects are long-lived (memoized by the
    session, or modules), so they go straight to the oldest generation,
    where a full collection still visits them. Skipped when the young
    generation is below the collection threshold (nothing to save), when the
    process froze objects itself (``gc.unfreeze`` would release them) and
    where the move is not O(1) (:func:`gc_promote_ok`).
    """
    if (
        gc.get_count()[0] >= gc.get_threshold()[0]
        and gc_promote_ok()
        and gc.get_freeze_count() == 0
    ):
        gc.freeze()
        gc.unfreeze()


@contextlib.contextmanager
def suspend_gc(*, promote: bool = False) -> Iterator[None]:
    """Pause cyclic GC for a short-lived burst of container allocations.

    Batch frame reads/writes allocate ~100 MB of short-lived objects in a
    tight loop; the resulting gen-0 collections cost ~40% of the unpickle
    loop (Rhodes R3: 470 -> 264 ms on the Monaco 1452-frame set) while no
    reference cycles are created. Restores the prior GC state, so nesting
    and callers that disabled GC themselves are safe.

    ``promote`` (bursts whose survivors are kept: memoized frames, imported
    modules) moves the survivors into the oldest generation on a normal exit
    (:func:`promote_young_objects`) instead of leaving them to the next young
    collection.
    """
    if gc.isenabled():
        gc.disable()
        try:
            yield
            if promote:
                promote_young_objects()
        finally:
            gc.enable()
    else:
        yield


__all__ = ["gc_promote_ok", "promote_young_objects", "suspend_gc"]
